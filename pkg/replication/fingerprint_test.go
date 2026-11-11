package replication

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/factory"
	"github.com/stretchr/testify/require"
)

// foreignETagStorage is a destination whose ETags follow another scheme than
// the local source's (as S3's MD5 does): it reports a fresh opaque ETag on
// every GetMetadata, so an ETag comparison with the source never matches.
type foreignETagStorage struct {
	common.Storage
	n atomic.Int64
}

func (f *foreignETagStorage) GetMetadata(ctx context.Context, key string) (*common.Metadata, error) {
	md, err := f.Storage.GetMetadata(ctx, key)
	if err != nil || md == nil {
		return md, err
	}
	f.n.Add(1)
	md.ETag = "foreign-" + md.ETag
	return md, nil
}

// PutWithMetadataResult reports the upload's identity in the same foreign
// form GetMetadata does.
func (f *foreignETagStorage) PutWithMetadataResult(ctx context.Context, key string, data io.Reader, md *common.Metadata) (*common.Metadata, error) {
	stored, err := f.Storage.(common.IdentityPutter).PutWithMetadataResult(ctx, key, data, md)
	if err != nil || stored == nil {
		return stored, err
	}
	out := *stored
	out.ETag = "foreign-" + stored.ETag
	return &out, nil
}

func newFingerprintSyncer(t *testing.T) (*Syncer, common.Storage) {
	t.Helper()
	source, err := factory.NewStorage("local", map[string]string{"path": t.TempDir()}, noCreds)
	require.NoError(t, err)
	destLocal, err := factory.NewStorage("local", map[string]string{"path": t.TempDir()}, noCreds)
	require.NoError(t, err)
	return &Syncer{
		policy:   common.ReplicationPolicy{ID: "fingerprint"},
		source:   source,
		dest:     &foreignETagStorage{Storage: destLocal},
		logger:   &mockLogger{},
		metrics:  NewReplicationMetrics(),
		auditLog: &mockAuditLogger{},
	}, source
}

// TestSync_SecondSyncWithNoChangeCopiesNothing (#222): a full sync from local
// to a backend with another ETag scheme copies N objects, a second sync with
// no change copies none, and changing one source object copies only it.
func TestSync_SecondSyncWithNoChangeCopiesNothing(t *testing.T) {
	const n = 5
	paths := map[string]func(*Syncer, context.Context) (*common.SyncResult, error){
		"serial":   (*Syncer).SyncAll,
		"parallel": func(s *Syncer, ctx context.Context) (*common.SyncResult, error) { return s.SyncAllParallel(ctx, 3) },
	}
	for name, sync := range paths {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			s, source := newFingerprintSyncer(t)
			for i := 0; i < n; i++ {
				require.NoError(t, source.PutWithMetadata(ctx, fmt.Sprintf("obj-%d", i),
					bytes.NewReader([]byte(fmt.Sprintf("data-%d", i))),
					&common.Metadata{Custom: map[string]string{"owner": "alice"}}))
			}

			r, err := sync(s, ctx)
			require.NoError(t, err)
			require.Equal(t, n, r.Synced, "first sync")
			require.Zero(t, r.Failed)

			r, err = sync(s, ctx)
			require.NoError(t, err)
			require.Zero(t, r.Synced, "second sync with no change")

			md, err := s.dest.GetMetadata(ctx, "obj-0")
			require.NoError(t, err)
			require.Equal(t, "alice", md.Custom["owner"], "user metadata kept")
			require.NotEmpty(t, md.Custom[MetaReplicationSourceETag])
			require.Equal(t, "6", md.Custom[MetaReplicationSourceSize])

			require.NoError(t, source.PutWithContext(ctx, "obj-2", bytes.NewReader([]byte("changed-and-longer"))))
			r, err = sync(s, ctx)
			require.NoError(t, err)
			require.Equal(t, 1, r.Synced, "sync after one source change")
		})
	}
}

func TestHasChanged_SourceFingerprint(t *testing.T) {
	src := &common.Metadata{ETag: "100-6", Size: 6}
	rec := func(etag, size string) *common.Metadata {
		md := sourceFingerprint(src)
		md.ETag, md.Custom[MetaReplicationSourceETag], md.Custom[MetaReplicationSourceSize] = "md5", etag, size
		return md
	}
	require.False(t, hasChanged(src, rec("100-6", "6")), "matching record")
	require.True(t, hasChanged(src, rec("101-6", "6")), "source ETag changed")
	require.True(t, hasChanged(src, rec("100-6", "7")), "recorded size differs")
	require.True(t, hasChanged(src, &common.Metadata{ETag: "md5", Size: 6, Custom: map[string]string{
		MetaReplicationSourceETag: "100-6"}}), "record without size")
	// No record: today's comparison (ETags differ, so changed).
	require.True(t, hasChanged(src, &common.Metadata{ETag: "md5", Size: 6}))
	require.False(t, hasChanged(src, &common.Metadata{ETag: "100-6", Size: 6}))

	fp := sourceFingerprint(&common.Metadata{Custom: map[string]string{MetaReplicationSourceETag: "upstream"}})
	require.NotContains(t, fp.Custom, MetaReplicationSourceETag, "no source ETag drops an upstream record")
	orig := map[string]string{"k": "v"}
	fp = sourceFingerprint(&common.Metadata{ETag: "e", Size: 1, Custom: orig})
	require.Equal(t, "e", fp.Custom[MetaReplicationSourceETag])
	require.Len(t, orig, 1, "source metadata not modified")
}

// readDest returns the destination copy of key.
func readDest(t *testing.T, s *Syncer, key string) string {
	t.Helper()
	r, err := s.dest.GetWithContext(context.Background(), key)
	require.NoError(t, err)
	defer func() { _ = r.Close() }()
	b, err := io.ReadAll(r)
	require.NoError(t, err)
	return string(b)
}

// TestSync_SameSecondSameSizeRewriteReplicated: local ETags are Unix seconds
// plus size, so two same-size writes in one second share an ETag. The
// rewrite must still be replicated (#222).
func TestSync_SameSecondSameSizeRewriteReplicated(t *testing.T) {
	ctx := context.Background()
	for attempt := 0; ; attempt++ {
		s, source := newFingerprintSyncer(t)
		require.NoError(t, source.PutWithContext(ctx, "k", bytes.NewReader([]byte("aaaa"))))
		before, err := source.GetMetadata(ctx, "k")
		require.NoError(t, err)
		r, err := s.SyncAll(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, r.Synced)

		require.NoError(t, source.PutWithContext(ctx, "k", bytes.NewReader([]byte("bbbb"))))
		after, err := source.GetMetadata(ctx, "k")
		require.NoError(t, err)
		if before.ETag != after.ETag {
			// Crossed a second boundary; the case needs one second.
			require.Less(t, attempt, 5, "could not write twice within one second")
			continue
		}

		r, err = s.SyncAll(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, r.Synced, "same-size rewrite in the same second")
		require.Equal(t, "bbbb", readDest(t, s, "k"))
		return
	}
}

// TestSync_MetadataOnlyUpdateReplicated: a source update that changes only
// metadata is a change (#222).
func TestSync_MetadataOnlyUpdateReplicated(t *testing.T) {
	ctx := context.Background()
	s, source := newFingerprintSyncer(t)
	require.NoError(t, source.PutWithMetadata(ctx, "k", bytes.NewReader([]byte("data")),
		&common.Metadata{ContentType: "text/plain", Custom: map[string]string{"owner": "alice"}}))
	r, err := s.SyncAll(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, r.Synced)

	md, err := source.GetMetadata(ctx, "k")
	require.NoError(t, err)
	md.Custom = map[string]string{"owner": "bob"}
	require.NoError(t, source.UpdateMetadata(ctx, "k", md))

	r, err = s.SyncAll(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, r.Synced, "metadata-only update")
	got, err := s.dest.GetMetadata(ctx, "k")
	require.NoError(t, err)
	require.Equal(t, "bob", got.Custom["owner"])

	r, err = s.SyncAll(ctx)
	require.NoError(t, err)
	require.Zero(t, r.Synced, "no change after the update was copied")
}

func TestHasChanged_PreciseFingerprint(t *testing.T) {
	t0 := time.Unix(100, 5)
	src := &common.Metadata{ETag: "100-6", Size: 6, LastModified: t0, ContentType: "a",
		Custom: map[string]string{"k": "v"}}
	dest := sourceFingerprint(src)
	require.False(t, hasChanged(src, dest))

	later := *src
	later.LastModified = time.Unix(100, 6)
	require.True(t, hasChanged(&later, dest), "same ETag, later nanosecond")

	meta := *src
	meta.Custom = map[string]string{"k": "w"}
	require.True(t, hasChanged(&meta, dest), "metadata-only change")

	ct := *src
	ct.ContentType = "b"
	require.True(t, hasChanged(&ct, dest), "content type change")

	old := &common.Metadata{Custom: map[string]string{
		MetaReplicationSourceETag: "100-6", MetaReplicationSourceSize: "6"}}
	require.True(t, hasChanged(src, old), "record without the precise fields")
}

// replaceOnOpen replaces the object with newer bytes right after it opens
// a reader on it, as a writer racing a sync would.
type replaceOnOpen struct {
	common.Storage
	data []byte
	done bool
}

func (r *replaceOnOpen) GetWithContext(ctx context.Context, key string) (io.ReadCloser, error) {
	rc, err := r.Storage.GetWithContext(ctx, key)
	if err == nil && !r.done {
		r.done = true
		if perr := r.Storage.PutWithContext(ctx, key, bytes.NewReader(r.data)); perr != nil {
			_ = rc.Close()
			return nil, perr
		}
	}
	return rc, err
}

// TestSyncObject_FingerprintDescribesCopiedBytes: a source replaced between
// the open and the metadata read must not stamp the newer fingerprint on the
// older bytes, or the next sync never copies the newer version (#222).
func TestSyncObject_FingerprintDescribesCopiedBytes(t *testing.T) {
	ctx := context.Background()
	s, source := newFingerprintSyncer(t)
	require.NoError(t, source.PutWithContext(ctx, "k", bytes.NewReader([]byte("old"))))
	s.source = &replaceOnOpen{Storage: source, data: []byte("newer-and-longer")}

	_, err := s.SyncObject(ctx, "k")
	require.NoError(t, err)
	require.Equal(t, "old", readDest(t, s, "k"), "the copy read before the replace")

	r, err := s.SyncAll(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, r.Synced, "the newer source is seen as a change")
	require.Equal(t, "newer-and-longer", readDest(t, s, "k"))
}

// TestSync_FullUserMetadataReplicates: the fingerprint entries do not push
// a source with the maximum number of user entries over the limit (#222).
func TestSync_FullUserMetadataReplicates(t *testing.T) {
	ctx := context.Background()
	s, source := newFingerprintSyncer(t)
	custom := make(map[string]string, common.MaxMetadataEntries)
	for i := 0; i < common.MaxMetadataEntries; i++ {
		custom[fmt.Sprintf("k%03d", i)] = "v"
	}
	require.NoError(t, source.PutWithMetadata(ctx, "k", bytes.NewReader([]byte("data")),
		&common.Metadata{Custom: custom}))

	r, err := s.SyncAll(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, r.Synced)
	require.Zero(t, r.Failed)
	md, err := s.dest.GetMetadata(ctx, "k")
	require.NoError(t, err)
	for k := range custom {
		require.Contains(t, md.Custom, k, "user entry kept")
	}
	require.NotEmpty(t, md.Custom[MetaReplicationSourceETag])

	r, err = s.SyncAll(ctx)
	require.NoError(t, err)
	require.Zero(t, r.Synced, "unchanged")
}

// A destination rewritten after the copy, keeping the custom metadata
// (and with it the source fingerprint), is not the copy any more: the
// next sync replaces it.
func TestSync_RewrittenDestinationKeepingRecordReplicated(t *testing.T) {
	ctx := context.Background()
	s, source := newFingerprintSyncer(t)
	require.NoError(t, source.PutWithMetadata(ctx, "k", bytes.NewReader([]byte("original")),
		&common.Metadata{Custom: map[string]string{"owner": "alice"}}))
	r, err := s.SyncAll(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, r.Synced)

	md, err := s.dest.GetMetadata(ctx, "k")
	require.NoError(t, err)
	require.NoError(t, s.dest.PutWithMetadata(ctx, "k", bytes.NewReader([]byte("rewritten at the destination")), md))

	r, err = s.SyncAll(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, r.Synced, "rewritten destination replaced")
	require.Equal(t, "original", readDest(t, s, "k"))

	r, err = s.SyncAll(ctx)
	require.NoError(t, err)
	require.Zero(t, r.Synced, "settled after the repair")
}

// growingSizeStorage stores more bytes than it is given, as a destination
// with client-side encryption does.
type growingSizeStorage struct {
	common.Storage
}

func (g growingSizeStorage) GetMetadata(ctx context.Context, key string) (*common.Metadata, error) {
	md, err := g.Storage.GetMetadata(ctx, key)
	if err != nil || md == nil {
		return md, err
	}
	out := *md
	out.Size += 16
	return &out, nil
}

// PutWithMetadataResult reports the grown size the store keeps.
func (g growingSizeStorage) PutWithMetadataResult(ctx context.Context, key string, data io.Reader, md *common.Metadata) (*common.Metadata, error) {
	stored, err := g.Storage.(common.IdentityPutter).PutWithMetadataResult(ctx, key, data, md)
	if err != nil || stored == nil {
		return stored, err
	}
	out := *stored
	out.Size += 16
	return &out, nil
}

// The record binds the size the destination reports after the write, not
// the source size, so a destination that stores more bytes settles.
func TestSync_DestinationStoringMoreBytesSettles(t *testing.T) {
	ctx := context.Background()
	s, source := newFingerprintSyncer(t)
	s.dest = growingSizeStorage{Storage: s.dest}
	require.NoError(t, source.PutWithContext(ctx, "k", bytes.NewReader([]byte("payload"))))
	r, err := s.SyncAll(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, r.Synced)
	r, err = s.SyncAll(ctx)
	require.NoError(t, err)
	require.Zero(t, r.Synced, "second sync with no change")
}
