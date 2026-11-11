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
	md.ETag = fmt.Sprintf("foreign-%d", f.n.Add(1))
	return md, nil
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
