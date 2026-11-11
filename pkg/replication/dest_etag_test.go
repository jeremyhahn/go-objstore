// Copyright (c) 2025 Jeremy Hahn
// Copyright (c) 2025 Automate The Things, LLC
//
// This file is part of go-objstore.
//
// go-objstore is dual-licensed:
//
// 1. GNU Affero General Public License v3.0 (AGPL-3.0)
//    See LICENSE file or visit https://www.gnu.org/licenses/agpl-3.0.html
//
// 2. Commercial License
//    Contact licensing@automatethethings.com for commercial licensing options.

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

// newLocalSyncer replicates between two local backends.
func newLocalSyncer(t *testing.T) (*Syncer, common.Storage) {
	t.Helper()
	source, err := factory.NewStorage("local", map[string]string{"path": t.TempDir()}, noCreds)
	require.NoError(t, err)
	dest, err := factory.NewStorage("local", map[string]string{"path": t.TempDir()}, noCreds)
	require.NoError(t, err)
	return &Syncer{
		policy:   common.ReplicationPolicy{ID: "dest-etag"},
		source:   source,
		dest:     dest,
		logger:   &mockLogger{},
		metrics:  NewReplicationMetrics(),
		auditLog: &mockAuditLogger{},
	}, source
}

// A destination rewritten after the copy with content of the same size,
// keeping the custom metadata, has a new ETag: the record names the copy's
// ETag, so the next sync replaces it.
func TestSync_SameSizeRewrittenDestinationReplicated(t *testing.T) {
	ctx := context.Background()
	s, source := newLocalSyncer(t)
	require.NoError(t, source.PutWithContext(ctx, "k", bytes.NewReader([]byte("original"))))
	r, err := s.SyncAll(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, r.Synced)

	md, err := s.dest.GetMetadata(ctx, "k")
	require.NoError(t, err)
	require.NotEmpty(t, md.ETag)
	require.Equal(t, md.ETag, md.Custom[MetaReplicationDestETag], "record names the copy's ETag")

	// Local ETags are Unix seconds and size: rewrite in a later second.
	start := time.Now().Unix()
	for time.Now().Unix() == start {
		time.Sleep(10 * time.Millisecond)
	}
	require.NoError(t, s.dest.PutWithMetadata(ctx, "k", bytes.NewReader([]byte("ORIGINAL")), md))

	r, err = s.SyncAll(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, r.Synced, "same-size rewrite replaced")
	require.Equal(t, "original", readDest(t, s, "k"))

	r, err = s.SyncAll(ctx)
	require.NoError(t, err)
	require.Zero(t, r.Synced, "settled after the repair")
}

// etagPerWriteStorage gives an object a new ETag on every write, metadata
// writes included, as Azure and GCS do.
type etagPerWriteStorage struct {
	common.Storage
	writes atomic.Int64
}

func (e *etagPerWriteStorage) PutWithMetadata(ctx context.Context, key string, data io.Reader, md *common.Metadata) error {
	e.writes.Add(1)
	return e.Storage.PutWithMetadata(ctx, key, data, md)
}

func (e *etagPerWriteStorage) UpdateMetadata(ctx context.Context, key string, md *common.Metadata) error {
	e.writes.Add(1)
	return e.Storage.UpdateMetadata(ctx, key, md)
}

func (e *etagPerWriteStorage) GetMetadata(ctx context.Context, key string) (*common.Metadata, error) {
	md, err := e.Storage.GetMetadata(ctx, key)
	if err != nil || md == nil {
		return md, err
	}
	md.ETag = fmt.Sprintf("w%d", e.writes.Load())
	return md, nil
}

// Where a metadata write changes the ETag, the record binds none (it could
// never match), and an unchanged object is not copied again.
func TestSync_DestinationWithoutIdentityIsRecopied(t *testing.T) {
	ctx := context.Background()
	s, source := newLocalSyncer(t)
	// The wrapper hides the local backend's PutWithMetadataResult: the
	// upload reports no identity, so nothing can be bound.
	s.dest = &etagPerWriteStorage{Storage: s.dest}
	require.NoError(t, source.PutWithContext(ctx, "a", bytes.NewReader([]byte("one"))))
	require.NoError(t, source.PutWithContext(ctx, "b", bytes.NewReader([]byte("two"))))

	r, err := s.SyncAll(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, r.Synced)
	for _, k := range []string{"a", "b"} {
		md, err := s.dest.GetMetadata(ctx, k)
		require.NoError(t, err)
		require.NotContains(t, md.Custom, MetaReplicationDestETag, k)
	}

	r, err = s.SyncAll(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, r.Synced, "an unbound copy is never skipped")
}

// rewriteAfterPutStorage rewrites the object it has just stored, between the
// upload and the syncer's record, as a concurrent writer could.
type rewriteAfterPutStorage struct {
	common.Storage
	ip common.IdentityPutter
}

func (r *rewriteAfterPutStorage) PutWithMetadataResult(ctx context.Context, key string, data io.Reader, md *common.Metadata) (*common.Metadata, error) {
	stored, err := r.ip.PutWithMetadataResult(ctx, key, data, md)
	if err != nil {
		return nil, err
	}
	time.Sleep(1100 * time.Millisecond) // a later second: a new local ETag
	if err := r.Storage.PutWithMetadata(ctx, key, bytes.NewReader([]byte("XXX")), &common.Metadata{}); err != nil {
		return nil, err
	}
	return stored, nil
}

func TestSync_DestinationRewrittenBeforeBindingIsRecopied(t *testing.T) {
	ctx := context.Background()
	s, source := newLocalSyncer(t)
	ip, ok := s.dest.(common.IdentityPutter)
	require.True(t, ok, "local reports the upload's identity")
	wrapper := &rewriteAfterPutStorage{Storage: s.dest, ip: ip}
	s.dest = wrapper
	require.NoError(t, source.PutWithContext(ctx, "a", bytes.NewReader([]byte("one"))))

	r, err := s.SyncAll(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, r.Synced)

	// The record names the copy's ETag, not the rewrite's: the next scan
	// sees the change and copies again.
	s.dest = wrapper.Storage
	r, err = s.SyncAll(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, r.Synced, "the rewrite is recopied")
	got, err := s.dest.GetWithContext(ctx, "a")
	require.NoError(t, err)
	b, err := io.ReadAll(got)
	require.NoError(t, err)
	require.NoError(t, got.Close())
	require.Equal(t, "one", string(b))

	r, err = s.SyncAll(ctx)
	require.NoError(t, err)
	require.Zero(t, r.Synced, "then it settles")
}

// An ETag recorded for the copy decides only when present.
func TestDestETagMatches(t *testing.T) {
	require.False(t, destETagMatches(&common.Metadata{ETag: "x"}), "no record decides changed")
	require.False(t, destETagMatches(&common.Metadata{ETag: "",
		Custom: map[string]string{MetaReplicationDestETag: ""}}))
	require.True(t, destETagMatches(&common.Metadata{ETag: "x",
		Custom: map[string]string{MetaReplicationDestETag: "x"}}))
	require.False(t, destETagMatches(&common.Metadata{ETag: "y",
		Custom: map[string]string{MetaReplicationDestETag: "x"}}))
}
