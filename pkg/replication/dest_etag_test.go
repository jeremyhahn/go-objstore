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
func TestSync_DestinationChangingETagOnMetadataWriteSettles(t *testing.T) {
	ctx := context.Background()
	s, source := newLocalSyncer(t)
	s.dest = &etagPerWriteStorage{Storage: s.dest}
	require.NoError(t, source.PutWithContext(ctx, "a", bytes.NewReader([]byte("one"))))
	require.NoError(t, source.PutWithContext(ctx, "b", bytes.NewReader([]byte("two"))))

	r, err := s.SyncAll(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, r.Synced)
	require.True(t, s.destETagUnstable.Load())
	for _, k := range []string{"a", "b"} {
		md, err := s.dest.GetMetadata(ctx, k)
		require.NoError(t, err)
		require.NotContains(t, md.Custom, MetaReplicationDestETag, k)
	}

	r, err = s.SyncAll(ctx)
	require.NoError(t, err)
	require.Zero(t, r.Synced, "second sync with no change")
}

// An ETag recorded for the copy decides only when present.
func TestDestETagMatches(t *testing.T) {
	require.True(t, destETagMatches(&common.Metadata{ETag: "x"}))
	require.True(t, destETagMatches(&common.Metadata{ETag: "x",
		Custom: map[string]string{MetaReplicationDestETag: "x"}}))
	require.False(t, destETagMatches(&common.Metadata{ETag: "y",
		Custom: map[string]string{MetaReplicationDestETag: "x"}}))
}
