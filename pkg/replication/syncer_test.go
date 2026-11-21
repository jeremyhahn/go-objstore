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
	"errors"
	"io"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/memory"
)

func TestSyncObject_Success(t *testing.T) {
	source := newExtendedMockStorage()
	dest := newExtendedMockStorage()

	// Add test data to source
	testData := []byte("test content")
	source.data["test.txt"] = testData
	source.objects["test.txt"] = &common.Metadata{
		Size:         int64(len(testData)),
		ETag:         "etag123",
		LastModified: time.Now(),
	}

	syncer := &Syncer{
		policy: common.ReplicationPolicy{
			ID:              "test-policy",
			ReplicationMode: common.ReplicationModeTransparent,
		},
		source:   source,
		dest:     dest,
		logger:   &mockLogger{},
		metrics:  NewReplicationMetrics(),
		auditLog: &mockAuditLogger{},
	}

	size, err := syncer.SyncObject(context.Background(), "test.txt")

	if err != nil {
		t.Fatalf("SyncObject failed: %v", err)
	}
	if size != int64(len(testData)) {
		t.Errorf("expected size %d, got %d", len(testData), size)
	}
	if !dest.putCalled {
		t.Error("dest.PutWithMetadata was not called")
	}

	// Verify data was synced
	syncedData := dest.data["test.txt"]
	if !bytes.Equal(syncedData, testData) {
		t.Error("synced data does not match source data")
	}
}

func TestSyncObject_SourceError(t *testing.T) {
	source := newExtendedMockStorage()
	dest := newExtendedMockStorage()

	source.getError = errors.New("source read failed")
	// The metadata read comes first and succeeds; the data read fails.
	source.objects["test.txt"] = &common.Metadata{Size: 1}

	syncer := &Syncer{
		policy: common.ReplicationPolicy{
			ID: "test-policy",
		},
		source:   source,
		dest:     dest,
		logger:   &mockLogger{},
		metrics:  NewReplicationMetrics(),
		auditLog: &mockAuditLogger{},
	}

	_, err := syncer.SyncObject(context.Background(), "test.txt")

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "failed to read source") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestSyncObject_DestinationError(t *testing.T) {
	source := newExtendedMockStorage()
	dest := newExtendedMockStorage()

	// Add test data to source
	testData := []byte("test content")
	source.data["test.txt"] = testData
	source.objects["test.txt"] = &common.Metadata{
		Size: int64(len(testData)),
	}

	dest.putError = errors.New("dest write failed")

	syncer := &Syncer{
		policy: common.ReplicationPolicy{
			ID: "test-policy",
		},
		source:   source,
		dest:     dest,
		logger:   &mockLogger{},
		metrics:  NewReplicationMetrics(),
		auditLog: &mockAuditLogger{},
	}

	_, err := syncer.SyncObject(context.Background(), "test.txt")

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "failed to write destination") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestSyncObject_MetadataError(t *testing.T) {
	source := newExtendedMockStorage()
	dest := newExtendedMockStorage()

	// Add test data to source but no metadata
	testData := []byte("test content")
	source.data["test.txt"] = testData
	source.getMetaError = errors.New("metadata not found")

	syncer := &Syncer{
		policy: common.ReplicationPolicy{
			ID: "test-policy",
		},
		source:   source,
		dest:     dest,
		logger:   &mockLogger{},
		metrics:  NewReplicationMetrics(),
		auditLog: &mockAuditLogger{},
	}

	_, err := syncer.SyncObject(context.Background(), "test.txt")

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "failed to get metadata") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestSyncAll_Success(t *testing.T) {
	source := newExtendedMockStorage()
	dest := newExtendedMockStorage()

	// Add test data to source
	testData1 := []byte("content1")
	testData2 := []byte("content2")

	source.data["file1.txt"] = testData1
	source.data["file2.txt"] = testData2
	source.objects["file1.txt"] = &common.Metadata{Size: int64(len(testData1)), ETag: "etag1"}
	source.objects["file2.txt"] = &common.Metadata{Size: int64(len(testData2)), ETag: "etag2"}

	syncer := &Syncer{
		policy: common.ReplicationPolicy{
			ID:              "test-policy",
			ReplicationMode: common.ReplicationModeTransparent,
		},
		source:   source,
		dest:     dest,
		logger:   &mockLogger{},
		metrics:  NewReplicationMetrics(),
		auditLog: &mockAuditLogger{},
	}

	result, err := syncer.SyncAll(context.Background())

	if err != nil {
		t.Fatalf("SyncAll failed: %v", err)
	}
	if result.PolicyID != "test-policy" {
		t.Errorf("expected policy ID 'test-policy', got '%s'", result.PolicyID)
	}
	if result.Synced != 2 {
		t.Errorf("expected 2 synced objects, got %d", result.Synced)
	}
	if result.Failed != 0 {
		t.Errorf("expected 0 failed objects, got %d", result.Failed)
	}
	if result.BytesTotal != int64(len(testData1)+len(testData2)) {
		t.Errorf("expected %d bytes, got %d", len(testData1)+len(testData2), result.BytesTotal)
	}
}

func TestSyncAll_PartialFailure(t *testing.T) {
	source := newExtendedMockStorage()
	dest := newExtendedMockStorage()

	// Add test data to source
	testData1 := []byte("content1")
	testData2 := []byte("content2")

	source.data["file1.txt"] = testData1
	source.data["file2.txt"] = testData2
	source.objects["file1.txt"] = &common.Metadata{Size: int64(len(testData1)), ETag: "etag1"}
	source.objects["file2.txt"] = &common.Metadata{Size: int64(len(testData2)), ETag: "etag2"}

	// Make destination fail after first put
	callCount := 0
	dest.putWithMetaFn = func(ctx context.Context, key string, data io.Reader, metadata *common.Metadata) error {
		callCount++
		if callCount > 1 {
			return errors.New("dest full")
		}
		return dest.defaultPutWithMetadata(ctx, key, data, metadata)
	}

	syncer := &Syncer{
		policy: common.ReplicationPolicy{
			ID: "test-policy",
		},
		source:   source,
		dest:     dest,
		logger:   &mockLogger{},
		metrics:  NewReplicationMetrics(),
		auditLog: &mockAuditLogger{},
	}

	result, err := syncer.SyncAll(context.Background())

	if err != nil {
		t.Fatalf("SyncAll should not return error: %v", err)
	}
	if result.Synced != 1 {
		t.Errorf("expected 1 synced object, got %d", result.Synced)
	}
	if result.Failed != 1 {
		t.Errorf("expected 1 failed object, got %d", result.Failed)
	}
	if len(result.Errors) != 1 {
		t.Errorf("expected 1 error message, got %d", len(result.Errors))
	}
}

func TestSyncAll_DetectionError(t *testing.T) {
	source := newExtendedMockStorage()
	dest := newExtendedMockStorage()

	source.listError = errors.New("list failed")

	syncer := &Syncer{
		policy: common.ReplicationPolicy{
			ID: "test-policy",
		},
		source:   source,
		dest:     dest,
		logger:   &mockLogger{},
		metrics:  NewReplicationMetrics(),
		auditLog: &mockAuditLogger{},
	}

	_, err := syncer.SyncAll(context.Background())

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "change detection failed") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestSyncAll_WithPrefix(t *testing.T) {
	source := newExtendedMockStorage()
	dest := newExtendedMockStorage()

	// Add test data with different prefixes
	testData1 := []byte("content1")
	testData2 := []byte("content2")

	source.data["photos/cat.jpg"] = testData1
	source.data["docs/report.pdf"] = testData2
	source.objects["photos/cat.jpg"] = &common.Metadata{Size: int64(len(testData1)), ETag: "etag1"}
	source.objects["docs/report.pdf"] = &common.Metadata{Size: int64(len(testData2)), ETag: "etag2"}

	syncer := &Syncer{
		policy: common.ReplicationPolicy{
			ID:           "test-policy",
			SourcePrefix: "photos/",
		},
		source:   source,
		dest:     dest,
		logger:   &mockLogger{},
		metrics:  NewReplicationMetrics(),
		auditLog: &mockAuditLogger{},
	}

	result, err := syncer.SyncAll(context.Background())

	if err != nil {
		t.Fatalf("SyncAll failed: %v", err)
	}
	// Should only sync objects matching the prefix
	if result.Synced != 1 {
		t.Errorf("expected 1 synced object (with prefix), got %d", result.Synced)
	}
}

func TestSyncAll_EmptySource(t *testing.T) {
	source := newExtendedMockStorage()
	dest := newExtendedMockStorage()

	syncer := &Syncer{
		policy: common.ReplicationPolicy{
			ID: "test-policy",
		},
		source:   source,
		dest:     dest,
		logger:   &mockLogger{},
		metrics:  NewReplicationMetrics(),
		auditLog: &mockAuditLogger{},
	}

	result, err := syncer.SyncAll(context.Background())

	if err != nil {
		t.Fatalf("SyncAll failed: %v", err)
	}
	if result.Synced != 0 {
		t.Errorf("expected 0 synced objects, got %d", result.Synced)
	}
	if result.Failed != 0 {
		t.Errorf("expected 0 failed objects, got %d", result.Failed)
	}
}

func TestSyncAll_TimingMetrics(t *testing.T) {
	source := newExtendedMockStorage()
	dest := newExtendedMockStorage()

	syncer := &Syncer{
		policy: common.ReplicationPolicy{
			ID: "test-policy",
		},
		source:   source,
		dest:     dest,
		logger:   &mockLogger{},
		metrics:  NewReplicationMetrics(),
		auditLog: &mockAuditLogger{},
	}

	result, err := syncer.SyncAll(context.Background())

	if err != nil {
		t.Fatalf("SyncAll failed: %v", err)
	}
	if result.Duration == 0 {
		t.Error("expected non-zero duration")
	}
}

// retaggingStorage behaves like Azure: a metadata-only write gives the
// object a new ETag, so a copy whose ETag is recorded by UpdateMetadata
// never matches that record.
type retaggingStorage struct {
	common.Storage
	mu     sync.Mutex
	writes map[string]int // metadata writes since the last upload, per key
	puts   int
}

func newRetaggingStorage() *retaggingStorage {
	return &retaggingStorage{Storage: memory.New(), writes: map[string]int{}}
}

func (r *retaggingStorage) MetadataWriteChangesETag() bool { return true }

func (r *retaggingStorage) PutWithMetadata(ctx context.Context, key string, data io.Reader, md *common.Metadata) error {
	_, err := r.PutWithMetadataResult(ctx, key, data, md)
	return err
}

func (r *retaggingStorage) PutWithMetadataResult(ctx context.Context, key string, data io.Reader, md *common.Metadata) (*common.Metadata, error) {
	if err := r.Storage.PutWithMetadata(ctx, key, data, md); err != nil {
		return nil, err
	}
	r.mu.Lock()
	r.puts++
	r.writes[key] = 0
	r.mu.Unlock()
	return r.GetMetadata(ctx, key)
}

func (r *retaggingStorage) UpdateMetadata(ctx context.Context, key string, md *common.Metadata) error {
	if err := r.Storage.UpdateMetadata(ctx, key, md); err != nil {
		return err
	}
	r.mu.Lock()
	r.writes[key]++
	r.mu.Unlock()
	return nil
}

func (r *retaggingStorage) GetMetadata(ctx context.Context, key string) (*common.Metadata, error) {
	md, err := r.Storage.GetMetadata(ctx, key)
	if err != nil || md == nil {
		return md, err
	}
	r.mu.Lock()
	n := r.writes[key]
	r.mu.Unlock()
	out := *md
	out.ETag = md.ETag + "-r" + strconv.Itoa(n)
	return &out, nil
}

func (r *retaggingStorage) putCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.puts
}

// TestSyncAll_RetaggingDestinationNotCopiedAgain: on a destination whose
// metadata writes re-tag the copy (Azure), the copy is bound by its upload,
// so the next sync of an unchanged source copies nothing.
func TestSyncAll_RetaggingDestinationNotCopiedAgain(t *testing.T) {
	for _, tc := range []struct {
		name       string
		destRetags bool
		wantPuts   int
	}{
		{"bound by the upload", true, 1},
		// Control: recording the upload ETag by a metadata write re-tags
		// the copy, so it never matches and every sync copies again.
		{"bound by a metadata write", false, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			source := memory.New()
			if err := source.PutWithMetadata(ctx, "file1.txt", bytes.NewBufferString("content1"),
				&common.Metadata{ContentType: "text/plain"}); err != nil {
				t.Fatal(err)
			}
			dest := newRetaggingStorage()
			if got := metadataWriteRetags(dest); !got {
				t.Fatal("metadataWriteRetags(dest) = false, want true")
			}
			s := &Syncer{
				policy:     common.ReplicationPolicy{ID: "p", ReplicationMode: common.ReplicationModeTransparent},
				source:     source,
				dest:       dest,
				destRetags: tc.destRetags,
				logger:     &mockLogger{},
				metrics:    NewReplicationMetrics(),
				auditLog:   &mockAuditLogger{},
			}

			first, err := s.SyncAll(ctx)
			if err != nil || first.Synced != 1 {
				t.Fatalf("first SyncAll: synced %v, err %v", first, err)
			}
			second, err := s.SyncAll(ctx)
			if err != nil {
				t.Fatalf("second SyncAll: %v", err)
			}
			if got := dest.putCount(); got != tc.wantPuts {
				t.Fatalf("destination uploads = %d, want %d (second sync synced %d)",
					got, tc.wantPuts, second.Synced)
			}
		})
	}
}
