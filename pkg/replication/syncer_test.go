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
	"strings"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
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
