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
	"io"
	"strings"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/audit"
	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// TestSyncIncrementalWithChangelog tests incremental sync with a changelog
func TestSyncIncrementalWithChangelog(t *testing.T) {
	ctx := context.Background()

	source := newExtendedMockStorage()
	dest := newExtendedMockStorage()

	// Add test data
	testKey := "test-key"
	testData := []byte("test-data")
	source.data[testKey] = testData
	source.objects[testKey] = &common.Metadata{
		Size:         int64(len(testData)),
		LastModified: time.Now(),
	}

	policy := common.ReplicationPolicy{
		ID:              "incremental-test",
		SourceBackend:   "local",
		DestinationBackend: "local",
		ReplicationMode: common.ReplicationModeOpaque,
	}

	syncer := &Syncer{
		policy:   policy,
		source:   source,
		dest:     dest,
		logger:   adapters.NewNoOpLogger(),
		auditLog: audit.NewNoOpAuditLogger(),
		metrics:  NewReplicationMetrics(),
	}

	// Create a mock changelog
	changelog := newMockChangeLog()
	changelog.events = []ChangeEvent{
		{
			Key:       testKey,
			Operation: "put",
			Timestamp: time.Now(),
			Processed: nil,
		},
	}

	// Run incremental sync
	result, err := syncer.SyncIncremental(ctx, changelog)
	if err != nil {
		t.Fatalf("SyncIncremental failed: %v", err)
	}

	if result.Synced != 1 {
		t.Errorf("Expected 1 object synced, got %d", result.Synced)
	}
}

// TestSyncIncrementalWithDelete tests incremental sync with delete operations
func TestSyncIncrementalWithDelete(t *testing.T) {
	ctx := context.Background()

	source := newExtendedMockStorage()
	dest := newExtendedMockStorage()

	// Put an object in destination that will be deleted
	testKey := "to-delete"
	dest.data[testKey] = []byte("old-data")
	dest.objects[testKey] = &common.Metadata{
		Size:         8,
		LastModified: time.Now(),
	}

	policy := common.ReplicationPolicy{
		ID:              "delete-test",
		SourceBackend:   "local",
		DestinationBackend: "local",
		ReplicationMode: common.ReplicationModeOpaque,
	}

	syncer := &Syncer{
		policy:   policy,
		source:   source,
		dest:     dest,
		logger:   adapters.NewNoOpLogger(),
		auditLog: audit.NewNoOpAuditLogger(),
		metrics:  NewReplicationMetrics(),
	}

	// Create changelog with delete operation
	changelog := newMockChangeLog()
	changelog.events = []ChangeEvent{
		{
			Key:       testKey,
			Operation: "delete",
			Timestamp: time.Now(),
			Processed: nil,
		},
	}

	// Run incremental sync
	result, err := syncer.SyncIncremental(ctx, changelog)
	if err != nil {
		t.Fatalf("SyncIncremental failed: %v", err)
	}

	if result.Deleted != 1 {
		t.Errorf("Expected 1 object deleted, got %d", result.Deleted)
	}

	// Verify object was deleted from dest
	if _, exists := dest.data[testKey]; exists {
		t.Error("Expected object to be deleted from destination")
	}
}

// TestSyncIncrementalWithUnknownOperation tests handling of unknown operations
func TestSyncIncrementalWithUnknownOperation(t *testing.T) {
	ctx := context.Background()

	source := newExtendedMockStorage()
	dest := newExtendedMockStorage()

	policy := common.ReplicationPolicy{
		ID:              "unknown-op-test",
		SourceBackend:   "local",
		DestinationBackend: "local",
		ReplicationMode: common.ReplicationModeOpaque,
	}

	syncer := &Syncer{
		policy:   policy,
		source:   source,
		dest:     dest,
		logger:   adapters.NewNoOpLogger(),
		auditLog: audit.NewNoOpAuditLogger(),
		metrics:  NewReplicationMetrics(),
	}

	// Create changelog with unknown operation
	changelog := newMockChangeLog()
	changelog.events = []ChangeEvent{
		{
			Key:       "some-key",
			Operation: "unknown-operation",
			Timestamp: time.Now(),
			Processed: nil,
		},
	}

	// Should handle gracefully
	result, err := syncer.SyncIncremental(ctx, changelog)
	if err != nil {
		t.Fatalf("SyncIncremental failed: %v", err)
	}

	// Should not sync or delete anything
	if result.Synced != 0 {
		t.Errorf("Expected 0 synced, got %d", result.Synced)
	}

	if result.Deleted != 0 {
		t.Errorf("Expected 0 deleted, got %d", result.Deleted)
	}
}

// TestSyncObjectMetadataError tests sync when metadata retrieval fails
func TestSyncObjectMetadataError(t *testing.T) {
	ctx := context.Background()

	source := newExtendedMockStorage()
	dest := newExtendedMockStorage()

	testKey := "test-key"
	source.data[testKey] = []byte("test-data")
	// Don't add metadata - will cause GetMetadata to fail

	policy := common.ReplicationPolicy{
		ID:              "metadata-error-test",
		SourceBackend:   "local",
		DestinationBackend: "local",
		ReplicationMode: common.ReplicationModeOpaque,
	}

	syncer := &Syncer{
		policy:   policy,
		source:   source,
		dest:     dest,
		logger:   adapters.NewNoOpLogger(),
		auditLog: audit.NewNoOpAuditLogger(),
		metrics:  NewReplicationMetrics(),
	}

	// Try to sync - should fail on metadata
	_, err := syncer.SyncObject(ctx, testKey)
	if err == nil {
		t.Error("Expected error when metadata is missing")
	}

	if !strings.Contains(err.Error(), "metadata") {
		t.Errorf("Expected metadata error, got: %v", err)
	}
}

// TestSyncObjectGetError tests sync when Get fails
func TestSyncObjectGetError(t *testing.T) {
	ctx := context.Background()

	source := newExtendedMockStorage()
	dest := newExtendedMockStorage()

	// Set source to error on Get
	source.getError = common.ErrInternal

	policy := common.ReplicationPolicy{
		ID:              "get-error-test",
		SourceBackend:   "local",
		DestinationBackend: "local",
		ReplicationMode: common.ReplicationModeOpaque,
	}

	syncer := &Syncer{
		policy:   policy,
		source:   source,
		dest:     dest,
		logger:   adapters.NewNoOpLogger(),
		auditLog: audit.NewNoOpAuditLogger(),
		metrics:  NewReplicationMetrics(),
	}

	// Try to sync - should fail on Get
	_, err := syncer.SyncObject(ctx, "any-key")
	if err == nil {
		t.Error("Expected error when Get fails")
	}
}

// TestSyncerClose tests the Close method
func TestSyncerClose(t *testing.T) {
	source := newExtendedMockStorage()
	dest := newExtendedMockStorage()

	policy := common.ReplicationPolicy{
		ID:              "close-test",
		SourceBackend:   "local",
		DestinationBackend: "local",
		ReplicationMode: common.ReplicationModeOpaque,
	}

	syncer := &Syncer{
		policy:   policy,
		source:   source,
		dest:     dest,
		logger:   adapters.NewNoOpLogger(),
		auditLog: audit.NewNoOpAuditLogger(),
		metrics:  NewReplicationMetrics(),
	}

	// Close should not error
	err := syncer.Close()
	if err != nil {
		t.Errorf("Close returned error: %v", err)
	}
}

// TestChangeDetectorWithErrors tests change detection with various error scenarios
func TestChangeDetectorWithErrors(t *testing.T) {
	ctx := context.Background()

	source := newExtendedMockStorage()
	dest := newExtendedMockStorage()

	// Set source to fail on list
	source.listError = common.ErrInternal

	detector := NewChangeDetector(source, dest)

	// Should fail on source list
	_, err := detector.DetectChanges(ctx, "")
	if err == nil {
		t.Error("Expected error when source list fails")
	}
}

// TestReadCloserWrapper tests reading from io.NopCloser
func TestReadCloserWrapper(t *testing.T) {
	data := []byte("test data")
	reader := io.NopCloser(bytes.NewReader(data))

	// Read all data
	buf := make([]byte, len(data))
	n, err := reader.Read(buf)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if n != len(data) {
		t.Errorf("Expected to read %d bytes, got %d", len(data), n)
	}

	if string(buf) != string(data) {
		t.Errorf("Expected data %q, got %q", string(data), string(buf))
	}

	// Close
	err = reader.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}
}

// TestSyncAllParallelSubmitError tests handling of submit errors
func TestSyncAllParallelSubmitError(t *testing.T) {
	ctx := context.Background()

	source := newExtendedMockStorage()
	dest := newExtendedMockStorage()

	// Add a large number of items to potentially overflow queue
	for i := 0; i < 1000; i++ {
		key := "key-" + string(rune('a'+i%26)) + string(rune('0'+i%10))
		source.data[key] = []byte("data")
		source.objects[key] = &common.Metadata{
			Size:         4,
			LastModified: time.Now(),
		}
	}

	policy := common.ReplicationPolicy{
		ID:              "submit-test",
		SourceBackend:   "local",
		DestinationBackend: "local",
		ReplicationMode: common.ReplicationModeOpaque,
	}

	syncer := &Syncer{
		policy:   policy,
		source:   source,
		dest:     dest,
		logger:   adapters.NewNoOpLogger(),
		auditLog: audit.NewNoOpAuditLogger(),
		metrics:  NewReplicationMetrics(),
	}

	// Run with limited workers - should still complete
	result, err := syncer.SyncAllParallel(ctx, 2)
	if err != nil {
		t.Fatalf("SyncAllParallel failed: %v", err)
	}

	// Should process all items
	totalProcessed := result.Synced + result.Failed
	if totalProcessed < 100 {
		t.Errorf("Expected to process many items, got %d", totalProcessed)
	}
}
