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
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockChangeLog implements ChangeLog for testing incremental sync
type mockChangeLog struct {
	events    []ChangeEvent
	processed map[string]map[string]bool // key -> policyID -> processed
}

func newMockChangeLog() *mockChangeLog {
	return &mockChangeLog{
		events:    make([]ChangeEvent, 0),
		processed: make(map[string]map[string]bool),
	}
}

func (m *mockChangeLog) RecordChange(event ChangeEvent) error {
	m.events = append(m.events, event)
	return nil
}

func (m *mockChangeLog) GetUnprocessed(policyID string) ([]ChangeEvent, error) {
	var unprocessed []ChangeEvent
	for _, event := range m.events {
		if m.processed[event.Key] == nil || !m.processed[event.Key][policyID] {
			unprocessed = append(unprocessed, event)
		}
	}
	return unprocessed, nil
}

func (m *mockChangeLog) MarkProcessed(key, policyID string) error {
	if m.processed[key] == nil {
		m.processed[key] = make(map[string]bool)
	}
	m.processed[key][policyID] = true
	return nil
}

func (m *mockChangeLog) Rotate() error {
	return nil
}

func (m *mockChangeLog) Close() error {
	return nil
}

func TestSyncIncremental_Success(t *testing.T) {
	source := newExtendedMockStorage()
	dest := newExtendedMockStorage()

	// Add objects to source
	testData1 := []byte("test data 1")
	testData2 := []byte("test data 2")
	source.data["file1.txt"] = testData1
	source.data["file2.txt"] = testData2
	source.objects["file1.txt"] = &common.Metadata{Size: int64(len(testData1)), ETag: "etag1"}
	source.objects["file2.txt"] = &common.Metadata{Size: int64(len(testData2)), ETag: "etag2"}

	// Create change log with put events
	changeLog := newMockChangeLog()
	changeLog.RecordChange(ChangeEvent{
		Key:       "file1.txt",
		Operation: "put",
		Timestamp: time.Now(),
		ETag:      "etag1",
		Size:      int64(len(testData1)),
	})
	changeLog.RecordChange(ChangeEvent{
		Key:       "file2.txt",
		Operation: "put",
		Timestamp: time.Now(),
		ETag:      "etag2",
		Size:      int64(len(testData2)),
	})

	// Create syncer
	policy := common.ReplicationPolicy{
		ID:                  "test-policy",
		SourceBackend:       "mock",
		DestinationBackend:  "mock",
		ReplicationMode:     common.ReplicationModeTransparent,
	}

	syncer := &Syncer{
		policy:   policy,
		source:   source,
		dest:     dest,
		logger:   &mockLogger{},
		auditLog: &mockAuditLogger{},
		metrics:  NewReplicationMetrics(),
	}

	// Run incremental sync
	result, err := syncer.SyncIncremental(context.Background(), changeLog)
	require.NoError(t, err)
	assert.NotNil(t, result)

	// Verify results
	assert.Equal(t, 2, result.Synced)
	assert.Equal(t, 0, result.Failed)
	assert.Equal(t, 0, result.Deleted)

	// Verify data was synced
	assert.Contains(t, dest.data, "file1.txt")
	assert.Contains(t, dest.data, "file2.txt")
	assert.Equal(t, testData1, dest.data["file1.txt"])
	assert.Equal(t, testData2, dest.data["file2.txt"])

	// Verify events were marked as processed
	unprocessed, err := changeLog.GetUnprocessed("test-policy")
	require.NoError(t, err)
	assert.Empty(t, unprocessed)
}

func TestSyncIncremental_DeleteEvents(t *testing.T) {
	source := newExtendedMockStorage()
	dest := newExtendedMockStorage()

	// Add object to destination (to be deleted)
	testData := []byte("test data")
	dest.data["file1.txt"] = testData
	dest.objects["file1.txt"] = &common.Metadata{Size: int64(len(testData)), ETag: "etag1"}

	// Create change log with delete event
	changeLog := newMockChangeLog()
	changeLog.RecordChange(ChangeEvent{
		Key:       "file1.txt",
		Operation: "delete",
		Timestamp: time.Now(),
	})

	// Create syncer
	policy := common.ReplicationPolicy{
		ID:                  "test-policy",
		SourceBackend:       "mock",
		DestinationBackend:  "mock",
		ReplicationMode:     common.ReplicationModeTransparent,
	}

	syncer := &Syncer{
		policy:   policy,
		source:   source,
		dest:     dest,
		logger:   &mockLogger{},
		auditLog: &mockAuditLogger{},
		metrics:  NewReplicationMetrics(),
	}

	// Run incremental sync
	result, err := syncer.SyncIncremental(context.Background(), changeLog)
	require.NoError(t, err)
	assert.NotNil(t, result)

	// Verify results
	assert.Equal(t, 0, result.Synced)
	assert.Equal(t, 1, result.Deleted)
	assert.Equal(t, 0, result.Failed)

	// Verify object was deleted (extendedMockStorage handles delete internally)
	assert.NotContains(t, dest.data, "file1.txt")

	// Verify event was marked as processed
	unprocessed, err := changeLog.GetUnprocessed("test-policy")
	require.NoError(t, err)
	assert.Empty(t, unprocessed)
}

func TestSyncIncremental_MixedEvents(t *testing.T) {
	source := newExtendedMockStorage()
	dest := newExtendedMockStorage()

	// Setup source data
	testData1 := []byte("test data 1")
	testData2 := []byte("test data 2")
	source.data["file1.txt"] = testData1
	source.data["file2.txt"] = testData2
	source.objects["file1.txt"] = &common.Metadata{Size: int64(len(testData1)), ETag: "etag1"}
	source.objects["file2.txt"] = &common.Metadata{Size: int64(len(testData2)), ETag: "etag2"}

	// Setup destination with file to delete
	testData3 := []byte("old data")
	dest.data["file3.txt"] = testData3
	dest.objects["file3.txt"] = &common.Metadata{Size: int64(len(testData3)), ETag: "etag3"}

	// Create change log with mixed events
	changeLog := newMockChangeLog()
	changeLog.RecordChange(ChangeEvent{
		Key:       "file1.txt",
		Operation: "put",
		Timestamp: time.Now(),
		ETag:      "etag1",
		Size:      int64(len(testData1)),
	})
	changeLog.RecordChange(ChangeEvent{
		Key:       "file2.txt",
		Operation: "put",
		Timestamp: time.Now(),
		ETag:      "etag2",
		Size:      int64(len(testData2)),
	})
	changeLog.RecordChange(ChangeEvent{
		Key:       "file3.txt",
		Operation: "delete",
		Timestamp: time.Now(),
	})

	// Create syncer
	policy := common.ReplicationPolicy{
		ID:                  "test-policy",
		SourceBackend:       "mock",
		DestinationBackend:  "mock",
		ReplicationMode:     common.ReplicationModeTransparent,
	}

	syncer := &Syncer{
		policy:   policy,
		source:   source,
		dest:     dest,
		logger:   &mockLogger{},
		auditLog: &mockAuditLogger{},
		metrics:  NewReplicationMetrics(),
	}

	// Run incremental sync
	result, err := syncer.SyncIncremental(context.Background(), changeLog)
	require.NoError(t, err)
	assert.NotNil(t, result)

	// Verify results
	assert.Equal(t, 2, result.Synced)
	assert.Equal(t, 1, result.Deleted)
	assert.Equal(t, 0, result.Failed)

	// Verify puts worked
	assert.Contains(t, dest.data, "file1.txt")
	assert.Contains(t, dest.data, "file2.txt")

	// Verify delete worked
	assert.NotContains(t, dest.data, "file3.txt")
}

func TestSyncIncremental_PartialFailure(t *testing.T) {
	source := newExtendedMockStorage()
	dest := newExtendedMockStorage()

	// Add one object to source
	testData1 := []byte("test data 1")
	source.data["file1.txt"] = testData1
	source.objects["file1.txt"] = &common.Metadata{Size: int64(len(testData1)), ETag: "etag1"}

	// file2.txt doesn't exist in source - will cause error

	// Create change log
	changeLog := newMockChangeLog()
	changeLog.RecordChange(ChangeEvent{
		Key:       "file1.txt",
		Operation: "put",
		Timestamp: time.Now(),
		ETag:      "etag1",
		Size:      int64(len(testData1)),
	})
	changeLog.RecordChange(ChangeEvent{
		Key:       "file2.txt",
		Operation: "put",
		Timestamp: time.Now(),
		ETag:      "etag2",
		Size:      100,
	})

	// Create syncer
	policy := common.ReplicationPolicy{
		ID:                  "test-policy",
		SourceBackend:       "mock",
		DestinationBackend:  "mock",
		ReplicationMode:     common.ReplicationModeTransparent,
	}

	syncer := &Syncer{
		policy:   policy,
		source:   source,
		dest:     dest,
		logger:   &mockLogger{},
		auditLog: &mockAuditLogger{},
		metrics:  NewReplicationMetrics(),
	}

	// Run incremental sync
	result, err := syncer.SyncIncremental(context.Background(), changeLog)
	require.NoError(t, err)
	assert.NotNil(t, result)

	// Verify results - one success, one failure
	assert.Equal(t, 1, result.Synced)
	assert.Equal(t, 1, result.Failed)
	assert.Len(t, result.Errors, 1)

	// Verify successful file was synced
	assert.Contains(t, dest.data, "file1.txt")

	// Verify only successful event was marked as processed
	unprocessed, err := changeLog.GetUnprocessed("test-policy")
	require.NoError(t, err)
	assert.Len(t, unprocessed, 1)
	assert.Equal(t, "file2.txt", unprocessed[0].Key)
}

func TestSyncIncremental_EmptyChangeLog(t *testing.T) {
	source := newExtendedMockStorage()
	dest := newExtendedMockStorage()

	// Create empty change log
	changeLog := newMockChangeLog()

	// Create syncer
	policy := common.ReplicationPolicy{
		ID:                  "test-policy",
		SourceBackend:       "mock",
		DestinationBackend:  "mock",
		ReplicationMode:     common.ReplicationModeTransparent,
	}

	syncer := &Syncer{
		policy:   policy,
		source:   source,
		dest:     dest,
		logger:   &mockLogger{},
		auditLog: &mockAuditLogger{},
		metrics:  NewReplicationMetrics(),
	}

	// Run incremental sync
	result, err := syncer.SyncIncremental(context.Background(), changeLog)
	require.NoError(t, err)
	assert.NotNil(t, result)

	// Verify no operations
	assert.Equal(t, 0, result.Synced)
	assert.Equal(t, 0, result.Deleted)
	assert.Equal(t, 0, result.Failed)
}

func TestSyncIncremental_AlreadyProcessed(t *testing.T) {
	source := newExtendedMockStorage()
	dest := newExtendedMockStorage()

	testData := []byte("test data")
	source.data["file1.txt"] = testData
	source.objects["file1.txt"] = &common.Metadata{Size: int64(len(testData)), ETag: "etag1"}

	// Create change log with already processed event
	changeLog := newMockChangeLog()
	changeLog.RecordChange(ChangeEvent{
		Key:       "file1.txt",
		Operation: "put",
		Timestamp: time.Now(),
		ETag:      "etag1",
		Size:      int64(len(testData)),
	})

	// Mark as processed
	changeLog.MarkProcessed("file1.txt", "test-policy")

	// Create syncer
	policy := common.ReplicationPolicy{
		ID:                  "test-policy",
		SourceBackend:       "mock",
		DestinationBackend:  "mock",
		ReplicationMode:     common.ReplicationModeTransparent,
	}

	syncer := &Syncer{
		policy:   policy,
		source:   source,
		dest:     dest,
		logger:   &mockLogger{},
		auditLog: &mockAuditLogger{},
		metrics:  NewReplicationMetrics(),
	}

	// Run incremental sync
	result, err := syncer.SyncIncremental(context.Background(), changeLog)
	require.NoError(t, err)
	assert.NotNil(t, result)

	// Verify nothing was synced
	assert.Equal(t, 0, result.Synced)
	assert.Equal(t, 0, result.Deleted)
	assert.Equal(t, 0, result.Failed)
}

func TestSyncIncremental_MultiplePolicies(t *testing.T) {
	source := newExtendedMockStorage()
	dest1 := newExtendedMockStorage()
	dest2 := newExtendedMockStorage()

	testData := []byte("test data")
	source.data["file1.txt"] = testData
	source.objects["file1.txt"] = &common.Metadata{Size: int64(len(testData)), ETag: "etag1"}

	// Create change log with one event
	changeLog := newMockChangeLog()
	changeLog.RecordChange(ChangeEvent{
		Key:       "file1.txt",
		Operation: "put",
		Timestamp: time.Now(),
		ETag:      "etag1",
		Size:      int64(len(testData)),
	})

	// Create first syncer (policy1)
	policy1 := common.ReplicationPolicy{
		ID:                  "policy1",
		SourceBackend:       "mock",
		DestinationBackend:  "mock",
		ReplicationMode:     common.ReplicationModeTransparent,
	}

	syncer1 := &Syncer{
		policy:   policy1,
		source:   source,
		dest:     dest1,
		logger:   &mockLogger{},
		auditLog: &mockAuditLogger{},
		metrics:  NewReplicationMetrics(),
	}

	// First sync
	result1, err := syncer1.SyncIncremental(context.Background(), changeLog)
	require.NoError(t, err)
	assert.Equal(t, 1, result1.Synced)

	// Verify event is processed for policy1 but not policy2
	unprocessed1, err := changeLog.GetUnprocessed("policy1")
	require.NoError(t, err)
	assert.Empty(t, unprocessed1)

	unprocessed2, err := changeLog.GetUnprocessed("policy2")
	require.NoError(t, err)
	assert.Len(t, unprocessed2, 1)

	// Create second syncer (policy2)
	policy2 := common.ReplicationPolicy{
		ID:                  "policy2",
		SourceBackend:       "mock",
		DestinationBackend:  "mock",
		ReplicationMode:     common.ReplicationModeTransparent,
	}

	syncer2 := &Syncer{
		policy:   policy2,
		source:   source,
		dest:     dest2,
		logger:   &mockLogger{},
		auditLog: &mockAuditLogger{},
		metrics:  NewReplicationMetrics(),
	}

	// Second sync
	result2, err := syncer2.SyncIncremental(context.Background(), changeLog)
	require.NoError(t, err)
	assert.Equal(t, 1, result2.Synced)

	// Now both should have empty unprocessed
	unprocessed2, err = changeLog.GetUnprocessed("policy2")
	require.NoError(t, err)
	assert.Empty(t, unprocessed2)
}

func TestSyncIncremental_UnknownOperation(t *testing.T) {
	source := newExtendedMockStorage()
	dest := newExtendedMockStorage()

	// Create change log with unknown operation
	changeLog := newMockChangeLog()
	changeLog.RecordChange(ChangeEvent{
		Key:       "file1.txt",
		Operation: "unknown",
		Timestamp: time.Now(),
	})

	// Create syncer
	policy := common.ReplicationPolicy{
		ID:                  "test-policy",
		SourceBackend:       "mock",
		DestinationBackend:  "mock",
		ReplicationMode:     common.ReplicationModeTransparent,
	}

	syncer := &Syncer{
		policy:   policy,
		source:   source,
		dest:     dest,
		logger:   &mockLogger{},
		auditLog: &mockAuditLogger{},
		metrics:  NewReplicationMetrics(),
	}

	// Run incremental sync - should not error but also not process
	result, err := syncer.SyncIncremental(context.Background(), changeLog)
	require.NoError(t, err)
	assert.NotNil(t, result)

	// Verify nothing was processed
	assert.Equal(t, 0, result.Synced)
	assert.Equal(t, 0, result.Deleted)
	assert.Equal(t, 0, result.Failed)

	// Event should still be unprocessed
	unprocessed, err := changeLog.GetUnprocessed("test-policy")
	require.NoError(t, err)
	assert.Len(t, unprocessed, 1)
}

func TestSyncIncremental_LargeChangeLog(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large changelog test in short mode")
	}

	source := newExtendedMockStorage()
	dest := newExtendedMockStorage()

	// Create many events
	const numEvents = 100
	changeLog := newMockChangeLog()

	for i := 0; i < numEvents; i++ {
		key := fmt.Sprintf("file%d.txt", i)
		testData := []byte(fmt.Sprintf("test data %d", i))

		source.data[key] = testData
		source.objects[key] = &common.Metadata{
			Size: int64(len(testData)),
			ETag: fmt.Sprintf("etag%d", i),
		}

		changeLog.RecordChange(ChangeEvent{
			Key:       key,
			Operation: "put",
			Timestamp: time.Now(),
			ETag:      fmt.Sprintf("etag%d", i),
			Size:      int64(len(testData)),
		})
	}

	// Create syncer
	policy := common.ReplicationPolicy{
		ID:                  "test-policy",
		SourceBackend:       "mock",
		DestinationBackend:  "mock",
		ReplicationMode:     common.ReplicationModeTransparent,
	}

	syncer := &Syncer{
		policy:   policy,
		source:   source,
		dest:     dest,
		logger:   &mockLogger{},
		auditLog: &mockAuditLogger{},
		metrics:  NewReplicationMetrics(),
	}

	// Run incremental sync
	result, err := syncer.SyncIncremental(context.Background(), changeLog)
	require.NoError(t, err)
	assert.NotNil(t, result)

	// Verify all synced
	assert.Equal(t, numEvents, result.Synced)
	assert.Equal(t, 0, result.Failed)

	// Verify all in destination
	assert.Len(t, dest.data, numEvents)

	// Verify all processed
	unprocessed, err := changeLog.GetUnprocessed("test-policy")
	require.NoError(t, err)
	assert.Empty(t, unprocessed)
}
