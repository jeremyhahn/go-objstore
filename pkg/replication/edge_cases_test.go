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

//go:build local

package replication

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/audit"
	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// TestSyncIncrementalSyncError tests incremental sync when sync fails
func TestSyncIncrementalSyncError(t *testing.T) {
	ctx := context.Background()

	source := newExtendedMockStorage()
	dest := newExtendedMockStorage()

	// Add data but make source fail on GetWithContext
	testKey := "fail-key"
	source.objects[testKey] = &common.Metadata{
		Size:         10,
		LastModified: time.Now(),
	}
	source.getError = errors.New("get error")

	policy := common.ReplicationPolicy{
		ID:              "sync-error-test",
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

	changelog := newMockChangeLog()
	changelog.RecordChange(ChangeEvent{
		Key:       testKey,
		Operation: "put",
		Timestamp: time.Now(),
	})

	result, err := syncer.SyncIncremental(ctx, changelog)
	if err != nil {
		t.Fatalf("SyncIncremental should not error completely: %v", err)
	}

	// Should have failed the sync
	if result.Failed != 1 {
		t.Errorf("Expected 1 failure, got %d", result.Failed)
	}

	if len(result.Errors) != 1 {
		t.Errorf("Expected 1 error, got %d", len(result.Errors))
	}
}

// TestSyncIncrementalDeleteError tests incremental sync when delete fails
func TestSyncIncrementalDeleteError(t *testing.T) {
	ctx := context.Background()

	source := newExtendedMockStorage()
	dest := &extendedMockStorageWithDeleteError{
		extendedMockStorage: newExtendedMockStorage(),
		deleteError:         errors.New("delete error"),
	}

	policy := common.ReplicationPolicy{
		ID:              "delete-error-test",
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

	changelog := newMockChangeLog()
	changelog.RecordChange(ChangeEvent{
		Key:       "delete-key",
		Operation: "delete",
		Timestamp: time.Now(),
	})

	result, err := syncer.SyncIncremental(ctx, changelog)
	if err != nil {
		t.Fatalf("SyncIncremental should not error completely: %v", err)
	}

	// Should have failed the delete
	if result.Failed != 1 {
		t.Errorf("Expected 1 failure, got %d", result.Failed)
	}

	if len(result.Errors) != 1 {
		t.Errorf("Expected 1 error, got %d", len(result.Errors))
	}
}

// TestSyncAllMetricsUpdateWithErrors tests that metrics are updated even when errors occur
func TestSyncAllMetricsUpdateWithErrors(t *testing.T) {
	ctx := context.Background()

	source := newExtendedMockStorage()
	dest := newExtendedMockStorage()

	// Add data that will partially fail
	for i := 0; i < 5; i++ {
		key := "key-" + string(rune('0'+i))
		source.data[key] = []byte("data")
		source.objects[key] = &common.Metadata{
			Size:         4,
			LastModified: time.Now(),
		}
	}

	// Make dest fail on specific keys
	dest.putWithMetaFn = func(ctx context.Context, key string, data io.Reader, metadata *common.Metadata) error {
		if key == "key-2" || key == "key-4" {
			return errors.New("put error")
		}
		return dest.defaultPutWithMetadata(ctx, key, data, metadata)
	}

	policy := common.ReplicationPolicy{
		ID:              "metrics-errors-test",
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

	result, err := syncer.SyncAll(ctx)
	if err != nil {
		t.Fatalf("SyncAll failed: %v", err)
	}

	// Should have 3 successes and 2 failures
	if result.Synced != 3 {
		t.Errorf("Expected 3 synced, got %d", result.Synced)
	}

	if result.Failed != 2 {
		t.Errorf("Expected 2 failures, got %d", result.Failed)
	}

	// Verify metrics reflect both successes and errors
	metrics := syncer.GetMetrics()
	snapshot := metrics.GetMetricsSnapshot()

	if snapshot.TotalObjectsSynced != 3 {
		t.Errorf("Expected 3 objects synced in metrics, got %d", snapshot.TotalObjectsSynced)
	}

	if snapshot.TotalErrors != 2 {
		t.Errorf("Expected 2 errors in metrics, got %d", snapshot.TotalErrors)
	}
}

// TestSyncPolicyParallelMetricsUpdate tests that parallel sync updates metrics correctly
func TestSyncPolicyParallelMetricsUpdate(t *testing.T) {
	ctx := context.Background()
	fs := newMockFileSystem()
	logger := adapters.NewNoOpLogger()
	auditLog := audit.NewNoOpAuditLogger()

	mgr, err := NewPersistentReplicationManager(fs, "test-policies.json", 5*time.Minute, logger, auditLog)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	policy := common.ReplicationPolicy{
		ID:              "parallel-metrics-test",
		SourceBackend:   "local",
		SourceSettings:  map[string]string{"path": "/tmp/src"},
		DestinationBackend: "local",
		DestinationSettings: map[string]string{"path": "/tmp/dst"},
		CheckInterval:   5 * time.Minute,
		Enabled:         true,
		ReplicationMode: common.ReplicationModeOpaque,
	}

	err = mgr.AddPolicy(policy)
	if err != nil {
		t.Fatalf("Failed to add policy: %v", err)
	}

	// Get initial metrics
	initialStatus, err := mgr.GetReplicationStatus("parallel-metrics-test")
	if err != nil {
		t.Fatalf("Failed to get initial status: %v", err)
	}

	initialSyncCount := initialStatus.SyncCount

	// Run parallel sync
	_, err = mgr.SyncPolicyParallel(ctx, "parallel-metrics-test", 4)
	if err != nil {
		t.Fatalf("SyncPolicyParallel failed: %v", err)
	}

	// Verify metrics updated
	updatedStatus, err := mgr.GetReplicationStatus("parallel-metrics-test")
	if err != nil {
		t.Fatalf("Failed to get updated status: %v", err)
	}

	if updatedStatus.SyncCount != initialSyncCount+1 {
		t.Errorf("Expected sync count to increase by 1, got %d (was %d)",
			updatedStatus.SyncCount, initialSyncCount)
	}

	// Verify LastSyncTime was updated
	updatedPolicy, err := mgr.GetPolicy("parallel-metrics-test")
	if err != nil {
		t.Fatalf("Failed to get updated policy: %v", err)
	}

	if updatedPolicy.LastSyncTime.IsZero() {
		t.Error("Expected LastSyncTime to be set")
	}
}

// extendedMockStorageWithDeleteError extends extendedMockStorage to fail on delete
type extendedMockStorageWithDeleteError struct {
	*extendedMockStorage
	deleteError error
}

func (e *extendedMockStorageWithDeleteError) DeleteWithContext(ctx context.Context, key string) error {
	if e.deleteError != nil {
		return e.deleteError
	}
	return e.extendedMockStorage.DeleteWithContext(ctx, key)
}

// TestSyncAllParallelEmptyKeys tests parallel sync with no keys to sync
func TestSyncAllParallelEmptyKeys(t *testing.T) {
	ctx := context.Background()

	source := newExtendedMockStorage()
	dest := newExtendedMockStorage()

	// No data - should result in no changes

	policy := common.ReplicationPolicy{
		ID:              "empty-parallel-test",
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

	result, err := syncer.SyncAllParallel(ctx, 4)
	if err != nil {
		t.Fatalf("SyncAllParallel failed: %v", err)
	}

	if result.Synced != 0 {
		t.Errorf("Expected 0 synced, got %d", result.Synced)
	}

	if result.Failed != 0 {
		t.Errorf("Expected 0 failed, got %d", result.Failed)
	}
}

// TestGetFactoriesNoopDefaults tests that getFactories returns noop factories by default
func TestGetFactoriesNoopDefaults(t *testing.T) {
	fs := newMockFileSystem()
	logger := adapters.NewNoOpLogger()
	auditLog := audit.NewNoOpAuditLogger()

	mgr, err := NewPersistentReplicationManager(fs, "test-policies.json", 5*time.Minute, logger, auditLog)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	// Get factories for non-existent policy - should return noops
	backend, source, dest := mgr.getFactories("nonexistent-policy")

	if backend == nil {
		t.Error("Expected non-nil backend factory")
	}

	if source == nil {
		t.Error("Expected non-nil source factory")
	}

	if dest == nil {
		t.Error("Expected non-nil dest factory")
	}

	// Verify they're actually usable noop factories
	backendEnc, err := backend.GetEncrypter("test-key")
	if err != nil {
		t.Errorf("Backend factory GetEncrypter failed: %v", err)
	}

	if backendEnc == nil {
		t.Error("Expected non-nil encrypter from backend factory")
	}
}

// TestNewSyncerOpaqueMode tests syncer creation in opaque mode
func TestNewSyncerOpaqueMode(t *testing.T) {
	policy := common.ReplicationPolicy{
		ID:              "opaque-test",
		SourceBackend:   "local",
		SourceSettings:  map[string]string{"path": "/tmp/src"},
		DestinationBackend: "local",
		DestinationSettings: map[string]string{"path": "/tmp/dst"},
		ReplicationMode: common.ReplicationModeOpaque,
		Encryption: &common.EncryptionPolicy{
			Source: &common.EncryptionConfig{
				Enabled: true,
			},
			Destination: &common.EncryptionConfig{
				Enabled: true,
			},
		},
	}

	syncer, err := NewSyncer(
		policy,
		NewNoopEncrypterFactory(),
		NewNoopEncrypterFactory(),
		NewNoopEncrypterFactory(),
		adapters.NewNoOpLogger(),
		audit.NewNoOpAuditLogger(),
	)

	if err != nil {
		t.Fatalf("NewSyncer failed: %v", err)
	}

	if syncer == nil {
		t.Fatal("Expected non-nil syncer")
	}

	// In opaque mode, encryption should not be wrapped
	// Just verify syncer was created successfully
}
