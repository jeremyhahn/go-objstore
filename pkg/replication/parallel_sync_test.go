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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/audit"
	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// TestSyncPolicyParallel tests the parallel sync functionality of a policy
func TestSyncPolicyParallel(t *testing.T) {
	ctx := context.Background()
	fs := newMockFileSystem()
	logger := adapters.NewNoOpLogger()
	auditLog := audit.NewNoOpAuditLogger()

	// Create manager
	mgr, err := NewPersistentReplicationManager(fs, "test-policies.json", 5*time.Minute, logger, auditLog)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	// Add a policy
	policy := common.ReplicationPolicy{
		ID:                  "parallel-test",
		SourceBackend:       "local",
		SourceSettings:      map[string]string{"path": "/tmp/replication-test-src"},
		DestinationBackend:  "local",
		DestinationSettings: map[string]string{"path": "/tmp/replication-test-dst"},
		CheckInterval:       5 * time.Minute,
		Enabled:             true,
		ReplicationMode:     common.ReplicationModeOpaque,
	}

	err = mgr.AddPolicy(policy)
	if err != nil {
		t.Fatalf("Failed to add policy: %v", err)
	}

	// Test with valid worker count
	result, err := mgr.SyncPolicyParallel(ctx, "parallel-test", 4)
	if err != nil {
		t.Fatalf("SyncPolicyParallel failed: %v", err)
	}

	if result == nil {
		t.Fatal("Expected non-nil result")
	}

	if result.PolicyID != "parallel-test" {
		t.Errorf("Expected policy ID 'parallel-test', got %s", result.PolicyID)
	}

	// Verify metrics were updated
	status, err := mgr.GetReplicationStatus("parallel-test")
	if err != nil {
		t.Fatalf("Failed to get status: %v", err)
	}

	if status.SyncCount != 1 {
		t.Errorf("Expected sync count 1, got %d", status.SyncCount)
	}

	// Verify last sync time was updated
	updatedPolicy, err := mgr.GetPolicy("parallel-test")
	if err != nil {
		t.Fatalf("Failed to get updated policy: %v", err)
	}

	if updatedPolicy.LastSyncTime.IsZero() {
		t.Error("Expected LastSyncTime to be updated")
	}
}

// TestSyncPolicyParallelNonExistent tests parallel sync with non-existent policy
func TestSyncPolicyParallelNonExistent(t *testing.T) {
	ctx := context.Background()
	fs := newMockFileSystem()
	logger := adapters.NewNoOpLogger()
	auditLog := audit.NewNoOpAuditLogger()

	mgr, err := NewPersistentReplicationManager(fs, "test-policies.json", 5*time.Minute, logger, auditLog)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	_, err = mgr.SyncPolicyParallel(ctx, "nonexistent", 4)
	if !errors.Is(err, common.ErrPolicyNotFound) {
		t.Errorf("Expected ErrPolicyNotFound, got %v", err)
	}
}

// TestSyncPolicyParallelInvalidWorkerCount tests parallel sync with invalid worker count
func TestSyncPolicyParallelInvalidWorkerCount(t *testing.T) {
	ctx := context.Background()
	fs := newMockFileSystem()
	logger := adapters.NewNoOpLogger()
	auditLog := audit.NewNoOpAuditLogger()

	mgr, err := NewPersistentReplicationManager(fs, "test-policies.json", 5*time.Minute, logger, auditLog)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	policy := common.ReplicationPolicy{
		ID:                  "test-policy",
		SourceBackend:       "local",
		SourceSettings:      map[string]string{"path": "/tmp/src"},
		DestinationBackend:  "local",
		DestinationSettings: map[string]string{"path": "/tmp/dst"},
		CheckInterval:       5 * time.Minute,
		Enabled:             true,
		ReplicationMode:     common.ReplicationModeOpaque,
	}

	err = mgr.AddPolicy(policy)
	if err != nil {
		t.Fatalf("Failed to add policy: %v", err)
	}

	// Test with 0 workers (should default to 4)
	result, err := mgr.SyncPolicyParallel(ctx, "test-policy", 0)
	if err != nil {
		t.Fatalf("SyncPolicyParallel failed: %v", err)
	}

	if result == nil {
		t.Fatal("Expected non-nil result")
	}

	// Test with negative workers (should default to 4)
	result, err = mgr.SyncPolicyParallel(ctx, "test-policy", -5)
	if err != nil {
		t.Fatalf("SyncPolicyParallel failed: %v", err)
	}

	if result == nil {
		t.Fatal("Expected non-nil result")
	}
}

// TestSyncAllParallel tests parallel sync of all objects in a syncer
func TestSyncAllParallel(t *testing.T) {
	ctx := context.Background()

	// Create mock source and destination backends
	source := newExtendedMockStorage()
	dest := newExtendedMockStorage()

	// Add some test data to source
	testData := []string{"key1", "key2", "key3", "key4", "key5"}
	for _, key := range testData {
		source.data[key] = []byte("test-data-" + key)
		source.objects[key] = &common.Metadata{
			Size:         int64(len("test-data-" + key)),
			LastModified: time.Now(),
		}
	}

	// Create syncer
	policy := common.ReplicationPolicy{
		ID:              "test-sync-parallel",
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

	// Run parallel sync with 3 workers
	result, err := syncer.SyncAllParallel(ctx, 3)
	if err != nil {
		t.Fatalf("SyncAllParallel failed: %v", err)
	}

	if result == nil {
		t.Fatal("Expected non-nil result")
	}

	// Verify all objects were synced
	if result.Synced != len(testData) {
		t.Errorf("Expected %d objects synced, got %d", len(testData), result.Synced)
	}

	if result.Failed != 0 {
		t.Errorf("Expected 0 failures, got %d", result.Failed)
	}

	// Verify metrics were updated
	metrics := syncer.GetMetrics()
	snapshot := metrics.GetMetricsSnapshot()

	if snapshot.TotalObjectsSynced != int64(len(testData)) {
		t.Errorf("Expected %d total objects synced in metrics, got %d", len(testData), snapshot.TotalObjectsSynced)
	}
}

// TestSyncAllParallelNoChanges tests parallel sync when no changes are detected
func TestSyncAllParallelNoChanges(t *testing.T) {
	ctx := context.Background()

	source := newExtendedMockStorage()
	dest := newExtendedMockStorage()

	// Both source and dest have same data
	testKey := "same-key"
	testData := []byte("same-data")
	testTime := time.Now()

	source.data[testKey] = testData
	source.objects[testKey] = &common.Metadata{
		Size:         int64(len(testData)),
		LastModified: testTime,
	}

	dest.data[testKey] = testData
	dest.objects[testKey] = &common.Metadata{
		Size:         int64(len(testData)),
		LastModified: testTime,
	}

	policy := common.ReplicationPolicy{
		ID:              "test-no-changes",
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

	result, err := syncer.SyncAllParallel(ctx, 2)
	if err != nil {
		t.Fatalf("SyncAllParallel failed: %v", err)
	}

	// Should have no objects to sync
	if result.Synced != 0 {
		t.Errorf("Expected 0 objects synced, got %d", result.Synced)
	}
}

// TestSyncAllParallelWithErrors tests parallel sync with some failures
func TestSyncAllParallelWithErrors(t *testing.T) {
	ctx := context.Background()

	source := newExtendedMockStorage()
	dest := newExtendedMockStorage()

	// Add test data - 5 keys
	for i := 1; i <= 5; i++ {
		key := "key-" + string(rune('0'+i))
		source.data[key] = []byte("data-" + key)
		source.objects[key] = &common.Metadata{
			Size:         int64(len("data-" + key)),
			LastModified: time.Now(),
		}
	}

	// Make destination fail for specific keys
	failKey := "key-3"
	dest.putWithMetaFn = func(ctx context.Context, key string, data io.Reader, metadata *common.Metadata) error {
		if key == failKey {
			return errors.New("simulated put error")
		}
		return dest.defaultPutWithMetadata(ctx, key, data, metadata)
	}

	policy := common.ReplicationPolicy{
		ID:              "test-with-errors",
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

	result, err := syncer.SyncAllParallel(ctx, 2)
	if err != nil {
		t.Fatalf("SyncAllParallel failed: %v", err)
	}

	// Should have 4 successful syncs and 1 failure
	if result.Synced != 4 {
		t.Errorf("Expected 4 objects synced, got %d", result.Synced)
	}

	if result.Failed != 1 {
		t.Errorf("Expected 1 failure, got %d", result.Failed)
	}

	if len(result.Errors) != 1 {
		t.Errorf("Expected 1 error message, got %d", len(result.Errors))
	}

	// Verify error message contains the failed key
	if len(result.Errors) > 0 && !strings.Contains(result.Errors[0], failKey) {
		t.Errorf("Expected error message to contain '%s', got: %s", failKey, result.Errors[0])
	}
}

// TestGetOrCreateMetricsConcurrent tests concurrent access to getOrCreateMetrics
func TestGetOrCreateMetricsConcurrent(t *testing.T) {
	fs := newMockFileSystem()
	logger := adapters.NewNoOpLogger()
	auditLog := audit.NewNoOpAuditLogger()

	mgr, err := NewPersistentReplicationManager(fs, "test-policies.json", 5*time.Minute, logger, auditLog)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	// Test concurrent access to getOrCreateMetrics
	policyID := "concurrent-test"
	var wg sync.WaitGroup
	numGoroutines := 10
	metricsRefs := make([]*ReplicationMetrics, numGoroutines)

	// All goroutines should get the same metrics instance
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			metricsRefs[idx] = mgr.getOrCreateMetrics(policyID)
		}(i)
	}

	wg.Wait()

	// Verify all got the same instance
	firstMetrics := metricsRefs[0]
	for i := 1; i < numGoroutines; i++ {
		if metricsRefs[i] != firstMetrics {
			t.Errorf("Goroutine %d got different metrics instance", i)
		}
	}
}

// TestMetricsCollection tests that metrics are properly collected during sync
func TestMetricsCollection(t *testing.T) {
	ctx := context.Background()

	source := newExtendedMockStorage()
	dest := newExtendedMockStorage()

	// Add test data
	testKeys := []string{"key1", "key2", "key3"}
	totalBytes := int64(0)
	for _, key := range testKeys {
		data := []byte("test-data-" + key)
		source.data[key] = data
		source.objects[key] = &common.Metadata{
			Size:         int64(len(data)),
			LastModified: time.Now(),
		}
		totalBytes += int64(len(data))
	}

	policy := common.ReplicationPolicy{
		ID:              "metrics-test",
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

	// Run sync
	result, err := syncer.SyncAll(ctx)
	if err != nil {
		t.Fatalf("SyncAll failed: %v", err)
	}

	// Verify result metrics
	if result.Synced != len(testKeys) {
		t.Errorf("Expected %d synced, got %d", len(testKeys), result.Synced)
	}

	if result.BytesTotal != totalBytes {
		t.Errorf("Expected %d bytes, got %d", totalBytes, result.BytesTotal)
	}

	// Verify syncer metrics
	metrics := syncer.GetMetrics()
	snapshot := metrics.GetMetricsSnapshot()

	if snapshot.TotalObjectsSynced != int64(len(testKeys)) {
		t.Errorf("Expected %d total objects synced, got %d", len(testKeys), snapshot.TotalObjectsSynced)
	}

	if snapshot.TotalBytesSynced != totalBytes {
		t.Errorf("Expected %d total bytes synced, got %d", totalBytes, snapshot.TotalBytesSynced)
	}

	if snapshot.SyncCount != 1 {
		t.Errorf("Expected 1 sync count, got %d", snapshot.SyncCount)
	}

	if snapshot.LastSyncTime.IsZero() {
		t.Error("Expected LastSyncTime to be set")
	}

	if snapshot.AverageSyncDuration <= 0 {
		t.Error("Expected positive average sync duration")
	}
}

// TestPolicyLifecycle tests adding, syncing, and removing policies
func TestPolicyLifecycle(t *testing.T) {
	ctx := context.Background()
	fs := newMockFileSystem()
	logger := adapters.NewNoOpLogger()
	auditLog := audit.NewNoOpAuditLogger()

	mgr, err := NewPersistentReplicationManager(fs, "test-policies.json", 5*time.Minute, logger, auditLog)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	// Step 1: Add policy
	policy := common.ReplicationPolicy{
		ID:              "lifecycle-test",
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

	// Verify policy exists
	retrieved, err := mgr.GetPolicy("lifecycle-test")
	if err != nil {
		t.Fatalf("Failed to get policy: %v", err)
	}
	if retrieved.ID != "lifecycle-test" {
		t.Errorf("Expected policy ID 'lifecycle-test', got %s", retrieved.ID)
	}

	// Step 2: Sync policy
	result, err := mgr.SyncPolicy(ctx, "lifecycle-test")
	if err != nil {
		t.Fatalf("Failed to sync policy: %v", err)
	}
	if result == nil {
		t.Fatal("Expected non-nil sync result")
	}

	// Verify metrics exist
	status, err := mgr.GetReplicationStatus("lifecycle-test")
	if err != nil {
		t.Fatalf("Failed to get status: %v", err)
	}
	if status.PolicyID != "lifecycle-test" {
		t.Errorf("Expected policy ID 'lifecycle-test', got %s", status.PolicyID)
	}

	// Step 3: Remove policy
	err = mgr.RemovePolicy("lifecycle-test")
	if err != nil {
		t.Fatalf("Failed to remove policy: %v", err)
	}

	// Verify policy is gone
	_, err = mgr.GetPolicy("lifecycle-test")
	if !errors.Is(err, common.ErrPolicyNotFound) {
		t.Errorf("Expected ErrPolicyNotFound after removal, got %v", err)
	}

	// Verify metrics are cleaned up
	_, err = mgr.GetReplicationStatus("lifecycle-test")
	if !errors.Is(err, common.ErrPolicyNotFound) {
		t.Errorf("Expected ErrPolicyNotFound for status after removal, got %v", err)
	}
}

// TestMultiBackendReplication tests replication across multiple backends
func TestMultiBackendReplication(t *testing.T) {
	ctx := context.Background()
	fs := newMockFileSystem()
	logger := adapters.NewNoOpLogger()
	auditLog := audit.NewNoOpAuditLogger()

	mgr, err := NewPersistentReplicationManager(fs, "test-policies.json", 5*time.Minute, logger, auditLog)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	// Add multiple policies for different backends
	policies := []common.ReplicationPolicy{
		{
			ID:              "policy-1",
			SourceBackend:   "local",
			SourceSettings:  map[string]string{"path": "/tmp/src1"},
			DestinationBackend: "local",
			DestinationSettings: map[string]string{"path": "/tmp/dst1"},
			CheckInterval:   5 * time.Minute,
			Enabled:         true,
			ReplicationMode: common.ReplicationModeOpaque,
		},
		{
			ID:              "policy-2",
			SourceBackend:   "local",
			SourceSettings:  map[string]string{"path": "/tmp/src2"},
			DestinationBackend: "local",
			DestinationSettings: map[string]string{"path": "/tmp/dst2"},
			CheckInterval:   5 * time.Minute,
			Enabled:         true,
			ReplicationMode: common.ReplicationModeOpaque,
		},
		{
			ID:              "policy-3-disabled",
			SourceBackend:   "local",
			SourceSettings:  map[string]string{"path": "/tmp/src3"},
			DestinationBackend: "local",
			DestinationSettings: map[string]string{"path": "/tmp/dst3"},
			CheckInterval:   5 * time.Minute,
			Enabled:         false, // Disabled
			ReplicationMode: common.ReplicationModeOpaque,
		},
	}

	for _, p := range policies {
		err = mgr.AddPolicy(p)
		if err != nil {
			t.Fatalf("Failed to add policy %s: %v", p.ID, err)
		}
	}

	// Sync all policies
	result, err := mgr.SyncAll(ctx)
	if err != nil {
		t.Fatalf("SyncAll failed: %v", err)
	}

	if result == nil {
		t.Fatal("Expected non-nil result")
	}

	// Verify all enabled policies have status
	for _, p := range policies {
		status, err := mgr.GetReplicationStatus(p.ID)
		if err != nil {
			t.Fatalf("Failed to get status for %s: %v", p.ID, err)
		}

		if p.Enabled {
			// Enabled policies should have been synced
			if status.SyncCount == 0 {
				t.Errorf("Expected sync count > 0 for enabled policy %s", p.ID)
			}
		}
	}
}

// TestErrorRecovery tests that errors in one policy don't affect others
func TestErrorRecovery(t *testing.T) {
	ctx := context.Background()
	fs := newMockFileSystem()
	logger := adapters.NewNoOpLogger()
	auditLog := audit.NewNoOpAuditLogger()

	mgr, err := NewPersistentReplicationManager(fs, "test-policies.json", 5*time.Minute, logger, auditLog)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	// Add good policy
	goodPolicy := common.ReplicationPolicy{
		ID:              "good-policy",
		SourceBackend:   "local",
		SourceSettings:  map[string]string{"path": "/tmp/src-good"},
		DestinationBackend: "local",
		DestinationSettings: map[string]string{"path": "/tmp/dst-good"},
		CheckInterval:   5 * time.Minute,
		Enabled:         true,
		ReplicationMode: common.ReplicationModeOpaque,
	}

	err = mgr.AddPolicy(goodPolicy)
	if err != nil {
		t.Fatalf("Failed to add good policy: %v", err)
	}

	// Add policy with invalid backend (will fail during sync)
	badPolicy := common.ReplicationPolicy{
		ID:              "bad-policy",
		SourceBackend:   "invalid-backend-type",
		SourceSettings:  map[string]string{"path": "/tmp/src-bad"},
		DestinationBackend: "local",
		DestinationSettings: map[string]string{"path": "/tmp/dst-bad"},
		CheckInterval:   5 * time.Minute,
		Enabled:         true,
		ReplicationMode: common.ReplicationModeOpaque,
	}

	err = mgr.AddPolicy(badPolicy)
	if err != nil {
		t.Fatalf("Failed to add bad policy: %v", err)
	}

	// Sync all - should handle errors gracefully
	result, err := mgr.SyncAll(ctx)
	if err != nil {
		t.Fatalf("SyncAll should not fail completely: %v", err)
	}

	// Should have at least one error from bad policy
	if result.Failed < 1 {
		t.Error("Expected at least 1 failure from bad policy")
	}

	if len(result.Errors) < 1 {
		t.Error("Expected at least 1 error message")
	}

	// Good policy should still be accessible
	status, err := mgr.GetReplicationStatus("good-policy")
	if err != nil {
		t.Fatalf("Failed to get status for good policy: %v", err)
	}

	if status == nil {
		t.Fatal("Expected non-nil status for good policy")
	}
}

// TestGetFactories tests the getFactories helper
func TestGetFactories(t *testing.T) {
	fs := newMockFileSystem()
	logger := adapters.NewNoOpLogger()
	auditLog := audit.NewNoOpAuditLogger()

	mgr, err := NewPersistentReplicationManager(fs, "test-policies.json", 5*time.Minute, logger, auditLog)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	// Add a policy
	policy := common.ReplicationPolicy{
		ID:              "factory-test",
		SourceBackend:   "local",
		DestinationBackend: "local",
		CheckInterval:   5 * time.Minute,
		Enabled:         true,
		ReplicationMode: common.ReplicationModeOpaque,
	}

	err = mgr.AddPolicy(policy)
	if err != nil {
		t.Fatalf("Failed to add policy: %v", err)
	}

	// Get factories without setting any (should return noops)
	backend, source, dest := mgr.getFactories("factory-test")
	if backend == nil {
		t.Error("Expected non-nil backend factory")
	}
	if source == nil {
		t.Error("Expected non-nil source factory")
	}
	if dest == nil {
		t.Error("Expected non-nil dest factory")
	}

	// Set custom factories
	customBackend := NewNoopEncrypterFactory()
	customSource := NewNoopEncrypterFactory()
	customDest := NewNoopEncrypterFactory()

	mgr.SetBackendEncrypterFactory("factory-test", customBackend)
	mgr.SetSourceEncrypterFactory("factory-test", customSource)
	mgr.SetDestinationEncrypterFactory("factory-test", customDest)

	// Get factories again - should return custom ones
	backend2, source2, dest2 := mgr.getFactories("factory-test")
	if backend2 != customBackend {
		t.Error("Expected custom backend factory")
	}
	if source2 != customSource {
		t.Error("Expected custom source factory")
	}
	if dest2 != customDest {
		t.Error("Expected custom dest factory")
	}
}
