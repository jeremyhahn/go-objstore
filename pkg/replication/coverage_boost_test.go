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
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/audit"
	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// TestSyncAllWithErrors tests SyncAll when some policies fail
func TestSyncAllWithErrors(t *testing.T) {
	ctx := context.Background()
	fs := newMockFileSystem()
	logger := adapters.NewNoOpLogger()
	auditLog := audit.NewNoOpAuditLogger()

	mgr, err := NewPersistentReplicationManager(fs, "test-policies.json", 5*time.Minute, logger, auditLog)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	// Add a good policy
	goodPolicy := common.ReplicationPolicy{
		ID:              "good-policy",
		SourceBackend:   "local",
		SourceSettings:  map[string]string{"path": "/tmp/good-src"},
		DestinationBackend: "local",
		DestinationSettings: map[string]string{"path": "/tmp/good-dst"},
		CheckInterval:   5 * time.Minute,
		Enabled:         true,
		ReplicationMode: common.ReplicationModeOpaque,
	}

	err = mgr.AddPolicy(goodPolicy)
	if err != nil {
		t.Fatalf("Failed to add good policy: %v", err)
	}

	// Add a policy with invalid backend
	badPolicy := common.ReplicationPolicy{
		ID:              "bad-policy",
		SourceBackend:   "invalid-backend",
		SourceSettings:  map[string]string{"path": "/tmp/bad-src"},
		DestinationBackend: "local",
		DestinationSettings: map[string]string{"path": "/tmp/bad-dst"},
		CheckInterval:   5 * time.Minute,
		Enabled:         true,
		ReplicationMode: common.ReplicationModeOpaque,
	}

	err = mgr.AddPolicy(badPolicy)
	if err != nil {
		t.Fatalf("Failed to add bad policy: %v", err)
	}

	// SyncAll should handle errors gracefully
	result, err := mgr.SyncAll(ctx)
	if err != nil {
		t.Fatalf("SyncAll should not return error: %v", err)
	}

	if result == nil {
		t.Fatal("Expected non-nil result")
	}

	// Should have errors from bad policy
	if result.Failed < 1 {
		t.Error("Expected at least 1 failure")
	}

	if len(result.Errors) < 1 {
		t.Error("Expected at least 1 error message")
	}

	// Check that error contains policy ID
	foundBadPolicyError := false
	for _, errMsg := range result.Errors {
		if strings.Contains(errMsg, "bad-policy") {
			foundBadPolicyError = true
			break
		}
	}

	if !foundBadPolicyError {
		t.Error("Expected error message to contain bad policy ID")
	}
}

// TestSyncAllWithResultErrors tests SyncAll when sync returns errors in result
func TestSyncAllWithResultErrors(t *testing.T) {
	ctx := context.Background()

	source := newExtendedMockStorage()
	dest := newExtendedMockStorage()

	// Add test data
	source.data["key1"] = []byte("data1")
	source.objects["key1"] = &common.Metadata{
		Size:         5,
		LastModified: time.Now(),
	}

	// Make dest fail for key1
	dest.putWithMetaFn = func(ctx context.Context, key string, data io.Reader, metadata *common.Metadata) error {
		return errors.New("dest error")
	}

	policy := common.ReplicationPolicy{
		ID:              "test-errors",
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

	// Should have errors
	if result.Failed != 1 {
		t.Errorf("Expected 1 failure, got %d", result.Failed)
	}

	if len(result.Errors) != 1 {
		t.Errorf("Expected 1 error, got %d", len(result.Errors))
	}
}

// TestSyncPolicyWithMetricsUpdate tests that metrics are updated after sync
func TestSyncPolicyWithMetricsUpdate(t *testing.T) {
	ctx := context.Background()
	fs := newMockFileSystem()
	logger := adapters.NewNoOpLogger()
	auditLog := audit.NewNoOpAuditLogger()

	mgr, err := NewPersistentReplicationManager(fs, "test-policies.json", 5*time.Minute, logger, auditLog)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	policy := common.ReplicationPolicy{
		ID:              "metrics-test",
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
	initialStatus, err := mgr.GetReplicationStatus("metrics-test")
	if err != nil {
		t.Fatalf("Failed to get initial status: %v", err)
	}

	initialSyncCount := initialStatus.SyncCount

	// Sync policy
	_, err = mgr.SyncPolicy(ctx, "metrics-test")
	if err != nil {
		t.Fatalf("SyncPolicy failed: %v", err)
	}

	// Verify metrics were updated
	updatedStatus, err := mgr.GetReplicationStatus("metrics-test")
	if err != nil {
		t.Fatalf("Failed to get updated status: %v", err)
	}

	if updatedStatus.SyncCount != initialSyncCount+1 {
		t.Errorf("Expected sync count to increase by 1, got %d (was %d)",
			updatedStatus.SyncCount, initialSyncCount)
	}

	// Verify LastSyncTime was updated on the policy itself
	updatedPolicy, err := mgr.GetPolicy("metrics-test")
	if err != nil {
		t.Fatalf("Failed to get updated policy: %v", err)
	}

	if updatedPolicy.LastSyncTime.IsZero() {
		t.Error("Expected LastSyncTime to be set after sync")
	}
}

// TestSaveErrorHandling tests save error scenarios
func TestSaveErrorHandling(t *testing.T) {
	// Create a mock filesystem that fails on write
	fs := &failingMockFileSystem{
		mockFileSystem: newMockFileSystem(),
		shouldFailOpen: false,
	}

	logger := adapters.NewNoOpLogger()
	auditLog := audit.NewNoOpAuditLogger()

	mgr, err := NewPersistentReplicationManager(fs, "test-policies.json", 5*time.Minute, logger, auditLog)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	policy := common.ReplicationPolicy{
		ID:              "test-policy",
		SourceBackend:   "local",
		DestinationBackend: "local",
		CheckInterval:   5 * time.Minute,
		Enabled:         true,
		ReplicationMode: common.ReplicationModeOpaque,
	}

	// Make filesystem fail on next operation
	fs.shouldFailOpen = true

	// This should fail to save
	err = mgr.AddPolicy(policy)
	if err == nil {
		t.Error("Expected error when filesystem fails")
	}
}

// TestGetReplicationStatusWithoutMetrics tests getting status when metrics don't exist yet
func TestGetReplicationStatusWithoutMetrics(t *testing.T) {
	fs := newMockFileSystem()
	logger := adapters.NewNoOpLogger()
	auditLog := audit.NewNoOpAuditLogger()

	mgr, err := NewPersistentReplicationManager(fs, "test-policies.json", 5*time.Minute, logger, auditLog)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	policy := common.ReplicationPolicy{
		ID:              "test-policy",
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

	// Manually delete metrics to simulate missing metrics
	mgr.mutex.Lock()
	delete(mgr.metrics, "test-policy")
	mgr.mutex.Unlock()

	// GetReplicationStatus should create new metrics
	status, err := mgr.GetReplicationStatus("test-policy")
	if err != nil {
		t.Fatalf("Failed to get status: %v", err)
	}

	if status == nil {
		t.Fatal("Expected non-nil status")
	}

	// Should have default values
	if status.TotalObjectsSynced != 0 {
		t.Errorf("Expected 0 objects synced, got %d", status.TotalObjectsSynced)
	}
}

// TestSyncPolicyParallelWithErrors tests parallel sync with syncer creation failure
func TestSyncPolicyParallelWithErrors(t *testing.T) {
	ctx := context.Background()
	fs := newMockFileSystem()
	logger := adapters.NewNoOpLogger()
	auditLog := audit.NewNoOpAuditLogger()

	mgr, err := NewPersistentReplicationManager(fs, "test-policies.json", 5*time.Minute, logger, auditLog)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	// Add policy with invalid backend
	policy := common.ReplicationPolicy{
		ID:              "invalid-backend",
		SourceBackend:   "nonexistent-backend",
		DestinationBackend: "local",
		CheckInterval:   5 * time.Minute,
		Enabled:         true,
		ReplicationMode: common.ReplicationModeOpaque,
	}

	err = mgr.AddPolicy(policy)
	if err != nil {
		t.Fatalf("Failed to add policy: %v", err)
	}

	// This should fail due to invalid backend
	_, err = mgr.SyncPolicyParallel(ctx, "invalid-backend", 4)
	if err == nil {
		t.Error("Expected error with invalid backend")
	}
}

// TestWorkerPoolErrorHandling tests worker error scenarios
func TestWorkerPoolErrorHandling(t *testing.T) {
	ctx := context.Background()

	source := newExtendedMockStorage()
	dest := newExtendedMockStorage()

	// Add data that will cause errors
	for i := 1; i <= 10; i++ {
		key := "key-" + string(rune('0'+i))
		source.data[key] = []byte("data-" + key)
		source.objects[key] = &common.Metadata{
			Size:         int64(len("data-" + key)),
			LastModified: time.Now(),
		}
	}

	// Make dest fail on odd keys
	dest.putWithMetaFn = func(ctx context.Context, key string, data io.Reader, metadata *common.Metadata) error {
		// Extract key number
		if len(key) > 4 {
			keyNum := key[4] - '0'
			if keyNum%2 == 1 {
				return errors.New("odd key error")
			}
		}
		return dest.defaultPutWithMetadata(ctx, key, data, metadata)
	}

	policy := common.ReplicationPolicy{
		ID:              "worker-errors",
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

	result, err := syncer.SyncAllParallel(ctx, 3)
	if err != nil {
		t.Fatalf("SyncAllParallel failed: %v", err)
	}

	// Should have some successes and some failures
	if result.Synced < 1 {
		t.Error("Expected at least some successful syncs")
	}

	if result.Failed < 1 {
		t.Error("Expected at least some failures")
	}

	totalProcessed := result.Synced + result.Failed
	if totalProcessed != 10 {
		t.Errorf("Expected 10 total processed, got %d", totalProcessed)
	}
}

// failingMockFileSystem wraps mockFileSystem to simulate failures
type failingMockFileSystem struct {
	*mockFileSystem
	shouldFailOpen bool
}

func (f *failingMockFileSystem) OpenFile(name string, flag int, perm os.FileMode) (ReplicationFile, error) {
	if f.shouldFailOpen {
		return nil, errors.New("simulated open failure")
	}
	return f.mockFileSystem.OpenFile(name, flag, perm)
}
