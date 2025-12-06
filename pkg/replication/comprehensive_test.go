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
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/audit"
	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// TestMultipleSyncRounds tests multiple sync operations in sequence
func TestMultipleSyncRounds(t *testing.T) {
	ctx := context.Background()
	fs := newMockFileSystem()
	logger := adapters.NewNoOpLogger()
	auditLog := audit.NewNoOpAuditLogger()

	mgr, err := NewPersistentReplicationManager(fs, "test-policies.json", 5*time.Minute, logger, auditLog)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	policy := common.ReplicationPolicy{
		ID:              "multi-sync-test",
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

	// Run multiple syncs
	for i := 0; i < 3; i++ {
		_, err = mgr.SyncPolicy(ctx, "multi-sync-test")
		if err != nil {
			t.Fatalf("Sync %d failed: %v", i, err)
		}
	}

	// Verify metrics show multiple syncs
	status, err := mgr.GetReplicationStatus("multi-sync-test")
	if err != nil {
		t.Fatalf("Failed to get status: %v", err)
	}

	if status.SyncCount != 3 {
		t.Errorf("Expected 3 syncs, got %d", status.SyncCount)
	}

	// Verify average duration is set
	if status.AverageSyncDuration <= 0 {
		t.Error("Expected positive average sync duration")
	}
}

// TestSyncAllMultiplePolicies tests syncing multiple policies at once
func TestSyncAllMultiplePolicies(t *testing.T) {
	ctx := context.Background()
	fs := newMockFileSystem()
	logger := adapters.NewNoOpLogger()
	auditLog := audit.NewNoOpAuditLogger()

	mgr, err := NewPersistentReplicationManager(fs, "test-policies.json", 5*time.Minute, logger, auditLog)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	// Add 5 policies
	for i := 1; i <= 5; i++ {
		policy := common.ReplicationPolicy{
			ID:              "policy-" + string(rune('0'+i)),
			SourceBackend:   "local",
			SourceSettings:  map[string]string{"path": "/tmp/src" + string(rune('0'+i))},
			DestinationBackend: "local",
			DestinationSettings: map[string]string{"path": "/tmp/dst" + string(rune('0'+i))},
			CheckInterval:   5 * time.Minute,
			Enabled:         true,
			ReplicationMode: common.ReplicationModeOpaque,
		}

		err = mgr.AddPolicy(policy)
		if err != nil {
			t.Fatalf("Failed to add policy %d: %v", i, err)
		}
	}

	// Sync all
	result, err := mgr.SyncAll(ctx)
	if err != nil {
		t.Fatalf("SyncAll failed: %v", err)
	}

	if result == nil {
		t.Fatal("Expected non-nil result")
	}

	// Verify all policies were processed
	for i := 1; i <= 5; i++ {
		status, err := mgr.GetReplicationStatus("policy-" + string(rune('0'+i)))
		if err != nil {
			t.Fatalf("Failed to get status for policy %d: %v", i, err)
		}

		if status.SyncCount != 1 {
			t.Errorf("Expected policy %d to be synced once, got %d", i, status.SyncCount)
		}
	}
}

// TestSyncAllParallelWithLargeDataset tests parallel sync with many objects
func TestSyncAllParallelWithLargeDataset(t *testing.T) {
	ctx := context.Background()

	source := newExtendedMockStorage()
	dest := newExtendedMockStorage()

	// Add 50 objects
	for i := 0; i < 50; i++ {
		key := "large-key-" + string(rune('a'+i%26)) + string(rune('0'+i%10))
		data := []byte("data-" + key)
		source.data[key] = data
		source.objects[key] = &common.Metadata{
			Size:         int64(len(data)),
			LastModified: time.Now(),
		}
	}

	policy := common.ReplicationPolicy{
		ID:              "large-dataset-test",
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

	// Run with 8 workers
	result, err := syncer.SyncAllParallel(ctx, 8)
	if err != nil {
		t.Fatalf("SyncAllParallel failed: %v", err)
	}

	// Should sync all 50 objects
	if result.Synced != 50 {
		t.Errorf("Expected 50 synced, got %d", result.Synced)
	}

	// Verify metrics
	metrics := syncer.GetMetrics()
	snapshot := metrics.GetMetricsSnapshot()

	if snapshot.TotalObjectsSynced != 50 {
		t.Errorf("Expected 50 objects synced in metrics, got %d", snapshot.TotalObjectsSynced)
	}

	if snapshot.TotalBytesSynced <= 0 {
		t.Error("Expected positive bytes synced")
	}
}

// TestPolicyReload tests loading policies from storage after restart
func TestPolicyReload(t *testing.T) {
	fs := newMockFileSystem()
	logger := adapters.NewNoOpLogger()
	auditLog := audit.NewNoOpAuditLogger()

	// Create first manager and add policies
	mgr1, err := NewPersistentReplicationManager(fs, "reload-test.json", 5*time.Minute, logger, auditLog)
	if err != nil {
		t.Fatalf("Failed to create first manager: %v", err)
	}

	policy := common.ReplicationPolicy{
		ID:              "reload-policy",
		SourceBackend:   "local",
		SourceSettings:  map[string]string{"path": "/tmp/src"},
		DestinationBackend: "local",
		DestinationSettings: map[string]string{"path": "/tmp/dst"},
		CheckInterval:   10 * time.Minute,
		Enabled:         true,
		ReplicationMode: common.ReplicationModeTransparent,
	}

	err = mgr1.AddPolicy(policy)
	if err != nil {
		t.Fatalf("Failed to add policy: %v", err)
	}

	// Create second manager - should load the policy
	mgr2, err := NewPersistentReplicationManager(fs, "reload-test.json", 5*time.Minute, logger, auditLog)
	if err != nil {
		t.Fatalf("Failed to create second manager: %v", err)
	}

	// Verify policy was loaded
	loadedPolicy, err := mgr2.GetPolicy("reload-policy")
	if err != nil {
		t.Fatalf("Failed to get loaded policy: %v", err)
	}

	if loadedPolicy.ID != "reload-policy" {
		t.Errorf("Expected policy ID 'reload-policy', got %s", loadedPolicy.ID)
	}

	if loadedPolicy.CheckInterval != 10*time.Minute {
		t.Errorf("Expected 10m interval, got %v", loadedPolicy.CheckInterval)
	}

	if loadedPolicy.ReplicationMode != common.ReplicationModeTransparent {
		t.Errorf("Expected transparent mode, got %s", loadedPolicy.ReplicationMode)
	}
}

// TestTransparentModeEncryption tests syncer creation in transparent mode with encryption
func TestTransparentModeEncryption(t *testing.T) {
	policy := common.ReplicationPolicy{
		ID:              "transparent-encrypt-test",
		SourceBackend:   "local",
		SourceSettings:  map[string]string{"path": "/tmp/src"},
		DestinationBackend: "local",
		DestinationSettings: map[string]string{"path": "/tmp/dst"},
		ReplicationMode: common.ReplicationModeTransparent,
		Encryption: &common.EncryptionPolicy{
			Source: &common.EncryptionConfig{
				Enabled:  true,
				Provider: "test",
			},
			Destination: &common.EncryptionConfig{
				Enabled:  true,
				Provider: "test",
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

	// In transparent mode, encryption should be applied
	// Just verify syncer was created successfully
}

// TestBackendEncryptionConfig tests syncer creation with backend encryption
func TestBackendEncryptionConfig(t *testing.T) {
	policy := common.ReplicationPolicy{
		ID:              "backend-encrypt-test",
		SourceBackend:   "local",
		SourceSettings:  map[string]string{"path": "/tmp/src"},
		DestinationBackend: "local",
		DestinationSettings: map[string]string{"path": "/tmp/dst"},
		ReplicationMode: common.ReplicationModeTransparent,
		Encryption: &common.EncryptionPolicy{
			Backend: &common.EncryptionConfig{
				Enabled:  true,
				Provider: "test",
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
}

// TestReplicationStatusWithMetrics tests getting status with all metric fields populated
func TestReplicationStatusWithMetrics(t *testing.T) {
	fs := newMockFileSystem()
	logger := adapters.NewNoOpLogger()
	auditLog := audit.NewNoOpAuditLogger()

	mgr, err := NewPersistentReplicationManager(fs, "test-policies.json", 5*time.Minute, logger, auditLog)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	policy := common.ReplicationPolicy{
		ID:              "metrics-status-test",
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

	// Manually populate some metrics
	metrics := mgr.getOrCreateMetrics("metrics-status-test")
	metrics.IncrementObjectsSynced(100)
	metrics.IncrementObjectsDeleted(10)
	metrics.IncrementBytesSynced(1024 * 1024)
	metrics.IncrementErrors(5)
	metrics.RecordSync(2 * time.Second)

	// Get status
	status, err := mgr.GetReplicationStatus("metrics-status-test")
	if err != nil {
		t.Fatalf("Failed to get status: %v", err)
	}

	// Verify all fields are populated correctly
	if status.TotalObjectsSynced != 100 {
		t.Errorf("Expected 100 objects synced, got %d", status.TotalObjectsSynced)
	}

	if status.TotalObjectsDeleted != 10 {
		t.Errorf("Expected 10 objects deleted, got %d", status.TotalObjectsDeleted)
	}

	if status.TotalBytesSynced != 1024*1024 {
		t.Errorf("Expected 1MB synced, got %d", status.TotalBytesSynced)
	}

	if status.TotalErrors != 5 {
		t.Errorf("Expected 5 errors, got %d", status.TotalErrors)
	}

	if status.SyncCount != 1 {
		t.Errorf("Expected 1 sync, got %d", status.SyncCount)
	}

	if status.AverageSyncDuration != 2*time.Second {
		t.Errorf("Expected 2s average duration, got %v", status.AverageSyncDuration)
	}
}
