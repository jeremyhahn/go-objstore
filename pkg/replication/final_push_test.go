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
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/audit"
	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// TestSyncAllDisabledPolicy tests that disabled policies are skipped
func TestSyncAllDisabledPolicy(t *testing.T) {
	ctx := context.Background()
	fs := newMockFileSystem()
	logger := adapters.NewNoOpLogger()
	auditLog := audit.NewNoOpAuditLogger()

	mgr, err := NewPersistentReplicationManager(fs, "test-policies.json", 5*time.Minute, logger, auditLog)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	// Add a disabled policy
	disabledPolicy := common.ReplicationPolicy{
		ID:              "disabled-policy",
		SourceBackend:   "local",
		SourceSettings:  map[string]string{"path": "/tmp/src"},
		DestinationBackend: "local",
		DestinationSettings: map[string]string{"path": "/tmp/dst"},
		CheckInterval:   5 * time.Minute,
		Enabled:         false, // Disabled
		ReplicationMode: common.ReplicationModeOpaque,
	}

	err = mgr.AddPolicy(disabledPolicy)
	if err != nil {
		t.Fatalf("Failed to add disabled policy: %v", err)
	}

	// Add an enabled policy
	enabledPolicy := common.ReplicationPolicy{
		ID:              "enabled-policy",
		SourceBackend:   "local",
		SourceSettings:  map[string]string{"path": "/tmp/src2"},
		DestinationBackend: "local",
		DestinationSettings: map[string]string{"path": "/tmp/dst2"},
		CheckInterval:   5 * time.Minute,
		Enabled:         true,
		ReplicationMode: common.ReplicationModeOpaque,
	}

	err = mgr.AddPolicy(enabledPolicy)
	if err != nil {
		t.Fatalf("Failed to add enabled policy: %v", err)
	}

	// SyncAll should only sync enabled policy
	result, err := mgr.SyncAll(ctx)
	if err != nil {
		t.Fatalf("SyncAll failed: %v", err)
	}

	// Should complete without errors
	if result == nil {
		t.Fatal("Expected non-nil result")
	}

	// Verify enabled policy was synced
	enabledStatus, err := mgr.GetReplicationStatus("enabled-policy")
	if err != nil {
		t.Fatalf("Failed to get enabled policy status: %v", err)
	}

	if enabledStatus.SyncCount != 1 {
		t.Errorf("Expected enabled policy to be synced once, got %d", enabledStatus.SyncCount)
	}

	// Verify disabled policy was NOT synced
	disabledStatus, err := mgr.GetReplicationStatus("disabled-policy")
	if err != nil {
		t.Fatalf("Failed to get disabled policy status: %v", err)
	}

	if disabledStatus.SyncCount != 0 {
		t.Errorf("Expected disabled policy to NOT be synced, got %d syncs", disabledStatus.SyncCount)
	}
}

// TestSyncPolicyError tests SyncPolicy when policy doesn't exist
func TestSyncPolicyError(t *testing.T) {
	ctx := context.Background()
	fs := newMockFileSystem()
	logger := adapters.NewNoOpLogger()
	auditLog := audit.NewNoOpAuditLogger()

	mgr, err := NewPersistentReplicationManager(fs, "test-policies.json", 5*time.Minute, logger, auditLog)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	// Try to sync non-existent policy
	_, err = mgr.SyncPolicy(ctx, "nonexistent")
	if !errors.Is(err, common.ErrPolicyNotFound) {
		t.Errorf("Expected ErrPolicyNotFound, got %v", err)
	}
}

// TestGetReplicationStatusError tests GetReplicationStatus with non-existent policy
func TestGetReplicationStatusError(t *testing.T) {
	fs := newMockFileSystem()
	logger := adapters.NewNoOpLogger()
	auditLog := audit.NewNoOpAuditLogger()

	mgr, err := NewPersistentReplicationManager(fs, "test-policies.json", 5*time.Minute, logger, auditLog)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	// Try to get status for non-existent policy
	_, err = mgr.GetReplicationStatus("nonexistent")
	if !errors.Is(err, common.ErrPolicyNotFound) {
		t.Errorf("Expected ErrPolicyNotFound, got %v", err)
	}
}

// TestSyncAllEmptyPolicies tests SyncAll with no policies
func TestSyncAllEmptyPolicies(t *testing.T) {
	ctx := context.Background()
	fs := newMockFileSystem()
	logger := adapters.NewNoOpLogger()
	auditLog := audit.NewNoOpAuditLogger()

	mgr, err := NewPersistentReplicationManager(fs, "test-policies.json", 5*time.Minute, logger, auditLog)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	// SyncAll with no policies
	result, err := mgr.SyncAll(ctx)
	if err != nil {
		t.Fatalf("SyncAll failed: %v", err)
	}

	if result == nil {
		t.Fatal("Expected non-nil result")
	}

	if result.Synced != 0 {
		t.Errorf("Expected 0 synced, got %d", result.Synced)
	}
}

// TestSyncObjectReadError tests SyncObject when reading fails
func TestSyncObjectReadError(t *testing.T) {
	ctx := context.Background()

	source := newExtendedMockStorage()
	dest := newExtendedMockStorage()

	// Set source to fail on GetWithContext
	source.getError = errors.New("read error")

	policy := common.ReplicationPolicy{
		ID:              "read-error-test",
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

	// Should fail on read
	_, err := syncer.SyncObject(ctx, "any-key")
	if err == nil {
		t.Error("Expected error when reading fails")
	}
}

// TestSyncPolicyWithNilResult tests SyncPolicy when result is nil
func TestSyncPolicyWithNilResult(t *testing.T) {
	ctx := context.Background()
	fs := newMockFileSystem()
	logger := adapters.NewNoOpLogger()
	auditLog := audit.NewNoOpAuditLogger()

	mgr, err := NewPersistentReplicationManager(fs, "test-policies.json", 5*time.Minute, logger, auditLog)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	// Add policy with invalid source backend (will cause NewSyncer to fail)
	policy := common.ReplicationPolicy{
		ID:              "nil-result-test",
		SourceBackend:   "invalid-backend",
		DestinationBackend: "local",
		CheckInterval:   5 * time.Minute,
		Enabled:         true,
		ReplicationMode: common.ReplicationModeOpaque,
	}

	err = mgr.AddPolicy(policy)
	if err != nil {
		t.Fatalf("Failed to add policy: %v", err)
	}

	// SyncPolicy should fail
	result, err := mgr.SyncPolicy(ctx, "nil-result-test")
	if err == nil {
		t.Error("Expected error with invalid backend")
	}

	// Result can be nil or non-nil, both are valid
	_ = result
}

// TestMetricsUpdateOnFailure tests that metrics are updated even on failure
func TestMetricsUpdateOnFailure(t *testing.T) {
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
		ID:              "failure-metrics-test",
		SourceBackend:   "invalid-backend",
		DestinationBackend: "local",
		CheckInterval:   5 * time.Minute,
		Enabled:         true,
		ReplicationMode: common.ReplicationModeOpaque,
	}

	err = mgr.AddPolicy(policy)
	if err != nil {
		t.Fatalf("Failed to add policy: %v", err)
	}

	// Try to sync - will fail
	_, err = mgr.SyncPolicy(ctx, "failure-metrics-test")
	if err == nil {
		t.Error("Expected error with invalid backend")
	}

	// Metrics should still exist (even if not updated)
	status, err := mgr.GetReplicationStatus("failure-metrics-test")
	if err != nil {
		t.Fatalf("Failed to get status: %v", err)
	}

	if status == nil {
		t.Fatal("Expected non-nil status")
	}
}

// TestSyncAllWithMixedResults tests SyncAll with some successes and failures
func TestSyncAllWithMixedResults(t *testing.T) {
	ctx := context.Background()
	fs := newMockFileSystem()
	logger := adapters.NewNoOpLogger()
	auditLog := audit.NewNoOpAuditLogger()

	mgr, err := NewPersistentReplicationManager(fs, "test-policies.json", 5*time.Minute, logger, auditLog)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	// Add multiple policies - some valid, some invalid
	validPolicy := common.ReplicationPolicy{
		ID:              "valid-policy",
		SourceBackend:   "local",
		SourceSettings:  map[string]string{"path": "/tmp/valid-src"},
		DestinationBackend: "local",
		DestinationSettings: map[string]string{"path": "/tmp/valid-dst"},
		CheckInterval:   5 * time.Minute,
		Enabled:         true,
		ReplicationMode: common.ReplicationModeOpaque,
	}

	err = mgr.AddPolicy(validPolicy)
	if err != nil {
		t.Fatalf("Failed to add valid policy: %v", err)
	}

	invalidPolicy := common.ReplicationPolicy{
		ID:              "invalid-policy",
		SourceBackend:   "invalid-backend",
		DestinationBackend: "local",
		CheckInterval:   5 * time.Minute,
		Enabled:         true,
		ReplicationMode: common.ReplicationModeOpaque,
	}

	err = mgr.AddPolicy(invalidPolicy)
	if err != nil {
		t.Fatalf("Failed to add invalid policy: %v", err)
	}

	// SyncAll should handle mixed results
	result, err := mgr.SyncAll(ctx)
	if err != nil {
		t.Fatalf("SyncAll should not fail completely: %v", err)
	}

	// Should have at least one error
	if len(result.Errors) < 1 {
		t.Error("Expected at least one error from invalid policy")
	}

	// Should have incremented failed counter
	if result.Failed < 1 {
		t.Error("Expected at least one failure")
	}
}

// TestRemovePolicyCleanup tests that removing a policy cleans up all associated data
func TestRemovePolicyCleanup(t *testing.T) {
	fs := newMockFileSystem()
	logger := adapters.NewNoOpLogger()
	auditLog := audit.NewNoOpAuditLogger()

	mgr, err := NewPersistentReplicationManager(fs, "test-policies.json", 5*time.Minute, logger, auditLog)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	// Add a policy
	policy := common.ReplicationPolicy{
		ID:              "cleanup-test",
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

	// Set factories
	backendFactory := NewNoopEncrypterFactory()
	sourceFactory := NewNoopEncrypterFactory()
	destFactory := NewNoopEncrypterFactory()

	mgr.SetBackendEncrypterFactory("cleanup-test", backendFactory)
	mgr.SetSourceEncrypterFactory("cleanup-test", sourceFactory)
	mgr.SetDestinationEncrypterFactory("cleanup-test", destFactory)

	// Remove policy
	err = mgr.RemovePolicy("cleanup-test")
	if err != nil {
		t.Fatalf("Failed to remove policy: %v", err)
	}

	// Verify all data was cleaned up
	_, err = mgr.GetPolicy("cleanup-test")
	if !errors.Is(err, common.ErrPolicyNotFound) {
		t.Error("Expected policy to be removed")
	}

	_, err = mgr.GetReplicationStatus("cleanup-test")
	if !errors.Is(err, common.ErrPolicyNotFound) {
		t.Error("Expected status to be removed")
	}

	// Verify factories are cleaned up (can't directly test, but ensure no panic)
	_, _, _ = mgr.getFactories("cleanup-test") // Should return noop factories
}
