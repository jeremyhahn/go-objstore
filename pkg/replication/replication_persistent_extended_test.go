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
	"encoding/json"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/audit"
	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// TestSyncPolicy_Success tests successful policy synchronization.
func TestSyncPolicy_Success(t *testing.T) {
	fs := newMockFileSystem()
	logger := adapters.NewNoOpLogger()
	auditLog := audit.NewNoOpAuditLogger()

	mgr, err := NewPersistentReplicationManager(fs, "test-policies.json", 5*time.Minute, logger, auditLog)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	// Add a policy with local source and destination
	policy := common.ReplicationPolicy{
		ID:                  "policy1",
		SourceBackend:       "local",
		SourceSettings:      map[string]string{"path": t.TempDir()},
		DestinationBackend:  "local",
		DestinationSettings: map[string]string{"path": t.TempDir()},
		CheckInterval:       5 * time.Minute,
		Enabled:             true,
		ReplicationMode:     common.ReplicationModeOpaque,
	}

	err = mgr.AddPolicy(policy)
	if err != nil {
		t.Fatalf("Failed to add policy: %v", err)
	}

	// Create source directory with a test file
	sourceDir := policy.SourceSettings["path"]
	testFile := sourceDir + "/test.txt"
	err = os.WriteFile(testFile, []byte("test content"), 0644)
	if err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}

	// Sync the policy
	result, err := mgr.SyncPolicy(context.Background(), "policy1")
	if err != nil {
		t.Fatalf("SyncPolicy failed: %v", err)
	}

	if result == nil {
		t.Fatal("Expected non-nil result")
	}

	// Verify policy last sync time was updated
	updatedPolicy, err := mgr.GetPolicy("policy1")
	if err != nil {
		t.Fatalf("Failed to get updated policy: %v", err)
	}

	if updatedPolicy.LastSyncTime.IsZero() {
		t.Error("Expected LastSyncTime to be set")
	}
}

// TestSyncPolicy_NotFound tests syncing a non-existent policy.
func TestSyncPolicy_NotFound(t *testing.T) {
	fs := newMockFileSystem()
	logger := adapters.NewNoOpLogger()
	auditLog := audit.NewNoOpAuditLogger()

	mgr, err := NewPersistentReplicationManager(fs, "test-policies.json", 5*time.Minute, logger, auditLog)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	_, err = mgr.SyncPolicy(context.Background(), "nonexistent")
	if !errors.Is(err, common.ErrPolicyNotFound) {
		t.Errorf("Expected ErrPolicyNotFound, got %v", err)
	}
}

// TestSyncAll_MultipleEnabled tests syncing multiple enabled policies.
func TestSyncAll_MultipleEnabled(t *testing.T) {
	fs := newMockFileSystem()
	logger := adapters.NewNoOpLogger()
	auditLog := audit.NewNoOpAuditLogger()

	mgr, err := NewPersistentReplicationManager(fs, "test-policies.json", 5*time.Minute, logger, auditLog)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	// Add multiple enabled policies
	for i := 1; i <= 3; i++ {
		policy := common.ReplicationPolicy{
			ID:                  "policy" + string(rune('0'+i)),
			SourceBackend:       "local",
			SourceSettings:      map[string]string{"path": t.TempDir()},
			DestinationBackend:  "local",
			DestinationSettings: map[string]string{"path": t.TempDir()},
			CheckInterval:       5 * time.Minute,
			Enabled:             true,
			ReplicationMode:     common.ReplicationModeOpaque,
		}

		err = mgr.AddPolicy(policy)
		if err != nil {
			t.Fatalf("Failed to add policy%d: %v", i, err)
		}
	}

	// Sync all policies
	result, err := mgr.SyncAll(context.Background())
	if err != nil {
		t.Fatalf("SyncAll failed: %v", err)
	}

	if result == nil {
		t.Fatal("Expected non-nil result")
	}

	if result.PolicyID != "all" {
		t.Errorf("Expected PolicyID 'all', got '%s'", result.PolicyID)
	}
}

// TestSyncAll_SomeDisabled tests that disabled policies are skipped.
func TestSyncAll_SomeDisabled(t *testing.T) {
	fs := newMockFileSystem()
	logger := adapters.NewNoOpLogger()
	auditLog := audit.NewNoOpAuditLogger()

	mgr, err := NewPersistentReplicationManager(fs, "test-policies.json", 5*time.Minute, logger, auditLog)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	// Add enabled policy
	policy1 := common.ReplicationPolicy{
		ID:                  "policy1",
		SourceBackend:       "local",
		SourceSettings:      map[string]string{"path": t.TempDir()},
		DestinationBackend:  "local",
		DestinationSettings: map[string]string{"path": t.TempDir()},
		CheckInterval:       5 * time.Minute,
		Enabled:             true,
		ReplicationMode:     common.ReplicationModeOpaque,
	}

	err = mgr.AddPolicy(policy1)
	if err != nil {
		t.Fatalf("Failed to add policy1: %v", err)
	}

	// Add disabled policy
	policy2 := common.ReplicationPolicy{
		ID:                  "policy2",
		SourceBackend:       "local",
		SourceSettings:      map[string]string{"path": t.TempDir()},
		DestinationBackend:  "local",
		DestinationSettings: map[string]string{"path": t.TempDir()},
		CheckInterval:       5 * time.Minute,
		Enabled:             false,
		ReplicationMode:     common.ReplicationModeOpaque,
	}

	err = mgr.AddPolicy(policy2)
	if err != nil {
		t.Fatalf("Failed to add policy2: %v", err)
	}

	// Sync all - should only sync enabled policy
	result, err := mgr.SyncAll(context.Background())
	if err != nil {
		t.Fatalf("SyncAll failed: %v", err)
	}

	if result == nil {
		t.Fatal("Expected non-nil result")
	}
}

// TestRun_ContextCancellation tests that Run stops when context is cancelled.
func TestRun_ContextCancellation(t *testing.T) {
	fs := newMockFileSystem()
	logger := adapters.NewNoOpLogger()
	auditLog := audit.NewNoOpAuditLogger()

	mgr, err := NewPersistentReplicationManager(fs, "test-policies.json", 100*time.Millisecond, logger, auditLog)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan bool)
	go func() {
		mgr.Run(ctx)
		done <- true
	}()

	// Cancel after a short delay
	time.Sleep(200 * time.Millisecond)
	cancel()

	// Wait for Run to complete
	select {
	case <-done:
		t.Log("Run stopped after context cancellation")
	case <-time.After(2 * time.Second):
		t.Error("Run did not stop in time after context cancellation")
	}
}

// TestRun_SyncErrors tests that Run handles sync errors gracefully.
func TestRun_SyncErrors(t *testing.T) {
	fs := newMockFileSystem()
	logger := adapters.NewNoOpLogger()
	auditLog := audit.NewNoOpAuditLogger()

	mgr, err := NewPersistentReplicationManager(fs, "test-policies.json", 50*time.Millisecond, logger, auditLog)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	// Add a policy that will fail (invalid backend)
	policy := common.ReplicationPolicy{
		ID:                  "failing-policy",
		SourceBackend:       "nonexistent",
		SourceSettings:      map[string]string{"path": "/invalid"},
		DestinationBackend:  "nonexistent",
		DestinationSettings: map[string]string{"path": "/invalid"},
		CheckInterval:       5 * time.Minute,
		Enabled:             true,
		ReplicationMode:     common.ReplicationModeOpaque,
	}

	err = mgr.AddPolicy(policy)
	if err != nil {
		t.Fatalf("Failed to add policy: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	done := make(chan bool)
	go func() {
		mgr.Run(ctx)
		done <- true
	}()

	// Wait for Run to complete
	<-done

	// Run should handle errors gracefully without panicking
	t.Log("Run handled sync errors gracefully")
}

// TestGetFactories_AllNil tests getFactories when no factories are set.
func TestGetFactories_AllNil(t *testing.T) {
	fs := newMockFileSystem()
	logger := adapters.NewNoOpLogger()
	auditLog := audit.NewNoOpAuditLogger()

	mgr, err := NewPersistentReplicationManager(fs, "test-policies.json", 5*time.Minute, logger, auditLog)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	policy := common.ReplicationPolicy{
		ID:                  "policy1",
		SourceBackend:       "local",
		SourceSettings:      map[string]string{"path": t.TempDir()},
		DestinationBackend:  "local",
		DestinationSettings: map[string]string{"path": t.TempDir()},
		CheckInterval:       5 * time.Minute,
		Enabled:             true,
		ReplicationMode:     common.ReplicationModeOpaque,
	}

	err = mgr.AddPolicy(policy)
	if err != nil {
		t.Fatalf("Failed to add policy: %v", err)
	}

	// Get factories without setting any - should return noop factories
	backend, source, dest := mgr.getFactories("policy1")

	if backend == nil {
		t.Error("Expected non-nil backend factory")
	}

	if source == nil {
		t.Error("Expected non-nil source factory")
	}

	if dest == nil {
		t.Error("Expected non-nil dest factory")
	}

	// Verify they are noop factories
	if backend.DefaultKeyID() != "" {
		t.Error("Expected noop backend factory")
	}

	if source.DefaultKeyID() != "" {
		t.Error("Expected noop source factory")
	}

	if dest.DefaultKeyID() != "" {
		t.Error("Expected noop dest factory")
	}
}

// TestGetFactories_Mixed tests getFactories with some factories set.
func TestGetFactories_Mixed(t *testing.T) {
	fs := newMockFileSystem()
	logger := adapters.NewNoOpLogger()
	auditLog := audit.NewNoOpAuditLogger()

	mgr, err := NewPersistentReplicationManager(fs, "test-policies.json", 5*time.Minute, logger, auditLog)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	policy := common.ReplicationPolicy{
		ID:                  "policy1",
		SourceBackend:       "local",
		SourceSettings:      map[string]string{"path": t.TempDir()},
		DestinationBackend:  "local",
		DestinationSettings: map[string]string{"path": t.TempDir()},
		CheckInterval:       5 * time.Minute,
		Enabled:             true,
		ReplicationMode:     common.ReplicationModeOpaque,
	}

	err = mgr.AddPolicy(policy)
	if err != nil {
		t.Fatalf("Failed to add policy: %v", err)
	}

	// Set only backend factory
	backendFactory := NewNoopEncrypterFactory()
	err = mgr.SetBackendEncrypterFactory("policy1", backendFactory)
	if err != nil {
		t.Fatalf("Failed to set backend factory: %v", err)
	}

	// Get factories - backend should be set, others should be noop
	backend, source, dest := mgr.getFactories("policy1")

	if backend == nil {
		t.Error("Expected non-nil backend factory")
	}

	if source == nil {
		t.Error("Expected non-nil source factory")
	}

	if dest == nil {
		t.Error("Expected non-nil dest factory")
	}
}

// TestLoad_EmptyFile tests loading from an empty policy file.
func TestLoad_EmptyFile(t *testing.T) {
	fs := newMockFileSystem()

	// Create an empty file
	file, _ := fs.OpenFile("test-policies.json", os.O_CREATE|os.O_RDWR, 0600)
	file.Write([]byte(""))
	file.Close()

	logger := adapters.NewNoOpLogger()
	auditLog := audit.NewNoOpAuditLogger()

	// This should fail with JSON unmarshal error
	_, err := NewPersistentReplicationManager(fs, "test-policies.json", 5*time.Minute, logger, auditLog)
	if err == nil {
		t.Error("Expected error when loading empty file")
	}
}

// TestSave_FileSystemError tests save failure due to filesystem error.
func TestSave_FileSystemError(t *testing.T) {
	// Create a filesystem that allows initial load, then fails on save
	fs := newMockFileSystem()

	logger := adapters.NewNoOpLogger()
	auditLog := audit.NewNoOpAuditLogger()

	mgr, err := NewPersistentReplicationManager(fs, "test-policies.json", 5*time.Minute, logger, auditLog)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	
	// Now replace filesystem with one that fails
	mgr.fs = &errorFileSystem{
		openError: errors.New("filesystem error"),
	}

	policy := common.ReplicationPolicy{
		ID:                  "policy1",
		SourceBackend:       "local",
		SourceSettings:      map[string]string{"path": "/source"},
		DestinationBackend:  "local",
		DestinationSettings: map[string]string{"path": "/dest"},
		CheckInterval:       5 * time.Minute,
		Enabled:             true,
		ReplicationMode:     common.ReplicationModeOpaque,
	}

	// This should fail when trying to save
	err = mgr.AddPolicy(policy)
	if err == nil {
		t.Error("Expected error when filesystem fails")
	}
}

// TestAddPolicy_SaveFailure tests that AddPolicy fails when save fails.
func TestAddPolicy_SaveFailure(t *testing.T) {
	fs := newMockFileSystem()
	logger := adapters.NewNoOpLogger()
	auditLog := audit.NewNoOpAuditLogger()

	mgr, err := NewPersistentReplicationManager(fs, "test-policies.json", 5*time.Minute, logger, auditLog)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	// Replace filesystem with one that fails on write
	mgr.fs = &errorFileSystem{
		openError: errors.New("write error"),
	}

	policy := common.ReplicationPolicy{
		ID:                  "policy1",
		SourceBackend:       "local",
		SourceSettings:      map[string]string{"path": "/source"},
		DestinationBackend:  "local",
		DestinationSettings: map[string]string{"path": "/dest"},
		CheckInterval:       5 * time.Minute,
		Enabled:             true,
		ReplicationMode:     common.ReplicationModeOpaque,
	}

	err = mgr.AddPolicy(policy)
	if err == nil {
		t.Error("Expected error when save fails")
	}
}

// TestRemovePolicy_SaveFailure tests that RemovePolicy fails when save fails.
func TestRemovePolicy_SaveFailure(t *testing.T) {
	fs := newMockFileSystem()
	logger := adapters.NewNoOpLogger()
	auditLog := audit.NewNoOpAuditLogger()

	mgr, err := NewPersistentReplicationManager(fs, "test-policies.json", 5*time.Minute, logger, auditLog)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	policy := common.ReplicationPolicy{
		ID:                  "policy1",
		SourceBackend:       "local",
		SourceSettings:      map[string]string{"path": "/source"},
		DestinationBackend:  "local",
		DestinationSettings: map[string]string{"path": "/dest"},
		CheckInterval:       5 * time.Minute,
		Enabled:             true,
		ReplicationMode:     common.ReplicationModeOpaque,
	}

	err = mgr.AddPolicy(policy)
	if err != nil {
		t.Fatalf("Failed to add policy: %v", err)
	}

	// Replace filesystem with one that fails on write
	mgr.fs = &errorFileSystem{
		openError: errors.New("write error"),
	}

	err = mgr.RemovePolicy("policy1")
	if err == nil {
		t.Error("Expected error when save fails")
	}
}

// TestLoad_WithNilPolicies tests loading a file with null policies map.
func TestLoad_WithNilPolicies(t *testing.T) {
	fs := newMockFileSystem()

	// Create a file with null policies
	data := persistedPolicies{
		Policies: nil,
	}
	jsonData, _ := json.MarshalIndent(data, "", "  ")

	file, _ := fs.OpenFile("test-policies.json", os.O_CREATE|os.O_RDWR, 0600)
	file.Write(jsonData)
	file.Close()

	logger := adapters.NewNoOpLogger()
	auditLog := audit.NewNoOpAuditLogger()

	mgr, err := NewPersistentReplicationManager(fs, "test-policies.json", 5*time.Minute, logger, auditLog)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	// Should have initialized empty map
	policies, err := mgr.GetPolicies()
	if err != nil {
		t.Fatalf("Failed to get policies: %v", err)
	}

	if policies == nil {
		t.Error("Expected non-nil policies slice")
	}

	if len(policies) != 0 {
		t.Errorf("Expected 0 policies, got %d", len(policies))
	}
}

// TestSyncAll_WithErrors tests that SyncAll aggregates errors from failed syncs.
func TestSyncAll_WithErrors(t *testing.T) {
	fs := newMockFileSystem()
	logger := adapters.NewNoOpLogger()
	auditLog := audit.NewNoOpAuditLogger()

	mgr, err := NewPersistentReplicationManager(fs, "test-policies.json", 5*time.Minute, logger, auditLog)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	// Add a policy with invalid backend that will fail
	policy1 := common.ReplicationPolicy{
		ID:                  "policy1",
		SourceBackend:       "invalid-backend",
		SourceSettings:      map[string]string{"path": "/invalid"},
		DestinationBackend:  "invalid-backend",
		DestinationSettings: map[string]string{"path": "/invalid"},
		CheckInterval:       5 * time.Minute,
		Enabled:             true,
		ReplicationMode:     common.ReplicationModeOpaque,
	}

	err = mgr.AddPolicy(policy1)
	if err != nil {
		t.Fatalf("Failed to add policy1: %v", err)
	}

	// Add another failing policy
	policy2 := common.ReplicationPolicy{
		ID:                  "policy2",
		SourceBackend:       "invalid-backend",
		SourceSettings:      map[string]string{"path": "/invalid"},
		DestinationBackend:  "invalid-backend",
		DestinationSettings: map[string]string{"path": "/invalid"},
		CheckInterval:       5 * time.Minute,
		Enabled:             true,
		ReplicationMode:     common.ReplicationModeOpaque,
	}

	err = mgr.AddPolicy(policy2)
	if err != nil {
		t.Fatalf("Failed to add policy2: %v", err)
	}

	// Sync all - should return result with errors
	result, err := mgr.SyncAll(context.Background())
	if err != nil {
		t.Fatalf("SyncAll should not return error: %v", err)
	}

	if result.Failed == 0 {
		t.Error("Expected failed count > 0")
	}

	if len(result.Errors) == 0 {
		t.Error("Expected errors to be recorded")
	}
}

// errorFileSystem is a mock filesystem that returns errors.
type errorFileSystem struct {
	openError   error
	removeError error
}

func (e *errorFileSystem) OpenFile(name string, flag int, perm os.FileMode) (ReplicationFile, error) {
	if e.openError != nil {
		return nil, e.openError
	}
	return nil, nil
}

func (e *errorFileSystem) Remove(name string) error {
	if e.removeError != nil {
		return e.removeError
	}
	return nil
}

// TestOSFileSystem_Remove tests the real OS filesystem Remove method.
func TestOSFileSystem_Remove(t *testing.T) {
	fs := &OSFileSystem{}

	// Create a temp file
	tmpFile := t.TempDir() + "/test.txt"
	err := os.WriteFile(tmpFile, []byte("test"), 0644)
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}

	// Remove the file
	err = fs.Remove(tmpFile)
	if err != nil {
		t.Errorf("Remove failed: %v", err)
	}

	// Verify file is gone
	_, err = os.Stat(tmpFile)
	if !os.IsNotExist(err) {
		t.Error("File should not exist after Remove")
	}
}

// TestMultipleManagersConcurrent tests multiple managers operating concurrently on different files.
func TestMultipleManagersConcurrent(t *testing.T) {
	var wg sync.WaitGroup

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			fs := newMockFileSystem()
			logger := adapters.NewNoOpLogger()
			auditLog := audit.NewNoOpAuditLogger()

			mgr, err := NewPersistentReplicationManager(fs, "test-policies.json", 5*time.Minute, logger, auditLog)
			if err != nil {
				t.Errorf("Manager %d: Failed to create: %v", id, err)
				return
			}

			policy := common.ReplicationPolicy{
				ID:                  "policy1",
				SourceBackend:       "local",
				SourceSettings:      map[string]string{"path": "/source"},
				DestinationBackend:  "local",
				DestinationSettings: map[string]string{"path": "/dest"},
				CheckInterval:       5 * time.Minute,
				Enabled:             true,
				ReplicationMode:     common.ReplicationModeOpaque,
			}

			err = mgr.AddPolicy(policy)
			if err != nil {
				t.Errorf("Manager %d: Failed to add policy: %v", id, err)
			}
		}(i)
	}

	wg.Wait()
}
