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
	"encoding/json"
	"errors"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/audit"
	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// mockFile implements ReplicationFile for testing.
type mockFile struct {
	data   *[]byte // Pointer to shared data
	offset int64
	closed bool
	mu     sync.Mutex
}

func newMockFile(data *[]byte) *mockFile {
	return &mockFile{
		data:   data,
		offset: 0,
		closed: false,
	}
}

func (m *mockFile) Read(p []byte) (n int, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return 0, os.ErrClosed
	}

	if m.offset >= int64(len(*m.data)) {
		return 0, io.EOF
	}

	n = copy(p, (*m.data)[m.offset:])
	m.offset += int64(n)
	return n, nil
}

func (m *mockFile) Write(p []byte) (n int, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return 0, os.ErrClosed
	}

	if m.offset == int64(len(*m.data)) {
		*m.data = append(*m.data, p...)
	} else {
		// Overwrite at current position
		for i, b := range p {
			if int64(i)+m.offset < int64(len(*m.data)) {
				(*m.data)[int64(i)+m.offset] = b
			} else {
				*m.data = append(*m.data, b)
			}
		}
	}

	n = len(p)
	m.offset += int64(n)
	return n, nil
}

func (m *mockFile) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.closed = true
	return nil
}

func (m *mockFile) Seek(offset int64, whence int) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return 0, os.ErrClosed
	}

	switch whence {
	case io.SeekStart:
		m.offset = offset
	case io.SeekCurrent:
		m.offset += offset
	case io.SeekEnd:
		m.offset = int64(len(*m.data)) + offset
	default:
		return 0, errors.New("invalid whence")
	}

	return m.offset, nil
}

func (m *mockFile) Truncate(size int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return os.ErrClosed
	}

	if size < int64(len(*m.data)) {
		*m.data = (*m.data)[:size]
	} else {
		*m.data = append(*m.data, make([]byte, size-int64(len(*m.data)))...)
	}

	return nil
}

func (m *mockFile) Sync() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return os.ErrClosed
	}

	return nil
}

// mockFileSystem implements FileSystem for testing.
type mockFileSystem struct {
	files map[string]*[]byte // Store data separately from file handles
	mu    sync.RWMutex
}

func newMockFileSystem() *mockFileSystem {
	return &mockFileSystem{
		files: make(map[string]*[]byte),
	}
}

func (mfs *mockFileSystem) OpenFile(name string, flag int, perm os.FileMode) (ReplicationFile, error) {
	mfs.mu.Lock()
	defer mfs.mu.Unlock()

	// Handle read-only mode
	if flag&os.O_RDONLY != 0 {
		data, exists := mfs.files[name]
		if !exists {
			return nil, os.ErrNotExist
		}
		// Create new file handle for reading (not reusing old handle)
		return newMockFile(data), nil
	}

	// Handle write mode (create or truncate)
	if flag&os.O_CREATE != 0 {
		if flag&os.O_TRUNC != 0 {
			// Create new or truncate existing
			data := make([]byte, 0)
			mfs.files[name] = &data
			return newMockFile(&data), nil
		}
		// Create only if doesn't exist
		if _, exists := mfs.files[name]; !exists {
			data := make([]byte, 0)
			mfs.files[name] = &data
			return newMockFile(&data), nil
		}
		// File exists, return handle to existing data
		return newMockFile(mfs.files[name]), nil
	}

	// If file doesn't exist and O_CREATE not specified
	data, exists := mfs.files[name]
	if !exists {
		return nil, os.ErrNotExist
	}

	// Truncate if requested
	if flag&os.O_TRUNC != 0 {
		*data = make([]byte, 0)
	}

	return newMockFile(data), nil
}

func (mfs *mockFileSystem) Remove(name string) error {
	mfs.mu.Lock()
	defer mfs.mu.Unlock()

	if _, exists := mfs.files[name]; !exists {
		return os.ErrNotExist
	}

	delete(mfs.files, name)
	return nil
}

func (mfs *mockFileSystem) fileExists(name string) bool {
	mfs.mu.RLock()
	defer mfs.mu.RUnlock()

	_, exists := mfs.files[name]
	return exists
}

func (mfs *mockFileSystem) readFile(name string) ([]byte, error) {
	mfs.mu.RLock()
	defer mfs.mu.RUnlock()

	data, exists := mfs.files[name]
	if !exists {
		return nil, os.ErrNotExist
	}

	return append([]byte(nil), *data...), nil
}

// TestAddPolicy tests adding a replication policy.
func TestAddPolicy(t *testing.T) {
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
		ReplicationMode:     common.ReplicationModeTransparent,
	}

	// Add policy
	err = mgr.AddPolicy(policy)
	if err != nil {
		t.Fatalf("Failed to add policy: %v", err)
	}

	// Verify policy was added
	retrieved, err := mgr.GetPolicy("policy1")
	if err != nil {
		t.Fatalf("Failed to get policy: %v", err)
	}

	if retrieved.ID != "policy1" {
		t.Errorf("Expected policy ID 'policy1', got %s", retrieved.ID)
	}

	// Verify persistence
	if !fs.fileExists("test-policies.json") {
		t.Error("Policy file was not created")
	}
}

// TestAddPolicyInvalidID tests adding a policy with an empty ID.
func TestAddPolicyInvalidID(t *testing.T) {
	fs := newMockFileSystem()
	logger := adapters.NewNoOpLogger()
	auditLog := audit.NewNoOpAuditLogger()

	mgr, err := NewPersistentReplicationManager(fs, "test-policies.json", 5*time.Minute, logger, auditLog)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	policy := common.ReplicationPolicy{
		ID:              "",
		SourceBackend:   "local",
		Enabled:         true,
		CheckInterval:   5 * time.Minute,
		ReplicationMode: common.ReplicationModeTransparent,
	}

	err = mgr.AddPolicy(policy)
	if !errors.Is(err, common.ErrInvalidPolicy) {
		t.Errorf("Expected ErrInvalidPolicy, got %v", err)
	}
}

// TestRemovePolicy tests removing a replication policy.
func TestRemovePolicy(t *testing.T) {
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
		ReplicationMode:     common.ReplicationModeTransparent,
	}

	// Add policy
	err = mgr.AddPolicy(policy)
	if err != nil {
		t.Fatalf("Failed to add policy: %v", err)
	}

	// Remove policy
	err = mgr.RemovePolicy("policy1")
	if err != nil {
		t.Fatalf("Failed to remove policy: %v", err)
	}

	// Verify policy was removed
	_, err = mgr.GetPolicy("policy1")
	if !errors.Is(err, common.ErrPolicyNotFound) {
		t.Errorf("Expected ErrPolicyNotFound, got %v", err)
	}
}

// TestRemovePolicyNotFound tests removing a non-existent policy.
func TestRemovePolicyNotFound(t *testing.T) {
	fs := newMockFileSystem()
	logger := adapters.NewNoOpLogger()
	auditLog := audit.NewNoOpAuditLogger()

	mgr, err := NewPersistentReplicationManager(fs, "test-policies.json", 5*time.Minute, logger, auditLog)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	err = mgr.RemovePolicy("nonexistent")
	if !errors.Is(err, common.ErrPolicyNotFound) {
		t.Errorf("Expected ErrPolicyNotFound, got %v", err)
	}
}

// TestGetPolicy tests retrieving a replication policy.
func TestGetPolicy(t *testing.T) {
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
		ReplicationMode:     common.ReplicationModeTransparent,
	}

	err = mgr.AddPolicy(policy)
	if err != nil {
		t.Fatalf("Failed to add policy: %v", err)
	}

	retrieved, err := mgr.GetPolicy("policy1")
	if err != nil {
		t.Fatalf("Failed to get policy: %v", err)
	}

	if retrieved.ID != "policy1" {
		t.Errorf("Expected policy ID 'policy1', got %s", retrieved.ID)
	}
	if retrieved.SourceBackend != "local" {
		t.Errorf("Expected source backend 'local', got %s", retrieved.SourceBackend)
	}
}

// TestGetPolicyNotFound tests retrieving a non-existent policy.
func TestGetPolicyNotFound(t *testing.T) {
	fs := newMockFileSystem()
	logger := adapters.NewNoOpLogger()
	auditLog := audit.NewNoOpAuditLogger()

	mgr, err := NewPersistentReplicationManager(fs, "test-policies.json", 5*time.Minute, logger, auditLog)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	_, err = mgr.GetPolicy("nonexistent")
	if !errors.Is(err, common.ErrPolicyNotFound) {
		t.Errorf("Expected ErrPolicyNotFound, got %v", err)
	}
}

// TestGetPolicies tests retrieving all replication policies.
func TestGetPolicies(t *testing.T) {
	fs := newMockFileSystem()
	logger := adapters.NewNoOpLogger()
	auditLog := audit.NewNoOpAuditLogger()

	mgr, err := NewPersistentReplicationManager(fs, "test-policies.json", 5*time.Minute, logger, auditLog)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	// Add multiple policies
	for i := 1; i <= 3; i++ {
		policy := common.ReplicationPolicy{
			ID:                  "policy" + string(rune('0'+i)),
			SourceBackend:       "local",
			SourceSettings:      map[string]string{"path": "/source"},
			DestinationBackend:  "local",
			DestinationSettings: map[string]string{"path": "/dest"},
			CheckInterval:       5 * time.Minute,
			Enabled:             true,
			ReplicationMode:     common.ReplicationModeTransparent,
		}

		err = mgr.AddPolicy(policy)
		if err != nil {
			t.Fatalf("Failed to add policy: %v", err)
		}
	}

	policies, err := mgr.GetPolicies()
	if err != nil {
		t.Fatalf("Failed to get policies: %v", err)
	}

	if len(policies) != 3 {
		t.Errorf("Expected 3 policies, got %d", len(policies))
	}
}

// TestGetPoliciesEmpty tests retrieving policies when none exist.
func TestGetPoliciesEmpty(t *testing.T) {
	fs := newMockFileSystem()
	logger := adapters.NewNoOpLogger()
	auditLog := audit.NewNoOpAuditLogger()

	mgr, err := NewPersistentReplicationManager(fs, "test-policies.json", 5*time.Minute, logger, auditLog)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	policies, err := mgr.GetPolicies()
	if err != nil {
		t.Fatalf("Failed to get policies: %v", err)
	}

	if len(policies) != 0 {
		t.Errorf("Expected 0 policies, got %d", len(policies))
	}
}

// TestSetEncrypterFactories tests setting encrypter factories.
func TestSetEncrypterFactories(t *testing.T) {
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
		ReplicationMode:     common.ReplicationModeTransparent,
	}

	err = mgr.AddPolicy(policy)
	if err != nil {
		t.Fatalf("Failed to add policy: %v", err)
	}

	// Create factories
	backendFactory := NewNoopEncrypterFactory()
	sourceFactory := NewNoopEncrypterFactory()
	destFactory := NewNoopEncrypterFactory()

	// Set backend factory
	err = mgr.SetBackendEncrypterFactory("policy1", backendFactory)
	if err != nil {
		t.Errorf("Failed to set backend factory: %v", err)
	}

	// Set source factory
	err = mgr.SetSourceEncrypterFactory("policy1", sourceFactory)
	if err != nil {
		t.Errorf("Failed to set source factory: %v", err)
	}

	// Set destination factory
	err = mgr.SetDestinationEncrypterFactory("policy1", destFactory)
	if err != nil {
		t.Errorf("Failed to set destination factory: %v", err)
	}

	// Test with non-existent policy
	err = mgr.SetBackendEncrypterFactory("nonexistent", backendFactory)
	if !errors.Is(err, common.ErrPolicyNotFound) {
		t.Errorf("Expected ErrPolicyNotFound, got %v", err)
	}

	err = mgr.SetSourceEncrypterFactory("nonexistent", sourceFactory)
	if !errors.Is(err, common.ErrPolicyNotFound) {
		t.Errorf("Expected ErrPolicyNotFound, got %v", err)
	}

	err = mgr.SetDestinationEncrypterFactory("nonexistent", destFactory)
	if !errors.Is(err, common.ErrPolicyNotFound) {
		t.Errorf("Expected ErrPolicyNotFound, got %v", err)
	}
}

// TestSaveLoad tests persistence of policies.
func TestSaveLoad(t *testing.T) {
	fs := newMockFileSystem()
	logger := adapters.NewNoOpLogger()
	auditLog := audit.NewNoOpAuditLogger()

	// Create first manager and add policies
	mgr1, err := NewPersistentReplicationManager(fs, "test-policies.json", 5*time.Minute, logger, auditLog)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	policy1 := common.ReplicationPolicy{
		ID:                  "policy1",
		SourceBackend:       "local",
		SourceSettings:      map[string]string{"path": "/source"},
		DestinationBackend:  "local",
		DestinationSettings: map[string]string{"path": "/dest"},
		CheckInterval:       5 * time.Minute,
		Enabled:             true,
		ReplicationMode:     common.ReplicationModeTransparent,
	}

	policy2 := common.ReplicationPolicy{
		ID:                  "policy2",
		SourceBackend:       "local",
		SourceSettings:      map[string]string{"path": "/source2"},
		DestinationBackend:  "local",
		DestinationSettings: map[string]string{"path": "/dest2"},
		CheckInterval:       10 * time.Minute,
		Enabled:             false,
		ReplicationMode:     common.ReplicationModeOpaque,
	}

	err = mgr1.AddPolicy(policy1)
	if err != nil {
		t.Fatalf("Failed to add policy1: %v", err)
	}

	err = mgr1.AddPolicy(policy2)
	if err != nil {
		t.Fatalf("Failed to add policy2: %v", err)
	}

	// Create second manager with same filesystem (simulates restart)
	mgr2, err := NewPersistentReplicationManager(fs, "test-policies.json", 5*time.Minute, logger, auditLog)
	if err != nil {
		t.Fatalf("Failed to create second manager: %v", err)
	}

	// Verify policies were loaded
	policies, err := mgr2.GetPolicies()
	if err != nil {
		t.Fatalf("Failed to get policies: %v", err)
	}

	if len(policies) != 2 {
		t.Fatalf("Expected 2 policies, got %d", len(policies))
	}

	// Verify policy1
	loaded1, err := mgr2.GetPolicy("policy1")
	if err != nil {
		t.Fatalf("Failed to get policy1: %v", err)
	}

	if loaded1.ID != "policy1" || loaded1.SourceBackend != "local" {
		t.Errorf("Policy1 not loaded correctly")
	}

	// Verify policy2
	loaded2, err := mgr2.GetPolicy("policy2")
	if err != nil {
		t.Fatalf("Failed to get policy2: %v", err)
	}

	if loaded2.ID != "policy2" || loaded2.Enabled != false {
		t.Errorf("Policy2 not loaded correctly")
	}
}

// TestSaveLoadWithEncryption tests persistence with encryption config.
func TestSaveLoadWithEncryption(t *testing.T) {
	fs := newMockFileSystem()
	logger := adapters.NewNoOpLogger()
	auditLog := audit.NewNoOpAuditLogger()

	mgr1, err := NewPersistentReplicationManager(fs, "test-policies.json", 5*time.Minute, logger, auditLog)
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
		ReplicationMode:     common.ReplicationModeTransparent,
		Encryption: &common.EncryptionPolicy{
			Backend: &common.EncryptionConfig{
				Enabled:    true,
				Provider:   "custom",
				DefaultKey: "backend-key",
			},
			Source: &common.EncryptionConfig{
				Enabled:    true,
				Provider:   "custom",
				DefaultKey: "source-key",
			},
			Destination: &common.EncryptionConfig{
				Enabled:    true,
				Provider:   "custom",
				DefaultKey: "dest-key",
			},
		},
	}

	err = mgr1.AddPolicy(policy)
	if err != nil {
		t.Fatalf("Failed to add policy: %v", err)
	}

	// Create second manager (simulate restart)
	mgr2, err := NewPersistentReplicationManager(fs, "test-policies.json", 5*time.Minute, logger, auditLog)
	if err != nil {
		t.Fatalf("Failed to create second manager: %v", err)
	}

	// Verify encryption config was loaded
	loaded, err := mgr2.GetPolicy("policy1")
	if err != nil {
		t.Fatalf("Failed to get policy: %v", err)
	}

	if loaded.Encryption == nil {
		t.Fatal("Encryption config not loaded")
	}

	if loaded.Encryption.Backend == nil || loaded.Encryption.Backend.DefaultKey != "backend-key" {
		t.Error("Backend encryption config not loaded correctly")
	}

	if loaded.Encryption.Source == nil || loaded.Encryption.Source.DefaultKey != "source-key" {
		t.Error("Source encryption config not loaded correctly")
	}

	if loaded.Encryption.Destination == nil || loaded.Encryption.Destination.DefaultKey != "dest-key" {
		t.Error("Destination encryption config not loaded correctly")
	}
}

// TestConcurrentAccess tests concurrent operations on the manager.
func TestConcurrentAccess(t *testing.T) {
	fs := newMockFileSystem()
	logger := adapters.NewNoOpLogger()
	auditLog := audit.NewNoOpAuditLogger()

	mgr, err := NewPersistentReplicationManager(fs, "test-policies.json", 5*time.Minute, logger, auditLog)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	// Add initial policy
	policy := common.ReplicationPolicy{
		ID:                  "policy0",
		SourceBackend:       "local",
		SourceSettings:      map[string]string{"path": "/source"},
		DestinationBackend:  "local",
		DestinationSettings: map[string]string{"path": "/dest"},
		CheckInterval:       5 * time.Minute,
		Enabled:             true,
		ReplicationMode:     common.ReplicationModeTransparent,
	}
	err = mgr.AddPolicy(policy)
	if err != nil {
		t.Fatalf("Failed to add initial policy: %v", err)
	}

	// Concurrently add, get, and remove policies
	var wg sync.WaitGroup
	numGoroutines := 10

	// Add policies
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			p := common.ReplicationPolicy{
				ID:                  "policy" + string(rune('0'+id)),
				SourceBackend:       "local",
				SourceSettings:      map[string]string{"path": "/source"},
				DestinationBackend:  "local",
				DestinationSettings: map[string]string{"path": "/dest"},
				CheckInterval:       5 * time.Minute,
				Enabled:             true,
				ReplicationMode:     common.ReplicationModeTransparent,
			}

			_ = mgr.AddPolicy(p)
		}(i)
	}

	// Get policies
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			_, _ = mgr.GetPolicies()
		}()
	}

	// Get specific policy
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			_, _ = mgr.GetPolicy("policy0")
		}()
	}

	wg.Wait()

	// Verify no corruption
	policies, err := mgr.GetPolicies()
	if err != nil {
		t.Fatalf("Failed to get policies after concurrent access: %v", err)
	}

	if len(policies) < 1 {
		t.Errorf("Expected at least 1 policy after concurrent access, got %d", len(policies))
	}
}

// TestRun tests the background sync process.
func TestRun(t *testing.T) {
	fs := newMockFileSystem()
	logger := adapters.NewNoOpLogger()
	auditLog := audit.NewNoOpAuditLogger()

	// Use a short interval for testing
	mgr, err := NewPersistentReplicationManager(fs, "test-policies.json", 100*time.Millisecond, logger, auditLog)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	// Start background sync
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	done := make(chan bool)
	go func() {
		mgr.Run(ctx)
		done <- true
	}()

	// Wait for context cancellation
	<-done

	// Verify manager stopped gracefully
	t.Log("Background sync stopped gracefully")
}

// TestRunWithStop tests stopping the background sync with Stop().
func TestRunWithStop(t *testing.T) {
	fs := newMockFileSystem()
	logger := adapters.NewNoOpLogger()
	auditLog := audit.NewNoOpAuditLogger()

	mgr, err := NewPersistentReplicationManager(fs, "test-policies.json", 100*time.Millisecond, logger, auditLog)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	ctx := context.Background()

	done := make(chan bool)
	go func() {
		mgr.Run(ctx)
		done <- true
	}()

	// Stop after a short delay
	time.Sleep(200 * time.Millisecond)
	mgr.Stop()

	// Wait for manager to stop
	select {
	case <-done:
		t.Log("Background sync stopped via Stop()")
	case <-time.After(1 * time.Second):
		t.Error("Manager did not stop in time")
	}
}

// TestDefaultValues tests that default values are set correctly.
func TestDefaultValues(t *testing.T) {
	// Test with nil filesystem (should use default)
	mgr, err := NewPersistentReplicationManager(nil, "", 0, nil, nil)
	if err != nil {
		t.Fatalf("Failed to create manager with nil values: %v", err)
	}

	if mgr.fs == nil {
		t.Error("Expected default filesystem to be set")
	}

	if mgr.policyFile != ".replication-policies.json" {
		t.Errorf("Expected default policy file '.replication-policies.json', got %s", mgr.policyFile)
	}

	if mgr.interval != 5*time.Minute {
		t.Errorf("Expected default interval 5m, got %v", mgr.interval)
	}

	if mgr.logger == nil {
		t.Error("Expected default logger to be set")
	}

	if mgr.auditLog == nil {
		t.Error("Expected default audit log to be set")
	}
}

// TestLoadCorruptedFile tests loading a corrupted policy file.
func TestLoadCorruptedFile(t *testing.T) {
	fs := newMockFileSystem()

	// Create a corrupted file
	file, _ := fs.OpenFile("test-policies.json", os.O_CREATE|os.O_RDWR, 0600)
	file.Write([]byte("invalid json"))
	file.Close()

	logger := adapters.NewNoOpLogger()
	auditLog := audit.NewNoOpAuditLogger()

	// This should fail to load
	_, err := NewPersistentReplicationManager(fs, "test-policies.json", 5*time.Minute, logger, auditLog)
	if err == nil {
		t.Error("Expected error when loading corrupted file")
	}
}

// TestJSONPersistence tests that the JSON format is correct.
func TestJSONPersistence(t *testing.T) {
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
		ReplicationMode:     common.ReplicationModeTransparent,
	}

	err = mgr.AddPolicy(policy)
	if err != nil {
		t.Fatalf("Failed to add policy: %v", err)
	}

	// Read the persisted file
	data, err := fs.readFile("test-policies.json")
	if err != nil {
		t.Fatalf("Failed to read policy file: %v", err)
	}

	// Verify it's valid JSON
	var persisted persistedPolicies
	err = json.Unmarshal(data, &persisted)
	if err != nil {
		t.Fatalf("Failed to unmarshal persisted data: %v", err)
	}

	if len(persisted.Policies) != 1 {
		t.Errorf("Expected 1 policy in JSON, got %d", len(persisted.Policies))
	}

	if _, exists := persisted.Policies["policy1"]; !exists {
		t.Error("Expected policy1 in persisted data")
	}
}
