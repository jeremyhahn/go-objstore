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

package cli

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// mockLifecycleStorage extends mockStorage with lifecycle functionality
type mockLifecycleStorage struct {
	*mockStorage
	policies          []common.LifecyclePolicy
	archiveError      error
	addPolicyError    error
	removePolicyError error
	getPoliciesError  error
}

func newMockLifecycleStorage() *mockLifecycleStorage {
	return &mockLifecycleStorage{
		mockStorage: newMockStorage(),
		policies:    []common.LifecyclePolicy{},
	}
}

func (m *mockLifecycleStorage) Archive(key string, destination common.Archiver) error {
	if m.archiveError != nil {
		return m.archiveError
	}
	if _, exists := m.data[key]; !exists {
		return &mockError{msg: "object not found"}
	}
	return nil
}

func (m *mockLifecycleStorage) AddPolicy(policy common.LifecyclePolicy) error {
	if m.addPolicyError != nil {
		return m.addPolicyError
	}
	// Check for duplicate ID
	for _, p := range m.policies {
		if p.ID == policy.ID {
			return errors.New("policy already exists")
		}
	}
	m.policies = append(m.policies, policy)
	return nil
}

func (m *mockLifecycleStorage) RemovePolicy(id string) error {
	if m.removePolicyError != nil {
		return m.removePolicyError
	}
	for i, p := range m.policies {
		if p.ID == id {
			m.policies = append(m.policies[:i], m.policies[i+1:]...)
			return nil
		}
	}
	return errors.New("policy not found")
}

func (m *mockLifecycleStorage) GetPolicies() ([]common.LifecyclePolicy, error) {
	if m.getPoliciesError != nil {
		return nil, m.getPoliciesError
	}
	return m.policies, nil
}

// mockArchiver for testing
type mockArchiver struct {
	putError error
}

func (m *mockArchiver) Put(key string, data io.Reader) error {
	if m.putError != nil {
		return m.putError
	}
	return nil
}

// TestArchiveCommand tests the archive command
func TestArchiveCommand(t *testing.T) {
	tests := []struct {
		name         string
		setupStorage func() *mockLifecycleStorage
		key          string
		destBackend  string
		wantError    bool
	}{
		{
			name: "successful archive",
			setupStorage: func() *mockLifecycleStorage {
				storage := newMockLifecycleStorage()
				storage.data["test.txt"] = []byte("test content")
				storage.metadata["test.txt"] = &common.Metadata{Size: 12}
				return storage
			},
			key:         "test.txt",
			destBackend: "local",
			wantError:   false,
		},
		{
			name: "object not found",
			setupStorage: func() *mockLifecycleStorage {
				return newMockLifecycleStorage()
			},
			key:         "nonexistent.txt",
			destBackend: "local",
			wantError:   true,
		},
		{
			name: "storage error",
			setupStorage: func() *mockLifecycleStorage {
				storage := newMockLifecycleStorage()
				storage.data["test.txt"] = []byte("test content")
				storage.archiveError = errors.New("storage error")
				return storage
			},
			key:         "test.txt",
			destBackend: "local",
			wantError:   true,
		},
		{
			name: "invalid backend",
			setupStorage: func() *mockLifecycleStorage {
				storage := newMockLifecycleStorage()
				storage.data["test.txt"] = []byte("test content")
				return storage
			},
			key:         "test.txt",
			destBackend: "invalid-backend",
			wantError:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage := tt.setupStorage()
			cfg := &Config{
				Backend:     "local",
				BackendPath: "/tmp/test",
			}
			ctx := &CommandContext{
				Storage: storage,
				Config:  cfg,
			}

			err := ctx.ArchiveCommand(tt.key, tt.destBackend)

			if tt.wantError {
				if err == nil {
					t.Errorf("ArchiveCommand() error = nil, wantError %v", tt.wantError)
				}
			} else {
				if err != nil {
					t.Errorf("ArchiveCommand() unexpected error = %v", err)
				}
			}
		})
	}
}

// TestAddPolicyCommand tests the add policy command
func TestAddPolicyCommand(t *testing.T) {
	tests := []struct {
		name          string
		setupStorage  func() *mockLifecycleStorage
		id            string
		prefix        string
		retentionDays string
		action        string
		wantError     bool
	}{
		{
			name: "successful add delete policy",
			setupStorage: func() *mockLifecycleStorage {
				return newMockLifecycleStorage()
			},
			id:            "policy1",
			prefix:        "logs/",
			retentionDays: "1",
			action:        "delete",
			wantError:     false,
		},
		{
			name: "successful add archive policy",
			setupStorage: func() *mockLifecycleStorage {
				return newMockLifecycleStorage()
			},
			id:            "policy2",
			prefix:        "data/",
			retentionDays: "30",
			action:        "archive",
			wantError:     true, // Expects error without glacier build tag
		},
		{
			name: "invalid retention days",
			setupStorage: func() *mockLifecycleStorage {
				return newMockLifecycleStorage()
			},
			id:            "policy3",
			prefix:        "logs/",
			retentionDays: "invalid",
			action:        "delete",
			wantError:     true,
		},
		{
			name: "duplicate policy",
			setupStorage: func() *mockLifecycleStorage {
				storage := newMockLifecycleStorage()
				storage.policies = []common.LifecyclePolicy{
					{ID: "existing", Prefix: "test/", Retention: 24 * time.Hour, Action: "delete"},
				}
				return storage
			},
			id:            "existing",
			prefix:        "logs/",
			retentionDays: "1",
			action:        "delete",
			wantError:     true,
		},
		{
			name: "storage error",
			setupStorage: func() *mockLifecycleStorage {
				storage := newMockLifecycleStorage()
				storage.addPolicyError = errors.New("storage error")
				return storage
			},
			id:            "policy4",
			prefix:        "logs/",
			retentionDays: "1",
			action:        "delete",
			wantError:     true,
		},
		{
			name: "zero retention days",
			setupStorage: func() *mockLifecycleStorage {
				return newMockLifecycleStorage()
			},
			id:            "policy5",
			prefix:        "temp/",
			retentionDays: "0",
			action:        "delete",
			wantError:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage := tt.setupStorage()
			cfg := &Config{
				Backend:     "local",
				BackendPath: "/tmp/test",
			}
			ctx := &CommandContext{
				Storage: storage,
				Config:  cfg,
			}

			err := ctx.AddPolicyCommand(tt.id, tt.prefix, tt.retentionDays, tt.action)

			if tt.wantError {
				if err == nil {
					t.Errorf("AddPolicyCommand() error = nil, wantError %v", tt.wantError)
				}
			} else {
				if err != nil {
					t.Errorf("AddPolicyCommand() unexpected error = %v", err)
				}
				// Verify policy was added
				policies, _ := storage.GetPolicies()
				found := false
				for _, p := range policies {
					if p.ID == tt.id {
						found = true
						if p.Prefix != tt.prefix {
							t.Errorf("Policy prefix = %v, want %v", p.Prefix, tt.prefix)
						}
						if p.Action != tt.action {
							t.Errorf("Policy action = %v, want %v", p.Action, tt.action)
						}
						break
					}
				}
				if !found {
					t.Error("AddPolicyCommand() policy not found in storage")
				}
			}
		})
	}
}

// TestRemovePolicyCommand tests the remove policy command
func TestRemovePolicyCommand(t *testing.T) {
	tests := []struct {
		name         string
		setupStorage func() *mockLifecycleStorage
		id           string
		wantError    bool
	}{
		{
			name: "successful remove",
			setupStorage: func() *mockLifecycleStorage {
				storage := newMockLifecycleStorage()
				storage.policies = []common.LifecyclePolicy{
					{ID: "policy1", Prefix: "logs/", Retention: 24 * time.Hour, Action: "delete"},
				}
				return storage
			},
			id:        "policy1",
			wantError: false,
		},
		{
			name: "policy not found",
			setupStorage: func() *mockLifecycleStorage {
				return newMockLifecycleStorage()
			},
			id:        "nonexistent",
			wantError: true,
		},
		{
			name: "storage error",
			setupStorage: func() *mockLifecycleStorage {
				storage := newMockLifecycleStorage()
				storage.removePolicyError = errors.New("storage error")
				return storage
			},
			id:        "policy1",
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage := tt.setupStorage()
			cfg := &Config{
				Backend: "local",
			}
			ctx := &CommandContext{
				Storage: storage,
				Config:  cfg,
			}

			err := ctx.RemovePolicyCommand(tt.id)

			if tt.wantError {
				if err == nil {
					t.Errorf("RemovePolicyCommand() error = nil, wantError %v", tt.wantError)
				}
			} else {
				if err != nil {
					t.Errorf("RemovePolicyCommand() unexpected error = %v", err)
				}
				// Verify policy was removed
				policies, _ := storage.GetPolicies()
				for _, p := range policies {
					if p.ID == tt.id {
						t.Error("RemovePolicyCommand() policy still exists in storage")
					}
				}
			}
		})
	}
}

// TestListPoliciesCommand tests the list policies command
func TestListPoliciesCommand(t *testing.T) {
	tests := []struct {
		name         string
		setupStorage func() *mockLifecycleStorage
		wantCount    int
		wantError    bool
	}{
		{
			name: "list all policies",
			setupStorage: func() *mockLifecycleStorage {
				storage := newMockLifecycleStorage()
				storage.policies = []common.LifecyclePolicy{
					{ID: "policy1", Prefix: "logs/", Retention: 24 * time.Hour, Action: "delete"},
					{ID: "policy2", Prefix: "data/", Retention: 30 * 24 * time.Hour, Action: "archive"},
				}
				return storage
			},
			wantCount: 2,
			wantError: false,
		},
		{
			name: "no policies",
			setupStorage: func() *mockLifecycleStorage {
				return newMockLifecycleStorage()
			},
			wantCount: 0,
			wantError: false,
		},
		{
			name: "storage error",
			setupStorage: func() *mockLifecycleStorage {
				storage := newMockLifecycleStorage()
				storage.getPoliciesError = errors.New("storage error")
				return storage
			},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage := tt.setupStorage()
			cfg := &Config{
				Backend: "local",
			}
			ctx := &CommandContext{
				Storage: storage,
				Config:  cfg,
			}

			policies, err := ctx.ListPoliciesCommand()

			if tt.wantError {
				if err == nil {
					t.Errorf("ListPoliciesCommand() error = nil, wantError %v", tt.wantError)
				}
			} else {
				if err != nil {
					t.Errorf("ListPoliciesCommand() unexpected error = %v", err)
				}
				if len(policies) != tt.wantCount {
					t.Errorf("ListPoliciesCommand() count = %d, want %d", len(policies), tt.wantCount)
				}
			}
		})
	}
}

// TestLifecycleCommandIntegration tests the complete lifecycle workflow
func TestLifecycleCommandIntegration(t *testing.T) {
	storage := newMockLifecycleStorage()
	cfg := &Config{
		Backend:     "local",
		BackendPath: "/tmp/test",
	}
	ctx := &CommandContext{
		Storage: storage,
		Config:  cfg,
	}

	// Add an object
	key := "test-data.txt"
	storage.data[key] = []byte("test content")
	storage.metadata[key] = &common.Metadata{Size: 12}

	// Add a lifecycle policy
	err := ctx.AddPolicyCommand("policy1", "test-", "7", "delete")
	if err != nil {
		t.Fatalf("AddPolicyCommand() error = %v", err)
	}

	// List policies
	policies, err := ctx.ListPoliciesCommand()
	if err != nil {
		t.Fatalf("ListPoliciesCommand() error = %v", err)
	}
	if len(policies) != 1 {
		t.Fatalf("ListPoliciesCommand() count = %d, want 1", len(policies))
	}

	// Verify policy details
	if policies[0].ID != "policy1" {
		t.Errorf("Policy ID = %v, want policy1", policies[0].ID)
	}
	if policies[0].Prefix != "test-" {
		t.Errorf("Policy prefix = %v, want test-", policies[0].Prefix)
	}
	if policies[0].Action != "delete" {
		t.Errorf("Policy action = %v, want delete", policies[0].Action)
	}

	expectedRetention := 7 * 24 * time.Hour
	if policies[0].Retention != expectedRetention {
		t.Errorf("Policy retention = %v, want %v", policies[0].Retention, expectedRetention)
	}

	// Archive the object - expect error since local backend may not be available
	err = ctx.ArchiveCommand(key, "local")
	if err == nil {
		t.Log("ArchiveCommand() succeeded - local backend is available")
	} else {
		t.Logf("ArchiveCommand() error = %v (expected - local backend not available)", err)
	}

	// Add another policy (use "delete" action to avoid archiver backend dependency)
	err = ctx.AddPolicyCommand("policy2", "archive-", "30", "delete")
	if err != nil {
		t.Fatalf("AddPolicyCommand() error = %v", err)
	}

	// List policies again
	policies, err = ctx.ListPoliciesCommand()
	if err != nil {
		t.Fatalf("ListPoliciesCommand() error = %v", err)
	}
	if len(policies) != 2 {
		t.Fatalf("ListPoliciesCommand() count = %d, want 2", len(policies))
	}

	// Remove first policy
	err = ctx.RemovePolicyCommand("policy1")
	if err != nil {
		t.Fatalf("RemovePolicyCommand() error = %v", err)
	}

	// Verify removal
	policies, err = ctx.ListPoliciesCommand()
	if err != nil {
		t.Fatalf("ListPoliciesCommand() error = %v", err)
	}
	if len(policies) != 1 {
		t.Fatalf("ListPoliciesCommand() count = %d, want 1", len(policies))
	}
	if policies[0].ID != "policy2" {
		t.Errorf("Remaining policy ID = %v, want policy2", policies[0].ID)
	}
}

// TestArchiveCommandEdgeCases tests edge cases for archive command
func TestArchiveCommandEdgeCases(t *testing.T) {
	tests := []struct {
		name         string
		setupStorage func() *mockLifecycleStorage
		setupConfig  func() *Config
		key          string
		destBackend  string
		wantError    bool
	}{
		{
			name: "archive to glacier",
			setupStorage: func() *mockLifecycleStorage {
				storage := newMockLifecycleStorage()
				storage.data["large-file.bin"] = bytes.Repeat([]byte("x"), 1024*1024) // 1MB
				return storage
			},
			setupConfig: func() *Config {
				return &Config{
					Backend:     "local",
					BackendPath: "/tmp/test",
				}
			},
			key:         "large-file.bin",
			destBackend: "glacier",
			wantError:   true, // Factory doesn't support glacier without proper config
		},
		{
			name: "archive empty object",
			setupStorage: func() *mockLifecycleStorage {
				storage := newMockLifecycleStorage()
				storage.data["empty.txt"] = []byte("")
				return storage
			},
			setupConfig: func() *Config {
				return &Config{
					Backend:     "local",
					BackendPath: "/tmp/test",
				}
			},
			key:         "empty.txt",
			destBackend: "local",
			wantError:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage := tt.setupStorage()
			cfg := tt.setupConfig()
			ctx := &CommandContext{
				Storage: storage,
				Config:  cfg,
			}

			err := ctx.ArchiveCommand(tt.key, tt.destBackend)

			if tt.wantError {
				if err == nil {
					t.Errorf("ArchiveCommand() error = nil, wantError %v", tt.wantError)
				}
			} else {
				if err != nil {
					t.Errorf("ArchiveCommand() unexpected error = %v", err)
				}
			}
		})
	}
}

// TestPolicyRetentionConversion tests retention days to seconds conversion
func TestPolicyRetentionConversion(t *testing.T) {
	tests := []struct {
		name          string
		retentionDays string
		wantDuration  time.Duration
		wantError     bool
	}{
		{
			name:          "1 day",
			retentionDays: "1",
			wantDuration:  24 * time.Hour,
			wantError:     false,
		},
		{
			name:          "7 days",
			retentionDays: "7",
			wantDuration:  7 * 24 * time.Hour,
			wantError:     false,
		},
		{
			name:          "30 days",
			retentionDays: "30",
			wantDuration:  30 * 24 * time.Hour,
			wantError:     false,
		},
		{
			name:          "365 days",
			retentionDays: "365",
			wantDuration:  365 * 24 * time.Hour,
			wantError:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage := newMockLifecycleStorage()
			cfg := &Config{
				Backend: "local",
			}
			ctx := &CommandContext{
				Storage: storage,
				Config:  cfg,
			}

			err := ctx.AddPolicyCommand("test", "test/", tt.retentionDays, "delete")
			if err != nil {
				t.Fatalf("AddPolicyCommand() error = %v", err)
			}

			policies, _ := storage.GetPolicies()
			if len(policies) != 1 {
				t.Fatal("Policy not added")
			}

			if policies[0].Retention != tt.wantDuration {
				t.Errorf("Retention = %v, want %v", policies[0].Retention, tt.wantDuration)
			}
		})
	}
}

// TestApplyPoliciesCommand tests the apply policies command
func TestApplyPoliciesCommand(t *testing.T) {
	tests := []struct {
		name         string
		setupStorage func() *mockLifecycleStorage
		setupConfig  func() *Config
		wantError    bool
	}{
		{
			name: "no policies to apply",
			setupStorage: func() *mockLifecycleStorage {
				return newMockLifecycleStorage()
			},
			setupConfig: func() *Config {
				return &Config{Backend: "local"}
			},
			wantError: false,
		},
		{
			name: "apply policies with delete action",
			setupStorage: func() *mockLifecycleStorage {
				storage := newMockLifecycleStorage()
				// Add an old object that should be deleted
				storage.data["logs/old.txt"] = []byte("old content")
				storage.metadata["logs/old.txt"] = &common.Metadata{
					Size:         11,
					LastModified: time.Now().Add(-48 * time.Hour),
				}
				// Add a recent object that should not be deleted
				storage.data["logs/recent.txt"] = []byte("recent content")
				storage.metadata["logs/recent.txt"] = &common.Metadata{
					Size:         14,
					LastModified: time.Now(),
				}
				// Add a policy to delete old logs
				storage.policies = []common.LifecyclePolicy{
					{
						ID:        "cleanup-logs",
						Prefix:    "logs/",
						Retention: 24 * time.Hour,
						Action:    "delete",
					},
				}
				return storage
			},
			setupConfig: func() *Config {
				return &Config{Backend: "local"}
			},
			wantError: false,
		},
		{
			name: "cloud backend error",
			setupStorage: func() *mockLifecycleStorage {
				storage := newMockLifecycleStorage()
				storage.policies = []common.LifecyclePolicy{
					{
						ID:        "test-policy",
						Prefix:    "data/",
						Retention: 24 * time.Hour,
						Action:    "delete",
					},
				}
				return storage
			},
			setupConfig: func() *Config {
				return &Config{Backend: "s3"}
			},
			wantError: true,
		},
		{
			name: "GetPolicies error",
			setupStorage: func() *mockLifecycleStorage {
				storage := newMockLifecycleStorage()
				storage.getPoliciesError = errors.New("storage error")
				return storage
			},
			setupConfig: func() *Config {
				return &Config{Backend: "local"}
			},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage := tt.setupStorage()
			cfg := tt.setupConfig()
			ctx := &CommandContext{
				Storage: storage,
				Config:  cfg,
			}

			err := ctx.ApplyPoliciesCommand()

			if tt.wantError {
				if err == nil {
					t.Errorf("ApplyPoliciesCommand() error = nil, wantError %v", tt.wantError)
				}
			} else {
				if err != nil {
					t.Errorf("ApplyPoliciesCommand() unexpected error = %v", err)
				}
			}
		})
	}
}
