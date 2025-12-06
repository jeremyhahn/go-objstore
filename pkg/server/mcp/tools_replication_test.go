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

package mcp

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/replication"
)

// MockStorageWithReplication extends MockStorage to support replication
type MockStorageWithReplication struct {
	*MockStorage
	repMgr *MockReplicationManager
	repErr error
}

func NewMockStorageWithReplication() *MockStorageWithReplication {
	return &MockStorageWithReplication{
		MockStorage: NewMockStorage(),
		repMgr:      NewMockReplicationManager(),
	}
}

func (m *MockStorageWithReplication) GetReplicationManager() (common.ReplicationManager, error) {
	if m.repErr != nil {
		return nil, m.repErr
	}
	return m.repMgr, nil
}

// MockReplicationManager implements common.ReplicationManager for testing
type MockReplicationManager struct {
	policies            map[string]common.ReplicationPolicy
	replicationStatuses map[string]*replication.ReplicationStatus
	syncCalled          bool
	syncPolicyID        string
	syncAllCalled       bool
	addError            error
	removeError         error
	getError            error
	syncError           error
	getStatusError      error
}

func NewMockReplicationManager() *MockReplicationManager {
	return &MockReplicationManager{
		policies:            make(map[string]common.ReplicationPolicy),
		replicationStatuses: make(map[string]*replication.ReplicationStatus),
	}
}

// GetReplicationStatus implements the optional status provider interface
func (m *MockReplicationManager) GetReplicationStatus(id string) (*replication.ReplicationStatus, error) {
	if m.getStatusError != nil {
		return nil, m.getStatusError
	}
	status, exists := m.replicationStatuses[id]
	if !exists {
		return nil, common.ErrPolicyNotFound
	}
	return status, nil
}

func (m *MockReplicationManager) AddPolicy(policy common.ReplicationPolicy) error {
	if m.addError != nil {
		return m.addError
	}
	m.policies[policy.ID] = policy
	return nil
}

func (m *MockReplicationManager) RemovePolicy(id string) error {
	if m.removeError != nil {
		return m.removeError
	}
	if _, exists := m.policies[id]; !exists {
		return common.ErrPolicyNotFound
	}
	delete(m.policies, id)
	return nil
}

func (m *MockReplicationManager) GetPolicy(id string) (*common.ReplicationPolicy, error) {
	if m.getError != nil {
		return nil, m.getError
	}
	policy, exists := m.policies[id]
	if !exists {
		return nil, common.ErrPolicyNotFound
	}
	return &policy, nil
}

func (m *MockReplicationManager) GetPolicies() ([]common.ReplicationPolicy, error) {
	if m.getError != nil {
		return nil, m.getError
	}
	policies := make([]common.ReplicationPolicy, 0, len(m.policies))
	for _, p := range m.policies {
		policies = append(policies, p)
	}
	return policies, nil
}

func (m *MockReplicationManager) SyncAll(ctx context.Context) (*common.SyncResult, error) {
	if m.syncError != nil {
		return nil, m.syncError
	}
	m.syncAllCalled = true
	return &common.SyncResult{
		PolicyID:   "all",
		Synced:     10,
		Deleted:    2,
		Failed:     1,
		BytesTotal: 1024,
		Duration:   5 * time.Second,
	}, nil
}

func (m *MockReplicationManager) SyncPolicy(ctx context.Context, policyID string) (*common.SyncResult, error) {
	if m.syncError != nil {
		return nil, m.syncError
	}
	if _, exists := m.policies[policyID]; !exists {
		return nil, common.ErrPolicyNotFound
	}
	m.syncCalled = true
	m.syncPolicyID = policyID
	return &common.SyncResult{
		PolicyID:   policyID,
		Synced:     5,
		Deleted:    1,
		Failed:     0,
		BytesTotal: 512,
		Duration:   2 * time.Second,
	}, nil
}

func (m *MockReplicationManager) SetBackendEncrypterFactory(policyID string, factory common.EncrypterFactory) error {
	return nil
}

func (m *MockReplicationManager) SetSourceEncrypterFactory(policyID string, factory common.EncrypterFactory) error {
	return nil
}

func (m *MockReplicationManager) SetDestinationEncrypterFactory(policyID string, factory common.EncrypterFactory) error {
	return nil
}

func (m *MockReplicationManager) Run(ctx context.Context) {
	// No-op for testing
}

func TestToolRegistry_RegisterReplicationTools(t *testing.T) {
	registry := NewToolRegistry()
	registry.RegisterReplicationTools()

	expectedTools := []string{
		"objstore_add_replication_policy",
		"objstore_remove_replication_policy",
		"objstore_list_replication_policies",
		"objstore_get_replication_policy",
		"objstore_trigger_replication",
		"objstore_get_replication_status",
	}

	for _, toolName := range expectedTools {
		tool, ok := registry.GetTool(toolName)
		if !ok {
			t.Errorf("tool %s not found", toolName)
		}
		if tool.Name != toolName {
			t.Errorf("expected tool name %s, got %s", toolName, tool.Name)
		}
		if tool.Description == "" {
			t.Errorf("tool %s has no description", toolName)
		}
		if tool.InputSchema == nil {
			t.Errorf("tool %s has no input schema", toolName)
		}
	}
}

func TestToolExecutor_ExecuteAddReplicationPolicy(t *testing.T) {
	storage := NewMockStorageWithReplication()
	executor := createTestToolExecutor(t, storage)

	tests := []struct {
		name      string
		args      map[string]any
		wantError bool
	}{
		{
			name: "valid policy",
			args: map[string]any{
				"id":                  "test-policy-1",
				"source_backend":      "local",
				"destination_backend": "s3",
				"check_interval":      float64(300),
				"enabled":             true,
				"replication_mode":    "transparent",
			},
			wantError: false,
		},
		{
			name: "with source settings",
			args: map[string]any{
				"id":                   "test-policy-2",
				"source_backend":       "local",
				"source_settings":      map[string]any{"path": "/data"},
				"destination_backend":  "s3",
				"destination_settings": map[string]any{"bucket": "backup"},
				"check_interval":       float64(600),
				"enabled":              true,
			},
			wantError: false,
		},
		{
			name: "with encryption policy",
			args: map[string]any{
				"id":                  "test-policy-3",
				"source_backend":      "local",
				"destination_backend": "s3",
				"check_interval":      float64(300),
				"encryption": map[string]any{
					"backend": map[string]any{
						"enabled":     true,
						"provider":    "custom",
						"default_key": "backend-key",
					},
					"source": map[string]any{
						"enabled":     true,
						"provider":    "custom",
						"default_key": "source-key",
					},
					"destination": map[string]any{
						"enabled":     true,
						"provider":    "custom",
						"default_key": "dest-key",
					},
				},
			},
			wantError: false,
		},
		{
			name: "missing id",
			args: map[string]any{
				"source_backend":      "local",
				"destination_backend": "s3",
				"check_interval":      float64(300),
			},
			wantError: true,
		},
		{
			name: "missing source_backend",
			args: map[string]any{
				"id":                  "test-policy-4",
				"destination_backend": "s3",
				"check_interval":      float64(300),
			},
			wantError: true,
		},
		{
			name: "missing destination_backend",
			args: map[string]any{
				"id":             "test-policy-5",
				"source_backend": "local",
				"check_interval": float64(300),
			},
			wantError: true,
		},
		{
			name: "missing check_interval",
			args: map[string]any{
				"id":                  "test-policy-6",
				"source_backend":      "local",
				"destination_backend": "s3",
			},
			wantError: true,
		},
		{
			name: "invalid check_interval",
			args: map[string]any{
				"id":                  "test-policy-7",
				"source_backend":      "local",
				"destination_backend": "s3",
				"check_interval":      float64(0),
			},
			wantError: true,
		},
		{
			name: "invalid replication_mode",
			args: map[string]any{
				"id":                  "test-policy-8",
				"source_backend":      "local",
				"destination_backend": "s3",
				"check_interval":      float64(300),
				"replication_mode":    "invalid",
			},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executor.Execute(context.Background(), "objstore_add_replication_policy", tt.args)
			if tt.wantError {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			var resultMap map[string]any
			if err := json.Unmarshal([]byte(result), &resultMap); err != nil {
				t.Errorf("failed to parse result: %v", err)
			}
			if !resultMap["success"].(bool) {
				t.Error("expected success to be true")
			}

			// Verify policy was added
			if id, ok := tt.args["id"].(string); ok {
				policy, err := storage.repMgr.GetPolicy(id)
				if err != nil {
					t.Errorf("policy not found: %v", err)
				}
				if policy.ID != id {
					t.Errorf("expected policy ID %s, got %s", id, policy.ID)
				}
			}
		})
	}
}

func TestToolExecutor_ExecuteRemoveReplicationPolicy(t *testing.T) {
	storage := NewMockStorageWithReplication()
	executor := createTestToolExecutor(t, storage)

	// Add a policy first
	storage.repMgr.AddPolicy(common.ReplicationPolicy{
		ID:                 "test-policy",
		SourceBackend:      "local",
		DestinationBackend: "s3",
		CheckInterval:      5 * time.Minute,
		Enabled:            true,
		ReplicationMode:    common.ReplicationModeTransparent,
	})

	tests := []struct {
		name      string
		args      map[string]any
		wantError bool
	}{
		{
			name: "remove existing policy",
			args: map[string]any{
				"id": "test-policy",
			},
			wantError: false,
		},
		{
			name: "remove non-existent policy",
			args: map[string]any{
				"id": "nonexistent",
			},
			wantError: true,
		},
		{
			name:      "missing id",
			args:      map[string]any{},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executor.Execute(context.Background(), "objstore_remove_replication_policy", tt.args)
			if tt.wantError {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			var resultMap map[string]any
			if err := json.Unmarshal([]byte(result), &resultMap); err != nil {
				t.Errorf("failed to parse result: %v", err)
			}
			if !resultMap["success"].(bool) {
				t.Error("expected success to be true")
			}
		})
	}
}

func TestToolExecutor_ExecuteListReplicationPolicies(t *testing.T) {
	storage := NewMockStorageWithReplication()
	executor := createTestToolExecutor(t, storage)

	// Add some policies
	storage.repMgr.AddPolicy(common.ReplicationPolicy{
		ID:                 "policy-1",
		SourceBackend:      "local",
		DestinationBackend: "s3",
		CheckInterval:      5 * time.Minute,
		Enabled:            true,
		ReplicationMode:    common.ReplicationModeTransparent,
	})
	storage.repMgr.AddPolicy(common.ReplicationPolicy{
		ID:                 "policy-2",
		SourceBackend:      "s3",
		DestinationBackend: "gcs",
		CheckInterval:      10 * time.Minute,
		Enabled:            false,
		ReplicationMode:    common.ReplicationModeOpaque,
	})

	result, err := executor.Execute(context.Background(), "objstore_list_replication_policies", map[string]any{})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}

	var resultMap map[string]any
	if err := json.Unmarshal([]byte(result), &resultMap); err != nil {
		t.Errorf("failed to parse result: %v", err)
	}

	if !resultMap["success"].(bool) {
		t.Error("expected success to be true")
	}

	count := int(resultMap["count"].(float64))
	if count != 2 {
		t.Errorf("expected 2 policies, got %d", count)
	}
}

func TestToolExecutor_ExecuteGetReplicationPolicy(t *testing.T) {
	storage := NewMockStorageWithReplication()
	executor := createTestToolExecutor(t, storage)

	// Add a policy
	storage.repMgr.AddPolicy(common.ReplicationPolicy{
		ID:                 "test-policy",
		SourceBackend:      "local",
		DestinationBackend: "s3",
		CheckInterval:      5 * time.Minute,
		Enabled:            true,
		ReplicationMode:    common.ReplicationModeTransparent,
	})

	tests := []struct {
		name      string
		args      map[string]any
		wantError bool
	}{
		{
			name: "get existing policy",
			args: map[string]any{
				"id": "test-policy",
			},
			wantError: false,
		},
		{
			name: "get non-existent policy",
			args: map[string]any{
				"id": "nonexistent",
			},
			wantError: true,
		},
		{
			name:      "missing id",
			args:      map[string]any{},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executor.Execute(context.Background(), "objstore_get_replication_policy", tt.args)
			if tt.wantError {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			var resultMap map[string]any
			if err := json.Unmarshal([]byte(result), &resultMap); err != nil {
				t.Errorf("failed to parse result: %v", err)
			}
			if !resultMap["success"].(bool) {
				t.Error("expected success to be true")
			}

			if id, ok := tt.args["id"].(string); ok {
				if resultMap["id"].(string) != id {
					t.Errorf("expected policy ID %s, got %s", id, resultMap["id"])
				}
			}
		})
	}
}

func TestToolExecutor_ExecuteTriggerReplication(t *testing.T) {
	storage := NewMockStorageWithReplication()
	executor := createTestToolExecutor(t, storage)

	// Add a policy
	storage.repMgr.AddPolicy(common.ReplicationPolicy{
		ID:                 "test-policy",
		SourceBackend:      "local",
		DestinationBackend: "s3",
		CheckInterval:      5 * time.Minute,
		Enabled:            true,
		ReplicationMode:    common.ReplicationModeTransparent,
	})

	tests := []struct {
		name         string
		args         map[string]any
		wantError    bool
		checkSyncAll bool
		checkSyncID  bool
	}{
		{
			name: "sync specific policy",
			args: map[string]any{
				"policy_id": "test-policy",
			},
			wantError:   false,
			checkSyncID: true,
		},
		{
			name:         "sync all policies",
			args:         map[string]any{},
			wantError:    false,
			checkSyncAll: true,
		},
		{
			name: "sync non-existent policy",
			args: map[string]any{
				"policy_id": "nonexistent",
			},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset flags
			storage.repMgr.syncCalled = false
			storage.repMgr.syncAllCalled = false
			storage.repMgr.syncPolicyID = ""

			result, err := executor.Execute(context.Background(), "objstore_trigger_replication", tt.args)
			if tt.wantError {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			var resultMap map[string]any
			if err := json.Unmarshal([]byte(result), &resultMap); err != nil {
				t.Errorf("failed to parse result: %v", err)
			}
			if !resultMap["success"].(bool) {
				t.Error("expected success to be true")
			}

			if tt.checkSyncAll && !storage.repMgr.syncAllCalled {
				t.Error("expected SyncAll to be called")
			}

			if tt.checkSyncID {
				if !storage.repMgr.syncCalled {
					t.Error("expected SyncPolicy to be called")
				}
				if policyID, ok := tt.args["policy_id"].(string); ok {
					if storage.repMgr.syncPolicyID != policyID {
						t.Errorf("expected sync policy ID %s, got %s", policyID, storage.repMgr.syncPolicyID)
					}
				}
			}
		})
	}
}

func TestReplicationToolsWithoutSupport(t *testing.T) {
	storage := NewMockStorage() // Regular storage without replication support
	executor := createTestToolExecutor(t, storage)

	tools := []string{
		"objstore_add_replication_policy",
		"objstore_remove_replication_policy",
		"objstore_list_replication_policies",
		"objstore_get_replication_policy",
		"objstore_trigger_replication",
	}

	for _, toolName := range tools {
		t.Run(toolName, func(t *testing.T) {
			var args map[string]any
			switch toolName {
			case "objstore_add_replication_policy":
				args = map[string]any{
					"id":                  "test",
					"source_backend":      "local",
					"destination_backend": "s3",
					"check_interval":      float64(300),
				}
			case "objstore_remove_replication_policy", "objstore_get_replication_policy":
				args = map[string]any{"id": "test"}
			case "objstore_trigger_replication":
				args = map[string]any{"policy_id": "test"}
			default:
				args = map[string]any{}
			}

			_, err := executor.Execute(context.Background(), toolName, args)
			if err == nil {
				t.Error("expected error for unsupported storage, got nil")
			}
			if !strings.Contains(err.Error(), "replication not supported") {
				t.Errorf("expected 'replication not supported' error, got: %v", err)
			}
		})
	}
}

func TestParseEncryptionConfig(t *testing.T) {
	tests := []struct {
		name     string
		input    map[string]any
		expected *common.EncryptionConfig
	}{
		{
			name: "full config",
			input: map[string]any{
				"enabled":     true,
				"provider":    "custom",
				"default_key": "test-key",
			},
			expected: &common.EncryptionConfig{
				Enabled:    true,
				Provider:   "custom",
				DefaultKey: "test-key",
			},
		},
		{
			name:  "empty config",
			input: map[string]any{},
			expected: &common.EncryptionConfig{
				Enabled:    false,
				Provider:   "",
				DefaultKey: "",
			},
		},
		{
			name: "partial config",
			input: map[string]any{
				"enabled": true,
			},
			expected: &common.EncryptionConfig{
				Enabled:    true,
				Provider:   "",
				DefaultKey: "",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseEncryptionConfig(tt.input)

			if result.Enabled != tt.expected.Enabled {
				t.Errorf("expected Enabled %v, got %v", tt.expected.Enabled, result.Enabled)
			}
			if result.Provider != tt.expected.Provider {
				t.Errorf("expected Provider %s, got %s", tt.expected.Provider, result.Provider)
			}
			if result.DefaultKey != tt.expected.DefaultKey {
				t.Errorf("expected DefaultKey %s, got %s", tt.expected.DefaultKey, result.DefaultKey)
			}
		})
	}
}

// Tests for GetReplicationStatus

func TestToolExecutor_ExecuteGetReplicationStatus_Success(t *testing.T) {
	storage := NewMockStorageWithReplication()
	executor := createTestToolExecutor(t, storage)

	// Set up a replication status
	storage.repMgr.replicationStatuses["test-policy"] = &replication.ReplicationStatus{
		PolicyID:           "test-policy",
		SourceBackend:      "local",
		DestinationBackend: "s3",
		Enabled:            true,
		TotalObjectsSynced: 100,
		TotalBytesSynced:   1024 * 1024,
	}

	result, err := executor.Execute(context.Background(), "objstore_get_replication_status", map[string]any{
		"policy_id": "test-policy",
	})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}

	var resultMap map[string]any
	if err := json.Unmarshal([]byte(result), &resultMap); err != nil {
		t.Errorf("failed to parse result: %v", err)
	}
	if !resultMap["success"].(bool) {
		t.Error("expected success to be true")
	}
	if resultMap["policy_id"].(string) != "test-policy" {
		t.Errorf("expected policy_id 'test-policy', got %s", resultMap["policy_id"])
	}
}

func TestToolExecutor_ExecuteGetReplicationStatus_MissingPolicyID(t *testing.T) {
	storage := NewMockStorageWithReplication()
	executor := createTestToolExecutor(t, storage)

	_, err := executor.Execute(context.Background(), "objstore_get_replication_status", map[string]any{})
	if err == nil {
		t.Error("expected error, got nil")
	}
}

func TestToolExecutor_ExecuteGetReplicationStatus_NotFound(t *testing.T) {
	storage := NewMockStorageWithReplication()
	executor := createTestToolExecutor(t, storage)

	_, err := executor.Execute(context.Background(), "objstore_get_replication_status", map[string]any{
		"policy_id": "nonexistent",
	})
	if err == nil {
		t.Error("expected error, got nil")
	}
}

func TestToolExecutor_ExecuteGetReplicationStatus_ReplicationNotSupported(t *testing.T) {
	storage := NewMockStorage() // Regular storage without replication support
	executor := createTestToolExecutor(t, storage)

	_, err := executor.Execute(context.Background(), "objstore_get_replication_status", map[string]any{
		"policy_id": "test-policy",
	})
	if err == nil {
		t.Error("expected error for unsupported storage, got nil")
	}
	if !strings.Contains(err.Error(), "replication not supported") {
		t.Errorf("expected 'replication not supported' error, got: %v", err)
	}
}

func TestToolExecutor_ExecuteGetReplicationStatus_Error(t *testing.T) {
	storage := NewMockStorageWithReplication()
	storage.repMgr.getStatusError = common.ErrInternal
	executor := createTestToolExecutor(t, storage)

	_, err := executor.Execute(context.Background(), "objstore_get_replication_status", map[string]any{
		"policy_id": "test-policy",
	})
	if err == nil {
		t.Error("expected error, got nil")
	}
}
