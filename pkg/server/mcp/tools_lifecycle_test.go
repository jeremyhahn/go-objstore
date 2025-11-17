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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// mockLifecycleStorage extends storage with lifecycle operations
type mockLifecycleStorage struct {
	data              map[string][]byte
	metadata          map[string]*common.Metadata
	policies          []common.LifecyclePolicy
	archiveError      error
	addPolicyError    error
	removePolicyError error
	getPoliciesError  error
}

func newMockLifecycleStorage() *mockLifecycleStorage {
	return &mockLifecycleStorage{
		data:     make(map[string][]byte),
		metadata: make(map[string]*common.Metadata),
		policies: []common.LifecyclePolicy{},
	}
}

func (m *mockLifecycleStorage) Configure(settings map[string]string) error {
	return nil
}

func (m *mockLifecycleStorage) Put(key string, data io.Reader) error {
	return m.PutWithContext(context.Background(), key, data)
}

func (m *mockLifecycleStorage) PutWithContext(ctx context.Context, key string, data io.Reader) error {
	content, err := io.ReadAll(data)
	if err != nil {
		return err
	}
	m.data[key] = content
	m.metadata[key] = &common.Metadata{
		Size:         int64(len(content)),
		LastModified: time.Now(),
	}
	return nil
}

func (m *mockLifecycleStorage) PutWithMetadata(ctx context.Context, key string, data io.Reader, metadata *common.Metadata) error {
	if err := m.PutWithContext(ctx, key, data); err != nil {
		return err
	}
	m.metadata[key] = metadata
	return nil
}

func (m *mockLifecycleStorage) Get(key string) (io.ReadCloser, error) {
	return m.GetWithContext(context.Background(), key)
}

func (m *mockLifecycleStorage) GetWithContext(ctx context.Context, key string) (io.ReadCloser, error) {
	content, exists := m.data[key]
	if !exists {
		return nil, errors.New("object not found")
	}
	return io.NopCloser(bytes.NewReader(content)), nil
}

func (m *mockLifecycleStorage) GetMetadata(ctx context.Context, key string) (*common.Metadata, error) {
	metadata, exists := m.metadata[key]
	if !exists {
		return nil, errors.New("object not found")
	}
	return metadata, nil
}

func (m *mockLifecycleStorage) UpdateMetadata(ctx context.Context, key string, metadata *common.Metadata) error {
	if _, exists := m.data[key]; !exists {
		return errors.New("object not found")
	}
	m.metadata[key] = metadata
	return nil
}

func (m *mockLifecycleStorage) Delete(key string) error {
	return m.DeleteWithContext(context.Background(), key)
}

func (m *mockLifecycleStorage) DeleteWithContext(ctx context.Context, key string) error {
	if _, exists := m.data[key]; !exists {
		return errors.New("object not found")
	}
	delete(m.data, key)
	delete(m.metadata, key)
	return nil
}

func (m *mockLifecycleStorage) Exists(ctx context.Context, key string) (bool, error) {
	_, exists := m.data[key]
	return exists, nil
}

func (m *mockLifecycleStorage) List(prefix string) ([]string, error) {
	return m.ListWithContext(context.Background(), prefix)
}

func (m *mockLifecycleStorage) ListWithContext(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	for key := range m.data {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	return keys, nil
}

func (m *mockLifecycleStorage) ListWithOptions(ctx context.Context, opts *common.ListOptions) (*common.ListResult, error) {
	var objects []*common.ObjectInfo
	for key, meta := range m.metadata {
		if opts.Prefix == "" || strings.HasPrefix(key, opts.Prefix) {
			objects = append(objects, &common.ObjectInfo{
				Key:      key,
				Metadata: meta,
			})
		}
	}
	return &common.ListResult{
		Objects:   objects,
		Truncated: false,
	}, nil
}

func (m *mockLifecycleStorage) Archive(key string, destination common.Archiver) error {
	if m.archiveError != nil {
		return m.archiveError
	}
	if _, exists := m.data[key]; !exists {
		return errors.New("object not found")
	}
	return nil
}

func (m *mockLifecycleStorage) AddPolicy(policy common.LifecyclePolicy) error {
	if m.addPolicyError != nil {
		return m.addPolicyError
	}
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
type mockArchiver struct{}

func (m *mockArchiver) Put(key string, data io.Reader) error {
	return nil
}

// TestExecuteArchiveTool tests the archive tool execution
func TestExecuteArchiveTool(t *testing.T) {
	tests := []struct {
		name         string
		setupStorage func() *mockLifecycleStorage
		args         map[string]any
		wantError    bool
		wantContains string
	}{
		{
			name: "successful archive",
			setupStorage: func() *mockLifecycleStorage {
				storage := newMockLifecycleStorage()
				storage.PutWithContext(context.Background(), "test.txt", strings.NewReader("content"))
				return storage
			},
			args: map[string]any{
				"key":              "test.txt",
				"destination_type": "local",
				"destination_settings": map[string]any{
					"path": "/tmp/archive-test",
				},
			},
			// Note: This may fail if the archiver backend is not available due to build constraints
			// The factory returns ErrUnknownArchiver when the backend isn't registered
			wantError:    false,
			wantContains: "",
		},
		{
			name: "missing key",
			setupStorage: func() *mockLifecycleStorage {
				return newMockLifecycleStorage()
			},
			args: map[string]any{
				"destination_type": "local",
			},
			wantError: true,
		},
		{
			name: "invalid key type",
			setupStorage: func() *mockLifecycleStorage {
				return newMockLifecycleStorage()
			},
			args: map[string]any{
				"key":              123,
				"destination_type": "local",
			},
			wantError: true,
		},
		{
			name: "object not found",
			setupStorage: func() *mockLifecycleStorage {
				return newMockLifecycleStorage()
			},
			args: map[string]any{
				"key":              "nonexistent.txt",
				"destination_type": "local",
			},
			wantError: true,
		},
		{
			name: "storage error",
			setupStorage: func() *mockLifecycleStorage {
				storage := newMockLifecycleStorage()
				storage.PutWithContext(context.Background(), "test.txt", strings.NewReader("content"))
				storage.archiveError = errors.New("storage error")
				return storage
			},
			args: map[string]any{
				"key":              "test.txt",
				"destination_type": "glacier",
			},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage := tt.setupStorage()
			executor := NewToolExecutor(storage)

			result, err := executor.Execute(context.Background(), "objstore_archive", tt.args)

			if tt.wantError {
				if err == nil {
					t.Errorf("Execute(objstore_archive) error = nil, wantError %v", tt.wantError)
				}
			} else {
				if err != nil {
					t.Errorf("Execute(objstore_archive) unexpected error = %v", err)
				}
				if !strings.Contains(result, tt.wantContains) {
					t.Errorf("Execute(objstore_archive) result = %v, want to contain %v", result, tt.wantContains)
				}
			}
		})
	}
}

// TestExecuteAddPolicyTool tests the add policy tool execution
func TestExecuteAddPolicyTool(t *testing.T) {
	tests := []struct {
		name         string
		setupStorage func() *mockLifecycleStorage
		args         map[string]any
		wantError    bool
		wantContains string
	}{
		{
			name: "successful add delete policy",
			setupStorage: func() *mockLifecycleStorage {
				return newMockLifecycleStorage()
			},
			args: map[string]any{
				"id":                "policy1",
				"prefix":            "logs/",
				"retention_seconds": float64(86400),
				"action":            "delete",
			},
			wantError:    false,
			wantContains: "success",
		},
		{
			name: "successful add archive policy",
			setupStorage: func() *mockLifecycleStorage {
				return newMockLifecycleStorage()
			},
			args: map[string]any{
				"id":                "policy2",
				"prefix":            "data/",
				"retention_seconds": float64(2592000),
				"action":            "archive",
				"destination_type":  "local",
				"destination_settings": map[string]any{
					"path": "/tmp/archive-test",
				},
			},
			// Note: This may fail if the archiver backend is not available due to build constraints
			// The factory returns ErrUnknownArchiver when the backend isn't registered
			wantError:    false,
			wantContains: "",
		},
		{
			name: "missing policy ID",
			setupStorage: func() *mockLifecycleStorage {
				return newMockLifecycleStorage()
			},
			args: map[string]any{
				"prefix":            "logs/",
				"retention_seconds": float64(86400),
				"action":            "delete",
			},
			wantError: true,
		},
		{
			name: "invalid ID type",
			setupStorage: func() *mockLifecycleStorage {
				return newMockLifecycleStorage()
			},
			args: map[string]any{
				"id":                123,
				"prefix":            "logs/",
				"retention_seconds": float64(86400),
				"action":            "delete",
			},
			wantError: true,
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
			args: map[string]any{
				"id":                "existing",
				"prefix":            "logs/",
				"retention_seconds": float64(86400),
				"action":            "delete",
			},
			wantError: true,
		},
		{
			name: "storage error",
			setupStorage: func() *mockLifecycleStorage {
				storage := newMockLifecycleStorage()
				storage.addPolicyError = errors.New("storage error")
				return storage
			},
			args: map[string]any{
				"id":                "policy3",
				"prefix":            "logs/",
				"retention_seconds": float64(86400),
				"action":            "delete",
			},
			wantError: true,
		},
		{
			name: "missing action",
			setupStorage: func() *mockLifecycleStorage {
				return newMockLifecycleStorage()
			},
			args: map[string]any{
				"id":                "policy4",
				"prefix":            "logs/",
				"retention_seconds": float64(86400),
			},
			wantError: true,
		},
		{
			name: "invalid action",
			setupStorage: func() *mockLifecycleStorage {
				return newMockLifecycleStorage()
			},
			args: map[string]any{
				"id":                "policy5",
				"prefix":            "logs/",
				"retention_seconds": float64(86400),
				"action":            "invalid",
			},
			wantError: true,
		},
		{
			name: "missing retention",
			setupStorage: func() *mockLifecycleStorage {
				return newMockLifecycleStorage()
			},
			args: map[string]any{
				"id":     "policy6",
				"prefix": "logs/",
				"action": "delete",
			},
			wantError: true,
		},
		{
			name: "invalid retention type",
			setupStorage: func() *mockLifecycleStorage {
				return newMockLifecycleStorage()
			},
			args: map[string]any{
				"id":                "policy7",
				"prefix":            "logs/",
				"retention_seconds": "not-a-number",
				"action":            "delete",
			},
			wantError: true,
		},
		{
			name: "negative retention",
			setupStorage: func() *mockLifecycleStorage {
				return newMockLifecycleStorage()
			},
			args: map[string]any{
				"id":                "policy8",
				"prefix":            "logs/",
				"retention_seconds": float64(-100),
				"action":            "delete",
			},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage := tt.setupStorage()
			executor := NewToolExecutor(storage)

			result, err := executor.Execute(context.Background(), "objstore_add_policy", tt.args)

			if tt.wantError {
				if err == nil {
					t.Errorf("Execute(objstore_add_policy) error = nil, wantError %v", tt.wantError)
				}
			} else {
				if err != nil {
					t.Errorf("Execute(objstore_add_policy) unexpected error = %v", err)
				}
				if !strings.Contains(result, tt.wantContains) {
					t.Errorf("Execute(objstore_add_policy) result = %v, want to contain %v", result, tt.wantContains)
				}
			}
		})
	}
}

// TestExecuteRemovePolicyTool tests the remove policy tool execution
func TestExecuteRemovePolicyTool(t *testing.T) {
	tests := []struct {
		name         string
		setupStorage func() *mockLifecycleStorage
		args         map[string]any
		wantError    bool
		wantContains string
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
			args: map[string]any{
				"id": "policy1",
			},
			wantError:    false,
			wantContains: "success",
		},
		{
			name: "missing policy ID",
			setupStorage: func() *mockLifecycleStorage {
				return newMockLifecycleStorage()
			},
			args:      map[string]any{},
			wantError: true,
		},
		{
			name: "invalid ID type",
			setupStorage: func() *mockLifecycleStorage {
				return newMockLifecycleStorage()
			},
			args: map[string]any{
				"id": 123,
			},
			wantError: true,
		},
		{
			name: "policy not found",
			setupStorage: func() *mockLifecycleStorage {
				return newMockLifecycleStorage()
			},
			args: map[string]any{
				"id": "nonexistent",
			},
			wantError: true,
		},
		{
			name: "storage error",
			setupStorage: func() *mockLifecycleStorage {
				storage := newMockLifecycleStorage()
				storage.removePolicyError = errors.New("storage error")
				return storage
			},
			args: map[string]any{
				"id": "policy1",
			},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage := tt.setupStorage()
			executor := NewToolExecutor(storage)

			result, err := executor.Execute(context.Background(), "objstore_remove_policy", tt.args)

			if tt.wantError {
				if err == nil {
					t.Errorf("Execute(objstore_remove_policy) error = nil, wantError %v", tt.wantError)
				}
			} else {
				if err != nil {
					t.Errorf("Execute(objstore_remove_policy) unexpected error = %v", err)
				}
				if !strings.Contains(result, tt.wantContains) {
					t.Errorf("Execute(objstore_remove_policy) result = %v, want to contain %v", result, tt.wantContains)
				}
			}
		})
	}
}

// TestExecuteGetPoliciesTool tests the get policies tool execution
func TestExecuteGetPoliciesTool(t *testing.T) {
	tests := []struct {
		name         string
		setupStorage func() *mockLifecycleStorage
		args         map[string]any
		wantError    bool
		wantCount    int
	}{
		{
			name: "get all policies",
			setupStorage: func() *mockLifecycleStorage {
				storage := newMockLifecycleStorage()
				storage.policies = []common.LifecyclePolicy{
					{ID: "policy1", Prefix: "logs/", Retention: 24 * time.Hour, Action: "delete"},
					{ID: "policy2", Prefix: "data/", Retention: 30 * 24 * time.Hour, Action: "archive"},
				}
				return storage
			},
			args:      map[string]any{},
			wantError: false,
			wantCount: 2,
		},
		{
			name: "filter by prefix",
			setupStorage: func() *mockLifecycleStorage {
				storage := newMockLifecycleStorage()
				storage.policies = []common.LifecyclePolicy{
					{ID: "policy1", Prefix: "logs/", Retention: 24 * time.Hour, Action: "delete"},
					{ID: "policy2", Prefix: "data/", Retention: 30 * 24 * time.Hour, Action: "delete"},
					{ID: "policy3", Prefix: "logs/", Retention: 7 * 24 * time.Hour, Action: "archive"},
				}
				return storage
			},
			args: map[string]any{
				"prefix": "logs/",
			},
			wantError: false,
			wantCount: 2,
		},
		{
			name: "no policies",
			setupStorage: func() *mockLifecycleStorage {
				return newMockLifecycleStorage()
			},
			args:      map[string]any{},
			wantError: false,
			wantCount: 0,
		},
		{
			name: "storage error",
			setupStorage: func() *mockLifecycleStorage {
				storage := newMockLifecycleStorage()
				storage.getPoliciesError = errors.New("storage error")
				return storage
			},
			args:      map[string]any{},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage := tt.setupStorage()
			executor := NewToolExecutor(storage)

			result, err := executor.Execute(context.Background(), "objstore_get_policies", tt.args)

			if tt.wantError {
				if err == nil {
					t.Errorf("Execute(objstore_get_policies) error = nil, wantError %v", tt.wantError)
				}
			} else {
				if err != nil {
					t.Errorf("Execute(objstore_get_policies) unexpected error = %v", err)
				}

				// Parse result
				var response map[string]any
				if err := json.Unmarshal([]byte(result), &response); err != nil {
					t.Fatalf("Failed to unmarshal result: %v", err)
				}

				policies, ok := response["policies"].([]any)
				if !ok {
					t.Fatal("Result missing policies field")
				}

				if len(policies) != tt.wantCount {
					t.Errorf("Execute(objstore_get_policies) count = %d, want %d", len(policies), tt.wantCount)
				}
			}
		})
	}
}

// TestToolRegistryLifecycleTools tests that lifecycle tools are registered
func TestToolRegistryLifecycleTools(t *testing.T) {
	registry := NewToolRegistry()
	registry.RegisterDefaultTools()

	lifecycleTools := []string{
		"objstore_archive",
		"objstore_add_policy",
		"objstore_remove_policy",
		"objstore_get_policies",
	}

	for _, toolName := range lifecycleTools {
		t.Run(toolName, func(t *testing.T) {
			tool, ok := registry.GetTool(toolName)
			if !ok {
				t.Errorf("Tool %s not registered", toolName)
			}
			if tool.Name != toolName {
				t.Errorf("Tool name = %v, want %v", tool.Name, toolName)
			}
			if tool.Description == "" {
				t.Error("Tool description is empty")
			}
			if tool.InputSchema == nil {
				t.Error("Tool input schema is nil")
			}
		})
	}
}

// Note: The actual MCP tool implementations (executeArchive, executeAddPolicy, etc.)
// would need to be added to tools.go. These tests assume those implementations exist.
// The test cases cover the expected behavior when those methods are implemented.

func TestToolExecutor_executeApplyPolicies_NoPolicies(t *testing.T) {
	storage := newMockLifecycleStorage()
	executor := NewToolExecutor(storage)

	result, err := executor.executeApplyPolicies(context.Background(), map[string]any{})
	if err != nil {
		t.Fatalf("executeApplyPolicies failed: %v", err)
	}

	var resultMap map[string]any
	if err := json.Unmarshal([]byte(result), &resultMap); err != nil {
		t.Fatalf("Failed to parse result JSON: %v", err)
	}

	if resultMap["policies_count"].(float64) != 0 {
		t.Errorf("Expected policies_count 0, got %v", resultMap["policies_count"])
	}
	if resultMap["objects_processed"].(float64) != 0 {
		t.Errorf("Expected objects_processed 0, got %v", resultMap["objects_processed"])
	}
}

func TestToolExecutor_executeApplyPolicies_WithPolicies(t *testing.T) {
	storage := newMockLifecycleStorage()

	// Add some test objects
	storage.PutWithContext(context.Background(), "old/file1.txt", bytes.NewReader([]byte("data1")))
	storage.PutWithContext(context.Background(), "old/file2.txt", bytes.NewReader([]byte("data2")))
	storage.PutWithContext(context.Background(), "new/file3.txt", bytes.NewReader([]byte("data3")))

	// Set old timestamps for old/ files
	oldTime := time.Now().Add(-48 * time.Hour)
	storage.metadata["old/file1.txt"].LastModified = oldTime
	storage.metadata["old/file2.txt"].LastModified = oldTime

	// Add policy to delete old files
	policy := common.LifecyclePolicy{
		ID:        "cleanup-old",
		Prefix:    "old/",
		Retention: 24 * time.Hour,
		Action:    "delete",
	}
	storage.AddPolicy(policy)

	executor := NewToolExecutor(storage)
	result, err := executor.executeApplyPolicies(context.Background(), map[string]any{})
	if err != nil {
		t.Fatalf("executeApplyPolicies failed: %v", err)
	}

	// Check result contains expected fields
	if !strings.Contains(result, "policies_count") {
		t.Error("Result missing policies_count")
	}
	if !strings.Contains(result, "objects_processed") {
		t.Error("Result missing objects_processed")
	}
}

func TestToolExecutor_executeApplyPolicies_GetPoliciesError(t *testing.T) {
	storage := newMockLifecycleStorage()
	storage.getPoliciesError = errors.New("database error")

	executor := NewToolExecutor(storage)
	_, err := executor.executeApplyPolicies(context.Background(), map[string]any{})
	if err == nil {
		t.Error("Expected error from GetPolicies, got nil")
	}
}
