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
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// MockStorage implements common.Storage for testing
type MockStorage struct {
	objects  map[string][]byte
	metadata map[string]*common.Metadata
}

func NewMockStorage() *MockStorage {
	return &MockStorage{
		objects:  make(map[string][]byte),
		metadata: make(map[string]*common.Metadata),
	}
}

func (m *MockStorage) Configure(settings map[string]string) error {
	return nil
}

func (m *MockStorage) Put(key string, data io.Reader) error {
	return m.PutWithContext(context.Background(), key, data)
}

func (m *MockStorage) PutWithContext(ctx context.Context, key string, data io.Reader) error {
	content, err := readAll(data)
	if err != nil {
		return err
	}
	m.objects[key] = content
	m.metadata[key] = &common.Metadata{
		Size:         int64(len(content)),
		LastModified: time.Now(),
	}
	return nil
}

func (m *MockStorage) PutWithMetadata(ctx context.Context, key string, data io.Reader, metadata *common.Metadata) error {
	content, err := readAll(data)
	if err != nil {
		return err
	}
	m.objects[key] = content
	metadata.Size = int64(len(content))
	metadata.LastModified = time.Now()
	m.metadata[key] = metadata
	return nil
}

func (m *MockStorage) Get(key string) (io.ReadCloser, error) {
	return m.GetWithContext(context.Background(), key)
}

func (m *MockStorage) GetWithContext(ctx context.Context, key string) (io.ReadCloser, error) {
	content, ok := m.objects[key]
	if !ok {
		return nil, fmt.Errorf("object not found")
	}
	return &mockReadCloser{strings.NewReader(string(content))}, nil
}

func (m *MockStorage) GetMetadata(ctx context.Context, key string) (*common.Metadata, error) {
	metadata, ok := m.metadata[key]
	if !ok {
		return nil, fmt.Errorf("object not found")
	}
	return metadata, nil
}

func (m *MockStorage) UpdateMetadata(ctx context.Context, key string, metadata *common.Metadata) error {
	if _, ok := m.objects[key]; !ok {
		return fmt.Errorf("object not found")
	}
	m.metadata[key] = metadata
	return nil
}

func (m *MockStorage) Delete(key string) error {
	return m.DeleteWithContext(context.Background(), key)
}

func (m *MockStorage) DeleteWithContext(ctx context.Context, key string) error {
	delete(m.objects, key)
	delete(m.metadata, key)
	return nil
}

func (m *MockStorage) Exists(ctx context.Context, key string) (bool, error) {
	_, ok := m.objects[key]
	return ok, nil
}

func (m *MockStorage) List(prefix string) ([]string, error) {
	return m.ListWithContext(context.Background(), prefix)
}

func (m *MockStorage) ListWithContext(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	for key := range m.objects {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	return keys, nil
}

func (m *MockStorage) ListWithOptions(ctx context.Context, opts *common.ListOptions) (*common.ListResult, error) {
	var objects []*common.ObjectInfo
	for key, content := range m.objects {
		if strings.HasPrefix(key, opts.Prefix) {
			meta := m.metadata[key]
			if meta == nil {
				meta = &common.Metadata{
					Size:         int64(len(content)),
					LastModified: time.Now(),
				}
			}
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

func (m *MockStorage) Archive(key string, destination common.Archiver) error {
	return nil
}

func (m *MockStorage) SetLifecyclePolicy(policy *common.LifecyclePolicy) error {
	return nil
}

func (m *MockStorage) GetLifecyclePolicy() *common.LifecyclePolicy {
	return nil
}

func (m *MockStorage) RunLifecycle(ctx context.Context) error {
	return nil
}

func (m *MockStorage) AddPolicy(policy common.LifecyclePolicy) error {
	return nil
}

func (m *MockStorage) RemovePolicy(id string) error {
	return nil
}

func (m *MockStorage) GetPolicies() ([]common.LifecyclePolicy, error) {
	return []common.LifecyclePolicy{}, nil
}

func readAll(data io.Reader) ([]byte, error) {
	buf := make([]byte, 0, 4096)
	tmp := make([]byte, 4096)
	for {
		n, err := data.Read(tmp)
		buf = append(buf, tmp[:n]...)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
	}
	return buf, nil
}

// ErrorMockStorage implements common.Storage but returns errors for testing
type ErrorMockStorage struct {
	putError      bool
	getError      bool
	deleteError   bool
	existsError   bool
	metadataError bool
	listError     bool
	readError     bool
}

func (m *ErrorMockStorage) Configure(settings map[string]string) error {
	return nil
}

func (m *ErrorMockStorage) Put(key string, data io.Reader) error {
	if m.putError {
		return fmt.Errorf("storage error")
	}
	return nil
}

func (m *ErrorMockStorage) PutWithContext(ctx context.Context, key string, data io.Reader) error {
	if m.putError {
		return fmt.Errorf("storage error")
	}
	return nil
}

func (m *ErrorMockStorage) PutWithMetadata(ctx context.Context, key string, data io.Reader, metadata *common.Metadata) error {
	if m.putError {
		return fmt.Errorf("storage error")
	}
	return nil
}

func (m *ErrorMockStorage) Get(key string) (io.ReadCloser, error) {
	if m.getError {
		return nil, fmt.Errorf("storage error")
	}
	return nil, nil
}

func (m *ErrorMockStorage) GetWithContext(ctx context.Context, key string) (io.ReadCloser, error) {
	if m.getError {
		return nil, fmt.Errorf("storage error")
	}
	if m.readError {
		return &badReadCloser{}, nil
	}
	return nil, nil
}

func (m *ErrorMockStorage) GetMetadata(ctx context.Context, key string) (*common.Metadata, error) {
	if m.metadataError {
		return nil, fmt.Errorf("storage error")
	}
	return nil, nil
}

func (m *ErrorMockStorage) UpdateMetadata(ctx context.Context, key string, metadata *common.Metadata) error {
	return nil
}

func (m *ErrorMockStorage) Delete(key string) error {
	if m.deleteError {
		return fmt.Errorf("storage error")
	}
	return nil
}

func (m *ErrorMockStorage) DeleteWithContext(ctx context.Context, key string) error {
	if m.deleteError {
		return fmt.Errorf("storage error")
	}
	return nil
}

func (m *ErrorMockStorage) Exists(ctx context.Context, key string) (bool, error) {
	if m.existsError {
		return false, fmt.Errorf("storage error")
	}
	return false, nil
}

func (m *ErrorMockStorage) List(prefix string) ([]string, error) {
	return nil, nil
}

func (m *ErrorMockStorage) ListWithContext(ctx context.Context, prefix string) ([]string, error) {
	return nil, nil
}

func (m *ErrorMockStorage) ListWithOptions(ctx context.Context, opts *common.ListOptions) (*common.ListResult, error) {
	if m.listError {
		return nil, fmt.Errorf("storage error")
	}
	return &common.ListResult{}, nil
}

func (m *ErrorMockStorage) Archive(key string, destination common.Archiver) error {
	return nil
}

func (m *ErrorMockStorage) SetLifecyclePolicy(policy *common.LifecyclePolicy) error {
	return nil
}

func (m *ErrorMockStorage) GetLifecyclePolicy() *common.LifecyclePolicy {
	return nil
}

func (m *ErrorMockStorage) RunLifecycle(ctx context.Context) error {
	return nil
}

func (m *ErrorMockStorage) AddPolicy(policy common.LifecyclePolicy) error {
	return nil
}

func (m *ErrorMockStorage) RemovePolicy(id string) error {
	return nil
}

func (m *ErrorMockStorage) GetPolicies() ([]common.LifecyclePolicy, error) {
	return []common.LifecyclePolicy{}, nil
}

func TestToolRegistry(t *testing.T) {
	registry := NewToolRegistry()
	registry.RegisterDefaultTools()

	tools := registry.ListTools()
	if len(tools) != 19 {
		t.Errorf("expected 19 tools, got %d", len(tools))
	}

	expectedTools := []string{
		"objstore_put",
		"objstore_get",
		"objstore_delete",
		"objstore_list",
		"objstore_exists",
		"objstore_get_metadata",
		"objstore_update_metadata",
		"objstore_health",
		"objstore_archive",
		"objstore_add_policy",
		"objstore_remove_policy",
		"objstore_get_policies",
		"objstore_apply_policies",
		"objstore_add_replication_policy",
		"objstore_remove_replication_policy",
		"objstore_list_replication_policies",
		"objstore_get_replication_policy",
		"objstore_trigger_replication",
	}

	for _, expectedTool := range expectedTools {
		tool, ok := registry.GetTool(expectedTool)
		if !ok {
			t.Errorf("tool %s not found", expectedTool)
		}
		if tool.Name != expectedTool {
			t.Errorf("expected tool name %s, got %s", expectedTool, tool.Name)
		}
		if tool.Description == "" {
			t.Errorf("tool %s has no description", expectedTool)
		}
		if tool.InputSchema == nil {
			t.Errorf("tool %s has no input schema", expectedTool)
		}
	}
}

func TestToolRegistry_GetTool(t *testing.T) {
	registry := NewToolRegistry()
	registry.RegisterDefaultTools()

	// Test getting existing tool
	tool, ok := registry.GetTool("objstore_put")
	if !ok {
		t.Error("expected to find objstore_put")
	}
	if tool.Name != "objstore_put" {
		t.Errorf("expected tool name objstore_put, got %s", tool.Name)
	}

	// Test getting non-existent tool
	_, ok = registry.GetTool("nonexistent")
	if ok {
		t.Error("expected not to find nonexistent tool")
	}
}

func TestToolExecutor_ExecutePut(t *testing.T) {
	storage := NewMockStorage()
	executor := createTestToolExecutor(t, storage)

	tests := []struct {
		name      string
		args      map[string]any
		wantError bool
	}{
		{
			name: "valid put",
			args: map[string]any{
				"key":  "test/file.txt",
				"data": "hello world",
			},
			wantError: false,
		},
		{
			name: "put with metadata",
			args: map[string]any{
				"key":  "test/file2.txt",
				"data": "hello world",
				"metadata": map[string]any{
					"content_type": "text/plain",
					"custom": map[string]any{
						"author": "test",
					},
				},
			},
			wantError: false,
		},
		{
			name: "missing key",
			args: map[string]any{
				"data": "hello world",
			},
			wantError: true,
		},
		{
			name: "missing data",
			args: map[string]any{
				"key": "test/file.txt",
			},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executor.Execute(context.Background(), "objstore_put", tt.args)
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

func TestToolExecutor_ExecuteGet(t *testing.T) {
	storage := NewMockStorage()
	executor := createTestToolExecutor(t, storage)

	// Put some test data
	storage.PutWithContext(context.Background(), "test/file.txt", strings.NewReader("hello world"))

	tests := []struct {
		name      string
		args      map[string]any
		wantError bool
	}{
		{
			name: "valid get",
			args: map[string]any{
				"key": "test/file.txt",
			},
			wantError: false,
		},
		{
			name: "missing key",
			args: map[string]any{
				"key": "nonexistent.txt",
			},
			wantError: true,
		},
		{
			name:      "no key parameter",
			args:      map[string]any{},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executor.Execute(context.Background(), "objstore_get", tt.args)
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
			if resultMap["data"].(string) != "hello world" {
				t.Errorf("expected data 'hello world', got '%s'", resultMap["data"])
			}
		})
	}
}

func TestToolExecutor_ExecuteDelete(t *testing.T) {
	storage := NewMockStorage()
	executor := createTestToolExecutor(t, storage)

	// Put some test data
	storage.PutWithContext(context.Background(), "test/file.txt", strings.NewReader("hello world"))

	tests := []struct {
		name      string
		args      map[string]any
		wantError bool
	}{
		{
			name: "valid delete",
			args: map[string]any{
				"key": "test/file.txt",
			},
			wantError: false,
		},
		{
			name:      "no key parameter",
			args:      map[string]any{},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executor.Execute(context.Background(), "objstore_delete", tt.args)
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

			// Verify object is deleted
			exists, _ := storage.Exists(context.Background(), tt.args["key"].(string))
			if exists {
				t.Error("expected object to be deleted")
			}
		})
	}
}

func TestToolExecutor_ExecuteList(t *testing.T) {
	storage := NewMockStorage()
	executor := createTestToolExecutor(t, storage)

	// Put some test data
	storage.PutWithContext(context.Background(), "test/file1.txt", strings.NewReader("data1"))
	storage.PutWithContext(context.Background(), "test/file2.txt", strings.NewReader("data2"))
	storage.PutWithContext(context.Background(), "other/file3.txt", strings.NewReader("data3"))

	tests := []struct {
		name          string
		args          map[string]any
		expectedCount int
		wantError     bool
	}{
		{
			name: "list with prefix",
			args: map[string]any{
				"prefix": "test/",
			},
			expectedCount: 2,
			wantError:     false,
		},
		{
			name:          "list all",
			args:          map[string]any{},
			expectedCount: 3,
			wantError:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executor.Execute(context.Background(), "objstore_list", tt.args)
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

			count := int(resultMap["count"].(float64))
			if count != tt.expectedCount {
				t.Errorf("expected %d objects, got %d", tt.expectedCount, count)
			}
		})
	}
}

func TestToolExecutor_ExecuteExists(t *testing.T) {
	storage := NewMockStorage()
	executor := createTestToolExecutor(t, storage)

	// Put some test data
	storage.PutWithContext(context.Background(), "test/file.txt", strings.NewReader("hello world"))

	tests := []struct {
		name           string
		args           map[string]any
		expectedExists bool
		wantError      bool
	}{
		{
			name: "existing object",
			args: map[string]any{
				"key": "test/file.txt",
			},
			expectedExists: true,
			wantError:      false,
		},
		{
			name: "non-existing object",
			args: map[string]any{
				"key": "nonexistent.txt",
			},
			expectedExists: false,
			wantError:      false,
		},
		{
			name:      "no key parameter",
			args:      map[string]any{},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executor.Execute(context.Background(), "objstore_exists", tt.args)
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
			if resultMap["exists"].(bool) != tt.expectedExists {
				t.Errorf("expected exists=%v, got %v", tt.expectedExists, resultMap["exists"])
			}
		})
	}
}

func TestToolExecutor_ExecuteGetMetadata(t *testing.T) {
	storage := NewMockStorage()
	executor := createTestToolExecutor(t, storage)

	// Put some test data with metadata
	metadata := &common.Metadata{
		ContentType: "text/plain",
		Custom: map[string]string{
			"author": "test",
		},
	}
	storage.PutWithMetadata(context.Background(), "test/file.txt", strings.NewReader("hello world"), metadata)

	tests := []struct {
		name      string
		args      map[string]any
		wantError bool
	}{
		{
			name: "valid get metadata",
			args: map[string]any{
				"key": "test/file.txt",
			},
			wantError: false,
		},
		{
			name: "non-existing object",
			args: map[string]any{
				"key": "nonexistent.txt",
			},
			wantError: true,
		},
		{
			name:      "no key parameter",
			args:      map[string]any{},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executor.Execute(context.Background(), "objstore_get_metadata", tt.args)
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
			if resultMap["content_type"].(string) != "text/plain" {
				t.Errorf("expected content_type 'text/plain', got '%s'", resultMap["content_type"])
			}
		})
	}
}

func TestToolExecutor_UnknownTool(t *testing.T) {
	storage := NewMockStorage()
	executor := createTestToolExecutor(t, storage)

	_, err := executor.Execute(context.Background(), "unknown_tool", map[string]any{})
	if err == nil {
		t.Error("expected error for unknown tool")
	}
	if !strings.Contains(err.Error(), "unknown tool") {
		t.Errorf("expected 'unknown tool' error, got: %v", err)
	}
}

func TestToolExecutor_StorageErrorOnPut(t *testing.T) {
	storage := &ErrorMockStorage{
		putError: true,
	}
	executor := createTestToolExecutor(t, storage)

	_, err := executor.Execute(context.Background(), "objstore_put", map[string]any{
		"key":  "test.txt",
		"data": "hello",
	})
	if err == nil {
		t.Error("expected error when storage put fails")
	}
	// Error is propagated from storage - just verify we got an error
	if err.Error() != "storage error" {
		t.Errorf("expected storage error to be propagated, got: %v", err)
	}
}

func TestToolExecutor_StorageErrorOnGet(t *testing.T) {
	storage := &ErrorMockStorage{
		getError: true,
	}
	executor := createTestToolExecutor(t, storage)

	_, err := executor.Execute(context.Background(), "objstore_get", map[string]any{
		"key": "test.txt",
	})
	if err == nil {
		t.Error("expected error when storage get fails")
	}
	// Error is propagated from storage - just verify we got an error
	if err.Error() != "storage error" {
		t.Errorf("expected storage error to be propagated, got: %v", err)
	}
}

func TestToolExecutor_GetWithReadError(t *testing.T) {
	storage := &ErrorMockStorage{
		readError: true,
	}
	executor := createTestToolExecutor(t, storage)

	_, err := executor.Execute(context.Background(), "objstore_get", map[string]any{
		"key": "test.txt",
	})
	if err == nil {
		t.Error("expected error when read fails")
	}
	// Error is propagated from storage - just verify we got an error
	if err.Error() != "read error" {
		t.Errorf("expected read error to be propagated, got: %v", err)
	}
}

func TestToolExecutor_StorageErrorOnDelete(t *testing.T) {
	storage := &ErrorMockStorage{
		deleteError: true,
	}
	executor := createTestToolExecutor(t, storage)

	_, err := executor.Execute(context.Background(), "objstore_delete", map[string]any{
		"key": "test.txt",
	})
	if err == nil {
		t.Error("expected error when storage delete fails")
	}
	// Error is propagated from storage - just verify we got an error
	if err.Error() != "storage error" {
		t.Errorf("expected storage error to be propagated, got: %v", err)
	}
}

func TestToolExecutor_StorageErrorOnExists(t *testing.T) {
	storage := &ErrorMockStorage{
		existsError: true,
	}
	executor := createTestToolExecutor(t, storage)

	_, err := executor.Execute(context.Background(), "objstore_exists", map[string]any{
		"key": "test.txt",
	})
	if err == nil {
		t.Error("expected error when storage exists fails")
	}
	// Error is propagated from storage - just verify we got an error
	if err.Error() != "storage error" {
		t.Errorf("expected storage error to be propagated, got: %v", err)
	}
}

func TestToolExecutor_StorageErrorOnGetMetadata(t *testing.T) {
	storage := &ErrorMockStorage{
		metadataError: true,
	}
	executor := createTestToolExecutor(t, storage)

	_, err := executor.Execute(context.Background(), "objstore_get_metadata", map[string]any{
		"key": "test.txt",
	})
	if err == nil {
		t.Error("expected error when storage metadata fails")
	}
	// Error is propagated from storage - just verify we got an error
	if err.Error() != "storage error" {
		t.Errorf("expected storage error to be propagated, got: %v", err)
	}
}

func TestToolExecutor_StorageErrorOnList(t *testing.T) {
	storage := &ErrorMockStorage{
		listError: true,
	}
	executor := createTestToolExecutor(t, storage)

	_, err := executor.Execute(context.Background(), "objstore_list", map[string]any{
		"prefix": "test/",
	})
	if err == nil {
		t.Error("expected error when storage list fails")
	}
	// Error is propagated from storage - just verify we got an error
	if err.Error() != "storage error" {
		t.Errorf("expected storage error to be propagated, got: %v", err)
	}
}

func TestToolExecutor_ExecuteUpdateMetadata(t *testing.T) {
	storage := NewMockStorage()
	executor := createTestToolExecutor(t, storage)

	// First put an object
	_, err := executor.Execute(context.Background(), "objstore_put", map[string]any{
		"key":  "test-key",
		"data": "test data",
	})
	if err != nil {
		t.Fatalf("failed to put object: %v", err)
	}

	tests := []struct {
		name      string
		args      map[string]any
		wantError bool
	}{
		{
			name: "successful metadata update",
			args: map[string]any{
				"key": "test-key",
				"metadata": map[string]any{
					"content_type":     "application/json",
					"content_encoding": "gzip",
					"custom": map[string]string{
						"author": "test",
					},
				},
			},
			wantError: false,
		},
		{
			name: "missing key",
			args: map[string]any{
				"metadata": map[string]any{
					"content_type": "text/plain",
				},
			},
			wantError: true,
		},
		{
			name: "invalid key type",
			args: map[string]any{
				"key":      123,
				"metadata": map[string]any{},
			},
			wantError: true,
		},
		{
			name: "missing metadata",
			args: map[string]any{
				"key": "test-key",
			},
			wantError: true,
		},
		{
			name: "custom metadata with mixed types",
			args: map[string]any{
				"key": "test-key",
				"metadata": map[string]any{
					"custom": map[string]any{
						"string_val": "valid",
						"int_val":    123,
						"bool_val":   true,
					},
				},
			},
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executor.Execute(context.Background(), "objstore_update_metadata", tt.args)
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

func TestToolExecutor_ExecuteHealth(t *testing.T) {
	storage := NewMockStorage()
	executor := createTestToolExecutor(t, storage)

	result, err := executor.Execute(context.Background(), "objstore_health", map[string]any{})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	var resultMap map[string]any
	if err := json.Unmarshal([]byte(result), &resultMap); err != nil {
		t.Errorf("failed to parse result: %v", err)
	}

	if status, ok := resultMap["status"]; !ok || status != "healthy" {
		t.Error("expected status to be 'healthy'")
	}

	if version, ok := resultMap["version"]; !ok || version == "" {
		t.Error("expected version to be present")
	}
}
