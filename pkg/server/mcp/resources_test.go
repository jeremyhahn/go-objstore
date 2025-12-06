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
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

func TestResourceManager_ListResources(t *testing.T) {
	storage := NewMockStorage()
	manager := createTestResourceManager(t, storage, "")

	// Add test objects
	storage.PutWithContext(context.Background(), "file1.txt", strings.NewReader("data1"))
	storage.PutWithContext(context.Background(), "file2.txt", strings.NewReader("data2"))
	storage.PutWithContext(context.Background(), "dir/file3.txt", strings.NewReader("data3"))

	resources, err := manager.ListResources(context.Background(), "")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if len(resources) != 3 {
		t.Errorf("expected 3 resources, got %d", len(resources))
	}

	for _, resource := range resources {
		if resource.URI == "" {
			t.Error("expected resource URI to be set")
		}
		if resource.Name == "" {
			t.Error("expected resource name to be set")
		}
		if !strings.HasPrefix(resource.URI, "objstore://") {
			t.Errorf("expected URI to start with 'objstore://', got %s", resource.URI)
		}
	}
}

func TestResourceManager_ListResourcesWithPrefix(t *testing.T) {
	storage := NewMockStorage()
	manager := createTestResourceManager(t, storage, "test/")

	// Add test objects
	storage.PutWithContext(context.Background(), "test/file1.txt", strings.NewReader("data1"))
	storage.PutWithContext(context.Background(), "test/file2.txt", strings.NewReader("data2"))
	storage.PutWithContext(context.Background(), "other/file3.txt", strings.NewReader("data3"))

	resources, err := manager.ListResources(context.Background(), "")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// Should only return objects with "test/" prefix
	if len(resources) != 2 {
		t.Errorf("expected 2 resources, got %d", len(resources))
	}

	for _, resource := range resources {
		if !strings.Contains(resource.URI, "test/") {
			t.Errorf("expected resource URI to contain 'test/', got %s", resource.URI)
		}
	}
}

func TestResourceManager_ReadResource(t *testing.T) {
	storage := NewMockStorage()
	manager := createTestResourceManager(t, storage, "")

	// Add test object
	testContent := "hello world"
	storage.PutWithContext(context.Background(), "test/file.txt", strings.NewReader(testContent))

	tests := []struct {
		name      string
		uri       string
		wantError bool
	}{
		{
			name:      "read with objstore:// prefix",
			uri:       "objstore://test/file.txt",
			wantError: false,
		},
		{
			name:      "read without prefix",
			uri:       "test/file.txt",
			wantError: false,
		},
		{
			name:      "read non-existent",
			uri:       "objstore://nonexistent.txt",
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			content, mimeType, err := manager.ReadResource(context.Background(), tt.uri)
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

			if content != testContent {
				t.Errorf("expected content '%s', got '%s'", testContent, content)
			}

			if mimeType == "" {
				t.Error("expected mimeType to be set")
			}
		})
	}
}

func TestResourceManager_ObjectKeyToURI(t *testing.T) {
	manager := NewResourceManager("", "")

	tests := []struct {
		key         string
		expectedURI string
	}{
		{
			key:         "file.txt",
			expectedURI: "objstore://file.txt",
		},
		{
			key:         "dir/file.txt",
			expectedURI: "objstore://dir/file.txt",
		},
		{
			key:         "deeply/nested/path/file.txt",
			expectedURI: "objstore://deeply/nested/path/file.txt",
		},
	}

	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			uri := manager.objectKeyToURI(tt.key)
			if uri != tt.expectedURI {
				t.Errorf("expected URI '%s', got '%s'", tt.expectedURI, uri)
			}
		})
	}
}

func TestResourceManager_URIToObjectKey(t *testing.T) {
	manager := NewResourceManager("", "")

	tests := []struct {
		uri         string
		expectedKey string
	}{
		{
			uri:         "objstore://file.txt",
			expectedKey: "file.txt",
		},
		{
			uri:         "objstore://dir/file.txt",
			expectedKey: "dir/file.txt",
		},
		{
			uri:         "file.txt",
			expectedKey: "file.txt",
		},
	}

	for _, tt := range tests {
		t.Run(tt.uri, func(t *testing.T) {
			key := manager.uriToObjectKey(tt.uri)
			if key != tt.expectedKey {
				t.Errorf("expected key '%s', got '%s'", tt.expectedKey, key)
			}
		})
	}
}

func TestResourceManager_ExtractName(t *testing.T) {
	manager := NewResourceManager("", "")

	tests := []struct {
		key          string
		expectedName string
	}{
		{
			key:          "file.txt",
			expectedName: "file.txt",
		},
		{
			key:          "dir/file.txt",
			expectedName: "file.txt",
		},
		{
			key:          "deeply/nested/path/file.txt",
			expectedName: "file.txt",
		},
		{
			key:          "",
			expectedName: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			name := manager.extractName(tt.key)
			if name != tt.expectedName {
				t.Errorf("expected name '%s', got '%s'", tt.expectedName, name)
			}
		})
	}
}

func TestResourceManager_SubscribeToResource(t *testing.T) {
	manager := NewResourceManager("", "")

	err := manager.SubscribeToResource(context.Background(), "objstore://test.txt")
	if err == nil {
		t.Error("expected error for unimplemented subscription")
	}
	if !strings.Contains(err.Error(), "not yet implemented") {
		t.Errorf("expected 'not yet implemented' error, got: %v", err)
	}
}

func TestResourceManager_UnsubscribeFromResource(t *testing.T) {
	manager := NewResourceManager("", "")

	err := manager.UnsubscribeFromResource(context.Background(), "objstore://test.txt")
	if err == nil {
		t.Error("expected error for unimplemented unsubscription")
	}
	if !strings.Contains(err.Error(), "not yet implemented") {
		t.Errorf("expected 'not yet implemented' error, got: %v", err)
	}
}

func TestResourceManager_ReadResourceWithReadError(t *testing.T) {
	storage := &ErrorResourceMockStorage{
		readError: true,
	}
	initTestFacade(t, storage)
	manager := NewResourceManager("", "")

	_, _, err := manager.ReadResource(context.Background(), "objstore://test.txt")
	if err == nil {
		t.Error("expected error when read fails")
	}
}

func TestResourceManager_ListResourcesWithStorageError(t *testing.T) {
	storage := &ErrorResourceMockStorage{
		listError: true,
	}
	initTestFacade(t, storage)
	manager := NewResourceManager("", "")

	_, err := manager.ListResources(context.Background(), "")
	if err == nil {
		t.Error("expected error when list fails")
	}
}

// ErrorResourceMockStorage is a mock storage for testing errors
type ErrorResourceMockStorage struct {
	readError bool
	listError bool
}

func (m *ErrorResourceMockStorage) Configure(settings map[string]string) error {
	return nil
}

func (m *ErrorResourceMockStorage) GetWithContext(ctx context.Context, key string) (io.ReadCloser, error) {
	if m.readError {
		return &badReadCloser{}, nil
	}
	return &mockReadCloser{strings.NewReader("data")}, nil
}

func (m *ErrorResourceMockStorage) GetMetadata(ctx context.Context, key string) (*common.Metadata, error) {
	if m.readError {
		return nil, fmt.Errorf("storage error")
	}
	return &common.Metadata{
		ContentType: "text/plain",
		Size:        4,
	}, nil
}

func (m *ErrorResourceMockStorage) ListWithOptions(ctx context.Context, opts *common.ListOptions) (*common.ListResult, error) {
	if m.listError {
		return nil, fmt.Errorf("storage error")
	}
	return &common.ListResult{}, nil
}

func (m *ErrorResourceMockStorage) Put(key string, data io.Reader) error {
	return nil
}

func (m *ErrorResourceMockStorage) PutWithContext(ctx context.Context, key string, data io.Reader) error {
	return nil
}

func (m *ErrorResourceMockStorage) PutWithMetadata(ctx context.Context, key string, data io.Reader, metadata *common.Metadata) error {
	return nil
}

func (m *ErrorResourceMockStorage) Get(key string) (io.ReadCloser, error) {
	return nil, nil
}

func (m *ErrorResourceMockStorage) UpdateMetadata(ctx context.Context, key string, metadata *common.Metadata) error {
	return nil
}

func (m *ErrorResourceMockStorage) Delete(key string) error {
	return nil
}

func (m *ErrorResourceMockStorage) DeleteWithContext(ctx context.Context, key string) error {
	return nil
}

func (m *ErrorResourceMockStorage) Exists(ctx context.Context, key string) (bool, error) {
	return false, nil
}

func (m *ErrorResourceMockStorage) List(prefix string) ([]string, error) {
	return nil, nil
}

func (m *ErrorResourceMockStorage) ListWithContext(ctx context.Context, prefix string) ([]string, error) {
	return nil, nil
}

func (m *ErrorResourceMockStorage) Archive(key string, destination common.Archiver) error {
	return nil
}

func (m *ErrorResourceMockStorage) SetLifecyclePolicy(policy *common.LifecyclePolicy) error {
	return nil
}

func (m *ErrorResourceMockStorage) GetLifecyclePolicy() *common.LifecyclePolicy {
	return nil
}

func (m *ErrorResourceMockStorage) RunLifecycle(ctx context.Context) error {
	return nil
}

func (m *ErrorResourceMockStorage) AddPolicy(policy common.LifecyclePolicy) error {
	return nil
}

func (m *ErrorResourceMockStorage) RemovePolicy(id string) error {
	return nil
}

func (m *ErrorResourceMockStorage) GetPolicies() ([]common.LifecyclePolicy, error) {
	return []common.LifecyclePolicy{}, nil
}

// badReadCloser is a reader that fails on read
type badReadCloser struct{}

func (e *badReadCloser) Read(p []byte) (int, error) {
	return 0, fmt.Errorf("read error")
}

func (e *badReadCloser) Close() error {
	return nil
}

type mockReadCloser struct {
	*strings.Reader
}

func (m *mockReadCloser) Close() error {
	return nil
}
