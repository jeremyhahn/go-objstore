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

package objstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// Mock storage implementation for testing
type mockStorage struct {
	name    string
	objects map[string][]byte
	err     error // Error to return for testing error cases
}

func newMockStorage(name string) *mockStorage {
	return &mockStorage{
		name:    name,
		objects: make(map[string][]byte),
	}
}

func (m *mockStorage) Configure(settings map[string]string) error {
	return m.err
}

func (m *mockStorage) Put(key string, data io.Reader) error {
	if m.err != nil {
		return m.err
	}
	content, err := io.ReadAll(data)
	if err != nil {
		return err
	}
	m.objects[key] = content
	return nil
}

func (m *mockStorage) PutWithContext(ctx context.Context, key string, data io.Reader) error {
	return m.Put(key, data)
}

func (m *mockStorage) PutWithMetadata(ctx context.Context, key string, data io.Reader, metadata *common.Metadata) error {
	return m.Put(key, data)
}

func (m *mockStorage) Get(key string) (io.ReadCloser, error) {
	if m.err != nil {
		return nil, m.err
	}
	content, ok := m.objects[key]
	if !ok {
		return nil, errors.New("object not found")
	}
	return io.NopCloser(bytes.NewReader(content)), nil
}

func (m *mockStorage) GetWithContext(ctx context.Context, key string) (io.ReadCloser, error) {
	return m.Get(key)
}

func (m *mockStorage) GetMetadata(ctx context.Context, key string) (*common.Metadata, error) {
	if m.err != nil {
		return nil, m.err
	}
	content, ok := m.objects[key]
	if !ok {
		return nil, errors.New("object not found")
	}
	return &common.Metadata{
		Size: int64(len(content)),
	}, nil
}

func (m *mockStorage) UpdateMetadata(ctx context.Context, key string, metadata *common.Metadata) error {
	return m.err
}

func (m *mockStorage) Delete(key string) error {
	if m.err != nil {
		return m.err
	}
	delete(m.objects, key)
	return nil
}

func (m *mockStorage) DeleteWithContext(ctx context.Context, key string) error {
	return m.Delete(key)
}

func (m *mockStorage) Exists(ctx context.Context, key string) (bool, error) {
	if m.err != nil {
		return false, m.err
	}
	_, ok := m.objects[key]
	return ok, nil
}

func (m *mockStorage) List(prefix string) ([]string, error) {
	if m.err != nil {
		return nil, m.err
	}
	var keys []string
	for k := range m.objects {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	return keys, nil
}

func (m *mockStorage) ListWithContext(ctx context.Context, prefix string) ([]string, error) {
	return m.List(prefix)
}

func (m *mockStorage) ListWithOptions(ctx context.Context, opts *common.ListOptions) (*common.ListResult, error) {
	if m.err != nil {
		return nil, m.err
	}
	keys, err := m.List(opts.Prefix)
	if err != nil {
		return nil, err
	}

	result := &common.ListResult{
		Objects: make([]*common.ObjectInfo, 0, len(keys)),
	}

	for _, key := range keys {
		result.Objects = append(result.Objects, &common.ObjectInfo{
			Key: key,
			Metadata: &common.Metadata{
				Size: int64(len(m.objects[key])),
			},
		})
	}

	return result, nil
}

func (m *mockStorage) Archive(key string, destination common.Archiver) error {
	return m.err
}

func (m *mockStorage) AddPolicy(policy common.LifecyclePolicy) error {
	return m.err
}

func (m *mockStorage) RemovePolicy(id string) error {
	return m.err
}

func (m *mockStorage) GetPolicies() ([]common.LifecyclePolicy, error) {
	if m.err != nil {
		return nil, m.err
	}
	return []common.LifecyclePolicy{
		{ID: "policy-1", Prefix: "logs/"},
	}, nil
}

// mockReplicationStorage extends mockStorage with replication capabilities
type mockReplicationStorage struct {
	*mockStorage
	replicationManager *mockReplicationManager
}

func newMockReplicationStorage(name string) *mockReplicationStorage {
	return &mockReplicationStorage{
		mockStorage:        newMockStorage(name),
		replicationManager: &mockReplicationManager{},
	}
}

func (m *mockReplicationStorage) GetReplicationManager() (common.ReplicationManager, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.replicationManager, nil
}

// mockReplicationManager implements common.ReplicationManager
type mockReplicationManager struct{}

func (m *mockReplicationManager) AddPolicy(policy common.ReplicationPolicy) error {
	return nil
}

func (m *mockReplicationManager) RemovePolicy(id string) error {
	return nil
}

func (m *mockReplicationManager) GetPolicy(id string) (*common.ReplicationPolicy, error) {
	return &common.ReplicationPolicy{ID: id}, nil
}

func (m *mockReplicationManager) GetPolicies() ([]common.ReplicationPolicy, error) {
	return []common.ReplicationPolicy{}, nil
}

func (m *mockReplicationManager) SyncAll(ctx context.Context) (*common.SyncResult, error) {
	return &common.SyncResult{}, nil
}

func (m *mockReplicationManager) SyncPolicy(ctx context.Context, policyID string) (*common.SyncResult, error) {
	return &common.SyncResult{}, nil
}

func (m *mockReplicationManager) SetBackendEncrypterFactory(policyID string, factory common.EncrypterFactory) error {
	return nil
}

func (m *mockReplicationManager) SetSourceEncrypterFactory(policyID string, factory common.EncrypterFactory) error {
	return nil
}

func (m *mockReplicationManager) SetDestinationEncrypterFactory(policyID string, factory common.EncrypterFactory) error {
	return nil
}

func (m *mockReplicationManager) Run(ctx context.Context) {
	// No-op for testing
}

// mockArchiver implements common.Archiver for testing
type mockArchiver struct {
	err error
}

func (m *mockArchiver) Put(key string, data io.Reader) error {
	return m.err
}

func TestInitialize(t *testing.T) {
	tests := []struct {
		name    string
		config  *FacadeConfig
		wantErr bool
	}{
		{
			name: "valid config with default backend",
			config: &FacadeConfig{
				Backends: map[string]common.Storage{
					"local": newMockStorage("local"),
				},
				DefaultBackend: "local",
			},
			wantErr: false,
		},
		{
			name: "valid config auto default",
			config: &FacadeConfig{
				Backends: map[string]common.Storage{
					"s3": newMockStorage("s3"),
				},
			},
			wantErr: false,
		},
		{
			name:    "nil config",
			config:  nil,
			wantErr: true,
		},
		{
			name: "empty backends",
			config: &FacadeConfig{
				Backends: map[string]common.Storage{},
			},
			wantErr: true,
		},
		{
			name: "default backend not found",
			config: &FacadeConfig{
				Backends: map[string]common.Storage{
					"local": newMockStorage("local"),
				},
				DefaultBackend: "s3",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset facade before each test
			Reset()

			err := Initialize(tt.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("Initialize() error = %v, wantErr %v", err, tt.wantErr)
			}

			if !tt.wantErr && !IsInitialized() {
				t.Error("Expected facade to be initialized")
			}
		})
	}
}

func TestBackend(t *testing.T) {
	// Setup
	Reset()
	err := Initialize(&FacadeConfig{
		Backends: map[string]common.Storage{
			"local": newMockStorage("local"),
			"s3":    newMockStorage("s3"),
		},
		DefaultBackend: "local",
	})
	if err != nil {
		t.Fatalf("Failed to initialize facade: %v", err)
	}

	tests := []struct {
		name        string
		backendName string
		wantErr     bool
	}{
		{"existing backend", "local", false},
		{"another existing backend", "s3", false},
		{"non-existent backend", "gcs", true},
		{"empty backend name", "", true},
		{"invalid backend name", "My-Backend", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Backend(tt.backendName)
			if (err != nil) != tt.wantErr {
				t.Errorf("Backend(%q) error = %v, wantErr %v", tt.backendName, err, tt.wantErr)
			}
		})
	}
}

func TestDefaultBackend(t *testing.T) {
	// Test uninitialized
	Reset()
	_, err := DefaultBackend()
	if err != ErrNotInitialized {
		t.Errorf("Expected ErrNotInitialized, got %v", err)
	}

	// Test initialized
	err = Initialize(&FacadeConfig{
		Backends: map[string]common.Storage{
			"local": newMockStorage("local"),
		},
		DefaultBackend: "local",
	})
	if err != nil {
		t.Fatalf("Failed to initialize facade: %v", err)
	}

	storage, err := DefaultBackend()
	if err != nil {
		t.Errorf("DefaultBackend() error = %v", err)
	}
	if storage == nil {
		t.Error("Expected non-nil storage")
	}
}

func TestBackends(t *testing.T) {
	// Test uninitialized
	Reset()
	backends := Backends()
	if backends != nil {
		t.Error("Expected nil for uninitialized facade")
	}

	// Test initialized
	err := Initialize(&FacadeConfig{
		Backends: map[string]common.Storage{
			"local": newMockStorage("local"),
			"s3":    newMockStorage("s3"),
			"gcs":   newMockStorage("gcs"),
		},
		DefaultBackend: "local",
	})
	if err != nil {
		t.Fatalf("Failed to initialize facade: %v", err)
	}

	backends = Backends()
	if len(backends) != 3 {
		t.Errorf("Expected 3 backends, got %d", len(backends))
	}

	// Verify all backends are present
	backendMap := make(map[string]bool)
	for _, b := range backends {
		backendMap[b] = true
	}
	for _, expected := range []string{"local", "s3", "gcs"} {
		if !backendMap[expected] {
			t.Errorf("Expected backend %q not found", expected)
		}
	}
}

func TestPut(t *testing.T) {
	Reset()
	mock := newMockStorage("local")
	err := Initialize(&FacadeConfig{
		Backends: map[string]common.Storage{
			"local": mock,
		},
		DefaultBackend: "local",
	})
	if err != nil {
		t.Fatalf("Failed to initialize facade: %v", err)
	}

	tests := []struct {
		name    string
		key     string
		data    string
		wantErr bool
	}{
		{"valid put", "test.txt", "hello world", false},
		{"invalid key empty", "", "data", true},
		{"invalid key path traversal", "../test.txt", "data", true},
		{"invalid key absolute", "/etc/passwd", "data", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Put(tt.key, strings.NewReader(tt.data))
			if (err != nil) != tt.wantErr {
				t.Errorf("Put() error = %v, wantErr %v", err, tt.wantErr)
			}

			if !tt.wantErr {
				// Verify data was stored
				if !bytes.Equal(mock.objects[tt.key], []byte(tt.data)) {
					t.Errorf("Expected data %q, got %q", tt.data, mock.objects[tt.key])
				}
			}
		})
	}
}

func TestGetWithContext(t *testing.T) {
	Reset()
	mock := newMockStorage("local")
	mock.objects["test.txt"] = []byte("hello world")

	err := Initialize(&FacadeConfig{
		Backends: map[string]common.Storage{
			"local": mock,
			"s3":    newMockStorage("s3"),
		},
		DefaultBackend: "local",
	})
	if err != nil {
		t.Fatalf("Failed to initialize facade: %v", err)
	}

	tests := []struct {
		name    string
		keyRef  string
		wantErr bool
		want    string
	}{
		{"get from default backend", "test.txt", false, "hello world"},
		{"get with backend prefix", "local:test.txt", false, "hello world"},
		{"non-existent key", "missing.txt", true, ""},
		{"invalid key reference", "../test.txt", true, ""},
		{"empty key reference", "", true, ""},
	}

	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader, err := GetWithContext(ctx, tt.keyRef)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetWithContext() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				defer reader.Close()
				content, _ := io.ReadAll(reader)
				if string(content) != tt.want {
					t.Errorf("Expected content %q, got %q", tt.want, string(content))
				}
			}
		})
	}
}

func TestDeleteWithContext(t *testing.T) {
	Reset()
	mock := newMockStorage("local")
	mock.objects["test.txt"] = []byte("hello world")

	err := Initialize(&FacadeConfig{
		Backends: map[string]common.Storage{
			"local": mock,
		},
		DefaultBackend: "local",
	})
	if err != nil {
		t.Fatalf("Failed to initialize facade: %v", err)
	}

	ctx := context.Background()

	// Valid delete
	err = DeleteWithContext(ctx, "test.txt")
	if err != nil {
		t.Errorf("DeleteWithContext() error = %v", err)
	}

	// Verify deletion
	if _, ok := mock.objects["test.txt"]; ok {
		t.Error("Expected object to be deleted")
	}

	// Invalid key
	err = DeleteWithContext(ctx, "../test.txt")
	if err == nil {
		t.Error("Expected error for invalid key")
	}
}

func TestExists(t *testing.T) {
	Reset()
	mock := newMockStorage("local")
	mock.objects["exists.txt"] = []byte("data")

	err := Initialize(&FacadeConfig{
		Backends: map[string]common.Storage{
			"local": mock,
		},
		DefaultBackend: "local",
	})
	if err != nil {
		t.Fatalf("Failed to initialize facade: %v", err)
	}

	ctx := context.Background()

	tests := []struct {
		name    string
		keyRef  string
		want    bool
		wantErr bool
	}{
		{"exists", "exists.txt", true, false},
		{"not exists", "missing.txt", false, false},
		{"invalid key", "../test.txt", false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			exists, err := Exists(ctx, tt.keyRef)
			if (err != nil) != tt.wantErr {
				t.Errorf("Exists() error = %v, wantErr %v", err, tt.wantErr)
			}
			if exists != tt.want {
				t.Errorf("Exists() = %v, want %v", exists, tt.want)
			}
		})
	}
}

func TestListWithContext(t *testing.T) {
	Reset()
	mock := newMockStorage("local")
	mock.objects["logs/app.log"] = []byte("log1")
	mock.objects["logs/error.log"] = []byte("log2")
	mock.objects["data/file.txt"] = []byte("data")

	err := Initialize(&FacadeConfig{
		Backends: map[string]common.Storage{
			"local": mock,
		},
		DefaultBackend: "local",
	})
	if err != nil {
		t.Fatalf("Failed to initialize facade: %v", err)
	}

	ctx := context.Background()

	tests := []struct {
		name      string
		prefix    string
		wantCount int
		wantErr   bool
	}{
		{"list logs", "logs/", 2, false},
		{"list data", "data/", 1, false},
		{"list all", "", 3, false},
		{"invalid prefix", "../", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			keys, err := ListWithContext(ctx, tt.prefix)
			if (err != nil) != tt.wantErr {
				t.Errorf("ListWithContext() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && len(keys) != tt.wantCount {
				t.Errorf("Expected %d keys, got %d", tt.wantCount, len(keys))
			}
		})
	}
}

func TestListWithOptions(t *testing.T) {
	Reset()
	mock := newMockStorage("local")
	mock.objects["logs/app.log"] = []byte("log1")
	mock.objects["logs/error.log"] = []byte("log2")

	err := Initialize(&FacadeConfig{
		Backends: map[string]common.Storage{
			"local": mock,
		},
		DefaultBackend: "local",
	})
	if err != nil {
		t.Fatalf("Failed to initialize facade: %v", err)
	}

	ctx := context.Background()
	opts := &common.ListOptions{
		Prefix:     "logs/",
		MaxResults: 10,
	}

	result, err := ListWithOptions(ctx, "", opts)
	if err != nil {
		t.Errorf("ListWithOptions() error = %v", err)
	}

	if len(result.Objects) != 2 {
		t.Errorf("Expected 2 objects, got %d", len(result.Objects))
	}

	// Test with invalid backend
	_, err = ListWithOptions(ctx, "INVALID", opts)
	if err == nil {
		t.Error("Expected error for invalid backend name")
	}
}

func TestPutWithMetadata(t *testing.T) {
	Reset()
	mock := newMockStorage("local")

	err := Initialize(&FacadeConfig{
		Backends: map[string]common.Storage{
			"local": mock,
		},
		DefaultBackend: "local",
	})
	if err != nil {
		t.Fatalf("Failed to initialize facade: %v", err)
	}

	ctx := context.Background()

	// Valid metadata
	metadata := &common.Metadata{
		ContentType: "text/plain",
		Custom: map[string]string{
			"author": "test",
		},
	}

	err = PutWithMetadata(ctx, "test.txt", strings.NewReader("data"), metadata)
	if err != nil {
		t.Errorf("PutWithMetadata() error = %v", err)
	}

	// Invalid key
	err = PutWithMetadata(ctx, "../test.txt", strings.NewReader("data"), metadata)
	if err == nil {
		t.Error("Expected error for invalid key")
	}

	// Invalid metadata (too many entries)
	invalidMetadata := &common.Metadata{
		Custom: make(map[string]string),
	}
	for i := 0; i < 101; i++ {
		invalidMetadata.Custom[string(rune('a'+i))] = "value"
	}

	err = PutWithMetadata(ctx, "test2.txt", strings.NewReader("data"), invalidMetadata)
	if err == nil {
		t.Error("Expected error for invalid metadata")
	}
}

func TestReset(t *testing.T) {
	// Initialize
	err := Initialize(&FacadeConfig{
		Backends: map[string]common.Storage{
			"local": newMockStorage("local"),
		},
		DefaultBackend: "local",
	})
	if err != nil {
		t.Fatalf("Failed to initialize facade: %v", err)
	}

	if !IsInitialized() {
		t.Error("Expected facade to be initialized")
	}

	// Reset
	Reset()

	if IsInitialized() {
		t.Error("Expected facade to be not initialized after reset")
	}

	// Can initialize again
	err = Initialize(&FacadeConfig{
		Backends: map[string]common.Storage{
			"s3": newMockStorage("s3"),
		},
		DefaultBackend: "s3",
	})
	if err != nil {
		t.Errorf("Failed to re-initialize facade: %v", err)
	}
}

func TestDelete(t *testing.T) {
	Reset()
	mock := newMockStorage("local")
	mock.objects["delete-me.txt"] = []byte("data")

	err := Initialize(&FacadeConfig{
		Backends: map[string]common.Storage{
			"local": mock,
		},
		DefaultBackend: "local",
	})
	if err != nil {
		t.Fatalf("Failed to initialize facade: %v", err)
	}

	tests := []struct {
		name    string
		key     string
		wantErr bool
	}{
		{"valid delete", "delete-me.txt", false},
		{"invalid key empty", "", true},
		{"invalid key path traversal", "../test.txt", true},
		{"invalid key absolute", "/etc/passwd", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Delete(tt.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("Delete() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestList(t *testing.T) {
	Reset()
	mock := newMockStorage("local")
	mock.objects["logs/app.log"] = []byte("log1")
	mock.objects["logs/error.log"] = []byte("log2")
	mock.objects["data/file.txt"] = []byte("data")

	err := Initialize(&FacadeConfig{
		Backends: map[string]common.Storage{
			"local": mock,
		},
		DefaultBackend: "local",
	})
	if err != nil {
		t.Fatalf("Failed to initialize facade: %v", err)
	}

	tests := []struct {
		name      string
		prefix    string
		wantCount int
		wantErr   bool
	}{
		{"list logs", "logs/", 2, false},
		{"list data", "data/", 1, false},
		{"list all", "", 3, false},
		{"invalid prefix", "../", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			keys, err := List(tt.prefix)
			if (err != nil) != tt.wantErr {
				t.Errorf("List() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && len(keys) != tt.wantCount {
				t.Errorf("Expected %d keys, got %d", tt.wantCount, len(keys))
			}
		})
	}
}

func TestGet(t *testing.T) {
	Reset()
	mock := newMockStorage("local")
	mock.objects["test.txt"] = []byte("hello world")

	err := Initialize(&FacadeConfig{
		Backends: map[string]common.Storage{
			"local": mock,
		},
		DefaultBackend: "local",
	})
	if err != nil {
		t.Fatalf("Failed to initialize facade: %v", err)
	}

	tests := []struct {
		name    string
		key     string
		want    string
		wantErr bool
	}{
		{"valid get", "test.txt", "hello world", false},
		{"non-existent", "missing.txt", "", true},
		{"invalid key empty", "", "", true},
		{"invalid key path traversal", "../test.txt", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader, err := Get(tt.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				defer reader.Close()
				content, _ := io.ReadAll(reader)
				if string(content) != tt.want {
					t.Errorf("Expected %q, got %q", tt.want, string(content))
				}
			}
		})
	}
}

func TestGetMetadata(t *testing.T) {
	Reset()
	mock := newMockStorage("local")
	mock.objects["test.txt"] = []byte("hello world")

	err := Initialize(&FacadeConfig{
		Backends: map[string]common.Storage{
			"local": mock,
		},
		DefaultBackend: "local",
	})
	if err != nil {
		t.Fatalf("Failed to initialize facade: %v", err)
	}

	ctx := context.Background()

	tests := []struct {
		name    string
		keyRef  string
		wantErr bool
	}{
		{"valid metadata", "test.txt", false},
		{"with backend prefix", "local:test.txt", false},
		{"non-existent", "missing.txt", true},
		{"invalid key", "../test.txt", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metadata, err := GetMetadata(ctx, tt.keyRef)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetMetadata() error = %v, wantErr %v", err, tt.wantErr)
			}

			if !tt.wantErr && metadata == nil {
				t.Error("Expected non-nil metadata")
			}
		})
	}
}

func TestUpdateMetadata(t *testing.T) {
	Reset()
	mock := newMockStorage("local")
	mock.objects["test.txt"] = []byte("hello world")

	err := Initialize(&FacadeConfig{
		Backends: map[string]common.Storage{
			"local": mock,
		},
		DefaultBackend: "local",
	})
	if err != nil {
		t.Fatalf("Failed to initialize facade: %v", err)
	}

	ctx := context.Background()

	tests := []struct {
		name     string
		keyRef   string
		metadata *common.Metadata
		wantErr  bool
	}{
		{
			name:   "valid update",
			keyRef: "test.txt",
			metadata: &common.Metadata{
				ContentType: "text/plain",
				Custom:      map[string]string{"author": "test"},
			},
			wantErr: false,
		},
		{
			name:   "with backend prefix",
			keyRef: "local:test.txt",
			metadata: &common.Metadata{
				ContentType: "text/plain",
			},
			wantErr: false,
		},
		{
			name:     "invalid key",
			keyRef:   "../test.txt",
			metadata: &common.Metadata{},
			wantErr:  true,
		},
		{
			name:   "invalid metadata too many entries",
			keyRef: "test.txt",
			metadata: func() *common.Metadata {
				m := &common.Metadata{Custom: make(map[string]string)}
				for i := 0; i < 101; i++ {
					m.Custom[string(rune('a'+i))] = "value"
				}
				return m
			}(),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := UpdateMetadata(ctx, tt.keyRef, tt.metadata)
			if (err != nil) != tt.wantErr {
				t.Errorf("UpdateMetadata() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestArchive(t *testing.T) {
	Reset()
	mock := newMockStorage("local")
	mock.objects["archive-me.txt"] = []byte("data to archive")

	err := Initialize(&FacadeConfig{
		Backends: map[string]common.Storage{
			"local": mock,
		},
		DefaultBackend: "local",
	})
	if err != nil {
		t.Fatalf("Failed to initialize facade: %v", err)
	}

	archiver := &mockArchiver{}

	tests := []struct {
		name    string
		keyRef  string
		wantErr bool
	}{
		{"valid archive", "archive-me.txt", false},
		{"with backend prefix", "local:archive-me.txt", false},
		{"invalid key", "../test.txt", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Archive(tt.keyRef, archiver)
			if (err != nil) != tt.wantErr {
				t.Errorf("Archive() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestAddPolicy(t *testing.T) {
	Reset()
	mock := newMockStorage("local")

	err := Initialize(&FacadeConfig{
		Backends: map[string]common.Storage{
			"local": mock,
		},
		DefaultBackend: "local",
	})
	if err != nil {
		t.Fatalf("Failed to initialize facade: %v", err)
	}

	tests := []struct {
		name        string
		backendName string
		policy      common.LifecyclePolicy
		wantErr     bool
	}{
		{
			name:        "valid policy default backend",
			backendName: "",
			policy: common.LifecyclePolicy{
				ID:     "policy-1",
				Prefix: "logs/",
			},
			wantErr: false,
		},
		{
			name:        "valid policy specific backend",
			backendName: "local",
			policy: common.LifecyclePolicy{
				ID:     "policy-2",
				Prefix: "data/",
			},
			wantErr: false,
		},
		{
			name:        "invalid backend name",
			backendName: "INVALID",
			policy:      common.LifecyclePolicy{},
			wantErr:     true,
		},
		{
			name:        "invalid policy prefix",
			backendName: "",
			policy: common.LifecyclePolicy{
				ID:     "policy-3",
				Prefix: "../etc/",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := AddPolicy(tt.backendName, tt.policy)
			if (err != nil) != tt.wantErr {
				t.Errorf("AddPolicy() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestRemovePolicy(t *testing.T) {
	Reset()
	mock := newMockStorage("local")

	err := Initialize(&FacadeConfig{
		Backends: map[string]common.Storage{
			"local": mock,
		},
		DefaultBackend: "local",
	})
	if err != nil {
		t.Fatalf("Failed to initialize facade: %v", err)
	}

	tests := []struct {
		name        string
		backendName string
		policyID    string
		wantErr     bool
	}{
		{"remove from default backend", "", "policy-1", false},
		{"remove from specific backend", "local", "policy-2", false},
		{"invalid backend name", "INVALID", "policy-3", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := RemovePolicy(tt.backendName, tt.policyID)
			if (err != nil) != tt.wantErr {
				t.Errorf("RemovePolicy() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestGetPolicies(t *testing.T) {
	Reset()
	mock := newMockStorage("local")

	err := Initialize(&FacadeConfig{
		Backends: map[string]common.Storage{
			"local": mock,
		},
		DefaultBackend: "local",
	})
	if err != nil {
		t.Fatalf("Failed to initialize facade: %v", err)
	}

	tests := []struct {
		name        string
		backendName string
		wantErr     bool
	}{
		{"get from default backend", "", false},
		{"get from specific backend", "local", false},
		{"invalid backend name", "INVALID", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			policies, err := GetPolicies(tt.backendName)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetPolicies() error = %v, wantErr %v", err, tt.wantErr)
			}

			if !tt.wantErr && policies == nil {
				t.Error("Expected non-nil policies")
			}
		})
	}
}

func TestGetReplicationManager(t *testing.T) {
	Reset()

	// Create a storage that supports replication
	replicableStorage := newMockReplicationStorage("local")

	// Create a storage that does not support replication
	nonReplicableStorage := newMockStorage("simple")

	err := Initialize(&FacadeConfig{
		Backends: map[string]common.Storage{
			"replicable":     replicableStorage,
			"non-replicable": nonReplicableStorage,
		},
		DefaultBackend: "replicable",
	})
	if err != nil {
		t.Fatalf("Failed to initialize facade: %v", err)
	}

	tests := []struct {
		name        string
		backendName string
		wantErr     bool
	}{
		{"get from replicable backend", "replicable", false},
		{"get from default (replicable)", "", false},
		{"get from non-replicable backend", "non-replicable", true},
		{"invalid backend name", "INVALID", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager, err := GetReplicationManager(tt.backendName)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetReplicationManager() error = %v, wantErr %v", err, tt.wantErr)
			}

			if !tt.wantErr && manager == nil {
				t.Error("Expected non-nil replication manager")
			}
		})
	}
}

func TestInitializeWithBackendConfigs(t *testing.T) {
	tests := []struct {
		name    string
		config  *FacadeConfig
		wantErr bool
	}{
		{
			name: "valid BackendConfigs",
			config: &FacadeConfig{
				BackendConfigs: map[string]BackendConfig{
					"local": {
						Type:     "local",
						Settings: map[string]string{"path": "/tmp/test-backend-configs"},
					},
				},
				DefaultBackend: "local",
			},
			wantErr: false,
		},
		{
			name: "invalid backend type",
			config: &FacadeConfig{
				BackendConfigs: map[string]BackendConfig{
					"invalid": {
						Type:     "nonexistent-backend-type",
						Settings: map[string]string{},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "mixed backends and backendconfigs",
			config: &FacadeConfig{
				Backends: map[string]common.Storage{
					"mock": newMockStorage("mock"),
				},
				BackendConfigs: map[string]BackendConfig{
					"local": {
						Type:     "local",
						Settings: map[string]string{"path": "/tmp/test-mixed"},
					},
				},
				DefaultBackend: "mock",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			Reset()

			err := Initialize(tt.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("Initialize() with BackendConfigs error = %v, wantErr %v", err, tt.wantErr)
			}

			if !tt.wantErr && !IsInitialized() {
				t.Error("Expected facade to be initialized")
			}
		})
	}
}

func TestPutWithContext(t *testing.T) {
	Reset()
	mock := newMockStorage("local")
	err := Initialize(&FacadeConfig{
		Backends: map[string]common.Storage{
			"local": mock,
		},
		DefaultBackend: "local",
	})
	if err != nil {
		t.Fatalf("Failed to initialize facade: %v", err)
	}

	ctx := context.Background()

	tests := []struct {
		name    string
		keyRef  string
		data    string
		wantErr bool
	}{
		{"valid put", "test.txt", "hello world", false},
		{"with backend prefix", "local:test2.txt", "data", false},
		{"invalid key empty", "", "data", true},
		{"invalid key path traversal", "../test.txt", "data", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := PutWithContext(ctx, tt.keyRef, strings.NewReader(tt.data))
			if (err != nil) != tt.wantErr {
				t.Errorf("PutWithContext() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestListWithOptionsSpecificBackend(t *testing.T) {
	Reset()
	mock := newMockStorage("local")
	mock.objects["test/a.txt"] = []byte("a")
	mock.objects["test/b.txt"] = []byte("b")

	err := Initialize(&FacadeConfig{
		Backends: map[string]common.Storage{
			"local": mock,
		},
		DefaultBackend: "local",
	})
	if err != nil {
		t.Fatalf("Failed to initialize facade: %v", err)
	}

	ctx := context.Background()
	opts := &common.ListOptions{
		Prefix:     "test/",
		MaxResults: 10,
	}

	// Test with specific backend name
	result, err := ListWithOptions(ctx, "local", opts)
	if err != nil {
		t.Errorf("ListWithOptions() with specific backend error = %v", err)
	}
	if len(result.Objects) != 2 {
		t.Errorf("Expected 2 objects, got %d", len(result.Objects))
	}

	// Test with invalid prefix in options
	invalidOpts := &common.ListOptions{
		Prefix: "../etc/",
	}
	_, err = ListWithOptions(ctx, "", invalidOpts)
	if err == nil {
		t.Error("Expected error for invalid prefix")
	}
}

func TestFacadeNotInitialized(t *testing.T) {
	Reset()

	ctx := context.Background()

	// Test all functions when facade is not initialized
	_, err := Backend("local")
	if err != ErrNotInitialized {
		t.Errorf("Backend() expected ErrNotInitialized, got %v", err)
	}

	_, err = DefaultBackend()
	if err != ErrNotInitialized {
		t.Errorf("DefaultBackend() expected ErrNotInitialized, got %v", err)
	}

	err = Put("test.txt", strings.NewReader("data"))
	if err != ErrNotInitialized {
		t.Errorf("Put() expected ErrNotInitialized, got %v", err)
	}

	_, err = Get("test.txt")
	if err != ErrNotInitialized {
		t.Errorf("Get() expected ErrNotInitialized, got %v", err)
	}

	err = Delete("test.txt")
	if err != ErrNotInitialized {
		t.Errorf("Delete() expected ErrNotInitialized, got %v", err)
	}

	_, err = List("")
	if err != ErrNotInitialized {
		t.Errorf("List() expected ErrNotInitialized, got %v", err)
	}

	_, err = Exists(ctx, "test.txt")
	if err != ErrNotInitialized {
		t.Errorf("Exists() expected ErrNotInitialized, got %v", err)
	}

	_, err = GetMetadata(ctx, "test.txt")
	if err != ErrNotInitialized {
		t.Errorf("GetMetadata() expected ErrNotInitialized, got %v", err)
	}

	err = UpdateMetadata(ctx, "test.txt", &common.Metadata{})
	if err != ErrNotInitialized {
		t.Errorf("UpdateMetadata() expected ErrNotInitialized, got %v", err)
	}

	err = Archive("test.txt", &mockArchiver{})
	if err != ErrNotInitialized {
		t.Errorf("Archive() expected ErrNotInitialized, got %v", err)
	}

	err = AddPolicy("", common.LifecyclePolicy{})
	if err != ErrNotInitialized {
		t.Errorf("AddPolicy() expected ErrNotInitialized, got %v", err)
	}

	err = RemovePolicy("", "policy-1")
	if err != ErrNotInitialized {
		t.Errorf("RemovePolicy() expected ErrNotInitialized, got %v", err)
	}

	_, err = GetPolicies("")
	if err != ErrNotInitialized {
		t.Errorf("GetPolicies() expected ErrNotInitialized, got %v", err)
	}

	_, err = GetReplicationManager("")
	if err != ErrNotInitialized {
		t.Errorf("GetReplicationManager() expected ErrNotInitialized, got %v", err)
	}
}

// Benchmark tests
func BenchmarkPut(b *testing.B) {
	Reset()
	err := Initialize(&FacadeConfig{
		Backends: map[string]common.Storage{
			"local": newMockStorage("local"),
		},
		DefaultBackend: "local",
	})
	if err != nil {
		b.Fatalf("Failed to initialize facade: %v", err)
	}

	data := []byte("benchmark data")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = Put("benchmark.txt", bytes.NewReader(data))
	}
}

func BenchmarkGetWithContext(b *testing.B) {
	Reset()
	mock := newMockStorage("local")
	mock.objects["benchmark.txt"] = []byte("benchmark data")

	err := Initialize(&FacadeConfig{
		Backends: map[string]common.Storage{
			"local": mock,
		},
		DefaultBackend: "local",
	})
	if err != nil {
		b.Fatalf("Failed to initialize facade: %v", err)
	}

	ctx := context.Background()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		reader, _ := GetWithContext(ctx, "benchmark.txt")
		if reader != nil {
			reader.Close()
		}
	}
}
