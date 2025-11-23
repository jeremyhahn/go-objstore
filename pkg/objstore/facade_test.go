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
	return []common.LifecyclePolicy{}, nil
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
