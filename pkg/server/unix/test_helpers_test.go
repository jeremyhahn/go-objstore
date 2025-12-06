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

package unix

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/objstore"
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
	if metadata == nil {
		metadata = &common.Metadata{}
	}
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
	if metadata == nil {
		metadata = &common.Metadata{}
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
		if opts.Prefix == "" || strings.HasPrefix(key, opts.Prefix) {
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

// mockReadCloser wraps a reader with a Close method
type mockReadCloser struct {
	io.Reader
}

func (m *mockReadCloser) Close() error {
	return nil
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

// mockLogger implements adapters.Logger for testing
type mockLogger struct{}

func (l *mockLogger) Debug(ctx context.Context, msg string, fields ...adapters.Field)    {}
func (l *mockLogger) Info(ctx context.Context, msg string, fields ...adapters.Field)     {}
func (l *mockLogger) Warn(ctx context.Context, msg string, fields ...adapters.Field)     {}
func (l *mockLogger) Error(ctx context.Context, msg string, fields ...adapters.Field)    {}
func (l *mockLogger) WithFields(fields ...adapters.Field) adapters.Logger                { return l }
func (l *mockLogger) WithContext(ctx context.Context) adapters.Logger                    { return l }
func (l *mockLogger) SetLevel(level adapters.LogLevel)                                   {}
func (l *mockLogger) GetLevel() adapters.LogLevel                                        { return adapters.DebugLevel }

// initTestFacade initializes the objstore facade with a mock storage for testing.
func initTestFacade(t *testing.T, storage common.Storage) {
	t.Helper()
	objstore.Reset()
	err := objstore.Initialize(&objstore.FacadeConfig{
		Backends:       map[string]common.Storage{"default": storage},
		DefaultBackend: "default",
	})
	if err != nil {
		t.Fatalf("Failed to initialize facade: %v", err)
	}
}

// createTestHandler creates a Handler for testing after setting up the facade.
func createTestHandler(t *testing.T, storage common.Storage) *Handler {
	t.Helper()
	initTestFacade(t, storage)
	return NewHandler("", &mockLogger{})
}

// createTestServer creates a Server for testing after setting up the facade.
func createTestServer(t *testing.T, storage common.Storage, socketPath string) *Server {
	t.Helper()
	initTestFacade(t, storage)
	server, err := NewServer(&ServerConfig{
		SocketPath: socketPath,
		Backend:    "",
		Logger:     &mockLogger{},
	})
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}
	return server
}

// tempSocketPath returns a unique temporary socket path for testing
func tempSocketPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(os.TempDir(), "objstore-test-"+t.Name()+".sock")
}

// cleanupSocket removes the socket file
func cleanupSocket(t *testing.T, path string) {
	t.Helper()
	os.Remove(path)
}
