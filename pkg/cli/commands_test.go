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
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/replication"
)

// mockStorage is a mock implementation of common.Storage for testing.
type mockStorage struct {
	data     map[string][]byte
	metadata map[string]*common.Metadata
}

func newMockStorage() *mockStorage {
	return &mockStorage{
		data:     make(map[string][]byte),
		metadata: make(map[string]*common.Metadata),
	}
}

func (m *mockStorage) Configure(settings map[string]string) error {
	return nil
}

func (m *mockStorage) Put(key string, data io.Reader) error {
	return m.PutWithContext(context.Background(), key, data)
}

func (m *mockStorage) PutWithContext(ctx context.Context, key string, data io.Reader) error {
	content, err := io.ReadAll(data)
	if err != nil {
		return err
	}
	m.data[key] = content
	return nil
}

func (m *mockStorage) PutWithMetadata(ctx context.Context, key string, data io.Reader, metadata *common.Metadata) error {
	if err := m.PutWithContext(ctx, key, data); err != nil {
		return err
	}
	m.metadata[key] = metadata
	return nil
}

func (m *mockStorage) Get(key string) (io.ReadCloser, error) {
	return m.GetWithContext(context.Background(), key)
}

func (m *mockStorage) GetWithContext(ctx context.Context, key string) (io.ReadCloser, error) {
	content, exists := m.data[key]
	if !exists {
		return nil, &mockError{msg: "object not found"}
	}
	return io.NopCloser(strings.NewReader(string(content))), nil
}

func (m *mockStorage) GetMetadata(ctx context.Context, key string) (*common.Metadata, error) {
	meta, exists := m.metadata[key]
	if !exists {
		return nil, &mockError{msg: "metadata not found"}
	}
	return meta, nil
}

func (m *mockStorage) UpdateMetadata(ctx context.Context, key string, metadata *common.Metadata) error {
	if _, exists := m.data[key]; !exists {
		return &mockError{msg: "object not found"}
	}
	m.metadata[key] = metadata
	return nil
}

func (m *mockStorage) Delete(key string) error {
	return m.DeleteWithContext(context.Background(), key)
}

func (m *mockStorage) DeleteWithContext(ctx context.Context, key string) error {
	if _, exists := m.data[key]; !exists {
		return &mockError{msg: "object not found"}
	}
	delete(m.data, key)
	delete(m.metadata, key)
	return nil
}

func (m *mockStorage) Exists(ctx context.Context, key string) (bool, error) {
	_, exists := m.data[key]
	return exists, nil
}

func (m *mockStorage) List(prefix string) ([]string, error) {
	return m.ListWithContext(context.Background(), prefix)
}

func (m *mockStorage) ListWithContext(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	for key := range m.data {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	return keys, nil
}

func (m *mockStorage) ListWithOptions(ctx context.Context, opts *common.ListOptions) (*common.ListResult, error) {
	var objects []*common.ObjectInfo
	for key, content := range m.data {
		if opts.Prefix == "" || strings.HasPrefix(key, opts.Prefix) {
			objects = append(objects, &common.ObjectInfo{
				Key: key,
				Metadata: &common.Metadata{
					Size:         int64(len(content)),
					LastModified: time.Now(),
				},
			})
		}
	}
	return &common.ListResult{Objects: objects}, nil
}

func (m *mockStorage) Archive(key string, destination common.Archiver) error {
	return nil
}

func (m *mockStorage) AddPolicy(policy common.LifecyclePolicy) error {
	return nil
}

func (m *mockStorage) RemovePolicy(id string) error {
	return nil
}

func (m *mockStorage) GetPolicies() ([]common.LifecyclePolicy, error) {
	return nil, nil
}

// mockError is a mock error for testing.
type mockError struct {
	msg string
}

func (e *mockError) Error() string {
	return e.msg
}

// mockClient is a mock implementation of client.Client for testing
type mockClient struct {
	healthError   error
	existsError   error
	putError      error
	getError      error
	deleteError   error
	listError     error
	metadataError error
}

func (m *mockClient) Put(ctx context.Context, key string, reader io.Reader, metadata *common.Metadata) error {
	if m.putError != nil {
		return m.putError
	}
	return nil
}

func (m *mockClient) Get(ctx context.Context, key string) (io.ReadCloser, *common.Metadata, error) {
	if m.getError != nil {
		return nil, nil, m.getError
	}
	return io.NopCloser(strings.NewReader("test data")), &common.Metadata{Size: 9}, nil
}

func (m *mockClient) Delete(ctx context.Context, key string) error {
	if m.deleteError != nil {
		return m.deleteError
	}
	return nil
}

func (m *mockClient) Exists(ctx context.Context, key string) (bool, error) {
	if m.existsError != nil {
		return false, m.existsError
	}
	return true, nil
}

func (m *mockClient) List(ctx context.Context, opts *common.ListOptions) (*common.ListResult, error) {
	if m.listError != nil {
		return nil, m.listError
	}
	return &common.ListResult{Objects: []*common.ObjectInfo{}}, nil
}

func (m *mockClient) GetMetadata(ctx context.Context, key string) (*common.Metadata, error) {
	if m.metadataError != nil {
		return nil, m.metadataError
	}
	return &common.Metadata{Size: 100}, nil
}

func (m *mockClient) UpdateMetadata(ctx context.Context, key string, metadata *common.Metadata) error {
	if m.metadataError != nil {
		return m.metadataError
	}
	return nil
}

func (m *mockClient) Archive(ctx context.Context, key, destinationType string, destinationSettings map[string]string) error {
	return nil
}

func (m *mockClient) AddPolicy(ctx context.Context, policy common.LifecyclePolicy) error {
	return nil
}

func (m *mockClient) RemovePolicy(ctx context.Context, policyID string) error {
	return nil
}

func (m *mockClient) GetPolicies(ctx context.Context) ([]common.LifecyclePolicy, error) {
	return []common.LifecyclePolicy{}, nil
}

func (m *mockClient) ApplyPolicies(ctx context.Context) (int, int, error) {
	return 0, 0, nil
}

func (m *mockClient) Health(ctx context.Context) error {
	if m.healthError != nil {
		return m.healthError
	}
	return nil
}

func (m *mockClient) Close() error {
	return nil
}

func TestNewCommandContext(t *testing.T) {
	t.Run("valid local config", func(t *testing.T) {
		tmpDir := t.TempDir()
		cfg := &Config{
			Backend:      "local",
			BackendPath:  tmpDir,
			OutputFormat: "text",
		}

		ctx, err := NewCommandContext(cfg)
		if err != nil {
			// Skip if local backend is not available (requires -tags local)
			if err.Error() == "unknown backend type" {
				t.Skip("local backend not available (requires -tags local)")
			}
			t.Fatalf("NewCommandContext failed: %v", err)
		}
		if ctx == nil {
			t.Fatal("Expected context, got nil")
		}
		if ctx.Storage == nil {
			t.Error("Expected storage backend")
		}
		if ctx.Config != cfg {
			t.Error("Config mismatch")
		}
		ctx.Close()
	})

	t.Run("invalid config", func(t *testing.T) {
		cfg := &Config{
			Backend:      "local",
			OutputFormat: "text",
			// Missing BackendPath
		}

		_, err := NewCommandContext(cfg)
		if err == nil {
			t.Error("Expected error for invalid config")
		}
	})

	t.Run("unsupported backend", func(t *testing.T) {
		cfg := &Config{
			Backend:      "unsupported",
			OutputFormat: "text",
		}

		_, err := NewCommandContext(cfg)
		if err == nil {
			t.Error("Expected error for unsupported backend")
		}
	})
}

func TestCommandContext_PutCommand(t *testing.T) {
	t.Run("successful put", func(t *testing.T) {
		storage := newMockStorage()
		ctx := &CommandContext{
			Storage: storage,
			Config:  &Config{OutputFormat: "text"},
		}

		// Create a test file
		tmpDir := t.TempDir()
		testFile := filepath.Join(tmpDir, "test.txt")
		testContent := "test content"
		if err := os.WriteFile(testFile, []byte(testContent), 0644); err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}

		err := ctx.PutCommand("test-key", testFile)
		if err != nil {
			t.Errorf("PutCommand failed: %v", err)
		}

		// Verify data was stored
		if string(storage.data["test-key"]) != testContent {
			t.Error("Data mismatch")
		}

		// Verify metadata was stored
		if storage.metadata["test-key"] == nil {
			t.Error("Expected metadata")
		}
		if storage.metadata["test-key"].Size != int64(len(testContent)) {
			t.Errorf("Expected metadata size %d, got %d", len(testContent), storage.metadata["test-key"].Size)
		}
	})

	t.Run("file not found", func(t *testing.T) {
		storage := newMockStorage()
		ctx := &CommandContext{
			Storage: storage,
			Config:  &Config{OutputFormat: "text"},
		}

		err := ctx.PutCommand("test-key", "/nonexistent/file.txt")
		if err == nil {
			t.Error("Expected error for nonexistent file")
		}
	})
}

func TestCommandContext_GetCommand(t *testing.T) {
	t.Run("successful get to file", func(t *testing.T) {
		storage := newMockStorage()
		storage.data["test-key"] = []byte("test content")

		ctx := &CommandContext{
			Storage: storage,
			Config:  &Config{OutputFormat: "text"},
		}

		tmpDir := t.TempDir()
		outputFile := filepath.Join(tmpDir, "output.txt")

		err := ctx.GetCommand("test-key", outputFile)
		if err != nil {
			t.Errorf("GetCommand failed: %v", err)
		}

		// Verify file content
		content, err := os.ReadFile(outputFile)
		if err != nil {
			t.Fatalf("Failed to read output file: %v", err)
		}
		if string(content) != "test content" {
			t.Errorf("Content mismatch: got %q, want %q", string(content), "test content")
		}
	})

	t.Run("successful get to stdout", func(t *testing.T) {
		storage := newMockStorage()
		storage.data["test-key"] = []byte("test content")

		ctx := &CommandContext{
			Storage: storage,
			Config:  &Config{OutputFormat: "text"},
		}

		// Get to stdout (empty string or "-")
		err := ctx.GetCommand("test-key", "")
		if err != nil {
			t.Errorf("GetCommand failed: %v", err)
		}

		err = ctx.GetCommand("test-key", "-")
		if err != nil {
			t.Errorf("GetCommand failed: %v", err)
		}
	})

	t.Run("object not found", func(t *testing.T) {
		storage := newMockStorage()
		ctx := &CommandContext{
			Storage: storage,
			Config:  &Config{OutputFormat: "text"},
		}

		tmpDir := t.TempDir()
		outputFile := filepath.Join(tmpDir, "output.txt")

		err := ctx.GetCommand("nonexistent-key", outputFile)
		if err == nil {
			t.Error("Expected error for nonexistent object")
		}
	})

	t.Run("invalid output path", func(t *testing.T) {
		storage := newMockStorage()
		storage.data["test-key"] = []byte("test content")

		ctx := &CommandContext{
			Storage: storage,
			Config:  &Config{OutputFormat: "text"},
		}

		// Try to write to an invalid path
		err := ctx.GetCommand("test-key", "/invalid/path/output.txt")
		if err == nil {
			t.Error("Expected error for invalid output path")
		}
	})
}

func TestCommandContext_DeleteCommand(t *testing.T) {
	t.Run("successful delete", func(t *testing.T) {
		storage := newMockStorage()
		storage.data["test-key"] = []byte("test content")

		ctx := &CommandContext{
			Storage: storage,
			Config:  &Config{OutputFormat: "text"},
		}

		err := ctx.DeleteCommand("test-key")
		if err != nil {
			t.Errorf("DeleteCommand failed: %v", err)
		}

		// Verify data was deleted
		if _, exists := storage.data["test-key"]; exists {
			t.Error("Object should have been deleted")
		}
	})

	t.Run("delete nonexistent object", func(t *testing.T) {
		storage := newMockStorage()
		ctx := &CommandContext{
			Storage: storage,
			Config:  &Config{OutputFormat: "text"},
		}

		err := ctx.DeleteCommand("nonexistent-key")
		if err == nil {
			t.Error("Expected error for nonexistent object")
		}
	})
}

func TestCommandContext_ListCommand(t *testing.T) {
	t.Run("list all objects", func(t *testing.T) {
		storage := newMockStorage()
		storage.data["test/file1.txt"] = []byte("content1")
		storage.data["test/file2.txt"] = []byte("content2")
		storage.data["other/file3.txt"] = []byte("content3")

		ctx := &CommandContext{
			Storage: storage,
			Config:  &Config{OutputFormat: "text"},
		}

		objects, err := ctx.ListCommand("")
		if err != nil {
			t.Errorf("ListCommand failed: %v", err)
		}

		if len(objects) != 3 {
			t.Errorf("Expected 3 objects, got %d", len(objects))
		}
	})

	t.Run("list with prefix", func(t *testing.T) {
		storage := newMockStorage()
		storage.data["test/file1.txt"] = []byte("content1")
		storage.data["test/file2.txt"] = []byte("content2")
		storage.data["other/file3.txt"] = []byte("content3")

		ctx := &CommandContext{
			Storage: storage,
			Config:  &Config{OutputFormat: "text"},
		}

		objects, err := ctx.ListCommand("test/")
		if err != nil {
			t.Errorf("ListCommand failed: %v", err)
		}

		if len(objects) != 2 {
			t.Errorf("Expected 2 objects, got %d", len(objects))
		}
	})

	t.Run("list empty storage", func(t *testing.T) {
		storage := newMockStorage()
		ctx := &CommandContext{
			Storage: storage,
			Config:  &Config{OutputFormat: "text"},
		}

		objects, err := ctx.ListCommand("")
		if err != nil {
			t.Errorf("ListCommand failed: %v", err)
		}

		if len(objects) != 0 {
			t.Errorf("Expected 0 objects, got %d", len(objects))
		}
	})
}

func TestCommandContext_ExistsCommand(t *testing.T) {
	t.Run("object exists", func(t *testing.T) {
		storage := newMockStorage()
		storage.data["test-key"] = []byte("content")

		ctx := &CommandContext{
			Storage: storage,
			Config:  &Config{OutputFormat: "text"},
		}

		exists, err := ctx.ExistsCommand("test-key")
		if err != nil {
			t.Errorf("ExistsCommand failed: %v", err)
		}

		if !exists {
			t.Error("Expected object to exist")
		}
	})

	t.Run("object does not exist", func(t *testing.T) {
		storage := newMockStorage()
		ctx := &CommandContext{
			Storage: storage,
			Config:  &Config{OutputFormat: "text"},
		}

		exists, err := ctx.ExistsCommand("nonexistent-key")
		if err != nil {
			t.Errorf("ExistsCommand failed: %v", err)
		}

		if exists {
			t.Error("Expected object to not exist")
		}
	})
}

func TestCommandContext_ConfigCommand(t *testing.T) {
	cfg := &Config{
		Backend:      "local",
		BackendPath:  "/tmp/storage",
		OutputFormat: "text",
	}

	ctx := &CommandContext{
		Storage: newMockStorage(),
		Config:  cfg,
	}

	result := ctx.ConfigCommand()
	if result != cfg {
		t.Error("ConfigCommand returned wrong config")
	}
}

func TestCommandContext_Close(t *testing.T) {
	ctx := &CommandContext{
		Storage: newMockStorage(),
		Config:  &Config{},
	}

	err := ctx.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}
}

func TestPutCommandWithLargeFile(t *testing.T) {
	storage := newMockStorage()
	ctx := &CommandContext{
		Storage: storage,
		Config:  &Config{OutputFormat: "text"},
	}

	// Create a larger test file
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "large.txt")
	content := strings.Repeat("x", 10000)
	if err := os.WriteFile(testFile, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	err := ctx.PutCommand("large-key", testFile)
	if err != nil {
		t.Errorf("PutCommand failed: %v", err)
	}

	// Verify size in metadata
	if storage.metadata["large-key"].Size != 10000 {
		t.Errorf("Expected size 10000, got %d", storage.metadata["large-key"].Size)
	}
}

func TestGetCommandReaderError(t *testing.T) {
	// Test error case when reader fails during copy
	storage := newMockStorage()
	storage.data["test-key"] = []byte("content")

	ctx := &CommandContext{
		Storage: storage,
		Config:  &Config{OutputFormat: "text"},
	}

	// Create output in a directory that doesn't exist
	err := ctx.GetCommand("test-key", "/invalid/nonexistent/path/file.txt")
	if err == nil {
		t.Error("Expected error when output file cannot be created")
	}
}

func TestCommandContext_PutCommand_Stdin(t *testing.T) {
	t.Run("put from stdin with empty string", func(t *testing.T) {
		storage := newMockStorage()
		ctx := &CommandContext{
			Storage: storage,
			Config:  &Config{OutputFormat: "text"},
		}

		// Simulate stdin by creating a pipe
		r, w, err := os.Pipe()
		if err != nil {
			t.Fatalf("Failed to create pipe: %v", err)
		}

		// Save original stdin and restore after test
		oldStdin := os.Stdin
		defer func() { os.Stdin = oldStdin }()
		os.Stdin = r

		// Write test data to stdin in goroutine
		testContent := "test content from stdin"
		go func() {
			defer w.Close()
			w.Write([]byte(testContent))
		}()

		// Call PutCommand with empty string to indicate stdin
		err = ctx.PutCommand("test-key", "")
		if err != nil {
			t.Errorf("PutCommand failed: %v", err)
		}

		// Verify data was stored
		if string(storage.data["test-key"]) != testContent {
			t.Errorf("Data mismatch: got %q, want %q", string(storage.data["test-key"]), testContent)
		}

		// Verify metadata was stored with size 0 (unknown)
		if storage.metadata["test-key"] == nil {
			t.Error("Expected metadata")
		}
		if storage.metadata["test-key"].Size != 0 {
			t.Errorf("Expected metadata size 0 for stdin, got %d", storage.metadata["test-key"].Size)
		}
	})

	t.Run("put from stdin with dash", func(t *testing.T) {
		storage := newMockStorage()
		ctx := &CommandContext{
			Storage: storage,
			Config:  &Config{OutputFormat: "text"},
		}

		// Simulate stdin by creating a pipe
		r, w, err := os.Pipe()
		if err != nil {
			t.Fatalf("Failed to create pipe: %v", err)
		}

		// Save original stdin and restore after test
		oldStdin := os.Stdin
		defer func() { os.Stdin = oldStdin }()
		os.Stdin = r

		// Write test data to stdin in goroutine
		testContent := "test content from dash"
		go func() {
			defer w.Close()
			w.Write([]byte(testContent))
		}()

		// Call PutCommand with dash to indicate stdin
		err = ctx.PutCommand("test-key-2", "-")
		if err != nil {
			t.Errorf("PutCommand failed: %v", err)
		}

		// Verify data was stored
		if string(storage.data["test-key-2"]) != testContent {
			t.Errorf("Data mismatch: got %q, want %q", string(storage.data["test-key-2"]), testContent)
		}
	})

	t.Run("pipe get to put", func(t *testing.T) {
		sourceStorage := newMockStorage()
		sourceStorage.data["source-key"] = []byte("piped content")

		destStorage := newMockStorage()

		sourceCtx := &CommandContext{
			Storage: sourceStorage,
			Config:  &Config{OutputFormat: "text"},
		}

		destCtx := &CommandContext{
			Storage: destStorage,
			Config:  &Config{OutputFormat: "text"},
		}

		// Create a pipe to simulate piping get output to put input
		r, w, err := os.Pipe()
		if err != nil {
			t.Fatalf("Failed to create pipe: %v", err)
		}

		// Save original stdout/stdin and restore after test
		oldStdout := os.Stdout
		oldStdin := os.Stdin
		defer func() {
			os.Stdout = oldStdout
			os.Stdin = oldStdin
		}()
		os.Stdout = w
		os.Stdin = r

		// Run get in goroutine
		done := make(chan error)
		go func() {
			err := sourceCtx.GetCommand("source-key", "")
			w.Close()
			done <- err
		}()

		// Run put
		err = destCtx.PutCommand("dest-key", "-")
		if err != nil {
			t.Errorf("PutCommand failed: %v", err)
		}

		// Wait for get to complete
		if err := <-done; err != nil {
			t.Errorf("GetCommand failed: %v", err)
		}

		// Verify data was piped correctly
		if string(destStorage.data["dest-key"]) != "piped content" {
			t.Errorf("Data mismatch: got %q, want %q", string(destStorage.data["dest-key"]), "piped content")
		}
	})
}

func TestGetMetadataCommand(t *testing.T) {
	storage := &mockStorage{
		data:     make(map[string][]byte),
		metadata: make(map[string]*common.Metadata),
	}
	ctx := &CommandContext{
		Storage: storage,
		Config:  &Config{Backend: "test"},
	}

	// Put object with metadata
	storage.data["test-key"] = []byte("test data")
	storage.metadata["test-key"] = &common.Metadata{
		Size:            9,
		ContentType:     "text/plain",
		ContentEncoding: "utf-8",
		Custom:          map[string]string{"author": "test"},
	}

	// Test successful get metadata
	metadata, err := ctx.GetMetadataCommand("test-key")
	if err != nil {
		t.Errorf("GetMetadataCommand failed: %v", err)
	}
	if metadata == nil {
		t.Fatal("Expected metadata, got nil")
	}
	if metadata.ContentType != "text/plain" {
		t.Errorf("Expected content type 'text/plain', got %q", metadata.ContentType)
	}

	// Test nonexistent key
	_, err = ctx.GetMetadataCommand("nonexistent")
	if err == nil {
		t.Error("Expected error for nonexistent key")
	}
}

func TestUpdateMetadataCommand(t *testing.T) {
	storage := &mockStorage{
		data:     make(map[string][]byte),
		metadata: make(map[string]*common.Metadata),
	}
	ctx := &CommandContext{
		Storage: storage,
		Config:  &Config{Backend: "test"},
	}

	// Put object
	storage.data["test-key"] = []byte("test data")
	storage.metadata["test-key"] = &common.Metadata{Size: 9}

	// Update metadata
	custom := map[string]string{"version": "1.0"}
	err := ctx.UpdateMetadataCommand("test-key", "application/json", "gzip", custom)
	if err != nil {
		t.Errorf("UpdateMetadataCommand failed: %v", err)
	}

	// Verify metadata was updated
	metadata := storage.metadata["test-key"]
	if metadata.ContentType != "application/json" {
		t.Errorf("Expected content type 'application/json', got %q", metadata.ContentType)
	}
	if metadata.ContentEncoding != "gzip" {
		t.Errorf("Expected encoding 'gzip', got %q", metadata.ContentEncoding)
	}
	if metadata.Custom["version"] != "1.0" {
		t.Errorf("Expected custom version '1.0', got %q", metadata.Custom["version"])
	}

	// Test nonexistent key
	err = ctx.UpdateMetadataCommand("nonexistent", "text/plain", "", nil)
	if err == nil {
		t.Error("Expected error for nonexistent key")
	}
}

func TestHealthCommand(t *testing.T) {
	storage := &mockStorage{
		data:     make(map[string][]byte),
		metadata: make(map[string]*common.Metadata),
	}
	ctx := &CommandContext{
		Storage: storage,
		Config:  &Config{Backend: "test"},
	}

	health, err := ctx.HealthCommand()
	if err != nil {
		t.Errorf("HealthCommand failed: %v", err)
	}
	if health == nil {
		t.Fatal("Expected health result, got nil")
	}
	if status, ok := health["status"]; !ok || status != "healthy" {
		t.Error("Expected status to be 'healthy'")
	}
	if backend, ok := health["backend"]; !ok || backend != "test" {
		t.Errorf("Expected backend 'test', got %v", backend)
	}
}

func TestHealthCommand_WithClient(t *testing.T) {
	mockClient := &mockClient{
		healthError: nil,
	}
	ctx := &CommandContext{
		Client: mockClient,
		Config: &Config{
			Server:         "localhost:8080",
			ServerProtocol: "grpc",
		},
	}

	health, err := ctx.HealthCommand()
	if err != nil {
		t.Errorf("HealthCommand with client failed: %v", err)
	}
	if status, ok := health["status"]; !ok || status != "healthy" {
		t.Error("Expected status to be 'healthy'")
	}
	if mode, ok := health["mode"]; !ok || mode != "remote" {
		t.Errorf("Expected mode 'remote', got %v", mode)
	}

	// Test client health error
	mockClient.healthError = errors.New("connection refused")
	health, err = ctx.HealthCommand()
	if err == nil {
		t.Error("Expected error when client health fails")
	}
	if status, ok := health["status"]; !ok || status != "unhealthy" {
		t.Errorf("Expected status 'unhealthy', got %v", health["status"])
	}
}

func TestExistsCommand_EdgeCases(t *testing.T) {
	// Test with client
	mockClient := &mockClient{}
	ctx := &CommandContext{
		Client: mockClient,
		Config: &Config{},
	}

	exists, err := ctx.ExistsCommand("test.txt")
	if err != nil {
		t.Errorf("ExistsCommand with client failed: %v", err)
	}
	if !exists {
		t.Error("Expected object to exist")
	}

	// Test with client error
	mockClient.existsError = errors.New("storage error")
	_, err = ctx.ExistsCommand("test.txt")
	if err == nil {
		t.Error("Expected error from client")
	}
}

func TestPutCommandWithMetadata_EdgeCases(t *testing.T) {
	// Create a temporary file for testing
	tmpFile, err := os.CreateTemp("", "test-metadata-*.json")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	tmpFile.WriteString("{}")
	tmpFile.Close()

	storage := newMockStorage()
	ctx := &CommandContext{
		Storage: storage,
		Config:  &Config{},
	}

	// Test with all metadata fields
	customFields := map[string]string{
		"key1": "value1",
		"key2": "value2",
	}

	err = ctx.PutCommandWithMetadata("test-full-metadata.json", tmpFile.Name(), "application/json", "gzip", customFields)
	if err != nil {
		t.Errorf("PutCommandWithMetadata failed: %v", err)
	}

	// Verify metadata was set
	if storage.metadata["test-full-metadata.json"].ContentType != "application/json" {
		t.Error("ContentType not set correctly")
	}
	if storage.metadata["test-full-metadata.json"].ContentEncoding != "gzip" {
		t.Error("ContentEncoding not set correctly")
	}
}

func TestNewCommandContext_ErrorPaths(t *testing.T) {
	// Test with invalid config (missing backend)
	cfg := &Config{}
	_, err := NewCommandContext(cfg)
	if err == nil {
		t.Error("Expected error for missing backend")
	}
}

func TestClose_WithClient(t *testing.T) {
	mockClient := &mockClient{}
	ctx := &CommandContext{
		Client: mockClient,
		Config: &Config{},
	}

	err := ctx.Close()
	if err != nil {
		t.Errorf("Close with client failed: %v", err)
	}
}

func TestListCommand_WithClient(t *testing.T) {
	mockClient := &mockClient{}
	ctx := &CommandContext{
		Client: mockClient,
		Config: &Config{},
	}

	result, err := ctx.ListCommand("")
	if err != nil {
		t.Errorf("ListCommand with client failed: %v", err)
	}
	if result == nil {
		t.Error("Expected result, got nil")
	}
}

func TestGetCommand_WithClient(t *testing.T) {
	mockClient := &mockClient{}
	ctx := &CommandContext{
		Client: mockClient,
		Config: &Config{},
	}

	tmpDir := t.TempDir()
	outPath := filepath.Join(tmpDir, "output.txt")

	err := ctx.GetCommand("test.txt", outPath)
	if err != nil {
		t.Errorf("GetCommand with client failed: %v", err)
	}

	// Verify file was created
	if _, err := os.Stat(outPath); os.IsNotExist(err) {
		t.Error("Expected output file to be created")
	}
}

func TestDeleteCommand_WithClient(t *testing.T) {
	mockClient := &mockClient{}
	ctx := &CommandContext{
		Client: mockClient,
		Config: &Config{},
	}

	err := ctx.DeleteCommand("test.txt")
	if err != nil {
		t.Errorf("DeleteCommand with client failed: %v", err)
	}
}

// Replication operations - stub implementations for mock
func (m *mockClient) AddReplicationPolicy(ctx context.Context, policy common.ReplicationPolicy) error {
	return nil
}

func (m *mockClient) RemoveReplicationPolicy(ctx context.Context, policyID string) error {
	return nil
}

func (m *mockClient) GetReplicationPolicy(ctx context.Context, policyID string) (*common.ReplicationPolicy, error) {
	return nil, nil
}

func (m *mockClient) GetReplicationPolicies(ctx context.Context) ([]common.ReplicationPolicy, error) {
	return nil, nil
}

func (m *mockClient) TriggerReplication(ctx context.Context, policyID string) (*common.SyncResult, error) {
	return nil, nil
}

func (m *mockClient) GetReplicationStatus(ctx context.Context, policyID string) (*replication.ReplicationStatus, error) {
	return nil, nil
}
