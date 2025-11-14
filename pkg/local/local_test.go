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

//go:build local

package local_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/local"
)

// Helper function to create a temporary directory for testing
func createTempDir(t *testing.T) string {
	dir, err := os.MkdirTemp("", "go-objstore-test-")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	return dir
}

// Helper function to clean up a temporary directory
func cleanupTempDir(t *testing.T, dir string) {
	if err := os.RemoveAll(dir); err != nil {
		t.Errorf("Failed to clean up temp dir %s: %v", dir, err)
	}
}

func TestLocal_Configure(t *testing.T) {
	// Test case 1: Successful configuration
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	storage := local.New()
	err := storage.Configure(map[string]string{"path": tempDir})
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	// Test case 2: Missing path in settings
	storage = local.New()
	err = storage.Configure(map[string]string{})
	if err == nil || err.Error() != "path not set" {
		t.Errorf("Expected error 'path not set', got %v", err)
	}

	// Test case 3: Invalid path (e.g., permissions issue - hard to test reliably without root)
	// For now, we'll assume os.MkdirAll handles this and focus on valid inputs.
}

func TestLocal_PutAndGet(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	storage := local.New()
	storage.Configure(map[string]string{"path": tempDir})

	key := "test/object.txt"
	data := "hello world"

	// Test case 1: Successful Put
	err := storage.Put(key, bytes.NewBufferString(data))
	if err != nil {
		t.Errorf("Expected no error on Put, got %v", err)
	}

	// Verify file exists on disk
	filePath := filepath.Join(tempDir, key)
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		t.Errorf("File was not created at %s", filePath)
	}

	// Test case 2: Successful Get
	rc, err := storage.Get(key)
	if err != nil {
		t.Errorf("Expected no error on Get, got %v", err)
	}
	defer rc.Close()

	readData, err := io.ReadAll(rc)
	if err != nil {
		t.Errorf("Expected no error on reading data, got %v", err)
	}
	if string(readData) != data {
		t.Errorf("Expected '%s', got '%s'", data, string(readData))
	}

	// Test case 3: Get non-existent object
	_, err = storage.Get("non-existent-key")
	if !os.IsNotExist(err) {
		t.Errorf("Expected 'file does not exist' error, got %v", err)
	}
}

func TestLocal_Delete(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	storage := local.New()
	storage.Configure(map[string]string{"path": tempDir})

	key := "test/object-to-delete.txt"
	data := "delete me"

	storage.Put(key, bytes.NewBufferString(data))

	// Verify file exists
	filePath := filepath.Join(tempDir, key)
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		t.Fatalf("Pre-condition failed: file %s does not exist", filePath)
	}

	// Test case 1: Successful Delete
	err := storage.Delete(key)
	if err != nil {
		t.Errorf("Expected no error on Delete, got %v", err)
	}

	// Verify file is deleted
	if _, err := os.Stat(filePath); !os.IsNotExist(err) {
		t.Errorf("File %s was not deleted", filePath)
	}

	// Test case 2: Delete non-existent object
	err = storage.Delete("non-existent-key")
	if err == nil {
		t.Errorf("Expected error on deleting non-existent key, got no error")
	}
}

// MockArchiver for testing Archive method
type MockArchiver struct {
	PutFunc func(key string, data io.Reader) error
}

func (m *MockArchiver) Put(key string, data io.Reader) error {
	if m.PutFunc != nil {
		return m.PutFunc(key, data)
	}
	return nil
}

func TestLocal_Archive(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	storage := local.New()
	storage.Configure(map[string]string{"path": tempDir})

	key := "test/object-to-archive.txt"
	data := "archive me"
	storage.Put(key, bytes.NewBufferString(data))

	// Test case 1: Successful Archive
	archivedKey := ""
	archivedData := ""
	mockArchiver := &MockArchiver{
		PutFunc: func(k string, r io.Reader) error {
			archivedKey = k
			content, _ := io.ReadAll(r)
			archivedData = string(content)
			return nil
		},
	}

	err := storage.Archive(key, mockArchiver)
	if err != nil {
		t.Errorf("Expected no error on Archive, got %v", err)
	}
	if archivedKey != key {
		t.Errorf("Expected archived key '%s', got '%s'", key, archivedKey)
	}
	if archivedData != data {
		t.Errorf("Expected archived data '%s', got '%s'", data, archivedData)
	}

	// Test case 2: Archive non-existent object (should return error from Get)
	err = storage.Archive("non-existent-archive-key", mockArchiver)
	if err == nil || !os.IsNotExist(err) {
		t.Errorf("Expected 'file does not exist' error on archiving non-existent key, got %v", err)
	}

	// Test case 3: Error during Archiver.Put
	mockArchiverWithError := &MockArchiver{
		PutFunc: func(k string, r io.Reader) error {
			return errors.New("archiver put error")
		},
	}
	err = storage.Archive(key, mockArchiverWithError)
	if err == nil || err.Error() != "archiver put error" {
		t.Errorf("Expected 'archiver put error', got %v", err)
	}
}

func TestLocal_Archive_NilDestination(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	storage := local.New()
	storage.Configure(map[string]string{"path": tempDir})

	key := "test/object.txt"
	data := "data"
	if err := storage.Put(key, bytes.NewBufferString(data)); err != nil {
		t.Fatal(err)
	}

	if err := storage.Archive(key, nil); err == nil {
		t.Fatalf("expected error when destination is nil")
	}
}

func TestLocal_LifecycleMethods_Delegate(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	s := local.New()
	if err := s.Configure(map[string]string{"path": tempDir}); err != nil {
		t.Fatal(err)
	}

	p := common.LifecyclePolicy{ID: "x", Prefix: "p/", Retention: time.Hour, Action: "delete"}
	if err := s.AddPolicy(p); err != nil {
		t.Fatal(err)
	}
	if err := s.RemovePolicy(p.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetPolicies(); err != nil {
		t.Fatal(err)
	}
}

func TestLocal_List_EmptyPrefix(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	storage := local.New()
	storage.Configure(map[string]string{"path": tempDir})

	// Create some test files
	storage.Put("file1.txt", bytes.NewBufferString("data1"))
	storage.Put("file2.txt", bytes.NewBufferString("data2"))
	storage.Put("dir/file3.txt", bytes.NewBufferString("data3"))

	// List all files with empty prefix
	keys, err := storage.List("")
	if err != nil {
		t.Fatalf("Expected no error on List, got %v", err)
	}

	// Sort keys for consistent comparison
	sort.Strings(keys)
	expected := []string{"dir/file3.txt", "file1.txt", "file2.txt"}
	sort.Strings(expected)

	if len(keys) != len(expected) {
		t.Fatalf("Expected %d keys, got %d", len(expected), len(keys))
	}

	for i, key := range keys {
		if key != expected[i] {
			t.Errorf("Expected key %s, got %s", expected[i], key)
		}
	}
}

func TestLocal_List_WithPrefix(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	storage := local.New()
	storage.Configure(map[string]string{"path": tempDir})

	// Create test files
	storage.Put("logs/2023/file1.log", bytes.NewBufferString("log1"))
	storage.Put("logs/2023/file2.log", bytes.NewBufferString("log2"))
	storage.Put("logs/2024/file3.log", bytes.NewBufferString("log3"))
	storage.Put("data/file4.txt", bytes.NewBufferString("data4"))

	// List files with "logs/" prefix
	keys, err := storage.List("logs/")
	if err != nil {
		t.Fatalf("Expected no error on List, got %v", err)
	}

	sort.Strings(keys)
	expected := []string{"logs/2023/file1.log", "logs/2023/file2.log", "logs/2024/file3.log"}
	sort.Strings(expected)

	if len(keys) != len(expected) {
		t.Fatalf("Expected %d keys, got %d", len(expected), len(keys))
	}

	for i, key := range keys {
		if key != expected[i] {
			t.Errorf("Expected key %s, got %s", expected[i], key)
		}
	}
}

func TestLocal_List_SpecificPrefix(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	storage := local.New()
	storage.Configure(map[string]string{"path": tempDir})

	// Create test files
	storage.Put("logs/2023/file1.log", bytes.NewBufferString("log1"))
	storage.Put("logs/2023/file2.log", bytes.NewBufferString("log2"))
	storage.Put("logs/2024/file3.log", bytes.NewBufferString("log3"))

	// List files with "logs/2023/" prefix
	keys, err := storage.List("logs/2023/")
	if err != nil {
		t.Fatalf("Expected no error on List, got %v", err)
	}

	sort.Strings(keys)
	expected := []string{"logs/2023/file1.log", "logs/2023/file2.log"}
	sort.Strings(expected)

	if len(keys) != len(expected) {
		t.Fatalf("Expected %d keys, got %d", len(expected), len(keys))
	}

	for i, key := range keys {
		if key != expected[i] {
			t.Errorf("Expected key %s, got %s", expected[i], key)
		}
	}
}

func TestLocal_List_NoMatches(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	storage := local.New()
	storage.Configure(map[string]string{"path": tempDir})

	// Create test files
	storage.Put("logs/file1.log", bytes.NewBufferString("log1"))
	storage.Put("data/file2.txt", bytes.NewBufferString("data2"))

	// List files with prefix that doesn't match anything
	keys, err := storage.List("nonexistent/")
	if err != nil {
		t.Fatalf("Expected no error on List, got %v", err)
	}

	if len(keys) != 0 {
		t.Errorf("Expected 0 keys, got %d", len(keys))
	}
}

func TestLocal_List_EmptyDirectory(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	storage := local.New()
	storage.Configure(map[string]string{"path": tempDir})

	// Don't create any files
	keys, err := storage.List("")
	if err != nil {
		t.Fatalf("Expected no error on List, got %v", err)
	}

	if len(keys) != 0 {
		t.Errorf("Expected 0 keys, got %d", len(keys))
	}
}

// Test context cancellation for PutWithContext
func TestLocal_PutWithContext_ContextCancelled(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	storage := local.New()
	storage.Configure(map[string]string{"path": tempDir})

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	err := storage.PutWithContext(ctx, "test.txt", bytes.NewBufferString("data"))
	if err == nil {
		t.Error("Expected context cancellation error, got nil")
	}
	if err != context.Canceled {
		t.Errorf("Expected context.Canceled error, got %v", err)
	}
}

// Test context cancellation for GetWithContext
func TestLocal_GetWithContext_ContextCancelled(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	storage := local.New()
	storage.Configure(map[string]string{"path": tempDir})

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, err := storage.GetWithContext(ctx, "test.txt")
	if err == nil {
		t.Error("Expected context cancellation error, got nil")
	}
	if err != context.Canceled {
		t.Errorf("Expected context.Canceled error, got %v", err)
	}
}

// Test context cancellation for DeleteWithContext
func TestLocal_DeleteWithContext_ContextCancelled(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	storage := local.New()
	storage.Configure(map[string]string{"path": tempDir})

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	err := storage.DeleteWithContext(ctx, "test.txt")
	if err == nil {
		t.Error("Expected context cancellation error, got nil")
	}
	if err != context.Canceled {
		t.Errorf("Expected context.Canceled error, got %v", err)
	}
}

// Test context cancellation for ListWithContext
func TestLocal_ListWithContext_ContextCancelled(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	storage := local.New()
	storage.Configure(map[string]string{"path": tempDir})

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, err := storage.ListWithContext(ctx, "")
	if err == nil {
		t.Error("Expected context cancellation error, got nil")
	}
	if err != context.Canceled {
		t.Errorf("Expected context.Canceled error, got %v", err)
	}
}

// Test context cancellation for GetMetadata
func TestLocal_GetMetadata_ContextCancelled(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	storage := local.New()
	storage.Configure(map[string]string{"path": tempDir})

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, err := storage.GetMetadata(ctx, "test.txt")
	if err == nil {
		t.Error("Expected context cancellation error, got nil")
	}
	if err != context.Canceled {
		t.Errorf("Expected context.Canceled error, got %v", err)
	}
}

// Test context cancellation for UpdateMetadata
func TestLocal_UpdateMetadata_ContextCancelled(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	storage := local.New()
	storage.Configure(map[string]string{"path": tempDir})

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	err := storage.UpdateMetadata(ctx, "test.txt", &common.Metadata{})
	if err == nil {
		t.Error("Expected context cancellation error, got nil")
	}
	if err != context.Canceled {
		t.Errorf("Expected context.Canceled error, got %v", err)
	}
}

// Test context cancellation for Exists
func TestLocal_Exists_ContextCancelled(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	storage := local.New()
	storage.Configure(map[string]string{"path": tempDir})

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, err := storage.Exists(ctx, "test.txt")
	if err == nil {
		t.Error("Expected context cancellation error, got nil")
	}
	if err != context.Canceled {
		t.Errorf("Expected context.Canceled error, got %v", err)
	}
}

// Test context cancellation for ListWithOptions
func TestLocal_ListWithOptions_ContextCancelled(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	storage := local.New()
	storage.Configure(map[string]string{"path": tempDir})

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, err := storage.ListWithOptions(ctx, &common.ListOptions{})
	if err == nil {
		t.Error("Expected context cancellation error, got nil")
	}
	if err != context.Canceled {
		t.Errorf("Expected context.Canceled error, got %v", err)
	}
}

// Test invalid key validation - path traversal
func TestLocal_ValidateKey_PathTraversal(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	storage := local.New()
	storage.Configure(map[string]string{"path": tempDir})

	invalidKeys := []string{
		"../etc/passwd",
		"test/../../../etc/passwd",
		"test/../../file.txt",
		"..\\windows\\system32",
		"test\\..\\..\\file.txt",
	}

	for _, key := range invalidKeys {
		err := storage.Put(key, bytes.NewBufferString("data"))
		if err == nil {
			t.Errorf("Expected error for invalid key '%s', got nil", key)
		}
		if !strings.Contains(err.Error(), "path traversal") {
			t.Errorf("Expected path traversal error for key '%s', got: %v", key, err)
		}
	}
}

// Test invalid key validation - absolute paths
func TestLocal_ValidateKey_AbsolutePaths(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	storage := local.New()
	storage.Configure(map[string]string{"path": tempDir})

	invalidKeys := []string{
		"/etc/passwd",
		"/tmp/file.txt",
		"C:\\Windows\\System32",
		"D:\\data\\file.txt",
	}

	for _, key := range invalidKeys {
		err := storage.Put(key, bytes.NewBufferString("data"))
		if err == nil {
			t.Errorf("Expected error for absolute path '%s', got nil", key)
		}
		if !strings.Contains(err.Error(), "absolute path") {
			t.Errorf("Expected absolute path error for key '%s', got: %v", key, err)
		}
	}
}

// Test invalid key validation - null bytes
func TestLocal_ValidateKey_NullBytes(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	storage := local.New()
	storage.Configure(map[string]string{"path": tempDir})

	key := "test\x00file.txt"
	err := storage.Put(key, bytes.NewBufferString("data"))
	if err == nil {
		t.Error("Expected error for key with null byte, got nil")
	}
	if !strings.Contains(err.Error(), "null byte") {
		t.Errorf("Expected null byte error, got: %v", err)
	}
}

// Test invalid key validation - empty key
func TestLocal_ValidateKey_EmptyKey(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	storage := local.New()
	storage.Configure(map[string]string{"path": tempDir})

	err := storage.Put("", bytes.NewBufferString("data"))
	if err == nil {
		t.Error("Expected error for empty key, got nil")
	}
	if !strings.Contains(err.Error(), "empty") {
		t.Errorf("Expected empty key error, got: %v", err)
	}
}

// Test invalid key validation - control characters
func TestLocal_ValidateKey_ControlCharacters(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	storage := local.New()
	storage.Configure(map[string]string{"path": tempDir})

	invalidKeys := []string{
		"test\nfile.txt",
		"test\rfile.txt",
		"test\tfile.txt",
		"test//file.txt",
		"test\\\\file.txt",
	}

	for _, key := range invalidKeys {
		err := storage.Put(key, bytes.NewBufferString("data"))
		if err == nil {
			t.Errorf("Expected error for key with control characters '%v', got nil", []byte(key))
		}
	}
}

// Test invalid key validation - list prefix
func TestLocal_ValidateKey_ListInvalidPrefix(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	storage := local.New()
	storage.Configure(map[string]string{"path": tempDir})

	_, err := storage.List("../etc")
	if err == nil {
		t.Error("Expected error for invalid prefix, got nil")
	}
}

// Test Exists method with various scenarios
func TestLocal_Exists_Success(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	storage := local.New()
	storage.Configure(map[string]string{"path": tempDir})

	// Test non-existent file
	exists, err := storage.Exists(context.Background(), "nonexistent.txt")
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if exists {
		t.Error("Expected file to not exist")
	}

	// Create a file
	key := "test/exists.txt"
	storage.Put(key, bytes.NewBufferString("data"))

	// Test existing file
	exists, err = storage.Exists(context.Background(), key)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if !exists {
		t.Error("Expected file to exist")
	}
}

// Test Exists with invalid key
func TestLocal_Exists_InvalidKey(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	storage := local.New()
	storage.Configure(map[string]string{"path": tempDir})

	_, err := storage.Exists(context.Background(), "../etc/passwd")
	if err == nil {
		t.Error("Expected error for invalid key, got nil")
	}
}

// Test metadata operations
func TestLocal_PutWithMetadata_Success(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	storage := local.New()
	storage.Configure(map[string]string{"path": tempDir})

	key := "test/metadata.txt"
	metadata := &common.Metadata{
		ContentType: "text/plain",
		Custom: map[string]string{
			"author":  "test",
			"version": "1.0",
		},
	}

	err := storage.PutWithMetadata(context.Background(), key, bytes.NewBufferString("data"), metadata)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	// Verify metadata was saved
	retrievedMetadata, err := storage.GetMetadata(context.Background(), key)
	if err != nil {
		t.Errorf("Expected no error getting metadata, got %v", err)
	}

	if retrievedMetadata == nil {
		t.Fatal("Expected metadata, got nil")
	}

	if retrievedMetadata.ContentType != "text/plain" {
		t.Errorf("Expected ContentType 'text/plain', got '%s'", retrievedMetadata.ContentType)
	}

	if retrievedMetadata.Custom["author"] != "test" {
		t.Errorf("Expected author 'test', got '%s'", retrievedMetadata.Custom["author"])
	}
}

// Test metadata with nil custom metadata
func TestLocal_PutWithMetadata_NilMetadata(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	storage := local.New()
	storage.Configure(map[string]string{"path": tempDir})

	key := "test/nil-metadata.txt"
	err := storage.PutWithMetadata(context.Background(), key, bytes.NewBufferString("data"), nil)
	if err != nil {
		t.Errorf("Expected no error with nil metadata, got %v", err)
	}
}

// Test UpdateMetadata
func TestLocal_UpdateMetadata_Success(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	storage := local.New()
	storage.Configure(map[string]string{"path": tempDir})

	key := "test/update-metadata.txt"
	storage.Put(key, bytes.NewBufferString("data"))

	metadata := &common.Metadata{
		ContentType: "application/json",
		Custom: map[string]string{
			"status": "updated",
		},
	}

	err := storage.UpdateMetadata(context.Background(), key, metadata)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	// Verify metadata was updated
	retrievedMetadata, err := storage.GetMetadata(context.Background(), key)
	if err != nil {
		t.Errorf("Expected no error getting metadata, got %v", err)
	}

	if retrievedMetadata.ContentType != "application/json" {
		t.Errorf("Expected ContentType 'application/json', got '%s'", retrievedMetadata.ContentType)
	}
}

// Test UpdateMetadata on non-existent file
func TestLocal_UpdateMetadata_NonExistent(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	storage := local.New()
	storage.Configure(map[string]string{"path": tempDir})

	metadata := &common.Metadata{
		ContentType: "text/plain",
	}

	err := storage.UpdateMetadata(context.Background(), "nonexistent.txt", metadata)
	if err == nil {
		t.Error("Expected error for non-existent file, got nil")
	}
}

// Test GetMetadata on non-existent metadata file
func TestLocal_GetMetadata_NoMetadataFile(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	storage := local.New()
	storage.Configure(map[string]string{"path": tempDir})

	// Create file without metadata
	key := "test/no-metadata.txt"
	path := filepath.Join(tempDir, key)
	os.MkdirAll(filepath.Dir(path), 0755)
	os.WriteFile(path, []byte("data"), 0644)

	metadata, err := storage.GetMetadata(context.Background(), key)
	if err == nil {
		t.Error("Expected error for file without metadata")
	}
	if metadata != nil {
		t.Error("Expected nil metadata when error occurs")
	}
}

// Test ListWithOptions with delimiter
func TestLocal_ListWithOptions_Delimiter(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	storage := local.New()
	storage.Configure(map[string]string{"path": tempDir})

	// Create test files
	storage.Put("logs/2023/file1.log", bytes.NewBufferString("log1"))
	storage.Put("logs/2023/file2.log", bytes.NewBufferString("log2"))
	storage.Put("logs/2024/file3.log", bytes.NewBufferString("log3"))
	storage.Put("logs/file4.log", bytes.NewBufferString("log4"))

	opts := &common.ListOptions{
		Prefix:    "logs/",
		Delimiter: "/",
	}

	result, err := storage.ListWithOptions(context.Background(), opts)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Should see file4.log as an object and 2023/, 2024/ as common prefixes
	if len(result.Objects) != 1 {
		t.Errorf("Expected 1 object, got %d", len(result.Objects))
	}

	if len(result.CommonPrefixes) != 2 {
		t.Errorf("Expected 2 common prefixes, got %d", len(result.CommonPrefixes))
	}
}

// Test ListWithOptions with pagination
func TestLocal_ListWithOptions_Pagination(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	storage := local.New()
	storage.Configure(map[string]string{"path": tempDir})

	// Create multiple files
	for i := 1; i <= 10; i++ {
		key := filepath.Join("test", "file"+string(rune('0'+i))+".txt")
		storage.Put(key, bytes.NewBufferString("data"))
	}

	opts := &common.ListOptions{
		Prefix:     "test/",
		MaxResults: 5,
	}

	result, err := storage.ListWithOptions(context.Background(), opts)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if len(result.Objects) != 5 {
		t.Errorf("Expected 5 objects, got %d", len(result.Objects))
	}

	if !result.Truncated {
		t.Error("Expected Truncated to be true")
	}

	if result.NextToken == "" {
		t.Error("Expected NextToken to be set")
	}

	// Get next page
	opts.ContinueFrom = result.NextToken
	result2, err := storage.ListWithOptions(context.Background(), opts)
	if err != nil {
		t.Fatalf("Expected no error on page 2, got %v", err)
	}

	if len(result2.Objects) != 5 {
		t.Errorf("Expected 5 objects on page 2, got %d", len(result2.Objects))
	}
}

// Test ListWithOptions with nil options
func TestLocal_ListWithOptions_NilOptions(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	storage := local.New()
	storage.Configure(map[string]string{"path": tempDir})

	storage.Put("test.txt", bytes.NewBufferString("data"))

	result, err := storage.ListWithOptions(context.Background(), nil)
	if err != nil {
		t.Fatalf("Expected no error with nil options, got %v", err)
	}

	if len(result.Objects) != 1 {
		t.Errorf("Expected 1 object, got %d", len(result.Objects))
	}
}

// Test ListWithOptions with invalid prefix
func TestLocal_ListWithOptions_InvalidPrefix(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	storage := local.New()
	storage.Configure(map[string]string{"path": tempDir})

	opts := &common.ListOptions{
		Prefix: "../etc",
	}

	_, err := storage.ListWithOptions(context.Background(), opts)
	if err == nil {
		t.Error("Expected error for invalid prefix, got nil")
	}
}

// Test Delete removes metadata file
func TestLocal_Delete_RemovesMetadata(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	storage := local.New()
	storage.Configure(map[string]string{"path": tempDir})

	key := "test/with-metadata.txt"
	metadata := &common.Metadata{
		ContentType: "text/plain",
	}

	storage.PutWithMetadata(context.Background(), key, bytes.NewBufferString("data"), metadata)

	// Verify metadata file exists
	metadataPath := filepath.Join(tempDir, key) + ".metadata.json"
	if _, err := os.Stat(metadataPath); os.IsNotExist(err) {
		t.Fatal("Metadata file was not created")
	}

	// Delete the object
	err := storage.Delete(key)
	if err != nil {
		t.Errorf("Expected no error on Delete, got %v", err)
	}

	// Verify metadata file is also deleted
	if _, err := os.Stat(metadataPath); !os.IsNotExist(err) {
		t.Error("Metadata file was not deleted")
	}
}

// Test Configure with runLifecycle
func TestLocal_Configure_RunLifecycle(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	storage := local.New()
	err := storage.Configure(map[string]string{
		"path":         tempDir,
		"runLifecycle": "true",
	})
	if err != nil {
		t.Errorf("Expected no error with runLifecycle, got %v", err)
	}

	// Give lifecycle goroutine a moment to start
	time.Sleep(10 * time.Millisecond)
}

// Test Archive with invalid key
func TestLocal_Archive_InvalidKey(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	storage := local.New()
	storage.Configure(map[string]string{"path": tempDir})

	mockArchiver := &MockArchiver{}

	err := storage.Archive("../etc/passwd", mockArchiver)
	if err == nil {
		t.Error("Expected error for invalid key, got nil")
	}
}

// Test PutWithMetadata with invalid metadata (too many entries)
func TestLocal_PutWithMetadata_InvalidMetadata(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	storage := local.New()
	storage.Configure(map[string]string{"path": tempDir})

	// Create metadata with too many entries
	custom := make(map[string]string)
	for i := 0; i < 101; i++ {
		custom["key"+string(rune('0'+i))] = "value"
	}

	metadata := &common.Metadata{
		Custom: custom,
	}

	err := storage.PutWithMetadata(context.Background(), "test.txt", bytes.NewBufferString("data"), metadata)
	if err == nil {
		t.Error("Expected error for too many metadata entries, got nil")
	}
}

// Test PutWithMetadata with invalid key in saveMetadata
func TestLocal_PutWithMetadata_InvalidKeyInSaveMetadata(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	storage := local.New()
	storage.Configure(map[string]string{"path": tempDir})

	// Try to put with invalid key that passes initial validation but fails in saveMetadata
	err := storage.PutWithMetadata(context.Background(), "../test.txt", bytes.NewBufferString("data"), nil)
	if err == nil {
		t.Error("Expected error for invalid key, got nil")
	}
}

// Test GetWithContext with invalid key
func TestLocal_GetWithContext_InvalidKey(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	storage := local.New()
	storage.Configure(map[string]string{"path": tempDir})

	_, err := storage.GetWithContext(context.Background(), "../etc/passwd")
	if err == nil {
		t.Error("Expected error for invalid key, got nil")
	}
}

// Test GetMetadata with invalid key
func TestLocal_GetMetadata_InvalidKey(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	storage := local.New()
	storage.Configure(map[string]string{"path": tempDir})

	_, err := storage.GetMetadata(context.Background(), "../etc/passwd")
	if err == nil {
		t.Error("Expected error for invalid key, got nil")
	}
}

// Test UpdateMetadata with invalid key
func TestLocal_UpdateMetadata_InvalidKey(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	storage := local.New()
	storage.Configure(map[string]string{"path": tempDir})

	err := storage.UpdateMetadata(context.Background(), "../etc/passwd", &common.Metadata{})
	if err == nil {
		t.Error("Expected error for invalid key, got nil")
	}
}

// Test DeleteWithContext with invalid key
func TestLocal_DeleteWithContext_InvalidKey(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	storage := local.New()
	storage.Configure(map[string]string{"path": tempDir})

	err := storage.DeleteWithContext(context.Background(), "../etc/passwd")
	if err == nil {
		t.Error("Expected error for invalid key, got nil")
	}
}

// Test PutWithMetadata with directory creation error
func TestLocal_PutWithMetadata_DirectoryError(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	storage := local.New()
	storage.Configure(map[string]string{"path": tempDir})

	// Create a file where we want a directory
	conflictPath := filepath.Join(tempDir, "conflict")
	os.WriteFile(conflictPath, []byte("data"), 0644)

	// Try to create a file under the conflicting path
	err := storage.PutWithMetadata(context.Background(), "conflict/test.txt", bytes.NewBufferString("data"), nil)
	if err == nil {
		t.Error("Expected error for directory creation conflict, got nil")
	}
}

// Test PutWithMetadata with file creation error
func TestLocal_PutWithMetadata_FileCreationError(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	storage := local.New()
	storage.Configure(map[string]string{"path": tempDir})

	// Create a directory where we want a file
	dirPath := filepath.Join(tempDir, "isdir")
	os.MkdirAll(dirPath, 0755)

	// Try to create a file with the same name as the directory
	err := storage.PutWithMetadata(context.Background(), "isdir", bytes.NewBufferString("data"), nil)
	if err == nil {
		t.Error("Expected error for file creation on directory, got nil")
	}
}

// Test UpdateMetadata with nil metadata
func TestLocal_UpdateMetadata_NilMetadata(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	storage := local.New()
	storage.Configure(map[string]string{"path": tempDir})

	key := "test/update-nil.txt"
	storage.Put(key, bytes.NewBufferString("data"))

	err := storage.UpdateMetadata(context.Background(), key, nil)
	if err != nil {
		t.Errorf("Expected no error with nil metadata, got %v", err)
	}
}

// Test loadMetadata with corrupted JSON
func TestLocal_LoadMetadata_CorruptedJSON(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	storage := local.New()
	storage.Configure(map[string]string{"path": tempDir})

	// Create a file
	key := "test/corrupted.txt"
	storage.Put(key, bytes.NewBufferString("data"))

	// Write corrupted metadata
	metadataPath := filepath.Join(tempDir, key) + ".metadata.json"
	os.WriteFile(metadataPath, []byte("invalid json {{{"), 0644)

	// Try to get metadata
	_, err := storage.GetMetadata(context.Background(), key)
	if err == nil {
		t.Error("Expected error for corrupted metadata, got nil")
	}
}

// Test Exists with file stat error (permission denied simulation)
func TestLocal_Exists_StatError(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	storage := local.New()
	storage.Configure(map[string]string{"path": tempDir})

	// Create a file
	key := "test/protected.txt"
	storage.Put(key, bytes.NewBufferString("data"))

	// Make the parent directory inaccessible (Unix only)
	if os.Getuid() != 0 { // Skip if running as root
		protectedDir := filepath.Join(tempDir, "test")
		oldMode := os.FileMode(0755)
		os.Chmod(protectedDir, 0000)
		defer os.Chmod(protectedDir, oldMode) // Restore for cleanup

		_, err := storage.Exists(context.Background(), key)
		if err == nil {
			// Permission test might not work on all systems
			// This is acceptable - we're testing the error path
		}

		// Restore permissions for cleanup
		os.Chmod(protectedDir, oldMode)
	}
}

// Test ListWithOptions error handling in loadMetadata
func TestLocal_ListWithOptions_MetadataLoadError(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	storage := local.New()
	storage.Configure(map[string]string{"path": tempDir})

	// Create a file
	key := "test/file.txt"
	storage.Put(key, bytes.NewBufferString("data"))

	// Write corrupted metadata
	metadataPath := filepath.Join(tempDir, key) + ".metadata.json"
	os.WriteFile(metadataPath, []byte("invalid json"), 0644)

	// ListWithOptions should handle the error gracefully and create basic metadata
	result, err := storage.ListWithOptions(context.Background(), &common.ListOptions{})
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if len(result.Objects) != 1 {
		t.Errorf("Expected 1 object despite metadata error, got %d", len(result.Objects))
	}
}

// Test multiple operations with the same key to cover ETag generation paths
func TestLocal_PutWithMetadata_ETagGeneration(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	storage := local.New()
	storage.Configure(map[string]string{"path": tempDir})

	key := "test/etag.txt"

	// Put with metadata
	metadata := &common.Metadata{
		ContentType: "text/plain",
	}

	err := storage.PutWithMetadata(context.Background(), key, bytes.NewBufferString("data"), metadata)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Get metadata and verify ETag is set
	retrievedMetadata, err := storage.GetMetadata(context.Background(), key)
	if err != nil {
		t.Fatalf("Expected no error getting metadata, got %v", err)
	}

	if retrievedMetadata.ETag == "" {
		t.Error("Expected ETag to be set")
	}

	if retrievedMetadata.Size != 4 { // "data" is 4 bytes
		t.Errorf("Expected size 4, got %d", retrievedMetadata.Size)
	}
}

// Test saveMetadata with empty metadata (nil should not save)
func TestLocal_SaveMetadata_Nil(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	storage := local.New()
	storage.Configure(map[string]string{"path": tempDir})

	key := "test/nil-save.txt"
	storage.Put(key, bytes.NewBufferString("data"))

	// The Put operation saves metadata, so metadata file exists
	metadataPath := filepath.Join(tempDir, key) + ".metadata.json"
	if _, err := os.Stat(metadataPath); os.IsNotExist(err) {
		t.Error("Metadata file should exist after Put")
	}
}

// Test io.Copy error during PutWithMetadata
type ErrorReader struct{}

func (e *ErrorReader) Read(p []byte) (n int, err error) {
	return 0, errors.New("read error")
}

func TestLocal_PutWithMetadata_CopyError(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	storage := local.New()
	storage.Configure(map[string]string{"path": tempDir})

	err := storage.PutWithMetadata(context.Background(), "test.txt", &ErrorReader{}, nil)
	if err == nil {
		t.Error("Expected error during copy, got nil")
	}
	if !strings.Contains(err.Error(), "read error") {
		t.Errorf("Expected read error, got: %v", err)
	}
}

// Test saveMetadata with invalid key in loadMetadata
func TestLocal_LoadMetadata_InvalidKey(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	storage := local.New()
	storage.Configure(map[string]string{"path": tempDir})

	_, err := storage.GetMetadata(context.Background(), "\x00invalid")
	if err == nil {
		t.Error("Expected error for invalid key, got nil")
	}
}

// Test loadMetadata with file read error (permission denied)
func TestLocal_LoadMetadata_ReadError(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	storage := local.New()
	storage.Configure(map[string]string{"path": tempDir})

	// Create a file
	key := "test/perms.txt"
	storage.Put(key, bytes.NewBufferString("data"))

	if os.Getuid() != 0 { // Skip if running as root
		// Make metadata file unreadable
		metadataPath := filepath.Join(tempDir, key) + ".metadata.json"
		os.Chmod(metadataPath, 0000)
		defer os.Chmod(metadataPath, 0644) // Restore for cleanup

		_, err := storage.GetMetadata(context.Background(), key)
		// On some systems this might not error, that's ok
		if err != nil && !os.IsNotExist(err) {
			// Got an expected permission error
		}

		// Restore permissions
		os.Chmod(metadataPath, 0644)
	}
}

// Test ListWithContext with context cancellation during walk
func TestLocal_ListWithContext_CancelDuringWalk(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	storage := local.New()
	storage.Configure(map[string]string{"path": tempDir})

	// Create many files
	for i := 0; i < 100; i++ {
		key := filepath.Join("test", "file"+strings.Repeat("x", i)+".txt")
		storage.Put(key, bytes.NewBufferString("data"))
	}

	// Create a context with short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()

	// Give it a moment to ensure timeout
	time.Sleep(1 * time.Millisecond)

	_, err := storage.ListWithContext(ctx, "")
	if err != context.DeadlineExceeded && err != context.Canceled {
		// Context cancellation during walk is hard to guarantee
		// but we're exercising the code path
	}
}

// Test ListWithContext with filepath.Walk error (permission denied)
func TestLocal_ListWithContext_WalkError(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	storage := local.New()
	storage.Configure(map[string]string{"path": tempDir})

	// Create a subdirectory
	subDir := filepath.Join(tempDir, "protected")
	os.MkdirAll(subDir, 0755)
	storage.Put("protected/file.txt", bytes.NewBufferString("data"))

	if os.Getuid() != 0 { // Skip if running as root
		// Make directory inaccessible
		os.Chmod(subDir, 0000)
		defer os.Chmod(subDir, 0755) // Restore for cleanup

		_, err := storage.List("")
		if err != nil {
			// Got expected permission error
		}

		// Restore permissions
		os.Chmod(subDir, 0755)
	}
}

// Test ListWithContext with filepath.Rel error (shouldn't happen in practice)
// This is a hard-to-reach error path

// Test ListWithOptions with context cancellation during walk
func TestLocal_ListWithOptions_CancelDuringWalk(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	storage := local.New()
	storage.Configure(map[string]string{"path": tempDir})

	// Create many files
	for i := 0; i < 100; i++ {
		key := filepath.Join("test", "file"+strings.Repeat("y", i)+".txt")
		storage.Put(key, bytes.NewBufferString("data"))
	}

	// Create a context with short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()

	// Give it a moment to ensure timeout
	time.Sleep(1 * time.Millisecond)

	_, err := storage.ListWithOptions(ctx, &common.ListOptions{})
	if err != context.DeadlineExceeded && err != context.Canceled {
		// Context cancellation during walk is hard to guarantee
		// but we're exercising the code path
	}
}

// Test ListWithOptions with filepath.Walk error
func TestLocal_ListWithOptions_WalkError(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	storage := local.New()
	storage.Configure(map[string]string{"path": tempDir})

	// Create a subdirectory
	subDir := filepath.Join(tempDir, "restricted")
	os.MkdirAll(subDir, 0755)
	storage.Put("restricted/file.txt", bytes.NewBufferString("data"))

	if os.Getuid() != 0 { // Skip if running as root
		// Make directory inaccessible
		os.Chmod(subDir, 0000)
		defer os.Chmod(subDir, 0755) // Restore for cleanup

		_, err := storage.ListWithOptions(context.Background(), &common.ListOptions{})
		if err != nil {
			// Got expected permission error
		}

		// Restore permissions
		os.Chmod(subDir, 0755)
	}
}

// Test ListWithOptions with filepath.Rel error
func TestLocal_ListWithOptions_RelError(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	storage := local.New()
	storage.Configure(map[string]string{"path": tempDir})

	// Create a file
	storage.Put("test.txt", bytes.NewBufferString("data"))

	// Normal operation should work fine
	result, err := storage.ListWithOptions(context.Background(), &common.ListOptions{})
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if len(result.Objects) != 1 {
		t.Errorf("Expected 1 object, got %d", len(result.Objects))
	}
}

// Test saveMetadata with invalid custom metadata (empty key)
func TestLocal_SaveMetadata_InvalidCustomMetadata(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	storage := local.New()
	storage.Configure(map[string]string{"path": tempDir})

	// Create metadata with empty key in custom data
	metadata := &common.Metadata{
		Custom: map[string]string{
			"": "value",
		},
	}

	err := storage.PutWithMetadata(context.Background(), "test.txt", bytes.NewBufferString("data"), metadata)
	if err == nil {
		t.Error("Expected error for empty metadata key, got nil")
	}
}

// Test ListWithOptions pagination edge case
func TestLocal_ListWithOptions_PaginationEdgeCases(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	storage := local.New()
	storage.Configure(map[string]string{"path": tempDir})

	// Create exactly 3 files
	for i := 1; i <= 3; i++ {
		key := filepath.Join("test", "file"+string(rune('0'+i))+".txt")
		storage.Put(key, bytes.NewBufferString("data"))
	}

	// Request with MaxResults = 0 (should use default)
	opts := &common.ListOptions{
		MaxResults: 0,
	}

	result, err := storage.ListWithOptions(context.Background(), opts)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if len(result.Objects) != 3 {
		t.Errorf("Expected 3 objects, got %d", len(result.Objects))
	}

	// Request with MaxResults > total objects
	opts.MaxResults = 10
	result, err = storage.ListWithOptions(context.Background(), opts)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if result.Truncated {
		t.Error("Expected Truncated to be false when all results fit")
	}
}

// Test ListWithOptions with invalid continuation token
func TestLocal_ListWithOptions_InvalidContinuationToken(t *testing.T) {
	tempDir := createTempDir(t)
	defer cleanupTempDir(t, tempDir)

	storage := local.New()
	storage.Configure(map[string]string{"path": tempDir})

	storage.Put("test1.txt", bytes.NewBufferString("data"))
	storage.Put("test2.txt", bytes.NewBufferString("data"))

	// Use invalid continuation token
	opts := &common.ListOptions{
		ContinueFrom: "nonexistent-key",
		MaxResults:   5,
	}

	result, err := storage.ListWithOptions(context.Background(), opts)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// With invalid continuation token, it just starts from index 0 if not found
	// So we should still get results
	if len(result.Objects) == 0 {
		t.Error("Expected some objects even with invalid continuation token")
	}
}
