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


package local

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// mockEncrypter implements common.Encrypter for testing
type mockEncrypter struct {
	keyID     string
	algorithm string
}

func (m *mockEncrypter) Encrypt(ctx context.Context, data io.Reader) (io.ReadCloser, error) {
	// Simple encryption: just add a prefix
	content, err := io.ReadAll(data)
	if err != nil {
		return nil, err
	}
	encrypted := append([]byte("ENCRYPTED:"), content...)
	return io.NopCloser(bytes.NewReader(encrypted)), nil
}

func (m *mockEncrypter) Decrypt(ctx context.Context, data io.Reader) (io.ReadCloser, error) {
	// Simple decryption: remove the prefix
	content, err := io.ReadAll(data)
	if err != nil {
		return nil, err
	}
	prefix := []byte("ENCRYPTED:")
	if !bytes.HasPrefix(content, prefix) {
		return nil, errors.New("invalid encrypted data")
	}
	decrypted := bytes.TrimPrefix(content, prefix)
	return io.NopCloser(bytes.NewReader(decrypted)), nil
}

func (m *mockEncrypter) Algorithm() string {
	return m.algorithm
}

func (m *mockEncrypter) KeyID() string {
	return m.keyID
}

// mockEncrypterFactory implements common.EncrypterFactory for testing
type mockEncrypterFactory struct {
	defaultKeyID string
	encrypters   map[string]common.Encrypter
}

func (m *mockEncrypterFactory) DefaultKeyID() string {
	return m.defaultKeyID
}

func (m *mockEncrypterFactory) GetEncrypter(keyID string) (common.Encrypter, error) {
	if keyID == "" {
		keyID = m.defaultKeyID
	}
	if enc, ok := m.encrypters[keyID]; ok {
		return enc, nil
	}
	return nil, errors.New("encrypter not found")
}

func (m *mockEncrypterFactory) Close() error {
	return nil
}

// TestLocalAtRestEncryption tests that data is encrypted at rest and decrypted on read
func TestLocalAtRestEncryption(t *testing.T) {
	tmpDir := t.TempDir()
	storage := New().(*Local)
	err := storage.Configure(map[string]string{"path": tmpDir})
	if err != nil {
		t.Fatalf("failed to configure storage: %v", err)
	}

	// Set up encryption factory
	factory := &mockEncrypterFactory{
		defaultKeyID: "key1",
		encrypters: map[string]common.Encrypter{
			"key1": &mockEncrypter{keyID: "key1", algorithm: "AES256-TEST"},
		},
	}
	storage.SetAtRestEncrypterFactory(factory)

	ctx := context.Background()
	testData := []byte("sensitive data")
	key := "test/encrypted-file.txt"

	// Put data (should be encrypted on disk)
	err = storage.Put(key, bytes.NewReader(testData))
	if err != nil {
		t.Fatalf("failed to put data: %v", err)
	}

	// Verify data is encrypted on disk
	diskPath := tmpDir + "/" + key
	diskData, err := os.ReadFile(diskPath)
	if err != nil {
		t.Fatalf("failed to read file from disk: %v", err)
	}
	if !bytes.HasPrefix(diskData, []byte("ENCRYPTED:")) {
		t.Error("data on disk should be encrypted")
	}
	if bytes.Equal(diskData, testData) {
		t.Error("plaintext data should not be stored on disk")
	}

	// Get data (should be decrypted)
	reader, err := storage.GetWithContext(ctx, key)
	if err != nil {
		t.Fatalf("failed to get data: %v", err)
	}
	defer reader.Close()

	retrievedData, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("failed to read decrypted data: %v", err)
	}

	if !bytes.Equal(retrievedData, testData) {
		t.Errorf("decrypted data = %s, want %s", string(retrievedData), string(testData))
	}

	// Verify encryption metadata was stored
	metadata, err := storage.GetMetadata(ctx, key)
	if err != nil {
		t.Fatalf("failed to get metadata: %v", err)
	}

	if metadata.Custom["encryption_algorithm"] != "AES256-TEST" {
		t.Errorf("encryption_algorithm = %s, want AES256-TEST",
			metadata.Custom["encryption_algorithm"])
	}
	if metadata.Custom["encryption_key_id"] != "key1" {
		t.Errorf("encryption_key_id = %s, want key1",
			metadata.Custom["encryption_key_id"])
	}
}

// TestLocalWithoutEncryption tests that without encryption factory, data is stored in plaintext
func TestLocalWithoutEncryption(t *testing.T) {
	tmpDir := t.TempDir()
	storage := New().(*Local)
	err := storage.Configure(map[string]string{"path": tmpDir})
	if err != nil {
		t.Fatalf("failed to configure storage: %v", err)
	}

	// No encryption factory set (noop behavior)

	ctx := context.Background()
	testData := []byte("plaintext data")
	key := "test/plaintext-file.txt"

	// Put data
	err = storage.Put(key, bytes.NewReader(testData))
	if err != nil {
		t.Fatalf("failed to put data: %v", err)
	}

	// Verify data is NOT encrypted on disk
	diskPath := tmpDir + "/" + key
	diskData, err := os.ReadFile(diskPath)
	if err != nil {
		t.Fatalf("failed to read file from disk: %v", err)
	}
	if !bytes.Equal(diskData, testData) {
		t.Error("data on disk should be plaintext when no encryption is set")
	}

	// Get data
	reader, err := storage.GetWithContext(ctx, key)
	if err != nil {
		t.Fatalf("failed to get data: %v", err)
	}
	defer reader.Close()

	retrievedData, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("failed to read data: %v", err)
	}

	if !bytes.Equal(retrievedData, testData) {
		t.Errorf("retrieved data = %s, want %s", string(retrievedData), string(testData))
	}

	// Verify no encryption metadata
	metadata, err := storage.GetMetadata(ctx, key)
	if err != nil {
		t.Fatalf("failed to get metadata: %v", err)
	}

	if _, exists := metadata.Custom["encryption_algorithm"]; exists {
		t.Error("encryption_algorithm should not be present without encryption")
	}
	if _, exists := metadata.Custom["encryption_key_id"]; exists {
		t.Error("encryption_key_id should not be present without encryption")
	}
}

// TestLocalEncryptionRoundtrip tests multiple put/get cycles with encryption
func TestLocalEncryptionRoundtrip(t *testing.T) {
	tmpDir := t.TempDir()
	storage := New().(*Local)
	err := storage.Configure(map[string]string{"path": tmpDir})
	if err != nil {
		t.Fatalf("failed to configure storage: %v", err)
	}

	// Set up encryption factory
	factory := &mockEncrypterFactory{
		defaultKeyID: "key1",
		encrypters: map[string]common.Encrypter{
			"key1": &mockEncrypter{keyID: "key1", algorithm: "AES256-TEST"},
		},
	}
	storage.SetAtRestEncrypterFactory(factory)

	ctx := context.Background()

	testCases := []struct {
		key  string
		data string
	}{
		{"test/file1.txt", "first file content"},
		{"test/file2.txt", "second file content"},
		{"test/nested/file3.txt", "nested file content"},
		{"test/special-chars-@#$.txt", "special characters in filename"},
	}

	// Put all files
	for _, tc := range testCases {
		err := storage.Put(tc.key, strings.NewReader(tc.data))
		if err != nil {
			t.Errorf("failed to put %s: %v", tc.key, err)
		}
	}

	// Get and verify all files
	for _, tc := range testCases {
		reader, err := storage.GetWithContext(ctx, tc.key)
		if err != nil {
			t.Errorf("failed to get %s: %v", tc.key, err)
			continue
		}

		retrievedData, err := io.ReadAll(reader)
		reader.Close()
		if err != nil {
			t.Errorf("failed to read %s: %v", tc.key, err)
			continue
		}

		if string(retrievedData) != tc.data {
			t.Errorf("data mismatch for %s: got %s, want %s",
				tc.key, string(retrievedData), tc.data)
		}
	}
}

// TestSetAtRestEncrypterFactory tests the SetAtRestEncrypterFactory method
func TestSetAtRestEncrypterFactory(t *testing.T) {
	tmpDir := t.TempDir()
	storage := New().(*Local)
	err := storage.Configure(map[string]string{"path": tmpDir})
	if err != nil {
		t.Fatalf("failed to configure storage: %v", err)
	}

	// Initially no factory
	if storage.atRestEncrypterFactory != nil {
		t.Error("atRestEncrypterFactory should initially be nil")
	}

	// Set factory
	factory := &mockEncrypterFactory{
		defaultKeyID: "key1",
		encrypters: map[string]common.Encrypter{
			"key1": &mockEncrypter{keyID: "key1", algorithm: "AES256-TEST"},
		},
	}
	storage.SetAtRestEncrypterFactory(factory)

	// Verify factory is set
	if storage.atRestEncrypterFactory == nil {
		t.Error("atRestEncrypterFactory should be set")
	}

	// Verify encryption works after setting factory
	testData := []byte("test data")
	key := "test/file.txt"

	err = storage.Put(key, bytes.NewReader(testData))
	if err != nil {
		t.Fatalf("failed to put data: %v", err)
	}

	// Verify data is encrypted on disk
	diskPath := tmpDir + "/" + key
	diskData, err := os.ReadFile(diskPath)
	if err != nil {
		t.Fatalf("failed to read file from disk: %v", err)
	}
	if !bytes.HasPrefix(diskData, []byte("ENCRYPTED:")) {
		t.Error("data should be encrypted after setting factory")
	}
}

// TestLocalEncryptionWithMetadata tests encryption with custom metadata
func TestLocalEncryptionWithMetadata(t *testing.T) {
	tmpDir := t.TempDir()
	storage := New().(*Local)
	err := storage.Configure(map[string]string{"path": tmpDir})
	if err != nil {
		t.Fatalf("failed to configure storage: %v", err)
	}

	// Set up encryption factory
	factory := &mockEncrypterFactory{
		defaultKeyID: "key1",
		encrypters: map[string]common.Encrypter{
			"key1": &mockEncrypter{keyID: "key1", algorithm: "AES256-TEST"},
		},
	}
	storage.SetAtRestEncrypterFactory(factory)

	ctx := context.Background()
	testData := []byte("data with metadata")
	key := "test/metadata-file.txt"

	// Custom metadata
	customMetadata := &common.Metadata{
		ContentType:     "text/plain",
		ContentEncoding: "utf-8",
		Custom: map[string]string{
			"author":  "test-user",
			"version": "1.0",
		},
	}

	// Put with metadata
	err = storage.PutWithMetadata(ctx, key, bytes.NewReader(testData), customMetadata)
	if err != nil {
		t.Fatalf("failed to put with metadata: %v", err)
	}

	// Get metadata
	metadata, err := storage.GetMetadata(ctx, key)
	if err != nil {
		t.Fatalf("failed to get metadata: %v", err)
	}

	// Verify custom metadata preserved
	if metadata.ContentType != "text/plain" {
		t.Errorf("ContentType = %s, want text/plain", metadata.ContentType)
	}
	if metadata.Custom["author"] != "test-user" {
		t.Errorf("author = %s, want test-user", metadata.Custom["author"])
	}
	if metadata.Custom["version"] != "1.0" {
		t.Errorf("version = %s, want 1.0", metadata.Custom["version"])
	}

	// Verify encryption metadata added
	if metadata.Custom["encryption_algorithm"] != "AES256-TEST" {
		t.Errorf("encryption_algorithm = %s, want AES256-TEST",
			metadata.Custom["encryption_algorithm"])
	}
	if metadata.Custom["encryption_key_id"] != "key1" {
		t.Errorf("encryption_key_id = %s, want key1",
			metadata.Custom["encryption_key_id"])
	}

	// Verify data can be decrypted
	reader, err := storage.GetWithContext(ctx, key)
	if err != nil {
		t.Fatalf("failed to get data: %v", err)
	}
	defer reader.Close()

	retrievedData, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("failed to read data: %v", err)
	}

	if !bytes.Equal(retrievedData, testData) {
		t.Errorf("retrieved data = %s, want %s", string(retrievedData), string(testData))
	}
}

// TestLocalEncryptionErrors tests error handling in encryption scenarios
func TestLocalEncryptionErrors(t *testing.T) {
	t.Run("encrypter factory returns error", func(t *testing.T) {
		tmpDir := t.TempDir()
		storage := New().(*Local)
		err := storage.Configure(map[string]string{"path": tmpDir})
		if err != nil {
			t.Fatalf("failed to configure storage: %v", err)
		}

		// Factory that returns error
		factory := &mockEncrypterFactory{
			defaultKeyID: "missing-key",
			encrypters:   map[string]common.Encrypter{}, // No encrypters available
		}
		storage.SetAtRestEncrypterFactory(factory)

		ctx := context.Background()
		testData := []byte("test data")
		key := "test/file.txt"

		// Put should fail
		err = storage.PutWithContext(ctx, key, bytes.NewReader(testData))
		if err == nil {
			t.Error("expected error when encrypter factory returns error")
		}
		if !strings.Contains(err.Error(), "failed to get encrypter") {
			t.Errorf("unexpected error message: %v", err)
		}
	})

	t.Run("decryption fails for corrupted data", func(t *testing.T) {
		tmpDir := t.TempDir()
		storage := New().(*Local)
		err := storage.Configure(map[string]string{"path": tmpDir})
		if err != nil {
			t.Fatalf("failed to configure storage: %v", err)
		}

		factory := &mockEncrypterFactory{
			defaultKeyID: "key1",
			encrypters: map[string]common.Encrypter{
				"key1": &mockEncrypter{keyID: "key1", algorithm: "AES256-TEST"},
			},
		}
		storage.SetAtRestEncrypterFactory(factory)

		// Put valid encrypted data first
		key := "test/file.txt"
		testData := []byte("test data")
		err = storage.Put(key, bytes.NewReader(testData))
		if err != nil {
			t.Fatalf("failed to put data: %v", err)
		}

		// Corrupt the data on disk (remove encryption prefix)
		diskPath := tmpDir + "/" + key
		corruptedData := []byte("CORRUPTED DATA")
		err = os.WriteFile(diskPath, corruptedData, 0600)
		if err != nil {
			t.Fatalf("failed to corrupt data: %v", err)
		}

		// Get should fail during decryption
		ctx := context.Background()
		_, err = storage.GetWithContext(ctx, key)
		if err == nil {
			t.Error("expected error when decrypting corrupted data")
		}
		if !strings.Contains(err.Error(), "decryption failed") {
			t.Errorf("unexpected error message: %v", err)
		}
	})
}

// TestLocalEncryptionWithDelete tests that encrypted files can be deleted
func TestLocalEncryptionWithDelete(t *testing.T) {
	tmpDir := t.TempDir()
	storage := New().(*Local)
	err := storage.Configure(map[string]string{"path": tmpDir})
	if err != nil {
		t.Fatalf("failed to configure storage: %v", err)
	}

	// Set up encryption factory
	factory := &mockEncrypterFactory{
		defaultKeyID: "key1",
		encrypters: map[string]common.Encrypter{
			"key1": &mockEncrypter{keyID: "key1", algorithm: "AES256-TEST"},
		},
	}
	storage.SetAtRestEncrypterFactory(factory)

	ctx := context.Background()
	testData := []byte("data to delete")
	key := "test/delete-file.txt"

	// Put encrypted data
	err = storage.Put(key, bytes.NewReader(testData))
	if err != nil {
		t.Fatalf("failed to put data: %v", err)
	}

	// Verify file exists
	exists, err := storage.Exists(ctx, key)
	if err != nil {
		t.Fatalf("failed to check existence: %v", err)
	}
	if !exists {
		t.Error("file should exist after put")
	}

	// Delete
	err = storage.DeleteWithContext(ctx, key)
	if err != nil {
		t.Fatalf("failed to delete: %v", err)
	}

	// Verify file no longer exists
	exists, err = storage.Exists(ctx, key)
	if err != nil {
		t.Fatalf("failed to check existence: %v", err)
	}
	if exists {
		t.Error("file should not exist after delete")
	}

	// Verify encrypted data file is gone
	diskPath := tmpDir + "/" + key
	if _, err := os.Stat(diskPath); !os.IsNotExist(err) {
		t.Error("encrypted data file should be deleted")
	}

	// Verify metadata file is gone
	metadataPath := diskPath + metadataSuffix
	if _, err := os.Stat(metadataPath); !os.IsNotExist(err) {
		t.Error("metadata file should be deleted")
	}
}

// TestLocalEncryptionWithList tests that encrypted files are listed correctly
func TestLocalEncryptionWithList(t *testing.T) {
	tmpDir := t.TempDir()
	storage := New().(*Local)
	err := storage.Configure(map[string]string{"path": tmpDir})
	if err != nil {
		t.Fatalf("failed to configure storage: %v", err)
	}

	// Set up encryption factory
	factory := &mockEncrypterFactory{
		defaultKeyID: "key1",
		encrypters: map[string]common.Encrypter{
			"key1": &mockEncrypter{keyID: "key1", algorithm: "AES256-TEST"},
		},
	}
	storage.SetAtRestEncrypterFactory(factory)

	ctx := context.Background()

	// Put multiple encrypted files
	files := []string{
		"test/file1.txt",
		"test/file2.txt",
		"test/nested/file3.txt",
	}

	for _, key := range files {
		err := storage.Put(key, strings.NewReader("data for "+key))
		if err != nil {
			t.Fatalf("failed to put %s: %v", key, err)
		}
	}

	// List with prefix
	keys, err := storage.ListWithContext(ctx, "test/")
	if err != nil {
		t.Fatalf("failed to list: %v", err)
	}

	if len(keys) != 3 {
		t.Errorf("expected 3 keys, got %d", len(keys))
	}

	// Verify all files are in the list
	keyMap := make(map[string]bool)
	for _, key := range keys {
		keyMap[key] = true
	}

	for _, expectedKey := range files {
		if !keyMap[expectedKey] {
			t.Errorf("expected key %s not found in list", expectedKey)
		}
	}
}

// TestLocalEncryptionWithListOptions tests ListWithOptions with encrypted files
func TestLocalEncryptionWithListOptions(t *testing.T) {
	tmpDir := t.TempDir()
	storage := New().(*Local)
	err := storage.Configure(map[string]string{"path": tmpDir})
	if err != nil {
		t.Fatalf("failed to configure storage: %v", err)
	}

	// Set up encryption factory
	factory := &mockEncrypterFactory{
		defaultKeyID: "key1",
		encrypters: map[string]common.Encrypter{
			"key1": &mockEncrypter{keyID: "key1", algorithm: "AES256-TEST"},
		},
	}
	storage.SetAtRestEncrypterFactory(factory)

	ctx := context.Background()

	// Put multiple encrypted files
	files := []string{
		"test/file1.txt",
		"test/file2.txt",
		"test/file3.txt",
	}

	for _, key := range files {
		err := storage.Put(key, strings.NewReader("encrypted data for "+key))
		if err != nil {
			t.Fatalf("failed to put %s: %v", key, err)
		}
	}

	// List with options
	result, err := storage.ListWithOptions(ctx, &common.ListOptions{
		Prefix:     "test/",
		MaxResults: 10,
	})
	if err != nil {
		t.Fatalf("failed to list with options: %v", err)
	}

	if len(result.Objects) != 3 {
		t.Errorf("expected 3 objects, got %d", len(result.Objects))
	}

	// Verify metadata includes encryption info
	for _, obj := range result.Objects {
		if obj.Metadata == nil {
			t.Errorf("metadata should not be nil for %s", obj.Key)
			continue
		}

		if obj.Metadata.Custom["encryption_algorithm"] != "AES256-TEST" {
			t.Errorf("encryption_algorithm should be AES256-TEST for %s", obj.Key)
		}
		if obj.Metadata.Custom["encryption_key_id"] != "key1" {
			t.Errorf("encryption_key_id should be key1 for %s", obj.Key)
		}
	}
}

// TestLocalEncryptionContextCancellation tests context cancellation with encryption
func TestLocalEncryptionContextCancellation(t *testing.T) {
	tmpDir := t.TempDir()
	storage := New().(*Local)
	err := storage.Configure(map[string]string{"path": tmpDir})
	if err != nil {
		t.Fatalf("failed to configure storage: %v", err)
	}

	// Set up encryption factory
	factory := &mockEncrypterFactory{
		defaultKeyID: "key1",
		encrypters: map[string]common.Encrypter{
			"key1": &mockEncrypter{keyID: "key1", algorithm: "AES256-TEST"},
		},
	}
	storage.SetAtRestEncrypterFactory(factory)

	t.Run("cancelled context on put", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		err := storage.PutWithContext(ctx, "test/file.txt", strings.NewReader("data"))
		if err != context.Canceled {
			t.Errorf("expected context.Canceled, got %v", err)
		}
	})

	t.Run("cancelled context on get", func(t *testing.T) {
		// Put a file first
		err := storage.Put("test/file.txt", strings.NewReader("data"))
		if err != nil {
			t.Fatalf("failed to put: %v", err)
		}

		// Try to get with cancelled context
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err = storage.GetWithContext(ctx, "test/file.txt")
		if err != context.Canceled {
			t.Errorf("expected context.Canceled, got %v", err)
		}
	})
}

// TestLocalEncryptionLargeFile tests encryption with a larger file
func TestLocalEncryptionLargeFile(t *testing.T) {
	tmpDir := t.TempDir()
	storage := New().(*Local)
	err := storage.Configure(map[string]string{"path": tmpDir})
	if err != nil {
		t.Fatalf("failed to configure storage: %v", err)
	}

	// Set up encryption factory
	factory := &mockEncrypterFactory{
		defaultKeyID: "key1",
		encrypters: map[string]common.Encrypter{
			"key1": &mockEncrypter{keyID: "key1", algorithm: "AES256-TEST"},
		},
	}
	storage.SetAtRestEncrypterFactory(factory)

	ctx := context.Background()
	key := "test/large-file.bin"

	// Create 1MB of data
	largeData := make([]byte, 1024*1024)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	// Put large file
	err = storage.Put(key, bytes.NewReader(largeData))
	if err != nil {
		t.Fatalf("failed to put large file: %v", err)
	}

	// Get and verify
	reader, err := storage.GetWithContext(ctx, key)
	if err != nil {
		t.Fatalf("failed to get large file: %v", err)
	}
	defer reader.Close()

	retrievedData, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("failed to read large file: %v", err)
	}

	if !bytes.Equal(retrievedData, largeData) {
		t.Error("large file data mismatch after encryption/decryption")
	}

	// Verify size in metadata
	metadata, err := storage.GetMetadata(ctx, key)
	if err != nil {
		t.Fatalf("failed to get metadata: %v", err)
	}

	// Note: Size will be larger due to encryption prefix
	if metadata.Size < int64(len(largeData)) {
		t.Errorf("metadata size %d should be >= original size %d", metadata.Size, len(largeData))
	}
}
