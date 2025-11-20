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

package common

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
)

// Test error variables
var (
	errTestInvalidEncryptedData = errors.New("invalid encrypted data")
	errTestEncrypterNotFound    = errors.New("encrypter not found")
	errTestNotFound             = errors.New("not found")
)

// mockEncrypter implements Encrypter for testing
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
		return nil, errTestInvalidEncryptedData
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

// mockEncrypterFactory implements EncrypterFactory for testing
type mockEncrypterFactory struct {
	defaultKeyID string
	encrypters   map[string]Encrypter
}

func (m *mockEncrypterFactory) DefaultKeyID() string {
	return m.defaultKeyID
}

func (m *mockEncrypterFactory) GetEncrypter(keyID string) (Encrypter, error) {
	if enc, ok := m.encrypters[keyID]; ok {
		return enc, nil
	}
	return nil, errTestEncrypterNotFound
}

func (m *mockEncrypterFactory) Close() error {
	return nil
}

// mockStorage implements Storage for testing encrypted storage
type mockUnderlyingStorage struct {
	data     map[string][]byte
	metadata map[string]*Metadata
}

func newMockUnderlyingStorage() *mockUnderlyingStorage {
	return &mockUnderlyingStorage{
		data:     make(map[string][]byte),
		metadata: make(map[string]*Metadata),
	}
}

func (m *mockUnderlyingStorage) Configure(settings map[string]string) error {
	return nil
}

func (m *mockUnderlyingStorage) Put(key string, data io.Reader) error {
	return m.PutWithContext(context.Background(), key, data)
}

func (m *mockUnderlyingStorage) PutWithContext(ctx context.Context, key string, data io.Reader) error {
	content, err := io.ReadAll(data)
	if err != nil {
		return err
	}
	m.data[key] = content
	m.metadata[key] = &Metadata{Size: int64(len(content))}
	return nil
}

func (m *mockUnderlyingStorage) PutWithMetadata(ctx context.Context, key string, data io.Reader, metadata *Metadata) error {
	content, err := io.ReadAll(data)
	if err != nil {
		return err
	}
	m.data[key] = content
	m.metadata[key] = metadata
	return nil
}

func (m *mockUnderlyingStorage) Get(key string) (io.ReadCloser, error) {
	return m.GetWithContext(context.Background(), key)
}

func (m *mockUnderlyingStorage) GetWithContext(ctx context.Context, key string) (io.ReadCloser, error) {
	data, ok := m.data[key]
	if !ok {
		return nil, errTestNotFound
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (m *mockUnderlyingStorage) GetMetadata(ctx context.Context, key string) (*Metadata, error) {
	metadata, ok := m.metadata[key]
	if !ok {
		return nil, errTestNotFound
	}
	return metadata, nil
}

func (m *mockUnderlyingStorage) UpdateMetadata(ctx context.Context, key string, metadata *Metadata) error {
	if _, ok := m.data[key]; !ok {
		return errTestNotFound
	}
	m.metadata[key] = metadata
	return nil
}

func (m *mockUnderlyingStorage) Delete(key string) error {
	return m.DeleteWithContext(context.Background(), key)
}

func (m *mockUnderlyingStorage) DeleteWithContext(ctx context.Context, key string) error {
	delete(m.data, key)
	delete(m.metadata, key)
	return nil
}

func (m *mockUnderlyingStorage) Exists(ctx context.Context, key string) (bool, error) {
	_, ok := m.data[key]
	return ok, nil
}

func (m *mockUnderlyingStorage) List(prefix string) ([]string, error) {
	return m.ListWithContext(context.Background(), prefix)
}

func (m *mockUnderlyingStorage) ListWithContext(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	for key := range m.data {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	return keys, nil
}

func (m *mockUnderlyingStorage) ListWithOptions(ctx context.Context, opts *ListOptions) (*ListResult, error) {
	var objects []*ObjectInfo
	for key := range m.data {
		if opts.Prefix == "" || strings.HasPrefix(key, opts.Prefix) {
			objects = append(objects, &ObjectInfo{
				Key:      key,
				Metadata: m.metadata[key],
			})
		}
	}
	return &ListResult{Objects: objects}, nil
}

func (m *mockUnderlyingStorage) Archive(key string, destination Archiver) error {
	return nil
}

func (m *mockUnderlyingStorage) AddPolicy(policy LifecyclePolicy) error {
	return nil
}

func (m *mockUnderlyingStorage) RemovePolicy(id string) error {
	return nil
}

func (m *mockUnderlyingStorage) GetPolicies() ([]LifecyclePolicy, error) {
	return []LifecyclePolicy{}, nil
}

// Tests

func TestNewEncryptedStorage(t *testing.T) {
	underlying := newMockUnderlyingStorage()
	factory := &mockEncrypterFactory{
		defaultKeyID: "key1",
		encrypters: map[string]Encrypter{
			"key1": &mockEncrypter{keyID: "key1", algorithm: "AES256"},
		},
	}

	storage := NewEncryptedStorage(underlying, factory)
	if storage == nil {
		t.Fatal("Expected encrypted storage, got nil")
	}
}

func TestEncryptedStorage_Configure(t *testing.T) {
	underlying := newMockUnderlyingStorage()
	factory := &mockEncrypterFactory{
		defaultKeyID: "key1",
		encrypters: map[string]Encrypter{
			"key1": &mockEncrypter{keyID: "key1", algorithm: "AES256"},
		},
	}

	storage := NewEncryptedStorage(underlying, factory)
	err := storage.Configure(map[string]string{"test": "value"})
	if err != nil {
		t.Errorf("Configure failed: %v", err)
	}
}

func TestEncryptedStorage_PutAndGet(t *testing.T) {
	underlying := newMockUnderlyingStorage()
	factory := &mockEncrypterFactory{
		defaultKeyID: "key1",
		encrypters: map[string]Encrypter{
			"key1": &mockEncrypter{keyID: "key1", algorithm: "AES256"},
		},
	}

	storage := NewEncryptedStorage(underlying, factory)

	// Put encrypted data
	testData := "hello world"
	err := storage.Put("test.txt", strings.NewReader(testData))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Verify underlying storage has encrypted data
	underlyingData := underlying.data["test.txt"]
	if !bytes.HasPrefix(underlyingData, []byte("ENCRYPTED:")) {
		t.Error("Data in underlying storage should be encrypted")
	}

	// Get decrypted data
	reader, err := storage.Get("test.txt")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	defer func() { _ = reader.Close() }()

	decrypted, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if string(decrypted) != testData {
		t.Errorf("Decrypted data = %s, want %s", string(decrypted), testData)
	}
}

func TestEncryptedStorage_PutWithContext(t *testing.T) {
	underlying := newMockUnderlyingStorage()
	factory := &mockEncrypterFactory{
		defaultKeyID: "key1",
		encrypters: map[string]Encrypter{
			"key1": &mockEncrypter{keyID: "key1", algorithm: "AES256"},
		},
	}

	storage := NewEncryptedStorage(underlying, factory)

	testData := "context test"
	err := storage.PutWithContext(context.Background(), "test.txt", strings.NewReader(testData))
	if err != nil {
		t.Fatalf("PutWithContext failed: %v", err)
	}

	// Verify data was encrypted
	if !bytes.HasPrefix(underlying.data["test.txt"], []byte("ENCRYPTED:")) {
		t.Error("Data should be encrypted")
	}
}

func TestEncryptedStorage_PutWithMetadata(t *testing.T) {
	underlying := newMockUnderlyingStorage()
	factory := &mockEncrypterFactory{
		defaultKeyID: "key1",
		encrypters: map[string]Encrypter{
			"key1": &mockEncrypter{keyID: "key1", algorithm: "AES256"},
		},
	}

	storage := NewEncryptedStorage(underlying, factory)

	metadata := &Metadata{
		ContentType: "text/plain",
		Custom:      map[string]string{"user": "test"},
	}

	testData := "metadata test"
	err := storage.PutWithMetadata(context.Background(), "test.txt", strings.NewReader(testData), metadata)
	if err != nil {
		t.Fatalf("PutWithMetadata failed: %v", err)
	}

	// Verify encryption metadata was added
	storedMetadata := underlying.metadata["test.txt"]
	if storedMetadata.Custom["encryption_algorithm"] != "AES256" {
		t.Error("Encryption algorithm not set in metadata")
	}
	if storedMetadata.Custom["encryption_key_id"] != "key1" {
		t.Error("Encryption key ID not set in metadata")
	}
	// Verify original custom metadata preserved
	if storedMetadata.Custom["user"] != "test" {
		t.Error("Original custom metadata not preserved")
	}
}

func TestEncryptedStorage_GetWithContext(t *testing.T) {
	underlying := newMockUnderlyingStorage()
	factory := &mockEncrypterFactory{
		defaultKeyID: "key1",
		encrypters: map[string]Encrypter{
			"key1": &mockEncrypter{keyID: "key1", algorithm: "AES256"},
		},
	}

	storage := NewEncryptedStorage(underlying, factory)

	// Put data
	testData := "context get test"
	_ = storage.PutWithContext(context.Background(), "test.txt", strings.NewReader(testData))

	// Get with context
	reader, err := storage.GetWithContext(context.Background(), "test.txt")
	if err != nil {
		t.Fatalf("GetWithContext failed: %v", err)
	}
	defer func() { _ = reader.Close() }()

	decrypted, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if string(decrypted) != testData {
		t.Errorf("Decrypted data = %s, want %s", string(decrypted), testData)
	}
}

func TestEncryptedStorage_GetMetadata(t *testing.T) {
	underlying := newMockUnderlyingStorage()
	factory := &mockEncrypterFactory{
		defaultKeyID: "key1",
		encrypters: map[string]Encrypter{
			"key1": &mockEncrypter{keyID: "key1", algorithm: "AES256"},
		},
	}

	storage := NewEncryptedStorage(underlying, factory)

	// Put with metadata
	metadata := &Metadata{ContentType: "text/plain"}
	_ = storage.PutWithMetadata(context.Background(), "test.txt", strings.NewReader("test"), metadata)

	// Get metadata
	retrieved, err := storage.GetMetadata(context.Background(), "test.txt")
	if err != nil {
		t.Fatalf("GetMetadata failed: %v", err)
	}

	if retrieved.ContentType != "text/plain" {
		t.Errorf("ContentType = %s, want text/plain", retrieved.ContentType)
	}
}

func TestEncryptedStorage_UpdateMetadata(t *testing.T) {
	underlying := newMockUnderlyingStorage()
	factory := &mockEncrypterFactory{
		defaultKeyID: "key1",
		encrypters: map[string]Encrypter{
			"key1": &mockEncrypter{keyID: "key1", algorithm: "AES256"},
		},
	}

	storage := NewEncryptedStorage(underlying, factory)

	// Put data
	_ = storage.Put("test.txt", strings.NewReader("test"))

	// Update metadata
	newMetadata := &Metadata{ContentType: "application/json"}
	err := storage.UpdateMetadata(context.Background(), "test.txt", newMetadata)
	if err != nil {
		t.Fatalf("UpdateMetadata failed: %v", err)
	}

	// Verify update
	retrieved, _ := storage.GetMetadata(context.Background(), "test.txt")
	if retrieved.ContentType != "application/json" {
		t.Error("Metadata not updated")
	}
}

func TestEncryptedStorage_Delete(t *testing.T) {
	underlying := newMockUnderlyingStorage()
	factory := &mockEncrypterFactory{
		defaultKeyID: "key1",
		encrypters: map[string]Encrypter{
			"key1": &mockEncrypter{keyID: "key1", algorithm: "AES256"},
		},
	}

	storage := NewEncryptedStorage(underlying, factory)

	// Put data
	_ = storage.Put("test.txt", strings.NewReader("test"))

	// Delete
	err := storage.Delete("test.txt")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify deletion
	_, err = storage.Get("test.txt")
	if err == nil {
		t.Error("Object should be deleted")
	}
}

func TestEncryptedStorage_DeleteWithContext(t *testing.T) {
	underlying := newMockUnderlyingStorage()
	factory := &mockEncrypterFactory{
		defaultKeyID: "key1",
		encrypters: map[string]Encrypter{
			"key1": &mockEncrypter{keyID: "key1", algorithm: "AES256"},
		},
	}

	storage := NewEncryptedStorage(underlying, factory)

	// Put data
	_ = storage.Put("test.txt", strings.NewReader("test"))

	// Delete with context
	err := storage.DeleteWithContext(context.Background(), "test.txt")
	if err != nil {
		t.Fatalf("DeleteWithContext failed: %v", err)
	}
}

func TestEncryptedStorage_Exists(t *testing.T) {
	underlying := newMockUnderlyingStorage()
	factory := &mockEncrypterFactory{
		defaultKeyID: "key1",
		encrypters: map[string]Encrypter{
			"key1": &mockEncrypter{keyID: "key1", algorithm: "AES256"},
		},
	}

	storage := NewEncryptedStorage(underlying, factory)

	// Check non-existent
	exists, err := storage.Exists(context.Background(), "test.txt")
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if exists {
		t.Error("Object should not exist")
	}

	// Put data
	_ = storage.Put("test.txt", strings.NewReader("test"))

	// Check exists
	exists, err = storage.Exists(context.Background(), "test.txt")
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if !exists {
		t.Error("Object should exist")
	}
}

func TestEncryptedStorage_List(t *testing.T) {
	underlying := newMockUnderlyingStorage()
	factory := &mockEncrypterFactory{
		defaultKeyID: "key1",
		encrypters: map[string]Encrypter{
			"key1": &mockEncrypter{keyID: "key1", algorithm: "AES256"},
		},
	}

	storage := NewEncryptedStorage(underlying, factory)

	// Put some objects
	storage.Put("logs/app.log", strings.NewReader("log1"))
	storage.Put("logs/error.log", strings.NewReader("log2"))
	storage.Put("data/file.txt", strings.NewReader("data"))

	// List with prefix
	keys, err := storage.List("logs/")
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}

	if len(keys) != 2 {
		t.Errorf("Expected 2 keys with logs/ prefix, got %d", len(keys))
	}
}

func TestEncryptedStorage_ListWithContext(t *testing.T) {
	underlying := newMockUnderlyingStorage()
	factory := &mockEncrypterFactory{
		defaultKeyID: "key1",
		encrypters: map[string]Encrypter{
			"key1": &mockEncrypter{keyID: "key1", algorithm: "AES256"},
		},
	}

	storage := NewEncryptedStorage(underlying, factory)

	// Put objects
	storage.Put("file1.txt", strings.NewReader("data1"))
	storage.Put("file2.txt", strings.NewReader("data2"))

	// List with context
	keys, err := storage.ListWithContext(context.Background(), "")
	if err != nil {
		t.Fatalf("ListWithContext failed: %v", err)
	}

	if len(keys) != 2 {
		t.Errorf("Expected 2 keys, got %d", len(keys))
	}
}

func TestEncryptedStorage_ListWithOptions(t *testing.T) {
	underlying := newMockUnderlyingStorage()
	factory := &mockEncrypterFactory{
		defaultKeyID: "key1",
		encrypters: map[string]Encrypter{
			"key1": &mockEncrypter{keyID: "key1", algorithm: "AES256"},
		},
	}

	storage := NewEncryptedStorage(underlying, factory)

	// Put objects
	storage.Put("logs/app.log", strings.NewReader("log1"))
	storage.Put("data/file.txt", strings.NewReader("data"))

	// List with options
	result, err := storage.ListWithOptions(context.Background(), &ListOptions{Prefix: "logs/"})
	if err != nil {
		t.Fatalf("ListWithOptions failed: %v", err)
	}

	if len(result.Objects) != 1 {
		t.Errorf("Expected 1 object with logs/ prefix, got %d", len(result.Objects))
	}
}

func TestEncryptedStorage_Archive(t *testing.T) {
	underlying := newMockUnderlyingStorage()
	factory := &mockEncrypterFactory{
		defaultKeyID: "key1",
		encrypters: map[string]Encrypter{
			"key1": &mockEncrypter{keyID: "key1", algorithm: "AES256"},
		},
	}

	storage := NewEncryptedStorage(underlying, factory)

	// Archive (just pass through)
	err := storage.Archive("test.txt", nil)
	if err != nil {
		t.Errorf("Archive failed: %v", err)
	}
}

func TestEncryptedStorage_LifecyclePolicies(t *testing.T) {
	underlying := newMockUnderlyingStorage()
	factory := &mockEncrypterFactory{
		defaultKeyID: "key1",
		encrypters: map[string]Encrypter{
			"key1": &mockEncrypter{keyID: "key1", algorithm: "AES256"},
		},
	}

	storage := NewEncryptedStorage(underlying, factory)

	// Add policy
	policy := LifecyclePolicy{ID: "test"}
	err := storage.AddPolicy(policy)
	if err != nil {
		t.Errorf("AddPolicy failed: %v", err)
	}

	// Get policies
	policies, err := storage.GetPolicies()
	if err != nil {
		t.Errorf("GetPolicies failed: %v", err)
	}
	if policies == nil {
		t.Error("Expected policies slice")
	}

	// Remove policy
	err = storage.RemovePolicy("test")
	if err != nil {
		t.Errorf("RemovePolicy failed: %v", err)
	}
}
