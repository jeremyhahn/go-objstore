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
	"context"
	"io"
)

// encryptedStorage wraps any Storage implementation with transparent encryption.
// All data written is encrypted, and all data read is decrypted.
type encryptedStorage struct {
	underlying       Storage
	encrypterFactory EncrypterFactory
	defaultKeyID     string
}

// NewEncryptedStorage creates a new encrypted storage wrapper.
// The underlying storage backend will store encrypted data.
// The encrypterFactory provides encryption/decryption operations.
func NewEncryptedStorage(underlying Storage, encrypterFactory EncrypterFactory) Storage {
	return &encryptedStorage{
		underlying:       underlying,
		encrypterFactory: encrypterFactory,
		defaultKeyID:     encrypterFactory.DefaultKeyID(),
	}
}

// Configure passes through configuration to the underlying storage
func (e *encryptedStorage) Configure(settings map[string]string) error {
	return e.underlying.Configure(settings)
}

// Put encrypts data and stores it in the underlying storage
func (e *encryptedStorage) Put(key string, data io.Reader) error {
	return e.PutWithContext(context.Background(), key, data)
}

// PutWithContext encrypts data and stores it in the underlying storage with context support
func (e *encryptedStorage) PutWithContext(ctx context.Context, key string, data io.Reader) error {
	// Get encrypter using default key ID
	encrypter, err := e.encrypterFactory.GetEncrypter(e.defaultKeyID)
	if err != nil {
		return err
	}

	// Encrypt the data
	encryptedData, err := encrypter.Encrypt(ctx, data)
	if err != nil {
		return err
	}
	defer func() { _ = encryptedData.Close() }()

	// Store the encrypted data
	return e.underlying.PutWithContext(ctx, key, encryptedData)
}

// PutWithMetadata encrypts data and stores it with metadata
func (e *encryptedStorage) PutWithMetadata(ctx context.Context, key string, data io.Reader, metadata *Metadata) error {
	// Get encrypter using default key ID
	encrypter, err := e.encrypterFactory.GetEncrypter(e.defaultKeyID)
	if err != nil {
		return err
	}

	// Encrypt the data
	encryptedData, err := encrypter.Encrypt(ctx, data)
	if err != nil {
		return err
	}
	defer func() { _ = encryptedData.Close() }()

	// Add encryption metadata to custom fields
	if metadata.Custom == nil {
		metadata.Custom = make(map[string]string)
	}
	metadata.Custom["encryption_algorithm"] = encrypter.Algorithm()
	metadata.Custom["encryption_key_id"] = encrypter.KeyID()

	// Store the encrypted data with metadata
	return e.underlying.PutWithMetadata(ctx, key, encryptedData, metadata)
}

// Get retrieves and decrypts data from the underlying storage
func (e *encryptedStorage) Get(key string) (io.ReadCloser, error) {
	return e.GetWithContext(context.Background(), key)
}

// GetWithContext retrieves and decrypts data from the underlying storage with context support
func (e *encryptedStorage) GetWithContext(ctx context.Context, key string) (io.ReadCloser, error) {
	// Get metadata to determine which key was used for encryption
	metadata, err := e.underlying.GetMetadata(ctx, key)
	var keyID string
	if err == nil && metadata != nil && metadata.Custom != nil {
		keyID = metadata.Custom["encryption_key_id"]
	}
	// If no key ID found in metadata, use default
	if keyID == "" {
		keyID = e.defaultKeyID
	}

	// Get the encrypted data
	encryptedData, err := e.underlying.GetWithContext(ctx, key)
	if err != nil {
		return nil, err
	}
	defer func() { _ = encryptedData.Close() }()

	// Get encrypter for decryption
	encrypter, err := e.encrypterFactory.GetEncrypter(keyID)
	if err != nil {
		return nil, err
	}

	// Decrypt the data
	decryptedData, err := encrypter.Decrypt(ctx, encryptedData)
	if err != nil {
		return nil, err
	}

	return decryptedData, nil
}

// GetMetadata retrieves metadata for an object (metadata is not encrypted)
func (e *encryptedStorage) GetMetadata(ctx context.Context, key string) (*Metadata, error) {
	return e.underlying.GetMetadata(ctx, key)
}

// UpdateMetadata updates metadata for an object (metadata is not encrypted)
func (e *encryptedStorage) UpdateMetadata(ctx context.Context, key string, metadata *Metadata) error {
	return e.underlying.UpdateMetadata(ctx, key, metadata)
}

// Delete removes an object from the underlying storage
func (e *encryptedStorage) Delete(key string) error {
	return e.underlying.Delete(key)
}

// DeleteWithContext removes an object from the underlying storage with context support
func (e *encryptedStorage) DeleteWithContext(ctx context.Context, key string) error {
	return e.underlying.DeleteWithContext(ctx, key)
}

// Exists checks if an object exists in the underlying storage
func (e *encryptedStorage) Exists(ctx context.Context, key string) (bool, error) {
	return e.underlying.Exists(ctx, key)
}

// List returns a list of keys from the underlying storage
func (e *encryptedStorage) List(prefix string) ([]string, error) {
	return e.underlying.List(prefix)
}

// ListWithContext returns a list of keys from the underlying storage with context support
func (e *encryptedStorage) ListWithContext(ctx context.Context, prefix string) ([]string, error) {
	return e.underlying.ListWithContext(ctx, prefix)
}

// ListWithOptions returns a paginated list of objects with metadata
func (e *encryptedStorage) ListWithOptions(ctx context.Context, opts *ListOptions) (*ListResult, error) {
	return e.underlying.ListWithOptions(ctx, opts)
}

// Archive copies an encrypted object to another backend
func (e *encryptedStorage) Archive(key string, destination Archiver) error {
	return e.underlying.Archive(key, destination)
}

// LifecycleManager delegation

func (e *encryptedStorage) AddPolicy(policy LifecyclePolicy) error {
	return e.underlying.AddPolicy(policy)
}

func (e *encryptedStorage) RemovePolicy(id string) error {
	return e.underlying.RemovePolicy(id)
}

func (e *encryptedStorage) GetPolicies() ([]LifecyclePolicy, error) {
	return e.underlying.GetPolicies()
}

// Ensure encryptedStorage implements Storage interface at compile time
var _ Storage = (*encryptedStorage)(nil)
