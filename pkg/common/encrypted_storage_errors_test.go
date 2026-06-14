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

// errClose is returned by errorCloseReadCloser.Close to drive the error
// branch inside readCloser.Close.
var errClose = errors.New("close error")

// errorCloseReadCloser wraps an io.Reader and returns an error from Close.
type errorCloseReadCloser struct {
	io.Reader
}

func (e *errorCloseReadCloser) Close() error { return errClose }

// errEncrypter returns errors from Encrypt or Decrypt.
type errEncrypter struct {
	encryptErr error
	decryptErr error
	keyID      string
	algorithm  string
}

func (e *errEncrypter) Encrypt(_ context.Context, _ io.Reader) (io.ReadCloser, error) {
	if e.encryptErr != nil {
		return nil, e.encryptErr
	}
	return io.NopCloser(bytes.NewReader([]byte("ENCRYPTED:data"))), nil
}

func (e *errEncrypter) Decrypt(_ context.Context, _ io.Reader) (io.ReadCloser, error) {
	if e.decryptErr != nil {
		return nil, e.decryptErr
	}
	return io.NopCloser(bytes.NewReader([]byte("data"))), nil
}

func (e *errEncrypter) Algorithm() string { return e.algorithm }
func (e *errEncrypter) KeyID() string     { return e.keyID }

// errEncrypterFactory returns an error from GetEncrypter for a specific key.
type errEncrypterFactory struct {
	defaultKeyID  string
	getErr        error
	goodEncrypter Encrypter
}

func (f *errEncrypterFactory) DefaultKeyID() string { return f.defaultKeyID }

func (f *errEncrypterFactory) GetEncrypter(keyID string) (Encrypter, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	return f.goodEncrypter, nil
}

func (f *errEncrypterFactory) Close() error { return nil }

// erroringStorage is a Storage that returns errors from selected operations.
type erroringStorage struct {
	*mockUnderlyingStorage
	getErr        error
	putErr        error
	putMetaErr    error
	getMetaErr    error
	getMetaResult *Metadata
}

func (s *erroringStorage) GetWithContext(_ context.Context, _ string) (io.ReadCloser, error) {
	if s.getErr != nil {
		return nil, s.getErr
	}
	return s.mockUnderlyingStorage.GetWithContext(context.Background(), "")
}

func (s *erroringStorage) GetMetadata(_ context.Context, _ string) (*Metadata, error) {
	if s.getMetaErr != nil {
		return nil, s.getMetaErr
	}
	return s.getMetaResult, nil
}

func (s *erroringStorage) PutWithContext(_ context.Context, _ string, _ io.Reader) error {
	if s.putErr != nil {
		return s.putErr
	}
	return nil
}

func (s *erroringStorage) PutWithMetadata(_ context.Context, _ string, _ io.Reader, _ *Metadata) error {
	if s.putMetaErr != nil {
		return s.putMetaErr
	}
	return nil
}

// TestReadCloser_Close_ErrorPropagation verifies that readCloser.Close
// returns the first error it encounters and continues closing the rest.
func TestReadCloser_Close_ErrorPropagation(t *testing.T) {
	// Two closers: first errors, second is a NopCloser. Close must return
	// the first error.
	rc := &readCloser{
		Reader: strings.NewReader("data"),
		closes: []io.Closer{
			&errorCloseReadCloser{Reader: strings.NewReader("x")},
			io.NopCloser(strings.NewReader("y")),
		},
	}
	err := rc.Close()
	if !errors.Is(err, errClose) {
		t.Errorf("Close() = %v, want errClose", err)
	}
}

// TestReadCloser_Close_MultipleErrors verifies that readCloser.Close
// returns only the first error even when multiple closers fail.
func TestReadCloser_Close_MultipleErrors(t *testing.T) {
	rc := &readCloser{
		Reader: strings.NewReader("data"),
		closes: []io.Closer{
			&errorCloseReadCloser{Reader: strings.NewReader("a")},
			&errorCloseReadCloser{Reader: strings.NewReader("b")},
		},
	}
	err := rc.Close()
	if !errors.Is(err, errClose) {
		t.Errorf("Close() = %v, want errClose", err)
	}
}

// TestEncryptedStorage_PutWithContext_GetEncrypterError verifies that
// PutWithContext propagates an error returned by GetEncrypter.
func TestEncryptedStorage_PutWithContext_GetEncrypterError(t *testing.T) {
	underlying := newMockUnderlyingStorage()
	errGet := errors.New("key not found")
	factory := &errEncrypterFactory{
		defaultKeyID: "missing",
		getErr:       errGet,
	}
	storage := NewEncryptedStorage(underlying, factory)

	err := storage.PutWithContext(context.Background(), "k", strings.NewReader("data"))
	if !errors.Is(err, errGet) {
		t.Errorf("PutWithContext() error = %v, want %v", err, errGet)
	}
}

// TestEncryptedStorage_PutWithContext_EncryptError verifies that
// PutWithContext propagates an error returned by Encrypt.
func TestEncryptedStorage_PutWithContext_EncryptError(t *testing.T) {
	underlying := newMockUnderlyingStorage()
	errEnc := errors.New("encrypt failed")
	factory := &errEncrypterFactory{
		defaultKeyID: "k1",
		goodEncrypter: &errEncrypter{
			encryptErr: errEnc,
			keyID:      "k1",
		},
	}
	storage := NewEncryptedStorage(underlying, factory)

	err := storage.PutWithContext(context.Background(), "k", strings.NewReader("data"))
	if !errors.Is(err, errEnc) {
		t.Errorf("PutWithContext() error = %v, want %v", err, errEnc)
	}
}

// TestEncryptedStorage_PutWithMetadata_GetEncrypterError verifies that
// PutWithMetadata propagates an error returned by GetEncrypter.
func TestEncryptedStorage_PutWithMetadata_GetEncrypterError(t *testing.T) {
	underlying := newMockUnderlyingStorage()
	errGet := errors.New("factory error")
	factory := &errEncrypterFactory{
		defaultKeyID: "missing",
		getErr:       errGet,
	}
	storage := NewEncryptedStorage(underlying, factory)

	meta := &Metadata{Custom: map[string]string{"x": "y"}}
	err := storage.PutWithMetadata(context.Background(), "k", strings.NewReader("data"), meta)
	if !errors.Is(err, errGet) {
		t.Errorf("PutWithMetadata() error = %v, want %v", err, errGet)
	}
}

// TestEncryptedStorage_PutWithMetadata_NilCustom verifies that
// PutWithMetadata initialises metadata.Custom when it is nil.
func TestEncryptedStorage_PutWithMetadata_NilCustom(t *testing.T) {
	underlying := newMockUnderlyingStorage()
	factory := &mockEncrypterFactory{
		defaultKeyID: "k1",
		encrypters: map[string]Encrypter{
			"k1": &mockEncrypter{keyID: "k1", algorithm: "AES256"},
		},
	}
	storage := NewEncryptedStorage(underlying, factory)

	// Pass metadata with nil Custom — code must initialise it.
	meta := &Metadata{}
	err := storage.PutWithMetadata(context.Background(), "k", strings.NewReader("hello"), meta)
	if err != nil {
		t.Fatalf("PutWithMetadata() unexpected error: %v", err)
	}

	stored := underlying.metadata["k"]
	if stored == nil || stored.Custom == nil {
		t.Fatal("metadata.Custom was not initialised")
	}
	if stored.Custom["encryption_key_id"] != "k1" {
		t.Errorf("encryption_key_id = %q, want k1", stored.Custom["encryption_key_id"])
	}
}

// TestEncryptedStorage_GetWithContext_GetEncrypterError verifies that
// GetWithContext closes the encrypted reader and propagates a GetEncrypter
// error encountered after the underlying read succeeded.
func TestEncryptedStorage_GetWithContext_GetEncrypterError(t *testing.T) {
	// Pre-populate underlying with raw data so GetWithContext can read it.
	underlying := newMockUnderlyingStorage()
	underlying.data["k"] = []byte("raw")
	underlying.metadata["k"] = &Metadata{}

	errGet := errors.New("encrypter unavailable")
	factory := &errEncrypterFactory{
		defaultKeyID: "k1",
		getErr:       errGet,
	}
	storage := NewEncryptedStorage(underlying, factory)

	_, err := storage.GetWithContext(context.Background(), "k")
	if !errors.Is(err, errGet) {
		t.Errorf("GetWithContext() error = %v, want %v", err, errGet)
	}
}

// TestEncryptedStorage_PutWithMetadata_EncryptError verifies that
// PutWithMetadata propagates an error returned by Encrypt.
func TestEncryptedStorage_PutWithMetadata_EncryptError(t *testing.T) {
	underlying := newMockUnderlyingStorage()
	errEnc := errors.New("encrypt failed")
	factory := &errEncrypterFactory{
		defaultKeyID: "k1",
		goodEncrypter: &errEncrypter{
			encryptErr: errEnc,
			keyID:      "k1",
		},
	}
	storage := NewEncryptedStorage(underlying, factory)

	meta := &Metadata{}
	err := storage.PutWithMetadata(context.Background(), "k", strings.NewReader("data"), meta)
	if !errors.Is(err, errEnc) {
		t.Errorf("PutWithMetadata() error = %v, want %v", err, errEnc)
	}
}

// TestEncryptedStorage_GetWithContext_GetError verifies that GetWithContext
// returns an error when the underlying Get fails.
func TestEncryptedStorage_GetWithContext_GetError(t *testing.T) {
	underlying := newMockUnderlyingStorage()
	factory := &mockEncrypterFactory{
		defaultKeyID: "k1",
		encrypters: map[string]Encrypter{
			"k1": &mockEncrypter{keyID: "k1", algorithm: "AES256"},
		},
	}
	storage := NewEncryptedStorage(underlying, factory)

	// "missing" key was never stored.
	_, err := storage.GetWithContext(context.Background(), "missing")
	if err == nil {
		t.Error("GetWithContext() expected error for missing key, got nil")
	}
}

// TestEncryptedStorage_GetWithContext_KeyFromMetadata verifies that
// GetWithContext reads the encryption key ID from object metadata when it is
// present, exercising the `if err == nil && metadata != nil && metadata.Custom != nil`
// branch.
func TestEncryptedStorage_GetWithContext_KeyFromMetadata(t *testing.T) {
	underlying := newMockUnderlyingStorage()
	factory := &mockEncrypterFactory{
		defaultKeyID: "k1",
		encrypters: map[string]Encrypter{
			"k1": &mockEncrypter{keyID: "k1", algorithm: "AES256"},
		},
	}
	storage := NewEncryptedStorage(underlying, factory)

	// PutWithMetadata stores encryption_key_id in the custom metadata, which
	// GetWithContext will find and use.
	meta := &Metadata{}
	if err := storage.PutWithMetadata(context.Background(), "k", strings.NewReader("hello"), meta); err != nil {
		t.Fatalf("PutWithMetadata() failed: %v", err)
	}

	reader, err := storage.GetWithContext(context.Background(), "k")
	if err != nil {
		t.Fatalf("GetWithContext() failed: %v", err)
	}
	defer func() { _ = reader.Close() }()

	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll() failed: %v", err)
	}
	if string(data) != "hello" {
		t.Errorf("decrypted = %q, want %q", data, "hello")
	}
}

// TestEncryptedStorage_GetWithContext_DecryptError verifies that
// GetWithContext closes the encrypted reader and propagates a Decrypt error.
func TestEncryptedStorage_GetWithContext_DecryptError(t *testing.T) {
	underlying := newMockUnderlyingStorage()
	// Store raw bytes — the mock encrypter expects "ENCRYPTED:" prefix for
	// Decrypt, so raw bytes will cause the decrypt error we want.
	underlying.data["k"] = []byte("not-encrypted-data")
	underlying.metadata["k"] = &Metadata{}

	factory := &mockEncrypterFactory{
		defaultKeyID: "k1",
		encrypters: map[string]Encrypter{
			"k1": &mockEncrypter{keyID: "k1", algorithm: "AES256"},
		},
	}
	storage := NewEncryptedStorage(underlying, factory)

	_, err := storage.GetWithContext(context.Background(), "k")
	if !errors.Is(err, errTestInvalidEncryptedData) {
		t.Errorf("GetWithContext() error = %v, want errTestInvalidEncryptedData", err)
	}
}
