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

// Encrypter provides encryption operations for object storage.
// Implementations must be thread-safe.
type Encrypter interface {
	// Encrypt encrypts data from the reader and returns an io.Reader containing
	// the encrypted data. The implementation handles buffering as needed.
	Encrypt(ctx context.Context, plaintext io.Reader) (io.ReadCloser, error)

	// Decrypt decrypts data from the reader and returns an io.Reader containing
	// the decrypted data. The implementation handles buffering as needed.
	Decrypt(ctx context.Context, ciphertext io.Reader) (io.ReadCloser, error)

	// Algorithm returns the encryption algorithm identifier
	Algorithm() string

	// KeyID returns the key identifier used for encryption
	KeyID() string
}

// EncrypterFactory creates Encrypter instances.
// This allows for key rotation and multi-key scenarios.
type EncrypterFactory interface {
	// GetEncrypter returns an encrypter for the specified key ID.
	// If keyID is empty, uses the default key.
	GetEncrypter(keyID string) (Encrypter, error)

	// DefaultKeyID returns the default key ID to use for new encryptions
	DefaultKeyID() string

	// Close releases any resources held by the factory
	Close() error
}
