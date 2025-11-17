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

package replication

import (
	"context"
	"io"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// NoopEncrypter implements common.Encrypter with passthrough (no encryption).
// It returns data unmodified, preserving ReadCloser interfaces where possible.
type NoopEncrypter struct{}

// NewNoopEncrypter creates a new NoopEncrypter instance.
func NewNoopEncrypter() *NoopEncrypter {
	return &NoopEncrypter{}
}

// Encrypt performs no encryption, returning the input reader as-is.
// If the input is already an io.ReadCloser, it is returned unchanged.
// Otherwise, it is wrapped with io.NopCloser.
func (n *NoopEncrypter) Encrypt(ctx context.Context, plaintext io.Reader) (io.ReadCloser, error) {
	if rc, ok := plaintext.(io.ReadCloser); ok {
		return rc, nil
	}
	return io.NopCloser(plaintext), nil
}

// Decrypt performs no decryption, returning the input reader as-is.
// If the input is already an io.ReadCloser, it is returned unchanged.
// Otherwise, it is wrapped with io.NopCloser.
func (n *NoopEncrypter) Decrypt(ctx context.Context, ciphertext io.Reader) (io.ReadCloser, error) {
	if rc, ok := ciphertext.(io.ReadCloser); ok {
		return rc, nil
	}
	return io.NopCloser(ciphertext), nil
}

// Algorithm returns "none" to indicate no encryption is used.
func (n *NoopEncrypter) Algorithm() string {
	return "none"
}

// KeyID returns an empty string since no encryption key is used.
func (n *NoopEncrypter) KeyID() string {
	return ""
}

// NoopEncrypterFactory implements common.EncrypterFactory for passthrough encryption.
// It returns the same NoopEncrypter instance for all key IDs.
type NoopEncrypterFactory struct {
	encrypter *NoopEncrypter
}

// NewNoopEncrypterFactory creates a new NoopEncrypterFactory instance.
func NewNoopEncrypterFactory() *NoopEncrypterFactory {
	return &NoopEncrypterFactory{
		encrypter: NewNoopEncrypter(),
	}
}

// GetEncrypter returns a NoopEncrypter instance.
// The keyID parameter is ignored since no encryption is performed.
func (f *NoopEncrypterFactory) GetEncrypter(keyID string) (common.Encrypter, error) {
	return f.encrypter, nil
}

// DefaultKeyID returns an empty string since no encryption key is used.
func (f *NoopEncrypterFactory) DefaultKeyID() string {
	return ""
}

// Close releases any resources held by the factory.
// NoopEncrypterFactory has no resources to release, so this is a no-op.
func (f *NoopEncrypterFactory) Close() error {
	return nil
}
