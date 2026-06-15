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

// Package main provides a self-contained example of implementing go-objstore's
// encryption interfaces using only the Go standard library. AESEncrypter and
// AESEncrypterFactory demonstrate how any application can wire in its own key
// management by satisfying common.Encrypter and common.EncrypterFactory — no
// external KMS dependency required.
package main

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

var (
	// Adapter errors
	ErrAtLeastOneKeyRequired     = errors.New("at least one key must be configured")
	ErrDefaultKeyRequired        = errors.New("default key ID is required")
	ErrDefaultKeyNotFound        = errors.New("default key ID not found in key map")
	ErrKeyNotFound               = errors.New("key ID not found")
	ErrEncryptionConfigRequired  = errors.New("encryption config is required")
	ErrEncryptionNotEnabled      = errors.New("encryption is not enabled")
	ErrKMSRequired               = errors.New("kms configuration is required when encryption is enabled")
	ErrKeystoreRequired          = errors.New("at least one keystore must be configured")
	ErrKeystoreNameEmpty         = errors.New("keystore name cannot be empty")
	ErrDuplicateKeystore         = errors.New("duplicate keystore name")
	ErrKeystoreTypeEmpty         = errors.New("keystore type cannot be empty")
	ErrKeyRequired               = errors.New("keystore must have at least one key")
	ErrKeyCNEmpty                = errors.New("key CN cannot be empty")
	ErrKeyAlgorithmEmpty         = errors.New("key algorithm cannot be empty")
	ErrDefaultKeyNotFoundInStore = errors.New("default key not found in any keystore")
	ErrNoKMSConfigured           = errors.New("no kms configured")
	ErrUnsupportedAlgorithm      = errors.New("unsupported key algorithm: only AES-256-GCM is supported")
	ErrUnsupportedKeySize        = errors.New("unsupported AES key size: only 256-bit keys are supported")
)

// AESEncrypter implements common.Encrypter using AES-256-GCM from the Go
// standard library. Wire format: 4-byte big-endian nonce length | nonce |
// ciphertext (GCM tag appended by cipher.AEAD.Seal).
type AESEncrypter struct {
	key   []byte // 32-byte AES-256 key
	keyID string
}

// NewAESEncrypter creates an AESEncrypter for the given 32-byte key and key ID.
func NewAESEncrypter(key []byte, keyID string) (*AESEncrypter, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf("AES-256 requires a 32-byte key, got %d bytes", len(key))
	}
	if keyID == "" {
		return nil, ErrDefaultKeyRequired
	}
	return &AESEncrypter{key: key, keyID: keyID}, nil
}

// Encrypt reads all plaintext, seals it with AES-256-GCM using a random nonce,
// and returns a reader over: 4-byte nonce-length | nonce | ciphertext+tag.
func (e *AESEncrypter) Encrypt(_ context.Context, plaintext io.Reader) (io.ReadCloser, error) {
	block, err := aes.NewCipher(e.key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}

	plaintextBytes, err := io.ReadAll(plaintext)
	if err != nil {
		return nil, err
	}

	ciphertext := gcm.Seal(nil, nonce, plaintextBytes, nil)

	buf := new(bytes.Buffer)
	nonceLen := uint32(len(nonce))
	if err := binary.Write(buf, binary.BigEndian, nonceLen); err != nil {
		return nil, err
	}
	if _, err := buf.Write(nonce); err != nil {
		return nil, err
	}
	if _, err := buf.Write(ciphertext); err != nil {
		return nil, err
	}

	return io.NopCloser(bytes.NewReader(buf.Bytes())), nil
}

// Decrypt reads the wire format produced by Encrypt and returns the plaintext.
func (e *AESEncrypter) Decrypt(_ context.Context, ciphertext io.Reader) (io.ReadCloser, error) {
	block, err := aes.NewCipher(e.key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	data, err := io.ReadAll(ciphertext)
	if err != nil {
		return nil, err
	}

	reader := bytes.NewReader(data)

	var nonceLen uint32
	if err := binary.Read(reader, binary.BigEndian, &nonceLen); err != nil {
		return nil, err
	}

	// Reject attacker-controlled nonce lengths before allocating. The nonce size
	// is fixed by the GCM construction, so a mismatch indicates corrupt or hostile
	// input; validating here also prevents a memory-amplification DoS where a tiny
	// 4-byte header (e.g. 0xFFFFFFFF) would otherwise drive a multi-gigabyte make.
	if nonceLen != uint32(gcm.NonceSize()) {
		return nil, fmt.Errorf("invalid nonce length: got %d, want %d", nonceLen, gcm.NonceSize())
	}

	nonce := make([]byte, nonceLen)
	if _, err := io.ReadFull(reader, nonce); err != nil {
		return nil, err
	}

	ciphertextBytes, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	plaintext, err := gcm.Open(nil, nonce, ciphertextBytes, nil)
	if err != nil {
		return nil, err
	}

	return io.NopCloser(bytes.NewReader(plaintext)), nil
}

// Algorithm returns the encryption algorithm identifier.
func (e *AESEncrypter) Algorithm() string {
	return "AES-256-GCM"
}

// KeyID returns the key identifier.
func (e *AESEncrypter) KeyID() string {
	return e.keyID
}

// AESEncrypterFactory implements common.EncrypterFactory using in-memory AES-256
// keys generated at startup. Each key CN in the configuration gets a fresh
// random 32-byte key; in a production system these would be loaded from your
// KMS (HashiCorp Vault, AWS KMS, GCP KMS, Azure Key Vault, etc.).
type AESEncrypterFactory struct {
	keys         map[string][]byte // keyID → 32-byte AES key
	defaultKeyID string
}

// NewAESEncrypterFactory creates a factory from a map of keyID→key and a default key ID.
func NewAESEncrypterFactory(keys map[string][]byte, defaultKeyID string) (*AESEncrypterFactory, error) {
	if len(keys) == 0 {
		return nil, ErrAtLeastOneKeyRequired
	}
	if defaultKeyID == "" {
		return nil, ErrDefaultKeyRequired
	}
	if _, ok := keys[defaultKeyID]; !ok {
		return nil, ErrDefaultKeyNotFound
	}
	return &AESEncrypterFactory{keys: keys, defaultKeyID: defaultKeyID}, nil
}

// GetEncrypter returns an AESEncrypter for the given key ID.
// If keyID is empty, the default key is used.
func (f *AESEncrypterFactory) GetEncrypter(keyID string) (common.Encrypter, error) {
	if keyID == "" {
		keyID = f.defaultKeyID
	}
	key, ok := f.keys[keyID]
	if !ok {
		return nil, ErrKeyNotFound
	}
	return NewAESEncrypter(key, keyID)
}

// DefaultKeyID returns the default key ID used for new encryptions.
func (f *AESEncrypterFactory) DefaultKeyID() string {
	return f.defaultKeyID
}

// Close releases any resources held by the factory (none for in-memory keys).
func (f *AESEncrypterFactory) Close() error {
	return nil
}

// Config and helper types for configuration.

// Config represents encryption configuration.
type Config struct {
	Enabled    bool       `yaml:"enabled" json:"enabled"`
	DefaultKey string     `yaml:"default_key" json:"default_key"`
	KMS        *KMSConfig `yaml:"kms" json:"kms"`
}

// KMSConfig represents the key management configuration.
type KMSConfig struct {
	Keystores []*KeystoreConfig `yaml:"keystores" json:"keystores"`
}

// KeystoreConfig represents a keystore configuration.
type KeystoreConfig struct {
	Name   string         `yaml:"name" json:"name"`
	Type   string         `yaml:"type" json:"type"`
	Config map[string]any `yaml:"config" json:"config"`
	Keys   []*KeyConfig   `yaml:"keys" json:"keys"`
}

// KeyConfig represents a key configuration.
type KeyConfig struct {
	CN        string `yaml:"cn" json:"cn"`
	Algorithm string `yaml:"algorithm" json:"algorithm"`
	KeySize   int    `yaml:"key_size,omitempty" json:"key_size,omitempty"`
}

// GetKeyIdentifier returns the key identifier (CN).
func (kc *KeyConfig) GetKeyIdentifier() string {
	return kc.CN
}

// Validate validates the configuration.
func (c *Config) Validate() error {
	if !c.Enabled {
		return nil
	}

	if c.DefaultKey == "" {
		return ErrDefaultKeyRequired
	}

	if c.KMS == nil {
		return ErrKMSRequired
	}

	if len(c.KMS.Keystores) == 0 {
		return ErrKeystoreRequired
	}

	keystoreNames := make(map[string]bool)
	hasDefaultKey := false

	for _, keystore := range c.KMS.Keystores {
		if keystore.Name == "" {
			return ErrKeystoreNameEmpty
		}
		if keystoreNames[keystore.Name] {
			return ErrDuplicateKeystore
		}
		keystoreNames[keystore.Name] = true

		if keystore.Type == "" {
			return ErrKeystoreTypeEmpty
		}

		if len(keystore.Keys) == 0 {
			return ErrKeyRequired
		}

		for _, key := range keystore.Keys {
			if key.CN == "" {
				return ErrKeyCNEmpty
			}
			if key.Algorithm == "" {
				return ErrKeyAlgorithmEmpty
			}
			if key.CN == c.DefaultKey {
				hasDefaultKey = true
			}
		}
	}

	if !hasDefaultKey {
		return ErrDefaultKeyNotFoundInStore
	}

	return nil
}

// NewEncrypterFactory creates an EncrypterFactory from configuration.
// Each key entry in the config gets a newly generated random AES-256 key.
// In production, replace the key-generation step with retrieval from your KMS.
func NewEncrypterFactory(config *Config) (common.EncrypterFactory, error) {
	if config == nil {
		return nil, ErrEncryptionConfigRequired
	}

	if err := config.Validate(); err != nil {
		return nil, err
	}

	if !config.Enabled {
		return nil, ErrEncryptionNotEnabled
	}

	keys := make(map[string][]byte)
	for _, keystore := range config.KMS.Keystores {
		for _, keyConfig := range keystore.Keys {
			if err := validateKeyConfig(keyConfig); err != nil {
				return nil, err
			}
			key, err := generateAESKey()
			if err != nil {
				return nil, err
			}
			keys[keyConfig.GetKeyIdentifier()] = key
		}
	}

	return NewAESEncrypterFactory(keys, config.DefaultKey)
}

// validateKeyConfig checks that the key configuration specifies AES-256.
func validateKeyConfig(kc *KeyConfig) error {
	if strings.ToUpper(kc.Algorithm) != "AES" {
		return ErrUnsupportedAlgorithm
	}
	keySize := kc.KeySize
	if keySize == 0 {
		keySize = 256
	}
	if keySize != 256 {
		return ErrUnsupportedKeySize
	}
	return nil
}

// generateAESKey creates a cryptographically random 32-byte (256-bit) AES key.
func generateAESKey() ([]byte, error) {
	key := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		return nil, err
	}
	return key, nil
}

// Compile-time interface assertions.
var _ common.Encrypter = (*AESEncrypter)(nil)
var _ common.EncrypterFactory = (*AESEncrypterFactory)(nil)
