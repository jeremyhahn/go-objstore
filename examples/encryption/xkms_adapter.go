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

// Package main provides an example of integrating go-xkms with go-objstore.
// This file demonstrates how applications can implement go-objstore's encryption
// interfaces using go-xkms as the backend.
//
// This is just one example - applications can use any encryption backend by
// implementing common.Encrypter and common.EncrypterFactory interfaces.
package main

import (
	"bytes"
	"context"
	"crypto"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/jeremyhahn/go-xkms/pkg/backend"
	"github.com/jeremyhahn/go-xkms/pkg/backend/software"
	"github.com/jeremyhahn/go-xkms/pkg/storage"
	"github.com/jeremyhahn/go-xkms/pkg/storage/memory"
	"github.com/jeremyhahn/go-xkms/pkg/types"
	"github.com/jeremyhahn/go-objstore/pkg/common"
)

var (
	// Adapter errors
	ErrSymmetricBackendRequired      = errors.New("symmetric backend is required")
	ErrKeyAttributesRequired         = errors.New("key attributes are required")
	ErrAtLeastOneKeyRequired         = errors.New("at least one key must be configured")
	ErrDefaultKeyRequired            = errors.New("default key ID is required")
	ErrDefaultKeyNotFound            = errors.New("default key ID not found in key attributes map")
	ErrKeyNotFound                   = errors.New("key ID not found")
	ErrEncryptionConfigRequired      = errors.New("encryption config is required")
	ErrInvalidEncryptionConfig       = errors.New("invalid encryption config")
	ErrEncryptionNotEnabled          = errors.New("encryption is not enabled")
	ErrXKMSRequired                  = errors.New("xkms configuration is required when encryption is enabled")
	ErrKeystoreRequired              = errors.New("at least one keystore must be configured")
	ErrKeystoreNameEmpty             = errors.New("keystore name cannot be empty")
	ErrDuplicateKeystore             = errors.New("duplicate keystore name")
	ErrKeystoreTypeEmpty             = errors.New("keystore type cannot be empty")
	ErrKeyRequired                   = errors.New("keystore must have at least one key")
	ErrKeyCNEmpty                    = errors.New("key CN cannot be empty")
	ErrKeyAlgorithmEmpty             = errors.New("key algorithm cannot be empty")
	ErrDefaultKeyNotFoundInStore     = errors.New("default key not found in any keystore")
	ErrNoXKMSConfigured              = errors.New("no xkms configured")
	ErrKeyNotFoundInKeystore         = errors.New("key not found in any keystore")
	ErrUnsupportedBackend            = errors.New("unsupported xkms backend type")
	ErrUnsupportedAlgorithm          = errors.New("unsupported key algorithm")
	ErrUnsupportedKeySize            = errors.New("unsupported AES key size")
	ErrUnsupportedCurve              = errors.New("unsupported curve")
	ErrUnsupportedHashAlgorithm      = errors.New("unsupported hash algorithm")
	ErrMultipleKeystoresNotSupported = errors.New("multiple keystores not yet supported - use single keystore for now")
	ErrFileBasedPKCS8NotImplemented  = errors.New("file-based PKCS#8 storage not yet implemented - use storage_type: memory for now")
)

// XKMSEncrypter implements common.Encrypter using go-xkms's SymmetricEncrypter.
type XKMSEncrypter struct {
	backend  types.SymmetricBackend
	keyAttrs *backend.KeyAttributes
	keyID    string
}

// NewXKMSEncrypter creates a new encrypter using go-xkms.
func NewXKMSEncrypter(symBackend types.SymmetricBackend, keyAttrs *backend.KeyAttributes, keyID string) (*XKMSEncrypter, error) {
	if symBackend == nil {
		return nil, ErrSymmetricBackendRequired
	}
	if keyAttrs == nil {
		return nil, ErrKeyAttributesRequired
	}

	return &XKMSEncrypter{
		backend:  symBackend,
		keyAttrs: keyAttrs,
		keyID:    keyID,
	}, nil
}

// Encrypt encrypts data using the symmetric key from go-xkms.
func (k *XKMSEncrypter) Encrypt(ctx context.Context, plaintext io.Reader) (io.ReadCloser, error) {
	encrypter, err := k.backend.SymmetricEncrypter(k.keyAttrs)
	if err != nil {
		return nil, err
	}

	plaintextData, err := io.ReadAll(plaintext)
	if err != nil {
		return nil, err
	}

	encryptedData, err := encrypter.Encrypt(plaintextData, &types.EncryptOptions{
		AdditionalData: []byte(k.keyID),
	})
	if err != nil {
		return nil, err
	}

	buf := new(bytes.Buffer)
	nonceLen := uint32(len(encryptedData.Nonce))
	if err := binary.Write(buf, binary.BigEndian, nonceLen); err != nil {
		return nil, err
	}
	if _, err := buf.Write(encryptedData.Nonce); err != nil {
		return nil, err
	}

	tagLen := uint32(len(encryptedData.Tag))
	if err := binary.Write(buf, binary.BigEndian, tagLen); err != nil {
		return nil, err
	}
	if _, err := buf.Write(encryptedData.Tag); err != nil {
		return nil, err
	}

	if _, err := buf.Write(encryptedData.Ciphertext); err != nil {
		return nil, err
	}

	return io.NopCloser(bytes.NewReader(buf.Bytes())), nil
}

// Decrypt decrypts data using the symmetric key from go-xkms.
func (k *XKMSEncrypter) Decrypt(ctx context.Context, ciphertext io.Reader) (io.ReadCloser, error) {
	encrypter, err := k.backend.SymmetricEncrypter(k.keyAttrs)
	if err != nil {
		return nil, err
	}

	encryptedBytes, err := io.ReadAll(ciphertext)
	if err != nil {
		return nil, err
	}

	reader := bytes.NewReader(encryptedBytes)

	var nonceLen uint32
	if err := binary.Read(reader, binary.BigEndian, &nonceLen); err != nil {
		return nil, err
	}

	nonce := make([]byte, nonceLen)
	if _, err := io.ReadFull(reader, nonce); err != nil {
		return nil, err
	}

	var tagLen uint32
	if err := binary.Read(reader, binary.BigEndian, &tagLen); err != nil {
		return nil, err
	}

	tag := make([]byte, tagLen)
	if _, err := io.ReadFull(reader, tag); err != nil {
		return nil, err
	}

	ciphertextData, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	encryptedData := &types.EncryptedData{
		Ciphertext: ciphertextData,
		Nonce:      nonce,
		Tag:        tag,
	}

	plaintext, err := encrypter.Decrypt(encryptedData, &types.DecryptOptions{
		AdditionalData: []byte(k.keyID),
	})
	if err != nil {
		return nil, err
	}

	return io.NopCloser(bytes.NewReader(plaintext)), nil
}

// Algorithm returns the encryption algorithm identifier
func (k *XKMSEncrypter) Algorithm() string {
	if k.keyAttrs.SymmetricAlgorithm != "" {
		switch k.keyAttrs.SymmetricAlgorithm {
		case types.SymmetricAES128GCM:
			return "AES-128-GCM"
		case types.SymmetricAES192GCM:
			return "AES-192-GCM"
		case types.SymmetricAES256GCM:
			return "AES-256-GCM"
		case types.SymmetricChaCha20Poly1305:
			return "ChaCha20-Poly1305"
		case types.SymmetricXChaCha20Poly1305:
			return "XChaCha20-Poly1305"
		default:
			return string(k.keyAttrs.SymmetricAlgorithm)
		}
	}

	switch k.keyAttrs.KeyAlgorithm.String() {
	case "RSA":
		return "RSA"
	case "ECDSA":
		if k.keyAttrs.ECCAttributes != nil {
			return fmt.Sprintf("ECDSA-%s", k.keyAttrs.ECCAttributes.Curve.Params().Name)
		}
		return "ECDSA"
	case "Ed25519":
		return "Ed25519"
	default:
		return "Unknown"
	}
}

// KeyID returns the key identifier
func (k *XKMSEncrypter) KeyID() string {
	return k.keyID
}

// XKMSEncrypterFactory implements common.EncrypterFactory using go-xkms.
type XKMSEncrypterFactory struct {
	backend      types.SymmetricBackend
	keyAttrsMap  map[string]*backend.KeyAttributes
	defaultKeyID string
}

// NewXKMSEncrypterFactory creates a new encrypter factory.
func NewXKMSEncrypterFactory(
	symBackend types.SymmetricBackend,
	keyAttrsMap map[string]*backend.KeyAttributes,
	defaultKeyID string,
) (*XKMSEncrypterFactory, error) {
	if symBackend == nil {
		return nil, ErrSymmetricBackendRequired
	}
	if len(keyAttrsMap) == 0 {
		return nil, ErrAtLeastOneKeyRequired
	}
	if defaultKeyID == "" {
		return nil, ErrDefaultKeyRequired
	}
	if _, ok := keyAttrsMap[defaultKeyID]; !ok {
		return nil, ErrDefaultKeyNotFound
	}

	return &XKMSEncrypterFactory{
		backend:      symBackend,
		keyAttrsMap:  keyAttrsMap,
		defaultKeyID: defaultKeyID,
	}, nil
}

// GetEncrypter returns an encrypter for the specified key ID.
func (f *XKMSEncrypterFactory) GetEncrypter(keyID string) (common.Encrypter, error) {
	if keyID == "" {
		keyID = f.defaultKeyID
	}

	keyAttrs, ok := f.keyAttrsMap[keyID]
	if !ok {
		return nil, ErrKeyNotFound
	}

	return NewXKMSEncrypter(f.backend, keyAttrs, keyID)
}

// DefaultKeyID returns the default key ID
func (f *XKMSEncrypterFactory) DefaultKeyID() string {
	return f.defaultKeyID
}

// Close releases resources
func (f *XKMSEncrypterFactory) Close() error {
	if f.backend != nil {
		return f.backend.Close()
	}
	return nil
}

// Config and helper types for configuration

// Config represents encryption configuration.
type Config struct {
	Enabled    bool        `yaml:"enabled" json:"enabled"`
	DefaultKey string      `yaml:"default_key" json:"default_key"`
	XKMS       *XKMSConfig `yaml:"xkms" json:"xkms"`
}

// XKMSConfig represents xKMS configuration.
type XKMSConfig struct {
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
	Curve     string `yaml:"curve,omitempty" json:"curve,omitempty"`
	Hash      string `yaml:"hash,omitempty" json:"hash,omitempty"`
	Password  string `yaml:"password,omitempty" json:"password,omitempty"`
}

// GetKeyIdentifier returns the key identifier (CN).
func (kc *KeyConfig) GetKeyIdentifier() string {
	return kc.CN
}

// ToBackendKeyAttributes converts KeyConfig to backend.KeyAttributes.
func (kc *KeyConfig) ToBackendKeyAttributes(keystore *KeystoreConfig) (*backend.KeyAttributes, error) {
	attrs := &backend.KeyAttributes{
		CN:        kc.CN,
		StoreType: backend.StoreType(keystore.Type),
	}

	switch strings.ToUpper(kc.Algorithm) {
	case "AES":
		keySize := kc.KeySize
		if keySize == 0 {
			keySize = 256
		}
		switch keySize {
		case 128:
			attrs.SymmetricAlgorithm = types.SymmetricAES128GCM
		case 192:
			attrs.SymmetricAlgorithm = types.SymmetricAES192GCM
		case 256:
			attrs.SymmetricAlgorithm = types.SymmetricAES256GCM
		default:
			return nil, ErrUnsupportedKeySize
		}
		attrs.AESAttributes = &types.AESAttributes{
			KeySize:   keySize,
			NonceSize: 12,
		}
	case "RSA":
		keySize := kc.KeySize
		if keySize == 0 {
			keySize = 2048
		}
		attrs.KeyAlgorithm = x509.RSA
		attrs.RSAAttributes = &types.RSAAttributes{
			KeySize: keySize,
		}
	case "ECDSA":
		curveName := kc.Curve
		if curveName == "" {
			curveName = "P-256"
		}
		curve := types.ParseCurve(curveName)
		if curve == nil {
			return nil, ErrUnsupportedCurve
		}
		attrs.KeyAlgorithm = x509.ECDSA
		attrs.ECCAttributes = &types.ECCAttributes{
			Curve: curve,
		}
	case "ED25519":
		attrs.KeyAlgorithm = x509.Ed25519
	default:
		return nil, ErrUnsupportedAlgorithm
	}

	if kc.Hash != "" {
		switch strings.ToUpper(kc.Hash) {
		case "SHA-256", "SHA256":
			attrs.Hash = crypto.SHA256
		case "SHA-384", "SHA384":
			attrs.Hash = crypto.SHA384
		case "SHA-512", "SHA512":
			attrs.Hash = crypto.SHA512
		default:
			return nil, ErrUnsupportedHashAlgorithm
		}
	}

	if kc.Password != "" {
		attrs.Password = backend.StaticPassword(kc.Password)
	}

	return attrs, nil
}

// Validate validates the configuration.
func (c *Config) Validate() error {
	if !c.Enabled {
		return nil
	}

	if c.DefaultKey == "" {
		return ErrDefaultKeyRequired
	}

	if c.XKMS == nil {
		return ErrXKMSRequired
	}

	if len(c.XKMS.Keystores) == 0 {
		return ErrKeystoreRequired
	}

	keystoreNames := make(map[string]bool)
	hasDefaultKey := false

	for _, keystore := range c.XKMS.Keystores {
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
// This shows how an application can integrate go-xkms with go-objstore.
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

	symBackend, err := createXKMSBackend(config.XKMS.Keystores)
	if err != nil {
		return nil, err
	}

	keyAttrsMap := make(map[string]*backend.KeyAttributes)
	for _, keystore := range config.XKMS.Keystores {
		for _, keyConfig := range keystore.Keys {
			keyIdentifier := keyConfig.GetKeyIdentifier()
			attrs, err := keyConfig.ToBackendKeyAttributes(keystore)
			if err != nil {
				return nil, err
			}
			keyAttrsMap[keyIdentifier] = attrs

			if err := ensureKeyExists(symBackend, attrs); err != nil {
				return nil, err
			}
		}
	}

	factory, err := NewXKMSEncrypterFactory(symBackend, keyAttrsMap, config.DefaultKey)
	if err != nil {
		return nil, err
	}

	return factory, nil
}

func createXKMSBackend(keystores []*KeystoreConfig) (types.SymmetricBackend, error) {
	if len(keystores) == 0 {
		return nil, ErrKeystoreRequired
	}

	if len(keystores) > 1 {
		return nil, ErrMultipleKeystoresNotSupported
	}

	keystore := keystores[0]

	switch keystore.Type {
	case "pkcs8", "software":
		return createPKCS8Backend(keystore)
	default:
		return nil, ErrUnsupportedBackend
	}
}

func createPKCS8Backend(keystore *KeystoreConfig) (types.SymmetricBackend, error) {
	storageType, _ := keystore.Config["storage_type"].(string)

	var keyStorage storage.KeyStorage
	if storageType == "memory" {
		keyStorage = memory.NewKeyStorage()
	} else {
		return nil, ErrFileBasedPKCS8NotImplemented
	}

	symBackend, err := software.NewBackend(&software.Config{
		KeyStorage: keyStorage,
	})
	if err != nil {
		return nil, err
	}

	return symBackend, nil
}

func ensureKeyExists(symBackend types.SymmetricBackend, attrs *backend.KeyAttributes) error {
	_, err := symBackend.GetSymmetricKey(attrs)
	if err == nil {
		return nil
	}

	_, err = symBackend.GenerateSymmetricKey(attrs)
	if err != nil {
		return err
	}

	return nil
}

// Ensure interfaces are implemented
var _ common.Encrypter = (*XKMSEncrypter)(nil)
var _ common.EncrypterFactory = (*XKMSEncrypterFactory)(nil)
