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

package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

func main() {
	fmt.Println("=== Object Store Encryption Example ===")
	fmt.Println()

	// Example 1: Basic encryption configuration
	basicExample()

	// Example 2: Key rotation scenario
	keyRotationExample()

	// Example 3: Configuration file-based setup
	configFileExample()
}

func basicExample() {
	fmt.Println("Example 1: Basic Encryption")
	fmt.Println("----------------------------")

	// Create a simple in-memory storage backend for demonstration
	mockStorage := &mockStorage{data: make(map[string][]byte)}

	// Create encryption configuration
	encryptionConfig := &Config{
		Enabled:    true,
		DefaultKey: "primary", // Changed from DefaultKeyID to DefaultKey
		Keychain: &KeychainConfig{
			Keystores: []*KeystoreConfig{
				{
					Name: "primary-keystore",
					Type: "software", // or "pkcs8"
					Config: map[string]any{
						"storage_type": "memory", // Use "memory" for this example
					},
					Keys: []*KeyConfig{
						{
							CN:        "primary",
							Algorithm: "AES",
							KeySize:   256,
						},
					},
				},
			},
		},
	}

	// Validate configuration
	if err := encryptionConfig.Validate(); err != nil {
		log.Fatalf("Invalid encryption config: %v", err)
	}

	// Create encrypter factory using the keychain adapter
	encrypterFactory, err := NewEncrypterFactory(encryptionConfig)
	if err != nil {
		log.Fatalf("Failed to create encrypter factory: %v", err)
	}
	defer encrypterFactory.Close()

	// Wrap storage with encryption
	encryptedStorage := common.NewEncryptedStorage(mockStorage, encrypterFactory)

	// Use the encrypted storage - all Put operations are automatically encrypted
	ctx := context.Background()
	testData := []byte("This is sensitive data that will be encrypted at rest")

	err = encryptedStorage.PutWithContext(ctx, "test-key", bytes.NewReader(testData))
	if err != nil {
		log.Fatalf("Failed to put encrypted data: %v", err)
	}
	fmt.Println("✓ Data encrypted and stored successfully")

	// All Get operations are automatically decrypted
	reader, err := encryptedStorage.GetWithContext(ctx, "test-key")
	if err != nil {
		log.Fatalf("Failed to get encrypted data: %v", err)
	}
	defer reader.Close()

	var retrievedData bytes.Buffer
	if _, err := retrievedData.ReadFrom(reader); err != nil {
		log.Fatalf("Failed to read decrypted data: %v", err)
	}

	if retrievedData.String() == string(testData) {
		fmt.Println("✓ Data decrypted successfully")
		fmt.Printf("  Original: %s\n", testData)
		fmt.Printf("  Retrieved: %s\n", retrievedData.String())
	} else {
		log.Fatal("✗ Decrypted data doesn't match original!")
	}

	fmt.Println()
}

func keyRotationExample() {
	fmt.Println("Example 2: Key Rotation")
	fmt.Println("-----------------------")

	// Configuration with multiple keys for rotation scenarios
	encryptionConfig := &Config{
		Enabled:    true,
		DefaultKey: "primary", // New encryptions use primary key
		Keychain: &KeychainConfig{
			Keystores: []*KeystoreConfig{
				{
					Name: "rotation-keystore",
					Type: "software",
					Config: map[string]any{
						"storage_type": "memory",
					},
					Keys: []*KeyConfig{
						{
							CN:        "primary",
							Algorithm: "AES",
							KeySize:   256,
						},
						{
							CN:        "old",
							Algorithm: "AES",
							KeySize:   256,
						},
					},
				},
			},
		},
	}

	fmt.Println("✓ Configuration supports multiple keys")
	fmt.Printf("  Default key for new encryptions: %s\n", encryptionConfig.DefaultKey)
	keyCount := 0
	for _, ks := range encryptionConfig.Keychain.Keystores {
		keyCount += len(ks.Keys)
	}
	fmt.Printf("  Available keys: %d\n", keyCount)
	fmt.Println("  - Old data encrypted with 'old' key can still be decrypted")
	fmt.Println("  - New data will be encrypted with 'primary' key")
	fmt.Println()
}

func configFileExample() {
	fmt.Println("Example 3: Configuration File")
	fmt.Println("-----------------------------")
	fmt.Println("Sample YAML configuration:")
	fmt.Println()
	fmt.Println("```yaml")
	fmt.Println("backend: local")
	fmt.Println("backend-path: ./storage")
	fmt.Println()
	fmt.Println("encryption:")
	fmt.Println("  enabled: true")
	fmt.Println("  default_key: primary")
	fmt.Println("  keychain:")
	fmt.Println("    keystores:")
	fmt.Println("      - name: primary-keystore")
	fmt.Println("        type: software")
	fmt.Println("        config:")
	fmt.Println("          storage_type: memory")
	fmt.Println("        keys:")
	fmt.Println("          - cn: primary")
	fmt.Println("            algorithm: AES")
	fmt.Println("            key_size: 256")
	fmt.Println("```")
	fmt.Println()
	fmt.Println("For AWS KMS (when implemented):")
	fmt.Println("```yaml")
	fmt.Println("encryption:")
	fmt.Println("  enabled: true")
	fmt.Println("  default_key: production")
	fmt.Println("  keychain:")
	fmt.Println("    keystores:")
	fmt.Println("      - name: aws-kms")
	fmt.Println("        type: awskms")
	fmt.Println("        config:")
	fmt.Println("          region: us-east-1")
	fmt.Println("        keys:")
	fmt.Println("          - cn: production")
	fmt.Println("            algorithm: AES")
	fmt.Println("            key_size: 256")
	fmt.Println("```")
	fmt.Println()
}

// Example helper function for production use
// Note: In production, replace mockStorage with your actual storage backend
func createProductionEncryptedStorage(baseStorage common.Storage, keyID string) (common.Storage, error) {
	// Create encryption config
	encryptionConfig := &Config{
		Enabled:    true,
		DefaultKey: keyID,
		Keychain: &KeychainConfig{
			Keystores: []*KeystoreConfig{
				{
					Name: "production-keystore",
					Type: "software",
					Config: map[string]any{
						"storage_type": "memory",
					},
					Keys: []*KeyConfig{
						{
							CN:        keyID,
							Algorithm: "AES",
							KeySize:   256,
						},
					},
				},
			},
		},
	}

	// Create encrypter factory
	encrypterFactory, err := NewEncrypterFactory(encryptionConfig)
	if err != nil {
		return nil, err
	}
	// Note: In production, you'd want to manage the lifecycle of encrypterFactory
	// and ensure Close() is called when done

	// Wrap with encryption
	return common.NewEncryptedStorage(baseStorage, encrypterFactory), nil
}

// mockStorage is a simple in-memory storage for demonstration purposes
type mockStorage struct {
	data map[string][]byte
}

func (m *mockStorage) Configure(settings map[string]string) error {
	return nil
}

func (m *mockStorage) Put(key string, data io.Reader) error {
	return m.PutWithContext(context.Background(), key, data)
}

func (m *mockStorage) PutWithContext(ctx context.Context, key string, data io.Reader) error {
	buf, err := io.ReadAll(data)
	if err != nil {
		return err
	}
	m.data[key] = buf
	return nil
}

func (m *mockStorage) PutWithMetadata(ctx context.Context, key string, data io.Reader, metadata *common.Metadata) error {
	return m.PutWithContext(ctx, key, data)
}

func (m *mockStorage) Get(key string) (io.ReadCloser, error) {
	return m.GetWithContext(context.Background(), key)
}

func (m *mockStorage) GetWithContext(ctx context.Context, key string) (io.ReadCloser, error) {
	data, ok := m.data[key]
	if !ok {
		return nil, fmt.Errorf("key not found: %s", key)
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (m *mockStorage) GetMetadata(ctx context.Context, key string) (*common.Metadata, error) {
	return &common.Metadata{}, nil
}

func (m *mockStorage) UpdateMetadata(ctx context.Context, key string, metadata *common.Metadata) error {
	return nil
}

func (m *mockStorage) Delete(key string) error {
	return m.DeleteWithContext(context.Background(), key)
}

func (m *mockStorage) DeleteWithContext(ctx context.Context, key string) error {
	delete(m.data, key)
	return nil
}

func (m *mockStorage) Exists(ctx context.Context, key string) (bool, error) {
	_, ok := m.data[key]
	return ok, nil
}

func (m *mockStorage) List(prefix string) ([]string, error) {
	return m.ListWithContext(context.Background(), prefix)
}

func (m *mockStorage) ListWithContext(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	for k := range m.data {
		keys = append(keys, k)
	}
	return keys, nil
}

func (m *mockStorage) ListWithOptions(ctx context.Context, opts *common.ListOptions) (*common.ListResult, error) {
	keys, err := m.ListWithContext(ctx, opts.Prefix)
	if err != nil {
		return nil, err
	}
	var objects []*common.ObjectInfo
	for _, k := range keys {
		objects = append(objects, &common.ObjectInfo{Key: k})
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
