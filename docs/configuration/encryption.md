# Encryption Configuration

Configuration reference for at-rest encryption.

## Overview

go-objstore provides encryption through an **abstraction layer** - you provide an implementation of the `Encrypter` and `EncrypterFactory` interfaces. This allows you to use **any key management solution** (go-keychain, HashiCorp Vault, cloud KMS, etc.) without hard-coded dependencies.

## Programmatic API

### Using the Encryption Abstraction

```go
import (
    "github.com/jeremyhahn/go-objstore/pkg/common"
    "github.com/jeremyhahn/go-objstore/pkg/factory"
)

// 1. Create your storage backend
storage, err := factory.NewStorage("s3", map[string]string{
    "bucket": "my-bucket",
    "region": "us-east-1",
})

// 2. Create your encrypter factory implementation
// (using go-keychain, Vault, or your own implementation)
encrypterFactory := myapp.NewEncrypterFactory(config)

// 3. Wrap storage with encryption
encryptedStorage := common.NewEncryptedStorage(storage, encrypterFactory)

// 4. Use normally - encryption is transparent
encryptedStorage.Put("file.txt", reader)
data, _ := encryptedStorage.Get("file.txt")
```

## Required Interfaces

You must implement these interfaces from `pkg/common/encryption.go`:

### Encrypter Interface

```go
type Encrypter interface {
    // Encrypt encrypts data from the reader
    Encrypt(ctx context.Context, plaintext io.Reader) (io.ReadCloser, error)

    // Decrypt decrypts data from the reader
    Decrypt(ctx context.Context, ciphertext io.Reader) (io.ReadCloser, error)

    // Algorithm returns the encryption algorithm identifier
    Algorithm() string

    // KeyID returns the key identifier used for encryption
    KeyID() string
}
```

### EncrypterFactory Interface

```go
type EncrypterFactory interface {
    // GetEncrypter returns an encrypter for the specified key ID
    // If keyID is empty, uses the default key
    GetEncrypter(keyID string) (Encrypter, error)

    // DefaultKeyID returns the default key ID for new encryptions
    DefaultKeyID() string

    // Close releases any resources held by the factory
    Close() error
}
```

## Implementation Options

You can use **any** key management solution by implementing the interfaces above:

Use [go-keychain](https://github.com/jeremyhahn/go-keychain) which provides enterprise-grade key management with support for:
- **PKCS#8** - Software-based key files
- **PKCS#11** - Hardware security modules (HSMs)
- **TPM 2.0** - Trusted platform module
- **AWS KMS** - Amazon Key Management Service
- **GCP KMS** - Google Cloud Key Management
- **Azure Key Vault** - Microsoft key vault
- **HashiCorp Vault** - Multi-cloud secrets management

```go
import "github.com/jeremyhahn/go-keychain"

// Create keychain with your configuration
keychainConfig := &keychain.Config{
    DefaultKeyID: "primary",
    Keystores: []keychain.KeystoreConfig{
        {
            Type: "software",
            Config: map[string]interface{}{
                "storage_type": "memory",
            },
        },
    },
}

kc, err := keychain.New(keychainConfig)
if err != nil {
    return err
}

// Use keychain as EncrypterFactory
encryptedStorage := common.NewEncryptedStorage(storage, kc)
```

### Option 2: HashiCorp Vault

```go
// Your Vault client wrapper implementing EncrypterFactory
type VaultEncrypterFactory struct {
    client      *vaultapi.Client
    mountPath   string
    defaultKey  string
}

func (v *VaultEncrypterFactory) GetEncrypter(keyID string) (common.Encrypter, error) {
    return &VaultEncrypter{
        client:    v.client,
        mountPath: v.mountPath,
        keyName:   keyID,
    }, nil
}

func (v *VaultEncrypterFactory) DefaultKeyID() string {
    return v.defaultKey
}

// Use your implementation
vaultFactory := myapp.NewVaultEncrypterFactory(config)
encryptedStorage := common.NewEncryptedStorage(storage, vaultFactory)
```

### Option 3: Cloud KMS (AWS/GCP/Azure)

```go
// Your KMS client wrapper implementing EncrypterFactory
type KMSEncrypterFactory struct {
    kmsClient   *kms.Client
    defaultKey  string
}

func (k *KMSEncrypterFactory) GetEncrypter(keyID string) (common.Encrypter, error) {
    return &KMSEncrypter{
        client: k.kmsClient,
        keyID:  keyID,
    }, nil
}

// Use your implementation
kmsFactory := myapp.NewKMSEncrypterFactory(config)
encryptedStorage := common.NewEncryptedStorage(storage, kmsFactory)
```

### Option 4: Custom Implementation

Implement your own key management:

```go
type CustomEncrypter struct {
    keyID     string
    key       []byte
    algorithm string
}

func (c *CustomEncrypter) Encrypt(ctx context.Context, plaintext io.Reader) (io.ReadCloser, error) {
    // Your encryption logic using crypto/aes, crypto/cipher, etc.
    // Return encrypted data as io.ReadCloser
}

func (c *CustomEncrypter) Decrypt(ctx context.Context, ciphertext io.Reader) (io.ReadCloser, error) {
    // Your decryption logic
}

func (c *CustomEncrypter) Algorithm() string {
    return c.algorithm
}

func (c *CustomEncrypter) KeyID() string {
    return c.keyID
}

type CustomFactory struct {
    keys       map[string]*CustomEncrypter
    defaultKey string
}

func (f *CustomFactory) GetEncrypter(keyID string) (common.Encrypter, error) {
    if keyID == "" {
        keyID = f.defaultKey
    }
    encrypter, ok := f.keys[keyID]
    if !ok {
        return nil, fmt.Errorf("key not found: %s", keyID)
    }
    return encrypter, nil
}

func (f *CustomFactory) DefaultKeyID() string {
    return f.defaultKey
}

func (f *CustomFactory) Close() error {
    return nil
}
```

## Key Rotation

Your `EncrypterFactory` implementation handles key rotation. Support multiple keys to enable rotation:

```go
type RotatingFactory struct {
    encrypters  map[string]common.Encrypter
    defaultKey  string
}

func (f *RotatingFactory) GetEncrypter(keyID string) (common.Encrypter, error) {
    if keyID == "" {
        keyID = f.defaultKey
    }
    enc, ok := f.encrypters[keyID]
    if !ok {
        return nil, fmt.Errorf("key not found: %s", keyID)
    }
    return enc, nil
}

func (f *RotatingFactory) DefaultKeyID() string {
    return f.defaultKey
}

// Add new key
func (f *RotatingFactory) AddKey(keyID string, encrypter common.Encrypter) {
    f.encrypters[keyID] = encrypter
}

// Rotate to new default key
func (f *RotatingFactory) RotateKey(newDefaultKey string) error {
    if _, ok := f.encrypters[newDefaultKey]; !ok {
        return fmt.Errorf("new key not found: %s", newDefaultKey)
    }
    f.defaultKey = newDefaultKey
    return nil
}
```

**Strategy:**
1. Add new key to factory
2. Change default key ID
3. New objects encrypted with new key
4. Existing objects decrypted with original key (stored in metadata)
5. Optional: Re-encrypt old objects via batch job

## Metadata Storage

The `encryptedStorage` wrapper automatically stores encryption metadata with each object:
- `encryption_key_id` - Key identifier used for encryption
- `encryption_algorithm` - Algorithm used (from `Encrypter.Algorithm()`)

This metadata enables:
- Automatic key selection during decryption
- Key rotation support
- Audit trail of encryption

## Algorithms

Your `Encrypter` implementation determines the algorithm. Common choices:

### Symmetric (Recommended)
- **AES-GCM** (128, 192, 256-bit keys)
- **ChaCha20-Poly1305**
- **XChaCha20-Poly1305**

### Why GCM/Poly1305?
- Authenticated encryption (AEAD)
- Prevents tampering
- Efficient performance
- Hardware acceleration

## Security Best Practices

### Key Protection
- Never commit keys to version control
- Use environment variables or secure vaults
- Prefer cloud KMS over software keys in production
- Rotate keys periodically (annually or more)
- Use HSMs for high-security requirements

### Access Control
- Limit key access to minimum required services
- Use IAM roles for cloud KMS
- Monitor key usage
- Enable key audit logging
- Separate keys by environment (dev/staging/prod)

## Complete Example

```go
package main

import (
    "github.com/jeremyhahn/go-objstore/pkg/common"
    "github.com/jeremyhahn/go-objstore/pkg/factory"
    "github.com/jeremyhahn/go-keychain"
)

func main() {
    // 1. Create storage backend
    storage, err := factory.NewStorage("s3", map[string]string{
        "bucket": "my-secure-bucket",
        "region": "us-east-1",
    })
    if err != nil {
        panic(err)
    }

    // 2. Set up encryption with go-keychain
    keychainConfig := &keychain.Config{
        DefaultKeyID: "prod-2024",
        Keystores: []keychain.KeystoreConfig{
            {
                Type: "awskms",
                Config: map[string]interface{}{
                    "region": "us-east-1",
                    "key_id": os.Getenv("AWS_KMS_KEY_ID"),
                },
            },
        },
    }

    kc, err := keychain.New(keychainConfig)
    if err != nil {
        panic(err)
    }
    defer kc.Close()

    // 3. Wrap with encryption
    encryptedStorage := common.NewEncryptedStorage(storage, kc)

    // 4. Use normally - encryption is transparent!
    err = encryptedStorage.Put("sensitive-data.txt", dataReader)
    if err != nil {
        panic(err)
    }

    // Data is encrypted at rest, decrypted automatically on read
    decryptedData, err := encryptedStorage.Get("sensitive-data.txt")
    if err != nil {
        panic(err)
    }
    defer decryptedData.Close()

    // Use decrypted data...
}
```

## Performance Considerations

- Encryption overhead is typically negligible compared to network/disk I/O
- Streaming design keeps memory usage constant regardless of object size
- Hardware acceleration (AES-NI) provides multi-gigabit throughput
- Consider async encryption for very high-throughput scenarios
