# Encryption Architecture

go-objstore provides optional at-rest encryption for all stored data through a clean abstraction layer. The encryption layer is completely pluggable - you provide an implementation of the `Encrypter` and `EncrypterFactory` interfaces, allowing integration with any key management solution.

## Design Philosophy

**Dependency Inversion:** go-objstore defines encryption interfaces but provides NO concrete implementations. This design eliminates hard dependencies on specific key management systems.

**Pluggable Architecture:** You can use:
- [go-keychain](https://github.com/jeremyhahn/go-keychain) (recommended)
- HashiCorp Vault
- Cloud KMS services (AWS/GCP/Azure)
- Your own custom key management
- Any solution implementing the interfaces

## Design Overview

Encryption is implemented as a transparent wrapper around storage backends. Applications interact with the storage interface normally, while encryption and decryption happen automatically.

Key design principles:
- Transparent to application code
- Works with any storage backend
- Supports key rotation without data migration
- Zero hard dependencies on key management systems
- Consumer provides key management implementation
- Minimal performance overhead

## Components

### 1. Encrypted Storage Wrapper (`encryptedStorage`)

**Location:** `pkg/common/encrypted_storage.go`

The wrapper intercepts all storage operations transparently:
- On write: encrypts data before passing to backend
- On read: decrypts data after retrieving from backend
- Preserves all `Storage` interface methods
- Automatically stores/retrieves encryption metadata
- Delegates to your `EncrypterFactory` for key management

**Usage:**
```go
encryptedStorage := common.NewEncryptedStorage(underlyingStorage, yourEncrypterFactory)
```

### 2. Encrypter Interface

**Location:** `pkg/common/encryption.go`

```go
type Encrypter interface {
    Encrypt(ctx context.Context, plaintext io.Reader) (io.ReadCloser, error)
    Decrypt(ctx context.Context, ciphertext io.Reader) (io.ReadCloser, error)
    Algorithm() string  // e.g., "AES-256-GCM"
    KeyID() string      // Key identifier for this encrypter
}
```

**Your responsibility:** Implement this interface with your chosen encryption algorithm and key access method.

### 3. EncrypterFactory Interface

**Location:** `pkg/common/encryption.go`

```go
type EncrypterFactory interface {
    GetEncrypter(keyID string) (Encrypter, error)
    DefaultKeyID() string
    Close() error
}
```

**Your responsibility:** Implement this interface to:
- Manage multiple encryption keys
- Support key rotation
- Integrate with your key management system (KMS, Vault, etc.)
- Handle key retrieval and caching

### 4. Key Management (Your Implementation)

go-objstore **does not provide** key management. You must bring your own:

**Option A:** Use [go-keychain](https://github.com/jeremyhahn/go-keychain)
- Enterprise-grade key management
- Supports HSM, TPM, KMS, software keys
- Implements `EncrypterFactory` interface
- Drop-in ready

**Option B:** Integrate your existing system
- Implement interfaces with your Vault/KMS client
- Full control over key lifecycle
- No additional dependencies

**Option C:** Custom implementation
- Direct crypto operations with Go stdlib
- Your own key storage and rotation logic

**Option D:** Use Service Provider
- Configure encryption at the bucket level
- Use Terraform or IaaC to create bucket with encryption policy

## Encryption Process

### Writing Encrypted Data
1. Application calls `storage.Put(key, data)`
2. `encryptedStorage` wrapper intercepts the call
3. Wrapper calls `encrypterFactory.GetEncrypter(defaultKeyID)`
4. Calls `encrypter.Encrypt(ctx, data)` - your implementation does the encryption
5. Encrypted data written to underlying backend storage
6. Metadata automatically stored with:
   - `encryption_key_id` (from `encrypter.KeyID()`)
   - `encryption_algorithm` (from `encrypter.Algorithm()`)

### Reading Encrypted Data
1. Application calls `storage.Get(key)`
2. `encryptedStorage` wrapper intercepts the call
3. Wrapper retrieves metadata to determine which key was used
4. Calls `encrypterFactory.GetEncrypter(keyIDFromMetadata)`
5. Retrieves encrypted data from underlying storage
6. Calls `encrypter.Decrypt(ctx, encryptedData)` - your implementation does the decryption
7. Returns decrypted stream to application

### Data Flow Diagram

```
Application
    ↓
Storage Interface (Put/Get)
    ↓
encryptedStorage Wrapper
    ↓
EncrypterFactory.GetEncrypter()
    ↓
Your Encrypter Implementation
    ↓
Your Key Management System
    ↓
Underlying Storage Backend
```

## Key Management (Your Choice)

go-objstore is **key-management-agnostic**. Your `EncrypterFactory` determines where keys come from:

### Common Implementations

**go-keychain** (recommended for most use cases):
```go
import "github.com/jeremyhahn/go-keychain"

kc, _ := keychain.New(config)
encryptedStorage := common.NewEncryptedStorage(storage, kc)
```

**HashiCorp Vault:**
```go
vaultFactory := &YourVaultFactory{
    client: vaultClient,
    mountPath: "transit",
    defaultKey: "objstore-key",
}
encryptedStorage := common.NewEncryptedStorage(storage, vaultFactory)
```

**Cloud KMS:**
```go
kmsFactory := &YourKMSFactory{
    client: kmsClient,
    keyARN: "arn:aws:kms:...",
}
encryptedStorage := common.NewEncryptedStorage(storage, kmsFactory)
```

### Multiple Keys
Support for multiple encryption keys enables:
- Key rotation without data migration
- Different keys for different data classes
- Gradual migration between key types
- Compliance with key lifecycle policies

### Key Rotation
Rotating encryption keys:
1. Configure new key as default
2. New objects encrypted with new key
3. Existing objects retain original key
4. Decrypt old objects using original key identifier
5. Re-encrypt old objects on access or via batch job

## Algorithms and Modes

### Symmetric Encryption
Uses AES-GCM authenticated encryption:
- AES-128-GCM (128-bit keys)
- AES-192-GCM (192-bit keys)
- AES-256-GCM (256-bit keys)
- ChaCha20-Poly1305
- XChaCha20-Poly1305

GCM mode provides:
- Authenticated encryption
- Prevents tampering
- Efficient performance
- Parallelizable operations

### Data Format
Encrypted objects store:
- Nonce (random value, never reused)
- Authentication tag
- Ciphertext
- Metadata with key identifier

## Performance Considerations

### Overhead
Encryption adds minimal overhead:
- Streaming operation on data in flight
- No additional storage roundtrips
- Negligible latency for large objects
- Dominated by network or disk I/O

### Memory Usage
Encryption operates on streams:
- No requirement to buffer entire object
- Fixed memory regardless of object size
- Suitable for objects of any size

### Throughput
AES-GCM throughput:
- Hardware acceleration on modern CPUs
- Multi-gigabit throughput per core
- Rarely the bottleneck in storage operations

## Security Properties

### Confidentiality
Data encrypted at rest in storage backend. Only holders of encryption keys can decrypt. Keys never stored with encrypted data.

### Integrity
Authentication tags prevent undetected tampering. Any modification to ciphertext or metadata detected during decryption.

### Key Security
Keys managed by YOUR implementation:
- Software keys protected by OS permissions
- Hardware keys never leave secure element (HSM/TPM)
- Cloud KMS keys accessed via API
- All key material encrypted at rest
- Your key management system controls all security properties

### Threat Model
Protection against:
- Unauthorized access to storage backend
- Theft of storage media
- Compromise of backup systems
- Accidental exposure of data

Does not protect against:
- Compromise of encryption keys themselves
- Access through authorized application code
- Physical tampering with running systems
- Side-channel attacks on encryption operations

## Integration Points

### Programmatic API (Recommended)

Enable encryption by wrapping your storage:

```go
// Create storage backend
storage, err := factory.NewStorage("s3", settings)

// Create your encrypter factory (any implementation)
encrypterFactory := yourapp.NewEncrypterFactory(config)

// Wrap with encryption
encryptedStorage := common.NewEncryptedStorage(storage, encrypterFactory)

// Use transparently
encryptedStorage.Put("key", data)
```

### Factory Integration (Optional)

If you want to integrate encryption into the factory pattern:

```go
// In your application code
type EncryptedStorageCreator struct {
    encrypterFactory common.EncrypterFactory
}

func (c *EncryptedStorageCreator) Create(backendType string, settings map[string]string) (common.Storage, error) {
    // Create underlying storage
    storage, err := factory.NewStorage(backendType, settings)
    if err != nil {
        return nil, err
    }

    // Wrap with encryption
    return common.NewEncryptedStorage(storage, c.encrypterFactory), nil
}
```

### Benefits of Abstraction

- **No vendor lock-in:** Switch key management systems without changing go-objstore
- **Testability:** Mock `EncrypterFactory` in unit tests
- **Flexibility:** Use different key systems for different environments
- **Clean dependencies:** go-objstore has zero dependencies on key management systems

## Metadata Storage

Encryption metadata stored alongside objects:
- Key identifier used for encryption
- Algorithm and mode
- Key version or rotation generation
- Custom encryption parameters

Metadata enables:
- Decryption with correct key
- Audit of encryption coverage
- Key rotation tracking
- Compliance reporting
