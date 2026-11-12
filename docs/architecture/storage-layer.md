# Storage Layer Architecture

> **Status: NORMATIVE** · Scope: go-objstore · Last verified: 2026-11-12
> Conflicts with this document are defects. Fix them here first.

The storage layer provides a unified interface for object storage operations across different backends. This document describes the interface design, backend implementations, and key concepts.

## Storage Interface

The `Storage` interface embeds `LifecycleManager` and has 18 methods: 15 declared in `pkg/common/storage.go` and the 3 of the embedded `LifecycleManager` (`pkg/common/lifecycle.go`). Besides `Configure`, they fall into five categories, and each method is listed once:

### Configuration
- `Configure` - Initialize backend with settings and credentials

### Basic Operations
- `Put` - Store an object
- `Get` - Retrieve an object
- `Delete` - Remove an object
- `List` - List objects with a prefix

### Context-Aware Operations
Context-aware versions of basic operations that support cancellation and timeouts:
- `PutWithContext`
- `GetWithContext`
- `DeleteWithContext`
- `ListWithContext`

### Metadata Operations
- `PutWithMetadata` - Store an object with metadata
- `GetMetadata` - Retrieve only object metadata
- `UpdateMetadata` - Modify existing metadata

### Lifecycle Management (Embedded)
- `AddPolicy` - Add a retention or archival policy
- `RemovePolicy` - Remove an existing policy
- `GetPolicies` - Retrieve all policies

### Advanced Operations
- `Exists` - Check if an object exists without retrieving it
- `ListWithOptions` - Advanced listing with pagination, delimiters, and filtering
- `Archive` - Copy an object to an `Archiver` destination

All backends implement the complete interface, ensuring consistent behavior across different storage systems.

## Backend Implementations

### Local Filesystem
Stores objects as files in a directory tree. Simple, fast, and useful for development and testing. Supports all operations including metadata stored in extended attributes or sidecar files.

### Amazon S3
Integrates with AWS S3 and S3-compatible services through the `endpoint` option. Uses the AWS SDK for Go v2. Supports all S3 features including metadata, multipart uploads, and lifecycle policies.

### Google Cloud Storage
Integrates with GCS using the official Google Cloud client library. Supports GCS-specific features like storage classes and Cloud IAM integration.

### Azure Blob Storage
Integrates with Azure Blob Storage using the Azure SDK for Go. Supports blob metadata, access tiers, and Azure-specific authentication methods.

### Archive Backends
AWS Glacier and Azure Archive are append-only backends used as lifecycle policy destinations. Objects written to these backends cannot be retrieved immediately and have different cost characteristics.

They are not primary storage. `factory.NewStorage` refuses the types `glacier` and `azurearchive` with `factory.ErrArchiveOnlyBackend`, and `factory.ListStorageBackends` and `factory.IsStorageBackendRegistered` leave them out. They are reached only as `Archiver` destinations, through `factory.NewArchiver` (registered with `factory.RegisterArchiver`).

### Build tags
The factory registers a backend only when its file is compiled in (`pkg/factory/factory_*.go`):
- `local` - always present, no tag (storage and archiver)
- `memory` - always present, no tag
- `s3` - build tag `awss3`
- `gcs` - build tag `gcpstorage`
- `azure` - build tag `azureblob`
- `glacier` - build tag `glacier` (archiver only)
- `azurearchive` - build tag `azurearchive` (archiver only)

Leaving a tag out reduces binary size and attack surface; the factory then reports the type as not registered.

## Key Concepts

### Object Keys
All backends use string keys to identify objects. Keys can contain path separators to create hierarchical namespaces, though the underlying storage is flat.

### Metadata
Each object can have associated metadata:
- Content-Type (MIME type)
- Content-Encoding (compression applied)
- Size in bytes
- Last modified timestamp
- ETag for versioning and caching
- Custom key-value pairs

### Readers and Writers
Read operations return `io.ReadCloser` interfaces. Callers must close readers to release resources. Write operations accept `io.Reader` interfaces, allowing streaming of large objects without loading them fully into memory.

### Context Support
All context-aware operations honor context cancellation and deadlines. This allows proper timeout handling and request cancellation across network boundaries.

### Error Handling
Backends return, or wrap, the sentinel errors of `pkg/common/errors.go`; a caller matches them with `errors.Is`:
- `ErrKeyNotFound` - the object does not exist
- `ErrMetadataNotFound` - the object has no stored metadata
- `ErrAlreadyExists` - the object or resource already exists
- `ErrInvalidArgument` - a key, setting or parameter is invalid
- `ErrPermissionDenied` - the backend refused the operation
- `ErrUnauthenticated` - the credentials were missing or rejected
- `ErrResourceExhausted` - a quota or rate limit was reached
- `ErrUnavailable` - the backend could not be reached; the call may be retried
- `ErrNotSupported` - the backend does not implement the operation
- `ErrNotConfigured` - the backend was used before `Configure` succeeded

Other failures come back as backend-specific errors, wrapped with context.

## Thread Safety

All backend implementations are safe for concurrent use from multiple goroutines. Internal locking protects shared state where necessary.

## Performance Characteristics

### Local Backend
- Fastest for single-machine deployments
- Limited by disk I/O
- No network overhead

### Cloud Backends (S3, GCS, Azure)
- Network latency affects all operations
- Bandwidth-limited for large objects
- Support concurrent operations for parallelism
- May have rate limits depending on configuration

### Archive Backends
- Write-only from application perspective
- Retrieval requires restoration period
- Lowest cost per GB
- Not suitable for frequently accessed data

## Facade Pattern

The recommended way to use go-objstore is through the `objstore` facade package. The facade provides:
- Centralized API with input validation
- Multi-backend routing via `backend:key` syntax
- Security validation for all inputs
- Simple initialization

### Simplified Initialization (Recommended)

The facade can create backends internally via the factory:

```go
import "github.com/jeremyhahn/go-objstore/pkg/objstore"

err := objstore.Initialize(&objstore.FacadeConfig{
    BackendConfigs: map[string]objstore.BackendConfig{
        "default": {
            Type:     "local",
            Settings: map[string]string{"path": "/data/storage"},
        },
        "archive": {
            Type:     "s3",
            Settings: map[string]string{"bucket": "my-archive"},
        },
    },
    DefaultBackend: "default",
})
```

### Using the Facade API

After initialization, use the facade functions directly:

```go
// Store an object (uses default backend)
objstore.Put("key.txt", reader)

// Retrieve an object
data, _ := objstore.Get("key.txt")

// Use specific backend with backend:key syntax
objstore.PutWithContext(ctx, "archive:backup.tar", reader)
objstore.GetWithContext(ctx, "archive:backup.tar")
```

## Factory Pattern (Internal)

The factory is now primarily used internally by the facade. Direct factory usage is still supported for legacy code or advanced use cases:

```go
import (
    "github.com/jeremyhahn/go-objstore/pkg/factory"
    "github.com/jeremyhahn/go-objstore/pkg/objstore"
    "github.com/jeremyhahn/go-objstore/pkg/common"
)

// Create storage manually
storage, _ := factory.NewStorage("local", map[string]string{"path": "/data"}, common.Credentials{})

// Initialize facade with pre-configured storage
objstore.Initialize(&objstore.FacadeConfig{
    Backends: map[string]common.Storage{"default": storage},
    DefaultBackend: "default",
})
```

The factory handles:
- Backend selection based on type string
- Configuration parsing and validation
- Backend initialization with proper credentials
- Error handling for missing dependencies

## Consumers: the go-qrdb storage provider

SPEC row `qrdb-storage-provider` (planned). When object storage is configured as a go-qrdb backend storage provider, go-qrdb's storage API uses the go-objstore `Storage` interface and its `local`, `s3`, `gcs` and `azure` backends. `glacier` and `azurearchive` (the SPEC row writes `azure-archive`) serve as archive destinations only, through `Archiver`, never as `Storage`.

- go-qrdb is the provider seam and consumes go-objstore. go-objstore takes no go-qrdb dependency and keeps go-enclave its only first-party dependency.
- go-quicbaas chooses which go-objstore backend each configuration uses.
- go-objstore keeps the interface above and the backend configuration it already documents: [backends](../backends/README.md) and [storage backend configuration](../configuration/storage-backends.md).

A consumer may rely on the following, for every backend:
- **Typed errors**: the sentinels under [Error Handling](#error-handling), matched with `errors.Is`.
- **Context cancellation**: the `...WithContext` methods and `ListWithOptions` honor cancellation and deadlines of the context passed in.
- **Concurrent use**: a configured backend is safe for use from multiple goroutines (see [Thread Safety](#thread-safety)).
- **Readers it must close**: `Get` and `GetWithContext` return an `io.ReadCloser`; the consumer closes it to release the connection or file handle.
