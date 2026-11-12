# Getting Started with go-objstore

> **Status: NORMATIVE** · Scope: go-objstore · Last verified: 2026-11-12
> Conflicts with this document are defects. Fix them here first.

Quick start guide for using go-objstore in your applications.

## Installation

Install the library:

```bash
go get github.com/jeremyhahn/go-objstore
```

## Basic Concepts

### Storage Interface
All backends implement the `Storage` interface with methods for Put, Get, Delete, and List operations.

### Facade Pattern (Recommended)
The `objstore` package provides a centralized API that handles validation, backend routing, and error handling. Initialize once at startup, then use throughout your application:

```go
import "github.com/jeremyhahn/go-objstore/pkg/objstore"

// Initialize at startup
err := objstore.Initialize(&objstore.FacadeConfig{
    BackendConfigs: map[string]objstore.BackendConfig{
        "default": {Type: "local", Settings: map[string]string{"path": "/data"}},
    },
    DefaultBackend: "default",
})

// Use throughout application
err = objstore.Put("myfile.txt", dataReader)
data, err := objstore.Get("myfile.txt")
```

### Factory Pattern
For advanced use cases, create storage backends directly with the factory and pass them to the facade.

### Context Support
Operations support context for cancellation and timeouts.

## First Steps

### Choose a Backend
Select a storage backend based on your needs:
- `local` - Development and testing
- `memory` - In-process storage for tests and caches
- `s3` - Amazon S3 or S3-compatible (build tag `awss3`)
- `gcs` - Google Cloud Storage (build tag `gcpstorage`)
- `azure` - Azure Blob Storage (build tag `azureblob`)

See [Storage Backends](../backends/README.md) for each backend's settings and build tags.

### Create Storage Instance
Use the factory with backend type and configuration map.

### Perform Operations
Store, retrieve, list, and delete objects using the Storage interface.

## Common Operations

### Storing Objects
Write data to storage using Put or PutWithContext. Pass an io.Reader for the data source.

### Retrieving Objects
Read data from storage using Get or GetWithContext. Returns an io.ReadCloser that must be closed after use.

### Listing Objects
List objects with a prefix filter using List or ListWithOptions for advanced features.

### Deleting Objects
Remove objects using Delete or DeleteWithContext.

### Working with Metadata
Store and retrieve custom metadata alongside objects using PutWithMetadata and GetMetadata.

## Error Handling

Errors are typed sentinels; compare them with `errors.Is`, never by their text:
- `factory.ErrUnknownBackend` - the backend ID is not registered (unknown, or built without its tag)
- `factory.ErrArchiveOnlyBackend` - `glacier` and `azurearchive` are archive destinations, not storage
- `common.ErrCredentialInSettings` - a credential was put in the settings map instead of `common.Credentials`
- `common.ErrPathNotSet`, `common.ErrBucketNotSet`, `common.ErrAccountNotSet` - a required setting is missing
- `common.ErrKeyNotFound` - no object at the requested key

See [Error Handling](../backends/README.md#error-handling) for an example.

## Best Practices

### Close Readers
Always close readers returned by Get operations to release resources.

### Use Context
Pass context to operations for proper timeout and cancellation handling.

### Handle Errors
Check and handle errors from all storage operations.

### Test Locally
Use local backend during development for fast iteration.

### Configure Timeouts
Set appropriate timeouts based on object sizes and network conditions.

## Next Steps

### Advanced Features
- Enable encryption for data at rest
- Configure lifecycle policies for automatic cleanup
- Use StorageFS for filesystem-like operations
- Set up servers for remote access

### Production Deployment
- Configure cloud backend credentials
- Enable TLS for secure communication
- Set up monitoring and metrics
- Implement proper error handling and retry logic

### Integration
- Connect to existing cloud infrastructure
- Integrate with application frameworks
- Add custom authentication
- Configure logging adapters

Refer to specific usage guides for detailed information on each feature.
