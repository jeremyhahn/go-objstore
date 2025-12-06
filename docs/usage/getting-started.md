# Getting Started with go-objstore

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
- `s3` - Amazon S3 or S3-compatible
- `gcs` - Google Cloud Storage
- `azure` - Azure Blob Storage
- `minio` - On-premises object storage

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

Check errors returned from all operations. Common errors include:
- Object not found
- Access denied
- Network failures
- Storage full

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
