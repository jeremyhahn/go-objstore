# Storage Layer Architecture

The storage layer provides a unified interface for object storage operations across different backends. This document describes the interface design, backend implementations, and key concepts.

## Storage Interface

The `Storage` interface defines 14 methods organized into four categories:

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

### Advanced Operations
- `Exists` - Check if an object exists without retrieving it
- `ListWithOptions` - Advanced listing with pagination, delimiters, and filtering
- `Archive` - Move an object to archival storage

All backends implement the complete interface, ensuring consistent behavior across different storage systems.

## Backend Implementations

### Local Filesystem
Stores objects as files in a directory tree. Simple, fast, and useful for development and testing. Supports all operations including metadata stored in extended attributes or sidecar files.

### Amazon S3
Integrates with AWS S3 and S3-compatible services like MinIO. Uses the AWS SDK for Go v2. Supports all S3 features including metadata, multipart uploads, and lifecycle policies.

### Google Cloud Storage
Integrates with GCS using the official Google Cloud client library. Supports GCS-specific features like storage classes and Cloud IAM integration.

### Azure Blob Storage
Integrates with Azure Blob Storage using the Azure SDK for Go. Supports blob metadata, access tiers, and Azure-specific authentication methods.

### MinIO
While technically S3-compatible, MinIO is often used as a separate backend for on-premises deployments. Uses the same implementation as S3 backend.

### Archive Backends
AWS Glacier and Azure Archive are append-only backends used as lifecycle policy destinations. Objects written to these backends cannot be retrieved immediately and have different cost characteristics.

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
Backends return typed errors for common conditions:
- Object not found
- Access denied
- Storage full
- Network errors
- Backend-specific errors

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

## Factory Pattern

The factory abstracts backend creation through a simple interface:

```
storage := factory.NewStorage(backendType, configMap)
```

The factory handles:
- Backend selection based on type string
- Configuration parsing and validation
- Backend initialization with proper credentials
- Error handling for missing dependencies

This allows application code to remain independent of specific backend implementations.

## Build Tags

Backends are conditionally compiled using build tags:
- `local` - Local filesystem backend
- `awss3` - Amazon S3 backend
- `gcs` - Google Cloud Storage backend
- `azureblob` - Azure Blob Storage backend
- `minio` - MinIO backend

This reduces binary size when only specific backends are needed and reduces attack surface.
