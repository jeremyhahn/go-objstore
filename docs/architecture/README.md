# Architecture Overview

go-objstore is a unified object storage abstraction library built around a consistent interface that works across multiple storage backends. The architecture emphasizes simplicity, extensibility, and testing.

## Core Components

### Storage Layer
The foundation is the `Storage` interface, which defines 14 methods for object operations. All backends implement this interface completely, ensuring consistent behavior whether you're using local filesystem or cloud storage.

Backends include local filesystem, Amazon S3, Google Cloud Storage, Azure Blob Storage, MinIO, AWS Glacier, and Azure Archive.

[Read more about the storage layer](storage-layer.md)

### Server Implementations
Multiple server implementations provide different protocols for accessing the storage layer:

- gRPC server for high-performance RPC with Protocol Buffers
- REST API server using HTTP/JSON
- QUIC server for HTTP/3 over QUIC
- MCP server for AI model integrations

All servers support TLS/mTLS and pluggable authentication.

[Read more about servers](servers.md)

### StorageFS
A filesystem abstraction layer that wraps any storage backend with familiar filesystem operations. Implements file and directory operations similar to Go's `os` package, making object storage feel like a traditional filesystem.

[Read more about StorageFS](storagefs.md)

### Encryption
Optional at-rest encryption using go-keychain for key management. Supports AES-GCM encryption with multiple key sizes and backends including software keystores and hardware security modules.

[Read more about encryption](encryption.md)

### Lifecycle Management
Automatic data retention and archival policies. Define rules for deleting old objects or moving them to cheaper archival storage based on age and prefix patterns.

[Read more about lifecycle policies](lifecycle.md)

## Design Principles

### Interface-Based Design
Everything implements well-defined interfaces. Storage backends implement `Storage`, servers implement `Server`, and adapters implement specific interfaces for logging, authentication, and TLS.

### Facade Pattern
The `objstore` package provides a centralized API for all storage operations. The facade pattern:
- Provides a single entry point for all storage operations
- Handles input validation and sanitization at a single point
- Routes operations to the appropriate backend using `backend:key` syntax
- Ensures consistent error handling across all access methods

Initialize the facade once at application startup, then use `objstore.*` functions throughout your code.

### Factory Pattern
The factory pattern abstracts backend creation. The facade uses the factory internally when you provide `BackendConfigs`. For advanced use cases, you can also create backends directly with the factory and pass them to the facade.

### Pluggable Adapters
Custom logging, authentication, and TLS configuration are injected through adapter interfaces. This allows integration with existing systems without modifying core code.

### Comprehensive Testing
All functionality is tested at both unit and integration levels. Integration tests use Docker to spin up real services (MinIO, Azurite, GCS emulator) for testing against actual implementations.

## Data Flow

### Write Operation
1. Client calls `Put()` or `PutWithContext()` on storage interface
2. Optional encryption wraps the data stream
3. Backend-specific implementation writes to underlying storage
4. Metadata is stored alongside the object
5. Lifecycle policies are evaluated if configured

### Read Operation
1. Client calls `Get()` or `GetWithContext()` on storage interface
2. Backend retrieves object from underlying storage
3. Optional decryption unwraps the data stream
4. Reader is returned to client
5. Client is responsible for closing the reader

### List Operation
1. Client calls `List()` or `ListWithOptions()` with prefix and options
2. Backend queries underlying storage for matching objects
3. Results are paginated if requested
4. List of object keys (and optionally metadata) is returned

## Component Dependencies

The architecture maintains clear dependency boundaries:

- Core interfaces in `pkg/common` have no external dependencies
- Storage backends depend only on `common` and their SDK
- Servers depend on `common` and their protocol library
- StorageFS depends on `common` and storage implementations
- Factory depends on all backends but clients depend only on factory

This allows selective compilation with build tags and keeps the core library lightweight.
