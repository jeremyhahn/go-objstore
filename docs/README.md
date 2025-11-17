# go-objstore Documentation

Complete documentation for go-objstore, a unified object storage abstraction library for Go.

## Documentation Structure

Documentation is organized into three main sections:

### [Architecture](architecture/)
Understand the design and components of go-objstore.

- [Overview](architecture/README.md) - High-level architecture
- [Storage Layer](architecture/storage-layer.md) - Unified storage interface and backends
- [Servers](architecture/servers.md) - gRPC, REST, QUIC, and MCP servers
- [StorageFS](architecture/storagefs.md) - Filesystem abstraction layer
- [Encryption](architecture/encryption.md) - At-rest encryption system
- [Lifecycle Management](architecture/lifecycle.md) - Automatic retention and archival

### [Configuration](configuration/)
Configure go-objstore components for your environment.

- [Overview](configuration/README.md) - Configuration methods and patterns
- [Storage Backends](configuration/storage-backends.md) - Local, S3, GCS, Azure, MinIO
- [gRPC Server](configuration/grpc-server.md) - gRPC server configuration
- [REST Server](configuration/rest-server.md) - REST API server configuration
- [QUIC Server](configuration/quic-server.md) - QUIC/HTTP3 server configuration
- [MCP Server](configuration/mcp-server.md) - Model Context Protocol server
- [Encryption](configuration/encryption.md) - Encryption and key management
- [Lifecycle Policies](configuration/lifecycle.md) - Data retention and archival
- [CLI Tool](configuration/cli.md) - Command-line interface

### [Usage](usage/)
Learn how to use go-objstore in your applications.

- [Getting Started](usage/getting-started.md) - Quick start guide
- [Storage Backends](usage/storage-backends.md) - Using different backends
- [CLI Tool](usage/cli.md) - Command-line usage
- [Deployment](usage/deployment.md) - Production deployment guide

Additional usage guides for servers, StorageFS, encryption, lifecycle, and C API are available in the examples directory.

## Quick Links

### Getting Started
New to go-objstore? Start with:
1. [Getting Started Guide](usage/getting-started.md)
2. [Architecture Overview](architecture/README.md)
3. [Storage Backend Configuration](configuration/storage-backends.md)

### Common Tasks
- [Configure a storage backend](configuration/storage-backends.md)
- [Run a server](configuration/grpc-server.md)
- [Enable encryption](configuration/encryption.md)
- [Set up lifecycle policies](configuration/lifecycle.md)
- [Deploy to production](usage/deployment.md)

### Reference
- [Storage Interface](architecture/storage-layer.md) - Complete interface documentation
- [Server Architecture](architecture/servers.md) - Server implementation details
- [Configuration Patterns](configuration/README.md) - Best practices

## Examples

Working code examples are available in the [`examples/`](../examples/) directory:

- Basic Usage - CRUD operations and common patterns
- Lifecycle Policies - Automatic data management
- StorageFS - Filesystem interface
- Encryption - At-rest encryption
- Servers - gRPC, REST, QUIC, MCP
- C API - C/C++ integration

See [examples/README.md](../examples/README.md) for full list.

## API Documentation

Go package documentation:
```bash
go doc github.com/jeremyhahn/go-objstore/pkg/common
```

Protocol documentation:
- gRPC: [api/proto/objstore.proto](../api/proto/objstore.proto)
- REST: [api/openapi/objstore.yaml](../api/openapi/objstore.yaml)
- MCP: [api/mcp/](../api/mcp/)

## Project Information

### Repository
https://github.com/jeremyhahn/go-objstore

### License
AGPL-3.0 - See [LICENSE](../LICENSE)

### Contributing
See main [README.md](../README.md) for contribution guidelines.

### Support
- GitHub Issues: https://github.com/jeremyhahn/go-objstore/issues
- Documentation: This directory
- Examples: [examples/](../examples/)

## Testing

### Running Tests
```bash
# Unit tests
make test

# Integration tests
make integration-test

# Coverage report
make coverage-report
```

See [testing documentation](testing.md) for details.

## Building

### Library
```bash
make build
```

### CLI Tool
```bash
make cli
```

### Servers
```bash
make server-grpc
make server-rest
make server-quic
make server-mcp
```

### C Library
```bash
make lib
```

## Development

### Project Structure
```
go-objstore/
├── pkg/              # Core packages
│   ├── common/       # Shared interfaces
│   ├── factory/      # Backend factory
│   ├── local/        # Local backend
│   ├── s3/           # S3 backend
│   ├── gcs/          # GCS backend
│   ├── azure/        # Azure backend
│   ├── storagefs/    # Filesystem abstraction
│   ├── encryption/   # Encryption support
│   └── server/       # Server implementations
├── cmd/              # Command-line tools
├── api/              # API definitions
├── examples/         # Usage examples
├── docs/             # Documentation
└── test/             # Integration tests
```

### Development Workflow
1. Make changes
2. Run tests: `make test`
3. Run integration tests: `make integration-test`
4. Build: `make build`
5. Test manually with examples
6. Submit pull request

## Additional Resources

### Cloud Provider Setup
- AWS S3: AWS documentation
- Google Cloud Storage: GCP documentation
- Azure Blob Storage: Azure documentation

### Protocol Documentation
- gRPC: https://grpc.io/
- QUIC: https://quicwg.org/
- MCP: Model Context Protocol specification

### Related Projects
- go-keychain: Key management library
- Storage SDKs: AWS SDK, Google Cloud SDK, Azure SDK
