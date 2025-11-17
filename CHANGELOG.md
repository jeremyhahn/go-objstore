# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0-alpha] - 2025-11-14

### Added

- **Core Storage Interface**: Unified Storage interface supporting multiple backend implementations
- **Multiple Storage Backends**:
  - Local filesystem storage for development and testing
  - Amazon S3 backend for AWS object storage
  - MinIO backend for self-hosted S3-compatible storage
  - Google Cloud Storage (GCS) backend for Google Cloud
  - Azure Blob Storage backend for Microsoft Azure
  - AWS Glacier for long-term cold storage archival
  - Azure Archive for Azure long-term archival

- **Advanced Features**:
  - Context support for all operations (cancellation and timeouts)
  - Metadata support with custom key-value pairs
  - Lifecycle policies for automatic deletion and archival
  - Pagination support for listing large object collections
  - Directory operations and filesystem abstraction via StorageFS
  - Pluggable adapters for custom logging and authentication

- **Replication System**:
  - Replication policies with configurable source and destination backends
  - Transparent replication mode (decrypt-copy-encrypt)
  - Opaque replication mode (copy encrypted blobs as-is)
  - Three-layer encryption support (backend at-rest, source DEK, destination DEK)
  - Change detection via metadata (ETag and LastModified)
  - Incremental sync with JSONL-based change log
  - Parallel worker support for high-performance replication
  - File system watcher for real-time change detection
  - Background sync with ticker-based scheduling

- **Server Implementations**:
  - gRPC server for service-to-service communication
  - REST API server with HTTP endpoints
  - QUIC/HTTP3 server for next-generation protocol support
  - MCP (Model Context Protocol) server for AI integration
  - Complete API parity across all server protocols (100%)

- **CLI Tool** (`objstore`):
  - Object storage and retrieval commands
  - Support for all backends via configuration
  - Configuration via YAML files, environment variables, or command-line flags
  - Data piping between backends for migration and backup
  - Flexible stdin/stdout support for shell integration
  - Local and remote operation support (--server flag)
  - Complete gRPC, REST, and QUIC client implementations
  - Replication policy management commands

- **C API**:
  - Shared library (`libobjstore`) for embedding in C/C++ applications
  - C language bindings for all core functionality
  - Compiled as dynamic library for flexibility and multiple language bindings

- **Protocol Buffers**:
  - gRPC protocol definitions for service communication
  - Efficient binary serialization and schema evolution support

- **Documentation**:
  - Comprehensive README with quick start guide
  - API reference documentation
  - Backend configuration guides
  - StorageFS filesystem abstraction guide
  - Lifecycle policies documentation
  - Replication user guide (856 lines)
  - C API reference
  - Testing guide with coverage metrics
  - Getting started tutorials

- **Testing**:
  - Comprehensive unit tests with 92% code coverage
  - Integration tests for all backends using Docker
  - Integration tests for all server implementations
  - Test fixtures and helpers for consistent testing
  - CI/CD workflow support

- **Build System**:
  - Makefile with convenient build and test targets
  - Modular build configuration with optional features
  - Docker and docker-compose for development and testing
  - Support for building CLI, server, and library binaries

- **Examples**:
  - Runnable Go examples for all major features
  - C client examples with C API usage
  - StorageFS filesystem examples

### Security

- TLS/mTLS support for secure gRPC communication
- Secure credential handling for cloud backends
- Context-aware timeout and cancellation for resource cleanup

[0.1.0-alpha]: https://github.com/jeremyhahn/go-objstore/releases/tag/v0.1.0-alpha