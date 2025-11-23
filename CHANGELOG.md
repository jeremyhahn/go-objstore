# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.2-alpha] - 2025-11-22

### Added

- Facade Pattern: Implemented comprehensive facade pattern for centralized object storage API
  - pkg/validation: Input validation preventing path traversal, injection attacks, and malformed input (98.2% test coverage)
  - pkg/objstore: Singleton facade with multi-backend support and backend:key routing syntax (57.7% test coverage)
  - Multi-backend support: Work with multiple storage backends simultaneously using backend:key syntax
  - Automatic validation: All inputs validated at facade layer to prevent security vulnerabilities
  - Security hardening: Blocks path traversal (.., ../), absolute paths, null bytes, control characters
  - Centralized error handling: Sanitized error messages prevent information disclosure
  - Thread-safe singleton pattern with Initialize/Reset for testing
  - Backend routing: Support for backend:key syntax (e.g., "s3:myfile.txt", "local:cache.dat")
  - Comprehensive documentation: Migration guide (docs/facade-migration.md) and working examples
  - Example code: Complete facade usage example in examples/facade-usage/main.go

### Changed

- README: Updated with facade pattern documentation and examples, showing both new facade pattern and legacy direct storage access
- Architecture: Introduced facade layer as recommended API while maintaining backward compatibility with direct storage access

### Security

- Input validation now enforces strict rules across all entry points
- Path traversal protection: Rejects .., ../, /.., /../ patterns
- Absolute path blocking: Prevents /etc/passwd, C:\Windows style attacks
- Null byte filtering: Blocks \x00 injection attempts
- Control character validation: Rejects \n, \r, \t in keys
- Backend name validation: Only lowercase alphanumeric and hyphens allowed
- Length limits: 1024 chars for keys, 64 for backend names
- Log injection prevention via SanitizeForLog function

## [0.1.1-alpha] - 2025-11-20

### Added

- FIPS 140-3 Builds: Added FIPS-compliant binary builds using Go 1.24's GOFIPS140 mode
  - FIPS builds available for Linux (amd64, arm64)
  - Binaries named with -fips suffix (e.g., objstore-fips)
  - NIST FIPS 140-3 cryptographic compliance for regulated environments
  - Release artifacts include both standard and FIPS variants

### Changed

- Release Workflow: Updated to only trigger on tags pushed from the main branch
  - CI continues to run on both main and develop branches
  - Release builds restricted to production branch for stability
- Go Version: Updated release builds to use Go 1.24 for FIPS 140-3 support

## [0.1.1-alpha] - 2025-11-20

### Fixed

- Dependency Compatibility: Downgraded github.com/quic-go/qpack from v0.6.0 to v0.5.1 to maintain compatibility with quic-go v0.56.0
- Build System: Removed restrictive //go:build local build tags from all files in pkg/local package, allowing the package to be included in normal builds without requiring explicit build tags
- Test Coverage: Updated Makefile test target to include all build tags (local, awss3, minio, gcpstorage, azureblob, glacier, azurearchive), ensuring comprehensive test coverage across all backend implementations
- Compilation Errors: Resolved compilation errors in pkg/replication/syncer.go and test files in pkg/server/quic caused by missing pkg/local package
- Linting Issues: Reduced linting errors from 290 to 64 (78% reduction)
  - Fixed all err113 errors by using errors.Is() instead of direct error comparisons
  - Fixed all errcheck errors by adding proper error handling for Close() calls
  - Fixed gocritic exitAfterDefer warnings by replacing os.Exit() with log.Fatal()
  - Fixed staticcheck SA9003 empty branch warnings
  - Fixed staticcheck SA1012 nil context warnings by using context.TODO()
  - Fixed staticcheck S1009 unnecessary nil checks before len()
  - Fixed staticcheck ST1011 variable naming issues
  - Added nolint directives for AWS SDK v1 deprecation warnings (migration to v2 planned)
- Code Quality: Extracted 35+ magic string constants across 25+ files to improve maintainability
  - Centralized repeated string literals (action types, content types, file names, etc.)
  - Eliminated typo risks from duplicated strings
  - Improved code readability with named constants

### Added

- Test Coverage: Added tests for GetReplicationStatusCommand and FormatReplicationStatus functions to improve CLI package coverage
- Package Coverage: Enabled testing for previously excluded packages:
  - pkg/minio: 94.4% coverage
  - pkg/azurearchive: 93.1% coverage
- Error Handling: Enhanced error handling across all packages
  - Proper error wrapping and checking throughout codebase
  - Consistent use of errors.Is() for error comparison
  - Deferred cleanup handlers with error checking

### Changed

- Test Coverage: Improved overall test coverage from 89.8% to 90.5%, exceeding the 90% coverage target
- CLI Package: Increased coverage from 85.1% to 91.5%
- Code Quality: Enhanced maintainability through systematic refactoring
  - Replaced magic strings with named constants across all packages
  - Improved error handling patterns
  - Better resource cleanup and context handling
- Dependencies: Updated golangci-lint configuration to enforce stricter code quality standards

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

[0.1.1-alpha]: https://github.com/jeremyhahn/go-objstore/releases/tag/v0.1.1-alpha
[0.1.0-alpha]: https://github.com/jeremyhahn/go-objstore/releases/tag/v0.1.0-alpha