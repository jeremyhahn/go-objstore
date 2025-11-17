# Integration Test Suite

Comprehensive Docker-based integration tests for all go-objstore interfaces including storage backends, CLI, and servers (gRPC, REST, QUIC, MCP).

## Overview

This test suite provides end-to-end testing of all go-objstore components in isolated Docker containers, ensuring:

- **Isolation**: Each test suite runs in its own container environment
- **Reproducibility**: Tests use consistent, versioned dependencies
- **Parallelization**: Tests can run concurrently without interference
- **Real Services**: Tests use actual service implementations (MinIO, Azurite, fake-gcs-server)

## Test Organization

```
test/integration/
├── common/          # Shared test utilities and helpers
├── local/           # Local filesystem backend tests
├── s3/              # AWS S3/MinIO backend tests
├── azure/           # Azure Blob/Azurite backend tests
├── gcs/             # Google Cloud Storage tests
├── factory/         # Factory pattern and common interface tests
├── cli/             # CLI integration tests
│   ├── docker-compose.yml
│   ├── Dockerfile
│   └── cli_test.go  # 17+ test scenarios
└── server/          # Server integration tests
    ├── docker-compose.yml
    ├── Dockerfile.*  # Separate Dockerfiles for each server
    ├── generate-certs.sh
    ├── grpc_test.go  # gRPC server tests (15+ scenarios)
    ├── rest_test.go  # REST API tests (20+ scenarios)
    ├── quic_test.go  # QUIC/HTTP3 tests (8+ scenarios)
    └── mcp_test.go   # MCP server tests (12+ scenarios)
```

## Prerequisites

- Docker (version 20.10+)
- Docker Compose (version 2.0+)
- Make
- Go 1.21+ (for local development)

## Quick Start

### Run All Integration Tests

```bash
make integration-test-all
```

This runs all integration tests:
- Storage backend tests (local, S3, Azure, GCS)
- Factory tests
- CLI tests
- Server tests (gRPC, REST, QUIC, MCP)

### Run Individual Test Suites

```bash
# Storage backend tests
make integration-test-local
make integration-test-s3
make integration-test-azure
make integration-test-gcs
make integration-test-factory

# CLI tests
make test-cli

# Server tests (all servers: gRPC, REST, QUIC, MCP)
make test-servers
```

## Test Suite Details

### 1. CLI Integration Tests (`test-cli`)

**Location**: `test/integration/cli/`

**Coverage**: 17+ test scenarios covering all CLI commands

**What's Tested**:
- `put` command: Simple files, large files, metadata, overwrites
- `get` command: Download to file, stdout, non-existent files
- `delete` command: Existing/non-existent objects
- `list` command: All objects, prefix filtering, pagination
- `exists` command: Existence checks with proper exit codes
- `config` command: Display current configuration
- Output formats: text, json, table
- Configuration sources: flags, environment variables, config files
- Error scenarios: Invalid inputs, missing files, bad backends
- Concurrent operations: Multiple simultaneous CLI calls
- Large files: 10MB+ file handling

**Run**:
```bash
make test-cli
```

### 2. Server Integration Tests (`test-servers`)

**Location**: `test/integration/server/`

**Coverage**: 55+ test scenarios across 4 server types

**What's Tested**:

#### gRPC Server (15+ tests)
- Health check endpoint
- Put operations: Simple, with metadata, large files, overwrites
- Get operations: Streaming retrieval, large files (10MB+)
- Delete operations: Existing/non-existent objects
- List operations: All, prefix filtering, delimiter, pagination
- Exists checks
- GetMetadata: Retrieve object metadata
- Error handling: Timeouts, cancelled contexts, invalid requests
- Concurrent operations: 50+ simultaneous requests

#### REST Server (20+ tests)
- Health check endpoint
- PUT /objects/:key: Simple, metadata, large files, multipart uploads
- GET /objects/:key: Download, range requests, accept-encoding
- HEAD /objects/:key: Metadata-only requests
- DELETE /objects/:key: Deletion with verification
- GET /objects: List with prefix, delimiter, pagination
- GET /metadata/:key: Metadata retrieval
- CORS support: Preflight requests, CORS headers
- Error handling: Invalid JSON, unsupported methods, request too large
- API versioning: /api/v1 paths, backwards compatibility
- Concurrent operations: 50+ simultaneous requests

#### QUIC Server (8+ tests)
- Health check over QUIC/HTTP3
- PUT operations over QUIC
- GET operations over QUIC
- Large file transfers
- TLS 1.3 requirement verification
- Performance comparison vs HTTP/2
- Concurrent operations: 30+ simultaneous requests
- Stream multiplexing: Parallel streams without head-of-line blocking

#### MCP Server (12+ tests)
- JSON-RPC 2.0 protocol compliance
- tools/list: List all 6 MCP tools
- objstore_put: Upload with/without metadata
- objstore_get: Download existing/non-existent objects
- objstore_delete: Delete objects
- objstore_list: List with prefix, pagination
- objstore_exists: Existence checks
- objstore_get_metadata: Metadata retrieval
- resources/list: List available resources
- resources/read: Read resource content
- Error handling: Invalid JSON, unknown methods, missing parameters
- Concurrent operations: 20+ simultaneous tool calls

**Run**:
```bash
make test-servers
```

### 3. Storage Backend Tests

```bash
# Local filesystem tests
make integration-test-local

# S3/MinIO tests
make integration-test-s3

# Azure Blob/Azurite tests
make integration-test-azure

# Google Cloud Storage tests
make integration-test-gcs

# Factory pattern tests
make integration-test-factory
```

**Local Storage Tests Include:**
- Basic CRUD operations
- Empty files and special characters
- Large file handling (1MB+)
- **Persistent lifecycle manager** (NEW)
  - Process restart scenarios
  - Policy persistence to disk
  - Multiple instances sharing policies
- **In-memory lifecycle manager**
  - Default behavior verification
  - Non-persistence validation

## Test Coverage Summary

| Test Suite | Number of Tests | Coverage Areas |
|------------|----------------|----------------|
| CLI | 17+ | All commands, formats, config, errors |
| gRPC Server | 15+ | All RPC methods, streaming, TLS |
| REST Server | 20+ | All endpoints, CORS, versioning |
| QUIC Server | 8+ | HTTP/3, TLS 1.3, multiplexing |
| MCP Server | 12+ | 6 tools, JSON-RPC, resources |
| **Total** | **72+** | **All interfaces tested** |

## Environment Variables

### CLI Tests
- `OBJECTSTORE_BACKEND`: Storage backend (default: local)
- `OBJECTSTORE_BACKEND_PATH`: Path for local storage
- `OBJECTSTORE_OUTPUT_FORMAT`: Output format (text, json, table)

### Server Tests
- `GRPC_SERVER_ADDR`: gRPC server address (default: grpc-server:50051)
- `REST_SERVER_ADDR`: REST server address (default: http://rest-server:8080)
- `QUIC_SERVER_ADDR`: QUIC server address (default: quic-server:4433)
- `MCP_SERVER_ADDR`: MCP server address (default: http://mcp-server:8081)
- `CERT_FILE`: Path to TLS certificate (default: /certs/server.crt)
- `GO_TEST_TIMEOUT`: Test timeout duration (default: 15m)

## Troubleshooting

### Tests Timeout

If tests timeout, check:
1. Docker resources (CPU/memory)
2. Network connectivity between containers
3. Server startup time (check health checks)

```bash
# Check container logs
docker-compose logs grpc-server
docker-compose logs rest-server
docker-compose logs quic-server
docker-compose logs mcp-server
```

### Certificate Issues

If TLS/QUIC tests fail:
```bash
cd test/integration/server
rm -rf certs
bash generate-certs.sh
```

### Port Conflicts

If ports are already in use:
```bash
# Check what's using the ports
lsof -i :50051  # gRPC
lsof -i :8080   # REST
lsof -i :4433   # QUIC
lsof -i :8081   # MCP
```

### Clean Up

Remove all test containers and volumes:
```bash
cd test/integration/cli
docker-compose down -v

cd test/integration/server
docker-compose down -v
```

## Debugging Tests

### Run Specific Test

```bash
# Run a specific CLI test
cd test/integration/cli
docker-compose run --rm test go test -v ./test/integration/cli/... -run TestCLIPutCommand

# Run specific server test
cd test/integration/server
docker-compose run --rm test go test -v ./test/integration/server/... -run TestGRPCPut
```

### Interactive Debugging

```bash
# Start servers without running tests
cd test/integration/server
docker-compose up -d grpc-server rest-server quic-server mcp-server

# Connect to test container for interactive debugging
docker-compose run --rm test sh
```

## Performance Considerations

### Test Execution Time

Approximate execution times:
- CLI tests: 1-2 minutes
- Server tests: 3-5 minutes
- All integration tests: 10-15 minutes

### Resource Requirements

Minimum recommended:
- CPU: 4 cores
- RAM: 8GB
- Disk: 10GB free space

## CI/CD Integration

### GitHub Actions Example

```yaml
name: Integration Tests

on: [push, pull_request]

jobs:
  integration:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Run CLI tests
        run: make test-cli
      - name: Run server tests
        run: make test-servers
```

## Best Practices

1. **Always run in Docker**: Never run integration tests on host OS
2. **Clean between runs**: Use `docker-compose down -v` to clean state
3. **Check logs**: If tests fail, check container logs
4. **Regenerate certs**: If TLS tests fail, regenerate certificates
5. **Use timeouts**: Set appropriate test timeouts for your environment

## Related Documentation

- [Main README](../../README.md)
- [CLI Documentation](../../docs/cli.md)
- [gRPC Server](../../docs/grpc-server.md)
- [REST Server](../../docs/grpc-server.md)
- [QUIC Server](../../docs/quic-server.md)
- [MCP Server](../../docs/mcp-server.md)
- [Storage Backends](../../docs/backends/README.md)
