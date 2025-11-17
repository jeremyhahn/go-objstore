# Testing Guide

## Overview

objstore has comprehensive test coverage with unit tests, integration tests with emulators, and real cloud integration tests.

## Test Coverage

### Coverage Goals

The project maintains comprehensive test coverage across all core packages:
- **Storage backends**: High coverage (S3, Local, Azure, GCS, MinIO)
- **Archive backends**: High coverage (Glacier, Azure Archive)
- **Core infrastructure**: High coverage (Factory, CLI, Server)
- **Filesystem abstraction**: Good coverage (StorageFS)

For current coverage statistics, check the [Codecov dashboard](https://codecov.io/gh/jeremyhahn/go-objstore) or run `make coverage-report` locally.

## Test Types

### 1. Unit Tests
- Fast, in-memory tests
- No external dependencies
- Run in < 5 seconds
- Located alongside source: `pkg/*/**/*_test.go`

### 2. Integration Tests (Emulators)
- Test with emulated cloud services
- Use Docker containers
- Build tag: `integration`
- Located in: `test/integration/*/`

### 3. Cloud Integration Tests
- Test against real cloud providers (AWS, GCP, Azure)
- Require authenticated cloud CLIs
- Build tag: `cloud_integration`
- Optional - must be explicitly enabled

### 4. Server Tests
- Test gRPC, REST, QUIC, and MCP servers
- Use emulated backends
- Located in: `test/integration/server/`

### 5. CLI Tests
- Test command-line interface
- End-to-end CLI workflows
- Located in: `test/integration/cli/`

## Running Tests

### Quick Start

```bash
# Run all unit tests
make test

# Run integration tests with emulators
make integration-test

# Run specific backend integration tests
make integration-test-local
make integration-test-s3
make integration-test-gcs
make integration-test-azure

# Run CLI tests
make test-cli

# Run server tests
make test-servers

# Generate coverage report
make coverage-report
```

### Unit Tests

```bash
# All unit tests
go test ./pkg/... -v

# Specific package
go test ./pkg/s3 -v

# With coverage
go test ./pkg/... -cover

# Coverage by package
make coverage-check
```

### Integration Tests with Emulators

```bash
# All integration tests
make integration-test

# This will:
# 1. Start emulators (MinIO, Azurite, fake-gcs-server) in Docker
# 2. Run tests with -tags=integration
# 3. Clean up all resources

# Specific backend tests
make integration-test-s3        # S3 with MinIO
make integration-test-gcs       # GCS with fake-gcs-server
make integration-test-azure     # Azure with Azurite
make integration-test-local     # Local filesystem
```

### Cloud Integration Tests (Real Cloud)

Test against real AWS S3, Google Cloud Storage, and Azure Blob Storage:

```bash
# AWS S3 (requires AWS CLI authentication)
make test-cloud-s3

# Google Cloud Storage (requires gcloud authentication)
make test-cloud-gcs

# Azure Blob Storage (requires account credentials)
export OBJSTORE_TEST_AZURE_ACCOUNT="your-account"
export OBJSTORE_TEST_AZURE_KEY="your-key"
make test-cloud-azure

# All cloud providers
make test-cloud-integration
```

**Requirements:**
- Authenticated cloud CLIs (aws, gcloud, az)
- Proper permissions to create/delete buckets
- Tests create bucket `go-objstore-integration-test`
- Tests automatically clean up after completion

**Environment Variables:**
```bash
# S3 (optional - uses AWS CLI credentials by default)
OBJSTORE_TEST_REAL_S3=1
OBJSTORE_TEST_S3_BUCKET=go-objstore-integration-test
OBJSTORE_TEST_S3_REGION=us-east-1

# GCS (optional - uses gcloud credentials by default)
OBJSTORE_TEST_REAL_GCS=1
OBJSTORE_TEST_GCS_BUCKET=go-objstore-integration-test

# Azure (required)
OBJSTORE_TEST_REAL_AZURE=1
OBJSTORE_TEST_AZURE_ACCOUNT=your-storage-account
OBJSTORE_TEST_AZURE_KEY=your-account-key
OBJSTORE_TEST_AZURE_CONTAINER=go-objstore-integration-test
```

## Test Organization

```
go-objstore/
├── pkg/                           # Source packages
│   ├── s3/
│   │   ├── s3.go
│   │   └── s3_test.go            # Unit tests
│   ├── gcs/
│   │   ├── gcs.go
│   │   └── gcs_test.go           # Unit tests
│   └── ...
└── test/
    └── integration/              # Integration tests
        ├── s3/
        │   ├── s3_test.go        # Emulator tests (tag: integration)
        │   └── s3_real_cloud_test.go  # Real cloud tests (tag: cloud_integration)
        ├── gcs/
        │   ├── gcs_test.go       # Emulator tests (tag: integration)
        │   └── gcs_real_cloud_test.go # Real cloud tests (tag: cloud_integration)
        ├── azure/
        │   ├── azure_test.go     # Emulator tests (tag: integration)
        │   └── azure_real_cloud_test.go # Real cloud tests (tag: cloud_integration)
        ├── minio/
        │   └── minio_test.go     # MinIO-specific tests
        ├── local/
        │   └── local_test.go     # Local filesystem tests
        ├── cli/
        │   └── cli_test.go       # CLI integration tests
        ├── server/
        │   ├── grpc_test.go      # gRPC server tests
        │   ├── rest_test.go      # REST server tests
        │   ├── quic_test.go      # QUIC server tests
        │   └── mcp_test.go       # MCP server tests
        ├── factory/
        │   └── factory_test.go   # Factory tests
        └── common/
            └── helpers.go        # Shared test utilities
```

## Docker Test Environment

### Emulator Services

Integration tests use Docker containers for cloud service emulation:

**MinIO (S3 Compatible)**
```yaml
image: quay.io/minio/minio:latest
ports: 9000:9000, 9001:9001
credentials: minioadmin/minioadmin
```

**Azurite (Azure Compatible)**
```yaml
image: mcr.microsoft.com/azure-storage/azurite:latest
port: 10000:10000
account: devstoreaccount1
key: Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==
```

**fake-gcs-server (GCS Emulator)**
```yaml
image: fsouza/fake-gcs-server:latest
port: 4443
backend: memory
scheme: http
```

### Environment Variables

Integration tests use these environment variables:

```bash
# S3/MinIO
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
S3_ENDPOINT=http://localhost:9000

# GCS Emulator
STORAGE_EMULATOR_HOST=http://localhost:4443

# Azure/Azurite
AZURE_STORAGE_ACCOUNT=devstoreaccount1
AZURE_STORAGE_KEY=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==
```

## Writing Tests

### Unit Test Example

```go
package s3

import (
    "bytes"
    "testing"
)

func TestS3_Put(t *testing.T) {
    tests := []struct {
        name    string
        key     string
        data    []byte
        wantErr bool
    }{
        {
            name:    "valid put",
            key:     "test.txt",
            data:    []byte("hello"),
            wantErr: false,
        },
        {
            name:    "empty key",
            key:     "",
            data:    []byte("data"),
            wantErr: true,
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            s := &S3{/* ... */}
            err := s.Put(tt.key, bytes.NewReader(tt.data))
            if (err != nil) != tt.wantErr {
                t.Errorf("Put() error = %v, wantErr %v", err, tt.wantErr)
            }
        })
    }
}
```

### Integration Test Example (Emulator)

```go
//go:build integration

package s3

import (
    "bytes"
    "context"
    "io"
    "testing"

    "github.com/jeremyhahn/go-objstore/pkg/factory"
)

func TestS3_BasicOps(t *testing.T) {
    // Create storage via factory
    storage, err := factory.NewStorage("s3", map[string]string{
        "bucket":   "test-bucket",
        "region":   "us-east-1",
        "endpoint": "http://localhost:9000",
    })
    if err != nil {
        t.Fatalf("Failed to create storage: %v", err)
    }

    ctx := context.Background()
    key := "test.txt"
    data := []byte("hello world")

    // Test Put
    if err := storage.PutWithContext(ctx, key, bytes.NewReader(data)); err != nil {
        t.Fatalf("Put failed: %v", err)
    }

    // Test Get
    reader, err := storage.GetWithContext(ctx, key)
    if err != nil {
        t.Fatalf("Get failed: %v", err)
    }
    defer reader.Close()

    got, _ := io.ReadAll(reader)
    if !bytes.Equal(got, data) {
        t.Errorf("Data mismatch: got %q, want %q", got, data)
    }

    // Test Delete
    if err := storage.DeleteWithContext(ctx, key); err != nil {
        t.Fatalf("Delete failed: %v", err)
    }
}
```

### Cloud Integration Test Example

```go
//go:build cloud_integration && awss3

package s3

import (
    "bytes"
    "context"
    "os"
    "testing"

    "github.com/jeremyhahn/go-objstore/pkg/factory"
)

func TestS3_RealCloud(t *testing.T) {
    if os.Getenv("OBJSTORE_TEST_REAL_S3") != "1" {
        t.Skip("Skipping real S3 test. Set OBJSTORE_TEST_REAL_S3=1 to enable")
    }

    bucket := os.Getenv("OBJSTORE_TEST_S3_BUCKET")
    if bucket == "" {
        bucket = "go-objstore-integration-test"
    }

    // Create storage
    storage, err := factory.NewStorage("s3", map[string]string{
        "bucket": bucket,
        "region": "us-east-1",
    })
    if err != nil {
        t.Fatalf("Failed to create S3 storage: %v", err)
    }

    // Run tests...
}
```

## Build Tags

The project uses Go build tags to separate test types:

- **No tag**: Unit tests (always run)
- **`integration`**: Emulator-based integration tests
- **`cloud_integration`**: Real cloud provider tests
- **Backend tags**: `awss3`, `gcpstorage`, `azureblob`, etc.

Example:
```bash
# Unit tests only
go test ./pkg/...

# Integration tests with emulators
go test -tags=integration ./test/integration/s3

# Real cloud tests
go test -tags=cloud_integration,awss3 ./test/integration/s3 -run TestS3_RealCloud
```

## CI/CD Integration

### GitHub Actions Example

```yaml
name: Tests

on: [push, pull_request]

jobs:
  unit-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-go@v4
        with:
          go-version: '1.21'
      - name: Run unit tests
        run: make test
      - name: Check coverage
        run: make coverage-check

  integration-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Run integration tests
        run: make integration-test
```

## Troubleshooting

### Docker Issues

**Tests fail with "connection refused"**
```bash
# Ensure Docker is running
docker ps

# Clean and restart
make integration-clean
make integration-test
```

**Port conflicts**
```bash
# Check for processes using test ports
lsof -i :9000   # MinIO
lsof -i :10000  # Azurite
lsof -i :4443   # fake-gcs-server

# Stop conflicting containers
docker compose -f test/integration/docker-compose.yml down -v
```

### Cloud Test Issues

**"Bucket does not exist" errors**
- Cloud tests create buckets automatically
- Ensure you have `s3:CreateBucket` permission (or similar)
- Alternatively, create bucket manually first

**AWS credentials not found**
```bash
# Verify AWS CLI is configured
aws sts get-caller-identity

# Set credentials if needed
aws configure
```

**GCS authentication errors**
```bash
# Verify gcloud is authenticated
gcloud auth list

# Set default project
gcloud config set project YOUR_PROJECT_ID

# Application default credentials
gcloud auth application-default login
```

**Azure authentication errors**
```bash
# Verify Azure CLI is authenticated
az account show

# Login if needed
az login

# Set subscription
az account set --subscription YOUR_SUBSCRIPTION_ID
```

### Performance Issues

**Slow integration tests**
- First run downloads Docker images (one-time cost)
- Subsequent runs use cached layers
- Typical runtime: 30-60 seconds

**Slow cloud tests**
- Network latency to cloud providers
- Bucket creation/deletion overhead
- Typical runtime: 2-5 seconds per test

## Best Practices

1. **Write tests first** (TDD approach)
2. **Test both success and error cases**
3. **Use table-driven tests** for multiple scenarios
4. **Keep unit tests fast** (< 100ms each)
5. **Mock external dependencies** in unit tests
6. **Use real emulators** for integration tests
7. **Clean up resources** in defer blocks
8. **Never modify host filesystem** in tests
9. **Use context for timeout control**
10. **Maintain high coverage** for core packages

### Testing Checklist

When adding a new feature:
- [ ] Write comprehensive unit tests
- [ ] Add integration tests with emulator
- [ ] Consider cloud integration test if backend-specific
- [ ] Update this documentation
- [ ] Run full test suite: `make test && make integration-test`
- [ ] Check coverage: `make coverage-check`

## Test Statistics

Run `make coverage-report` to see detailed test statistics including:
- Number of unit tests
- Number of integration tests
- Number of cloud tests
- Overall coverage percentage
- Coverage by package
- Execution times

CI/CD test results and coverage trends are available on [GitHub Actions](https://github.com/jeremyhahn/go-objstore/actions) and [Codecov](https://codecov.io/gh/jeremyhahn/go-objstore).
