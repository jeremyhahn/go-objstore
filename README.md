# go-objstore

A unified object storage and file system abstraction library for Go.

[![Go Version](https://img.shields.io/badge/go-1.26+-blue.svg)](https://golang.org)
[![codecov](https://codecov.io/gh/jeremyhahn/go-objstore/branch/main/graph/badge.svg)](https://codecov.io/gh/jeremyhahn/go-objstore)
[![Security](https://img.shields.io/badge/security-0%20issues-brightgreen.svg)](#test-coverage)
[![AGPL-3.0 License](https://img.shields.io/badge/license-AGPL--3.0-blue.svg)](LICENSE-AGPL-3.txt)
[![Commercial License](https://img.shields.io/badge/license-Commercial-green.svg)](LICENSE-COMMERCIAL.md)


## Features

- Unified API across all storage backends
- Facade pattern with a single, validated entry point for all storage operations
- Work with multiple storage backends at once
- Input validation against path traversal and injection
- Replication and sync between backends, with optional encryption
- Pluggable adapters for logging and authentication
- Filesystem interface with directory operations
- Lifecycle policies for automatic deletion and archival
- Encryption at rest with pluggable encrypters
- Server protocols: gRPC, REST, QUIC/HTTP3, Unix socket, and MCP
- CLI tool configurable via file, environment, or flags
- C API for embedding in C/C++ applications
- TLS/mTLS support

## Quick Start

### Installation

```bash
go get github.com/jeremyhahn/go-objstore
```

### Basic Usage (Facade Pattern - Recommended)

```go
package main

import (
    "bytes"
    "context"
    "fmt"
    "io"

    "github.com/jeremyhahn/go-objstore/pkg/common"
    "github.com/jeremyhahn/go-objstore/pkg/factory"
    "github.com/jeremyhahn/go-objstore/pkg/objstore"
)

func main() {
    // Create storage backends
    local, _ := factory.NewStorage("local", map[string]string{
        "path": "/tmp/my-storage",
    })

    // Initialize facade (do this once at app startup)
    objstore.Initialize(&objstore.FacadeConfig{
        Backends: map[string]common.Storage{
            "local": local,
        },
        DefaultBackend: "local",
    })
    defer objstore.Reset()

    // Store data
    data := []byte("Hello, World!")
    objstore.Put("greeting.txt", bytes.NewReader(data))

    // Retrieve data
    reader, _ := objstore.Get("greeting.txt")
    defer reader.Close()

    content, _ := io.ReadAll(reader)
    fmt.Println(string(content))  // Output: Hello, World!

    // Delete data
    objstore.Delete("greeting.txt")
}
```

### Direct Storage Access (Legacy)

For backward compatibility, you can still use direct storage access:

```go
// Create a storage backend
storage, err := factory.NewStorage("local", map[string]string{
    "path": "/tmp/my-storage",
})
if err != nil {
    panic(err)
}

// Use storage directly
storage.Put("greeting.txt", bytes.NewReader(data))
```

**Note:** Use the facade pattern for new code. It adds input validation, multi-backend support, and sanitized error messages.

## Supported Backends

| Backend | Type | Use Case |
|---------|------|----------|
| Local | Storage | Development, testing, local archives |
| S3 | Storage | AWS object storage, high availability |
| MinIO | Storage | Self-hosted S3-compatible object storage |
| GCS | Storage | Google Cloud object storage |
| Azure Blob | Storage | Microsoft Azure object storage |
| Memory | Storage | Unit tests, ephemeral/in-memory |
| Glacier | Archive-only | AWS long-term cold storage |
| Azure Archive | Archive-only | Azure long-term cold storage |

## Backend Configuration

### Local Storage

```go
storage, _ := factory.NewStorage("local", map[string]string{
    "path": "/var/data/storage",
})
```

### Memory Storage

```go
storage, _ := factory.NewStorage("memory", map[string]string{})
```

### Amazon S3

```go
storage, _ := factory.NewStorage("s3", map[string]string{
    "region": "us-east-1",
    "bucket": "my-bucket",
    // Optional: for custom endpoints (MinIO, LocalStack)
    "endpoint":       "http://localhost:9000",
    "forcePathStyle": "true",
    "accessKey":      "minioadmin",
    "secretKey":      "minioadmin",
})
```

### MinIO

```go
storage, _ := factory.NewStorage("minio", map[string]string{
    "bucket":    "my-bucket",
    "endpoint":  "http://localhost:9000",
    "accessKey": "minioadmin",
    "secretKey": "minioadmin",
    // Optional: defaults to "us-east-1"
    "region":    "us-east-1",
})
```

### Google Cloud Storage

```go
storage, _ := factory.NewStorage("gcs", map[string]string{
    "bucket": "my-gcs-bucket",
})
```

### Azure Blob Storage

```go
storage, _ := factory.NewStorage("azure", map[string]string{
    "accountName":   "myaccount",
    "accountKey":    "base64key==",
    "containerName": "mycontainer",
})
```

## Advanced Features

### Facade Pattern (Recommended)

The facade is a single API for working with multiple storage backends. It validates input consistently at every entry point.

#### Benefits

- Work with multiple storage backends at once
- Target a specific backend with the `backend:key` syntax
- Validates input against path traversal, injection, and malformed keys
- Single entry point for all storage operations
- Sanitized error messages that don't disclose internal detail

#### Multi-Backend Example

```go
import (
    "github.com/jeremyhahn/go-objstore/pkg/common"
    "github.com/jeremyhahn/go-objstore/pkg/factory"
    "github.com/jeremyhahn/go-objstore/pkg/objstore"
)

// Create multiple storage backends
local, _ := factory.NewStorage("local", map[string]string{
    "path": "/tmp/local-storage",
})

s3, _ := factory.NewStorage("s3", map[string]string{
    "bucket": "my-bucket",
    "region": "us-east-1",
})

// Initialize facade once at application startup
objstore.Initialize(&objstore.FacadeConfig{
    Backends: map[string]common.Storage{
        "local": local,
        "s3":    s3,
    },
    DefaultBackend: "local",
})
defer objstore.Reset()

// Use default backend
objstore.Put("file.txt", data)

// Target specific backend
objstore.PutWithContext(ctx, "s3:backups/file.txt", data)
objstore.PutWithContext(ctx, "local:cache/temp.dat", data)

// Get from specific backend
reader, _ := objstore.GetWithContext(ctx, "s3:backups/file.txt")

// List all available backends
backends := objstore.Backends()  // ["local", "s3"]
```

#### Security Features

The facade automatically validates all inputs to prevent attacks:

```go
// These all fail with validation errors
objstore.Put("../../../etc/passwd", data)        // Path traversal blocked
objstore.Put("/etc/passwd", data)                // Absolute path blocked
objstore.Put("file\x00.txt", data)              // Null byte blocked
objstore.Put("file\n.txt", data)                // Control character blocked
objstore.PutWithContext(ctx, "INVALID:key", data) // Invalid backend name blocked
```

For detailed migration guide and examples, see [docs/facade-migration.md](docs/facade-migration.md).

### Filesystem Interface

Use object storage with familiar filesystem operations:

```go
import "github.com/jeremyhahn/go-objstore/pkg/storagefs"

fs := storagefs.New(storage)

// Create directories
fs.MkdirAll("docs/2024", 0755)

// Create and write to a file
file, _ := fs.Create("docs/readme.txt")
file.WriteString("Hello from StorageFS!")
file.Close()

// List directory contents
dir, _ := fs.Open("docs")
defer dir.Close()

entries, _ := dir.Readdir(-1)
for _, entry := range entries {
    fmt.Printf("%s (dir: %v, size: %d bytes)\n",
        entry.Name(), entry.IsDir(), entry.Size())
}

// Read directory names only
dir2, _ := fs.Open("docs")
names, _ := dir2.Readdirnames(-1)
for _, name := range names {
    fmt.Println(name)
}
```

### Lifecycle Policies

Automate data retention and archival:

```go
import (
    "time"
    "github.com/jeremyhahn/go-objstore/pkg/common"
)

// Delete old logs after 30 days
deletePolicy := common.LifecyclePolicy{
    ID:        "cleanup-old-logs",
    Prefix:    "logs/",
    Action:    "delete",
    Retention: 30 * 24 * time.Hour,
}
storage.AddPolicy(deletePolicy)

// Archive data to Glacier after 90 days
glacier, _ := factory.NewArchiver("glacier", map[string]string{
    "vaultName": "long-term-archive",
    "region":    "us-east-1",
})

archivePolicy := common.LifecyclePolicy{
    ID:          "archive-old-data",
    Prefix:      "data/",
    Action:      "archive",
    Destination: glacier,
    Retention:   90 * 24 * time.Hour,
}
storage.AddPolicy(archivePolicy)
```

### Replication

Replicate and sync data between storage backends with optional encryption:

```go
import (
    "context"
    "time"
    "github.com/jeremyhahn/go-objstore/pkg/objstore"
)

// Enable replication on the default backend
objstore.EnableReplication("", &objstore.ReplicationConfig{
    PolicyFilePath:  "/data/replication-policies.json",
    Interval:        10 * time.Minute,
    RunInBackground: true,
})

// Get the replication manager
mgr, _ := objstore.GetReplicationManager("")

// Create a local-to-local replication policy
policy := common.ReplicationPolicy{
    ID:                  "backup-daily",
    SourceBackend:       "local",
    SourceSettings:      map[string]string{"path": "/data/source"},
    SourcePrefix:        "documents/",
    DestinationBackend:  "local",
    DestinationSettings: map[string]string{"path": "/data/backup"},
    CheckInterval:       1 * time.Hour,
    Enabled:             true,
    ReplicationMode:     common.ReplicationModeTransparent,
}

// Add the policy
mgr.AddPolicy(policy)

// Trigger sync for a specific policy
result, _ := mgr.SyncPolicy(context.Background(), "backup-daily")
fmt.Printf("Synced %d objects, %d bytes\n", result.Synced, result.BytesTotal)

// Or sync all policies in parallel
result, _ := mgr.SyncAllParallel(context.Background(), 4)
```

**Replication modes:**
- `transparent`: Decrypts at source and re-encrypts at destination (use when source and destination have different DEKs)
- `opaque`: Copies encrypted blobs as-is without DEK operations (use for backup scenarios or performance)

**Three-layer encryption:**
1. Backend at-rest encryption (EncryptionPolicy.Backend)
2. Source DEK layer (EncryptionPolicy.Source)
3. Destination DEK layer (EncryptionPolicy.Destination)

### Encryption at Rest

Add transparent encryption to any storage backend:

```go
import (
    "github.com/jeremyhahn/go-objstore/pkg/common"
)

// Create an encrypter factory (example: using a KMS adapter)
factory := yourKMSAdapter.GetEncrypterFactory()

// Wrap the storage with encryption
encrypted := common.NewEncryptedStorage(storage, factory)

// Now all Put/Get operations are encrypted/decrypted transparently
encrypted.Put("sensitive-data.txt", reader)
encrypted.Get("sensitive-data.txt")
```

See the [encryption example](examples/encryption/) for a complete AES-256-GCM implementation with KMS adapter.

### Authentication & Authorization

Authentication and authorization are pluggable:

```go
import (
    "github.com/jeremyhahn/go-objstore/pkg/adapters"
)

// Default: allow-all (library-first, no auth required)
auth := adapters.NewNoOpAuthenticator()
authz := adapters.NewNoOpAuthorizer()

// Bearer token authentication
auth := adapters.NewBearerTokenAuthenticator(
    func(ctx context.Context, token string) (*adapters.Principal, error) {
        // Validate token and return principal
        return &adapters.Principal{
            ID:    "user-123",
            Name:  "Alice",
            Type:  "user",
            Roles: []string{"reader"},
        }, nil
    },
)

// Role-based authorization
authz := adapters.NewRBACAuthorizer(map[string][]string{
    "reader":  {adapters.ActionRead, adapters.ActionList},
    "editor":  {adapters.ActionRead, adapters.ActionWrite, adapters.ActionList},
    "admin":   {"*"}, // Wildcard grants all actions
})

// Composite authenticator (try multiple methods)
auth := adapters.NewCompositeAuthenticator(
    adapters.NewBearerTokenAuthenticator(validateJWT),
    adapters.NewMTLSAuthenticator(extractFromCert, caRoots),
)
```

**Action vocabulary:** `read`, `write`, `delete`, `list`, `admin`

**Resource categories:** `object`, `policy`, `replication`

**Unix socket authentication (Linux):** The Unix socket server derives principals from process credentials (UID/GID) via SO_PEERCRED; socket file permissions are the primary access gate.

### Audit Logging

Record storage operations to an audit log:

```go
import (
    "github.com/jeremyhahn/go-objstore/pkg/audit"
)

// Create an audit logger
auditLog := audit.NewDefaultAuditLogger()

// Log authentication events
auditLog.LogAuthFailure(ctx, "user-123", "alice", "192.168.1.1", "req-456", "invalid credentials")
auditLog.LogAuthSuccess(ctx, "user-123", "alice", "192.168.1.1", "req-456")

// Log object operations
auditLog.LogObjectMutation(
    ctx,
    audit.EventObjectCreated,
    "user-123", "alice", "bucket", "file.txt",
    "192.168.1.1", "req-456",
    1024, // bytes transferred
    audit.ResultSuccess,
    nil,  // error
)

// Log access
auditLog.LogObjectAccess(ctx, "user-123", "alice", "bucket", "file.txt",
    "192.168.1.1", "req-456", audit.ResultSuccess, nil)

// Log policy changes
auditLog.LogPolicyChange(ctx, "user-123", "alice", "bucket", "policy-1",
    "192.168.1.1", "req-456", audit.ResultSuccess, nil)
```

**Event types:** AUTH_FAILURE, AUTH_SUCCESS, OBJECT_CREATED, OBJECT_DELETED, OBJECT_ACCESSED, OBJECT_METADATA_UPDATED, OBJECT_ARCHIVED, POLICY_CHANGED, LIST_OBJECTS

### Context Support

All operations support context for cancellation and timeouts:

```go
import "context"

ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

// Context-aware operations
err := storage.PutWithContext(ctx, "key", data)
reader, err := storage.GetWithContext(ctx, "key")
err = storage.DeleteWithContext(ctx, "key")
```

### Metadata Support

Store and retrieve custom metadata:

```go
metadata := &common.Metadata{
    ContentType:     "application/json",
    ContentEncoding: "utf-8",
    Custom: map[string]string{
        "author":  "john-doe",
        "version": "1.0",
    },
}

// Put with metadata
storage.PutWithMetadata(ctx, "data.json", reader, metadata)

// Get metadata
meta, _ := storage.GetMetadata(ctx, "data.json")
fmt.Println(meta.Custom["author"])  // Output: john-doe

// Update metadata
meta.Custom["version"] = "2.0"
storage.UpdateMetadata(ctx, "data.json", meta)
```

### List with Pagination

Efficiently list large directories:

```go
opts := &common.ListOptions{
    Prefix:     "logs/2024/",
    MaxResults: 100,
    Delimiter:  "/",
}

result, _ := storage.ListWithOptions(ctx, opts)
for _, obj := range result.Objects {
    fmt.Printf("%s (%d bytes)\n", obj.Key, obj.Metadata.Size)
}

// Get next page
if result.Truncated {
    opts.ContinueFrom = result.NextToken
    nextPage, _ := storage.ListWithOptions(ctx, opts)
}
```

## Client SDKs

Official client SDKs are available for six languages. Each implements the same 19 canonical operations and auto-selects an available protocol (REST, gRPC, or QUIC).

| Language | Package | Operations | Protocols |
|----------|---------|-----------|-----------|
| Python | `pip install go-objstore` | 19 | REST, gRPC, QUIC |
| Ruby | `gem install go-objstore` | 19 | REST, gRPC, QUIC |
| Go | `go get github.com/jeremyhahn/go-objstore/api/sdks/go` | 19 | REST, gRPC, QUIC |
| Rust | `cargo add go-objstore` | 19 | REST, gRPC, QUIC |
| TypeScript/JavaScript | `npm install @go-objstore/client` | 19 | REST, gRPC, QUIC |
| C# | `dotnet add go-objstore` | 19 | REST, gRPC, QUIC |

**Canonical operations (19):**
- Objects: Put, Get, Delete, List, Exists
- Metadata: GetMetadata, UpdateMetadata
- Service: Health
- Archival: Archive
- Lifecycle: AddPolicy, RemovePolicy, GetPolicies, ApplyPolicies
- Replication: AddReplicationPolicy, RemoveReplicationPolicy, GetReplicationPolicies, GetReplicationPolicy, TriggerReplication, GetReplicationStatus

For per-language installation and usage, see [api/sdks/README.md](api/sdks/README.md).

## C API

Embed go-objstore in C/C++ applications:

```c
#include "libobjstore.h"

int main(void) {
    // Create storage
    char *keys[] = {"path"};
    char *values[] = {"/tmp/storage"};
    int handle = ObjstoreNewStorage("local", keys, values, 1);

    // Store data
    char *data = "Hello from C!";
    ObjstorePut(handle, "test.txt", data, strlen(data));

    // Retrieve data
    char buffer[256];
    int len = ObjstoreGet(handle, "test.txt", buffer, 256);
    buffer[len] = '\0';
    printf("%s\n", buffer);

    // Cleanup
    ObjstoreDelete(handle, "test.txt");
    ObjstoreClose(handle);
    return 0;
}
```

Build instructions:

```bash
# Build the shared library
make lib

# Compile your C program
gcc -o myapp myapp.c -L./bin -lobjstore -lpthread -ldl

# Run with library path
LD_LIBRARY_PATH=./bin ./myapp
```

## Documentation

Documentation is in the [docs/](docs/) directory.

### Architecture

- [Architecture Overview](docs/architecture/README.md)
- [Storage Layer](docs/architecture/storage-layer.md)
- [Servers](docs/architecture/servers.md)
- [StorageFS](docs/architecture/storagefs.md)
- [Encryption](docs/architecture/encryption.md)
- [Lifecycle Management](docs/architecture/lifecycle.md)

### Configuration

- [Configuration Guide](docs/configuration/README.md)
- [Storage Backends](docs/configuration/storage-backends.md)
- [Servers](docs/configuration/grpc-server.md) (gRPC, REST, QUIC, Unix socket, MCP)
- [Encryption](docs/configuration/encryption.md)
- [Lifecycle Policies](docs/configuration/lifecycle.md)
- [CLI Tool](docs/configuration/cli.md)

### Usage

- [Getting Started](docs/usage/getting-started.md)
- [Using Storage Backends](docs/usage/storage-backends.md)
- [Using the CLI](docs/usage/cli.md)
- [Deployment Guide](docs/usage/deployment.md)

### Additional Resources

- [C API Reference](docs/c_client/README.md)
- [Testing Guide](docs/testing.md)

## Examples

Example code is available in the [examples/](examples/) directory:

- [Basic Usage](examples/basic-usage/) - Simple storage operations
- [C Client](examples/c_client/) - Using from C applications
- [Encryption](examples/encryption/) - Transparent encryption with KMS adapter
- [Facade Usage](examples/facade-usage/) - Multi-backend facade pattern
- [gRPC Client](examples/grpc-client/) - gRPC client usage
- [Lifecycle Policies](examples/lifecycle-policies/) - Automated retention and archival
- [StorageFS](examples/storagefs/) - Filesystem interface examples

## Project Structure

```
go-objstore/
├── pkg/                        # Core packages
│   ├── factory/               # Backend factory
│   ├── common/                # Shared interfaces and types
│   ├── local/                 # Local filesystem backend
│   ├── memory/                # In-memory storage backend
│   ├── s3/                    # Amazon S3 backend
│   ├── gcs/                   # Google Cloud Storage backend
│   ├── azure/                 # Azure Blob Storage backend
│   ├── minio/                 # MinIO S3-compatible backend
│   ├── glacier/               # AWS Glacier archiver
│   ├── azurearchive/          # Azure Archive archiver
│   ├── storagefs/             # Filesystem abstraction
│   ├── replication/           # Replication engine
│   ├── audit/                 # Audit logging
│   ├── adapters/              # Custom logging and TLS adapters
│   ├── pool/                  # Volume pool (backend placement)
│   ├── cli/                   # CLI commands and config
│   ├── version/               # Version information
│   ├── objstore/              # Facade pattern implementation
│   └── server/                # Server implementations
│       ├── grpc/              # gRPC server
│       ├── rest/              # REST API server
│       ├── quic/              # QUIC/HTTP3 server
│       ├── unix/              # Unix socket server
│       ├── mcp/               # MCP server
│       └── middleware/        # Rate limiting and security middleware
├── cmd/
│   ├── objstore/              # CLI binary
│   ├── objstore-server/       # All-in-one multi-protocol server
│   ├── objstore-grpc-server/  # Individual gRPC server
│   ├── objstore-rest-server/  # Individual REST server
│   ├── objstore-quic-server/  # Individual QUIC/HTTP3 server
│   ├── objstore-mcp-server/   # Individual MCP server
│   └── objstorelib/           # C API shared library
├── api/                       # API definitions
│   ├── proto/                 # Protocol buffers for gRPC
│   ├── openapi/               # OpenAPI specs for REST
│   ├── mcp/                   # MCP server configuration
│   └── sdks/                  # Official client SDKs (Python, Ruby, Go, Rust, TypeScript, C#)
├── examples/                  # Usage examples
├── test/integration/          # Integration tests
└── docs/                      # Documentation
```

## Development

### Prerequisites

- Go 1.26.4 or higher
- Docker (for integration tests)
- Make

### Building

```bash
# Install dependencies
make deps

# Build the library
make build

# Build CLI tool
make build-cli

# Build server
make build-server

# Build C shared library
make lib
```

### Testing

```bash
# Run unit tests (fast, in-memory)
make test

# Run all integration tests (backends + CLI)
make integration-test

# Run ALL integration tests including servers
make integration-test-all

# Run specific backend tests
make integration-test-local
make integration-test-s3
make integration-test-azure
make integration-test-gcs
make integration-test-minio
make integration-test-factory

# Run CLI integration tests
make integration-test-cli

# Run server integration tests (gRPC, REST, QUIC, Unix socket, MCP)
make test-servers

# Generate coverage report
make coverage-report

# Check per-package coverage (highlights packages under 90%)
make coverage-check
```

### Test Coverage

Unit tests run with no external dependencies. Integration tests use Docker-based emulators for the servers and backends. CLI integration tests build the CLI binary if it isn't present. Security scanning uses gosec and govulncheck. For coverage statistics, see the badge above or run `make coverage-report`.

## Architecture

### Storage Interface

All backends implement a common Storage interface:

```go
type Storage interface {
    LifecycleManager  // Embedded: AddPolicy, RemovePolicy, GetPolicies

    // Configuration
    Configure(settings map[string]string) error

    // Basic operations
    Put(key string, data io.Reader) error
    Get(key string) (io.ReadCloser, error)
    Delete(key string) error
    List(prefix string) ([]string, error)

    // Context-aware operations
    PutWithContext(ctx context.Context, key string, data io.Reader) error
    GetWithContext(ctx context.Context, key string) (io.ReadCloser, error)
    DeleteWithContext(ctx context.Context, key string) error
    ListWithContext(ctx context.Context, prefix string) ([]string, error)

    // Metadata operations
    PutWithMetadata(ctx context.Context, key string, data io.Reader, metadata *Metadata) error
    GetMetadata(ctx context.Context, key string) (*Metadata, error)
    UpdateMetadata(ctx context.Context, key string, metadata *Metadata) error

    // Advanced operations
    Exists(ctx context.Context, key string) (bool, error)
    ListWithOptions(ctx context.Context, opts *ListOptions) (*ListResult, error)
    Archive(key string, destination Archiver) error
}
```

### Factory Pattern

The factory creates backends through a single function:

```go
storage, err := factory.NewStorage(backendType, config)
archiver, err := factory.NewArchiver(archiverType, config)
```

It hides backend-specific initialization behind one interface.

## Performance

All backends support concurrent reads and writes. Buffered I/O helps throughput. The local backend is the fastest; cloud backends add network overhead. See [docs/testing.md](docs/testing.md) for benchmarks.

## Best Practices

1. Always close readers returned by Get() to prevent resource leaks
2. Handle errors from all storage operations
3. Use context for cancellation and timeouts on long operations
4. Enable lifecycle policies for automatic cleanup
5. Choose backends wisely based on cost, performance, and durability needs
6. Use StorageFS when you need standard filesystem operations
7. Test with emulators before deploying to cloud

## Server Interfaces

There are two ways to run servers: an all-in-one multi-protocol binary, or individual binaries per protocol.

### CLI Tool

```bash
# Run the CLI
./bin/objstore --help

# Store an object from file
./bin/objstore put myfile.txt mykey

# Store from stdin
echo "Hello World" | ./bin/objstore put - mykey
cat data.txt | ./bin/objstore put - mykey

# Retrieve an object to file
./bin/objstore get mykey output.txt

# Retrieve to stdout
./bin/objstore get mykey
./bin/objstore get mykey -

# Pipe between backends (copy/migrate data)
./bin/objstore get mykey --backend local | \
  ./bin/objstore put - mykey --backend s3

# List objects
./bin/objstore list

# Check if object exists
./bin/objstore exists mykey

# Retrieve only an object's metadata (a flag on get, not a separate command)
./bin/objstore get mykey --metadata

# Lifecycle policy management
# Args: <id> <prefix> <retention-days> <action>  (action: delete or archive)
./bin/objstore policy add cleanup logs/ 30 delete
./bin/objstore policy list
./bin/objstore policy remove cleanup
./bin/objstore policy apply

# Replication management
# Args: <id> <source-backend> <destination-backend>; paths/options via flags
./bin/objstore replication add backup-daily local local \
  --source-path /data/source --dest-path /data/backup --interval 1h
./bin/objstore replication list
./bin/objstore replication trigger backup-daily
./bin/objstore replication status backup-daily

# Health check
./bin/objstore health

# Remote operation (against a running server)
./bin/objstore get mykey --server http://localhost:8080 --server-protocol rest
./bin/objstore get mykey --server localhost:50051 --server-protocol grpc

# Configure via config file, env vars, or flags
./bin/objstore --config .objstore.yaml put mykey data.txt
```

### All-in-One Multi-Protocol Server

Run all five server protocols simultaneously with a single binary:

```bash
# Start all services (gRPC, REST, QUIC, Unix socket, MCP)
./bin/objstore-server --quic-self-signed

# Customize ports and addresses
./bin/objstore-server \
  --grpc-addr :50051 \
  --rest-port 8080 \
  --quic-addr :4433 \
  --mcp-addr :8081 \
  --unix --unix-socket /var/run/objstore.sock \
  --quic-self-signed

# Disable specific services
./bin/objstore-server --grpc=false --quic=false

# With production TLS for QUIC
./bin/objstore-server \
  --quic-tls-cert cert.pem \
  --quic-tls-key key.pem
```

### Individual Server Binaries

Run individual protocols separately for focused deployments:

gRPC Server:
```bash
# Start gRPC server only
./bin/objstore-grpc-server --addr :50051

# With TLS
./bin/objstore-grpc-server --addr :50051 --tls-cert cert.pem --tls-key key.pem
```

REST API Server:
```bash
# Start REST server only
./bin/objstore-rest-server --port 8080

# Access via HTTP
curl http://localhost:8080/objects/mykey
```

QUIC/HTTP3 Server:
```bash
# Start QUIC server only
./bin/objstore-quic-server -addr :4433 -tlscert cert.pem -tlskey key.pem

# With self-signed certificate (testing only)
./bin/objstore-quic-server -addr :4433 -selfsigned
```

MCP Server:
```bash
# Start MCP server only (stdio mode for Claude Desktop)
./bin/objstore-mcp-server -mode stdio

# HTTP mode
./bin/objstore-mcp-server -mode http -addr :8081
```

**Note:** There is no standalone Unix socket server binary. The Unix socket server is available only via the all-in-one `objstore-server --unix` command.

### Deployment Patterns

Development - All Services:
```bash
# Quick development setup with all protocols
./bin/objstore-server --quic-self-signed
```

Production - Load Balanced:
```bash
# Multiple instances of specific protocols behind load balancers
./bin/objstore-grpc-server --addr :50051 &
./bin/objstore-rest-server --port 8080 &
```

Microservices - Dedicated Services:
```bash
# Different protocols in different containers/hosts
docker run objstore-grpc-server
docker run objstore-rest-server
docker run objstore-quic-server
```


---

## License

[![AGPL-3.0](https://www.gnu.org/graphics/agplv3-155x51.png)](https://www.gnu.org/licenses/agpl-3.0.html)

go-objstore is available under a dual-license model:

### Option 1: GNU Affero General Public License v3.0 (AGPL-3.0)

The open-source version of go-objstore is licensed under the [AGPL-3.0](LICENSE-AGPL-3.txt).

What does this mean?

- Free to use, modify, and distribute
- Perfect for open-source projects
- If you modify and deploy as a network service (SaaS), you must disclose your source code
- Derivative works must also be licensed under AGPL-3.0

The AGPL-3.0 requires that if you modify this software and provide it as a service over a network (including SaaS deployments), you must make your modified source code available under the same license.

### Option 2: Commercial License

If you wish to use go-objstore in proprietary software without the source disclosure requirements of AGPL-3.0, a commercial license is available from Automate The Things, LLC.

Commercial License Benefits:

- Use in closed-source applications
- No source code disclosure requirements
- Modify and keep changes private
- Professional support and SLA options
- Custom development available
- Legal protections and indemnification

Contact for Commercial Licensing:

For pricing and commercial licensing inquiries, email licensing@automatethethings.com or visit https://automatethethings.com

See [LICENSE-COMMERCIAL.md](LICENSE-COMMERCIAL.md) for more details.

### Choosing the Right License

| Use Case | Recommended License |
|----------|-------------------|
| Open-source projects | AGPL-3.0 |
| Internal use with source disclosure | AGPL-3.0 |
| SaaS/Cloud services (open-source) | AGPL-3.0 |
| Proprietary SaaS products | Commercial |
| Closed-source applications | Commercial |
| Embedded in commercial products | Commercial |
| Need professional support | Commercial |

---

Copyright 2025 Automate The Things, LLC. All rights reserved.


## Support

Please consider supporting this project for ongoing success and sustainability. I'm a passionate open source contributor making a professional living creating free, secure, scalable, robust, enterprise grade, distributed systems and cloud native solutions.

I'm also available for international consulting opportunities. Please let me know how I can assist you or your organization in achieving your desired security posture and technology goals.

https://github.com/sponsors/jeremyhahn

https://www.linkedin.com/in/jeremyhahn
