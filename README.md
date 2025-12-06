# go-objstore

A unified object storage and file system abstraction library for Go.

[![Go Version](https://img.shields.io/badge/go-1.23+-blue.svg)](https://golang.org)
[![codecov](https://codecov.io/gh/jeremyhahn/go-objstore/branch/main/graph/badge.svg)](https://codecov.io/gh/jeremyhahn/go-objstore)
[![Security](https://img.shields.io/badge/security-0%20issues-brightgreen.svg)](#test-coverage)
[![AGPL-3.0 License](https://img.shields.io/badge/license-AGPL--3.0-blue.svg)](LICENSE-AGPL-3.txt)
[![Commercial License](https://img.shields.io/badge/license-Commercial-green.svg)](LICENSE-COMMERCIAL.md)


## Features

- **Facade Pattern**: Centralized, secure API for all storage operations
- **Multi-Backend Support**: Work with multiple storage backends simultaneously
- **Input Validation**: Built-in protection against injection attacks
- Unified API across all storage backends
- Replication and sync between storage backends with encryption support
- Pluggable adapters for custom logging and authentication
- Full filesystem interface with directory operations
- Lifecycle policies for automatic deletion and archival
- Multiple server interfaces: gRPC, REST, QUIC/HTTP3, and MCP
- CLI tool with flexible configuration options
- C API for embedding in C/C++ applications
- TLS/mTLS support for secure communication

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

**Note:** The facade pattern is recommended for new code as it provides centralized validation, multi-backend support, and enhanced security.

## Supported Backends

| Backend | Type | Use Case |
|---------|------|----------|
| Local | Storage | Development, testing, local archives |
| S3 | Storage | AWS object storage, high availability |
| MinIO | Storage | Self-hosted S3-compatible object storage |
| GCS | Storage | Google Cloud object storage |
| Azure Blob | Storage | Microsoft Azure object storage |
| Glacier | Archive-only | AWS long-term cold storage |
| Azure Archive | Archive-only | Azure long-term cold storage |

## Backend Configuration

### Local Storage

```go
storage, _ := factory.NewStorage("local", map[string]string{
    "path": "/var/data/storage",
})
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

The facade pattern provides a centralized, secure API for working with multiple storage backends. It prevents leaky abstractions and ensures consistent validation across all entry points.

#### Benefits

- **Multi-Backend Support**: Work with multiple storage backends simultaneously
- **Backend Routing**: Use `backend:key` syntax to target specific backends
- **Automatic Validation**: Built-in protection against path traversal, injection attacks, and malformed input
- **Centralized API**: Single entry point for all storage operations
- **Security**: Sanitized error messages prevent information disclosure

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

Complete documentation is available in the [docs/](docs/) directory.

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
- [Servers](docs/configuration/grpc-server.md) (gRPC, REST, QUIC, MCP)
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

- [C Client](examples/c_client/) - Using from C applications
- [StorageFS](examples/storagefs/) - Filesystem interface examples

## Project Structure

```
go-objstore/
├── pkg/                        # Core packages
│   ├── factory/               # Backend factory
│   ├── common/                # Shared interfaces and types
│   ├── local/                 # Local filesystem backend
│   ├── s3/                    # Amazon S3 backend
│   ├── gcs/                   # Google Cloud Storage backend
│   ├── azure/                 # Azure Blob Storage backend
│   ├── glacier/               # AWS Glacier archiver
│   ├── azurearchive/          # Azure Archive archiver
│   ├── storagefs/             # Filesystem abstraction
│   ├── cli/                   # CLI commands and config
│   └── server/                # Server implementations
│       ├── grpc/              # gRPC server
│       ├── rest/              # REST API server
│       ├── quic/              # QUIC/HTTP3 server
│       └── mcp/               # MCP server
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
│   └── mcp/                   # MCP server configuration
├── examples/                   # Usage examples
├── test/integration/          # Integration tests
└── docs/                      # Documentation
```

## Development

### Prerequisites

- Go 1.23 or higher
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

# Run server integration tests (gRPC, REST, QUIC, MCP)
make test-servers

# Generate coverage report
make coverage-report

# Check per-package coverage (highlights packages under 90%)
make coverage-check
```

### Test Coverage

Unit tests run quickly with no external dependencies. Integration tests use Docker-based emulators for all servers and backends. CLI integration tests automatically build the CLI binary if not present. Security scanning is performed using gosec and govulncheck. For detailed coverage statistics, see the badge above or run `make coverage-report`.

## Architecture

### Storage Interface

All backends implement a common Storage interface:

```go
type Storage interface {
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

    // Lifecycle management
    AddPolicy(policy LifecyclePolicy) error
    RemovePolicy(id string) error
    GetPolicies() ([]LifecyclePolicy, error)
}
```

### Factory Pattern

The factory pattern provides a uniform way to create backends:

```go
storage, err := factory.NewStorage(backendType, config)
archiver, err := factory.NewArchiver(archiverType, config)
```

This abstracts backend-specific initialization while ensuring a consistent interface.

## Performance

All backends support concurrent read/write operations. Use buffered I/O for better performance. The local backend is fastest for development and testing. Cloud backends have network I/O overhead. See [docs/testing.md](docs/testing.md) for benchmarks.

## Best Practices

1. Always close readers returned by Get() to prevent resource leaks
2. Handle errors from all storage operations
3. Use context for cancellation and timeouts on long operations
4. Enable lifecycle policies for automatic cleanup
5. Choose backends wisely based on cost, performance, and durability needs
6. Use StorageFS when you need standard filesystem operations
7. Test with emulators before deploying to cloud

## Server Interfaces

The project provides flexible server deployment options with both an all-in-one multi-protocol server and individual server binaries for each protocol.

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

# Configure via config file, env vars, or flags
./bin/objstore --config .objstore.yaml put mykey data.txt
```

### All-in-One Multi-Protocol Server

Run all four server protocols simultaneously with a single binary:

```bash
# Start all services (gRPC, REST, QUIC, MCP)
./bin/objstore-server --quic-self-signed

# Customize ports and addresses
./bin/objstore-server \
  --grpc-addr :50051 \
  --rest-port 8080 \
  --quic-addr :4433 \
  --mcp-addr :8081 \
  --quic-self-signed

# Disable specific services
./bin/objstore-server --quic=false --mcp=false

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
