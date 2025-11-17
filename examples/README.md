# go-objstore Examples

Practical examples demonstrating how to use go-objstore in your applications.

## Available Examples

### 1. Basic Usage (`basic-usage/`)

Demonstrates fundamental operations with different storage backends:
- Creating storage backends (local, S3, GCS, Azure)
- Basic CRUD operations (Put, Get, Delete, List)
- Context-aware operations with timeouts and cancellation
- Metadata management
- List operations with pagination

**Run:**
```bash
cd basic-usage
go run main.go
```

### 2. StorageFS (`storagefs/`)

Shows how to use object storage with a filesystem interface:
- Creating filesystems on top of object storage
- File operations (Create, Open, Read, Write)
- Directory operations (Mkdir, ReadDir)
- File metadata and stats
- Copying files between different storage backends

**Run:**
```bash
cd storagefs
go run main.go
```

### 3. Lifecycle Policies (`lifecycle-policies/`)

Demonstrates automatic data retention and archival:
- Creating delete policies for old data
- Setting up archive policies to move data to cold storage
- Managing multiple policies
- Multi-tier archival strategies
- Lifecycle manager implementation

**Run:**
```bash
cd lifecycle-policies
go run main.go
```

### 4. C Client (`c_client/`)

Shows how to use go-objstore from C applications:
- Building the shared library
- Simple C usage example
- Comprehensive test program
- Error handling in C

**Build and Run:**
```bash
cd c_client
make
./simple_example
./test_objstore
```

See [c_client/README.md](c_client/README.md) for detailed instructions.

## Quick Reference

### Creating Storage Backends

```go
import "github.com/jeremyhahn/go-objstore/pkg/factory"

// Local storage
local, _ := factory.NewStorage("local", map[string]string{
    "path": "/tmp/storage",
})

// Amazon S3
s3, _ := factory.NewStorage("s3", map[string]string{
    "bucket": "my-bucket",
    "region": "us-east-1",
})

// Google Cloud Storage
gcs, _ := factory.NewStorage("gcs", map[string]string{
    "bucket": "my-gcs-bucket",
})

// Azure Blob Storage
azure, _ := factory.NewStorage("azure", map[string]string{
    "accountName":   "myaccount",
    "accountKey":    "key==",
    "containerName": "mycontainer",
})
```

### Basic Operations

```go
import (
    "bytes"
    "io"
)

// Put (store)
data := []byte("content")
storage.Put("file.txt", bytes.NewReader(data))

// Get (retrieve)
reader, _ := storage.Get("file.txt")
content, _ := io.ReadAll(reader)
reader.Close()

// Delete
storage.Delete("file.txt")

// List
keys, _ := storage.List("prefix/")

// Exists
exists, _ := storage.Exists(ctx, "file.txt")
```

### Using StorageFS

```go
import "github.com/jeremyhahn/go-objstore/pkg/storagefs"

fs := storagefs.New(storage)

// Create file
file, _ := fs.Create("test.txt")
file.WriteString("content")
file.Close()

// Read file
content, _ := fs.ReadFile("test.txt")

// Create directory
fs.MkdirAll("path/to/dir", 0755)

// List directory
entries, _ := fs.ReadDir("path/to/dir")
```

### Lifecycle Policies

```go
import (
    "time"
    "github.com/jeremyhahn/go-objstore/pkg/common"
)

// Delete policy
deletePolicy := common.LifecyclePolicy{
    ID:        "cleanup-logs",
    Prefix:    "logs/",
    Action:    "delete",
    Retention: 30 * 24 * time.Hour,
}
storage.AddPolicy(deletePolicy)

// Archive policy
archiver, _ := factory.NewArchiver("glacier", map[string]string{
    "vaultName": "archive",
    "region":    "us-east-1",
})

archivePolicy := common.LifecyclePolicy{
    ID:          "archive-old-data",
    Prefix:      "data/",
    Action:      "archive",
    Destination: archiver,
    Retention:   90 * 24 * time.Hour,
}
storage.AddPolicy(archivePolicy)
```

## Running Examples Locally

### Prerequisites

- Go 1.23 or higher
- For S3 examples: MinIO (optional)
- For C examples: GCC compiler

### Setup MinIO for S3 Examples

```bash
# Run MinIO in Docker
docker run -d -p 9000:9000 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data

# Create a bucket
docker exec <container-id> mc mb /data/demo-bucket
```

### Building C Examples

```bash
# First build the shared library from project root
cd /home/jhahn/sources/go-objstore
make lib

# Then build C examples
cd examples/c_client
make
```

## Integration with Your Application

### Example: Web Application

```go
package main

import (
    "net/http"
    "io"
    "github.com/jeremyhahn/go-objstore/pkg/factory"
    "github.com/jeremyhahn/go-objstore/pkg/common"
)

var storage common.Storage

func init() {
    // Initialize storage backend
    storage, _ = factory.NewStorage("s3", map[string]string{
        "bucket": "uploads",
        "region": "us-east-1",
    })
}

func uploadHandler(w http.ResponseWriter, r *http.Request) {
    file, header, _ := r.FormFile("file")
    defer file.Close()

    // Store uploaded file
    key := "uploads/" + header.Filename
    storage.Put(key, file)

    w.Write([]byte("File uploaded successfully"))
}

func downloadHandler(w http.ResponseWriter, r *http.Request) {
    key := r.URL.Query().Get("file")

    // Retrieve file
    reader, err := storage.Get(key)
    if err != nil {
        http.Error(w, "File not found", 404)
        return
    }
    defer reader.Close()

    io.Copy(w, reader)
}

func main() {
    http.HandleFunc("/upload", uploadHandler)
    http.HandleFunc("/download", downloadHandler)
    http.ListenAndServe(":8080", nil)
}
```

### Example: CLI Tool

```go
package main

import (
    "flag"
    "fmt"
    "os"
    "io"
    "github.com/jeremyhahn/go-objstore/pkg/factory"
)

func main() {
    backend := flag.String("backend", "local", "Storage backend")
    path := flag.String("path", "/tmp/storage", "Storage path")
    flag.Parse()

    storage, _ := factory.NewStorage(*backend, map[string]string{
        "path": *path,
    })

    command := flag.Arg(0)
    switch command {
    case "put":
        key := flag.Arg(1)
        file, _ := os.Open(flag.Arg(2))
        defer file.Close()
        storage.Put(key, file)
        fmt.Println("Stored:", key)

    case "get":
        key := flag.Arg(1)
        reader, _ := storage.Get(key)
        io.Copy(os.Stdout, reader)
        reader.Close()

    case "list":
        prefix := flag.Arg(1)
        keys, _ := storage.List(prefix)
        for _, key := range keys {
            fmt.Println(key)
        }
    }
}
```

## Best Practices

1. **Always close readers** to prevent resource leaks
2. **Handle errors** from all storage operations
3. **Use contexts** for timeout and cancellation
4. **Implement retry logic** for cloud backends
5. **Use lifecycle policies** for automatic cleanup
6. **Test with emulators** before deploying to production

## Troubleshooting

### Common Issues

**"Failed to create storage"**
- Check credentials are set correctly
- For cloud backends, verify credentials environment variables
- For local backend, ensure directory exists and is writable

**"Connection refused" with S3**
- Ensure MinIO is running if using local S3
- Check endpoint URL is correct
- Verify network connectivity

**"Permission denied"**
- Check AWS credentials have correct IAM permissions
- For local storage, verify file system permissions
- For Azure, check account key is valid

### Getting Help

- **Documentation:** [../docs/](../docs/)
- **Testing Guide:** [../docs/testing.md](../docs/testing.md)

## Contributing Examples

Have a useful example? Contributions are welcome!

1. Create a new directory under `examples/`
2. Add a clear, well-commented `main.go`
3. Update this README with your example
4. Submit a pull request

## License

These examples are part of the go-objstore project and are licensed under the GNU Affero General Public License v3.0 (AGPL-3.0).
