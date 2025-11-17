# Storage Backends

go-objstore provides a unified interface for multiple storage backends, allowing you to seamlessly switch between local storage and cloud providers.

## Supported Backends

### Local Storage
- **Backend ID**: `local`
- **Configuration**: `{"path": "/path/to/storage"}`
- **Use Case**: Development, testing, local file storage
- **Features**: Full feature support including lifecycle policies

### Amazon S3
- **Backend ID**: `s3`
- **Configuration**: `{"region": "us-east-1", "bucket": "my-bucket"}`
- **Credentials**: Uses AWS SDK credential chain (env vars, ~/.aws/credentials, IAM roles)
- **Features**: Full S3 API support, lifecycle policies

### Google Cloud Storage (GCS)
- **Backend ID**: `gcs`
- **Configuration**: `{"bucket": "my-gcs-bucket"}`
- **Credentials**: Uses Google Application Default Credentials
- **Features**: Full GCS API support, lifecycle policies

### Azure Blob Storage
- **Backend ID**: `azure`
- **Configuration**: `{"accountName": "myaccount", "accountKey": "key==", "containerName": "mycontainer"}`
- **Features**: Full Azure Blob API support, lifecycle policies

## Archive-Only Backends

These backends can only be used as archive destinations, not as primary storage:

### AWS Glacier
- **Backend ID**: `glacier`
- **Configuration**: `{"region": "us-east-1", "vault": "my-vault"}`
- **Use Case**: Long-term archival storage
- **Retrieval**: Requires restore process before access

### Azure Archive
- **Backend ID**: `azure-archive`
- **Configuration**: `{"accountName": "myaccount", "accountKey": "key==", "containerName": "archive"}`
- **Use Case**: Cost-effective long-term storage
- **Access Tier**: Archive tier with delayed retrieval

## Usage Examples

### Creating a Backend

```go
import "go-objstore/pkg/factory"

// Local storage
storage, err := factory.NewStorage("local", map[string]string{
    "path": "/var/data",
})

// S3
storage, err := factory.NewStorage("s3", map[string]string{
    "region": "us-east-1",
    "bucket": "my-bucket",
})

// GCS
storage, err := factory.NewStorage("gcs", map[string]string{
    "bucket": "my-gcs-bucket",
})

// Azure
storage, err := factory.NewStorage("azure", map[string]string{
    "accountName": "myaccount",
    "accountKey":   "base64key==",
    "containerName": "mycontainer",
})
```

### Basic Operations

All backends implement the same `Storage` interface:

```go
// Store data
err := storage.Put("path/to/file.txt", bytes.NewReader(data))

// Retrieve data
reader, err := storage.Get("path/to/file.txt")
defer reader.Close()
data, _ := io.ReadAll(reader)

// Delete data
err := storage.Delete("path/to/file.txt")

// List keys with prefix
keys, err := storage.List("path/to/")
```

## Backend Selection Guide

| Backend | Cost | Speed | Durability | Use Case |
|---------|------|-------|------------|----------|
| Local | Low | Fastest | Single machine | Development, testing |
| S3 | Medium | Fast | 99.999999999% | Production, general purpose |
| GCS | Medium | Fast | 99.999999999% | Production, Google Cloud |
| Azure | Medium | Fast | High | Production, Azure ecosystem |
| Glacier | Lowest | Slow (hours) | 99.999999999% | Long-term archive |
| Azure Archive | Lowest | Slow (hours) | High | Long-term archive |

## Configuration Best Practices

1. **Use environment variables** for credentials rather than hardcoding
2. **Validate configuration** before creating storage backend
3. **Handle errors** from Configure() method
4. **Test connectivity** after backend creation
5. **Use appropriate backend** for your durability and cost requirements

## Error Handling

```go
storage, err := factory.NewStorage("s3", config)
if err != nil {
    if strings.Contains(err.Error(), "credentials") {
        // Handle credential errors
    }
    if strings.Contains(err.Error(), "bucket") {
        // Handle bucket configuration errors
    }
}
```

## Thread Safety

All backend implementations are thread-safe and can be used concurrently from multiple goroutines.

## Resource Cleanup

Backends don't require explicit cleanup, but ensure you close all readers returned by `Get()`:

```go
reader, err := storage.Get(key)
if err != nil {
    return err
}
defer reader.Close()  // Always close readers

data, err := io.ReadAll(reader)
```

## See Also

- [Lifecycle Policies](../lifecycle/README.md)
- [StorageFS Filesystem Abstraction](../storagefs/README.md)
