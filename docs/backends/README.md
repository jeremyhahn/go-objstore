# Storage Backends

> **Status: NORMATIVE** · Scope: go-objstore · Last verified: 2026-11-12
> Conflicts with this document are defects. Fix them here first.

go-objstore provides a unified interface for multiple storage backends, letting you switch between local storage and cloud providers with no code changes.

## Supported Backends

### Local Storage
- **Backend ID**: `local`
- **Configuration**: `{"path": "/path/to/storage"}`
- **Required**: `path`; a missing one returns `common.ErrPathNotSet`
- **Use Case**: Development, testing, local file storage
- **Features**: Full feature support including lifecycle policies

### Memory
- **Backend ID**: `memory`
- **Configuration**: `{}` (no required settings)
- **Use Case**: In-process storage for tests and caches; contents are lost when the process exits
- **Features**: Same `Storage` interface as the other backends, held in process memory

### Amazon S3
- **Backend ID**: `s3`
- **Configuration**: `{"region": "us-east-1", "bucket": "my-bucket"}`
- **Credentials**: Uses AWS SDK credential chain (env vars, ~/.aws/credentials, IAM roles)
- **Features**: Full S3 API support, lifecycle policies

### Google Cloud Storage (GCS)
- **Backend ID**: `gcs`
- **Configuration**: `{"bucket": "my-gcs-bucket"}` (optional: `project`)
- **Required**: `bucket`; a missing one returns `common.ErrBucketNotSet`
- **Credentials**: Uses Google Application Default Credentials
- **Features**: Full GCS API support, lifecycle policies

### Azure Blob Storage
- **Backend ID**: `azure`
- **Configuration**: `{"accountName": "myaccount", "containerName": "mycontainer"}`
- **Required**: `accountName`, `containerName` and the account key in `Credentials.Secret`; a missing one returns `common.ErrAccountNotSet`
- **Credentials**: the account key is passed as `common.Credentials{Secret: handle}`, never in the settings map
- **Features**: Full Azure Blob API support, lifecycle policies

## Archive-Only Backends

These backends can only be used as archive destinations, not as primary storage:

### AWS Glacier
- **Backend ID**: `glacier`
- **Configuration**: `{"region": "us-east-1", "vault": "my-vault"}`
- **Use Case**: Long-term archival storage
- **Retrieval**: Requires restore process before access

### Azure Archive
- **Backend ID**: `azurearchive`
- **Configuration**: `{"accountName": "myaccount", "containerName": "archive"}`
- **Required**: `accountName`, `containerName` and the account key in `Credentials.Secret`; a missing one returns `common.ErrAccountNotSet`
- **Credentials**: the account key is passed as `common.Credentials{Secret: handle}`, never in the settings map
- **Use Case**: Cost-effective long-term storage
- **Access Tier**: Archive tier with delayed retrieval

## Build tags

`local` and `memory` are always registered. The other backends are registered
with the factory only when the binary is built with their tag:

| Backend ID | Build tag |
|------------|-----------|
| `s3` | `awss3` |
| `gcs` | `gcpstorage` |
| `azure` | `azureblob` |
| `glacier` | `glacier` |
| `azurearchive` | `azurearchive` |

A backend whose tag was not set is unknown to the factory:
`factory.NewStorage` returns `factory.ErrUnknownBackend` for it.

## Usage Examples

### Creating a Backend

```go
import (
    "github.com/jeremyhahn/go-objstore/pkg/common"
    "github.com/jeremyhahn/go-objstore/pkg/factory"
)

// Local storage
storage, err := factory.NewStorage("local", map[string]string{
    "path": "/var/data",
}, common.Credentials{})

// S3
storage, err := factory.NewStorage("s3", map[string]string{
    "region": "us-east-1",
    "bucket": "my-bucket",
}, common.Credentials{})

// GCS
storage, err := factory.NewStorage("gcs", map[string]string{
    "bucket": "my-gcs-bucket",
}, common.Credentials{})

// Azure
storage, err := factory.NewStorage("azure", map[string]string{
    "accountName": "myaccount",
    "containerName": "mycontainer",
}, common.Credentials{Secret: accountKey}) // accountKey is an enclave.ByteAccess
```

Credentials are go-enclave handles (`enclave.ByteAccess`), never settings
values: an `accessKey`, `secretKey` or `accountKey` entry in the map is rejected
with `common.ErrCredentialInSettings`. S3 reads `Key` (access key) and `Secret`
(secret key), and a nil field uses the SDK's default credential chain; Azure
and Azure Archive read `Secret` (account key) and require it. See
[Protected Memory](../architecture/protected-memory.md).

The handle is the credential path wherever a credential is accepted:

- **Go API:** pass `common.Credentials` to `Configure` and the factory.
- **Server archive:** the REST, gRPC, QUIC, unix and MCP servers still accept
  credential entries in `destination_settings`; at the API boundary they are
  moved into a handle that is destroyed when the call ends, and the backend's
  settings hold no credential. The wire format is unchanged.
- **C library:** `ObjstoreNewStorageWithCredentials` takes the key and secret as
  buffers and copies them into a handle for the call. The caller wipes its own
  buffers. See the [C client](../c_client/README.md).
- **Replication policies:** persisted policies hold no credential and
  `AddPolicy` refuses one in their settings; the embedding application supplies
  a `replication.CredentialProvider` that yields a handle per target at sync
  time, and without one the SDK default chain applies. See
  [Replication](../replication/README.md).

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

1. **Pass credentials as handles** in `common.Credentials`, never in the settings map (a credential entry there returns `common.ErrCredentialInSettings`)
2. **Validate configuration** before creating storage backend
3. **Handle errors** from Configure() method
4. **Use appropriate backend** for your durability and cost requirements

## Error Handling

Errors are typed: compare them with `errors.Is`, never by their text.

```go
storage, err := factory.NewStorage(backendType, settings, creds)
switch {
case errors.Is(err, factory.ErrUnknownBackend):
    // backend ID not registered (unknown, or built without its tag)
case errors.Is(err, factory.ErrArchiveOnlyBackend):
    // glacier and azurearchive are archive destinations, not storage
case errors.Is(err, common.ErrCredentialInSettings):
    // a credential was put in the settings map; pass it in common.Credentials
case errors.Is(err, common.ErrPathNotSet),
    errors.Is(err, common.ErrBucketNotSet),
    errors.Is(err, common.ErrAccountNotSet):
    // a required setting (or the Azure account key) is missing
case err != nil:
    // other configuration or client error
}

reader, err := storage.Get("path/to/file.txt")
if errors.Is(err, common.ErrKeyNotFound) {
    // no object at this key
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
