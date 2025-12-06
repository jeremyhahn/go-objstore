# Go-ObjStore C# SDK

A comprehensive C# SDK for [go-objstore](https://github.com/jeremyhahn/go-objstore) with support for REST, gRPC, and QUIC/HTTP3 protocols.

## Features

- **Multiple Protocol Support**: REST, gRPC, and QUIC/HTTP3
- **Async/Await Pattern**: Modern asynchronous API design
- **Full API Coverage**: All go-objstore operations supported
- **Type-Safe**: Strongly-typed models and interfaces
- **High Test Coverage**: 90%+ code coverage with comprehensive unit and integration tests
- **.NET 8+**: Built for modern .NET

## Installation

### From NuGet (when published)

```bash
dotnet add package ObjStore.SDK
```

### From Source

```bash
git clone https://github.com/jeremyhahn/go-objstore.git
cd go-objstore/api/sdks/csharp
dotnet build
```

## Quick Start

### Using REST Client

```csharp
using ObjStore.SDK;
using ObjStore.SDK.Models;
using System.Text;

// Create a REST client
using var client = ObjectStoreClientFactory.CreateRestClient("http://localhost:8080");

// Put an object
var data = Encoding.UTF8.GetBytes("Hello, World!");
var metadata = new ObjectMetadata
{
    ContentType = "text/plain",
    Custom = new Dictionary<string, string>
    {
        ["author"] = "John Doe",
        ["department"] = "Engineering"
    }
};

var etag = await client.PutAsync("documents/hello.txt", data, metadata);
Console.WriteLine($"Object stored with ETag: {etag}");

// Get an object
var (retrievedData, retrievedMetadata) = await client.GetAsync("documents/hello.txt");
var content = Encoding.UTF8.GetString(retrievedData);
Console.WriteLine($"Retrieved: {content}");

// List objects
var listResponse = await client.ListAsync(prefix: "documents/");
foreach (var obj in listResponse.Objects)
{
    Console.WriteLine($"Found: {obj.Key} ({obj.Metadata?.Size} bytes)");
}

// Delete an object
await client.DeleteAsync("documents/hello.txt");
```

### Using gRPC Client

```csharp
using var client = ObjectStoreClientFactory.CreateGrpcClient("http://localhost:9090");

// Same API as REST client
var etag = await client.PutAsync("test/file.txt", data);
var (retrievedData, metadata) = await client.GetAsync("test/file.txt");
```

### Using QUIC/HTTP3 Client

```csharp
using var client = ObjectStoreClientFactory.CreateQuicClient("https://localhost:8443");

// Same API as REST and gRPC clients
var etag = await client.PutAsync("test/file.txt", data);
```

## Advanced Usage

### Lifecycle Policies

```csharp
using var client = ObjectStoreClientFactory.CreateRestClient("http://localhost:8080");

// Add a lifecycle policy
var policy = new LifecyclePolicy
{
    Id = "archive-old-logs",
    Prefix = "logs/",
    RetentionSeconds = 86400 * 30, // 30 days
    Action = "delete"
};

await client.AddPolicyAsync(policy);

// Get all policies
var policies = await client.GetPoliciesAsync();

// Apply policies
var (success, policiesCount, objectsProcessed) = await client.ApplyPoliciesAsync();
Console.WriteLine($"Applied {policiesCount} policies to {objectsProcessed} objects");

// Remove a policy
await client.RemovePolicyAsync("archive-old-logs");
```

### Replication Policies

```csharp
using var client = ObjectStoreClientFactory.CreateRestClient("http://localhost:8080");

// Add a replication policy
var replicationPolicy = new ReplicationPolicy
{
    Id = "replicate-to-backup",
    SourceBackend = "s3",
    SourceSettings = new Dictionary<string, string>
    {
        ["bucket"] = "primary-bucket",
        ["region"] = "us-east-1"
    },
    DestinationBackend = "gcs",
    DestinationSettings = new Dictionary<string, string>
    {
        ["bucket"] = "backup-bucket"
    },
    CheckIntervalSeconds = 300,
    Enabled = true,
    ReplicationMode = ReplicationMode.Transparent
};

await client.AddReplicationPolicyAsync(replicationPolicy);

// Trigger replication
await client.TriggerReplicationAsync(policyId: "replicate-to-backup");

// Get replication status
var status = await client.GetReplicationStatusAsync("replicate-to-backup");
Console.WriteLine($"Synced: {status.TotalObjectsSynced}, Errors: {status.TotalErrors}");
```

### Archive Objects

```csharp
using var client = ObjectStoreClientFactory.CreateRestClient("http://localhost:8080");

// Archive an object to a different backend
await client.ArchiveAsync(
    key: "important/document.pdf",
    destinationType: "glacier",
    destinationSettings: new Dictionary<string, string>
    {
        ["vault"] = "archive-vault",
        ["region"] = "us-west-2"
    }
);
```

### Metadata Operations

```csharp
using var client = ObjectStoreClientFactory.CreateRestClient("http://localhost:8080");

// Get metadata without downloading content
var metadata = await client.GetMetadataAsync("documents/large-file.bin");
Console.WriteLine($"Size: {metadata.Size} bytes");
Console.WriteLine($"Last Modified: {metadata.LastModified}");

// Update metadata
var updatedMetadata = new ObjectMetadata
{
    ContentType = "application/json",
    Custom = new Dictionary<string, string>
    {
        ["version"] = "2.0",
        ["updated"] = DateTime.UtcNow.ToString("o")
    }
};
await client.UpdateMetadataAsync("documents/config.json", updatedMetadata);
```

### Health Checks

```csharp
using var client = ObjectStoreClientFactory.CreateRestClient("http://localhost:8080");

var health = await client.HealthAsync();
if (health.Status == HealthStatus.Serving)
{
    Console.WriteLine("Service is healthy");
}
```

## Building from Source

### Prerequisites

- .NET 8.0 SDK or later
- Docker (for integration tests)
- Make (optional, for using Makefile)

### Build Commands

```bash
# Restore packages
make restore

# Build
make build

# Run unit tests
make test

# Run integration tests (requires Docker with go-objstore image)
make integration-test

# Generate code coverage report
make coverage

# Run all tests and generate coverage
make all

# Clean build artifacts
make clean

# Create NuGet package
make package
```

## API Reference

### IObjectStoreClient Interface

All client implementations (`RestClient`, `GrpcClient`, `QuicClient`) implement the `IObjectStoreClient` interface:

#### Object Operations

- `Task<string?> PutAsync(string key, byte[] data, ObjectMetadata? metadata = null, CancellationToken cancellationToken = default)`
- `Task<(byte[] Data, ObjectMetadata? Metadata)> GetAsync(string key, CancellationToken cancellationToken = default)`
- `Task<bool> DeleteAsync(string key, CancellationToken cancellationToken = default)`
- `Task<ListObjectsResponse> ListAsync(string? prefix = null, string? delimiter = null, int? maxResults = null, string? continueFrom = null, CancellationToken cancellationToken = default)`
- `Task<bool> ExistsAsync(string key, CancellationToken cancellationToken = default)`

#### Metadata Operations

- `Task<ObjectMetadata?> GetMetadataAsync(string key, CancellationToken cancellationToken = default)`
- `Task<bool> UpdateMetadataAsync(string key, ObjectMetadata metadata, CancellationToken cancellationToken = default)`

#### Lifecycle Operations

- `Task<bool> AddPolicyAsync(LifecyclePolicy policy, CancellationToken cancellationToken = default)`
- `Task<bool> RemovePolicyAsync(string id, CancellationToken cancellationToken = default)`
- `Task<List<LifecyclePolicy>> GetPoliciesAsync(string? prefix = null, CancellationToken cancellationToken = default)`
- `Task<(bool Success, int PoliciesCount, int ObjectsProcessed)> ApplyPoliciesAsync(CancellationToken cancellationToken = default)`

#### Replication Operations

- `Task<bool> AddReplicationPolicyAsync(ReplicationPolicy policy, CancellationToken cancellationToken = default)`
- `Task<bool> RemoveReplicationPolicyAsync(string id, CancellationToken cancellationToken = default)`
- `Task<List<ReplicationPolicy>> GetReplicationPoliciesAsync(CancellationToken cancellationToken = default)`
- `Task<ReplicationPolicy?> GetReplicationPolicyAsync(string id, CancellationToken cancellationToken = default)`
- `Task<bool> TriggerReplicationAsync(string? policyId = null, bool parallel = false, int workerCount = 4, CancellationToken cancellationToken = default)`
- `Task<ReplicationStatus?> GetReplicationStatusAsync(string id, CancellationToken cancellationToken = default)`

#### Other Operations

- `Task<HealthResponse> HealthAsync(string? service = null, CancellationToken cancellationToken = default)`
- `Task<bool> ArchiveAsync(string key, string destinationType, Dictionary<string, string>? destinationSettings = null, CancellationToken cancellationToken = default)`

## Testing

The SDK includes comprehensive unit and integration tests:

### Unit Tests

- **RestClientTests**: Tests for REST client operations
- **ModelTests**: Tests for data models and serialization
- **ObjectStoreClientFactoryTests**: Tests for client factory

Run unit tests:
```bash
make test
# or
dotnet test --filter "FullyQualifiedName~Unit"
```

### Integration Tests

Integration tests require Docker with the go-objstore image:

```bash
# Build go-objstore Docker image first
cd /path/to/go-objstore
make docker-build

# Run integration tests
cd api/sdks/csharp
make integration-test
```

## Code Coverage

The SDK maintains 90%+ code coverage. Generate coverage reports:

```bash
make coverage-report
```

This generates an HTML report in `./coverage/report/index.html`.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass and coverage remains above 90%
5. Submit a pull request

## License

This SDK follows the same license as go-objstore: AGPL-3.0

For commercial licensing options, see [LICENSE-COMMERCIAL.md](../../../LICENSE-COMMERCIAL.md)

## Support

- Issues: [GitHub Issues](https://github.com/jeremyhahn/go-objstore/issues)
- Documentation: [Go-ObjStore Docs](https://github.com/jeremyhahn/go-objstore)

## Changelog

### Version 0.1.0

- Initial release
- REST, gRPC, and QUIC/HTTP3 protocol support
- Full API coverage for all go-objstore operations
- Comprehensive unit and integration tests
- 90%+ code coverage
