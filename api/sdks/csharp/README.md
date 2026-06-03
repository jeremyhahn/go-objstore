# Go-ObjStore C# SDK

A comprehensive C# SDK for [go-objstore](https://github.com/jeremyhahn/go-objstore) with support for REST, gRPC, QUIC/HTTP3, MCP, and Unix domain socket protocols.

## Transport Support

| Protocol | Description | Auth | TLS Config | Streaming |
|----------|-------------|------|------------|-----------|
| REST | HTTP/1.1 + HTTP/2 | Bearer, Headers, X-Tenant-ID | HttpClient handler | Yes (native) |
| gRPC | HTTP/2 Protobuf | Bearer metadata, Headers, x-tenant-id | AllowInsecureTls, CaCertificatePath | Yes (server-streaming) |
| QUIC | HTTP/3 | Bearer, Headers, X-Tenant-ID | HttpClient handler | Yes (native) |
| MCP | HTTP POST JSON-RPC 2.0 | Bearer, Headers, X-Tenant-ID | HttpClient handler | Buffered |
| Unix | Unix domain socket JSON-RPC 2.0 | Peer-credential (server-side) | N/A | Buffered |

## Features

- **Five Transport Protocols**: REST, gRPC, QUIC/HTTP3, MCP, and Unix domain socket
- **Async/Await Pattern**: Modern asynchronous API design
- **Full API Coverage**: All 19 go-objstore operations supported
- **Streaming**: Native streaming for REST/QUIC/gRPC; buffered for MCP/Unix
- **App-Layer Auth**: Optional bearer token, custom headers, and tenant ID (REST/QUIC/MCP/gRPC)
- **gRPC TLS**: Configurable insecure-skip-verify and custom CA certificate
- **Type-Safe**: Strongly-typed models and interfaces
- **High Test Coverage**: 90%+ code coverage with comprehensive unit and integration tests
- **.NET 9**: Built for modern .NET

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
using var client = ObjectStoreClientFactory.CreateQuicClient("https://localhost:4433");

// Same API as REST and gRPC clients
var etag = await client.PutAsync("test/file.txt", data);
```

### Using MCP Client

MCP (Model Context Protocol) transports object store operations as JSON-RPC 2.0 tool calls over HTTP POST.

```csharp
using var client = ObjectStoreClientFactory.CreateMcpClient("http://localhost:8081");

// Same API as all other clients
var etag = await client.PutAsync("test/file.txt", data);
var (bytes, _) = await client.GetAsync("test/file.txt");
```

### Using Unix Domain Socket Client

The Unix client communicates over a local socket using newline-delimited JSON-RPC 2.0.
Authentication is peer-credential based on the server side; no token is required.

```csharp
using var client = ObjectStoreClientFactory.CreateUnixClient("/var/run/objstore/objstore.sock");

var etag = await client.PutAsync("local/file.bin", data);
var health = await client.HealthAsync();
```

### App-Layer Authentication

All HTTP-based clients (REST, QUIC, MCP) and the gRPC client accept optional auth parameters.
The SDK only transmits caller-supplied credentials; no auth logic is performed.

```csharp
// Bearer token
using var client = new RestClient(
    "http://localhost:8080",
    token: "my-bearer-token");

// Bearer token + tenant ID + custom headers
using var client = new RestClient(
    "http://localhost:8080",
    token: "my-bearer-token",
    tenantId: "acme-corp",
    headers: new Dictionary<string, string> { ["X-App-Version"] = "2.0" });

// gRPC with token, tenant and TLS options
using var client = new GrpcClient(
    "https://localhost:9090",
    allowInsecureTls: false,
    caCertPath: "/etc/ssl/certs/my-ca.pem",
    token: "my-bearer-token",
    tenantId: "acme-corp");
```

### Streaming

```csharp
using var client = ObjectStoreClientFactory.CreateRestClient("http://localhost:8080");

// Stream upload — avoids buffering the entire file in memory
await using var fileStream = File.OpenRead("/large/file.bin");
await client.PutStreamAsync("backups/file.bin", fileStream);

// Stream download — read response as it arrives
var (stream, metadata) = await client.GetStreamAsync("backups/file.bin");
await stream.CopyToAsync(File.OpenWrite("/local/file.bin"));
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

## Error Handling

All SDK errors derive from `ObjectStoreException`, which exposes a nullable `StatusCode`
(the HTTP status code or its equivalent for non-HTTP transports):

```text
ObjectStoreException                  base type, optional StatusCode
├── ValidationException               invalid request (400, gRPC InvalidArgument, JSON-RPC -32602)
├── AuthenticationException           missing/invalid credentials (401, gRPC Unauthenticated, JSON-RPC -32002)
├── AuthorizationException            permission denied (403, gRPC PermissionDenied, JSON-RPC -32001)
├── ObjectNotFoundException           object missing (404, gRPC NotFound, JSON-RPC -32004); carries Key
├── PolicyNotFoundException           lifecycle/replication policy missing (404); carries PolicyId, PolicyType
├── AlreadyExistsException            resource already exists (409, gRPC AlreadyExists, JSON-RPC -32005)
├── RateLimitException                rate limited (429, gRPC ResourceExhausted, JSON-RPC -32029)
├── ConnectionException               transport-level connection failure; carries Endpoint
└── OperationFailedException          any other failure (5xx etc.); carries Operation and StatusCode
```

The mapping is identical across all transports — classification is always by status/error
code, never by message substring:

| Exception | HTTP (REST/QUIC/MCP) | gRPC | JSON-RPC (MCP/Unix) |
|-----------|----------------------|------|---------------------|
| `ValidationException` | 400 | `InvalidArgument` | -32602 |
| `AuthenticationException` | 401 | `Unauthenticated` | -32002 |
| `AuthorizationException` | 403 | `PermissionDenied` | -32001 |
| `ObjectNotFoundException` | 404 | `NotFound` | -32004 |
| `AlreadyExistsException` | 409 | `AlreadyExists` | -32005 |
| `RateLimitException` | 429 | `ResourceExhausted` | -32029 |
| `OperationFailedException` | 5xx / other | other codes | other codes |

```csharp
using ObjStore.SDK.Exceptions;

try
{
    var (data, _) = await client.GetAsync("documents/report.pdf");
}
catch (ObjectNotFoundException ex)
{
    Console.WriteLine($"Missing object: {ex.Key}");
}
catch (AuthenticationException)
{
    // Refresh the bearer token and retry
}
catch (RateLimitException)
{
    // Back off and retry later
}
catch (ObjectStoreException ex)
{
    Console.WriteLine($"Operation failed (HTTP {ex.StatusCode}): {ex.Message}");
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

All client implementations (`RestClient`, `GrpcClient`, `QuicClient`, `McpClient`, `UnixClient`) implement the `IObjectStoreClient` interface:

#### Object Operations

- `Task<string?> PutAsync(string key, byte[] data, ObjectMetadata? metadata = null, CancellationToken cancellationToken = default)`
- `Task<string?> PutWithMetadataAsync(string key, byte[] data, ObjectMetadata metadata, CancellationToken cancellationToken = default)`
- `Task<string?> PutStreamAsync(string key, Stream data, ObjectMetadata? metadata = null, CancellationToken cancellationToken = default)`
- `Task<(byte[] Data, ObjectMetadata? Metadata)> GetAsync(string key, CancellationToken cancellationToken = default)`
- `Task<(Stream Data, ObjectMetadata? Metadata)> GetStreamAsync(string key, CancellationToken cancellationToken = default)`
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
- `Task<TriggerReplicationResult> TriggerReplicationAsync(string? policyId = null, bool parallel = false, int workerCount = 4, CancellationToken cancellationToken = default)`
- `Task<ReplicationStatus?> GetReplicationStatusAsync(string id, CancellationToken cancellationToken = default)`

#### Other Operations

- `Task<HealthResponse> HealthAsync(string? service = null, CancellationToken cancellationToken = default)`
- `Task<bool> ArchiveAsync(string key, string destinationType, Dictionary<string, string>? destinationSettings = null, CancellationToken cancellationToken = default)`

## Testing

The SDK includes comprehensive unit and integration tests:

### Unit Tests

- **RestClientTests**: Tests for REST client operations
- **GrpcClientTests**: Tests for gRPC client operations
- **QuicClientTests**: Tests for QUIC/HTTP3 client operations
- **McpClientTests**: Tests for MCP client operations (mock HttpMessageHandler)
- **UnixClientTests**: Tests for Unix domain socket client operations (in-process mock server)
- **ModelTests**: Tests for data models and serialization
- **ObjectStoreClientFactoryTests**: Tests for client factory (all 5 protocols)

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

### Version 0.3.0

- Canonical error mapping across all transports: added `ValidationException`,
  `AuthenticationException`, `AuthorizationException`, `AlreadyExistsException`
  and `RateLimitException`; REST/QUIC/MCP HTTP statuses, gRPC status codes and
  JSON-RPC error codes now map to the same exception types (see Error Handling)
- Added MCP (Model Context Protocol) transport client (`McpClient`)
- Added Unix domain socket transport client (`UnixClient`)
- Added `PutStreamAsync` and `GetStreamAsync` to all clients and `IObjectStoreClient`
- Added app-layer auth: `Token`, `Headers`, `TenantId` on REST, QUIC, MCP, gRPC
- Fixed gRPC TLS configuration gap: `AllowInsecureTls` and `CaCertificatePath` options
- Extended `Protocol` enum with `MCP` and `Unix` values
- Updated `ObjectStoreClientFactory` with `CreateMcpClient` and `CreateUnixClient`
- Added DI extensions `AddObjectStoreMcpClient` and `AddObjectStoreUnixClient`

### Version 0.2.0

- Go toolchain updated to 1.26.4
- API parity across all SDKs

### Version 0.1.0

- Initial release
- REST, gRPC, and QUIC/HTTP3 protocol support
- Full API coverage for all go-objstore operations
- Comprehensive unit and integration tests
- 90%+ code coverage
