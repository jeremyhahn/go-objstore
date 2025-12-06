# Go Client SDK for go-objstore

[![Go Report Card](https://goreportcard.com/badge/github.com/jeremyhahn/go-objstore)](https://goreportcard.com/report/github.com/jeremyhahn/go-objstore)
[![License: AGPL v3](https://img.shields.io/badge/License-AGPL%20v3-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)

A comprehensive Go client SDK for [go-objstore](https://github.com/jeremyhahn/go-objstore) supporting REST, gRPC, and QUIC/HTTP3 protocols.

## Features

- **Multi-Protocol Support**: REST, gRPC, and QUIC/HTTP3
- **Unified Interface**: Single API across all protocols
- **Full Feature Coverage**: All object storage operations including:
  - Object CRUD operations (Put, Get, Delete, List)
  - Metadata management
  - Lifecycle policies
  - Replication policies
  - Archive operations
  - Health checks
- **Retry Logic with Exponential Backoff**: Configurable retry behavior for transient failures
- **Input Validation**: Automatic validation of parameters before making requests
- **Robust Error Handling**: Consistent error wrapping with sentinel errors for easy error checking
- **TLS Support**: Secure connections with certificate validation
- **Context Support**: Proper context propagation for timeouts and cancellation
- **Type Safety**: Strong typing with Go structs and interfaces
- **Well-Tested**: 90%+ code coverage with unit and integration tests

## Installation

```bash
go get github.com/jeremyhahn/go-objstore/api/sdks/go
```

## Quick Start

### gRPC Client

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    objstore "github.com/jeremyhahn/go-objstore/api/sdks/go"
)

func main() {
    // Create gRPC client
    config := &objstore.ClientConfig{
        Protocol:          objstore.ProtocolGRPC,
        Address:           "localhost:50051",
        ConnectionTimeout: 10 * time.Second,
        RequestTimeout:    30 * time.Second,
    }

    client, err := objstore.NewClient(config)
    if err != nil {
        log.Fatalf("Failed to create client: %v", err)
    }
    defer client.Close()

    ctx := context.Background()

    // Put an object
    data := []byte("Hello, World!")
    metadata := &objstore.Metadata{
        ContentType: "text/plain",
        Custom: map[string]string{
            "author": "john.doe",
        },
    }

    result, err := client.Put(ctx, "my-object", data, metadata)
    if err != nil {
        log.Fatalf("Failed to put object: %v", err)
    }
    fmt.Printf("Object stored with ETag: %s\n", result.ETag)

    // Get the object
    getResult, err := client.Get(ctx, "my-object")
    if err != nil {
        log.Fatalf("Failed to get object: %v", err)
    }
    fmt.Printf("Retrieved data: %s\n", string(getResult.Data))
}
```

### REST Client

```go
config := &objstore.ClientConfig{
    Protocol:       objstore.ProtocolREST,
    Address:        "localhost:8080",
    RequestTimeout: 30 * time.Second,
}

client, err := objstore.NewClient(config)
if err != nil {
    log.Fatalf("Failed to create client: %v", err)
}
defer client.Close()

// Use the same API as gRPC client
result, err := client.Put(ctx, "my-object", data, metadata)
```

### QUIC/HTTP3 Client

```go
config := &objstore.ClientConfig{
    Protocol:           objstore.ProtocolQUIC,
    Address:            "localhost:4433",
    UseTLS:             true,
    InsecureSkipVerify: true, // For testing only!
    ConnectionTimeout:  10 * time.Second,
    RequestTimeout:     30 * time.Second,
}

client, err := objstore.NewClient(config)
if err != nil {
    log.Fatalf("Failed to create client: %v", err)
}
defer client.Close()
```

## TLS Configuration

### With Certificate Files

```go
config := &objstore.ClientConfig{
    Protocol: objstore.ProtocolGRPC,
    Address:  "secure-server.example.com:50051",
    UseTLS:   true,
    CAFile:   "/path/to/ca.pem",
    CertFile: "/path/to/client-cert.pem",
    KeyFile:  "/path/to/client-key.pem",
}

client, err := objstore.NewClient(config)
```

### Skip Verification (Testing Only)

```go
config := &objstore.ClientConfig{
    Protocol:           objstore.ProtocolGRPC,
    Address:            "localhost:50051",
    UseTLS:             true,
    InsecureSkipVerify: true, // ⚠️ DO NOT use in production!
}
```

## Examples

### List Objects

```go
opts := &objstore.ListOptions{
    Prefix:     "documents/",
    Delimiter:  "/",
    MaxResults: 100,
}

result, err := client.List(ctx, opts)
if err != nil {
    log.Fatalf("Failed to list objects: %v", err)
}

for _, obj := range result.Objects {
    fmt.Printf("Key: %s, Size: %d bytes\n", obj.Key, obj.Metadata.Size)
}

// Handle pagination
if result.Truncated {
    opts.ContinueFrom = result.NextToken
    // Continue listing...
}
```

### Check Object Existence

```go
exists, err := client.Exists(ctx, "my-object")
if err != nil {
    log.Fatalf("Failed to check existence: %v", err)
}

if exists {
    fmt.Println("Object exists!")
}
```

### Metadata Operations

```go
// Get metadata only (without downloading object)
metadata, err := client.GetMetadata(ctx, "my-object")
if err != nil {
    log.Fatalf("Failed to get metadata: %v", err)
}
fmt.Printf("Size: %d, ETag: %s\n", metadata.Size, metadata.ETag)

// Update metadata
newMetadata := &objstore.Metadata{
    ContentType: "application/json",
    Custom: map[string]string{
        "version": "2.0",
    },
}

err = client.UpdateMetadata(ctx, "my-object", newMetadata)
if err != nil {
    log.Fatalf("Failed to update metadata: %v", err)
}
```

### Lifecycle Policies

```go
policy := &objstore.LifecyclePolicy{
    ID:               "delete-old-logs",
    Prefix:           "logs/",
    RetentionSeconds: 86400 * 30, // 30 days
    Action:           "delete",
}

// Add policy
err = client.AddPolicy(ctx, policy)
if err != nil {
    log.Fatalf("Failed to add policy: %v", err)
}

// Get all policies
policies, err := client.GetPolicies(ctx, "")
if err != nil {
    log.Fatalf("Failed to get policies: %v", err)
}

// Apply policies
result, err := client.ApplyPolicies(ctx)
if err != nil {
    log.Fatalf("Failed to apply policies: %v", err)
}
fmt.Printf("Processed %d objects\n", result.ObjectsProcessed)

// Remove policy
err = client.RemovePolicy(ctx, "delete-old-logs")
```

### Replication

```go
policy := &objstore.ReplicationPolicy{
    ID:            "backup-to-s3",
    SourceBackend: "local",
    SourceSettings: map[string]string{
        "path": "/data/primary",
    },
    SourcePrefix:       "important/",
    DestinationBackend: "s3",
    DestinationSettings: map[string]string{
        "bucket": "backup-bucket",
        "region": "us-east-1",
    },
    CheckIntervalSeconds: 3600,
    Enabled:              true,
}

// Add replication policy
err = client.AddReplicationPolicy(ctx, policy)
if err != nil {
    log.Fatalf("Failed to add replication policy: %v", err)
}

// Trigger immediate replication
result, err := client.TriggerReplication(ctx, &objstore.TriggerReplicationOptions{
    PolicyID:    "backup-to-s3",
    Parallel:    true,
    WorkerCount: 4,
})
if err != nil {
    log.Fatalf("Failed to trigger replication: %v", err)
}
fmt.Printf("Synced: %d, Failed: %d\n", result.Synced, result.Failed)

// Get replication status
status, err := client.GetReplicationStatus(ctx, "backup-to-s3")
if err != nil {
    log.Fatalf("Failed to get status: %v", err)
}
fmt.Printf("Total synced: %d objects, %d bytes\n",
    status.TotalObjectsSynced, status.TotalBytesSynced)
```

### Archive Operations

```go
// Archive to Glacier
err = client.Archive(ctx, "old-data.zip", "glacier", map[string]string{
    "vault":  "archive-vault",
    "region": "us-west-2",
})
if err != nil {
    log.Fatalf("Failed to archive: %v", err)
}
```

### Health Check

```go
health, err := client.Health(ctx)
if err != nil {
    log.Fatalf("Health check failed: %v", err)
}

if health.Status == "SERVING" {
    fmt.Println("Server is healthy")
}
```

## Error Handling

The SDK provides typed errors for common scenarios:

```go
result, err := client.Get(ctx, "nonexistent-key")
if err != nil {
    if err == objstore.ErrObjectNotFound {
        fmt.Println("Object does not exist")
    } else if err == objstore.ErrConnectionFailed {
        fmt.Println("Failed to connect to server")
    } else {
        log.Fatalf("Unexpected error: %v", err)
    }
}
```

Available error constants:
- `ErrInvalidProtocol`
- `ErrConnectionFailed`
- `ErrObjectNotFound`
- `ErrInvalidConfig`
- `ErrStreamingNotSupported`
- `ErrPolicyNotFound`
- `ErrOperationFailed`
- `ErrInvalidKey` - Key cannot be empty
- `ErrInvalidData` - Data cannot be nil for Put operations
- `ErrInvalidPolicyID` - Policy ID cannot be empty
- `ErrInvalidPolicy` - Policy cannot be nil
- `ErrInvalidMetadata` - Metadata cannot be nil for operations requiring it
- `ErrTimeout` - Operation timeout (retryable)
- `ErrTemporaryFailure` - Temporary failure (retryable)

## Testing

### Run Unit Tests

```bash
make test
```

### Run Tests with Coverage

```bash
make coverage
```

This generates both `coverage.txt` and `coverage.html` files.

### Run Integration Tests

```bash
make integration-test
```

Integration tests use Docker Compose to start go-objstore servers and test all operations against real servers.

## Development

### Build

```bash
make build
```

### Format Code

```bash
make fmt
```

### Lint

```bash
make lint
```

### Install Development Tools

```bash
make install-tools
```

### Clean

```bash
make clean
```

## Protocol Comparison

| Feature | gRPC | REST | QUIC |
|---------|------|------|------|
| Object CRUD | ✅ | ✅ | ✅ |
| Metadata | ✅ | ✅ | ✅ |
| Lifecycle Policies | ✅ | ❌ | ✅ |
| Replication | ✅ | ❌ | ✅ |
| Archive | ✅ | ❌ | ✅ |
| Streaming | ✅ | ❌ | ✅ |
| HTTP/2 | ✅ | ❌ | ❌ |
| HTTP/3 | ❌ | ❌ | ✅ |
| Binary Protocol | ✅ | ❌ | ✅ |

**Recommendation**: Use gRPC for maximum feature support and performance. Use REST for simple use cases and broad compatibility. Use QUIC for low-latency scenarios and mobile networks.

## Retry Configuration

The SDK supports configurable retry logic with exponential backoff for handling transient failures. Retries are **disabled by default** for backwards compatibility.

### Enable Retry with Default Settings

```go
config := &objstore.ClientConfig{
    Protocol: objstore.ProtocolGRPC,
    Address:  "localhost:50051",
    Retry: &objstore.RetryConfig{
        Enabled: true,
        // Uses defaults: MaxRetries=3, InitialBackoff=100ms, MaxBackoff=5s
    },
}

client, err := objstore.NewClient(config)
```

### Custom Retry Configuration

```go
config := &objstore.ClientConfig{
    Protocol: objstore.ProtocolGRPC,
    Address:  "localhost:50051",
    Retry: &objstore.RetryConfig{
        Enabled:        true,
        MaxRetries:     5,                     // Maximum 5 retry attempts
        InitialBackoff: 200 * time.Millisecond, // Start with 200ms backoff
        MaxBackoff:     10 * time.Second,       // Cap backoff at 10s
    },
}
```

### Custom Retryable Errors

```go
config := &objstore.ClientConfig{
    Protocol: objstore.ProtocolGRPC,
    Address:  "localhost:50051",
    Retry: &objstore.RetryConfig{
        Enabled:    true,
        MaxRetries: 3,
        RetryableErrors: []error{
            objstore.ErrConnectionFailed,
            objstore.ErrTimeout,
            objstore.ErrTemporaryFailure,
        },
    },
}
```

The retry logic automatically handles:
- Connection failures
- Timeout errors
- gRPC status codes (Unavailable, DeadlineExceeded, ResourceExhausted, Aborted)
- Common transient error patterns in error messages
- Exponential backoff with jitter to prevent thundering herd
- Context cancellation for graceful shutdown

## Configuration Options

### ClientConfig Fields

- `Protocol`: Protocol to use (REST, gRPC, QUIC)
- `Address`: Server address (host:port)
- `UseTLS`: Enable TLS encryption
- `CertFile`: Path to client certificate
- `KeyFile`: Path to client private key
- `CAFile`: Path to CA certificate
- `InsecureSkipVerify`: Skip TLS verification (testing only)
- `ConnectionTimeout`: Timeout for establishing connection
- `RequestTimeout`: Timeout for individual requests
- `MaxRecvMsgSize`: Maximum receive message size (gRPC)
- `MaxSendMsgSize`: Maximum send message size (gRPC)
- `MaxStreams`: Maximum concurrent streams (QUIC)
- `Retry`: Retry configuration (optional, disabled by default)

### RetryConfig Fields

- `Enabled`: Enable retry logic (default: false)
- `MaxRetries`: Maximum number of retry attempts (default: 3)
- `InitialBackoff`: Initial backoff duration (default: 100ms)
- `MaxBackoff`: Maximum backoff duration (default: 5s)
- `RetryableErrors`: Custom list of errors that should trigger retries (optional)

## Contributing

Contributions are welcome! Please ensure:

1. All tests pass: `make test`
2. Code is formatted: `make fmt`
3. Code is linted: `make lint`
4. Coverage remains above 90%: `make coverage`

## License

This SDK is dual-licensed:

1. **GNU Affero General Public License v3.0 (AGPL-3.0)**
   - See [LICENSE](https://www.gnu.org/licenses/agpl-3.0.html)

2. **Commercial License**
   - Contact licensing@automatethethings.com for commercial licensing options

## Support

- Documentation: [https://github.com/jeremyhahn/go-objstore](https://github.com/jeremyhahn/go-objstore)
- Issues: [https://github.com/jeremyhahn/go-objstore/issues](https://github.com/jeremyhahn/go-objstore/issues)
- Commercial Support: licensing@automatethethings.com

## Acknowledgments

Built with:
- [gRPC](https://grpc.io/)
- [quic-go](https://github.com/quic-go/quic-go)
- [Protocol Buffers](https://protobuf.dev/)
