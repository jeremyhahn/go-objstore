# go-objstore Rust SDK

A comprehensive Rust SDK for the [go-objstore](https://github.com/jeremyhahn/go-objstore) library, providing unified access to object storage operations via multiple protocols: REST, gRPC, and QUIC/HTTP3.

## Features

- **Multi-protocol support**: REST, gRPC, and QUIC/HTTP3
- **Async/await**: Built on Tokio for efficient async operations
- **Type-safe**: Strong typing with comprehensive error handling
- **Unified interface**: Common trait for all protocols
- **Advanced features**: Lifecycle policies, replication, archiving (gRPC)
- **Well-tested**: 90%+ code coverage with unit and integration tests
- **Production-ready**: Follows Rust best practices

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
go-objstore = "0.1.2"
tokio = { version = "1.35", features = ["full"] }
bytes = "1.5"
```

## Quick Start

```rust
use go_objstore::{ObjectStoreClient, ObjectStore};
use bytes::Bytes;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a REST client
    let mut client = ObjectStoreClient::rest("http://localhost:8080")?;

    // Put an object
    let data = Bytes::from("Hello, World!");
    client.put("test.txt", data, None).await?;

    // Get the object
    let (data, metadata) = client.get("test.txt").await?;
    println!("Retrieved {} bytes", metadata.size);

    // Check if exists
    if client.exists("test.txt").await? {
        println!("Object exists!");
    }

    // Delete the object
    client.delete("test.txt").await?;

    Ok(())
}
```

## Protocols

### REST Client

The REST client uses HTTP/1.1 or HTTP/2 for communication:

```rust
use go_objstore::ObjectStoreClient;

let client = ObjectStoreClient::rest("http://localhost:8080")?;
```

**Supported Operations:**
- Put, Get, Delete
- List objects with prefix filtering
- Metadata operations (get, update)
- Health check
- Existence check

### gRPC Client

The gRPC client provides full access to all go-objstore features:

```rust
use go_objstore::ObjectStoreClient;

let client = ObjectStoreClient::grpc("http://localhost:50051").await?;
```

**Supported Operations:**
- All REST operations
- Lifecycle policies (add, remove, get, apply)
- Replication policies (add, remove, get, trigger, status)
- Archive operations
- Streaming for large files

### QUIC/HTTP3 Client

The QUIC client uses HTTP/3 over QUIC for low-latency operations:

```rust
use go_objstore::ObjectStoreClient;
use std::net::SocketAddr;

let addr: SocketAddr = "127.0.0.1:4433".parse()?;
let client = ObjectStoreClient::quic(addr, "localhost").await?;
```

**Supported Operations:**
- Put, Get, Delete
- Health check
- Existence check

**Note:** QUIC requires the server to have HTTP3 enabled.

## Usage Examples

### Basic Operations

```rust
use go_objstore::{ObjectStoreClient, ObjectStore, Metadata};
use bytes::Bytes;
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = ObjectStoreClient::rest("http://localhost:8080")?;

    // Put with metadata
    let mut custom = HashMap::new();
    custom.insert("author".to_string(), "Alice".to_string());

    let metadata = Metadata {
        content_type: Some("text/plain".to_string()),
        custom,
        ..Default::default()
    };

    let data = Bytes::from("Hello, World!");
    client.put("hello.txt", data, Some(metadata)).await?;

    // List objects
    let list_req = go_objstore::ListRequest {
        prefix: Some("".to_string()),
        max_results: Some(100),
        ..Default::default()
    };

    let response = client.list(list_req).await?;
    for obj in response.objects {
        println!("- {} ({} bytes)", obj.key, obj.metadata.size);
    }

    Ok(())
}
```

### Lifecycle Policies (gRPC only)

```rust
use go_objstore::{ObjectStoreClient, LifecyclePolicy};
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = ObjectStoreClient::grpc("http://localhost:50051").await?;

    // Add a lifecycle policy
    let policy = LifecyclePolicy {
        id: "delete-old-logs".to_string(),
        prefix: "logs/".to_string(),
        retention_seconds: 86400 * 7, // 7 days
        action: "delete".to_string(),
        destination_type: None,
        destination_settings: HashMap::new(),
    };

    client.add_policy(policy).await?;

    // Get all policies
    let policies = client.get_policies(None).await?;
    for policy in policies {
        println!("Policy: {} ({})", policy.id, policy.action);
    }

    // Apply policies
    let (policies_count, objects_processed) = client.apply_policies().await?;
    println!("Applied {} policies to {} objects", policies_count, objects_processed);

    // Remove policy
    client.remove_policy("delete-old-logs").await?;

    Ok(())
}
```

### Archive Operations (gRPC only)

Archive objects to cold storage backends like Glacier or Azure Archive:

```rust
use go_objstore::ObjectStoreClient;
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = ObjectStoreClient::grpc("http://localhost:50051").await?;

    // Archive to local storage
    let mut settings = HashMap::new();
    settings.insert("path".to_string(), "/archive/storage".to_string());

    client.archive("old-documents/archive.zip", "local", settings).await?;
    println!("Object archived successfully");

    // Archive to Glacier
    let mut glacier_settings = HashMap::new();
    glacier_settings.insert("region".to_string(), "us-east-1".to_string());
    glacier_settings.insert("vault".to_string(), "my-backup-vault".to_string());

    client.archive("backups/database.sql", "glacier", glacier_settings).await?;

    // Archive to Azure Archive Storage
    let mut azure_settings = HashMap::new();
    azure_settings.insert("account".to_string(), "mystorageaccount".to_string());
    azure_settings.insert("container".to_string(), "archive-container".to_string());

    client.archive("logs/2024/january.log", "azurearchive", azure_settings).await?;

    Ok(())
}
```

### Replication Policies (gRPC only)

Set up cross-backend data replication for disaster recovery or multi-region deployments:

#### Add a Replication Policy

```rust
use go_objstore::{ObjectStoreClient, ReplicationPolicy, ReplicationMode};
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = ObjectStoreClient::grpc("http://localhost:50051").await?;

    // Configure S3 to GCS replication
    let mut source_settings = HashMap::new();
    source_settings.insert("bucket".to_string(), "my-s3-bucket".to_string());
    source_settings.insert("region".to_string(), "us-east-1".to_string());

    let mut dest_settings = HashMap::new();
    dest_settings.insert("bucket".to_string(), "my-gcs-bucket".to_string());
    dest_settings.insert("project".to_string(), "my-project".to_string());

    let policy = ReplicationPolicy {
        id: "s3-to-gcs-replication".to_string(),
        source_backend: "s3".to_string(),
        source_settings,
        source_prefix: "data/".to_string(),
        destination_backend: "gcs".to_string(),
        destination_settings: dest_settings,
        check_interval_seconds: 3600, // Check every hour
        last_sync_time: None,
        enabled: true,
        encryption: None,
        replication_mode: ReplicationMode::Transparent,
    };

    client.add_replication_policy(policy).await?;
    println!("Replication policy added");

    Ok(())
}
```

#### Get Replication Policies

```rust
// Get all replication policies
let policies = client.get_replication_policies().await?;
for policy in policies {
    println!("Policy: {}", policy.id);
    println!("  Source: {}", policy.source_backend);
    println!("  Destination: {}", policy.destination_backend);
    println!("  Enabled: {}", policy.enabled);
}

// Get a specific policy
let policy = client.get_replication_policy("s3-to-gcs-replication").await?;
println!("Check interval: {}s", policy.check_interval_seconds);
```

#### Trigger Replication

```rust
// Trigger all policies
let result = client.trigger_replication(None, false, 1).await?;

// Trigger specific policy
let result = client.trigger_replication(
    Some("s3-to-gcs-replication".to_string()),
    false,
    1
).await?;

// Parallel replication with custom workers
let result = client.trigger_replication(
    Some("s3-to-gcs-replication".to_string()),
    true,  // parallel
    8      // worker_count
).await?;

println!("Synced: {}, Deleted: {}, Failed: {}",
    result.synced, result.deleted, result.failed);
println!("Bytes: {}, Duration: {}ms",
    result.bytes_total, result.duration_ms);
```

#### Get Replication Status

```rust
let status = client.get_replication_status("s3-to-gcs-replication").await?;

println!("Total objects synced: {}", status.total_objects_synced);
println!("Total objects deleted: {}", status.total_objects_deleted);
println!("Total bytes synced: {}", status.total_bytes_synced);
println!("Total errors: {}", status.total_errors);
println!("Average duration: {}ms", status.average_sync_duration_ms);
println!("Sync count: {}", status.sync_count);
```

#### Remove a Replication Policy

```rust
client.remove_replication_policy("s3-to-gcs-replication").await?;
println!("Replication policy removed");
```

### Using the Trait

The `ObjectStore` trait allows for protocol-agnostic code:

```rust
use go_objstore::{ObjectStore, ObjectStoreClient};
use bytes::Bytes;

async fn store_data(
    client: &mut impl ObjectStore,
    key: &str,
    data: Bytes
) -> Result<(), Box<dyn std::error::Error>> {
    client.put(key, data, None).await?;
    println!("Stored {}", key);
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut rest_client = ObjectStoreClient::rest("http://localhost:8080")?;
    let mut grpc_client = ObjectStoreClient::grpc("http://localhost:50051").await?;

    let data = Bytes::from("test data");

    // Same function works with both protocols
    store_data(&mut rest_client, "rest-test.txt", data.clone()).await?;
    store_data(&mut grpc_client, "grpc-test.txt", data).await?;

    Ok(())
}
```

## Building

```bash
# Build the project
make build

# Or use cargo directly
cargo build --release
```

## Testing

### Unit Tests

```bash
# Run unit tests
make test

# Or use cargo
cargo test --lib
```

### Integration Tests

Integration tests require a running go-objstore server:

```bash
# Run integration tests with Docker
make docker-test

# Or manually start the server and run tests
cargo test --test integration_test -- --ignored
```

### Code Coverage

```bash
# Generate coverage report with llvm-cov
make coverage

# Or with tarpaulin
make coverage-tarpaulin
```

## Examples

Run the provided examples:

```bash
# REST client example
make example-rest

# gRPC client example
make example-grpc

# QUIC client example
make example-quic

# Unified client example
make example-unified
```

Or with cargo:

```bash
cargo run --example rest_client
cargo run --example grpc_client
cargo run --example quic_client
cargo run --example unified_client
```

## Error Handling

The SDK uses a comprehensive error type:

```rust
use go_objstore::{Error, Result};

async fn example() -> Result<()> {
    let mut client = ObjectStoreClient::rest("http://localhost:8080")?;

    match client.get("nonexistent.txt").await {
        Ok((data, _)) => println!("Got data: {} bytes", data.len()),
        Err(Error::NotFound(key)) => println!("Object not found: {}", key),
        Err(Error::Http(e)) => println!("HTTP error: {}", e),
        Err(e) => println!("Other error: {}", e),
    }

    Ok(())
}
```

## Development

### Prerequisites

- Rust 1.70 or later
- Go 1.21 or later (for running the server)
- protoc (Protocol Buffers compiler)

### Setup

```bash
# Install development dependencies
make install-dev

# Format code
make fmt

# Run linter
make clippy

# Run all checks
make ci
```

### Project Structure

```
rust/
├── src/
│   ├── lib.rs              # Main library entry point
│   ├── client.rs           # Unified client implementation
│   ├── error.rs            # Error types
│   ├── types.rs            # Common types
│   ├── rest_client.rs      # REST client
│   ├── grpc_client.rs      # gRPC client
│   ├── quic_client.rs      # QUIC client
│   └── proto/              # Generated protobuf code
├── tests/
│   └── integration_test.rs # Integration tests
├── examples/               # Usage examples
├── scripts/
│   └── docker-test.sh      # Docker integration test script
├── Cargo.toml              # Dependencies and metadata
├── build.rs                # Build script for protobuf
├── Makefile                # Build and test targets
└── README.md               # This file
```

## Performance

- **Async/await**: Non-blocking I/O for high concurrency
- **Connection pooling**: Efficient connection reuse
- **Streaming**: Large file support with streaming APIs
- **Zero-copy**: Efficient data handling with `Bytes`

## License

AGPL-3.0 - See LICENSE file for details

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass (`make ci`)
5. Submit a pull request

## Support

- **Issues**: [GitHub Issues](https://github.com/jeremyhahn/go-objstore/issues)
- **Documentation**: Run `cargo doc --open` for detailed API docs

## Changelog

### v0.1.2 (2025-11-23)

- Initial release
- REST, gRPC, and QUIC/HTTP3 support
- Comprehensive test coverage
- Full API implementation
- Examples and documentation
