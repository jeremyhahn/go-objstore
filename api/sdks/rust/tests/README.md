# Rust SDK Integration Tests

## Overview

This directory contains comprehensive integration tests for the go-objstore Rust SDK, validating all operations across all supported protocols (REST, gRPC, QUIC).

## Test Files

### `integration_comprehensive.rs`

A table-driven comprehensive test suite that validates all 19 API operations across all 3 protocols using modern Rust testing patterns.

#### Operations Tested (19 total)

**Basic Operations (5):**
- `put` - Store objects with optional metadata
- `get` - Retrieve objects and metadata
- `delete` - Remove objects
- `exists` - Check object existence
- `list` - List objects with prefix filtering and pagination

**Metadata Operations (2):**
- `getMetadata` - Retrieve object metadata
- `updateMetadata` - Update object metadata

**Lifecycle Operations (4 - gRPC only):**
- `addPolicy` - Add lifecycle policies
- `removePolicy` - Remove lifecycle policies
- `getPolicies` - List lifecycle policies
- `applyPolicies` - Apply lifecycle policies to objects

**Replication Operations (6 - gRPC only):**
- `addReplicationPolicy` - Add replication policies
- `removeReplicationPolicy` - Remove replication policies
- `getReplicationPolicies` - List all replication policies
- `getReplicationPolicy` - Get specific replication policy
- `triggerReplication` - Manually trigger replication
- `getReplicationStatus` - Get replication metrics and status

**Archive Operations (1 - gRPC only):**
- `archive` - Archive objects to alternative storage

**Health Operations (1):**
- `health` - Server health check

#### Protocols Tested (3 total)

1. **REST** - HTTP/1.1 RESTful API
2. **gRPC** - HTTP/2 gRPC with Protocol Buffers
3. **QUIC** - HTTP/3 over QUIC with TLS

## Configuration

### Environment Variables

Configure server endpoints using environment variables:

```bash
# REST server endpoint (default: localhost:8080)
export REST_HOST="localhost:8080"

# gRPC server endpoint (default: localhost:9090)
export GRPC_HOST="localhost:9090"

# QUIC server endpoint (default: localhost:8443)
export QUIC_HOST="localhost:8443"
```

### Server Requirements

Before running tests, ensure the appropriate servers are running:

#### REST Server
```bash
# Start REST server on port 8080
./objstore-rest-server --port 8080
```

#### gRPC Server
```bash
# Start gRPC server on port 9090
./objstore-grpc-server --port 9090
```

#### QUIC Server
```bash
# Start QUIC server on port 8443
./objstore-quic-server --port 8443
```

## Running Tests

### Run All Integration Tests

```bash
# Run all comprehensive integration tests
cargo test --test integration_comprehensive -- --ignored

# Run with verbose output
cargo test --test integration_comprehensive -- --ignored --nocapture
```

### Run Tests by Protocol

```bash
# REST protocol tests only
cargo test --test integration_comprehensive rest_ -- --ignored

# gRPC protocol tests only
cargo test --test integration_comprehensive grpc_ -- --ignored

# QUIC protocol tests only
cargo test --test integration_comprehensive quic_ -- --ignored
```

### Run Tests by Operation Category

```bash
# Basic operations (put, get, delete, exists, list)
cargo test --test integration_comprehensive basic_ -- --ignored

# Metadata operations
cargo test --test integration_comprehensive metadata_ -- --ignored

# Health check
cargo test --test integration_comprehensive health -- --ignored

# Lifecycle operations (gRPC only)
cargo test --test integration_comprehensive lifecycle_ -- --ignored

# Replication operations (gRPC only)
cargo test --test integration_comprehensive replication_ -- --ignored

# Archive operations (gRPC only)
cargo test --test integration_comprehensive archive -- --ignored
```

### Run Specific Tests

```bash
# Run a specific protocol+operation test
cargo test --test integration_comprehensive test_basic_put_rest -- --ignored

# Run cross-protocol consistency test
cargo test --test integration_comprehensive test_cross_protocol_consistency -- --ignored
```

### Run Without Ignored Flag (for CI/CD)

When servers are guaranteed to be available (e.g., in CI/CD):

```bash
# Remove --ignored flag and set environment
cargo test --test integration_comprehensive
```

## Test Architecture

### Table-Driven Testing Pattern

The test suite uses Rust's macro system to generate tests from a table-driven specification:

```rust
// Test function defines the behavior
async fn test_put_operation(
    client: ObjectStoreClient,
    protocol: Protocol,
) -> Result<(), Box<dyn std::error::Error>> {
    // Test implementation
}

// Macro generates protocol-specific tests
protocol_test!(Rest, test_basic_put, test_put_operation);
protocol_test!(Grpc, test_basic_put, test_put_operation);
protocol_test!(Quic, test_basic_put, test_put_operation);
```

This generates three test functions:
- `test_basic_put_rest()`
- `test_basic_put_grpc()`
- `test_basic_put_quic()`

### Protocol Configuration

The `Protocol` enum defines protocol-specific behavior:

```rust
enum Protocol {
    Rest,   // HTTP/1.1 REST API
    Grpc,   // HTTP/2 gRPC
    Quic,   // HTTP/3 QUIC
}

impl Protocol {
    // Check if protocol supports advanced features
    const fn supports_advanced_features(&self) -> bool {
        matches!(self, Protocol::Grpc)
    }
}
```

### Test Helpers

**Client Creation:**
```rust
// Automatically configures client from environment
let client = create_client(Protocol::Rest).await?;
```

**Test Data Generation:**
```rust
// Generate protocol-specific test keys
let key = test_key(Protocol::Rest, "test.txt");
// Result: "test-rest-test.txt"

// Generate test data
let data = test_data("Hello, World!");

// Generate test metadata
let metadata = test_metadata(vec![
    ("author", "test-suite"),
    ("version", "1.0"),
]);
```

**Cleanup:**
```rust
// Cleanup multiple keys after test
cleanup_keys(&client, &["key1", "key2", "key3"]).await;
```

## Cross-Protocol Consistency Testing

The test suite includes cross-protocol consistency tests that verify all protocols produce identical results:

```rust
#[tokio::test]
async fn test_cross_protocol_consistency() {
    // Put via REST, Get via gRPC, verify consistency
    // Put via gRPC, Get via QUIC, verify consistency
    // etc.
}
```

## Test Coverage Verification

The test suite includes coverage verification tests:

```rust
#[test]
fn test_operation_coverage() {
    // Verifies all 19 operations are tested
}

#[test]
fn test_protocol_coverage() {
    // Verifies all 3 protocols are tested
}
```

## Expected Test Results

### All Servers Available

When all three servers (REST, gRPC, QUIC) are available:

```
test test_basic_delete_grpc ... ok
test test_basic_delete_quic ... ok
test test_basic_delete_rest ... ok
test test_basic_exists_grpc ... ok
test test_basic_exists_quic ... ok
test test_basic_exists_rest ... ok
...
test test_cross_protocol_consistency ... ok

test result: ok. 57 passed; 0 failed; 0 ignored; 0 measured
```

### Partial Server Availability

Tests automatically skip unavailable protocols:

```
test test_basic_put_rest ... ok
test test_basic_put_grpc ... FAILED (connection refused)
test test_basic_put_quic ... SKIPPED

test result: ok. 19 passed; 1 failed; 19 skipped
```

## Advanced Features Testing

### Lifecycle Policies (gRPC only)

Tests for time-based object retention and automatic deletion/archiving:

```bash
cargo test --test integration_comprehensive lifecycle_ -- --ignored
```

### Replication Policies (gRPC only)

Tests for cross-backend replication with encryption support:

```bash
cargo test --test integration_comprehensive replication_ -- --ignored
```

### Archive Operations (gRPC only)

Tests for moving objects to archival storage:

```bash
cargo test --test integration_comprehensive archive -- --ignored
```

## Continuous Integration

### GitHub Actions Example

```yaml
name: Rust SDK Integration Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest

    services:
      objstore-rest:
        image: objstore:latest
        ports:
          - 8080:8080

      objstore-grpc:
        image: objstore:latest
        ports:
          - 9090:9090

      objstore-quic:
        image: objstore:latest
        ports:
          - 8443:8443

    steps:
      - uses: actions/checkout@v3

      - name: Install Rust
        uses: actions-rs/toolchain@v1
        with:
          toolchain: stable

      - name: Run integration tests
        working-directory: api/sdks/rust
        env:
          REST_HOST: localhost:8080
          GRPC_HOST: localhost:9090
          QUIC_HOST: localhost:8443
        run: |
          cargo test --test integration_comprehensive -- --ignored
```

## Troubleshooting

### Connection Errors

**Problem:** `connection refused` errors

**Solution:**
1. Verify servers are running: `netstat -tulpn | grep -E '(8080|9090|8443)'`
2. Check server logs for startup errors
3. Verify firewall rules allow connections

### TLS/Certificate Errors (QUIC)

**Problem:** TLS verification failures for QUIC tests

**Solution:**
1. For testing: Disable TLS verification in client
2. For production: Install proper certificates
3. Check server certificate configuration

### Timeout Errors

**Problem:** Tests timeout waiting for responses

**Solution:**
1. Increase test timeout in `Cargo.toml`
2. Check server performance and load
3. Verify network latency

### Test Data Cleanup

**Problem:** Tests fail due to leftover data from previous runs

**Solution:**
```bash
# Clear test data before running tests
rm -rf /tmp/objstore-test-*

# Or start servers with fresh state
./objstore-rest-server --data-dir /tmp/fresh-test-data
```

## Test Metrics

The comprehensive test suite provides:

- **57 protocol-specific tests** (19 operations × 3 protocols)
- **1 cross-protocol consistency test**
- **3 coverage verification tests**
- **100% operation coverage** (all 19 operations)
- **100% protocol coverage** (all 3 protocols)

## Contributing

When adding new operations or protocols:

1. Add test function following naming convention: `test_<operation>_operation()`
2. Use `protocol_test!` macro to generate protocol-specific tests
3. Update operation count in `test_operation_coverage()`
4. Update this README with new operation details
5. Verify test passes for all applicable protocols

## License

AGPL-3.0 - See LICENSE file for details
