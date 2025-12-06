# Go-ObjStore Python SDK

A comprehensive Python SDK for [go-objstore](https://github.com/jeremyhahn/go-objstore) with support for REST, gRPC, and QUIC/HTTP3 protocols.

## Features

- **Multi-Protocol Support**: REST, gRPC, and QUIC/HTTP3
- **Unified API**: Consistent interface across all protocols
- **Type Safety**: Full type hints and Pydantic models
- **Async Support**: Async operations for QUIC/HTTP3
- **Streaming**: Efficient streaming for large objects
- **Retry Logic**: Automatic retry with exponential backoff
- **Comprehensive Testing**: 90%+ code coverage
- **Well Documented**: Detailed docstrings and examples

## Installation

### Using pip

```bash
pip install go-objstore-sdk
```

### Using poetry

```bash
poetry add go-objstore-sdk
```

### From source

```bash
git clone https://github.com/jeremyhahn/go-objstore.git
cd go-objstore/api/sdks/python
pip install -e ".[dev]"
```

## Quick Start

### REST Client

```python
from objstore import ObjectStoreClient, Protocol, Metadata

# Create client
client = ObjectStoreClient(
    protocol=Protocol.REST,
    base_url="http://localhost:8080"
)

# Upload an object
with client:
    # Simple upload
    response = client.put("my-key", b"my-data")
    print(f"Upload successful: {response.success}")

    # Upload with metadata
    metadata = Metadata(
        content_type="text/plain",
        custom={"author": "john", "version": "1.0"}
    )
    client.put("my-key", b"my-data", metadata=metadata)

    # Download an object
    data, metadata = client.get("my-key")
    print(f"Downloaded {len(data)} bytes")

    # Stream large objects
    for chunk in client.get_stream("large-file"):
        process_chunk(chunk)

    # List objects
    result = client.list(prefix="documents/", max_results=100)
    for obj in result.objects:
        print(f"Key: {obj.key}, Size: {obj.metadata.size}")

    # Check existence
    exists = client.exists("my-key")
    print(f"Object exists: {exists.exists}")

    # Get metadata only
    metadata = client.get_metadata("my-key")
    print(f"Content-Type: {metadata.content_type}")

    # Update metadata
    new_metadata = Metadata(content_type="application/json")
    client.update_metadata("my-key", new_metadata)

    # Delete an object
    response = client.delete("my-key")
    print(f"Delete successful: {response.success}")

    # Health check
    health = client.health()
    print(f"Server status: {health.status}")
```

### gRPC Client

```python
from objstore import ObjectStoreClient, Protocol

# Create gRPC client
client = ObjectStoreClient(
    protocol=Protocol.GRPC,
    host="localhost",
    port=50051
)

with client:
    # Same API as REST
    client.put("my-key", b"my-data")
    data, metadata = client.get("my-key")
    client.delete("my-key")
```

### QUIC/HTTP3 Client

```python
from objstore import ObjectStoreClient, Protocol

# Create QUIC client
client = ObjectStoreClient(
    protocol=Protocol.QUIC,
    base_url="https://localhost:4433",
    verify_ssl=False  # For development
)

with client:
    # Same API as REST and gRPC
    client.put("my-key", b"my-data")
    data, metadata = client.get("my-key")
    client.delete("my-key")
```

### Advanced Usage

#### Custom Retry Configuration

```python
client = ObjectStoreClient(
    protocol=Protocol.REST,
    base_url="http://localhost:8080",
    timeout=60,
    max_retries=5
)
```

#### Working with Large Files

```python
# Streaming upload
with open("large-file.bin", "rb") as f:
    client.put("large-file", f)

# Streaming download
with open("downloaded-file.bin", "wb") as f:
    for chunk in client.get_stream("large-file"):
        f.write(chunk)
```

#### Pagination

```python
# List with pagination
next_token = None
all_objects = []

while True:
    result = client.list(
        prefix="data/",
        max_results=100,
        continue_from=next_token
    )
    all_objects.extend(result.objects)

    if not result.truncated:
        break

    next_token = result.next_token
```

#### Hierarchical Listing

```python
# List with delimiter for directory-like structure
result = client.list(prefix="documents/", delimiter="/")

# Files in the prefix
for obj in result.objects:
    print(f"File: {obj.key}")

# Subdirectories
for prefix in result.common_prefixes:
    print(f"Directory: {prefix}")
```

## Error Handling

The SDK provides specific exceptions for different error conditions:

```python
from objstore import ObjectStoreClient, Protocol
from objstore.exceptions import (
    ObjectNotFoundError,
    ConnectionError,
    AuthenticationError,
    ValidationError,
    ServerError,
    TimeoutError
)

client = ObjectStoreClient(protocol=Protocol.REST)

try:
    data, metadata = client.get("my-key")
except ObjectNotFoundError:
    print("Object not found")
except ConnectionError:
    print("Failed to connect to server")
except AuthenticationError:
    print("Authentication failed")
except ValidationError:
    print("Invalid request")
except ServerError:
    print("Server error")
except TimeoutError:
    print("Request timed out")
```

## Development

### Setup Development Environment

```bash
# Clone repository
git clone https://github.com/jeremyhahn/go-objstore.git
cd go-objstore/api/sdks/python

# Install dependencies
make install

# Generate gRPC code (optional, for gRPC support)
make generate-proto
```

### Running Tests

```bash
# Run unit tests
make test

# Run integration tests
make integration-test

# Run Docker integration tests (tests all protocols)
make docker-test

# Generate coverage report
make coverage
```

### Code Quality

```bash
# Format code
make format

# Run linters
make lint
```

## Testing

The SDK includes comprehensive tests:

- **Unit Tests**: Mock-based tests with 90%+ coverage
- **Integration Tests**: Tests against real go-objstore server
- **Docker Tests**: Automated tests with Docker Compose
- **Protocol Tests**: Tests for REST, gRPC, and QUIC

### Running Integration Tests

Integration tests require a running go-objstore server:

```bash
# Start server (in separate terminal)
docker run -p 8080:8080 -p 50051:50051 -p 4433:4433 \
    ghcr.io/jeremyhahn/go-objstore:latest

# Run integration tests
pytest tests/integration/ -v
```

### Docker Integration Tests

The SDK includes Docker Compose configuration for automated testing:

```bash
cd tests/integration
docker-compose up --build --abort-on-container-exit
docker-compose down -v
```

### Archive Operations

Archive objects to cold storage backends like Glacier or Azure Archive:

```python
from objstore import ObjectStoreClient, Protocol

client = ObjectStoreClient(protocol=Protocol.REST)

with client:
    # Archive to local storage
    response = client.archive(
        "old-documents/archive.zip",
        destination_type="local",
        settings={"path": "/archive/storage"}
    )
    print(f"Archived: {response.success}")

    # Archive to Glacier
    response = client.archive(
        "backups/database.sql",
        destination_type="glacier",
        settings={
            "region": "us-east-1",
            "vault": "my-backup-vault"
        }
    )

    # Archive to Azure Archive Storage
    response = client.archive(
        "logs/2024/january.log",
        destination_type="azurearchive",
        settings={
            "account": "mystorageaccount",
            "container": "archive-container"
        }
    )
```

### Lifecycle Policies

Automatically manage object retention and archival with lifecycle policies:

#### Add a Lifecycle Policy

```python
from objstore import ObjectStoreClient, Protocol
from objstore.models import LifecyclePolicy

client = ObjectStoreClient(protocol=Protocol.REST)

with client:
    # Delete old logs after 30 days
    policy = LifecyclePolicy(
        id="delete-old-logs",
        prefix="logs/",
        retention_seconds=30 * 24 * 60 * 60,  # 30 days
        action="delete"
    )
    response = client.add_policy(policy)
    print(f"Policy added: {response.success}")

    # Archive old data after 90 days
    archive_policy = LifecyclePolicy(
        id="archive-old-data",
        prefix="data/",
        retention_seconds=90 * 24 * 60 * 60,  # 90 days
        action="archive",
        destination_type="glacier",
        destination_settings={"region": "us-west-2"}
    )
    client.add_policy(archive_policy)
```

#### Get Policies

```python
# Get all policies
response = client.get_policies()
for policy in response.policies:
    print(f"Policy: {policy.id}")
    print(f"  Prefix: {policy.prefix}")
    print(f"  Retention: {policy.retention_seconds} seconds")
    print(f"  Action: {policy.action}")

# Get policies for specific prefix
response = client.get_policies(prefix="logs/")
```

#### Remove a Policy

```python
response = client.remove_policy("delete-old-logs")
print(f"Removed: {response.success}")
```

#### Apply Policies

Manually trigger policy evaluation and execution:

```python
response = client.apply_policies()
print(f"Applied {response.policies_count} policies")
print(f"Processed {response.objects_processed} objects")
```

### Replication Policies

Set up cross-backend data replication for disaster recovery or multi-region deployments:

#### Add a Replication Policy

```python
from objstore import ObjectStoreClient, Protocol
from objstore.models import ReplicationPolicy

client = ObjectStoreClient(protocol=Protocol.REST)

with client:
    policy = ReplicationPolicy(
        id="s3-to-gcs-replication",
        source_backend="s3",
        source_settings={
            "bucket": "my-s3-bucket",
            "region": "us-east-1"
        },
        source_prefix="data/",
        destination_backend="gcs",
        destination_settings={
            "bucket": "my-gcs-bucket",
            "project": "my-project"
        },
        check_interval_seconds=3600,  # Check every hour
        enabled=True,
        replication_mode="TRANSPARENT"
    )
    response = client.add_replication_policy(policy)
    print(f"Replication policy added: {response.success}")
```

#### Get Replication Policies

```python
# Get all replication policies
response = client.get_replication_policies()
for policy in response.policies:
    print(f"Policy: {policy.id}")
    print(f"  Source: {policy.source_backend}")
    print(f"  Destination: {policy.destination_backend}")
    print(f"  Enabled: {policy.enabled}")

# Get a specific policy
policy = client.get_replication_policy("s3-to-gcs-replication")
print(f"Check interval: {policy.check_interval_seconds}s")
```

#### Trigger Replication

Manually trigger replication sync:

```python
from objstore.models import TriggerReplicationOptions

# Trigger all policies
response = client.trigger_replication(TriggerReplicationOptions())

# Trigger specific policy
opts = TriggerReplicationOptions(policy_id="s3-to-gcs-replication")
response = client.trigger_replication(opts)

# Parallel replication with custom workers
opts = TriggerReplicationOptions(
    policy_id="s3-to-gcs-replication",
    parallel=True,
    worker_count=8
)
response = client.trigger_replication(opts)

# Check results
result = response.result
print(f"Synced: {result.synced}")
print(f"Deleted: {result.deleted}")
print(f"Failed: {result.failed}")
print(f"Bytes: {result.bytes_total}")
print(f"Duration: {result.duration_ms}ms")
```

#### Get Replication Status

```python
response = client.get_replication_status("s3-to-gcs-replication")
status = response.status

print(f"Total objects synced: {status.total_objects_synced}")
print(f"Total objects deleted: {status.total_objects_deleted}")
print(f"Total bytes synced: {status.total_bytes_synced}")
print(f"Total errors: {status.total_errors}")
print(f"Average duration: {status.average_sync_duration_ms}ms")
print(f"Sync count: {status.sync_count}")
```

#### Remove a Replication Policy

```python
response = client.remove_replication_policy("s3-to-gcs-replication")
print(f"Removed: {response.success}")
```

## API Reference

### ObjectStoreClient

Main client class with unified API across protocols.

**Core Methods:**
- `put(key, data, metadata=None)` - Upload object
- `get(key)` - Download object
- `get_stream(key)` - Stream download
- `delete(key)` - Delete object
- `list(prefix='', delimiter='', max_results=100, continue_from=None)` - List objects
- `exists(key)` - Check existence
- `get_metadata(key)` - Get metadata
- `update_metadata(key, metadata)` - Update metadata
- `health()` - Health check

**Archive Methods:**
- `archive(key, destination_type, settings)` - Archive object to cold storage

**Lifecycle Policy Methods:**
- `add_policy(policy)` - Add lifecycle policy
- `remove_policy(policy_id)` - Remove lifecycle policy
- `get_policies(prefix='')` - Get lifecycle policies
- `apply_policies()` - Apply all lifecycle policies

**Replication Methods:**
- `add_replication_policy(policy)` - Add replication policy
- `remove_replication_policy(policy_id)` - Remove replication policy
- `get_replication_policies()` - Get all replication policies
- `get_replication_policy(policy_id)` - Get specific replication policy
- `trigger_replication(opts)` - Trigger replication sync
- `get_replication_status(policy_id)` - Get replication status

### Models

- `Metadata` - Object metadata
- `ObjectInfo` - Object information
- `ListResponse` - List operation response
- `PutResponse` - Put operation response
- `DeleteResponse` - Delete operation response
- `ExistsResponse` - Exists operation response
- `HealthResponse` - Health check response
- `ArchiveResponse` - Archive operation response
- `LifecyclePolicy` - Lifecycle policy configuration
- `PolicyResponse` - Policy operation response
- `GetPoliciesResponse` - Get policies response
- `ApplyPoliciesResponse` - Apply policies response
- `ReplicationPolicy` - Replication policy configuration
- `GetReplicationPoliciesResponse` - Get replication policies response
- `TriggerReplicationOptions` - Trigger replication options
- `TriggerReplicationResponse` - Trigger replication response
- `GetReplicationStatusResponse` - Replication status response

### Exceptions

- `ObjectStoreError` - Base exception
- `ObjectNotFoundError` - Object not found (404)
- `ConnectionError` - Connection failed
- `AuthenticationError` - Authentication failed (401)
- `ValidationError` - Validation error (400)
- `ServerError` - Server error (5xx)
- `TimeoutError` - Request timeout

## Configuration

### Environment Variables

The SDK respects the following environment variables for integration tests:

- `OBJSTORE_REST_URL` - REST API URL (default: http://localhost:8080)
- `OBJSTORE_GRPC_HOST` - gRPC host (default: localhost)
- `OBJSTORE_GRPC_PORT` - gRPC port (default: 50051)
- `OBJSTORE_QUIC_URL` - QUIC URL (default: https://localhost:4433)

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## License

AGPL-3.0 - See LICENSE file for details

## Support

- GitHub Issues: https://github.com/jeremyhahn/go-objstore/issues
- Documentation: https://github.com/jeremyhahn/go-objstore

## Changelog

### 0.1.0 (2025-11-23)

- Initial release
- REST, gRPC, and QUIC/HTTP3 support
- Comprehensive test suite
- Full API coverage
- Streaming support
- Retry logic with exponential backoff
