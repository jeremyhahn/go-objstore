# Go ObjectStore JavaScript SDK

A comprehensive JavaScript SDK for [go-objstore](https://github.com/jeremyhahn/go-objstore) - a unified object storage library supporting multiple protocols and backends.

## Features

- **Multi-Protocol Support**: REST, gRPC, and QUIC/HTTP3
- **Unified API**: Consistent interface across all protocols
- **Comprehensive Operations**: Put, Get, Delete, List, Exists, Metadata management
- **Advanced Features**: Lifecycle policies, Replication policies, Archival
- **Modern JavaScript**: ESM and CommonJS support
- **TypeScript Ready**: Type definitions included
- **Full Test Coverage**: 90%+ code coverage with unit and integration tests
- **Node.js & Browser**: Works in both environments (with limitations for gRPC in browser)

## Installation

```bash
npm install @go-objstore/client
```

## Quick Start

### REST Client

```javascript
import { ObjectStoreClient } from '@go-objstore/client';

// Create client
const client = new ObjectStoreClient({
  protocol: 'rest',
  baseURL: 'http://localhost:8080'
});

// Upload an object
await client.put('documents/report.pdf', fileBuffer, {
  contentType: 'application/pdf',
  custom: { author: 'John Doe' }
});

// Download an object
const result = await client.get('documents/report.pdf');
console.log('Downloaded:', result.data);
console.log('Metadata:', result.metadata);

// List objects
const list = await client.list({ prefix: 'documents/', limit: 100 });
console.log('Objects:', list.objects);

// Check if exists
const exists = await client.exists('documents/report.pdf');
console.log('Exists:', exists.exists);

// Delete an object
await client.delete('documents/report.pdf');

// Clean up
client.close();
```

### gRPC Client

```javascript
import { ObjectStoreClient } from '@go-objstore/client';

const client = new ObjectStoreClient({
  protocol: 'grpc',
  baseURL: 'localhost:50051',
  insecure: true  // Use TLS in production
});

// All operations work the same as REST
await client.put('data.json', Buffer.from(JSON.stringify({ key: 'value' })));
const result = await client.get('data.json');

client.close();
```

### QUIC/HTTP3 Client

```javascript
import { ObjectStoreClient } from '@go-objstore/client';

const client = new ObjectStoreClient({
  protocol: 'quic',
  baseURL: 'http://localhost:8443'
});

await client.put('fast-data.bin', dataBuffer);
const result = await client.get('fast-data.bin');

client.close();
```

## API Reference

### ObjectStoreClient

#### Constructor Options

```javascript
new ObjectStoreClient({
  protocol: 'rest' | 'grpc' | 'quic',  // Required
  baseURL: string,                      // Required: URL for REST/QUIC or address for gRPC
  timeout: number,                      // Optional: Request timeout in ms (default: 30000)
  headers: object,                      // Optional: Additional headers (REST/QUIC only)
  insecure: boolean,                    // Optional: Use insecure connection (gRPC only, default: true)
  credentials: object                   // Optional: gRPC credentials
})
```

### Core Operations

#### put(key, data, metadata?)

Upload an object to storage.

```javascript
await client.put('path/to/file.txt', Buffer.from('content'), {
  contentType: 'text/plain',
  contentEncoding: 'gzip',
  custom: {
    author: 'John Doe',
    department: 'Engineering'
  }
});
```

**Parameters:**
- `key` (string): Storage key/path
- `data` (Buffer|Uint8Array|string): Object data
- `metadata` (object, optional): Object metadata

**Returns:** `Promise<{ success: boolean, message: string, etag: string }>`

#### get(key)

Retrieve an object from storage.

```javascript
const result = await client.get('path/to/file.txt');
console.log(result.data);      // Buffer
console.log(result.metadata);  // { contentType, contentLength, etag, lastModified }
```

**Parameters:**
- `key` (string): Storage key/path

**Returns:** `Promise<{ data: Buffer, metadata: object }>`

#### delete(key)

Delete an object from storage.

```javascript
await client.delete('path/to/file.txt');
```

**Parameters:**
- `key` (string): Storage key/path

**Returns:** `Promise<{ success: boolean, message: string }>`

#### list(options?)

List objects matching criteria.

```javascript
const result = await client.list({
  prefix: 'documents/',
  delimiter: '/',
  limit: 100,
  token: 'continuation-token'
});

console.log(result.objects);        // Array of objects
console.log(result.commonPrefixes); // Common prefixes (when using delimiter)
console.log(result.nextToken);      // Token for next page
console.log(result.truncated);      // More results available
```

**Parameters:**
- `options` (object, optional):
  - `prefix` (string): Filter by prefix
  - `delimiter` (string): Hierarchical delimiter
  - `limit` (number): Max results (REST/QUIC)
  - `maxResults` (number): Max results (gRPC)
  - `token` (string): Continuation token (REST/QUIC)
  - `continueFrom` (string): Continuation token (gRPC)

**Returns:** `Promise<{ objects: array, commonPrefixes: array, nextToken: string, truncated: boolean }>`

#### exists(key)

Check if an object exists.

```javascript
const result = await client.exists('path/to/file.txt');
console.log(result.exists); // true or false
```

**Parameters:**
- `key` (string): Storage key/path

**Returns:** `Promise<{ exists: boolean }>`

### Metadata Operations

#### getMetadata(key)

Get object metadata without downloading content.

```javascript
const result = await client.getMetadata('path/to/file.txt');
console.log(result.metadata);
```

**Parameters:**
- `key` (string): Storage key/path

**Returns:** `Promise<{ success: boolean, metadata: object }>`

#### updateMetadata(key, metadata)

Update object metadata.

```javascript
await client.updateMetadata('path/to/file.txt', {
  contentType: 'text/plain',
  custom: { version: '2.0' }
});
```

**Parameters:**
- `key` (string): Storage key/path
- `metadata` (object): New metadata

**Returns:** `Promise<{ success: boolean, message: string }>`

### Health Check

#### health(service?)

Check service health.

```javascript
const result = await client.health();
console.log(result.status); // 'healthy' or 'SERVING'
```

**Parameters:**
- `service` (string, optional): Service name (gRPC only)

**Returns:** `Promise<{ status: string, message: string }>`

### Archive Operations

#### archive(key, destinationType, destinationSettings?)

Archive an object to archival storage.

```javascript
await client.archive('old-data.tar', 'glacier', {
  region: 'us-east-1',
  storageClass: 'GLACIER'
});
```

**Parameters:**
- `key` (string): Storage key/path
- `destinationType` (string): Destination backend type
- `destinationSettings` (object, optional): Backend-specific settings

**Returns:** `Promise<{ success: boolean, message: string }>`

### Lifecycle Policies

#### addPolicy(policy)

Add a lifecycle policy.

```javascript
await client.addPolicy({
  id: 'delete-old-logs',
  prefix: 'logs/',
  retention_seconds: 2592000, // 30 days
  action: 'delete'
});
```

**Parameters:**
- `policy` (object): Policy configuration

**Returns:** `Promise<{ success: boolean, message: string }>`

#### removePolicy(id)

Remove a lifecycle policy.

```javascript
await client.removePolicy('delete-old-logs');
```

**Parameters:**
- `id` (string): Policy ID

**Returns:** `Promise<{ success: boolean, message: string }>`

#### getPolicies(prefix?)

Get all lifecycle policies.

```javascript
const result = await client.getPolicies('logs/');
console.log(result.policies);
```

**Parameters:**
- `prefix` (string, optional): Filter by prefix

**Returns:** `Promise<{ success: boolean, policies: array }>`

#### applyPolicies()

Apply all lifecycle policies.

```javascript
const result = await client.applyPolicies();
console.log('Policies applied:', result.policiesCount);
console.log('Objects processed:', result.objectsProcessed);
```

**Returns:** `Promise<{ success: boolean, policiesCount: number, objectsProcessed: number, message: string }>`

### Replication Policies

#### addReplicationPolicy(policy)

Add a replication policy.

```javascript
await client.addReplicationPolicy({
  id: 's3-to-gcs-backup',
  source_backend: 's3',
  source_settings: {
    region: 'us-east-1',
    bucket: 'source-bucket'
  },
  source_prefix: 'data/',
  destination_backend: 'gcs',
  destination_settings: {
    project: 'my-project',
    bucket: 'backup-bucket'
  },
  check_interval_seconds: 3600,
  enabled: true
});
```

**Parameters:**
- `policy` (object): Replication policy configuration

**Returns:** `Promise<{ success: boolean, message: string }>`

#### removeReplicationPolicy(id)

Remove a replication policy.

```javascript
await client.removeReplicationPolicy('s3-to-gcs-backup');
```

**Parameters:**
- `id` (string): Policy ID

**Returns:** `Promise<{ success: boolean, message: string }>`

#### getReplicationPolicies()

Get all replication policies.

```javascript
const result = await client.getReplicationPolicies();
console.log(result.policies);
```

**Returns:** `Promise<{ policies: array }>`

#### getReplicationPolicy(id)

Get a specific replication policy.

```javascript
const result = await client.getReplicationPolicy('s3-to-gcs-backup');
console.log(result.policy);
```

**Parameters:**
- `id` (string): Policy ID

**Returns:** `Promise<{ policy: object }>`

#### triggerReplication(options?)

Trigger replication sync.

```javascript
const result = await client.triggerReplication({
  policyId: 's3-to-gcs-backup',
  parallel: true,
  workerCount: 4
});

console.log('Synced:', result.result.synced);
console.log('Failed:', result.result.failed);
```

**Parameters:**
- `options` (object, optional):
  - `policyId` (string): Policy ID (empty for all)
  - `parallel` (boolean): Use parallel workers
  - `workerCount` (number): Number of workers

**Returns:** `Promise<{ success: boolean, result: object, message: string }>`

#### getReplicationStatus(id)

Get replication status and metrics.

```javascript
const result = await client.getReplicationStatus('s3-to-gcs-backup');
console.log('Total synced:', result.status.total_objects_synced);
console.log('Last sync:', result.status.last_sync_time);
```

**Parameters:**
- `id` (string): Policy ID

**Returns:** `Promise<{ success: boolean, status: object, message: string }>`

## Development

### Prerequisites

- Node.js >= 18.0.0
- npm >= 9.0.0
- Docker and Docker Compose (for integration tests)

### Setup

```bash
# Clone the repository
git clone https://github.com/jeremyhahn/go-objstore.git
cd go-objstore/api/sdks/javascript

# Install dependencies
npm install

# Generate protobuf code
make proto

# Build the SDK
make build
```

### Testing

```bash
# Run all tests
make test

# Run unit tests only
make test-unit

# Run integration tests (requires Docker)
make test-integration

# Generate coverage report
make coverage

# Watch mode
make watch
```

### Code Quality

```bash
# Lint code
make lint

# Format code
make format

# Run all verifications
make verify
```

## Browser Support

The SDK can be used in browsers with some limitations:

- **REST Client**: Full support
- **QUIC Client**: Full support
- **gRPC Client**: Limited support (requires gRPC-Web proxy)

### Browser Example

```javascript
import { ObjectStoreClient } from '@go-objstore/client';

const client = new ObjectStoreClient({
  protocol: 'rest',
  baseURL: 'https://api.example.com'
});

const fileInput = document.getElementById('fileInput');
const file = fileInput.files[0];
const arrayBuffer = await file.arrayBuffer();

await client.put('uploads/' + file.name, new Uint8Array(arrayBuffer), {
  contentType: file.type
});
```

## Protocol Comparison

| Feature | REST | gRPC | QUIC/HTTP3 |
|---------|------|------|------------|
| Browser Support | ✅ Full | ⚠️ Limited | ✅ Full |
| Streaming | ❌ No | ✅ Yes | ⚠️ Partial |
| Performance | Good | Excellent | Excellent |
| Ease of Use | Easy | Moderate | Easy |
| Network Usage | Higher | Lower | Lower |

## Error Handling

All methods throw errors on failure. Use try-catch for error handling:

```javascript
try {
  await client.put('file.txt', data);
} catch (error) {
  console.error('Upload failed:', error.message);
  // Handle error appropriately
}
```

## Contributing

Contributions are welcome! Please see the main [go-objstore repository](https://github.com/jeremyhahn/go-objstore) for contribution guidelines.

## License

This project is licensed under the AGPL-3.0 License. See the [LICENSE](../../../LICENSE) file for details.

Commercial licenses are available. See [LICENSE-COMMERCIAL.md](../../../LICENSE-COMMERCIAL.md) for more information.

## Support

- **Issues**: [GitHub Issues](https://github.com/jeremyhahn/go-objstore/issues)
- **Documentation**: [Full Documentation](https://github.com/jeremyhahn/go-objstore/tree/main/docs)

## Changelog

See [CHANGELOG.md](../../../CHANGELOG.md) for release history.
