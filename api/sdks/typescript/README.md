# go-objstore TypeScript SDK

A comprehensive TypeScript SDK for [go-objstore](https://github.com/jeremyhahn/go-objstore) with support for REST, gRPC, and QUIC/HTTP3 protocols.

## Features

- **Multiple Protocol Support**: REST, gRPC, and QUIC/HTTP3
- **Full Type Safety**: Complete TypeScript type definitions
- **Comprehensive API Coverage**: All object storage operations including:
  - Object CRUD operations (Put, Get, Delete, List, Exists)
  - Metadata operations (GetMetadata, UpdateMetadata)
  - Lifecycle policies (Add, Remove, Get, Apply)
  - Replication policies with encryption support
  - Archive operations
  - Health monitoring
- **Promise-based API**: Modern async/await support
- **Streaming Support**: Efficient handling of large files (gRPC)
- **90%+ Test Coverage**: Comprehensive unit and integration tests

## Installation

```bash
npm install @go-objstore/typescript-sdk
```

## Quick Start

### REST Client

```typescript
import { ObjectStoreClient } from '@go-objstore/typescript-sdk';

const client = new ObjectStoreClient({
  protocol: 'rest',
  rest: {
    baseUrl: 'http://localhost:8080',
    timeout: 30000,
  },
});

// Upload an object
await client.put({
  key: 'documents/file.pdf',
  data: Buffer.from('file contents'),
  metadata: {
    contentType: 'application/pdf',
    custom: {
      author: 'John Doe',
      department: 'Engineering',
    },
  },
});

// Download an object
const response = await client.get({ key: 'documents/file.pdf' });
console.log('Data:', response.data.toString());
console.log('Metadata:', response.metadata);

// List objects
const list = await client.list({ prefix: 'documents/' });
console.log('Objects:', list.objects);

// Check if object exists
const exists = await client.exists({ key: 'documents/file.pdf' });
console.log('Exists:', exists.exists);

// Delete an object
await client.delete({ key: 'documents/file.pdf' });
```

### gRPC Client

```typescript
import { ObjectStoreClient } from '@go-objstore/typescript-sdk';

const client = new ObjectStoreClient({
  protocol: 'grpc',
  grpc: {
    address: 'localhost:50051',
    secure: false, // Use true for TLS
  },
});

// Same API as REST client
await client.put({
  key: 'data.json',
  data: Buffer.from(JSON.stringify({ hello: 'world' })),
});

const data = await client.get({ key: 'data.json' });
```

### QUIC/HTTP3 Client

```typescript
import { ObjectStoreClient } from '@go-objstore/typescript-sdk';

const client = new ObjectStoreClient({
  protocol: 'quic',
  quic: {
    address: 'localhost:8443',
    secure: true,
  },
});

// Same unified API
await client.put({
  key: 'test.txt',
  data: Buffer.from('Hello QUIC!'),
});
```

## API Documentation

### Object Operations

#### Put

Store an object in the backend:

```typescript
const response = await client.put({
  key: 'path/to/file.txt',
  data: Buffer.from('content'),
  metadata: {
    contentType: 'text/plain',
    contentEncoding: 'gzip',
    custom: {
      key1: 'value1',
      key2: 'value2',
    },
  },
});

console.log('ETag:', response.etag);
```

#### Get

Retrieve an object:

```typescript
const response = await client.get({ key: 'path/to/file.txt' });
console.log('Data:', response.data);
console.log('Content Type:', response.metadata?.contentType);
console.log('Size:', response.metadata?.size);
```

#### Delete

Remove an object:

```typescript
await client.delete({ key: 'path/to/file.txt' });
```

#### List

List objects with optional filtering:

```typescript
const response = await client.list({
  prefix: 'documents/',
  delimiter: '/',
  maxResults: 100,
  continueFrom: 'pagination-token',
});

for (const obj of response.objects) {
  console.log(`${obj.key}: ${obj.metadata?.size} bytes`);
}

// Pagination
if (response.truncated && response.nextToken) {
  const nextPage = await client.list({
    prefix: 'documents/',
    continueFrom: response.nextToken,
  });
}
```

#### Exists

Check if an object exists:

```typescript
const response = await client.exists({ key: 'path/to/file.txt' });
if (response.exists) {
  console.log('Object exists');
}
```

### Metadata Operations

#### Get Metadata

Retrieve metadata without downloading the object:

```typescript
const response = await client.getMetadata({ key: 'path/to/file.txt' });
console.log('Content Type:', response.metadata?.contentType);
console.log('Size:', response.metadata?.size);
console.log('Custom Metadata:', response.metadata?.custom);
```

#### Update Metadata

Update object metadata:

```typescript
await client.updateMetadata({
  key: 'path/to/file.txt',
  metadata: {
    contentType: 'application/json',
    custom: {
      version: '2.0',
      updated: new Date().toISOString(),
    },
  },
});
```

### Lifecycle Policies

#### Add Policy

```typescript
await client.addPolicy({
  policy: {
    id: 'delete-old-logs',
    prefix: 'logs/',
    retentionSeconds: 86400 * 30, // 30 days
    action: 'delete',
  },
});
```

#### Archive Policy

```typescript
await client.addPolicy({
  policy: {
    id: 'archive-old-data',
    prefix: 'archive/',
    retentionSeconds: 86400 * 90, // 90 days
    action: 'archive',
    destinationType: 'glacier',
    destinationSettings: {
      tier: 'standard',
    },
  },
});
```

#### Get Policies

```typescript
const response = await client.getPolicies({ prefix: 'logs/' });
for (const policy of response.policies) {
  console.log(`Policy ${policy.id}: ${policy.action} after ${policy.retentionSeconds}s`);
}
```

#### Apply Policies

```typescript
const response = await client.applyPolicies();
console.log(`Applied ${response.policiesCount} policies`);
console.log(`Processed ${response.objectsProcessed} objects`);
```

### Replication Policies

#### Add Replication Policy

```typescript
import { ReplicationMode } from '@go-objstore/typescript-sdk';

await client.addReplicationPolicy({
  policy: {
    id: 's3-to-gcs',
    sourceBackend: 's3',
    sourceSettings: {
      bucket: 'source-bucket',
      region: 'us-east-1',
    },
    sourcePrefix: 'data/',
    destinationBackend: 'gcs',
    destinationSettings: {
      bucket: 'destination-bucket',
      project: 'my-project',
    },
    checkIntervalSeconds: 3600, // Check every hour
    enabled: true,
    replicationMode: ReplicationMode.TRANSPARENT,
    encryption: {
      source: {
        enabled: true,
        provider: 'custom',
        defaultKey: 'source-key-id',
      },
      destination: {
        enabled: true,
        provider: 'custom',
        defaultKey: 'dest-key-id',
      },
    },
  },
});
```

#### Trigger Replication

```typescript
const response = await client.triggerReplication({
  policyId: 's3-to-gcs',
  parallel: true,
  workerCount: 4,
});

console.log(`Synced: ${response.result?.synced}`);
console.log(`Deleted: ${response.result?.deleted}`);
console.log(`Failed: ${response.result?.failed}`);
console.log(`Duration: ${response.result?.durationMs}ms`);
```

#### Get Replication Status

```typescript
const response = await client.getReplicationStatus({ id: 's3-to-gcs' });
const status = response.status!;

console.log(`Total synced: ${status.totalObjectsSynced}`);
console.log(`Total deleted: ${status.totalObjectsDeleted}`);
console.log(`Total bytes: ${status.totalBytesSynced}`);
console.log(`Errors: ${status.totalErrors}`);
console.log(`Last sync: ${status.lastSyncTime}`);
```

### Archive Operations

```typescript
await client.archive({
  key: 'old-data/file.tar.gz',
  destinationType: 'glacier',
  destinationSettings: {
    tier: 'deep-archive',
  },
});
```

### Health Check

```typescript
import { HealthStatus } from '@go-objstore/typescript-sdk';

const response = await client.health();
if (response.status === HealthStatus.SERVING) {
  console.log('Service is healthy');
} else {
  console.log('Service is not healthy:', response.message);
}
```

## Development

### Prerequisites

- Node.js >= 18.0.0
- npm >= 9.0.0
- Docker (for integration tests)

### Setup

```bash
# Install dependencies
make install

# Build the project
make build

# Run tests
make test

# Run unit tests only
make test-unit

# Run integration tests
make test-integration

# Generate coverage report
make coverage

# Lint code
make lint

# Format code
make format
```

### Project Structure

```
typescript/
├── src/
│   ├── clients/
│   │   ├── rest-client.ts      # REST implementation
│   │   ├── grpc-client.ts      # gRPC implementation
│   │   └── quic-client.ts      # QUIC/HTTP3 implementation
│   ├── client.ts               # Unified client facade
│   ├── types.ts                # Type definitions
│   └── index.ts                # Main entry point
├── tests/
│   ├── unit/                   # Unit tests with mocking
│   └── integration/            # Docker integration tests
├── dist/                       # Compiled JavaScript
├── coverage/                   # Coverage reports
├── Makefile                    # Build automation
├── package.json                # NPM configuration
├── tsconfig.json               # TypeScript configuration
└── README.md                   # This file
```

## Testing

The SDK includes comprehensive test coverage:

### Unit Tests

- Mock-based testing for all client operations
- 90%+ code coverage
- Fast execution without external dependencies

```bash
npm run test:unit
```

### Integration Tests

- Docker-based tests against real go-objstore server
- Tests all protocols (REST, gRPC, QUIC)
- Verifies all operations end-to-end

```bash
npm run test:integration
```

## Type Definitions

The SDK provides complete TypeScript type definitions for all operations. Your IDE will provide full autocomplete and type checking.

```typescript
import type {
  ObjectStoreClient,
  PutRequest,
  PutResponse,
  GetRequest,
  GetResponse,
  Metadata,
  LifecyclePolicy,
  ReplicationPolicy,
} from '@go-objstore/typescript-sdk';
```

## Error Handling

```typescript
try {
  await client.get({ key: 'non-existent' });
} catch (error) {
  if (error instanceof Error) {
    console.error('Error:', error.message);
  }
}
```

## Resource Cleanup

Always close the client when done:

```typescript
await client.close();
```

## Contributing

Contributions are welcome! Please ensure:

1. All tests pass: `make test`
2. Code is formatted: `make format`
3. No linting errors: `make lint`
4. Test coverage remains above 90%: `make coverage`

## License

AGPL-3.0 - See LICENSE file for details

## Support

- GitHub Issues: https://github.com/jeremyhahn/go-objstore/issues
- Documentation: https://github.com/jeremyhahn/go-objstore

## Changelog

### 0.1.0 (Initial Release)

- REST client implementation
- gRPC client with streaming support
- QUIC/HTTP3 client
- Unified ObjectStoreClient facade
- Complete type definitions
- 90%+ test coverage
- Docker integration tests
- Comprehensive documentation
