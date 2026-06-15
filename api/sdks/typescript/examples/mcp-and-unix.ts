/**
 * Example: MCP and Unix domain socket clients, streaming, and app-layer auth.
 *
 * Run with:
 *   npx ts-node examples/mcp-and-unix.ts
 *
 * Prerequisites: a running go-objstore MCP server at http://localhost:9090 and
 * a Unix socket server at /tmp/objstore.sock.
 */

import { Readable } from 'stream';
import {
  ObjectStoreClient,
  McpClient,
  UnixClient,
  RestClient,
  ReplicationMode,
} from '../src';

// ---------------------------------------------------------------------------
// MCP client – HTTP POST JSON-RPC 2.0 transport
// ---------------------------------------------------------------------------

async function mcpExample(): Promise<void> {
  // Authenticated MCP client: token and tenant forwarded on every request.
  const mcp = new McpClient({
    baseUrl: 'http://localhost:9090',
    token: process.env.OBJSTORE_TOKEN,
    tenantId: process.env.OBJSTORE_TENANT,
    timeout: 15000,
  });

  // Put an object.
  await mcp.put({
    key: 'mcp/hello.txt',
    data: Buffer.from('Hello from MCP!'),
    metadata: { contentType: 'text/plain' },
  });
  console.log('MCP put: ok');

  // Get it back.
  const { data } = await mcp.get({ key: 'mcp/hello.txt' });
  console.log('MCP get:', data.toString());

  // Health check.
  const health = await mcp.health();
  console.log('MCP health status:', health.status);

  // List objects.
  const list = await mcp.list({ prefix: 'mcp/' });
  console.log('MCP list:', list.objects.map((o) => o.key));

  // Delete.
  await mcp.delete({ key: 'mcp/hello.txt' });
  console.log('MCP delete: ok');

  // The factory accepts 'mcp' as a protocol.
  const unified = new ObjectStoreClient({
    protocol: 'mcp',
    mcp: { baseUrl: 'http://localhost:9090', token: process.env.OBJSTORE_TOKEN },
  });
  console.log('Unified MCP health:', (await unified.health()).status);
  await unified.close();

  await mcp.close();
}

// ---------------------------------------------------------------------------
// Unix domain socket client – newline-delimited JSON-RPC 2.0
// ---------------------------------------------------------------------------

async function unixExample(): Promise<void> {
  // No auth fields: the server authenticates via peer credentials.
  const unix = new UnixClient({ socketPath: '/tmp/objstore.sock', timeout: 10000 });

  await unix.put({ key: 'unix/hello.txt', data: Buffer.from('Hello from Unix!') });
  console.log('Unix put: ok');

  const { data } = await unix.get({ key: 'unix/hello.txt' });
  console.log('Unix get:', data.toString());

  const health = await unix.health();
  console.log('Unix health:', health.status);

  const { exists } = await unix.exists({ key: 'unix/hello.txt' });
  console.log('Unix exists:', exists);

  await unix.delete({ key: 'unix/hello.txt' });
  console.log('Unix delete: ok');

  // The factory accepts 'unix' as a protocol.
  const unified = new ObjectStoreClient({
    protocol: 'unix',
    unix: { socketPath: '/tmp/objstore.sock' },
  });
  await unified.close();

  await unix.close();
}

// ---------------------------------------------------------------------------
// Streaming – REST getStream / putStream
// ---------------------------------------------------------------------------

async function streamingExample(): Promise<void> {
  const rest = new RestClient({
    baseUrl: 'http://localhost:8080',
    token: process.env.OBJSTORE_TOKEN,
    tenantId: process.env.OBJSTORE_TENANT,
  });

  // putStream: upload data from an AsyncIterable without fully buffering it.
  async function* sampleData(): AsyncIterable<Buffer> {
    yield Buffer.from('chunk 1 – ');
    yield Buffer.from('chunk 2 – ');
    yield Buffer.from('chunk 3');
  }

  const putResp = await rest.putStream('stream/test.txt', Readable.from(sampleData()));
  console.log('REST putStream:', putResp.success);

  // getStream: pipe object data directly to stdout without buffering.
  const readable = await rest.getStream('stream/test.txt');
  process.stdout.write('REST getStream: ');
  await new Promise<void>((resolve, reject) => {
    readable.on('data', (chunk: Buffer) => process.stdout.write(chunk));
    readable.on('end', () => { process.stdout.write('\n'); resolve(); });
    readable.on('error', reject);
  });

  await rest.delete({ key: 'stream/test.txt' });

  await rest.close();
}

// ---------------------------------------------------------------------------
// Auth headers on existing REST client
// ---------------------------------------------------------------------------

async function authExample(): Promise<void> {
  // token + tenantId: automatically included in every request.
  const client = new RestClient({
    baseUrl: 'http://localhost:8080',
    token: 'my-bearer-token',
    tenantId: 'tenant-123',
    headers: { 'X-Custom-Header': 'value' },
  });

  const health = await client.health();
  console.log('Auth REST health:', health.status);
  await client.close();
}

// ---------------------------------------------------------------------------
// Replication example via MCP
// ---------------------------------------------------------------------------

async function replicationExample(): Promise<void> {
  const mcp = new McpClient({ baseUrl: 'http://localhost:9090' });

  await mcp.addReplicationPolicy({
    policy: {
      id: 'mcp-s3-to-gcs',
      sourceBackend: 's3',
      sourceSettings: { bucket: 'src', region: 'us-east-1' },
      sourcePrefix: 'data/',
      destinationBackend: 'gcs',
      destinationSettings: { bucket: 'dst', project: 'my-project' },
      checkIntervalSeconds: 3600,
      enabled: true,
      replicationMode: ReplicationMode.TRANSPARENT,
    },
  });
  console.log('MCP addReplicationPolicy: ok');

  const trigger = await mcp.triggerReplication({ policyId: 'mcp-s3-to-gcs' });
  console.log('MCP triggerReplication synced:', trigger.result?.synced);

  await mcp.removeReplicationPolicy({ id: 'mcp-s3-to-gcs' });
  await mcp.close();
}

// Run all examples when the servers are available.
(async () => {
  console.log('=== MCP example ===');
  await mcpExample().catch((e: Error) => console.warn('MCP server unavailable:', e.message));

  console.log('\n=== Unix socket example ===');
  await unixExample().catch((e: Error) => console.warn('Unix server unavailable:', e.message));

  console.log('\n=== Streaming example ===');
  await streamingExample().catch((e: Error) =>
    console.warn('REST server unavailable:', e.message)
  );

  console.log('\n=== Auth example ===');
  await authExample().catch((e: Error) => console.warn('REST server unavailable:', e.message));

  console.log('\n=== Replication via MCP example ===');
  await replicationExample().catch((e: Error) =>
    console.warn('MCP server unavailable:', e.message)
  );
})();
