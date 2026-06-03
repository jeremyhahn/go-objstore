import * as net from 'net';
import * as os from 'os';
import * as path from 'path';
import * as fs from 'fs';
import { UnixClient } from '../../src/clients/unix-client';
import { HealthStatus, ReplicationMode } from '../../src/types';
import {
  ObjectNotFoundError,
  AuthenticationError,
  AuthorizationError,
  ValidationError,
  AlreadyExistsError,
  RateLimitError,
  ServerError,
  ConnectionError,
} from '../../src/errors';

/**
 * UnixClient unit-test matrix.
 *
 * A real Unix domain socket server is started per test (on a temp path) so the
 * net module is exercised with real I/O. Matching the real server
 * (pkg/server/unix), each connection is persistent: the server loops over
 * newline-delimited JSON-RPC 2.0 requests, calling the configured handler and
 * writing one response line per request, keeping the connection open.
 */

type Handler = (req: {
  method: string;
  params: unknown;
  id: unknown;
}) => {
  result?: unknown;
  error?: { code: number; message: string };
  /** Override the response id (to simulate a misbehaving server). */
  respId?: unknown;
};

interface MockServer {
  socketPath: string;
  close: () => Promise<void>;
  /** Number of connections accepted so far. */
  connectionCount: () => number;
}

/** Start a persistent-connection mock Unix-socket server. */
function startMockServer(
  handler: Handler,
  opts?: { closeAfterResponse?: boolean }
): Promise<MockServer> {
  return new Promise((resolve, reject) => {
    const socketPath = path.join(os.tmpdir(), `test-unix-${Date.now()}-${Math.random().toString(36).slice(2)}.sock`);
    // Remove stale socket file if any.
    try { fs.unlinkSync(socketPath); } catch { /* ignore */ }

    let connections = 0;
    const sockets: net.Socket[] = [];

    const server = net.createServer((socket) => {
      connections++;
      sockets.push(socket);
      let buf = '';
      socket.on('data', (chunk: Buffer) => {
        buf += chunk.toString('utf8');
        let idx: number;
        while ((idx = buf.indexOf('\n')) !== -1) {
          const line = buf.substring(0, idx);
          buf = buf.substring(idx + 1);
          let req: { method: string; params: unknown; id: unknown };
          try {
            req = JSON.parse(line) as { method: string; params: unknown; id: unknown };
          } catch {
            socket.write(JSON.stringify({ jsonrpc: '2.0', error: { code: -32700, message: 'parse error' }, id: null }) + '\n');
            socket.destroy();
            return;
          }
          const handlerResult = handler(req);
          const resp: Record<string, unknown> = {
            jsonrpc: '2.0',
            id: handlerResult.respId !== undefined ? handlerResult.respId : req.id,
          };
          if (handlerResult.error) {
            resp.error = handlerResult.error;
          } else {
            resp.result = handlerResult.result ?? {};
          }
          socket.write(JSON.stringify(resp) + '\n');
          if (opts?.closeAfterResponse) {
            socket.end();
            return;
          }
        }
      });
      socket.on('error', () => { /* ignore client resets */ });
    });

    server.listen(socketPath, () => {
      resolve({
        socketPath,
        connectionCount: () => connections,
        close: () =>
          new Promise<void>((res) => {
            for (const s of sockets) { s.destroy(); }
            server.close(() => {
              try { fs.unlinkSync(socketPath); } catch { /* ignore */ }
              res();
            });
          }),
      });
    });

    server.on('error', reject);
  });
}

/** Shortcut: start a mock that always returns the given result for any method. */
async function mockResult(result: unknown): Promise<MockServer> {
  return startMockServer(() => ({ result }));
}

/** Shortcut: start a mock that always returns the given RPC error. */
async function mockRpcError(code: number, message: string): Promise<MockServer> {
  return startMockServer(() => ({ error: { code, message } }));
}

describe('UnixClient', () => {
  // -------------------------------------------------------------------------
  // put
  // -------------------------------------------------------------------------
  describe('put', () => {
    it('unix_put_success', async () => {
      const srv = await mockResult({ message: 'ok' });
      const client = new UnixClient({ socketPath: srv.socketPath });
      const resp = await client.put({ key: 'k', data: Buffer.from('hello') });
      expect(resp.success).toBe(true);
      await srv.close();
    });

    it('unix_put_rpc_error', async () => {
      const srv = await mockRpcError(-32603, 'store failed');
      const client = new UnixClient({ socketPath: srv.socketPath });
      await expect(client.put({ key: 'k', data: Buffer.from('x') })).rejects.toThrow(
        'Unix RPC error'
      );
      await srv.close();
    });
  });

  // -------------------------------------------------------------------------
  // get
  // -------------------------------------------------------------------------
  describe('get', () => {
    it('unix_get_success', async () => {
      const encoded = Buffer.from('world').toString('base64');
      const srv = await mockResult({
        data: encoded,
        metadata: { content_type: 'text/plain' },
      });
      const client = new UnixClient({ socketPath: srv.socketPath });
      const resp = await client.get({ key: 'k' });
      expect(resp.data.toString()).toBe('world');
      expect(resp.metadata?.contentType).toBe('text/plain');
      await srv.close();
    });

    it('unix_get_not_found', async () => {
      const srv = await mockRpcError(-32000, 'not found');
      const client = new UnixClient({ socketPath: srv.socketPath });
      await expect(client.get({ key: 'missing' })).rejects.toThrow('Unix RPC error');
      await srv.close();
    });

    it('unix_get_error', async () => {
      const srv = await mockRpcError(-32603, 'internal error');
      const client = new UnixClient({ socketPath: srv.socketPath });
      await expect(client.get({ key: 'k' })).rejects.toThrow('Unix RPC error');
      await srv.close();
    });
  });

  // -------------------------------------------------------------------------
  // delete
  // -------------------------------------------------------------------------
  describe('delete', () => {
    it('unix_delete_success', async () => {
      const srv = await mockResult({ deleted: true });
      const client = new UnixClient({ socketPath: srv.socketPath });
      const resp = await client.delete({ key: 'k' });
      expect(resp.success).toBe(true);
      await srv.close();
    });

    it('unix_delete_error', async () => {
      const srv = await mockRpcError(-32603, 'err');
      const client = new UnixClient({ socketPath: srv.socketPath });
      await expect(client.delete({ key: 'k' })).rejects.toThrow('Unix RPC error');
      await srv.close();
    });
  });

  // -------------------------------------------------------------------------
  // list
  // -------------------------------------------------------------------------
  describe('list', () => {
    it('unix_list_success', async () => {
      const srv = await mockResult({
        objects: [
          { key: 'a', size: 10, last_modified: '2025-01-01T00:00:00Z', etag: 'e1' },
          { key: 'b', size: 20 },
        ],
        next_cursor: '',
        is_truncated: false,
      });
      const client = new UnixClient({ socketPath: srv.socketPath });
      const resp = await client.list({ prefix: '' });
      expect(resp.objects).toHaveLength(2);
      expect(resp.objects[0].key).toBe('a');
      await srv.close();
    });

    it('unix_list_error', async () => {
      const srv = await mockRpcError(-32603, 'err');
      const client = new UnixClient({ socketPath: srv.socketPath });
      await expect(client.list()).rejects.toThrow('Unix RPC error');
      await srv.close();
    });
  });

  // -------------------------------------------------------------------------
  // exists
  // -------------------------------------------------------------------------
  describe('exists', () => {
    it('unix_exists_true', async () => {
      const srv = await mockResult({ exists: true });
      const client = new UnixClient({ socketPath: srv.socketPath });
      const resp = await client.exists({ key: 'k' });
      expect(resp.exists).toBe(true);
      await srv.close();
    });

    it('unix_exists_false', async () => {
      const srv = await mockResult({ exists: false });
      const client = new UnixClient({ socketPath: srv.socketPath });
      const resp = await client.exists({ key: 'k' });
      expect(resp.exists).toBe(false);
      await srv.close();
    });

    it('unix_exists_error', async () => {
      const srv = await mockRpcError(-32603, 'err');
      const client = new UnixClient({ socketPath: srv.socketPath });
      await expect(client.exists({ key: 'k' })).rejects.toThrow('Unix RPC error');
      await srv.close();
    });
  });

  // -------------------------------------------------------------------------
  // getMetadata
  // -------------------------------------------------------------------------
  describe('getMetadata', () => {
    it('unix_getMetadata_success', async () => {
      const srv = await mockResult({
        content_type: 'text/plain',
        size: 42,
        etag: '"abc"',
        last_modified: '2025-06-01T00:00:00Z',
      });
      const client = new UnixClient({ socketPath: srv.socketPath });
      const resp = await client.getMetadata({ key: 'k' });
      expect(resp.success).toBe(true);
      expect(resp.metadata?.contentType).toBe('text/plain');
      expect(resp.metadata?.size).toBe(42);
      await srv.close();
    });

    it('unix_getMetadata_error', async () => {
      const srv = await mockRpcError(-32603, 'err');
      const client = new UnixClient({ socketPath: srv.socketPath });
      await expect(client.getMetadata({ key: 'k' })).rejects.toThrow('Unix RPC error');
      await srv.close();
    });
  });

  // -------------------------------------------------------------------------
  // updateMetadata
  // -------------------------------------------------------------------------
  describe('updateMetadata', () => {
    it('unix_updateMetadata_success', async () => {
      const srv = await mockResult({ updated: true });
      const client = new UnixClient({ socketPath: srv.socketPath });
      const resp = await client.updateMetadata({
        key: 'k',
        metadata: { contentType: 'application/json' },
      });
      expect(resp.success).toBe(true);
      await srv.close();
    });

    it('unix_updateMetadata_error', async () => {
      const srv = await mockRpcError(-32603, 'err');
      const client = new UnixClient({ socketPath: srv.socketPath });
      await expect(
        client.updateMetadata({ key: 'k', metadata: { contentType: 'x' } })
      ).rejects.toThrow('Unix RPC error');
      await srv.close();
    });
  });

  // -------------------------------------------------------------------------
  // health
  // -------------------------------------------------------------------------
  describe('health', () => {
    it('unix_health_healthy', async () => {
      const srv = await mockResult({ status: 'healthy', version: '0.2.0' });
      const client = new UnixClient({ socketPath: srv.socketPath });
      const resp = await client.health();
      expect(resp.status).toBe(HealthStatus.SERVING);
      await srv.close();
    });

    it('unix_health_unhealthy', async () => {
      const srv = await mockResult({ status: 'unhealthy' });
      const client = new UnixClient({ socketPath: srv.socketPath });
      const resp = await client.health();
      expect(resp.status).toBe(HealthStatus.NOT_SERVING);
      await srv.close();
    });

    it('unix_health_error', async () => {
      const srv = await mockRpcError(-32603, 'down');
      const client = new UnixClient({ socketPath: srv.socketPath });
      await expect(client.health()).rejects.toThrow('Unix RPC error');
      await srv.close();
    });
  });

  // -------------------------------------------------------------------------
  // archive
  // -------------------------------------------------------------------------
  describe('archive', () => {
    it('unix_archive_success', async () => {
      const srv = await mockResult({ archived: true });
      const client = new UnixClient({ socketPath: srv.socketPath });
      const resp = await client.archive({ key: 'k', destinationType: 'glacier' });
      expect(resp.success).toBe(true);
      await srv.close();
    });

    it('unix_archive_error', async () => {
      const srv = await mockRpcError(-32603, 'err');
      const client = new UnixClient({ socketPath: srv.socketPath });
      await expect(client.archive({ key: 'k', destinationType: 'glacier' })).rejects.toThrow(
        'Unix RPC error'
      );
      await srv.close();
    });
  });

  // -------------------------------------------------------------------------
  // addPolicy
  // -------------------------------------------------------------------------
  describe('addPolicy', () => {
    it('unix_addPolicy_success', async () => {
      const srv = await mockResult({ added: true });
      const client = new UnixClient({ socketPath: srv.socketPath });
      const resp = await client.addPolicy({
        policy: { id: 'p1', prefix: 'logs/', retentionSeconds: 86400, action: 'delete' },
      });
      expect(resp.success).toBe(true);
      await srv.close();
    });

    it('unix_addPolicy_sends_retention_seconds', async () => {
      // Sub-day retention is supported: the client sends retention_seconds
      // verbatim instead of rejecting non-whole-day values.
      let captured: Record<string, unknown> | undefined;
      const srv = await startMockServer((req) => {
        captured = req.params as Record<string, unknown>;
        return { result: { added: true } };
      });
      const client = new UnixClient({ socketPath: srv.socketPath });
      const resp = await client.addPolicy({
        policy: { id: 'p2', prefix: 'tmp/', retentionSeconds: 90000, action: 'delete' },
      });
      expect(resp.success).toBe(true);
      expect(captured?.retention_seconds).toBe(90000);
      await srv.close();
    });

    it('unix_addPolicy_error', async () => {
      const srv = await mockRpcError(-32603, 'err');
      const client = new UnixClient({ socketPath: srv.socketPath });
      await expect(
        client.addPolicy({
          policy: { id: 'p1', prefix: '', retentionSeconds: 86400, action: 'delete' },
        })
      ).rejects.toThrow('Unix RPC error');
      await srv.close();
    });
  });

  // -------------------------------------------------------------------------
  // removePolicy
  // -------------------------------------------------------------------------
  describe('removePolicy', () => {
    it('unix_removePolicy_success', async () => {
      const srv = await mockResult({ removed: true });
      const client = new UnixClient({ socketPath: srv.socketPath });
      const resp = await client.removePolicy({ id: 'p1' });
      expect(resp.success).toBe(true);
      await srv.close();
    });

    it('unix_removePolicy_error', async () => {
      const srv = await mockRpcError(-32603, 'err');
      const client = new UnixClient({ socketPath: srv.socketPath });
      await expect(client.removePolicy({ id: 'p1' })).rejects.toThrow('Unix RPC error');
      await srv.close();
    });
  });

  // -------------------------------------------------------------------------
  // getPolicies
  // -------------------------------------------------------------------------
  describe('getPolicies', () => {
    it('unix_getPolicies_bare_array', async () => {
      // The server returns a BARE JSON array as the JSON-RPC result.
      const srv = await mockResult([
        { id: 'p1', prefix: 'logs/', action: 'delete', after_days: 1 },
      ]);
      const client = new UnixClient({ socketPath: srv.socketPath });
      const resp = await client.getPolicies();
      expect(resp.policies).toHaveLength(1);
      expect(resp.policies[0].id).toBe('p1');
      expect(resp.policies[0].retentionSeconds).toBe(86400);
      await srv.close();
    });

    it('unix_getPolicies_wrapped_shape', async () => {
      // Defensive: a wrapped { policies: [...] } result is accepted too.
      const srv = await mockResult({
        policies: [{ id: 'p1', prefix: 'logs/', action: 'delete', after_days: 1 }],
      });
      const client = new UnixClient({ socketPath: srv.socketPath });
      const resp = await client.getPolicies();
      expect(resp.policies).toHaveLength(1);
      expect(resp.policies[0].id).toBe('p1');
      await srv.close();
    });

    it('unix_getPolicies_prefers_retention_seconds', async () => {
      const srv = await mockResult([
        { id: 'p1', prefix: '', action: 'delete', after_days: 1, retention_seconds: 90000 },
      ]);
      const client = new UnixClient({ socketPath: srv.socketPath });
      const resp = await client.getPolicies();
      expect(resp.policies[0].retentionSeconds).toBe(90000);
      await srv.close();
    });

    it('unix_getPolicies_error', async () => {
      const srv = await mockRpcError(-32603, 'err');
      const client = new UnixClient({ socketPath: srv.socketPath });
      await expect(client.getPolicies()).rejects.toThrow('Unix RPC error');
      await srv.close();
    });
  });

  // -------------------------------------------------------------------------
  // applyPolicies
  // -------------------------------------------------------------------------
  describe('applyPolicies', () => {
    it('unix_applyPolicies_success', async () => {
      const srv = await mockResult({ policies_count: 1, objects_processed: 3 });
      const client = new UnixClient({ socketPath: srv.socketPath });
      const resp = await client.applyPolicies();
      expect(resp.policiesCount).toBe(1);
      expect(resp.objectsProcessed).toBe(3);
      await srv.close();
    });

    it('unix_applyPolicies_error', async () => {
      const srv = await mockRpcError(-32603, 'err');
      const client = new UnixClient({ socketPath: srv.socketPath });
      await expect(client.applyPolicies()).rejects.toThrow('Unix RPC error');
      await srv.close();
    });
  });

  // -------------------------------------------------------------------------
  // addReplicationPolicy
  // -------------------------------------------------------------------------
  describe('addReplicationPolicy', () => {
    it('unix_addReplicationPolicy_success', async () => {
      const srv = await mockResult({ added: true });
      const client = new UnixClient({ socketPath: srv.socketPath });
      const resp = await client.addReplicationPolicy({
        policy: {
          id: 'r1',
          sourceBackend: 's3',
          sourceSettings: {},
          sourcePrefix: '',
          destinationBackend: 'gcs',
          destinationSettings: {},
          checkIntervalSeconds: 60,
          enabled: true,
          replicationMode: ReplicationMode.TRANSPARENT,
        },
      });
      expect(resp.success).toBe(true);
      await srv.close();
    });

    it('unix_addReplicationPolicy_error', async () => {
      const srv = await mockRpcError(-32603, 'err');
      const client = new UnixClient({ socketPath: srv.socketPath });
      await expect(
        client.addReplicationPolicy({
          policy: {
            id: 'r1',
            sourceBackend: 's3',
            sourceSettings: {},
            sourcePrefix: '',
            destinationBackend: 'gcs',
            destinationSettings: {},
            checkIntervalSeconds: 60,
            enabled: true,
            replicationMode: ReplicationMode.TRANSPARENT,
          },
        })
      ).rejects.toThrow('Unix RPC error');
      await srv.close();
    });
  });

  // -------------------------------------------------------------------------
  // removeReplicationPolicy
  // -------------------------------------------------------------------------
  describe('removeReplicationPolicy', () => {
    it('unix_removeReplicationPolicy_success', async () => {
      const srv = await mockResult({ removed: true });
      const client = new UnixClient({ socketPath: srv.socketPath });
      const resp = await client.removeReplicationPolicy({ id: 'r1' });
      expect(resp.success).toBe(true);
      await srv.close();
    });

    it('unix_removeReplicationPolicy_error', async () => {
      const srv = await mockRpcError(-32603, 'err');
      const client = new UnixClient({ socketPath: srv.socketPath });
      await expect(client.removeReplicationPolicy({ id: 'r1' })).rejects.toThrow('Unix RPC error');
      await srv.close();
    });
  });

  // -------------------------------------------------------------------------
  // getReplicationPolicies
  // -------------------------------------------------------------------------
  describe('getReplicationPolicies', () => {
    it('unix_getReplicationPolicies_bare_array', async () => {
      // The server returns a BARE JSON array as the JSON-RPC result.
      const srv = await mockResult([{ id: 'r1', destination_type: 'gcs', enabled: true }]);
      const client = new UnixClient({ socketPath: srv.socketPath });
      const resp = await client.getReplicationPolicies();
      expect(resp.policies).toHaveLength(1);
      expect(resp.policies[0].id).toBe('r1');
      await srv.close();
    });

    it('unix_getReplicationPolicies_wrapped_shape', async () => {
      // Defensive: a wrapped { policies: [...] } result is accepted too.
      const srv = await mockResult({
        policies: [{ id: 'r1', destination_type: 'gcs', enabled: true }],
      });
      const client = new UnixClient({ socketPath: srv.socketPath });
      const resp = await client.getReplicationPolicies();
      expect(resp.policies).toHaveLength(1);
      expect(resp.policies[0].id).toBe('r1');
      await srv.close();
    });

    it('unix_getReplicationPolicies_error', async () => {
      const srv = await mockRpcError(-32603, 'err');
      const client = new UnixClient({ socketPath: srv.socketPath });
      await expect(client.getReplicationPolicies()).rejects.toThrow('Unix RPC error');
      await srv.close();
    });
  });

  // -------------------------------------------------------------------------
  // getReplicationPolicy
  // -------------------------------------------------------------------------
  describe('getReplicationPolicy', () => {
    it('unix_getReplicationPolicy_success', async () => {
      const srv = await mockResult({ id: 'r1', destination_type: 'gcs', enabled: true });
      const client = new UnixClient({ socketPath: srv.socketPath });
      const resp = await client.getReplicationPolicy({ id: 'r1' });
      expect(resp.policy?.id).toBe('r1');
      await srv.close();
    });

    it('unix_getReplicationPolicy_error', async () => {
      const srv = await mockRpcError(-32603, 'err');
      const client = new UnixClient({ socketPath: srv.socketPath });
      await expect(client.getReplicationPolicy({ id: 'r1' })).rejects.toThrow('Unix RPC error');
      await srv.close();
    });
  });

  // -------------------------------------------------------------------------
  // triggerReplication
  // -------------------------------------------------------------------------
  describe('triggerReplication', () => {
    it('unix_triggerReplication_success', async () => {
      const srv = await mockResult({ objects_synced: 5, objects_failed: 0, bytes_transferred: 1024 });
      const client = new UnixClient({ socketPath: srv.socketPath });
      const resp = await client.triggerReplication({ policyId: 'r1' });
      expect(resp.success).toBe(true);
      expect(resp.result?.synced).toBe(5);
      await srv.close();
    });

    it('unix_triggerReplication_error', async () => {
      const srv = await mockRpcError(-32603, 'err');
      const client = new UnixClient({ socketPath: srv.socketPath });
      await expect(client.triggerReplication({})).rejects.toThrow('Unix RPC error');
      await srv.close();
    });
  });

  // -------------------------------------------------------------------------
  // getReplicationStatus
  // -------------------------------------------------------------------------
  describe('getReplicationStatus', () => {
    it('unix_getReplicationStatus_success', async () => {
      const srv = await mockResult({
        policy_id: 'r1',
        status: 'active',
        objects_synced: 10,
        last_sync_time: '2025-06-01T00:00:00Z',
      });
      const client = new UnixClient({ socketPath: srv.socketPath });
      const resp = await client.getReplicationStatus({ id: 'r1' });
      expect(resp.success).toBe(true);
      expect(resp.status?.policyId).toBe('r1');
      expect(resp.status?.totalObjectsSynced).toBe(10);
      await srv.close();
    });

    it('unix_getReplicationStatus_error', async () => {
      const srv = await mockRpcError(-32603, 'err');
      const client = new UnixClient({ socketPath: srv.socketPath });
      await expect(client.getReplicationStatus({ id: 'r1' })).rejects.toThrow('Unix RPC error');
      await srv.close();
    });
  });

  // -------------------------------------------------------------------------
  // close
  // -------------------------------------------------------------------------
  describe('close', () => {
    it('unix_close', async () => {
      const socketPath = '/tmp/no-such-socket.sock';
      const client = new UnixClient({ socketPath });
      await expect(client.close()).resolves.toBeUndefined();
    });
  });

  // -------------------------------------------------------------------------
  // connection_error: socket doesn't exist
  // -------------------------------------------------------------------------
  describe('connection_error', () => {
    it('unix_no_socket_throws', async () => {
      const client = new UnixClient({ socketPath: '/tmp/does-not-exist-ever.sock' });
      await expect(client.health()).rejects.toThrow('Unix socket error');
    });
  });

  // -------------------------------------------------------------------------
  // typed JSON-RPC error mapping
  // -------------------------------------------------------------------------
  describe('typed_errors', () => {
    it('unix_not_found_maps_to_ObjectNotFoundError', async () => {
      const srv = await mockRpcError(-32004, 'object not found: missing');
      const client = new UnixClient({ socketPath: srv.socketPath });
      const err = await client.get({ key: 'missing' }).then(
        () => undefined,
        (e) => e
      );
      expect(err).toBeInstanceOf(ObjectNotFoundError);
      expect((err as Error).message).toContain('Unix RPC error (-32004)');
      expect((err as Error).message).toContain('object not found: missing');
      await srv.close();
    });

    it('unix_unauthenticated_maps_to_AuthenticationError', async () => {
      const srv = await mockRpcError(-32002, 'missing credentials');
      const client = new UnixClient({ socketPath: srv.socketPath });
      await expect(client.get({ key: 'k' })).rejects.toBeInstanceOf(AuthenticationError);
      await srv.close();
    });

    it('unix_forbidden_maps_to_AuthorizationError', async () => {
      const srv = await mockRpcError(-32001, 'access denied');
      const client = new UnixClient({ socketPath: srv.socketPath });
      await expect(client.delete({ key: 'k' })).rejects.toBeInstanceOf(AuthorizationError);
      await srv.close();
    });

    it('unix_invalid_params_maps_to_ValidationError', async () => {
      const srv = await mockRpcError(-32602, 'key is required');
      const client = new UnixClient({ socketPath: srv.socketPath });
      await expect(client.get({ key: 'k' })).rejects.toBeInstanceOf(ValidationError);
      await srv.close();
    });

    it('unix_already_exists_maps_to_AlreadyExistsError', async () => {
      const srv = await mockRpcError(-32005, 'object already exists');
      const client = new UnixClient({ socketPath: srv.socketPath });
      const err = await client.put({ key: 'k', data: Buffer.from('v') }).then(
        () => undefined,
        (e) => e
      );
      expect(err).toBeInstanceOf(AlreadyExistsError);
      expect((err as AlreadyExistsError).statusCode).toBe(409);
      await srv.close();
    });

    it('unix_rate_limited_maps_to_RateLimitError', async () => {
      const srv = await mockRpcError(-32029, 'rate limited');
      const client = new UnixClient({ socketPath: srv.socketPath });
      const err = await client.get({ key: 'k' }).then(
        () => undefined,
        (e) => e
      );
      expect(err).toBeInstanceOf(RateLimitError);
      expect((err as RateLimitError).statusCode).toBe(429);
      await srv.close();
    });

    it('unix_unknown_code_maps_to_ServerError', async () => {
      const srv = await mockRpcError(-32603, 'internal error');
      const client = new UnixClient({ socketPath: srv.socketPath });
      await expect(client.get({ key: 'k' })).rejects.toBeInstanceOf(ServerError);
      await srv.close();
    });
  });

  // -------------------------------------------------------------------------
  // persistent connection
  // -------------------------------------------------------------------------
  describe('persistent_connection', () => {
    it('unix_reuses_one_connection_across_rpcs', async () => {
      const srv = await mockResult({ exists: true });
      const client = new UnixClient({ socketPath: srv.socketPath });
      await client.exists({ key: 'a' });
      await client.exists({ key: 'b' });
      await client.exists({ key: 'c' });
      expect(srv.connectionCount()).toBe(1);
      await client.close();
      await srv.close();
    });

    it('unix_reconnects_after_server_closes_connection', async () => {
      // Simulate the server's idle deadline by closing after each response.
      const srv = await startMockServer(() => ({ result: { exists: true } }), {
        closeAfterResponse: true,
      });
      const client = new UnixClient({ socketPath: srv.socketPath });
      const first = await client.exists({ key: 'a' });
      // Wait for the client to observe the server-side close.
      await new Promise((res) => setTimeout(res, 50));
      const second = await client.exists({ key: 'b' });
      expect(first.exists).toBe(true);
      expect(second.exists).toBe(true);
      expect(srv.connectionCount()).toBe(2);
      await client.close();
      await srv.close();
    });

    it('unix_id_mismatch_rejects_and_reconnects', async () => {
      let calls = 0;
      const srv = await startMockServer(() => {
        calls++;
        if (calls === 1) {
          return { result: { exists: true }, respId: 999999 };
        }
        return { result: { exists: true } };
      });
      const client = new UnixClient({ socketPath: srv.socketPath });
      const err = await client.exists({ key: 'a' }).then(
        () => undefined,
        (e) => e
      );
      expect(err).toBeInstanceOf(ConnectionError);
      expect((err as Error).message).toContain('id mismatch');
      // The bad connection is destroyed; the next call dials a fresh one.
      const resp = await client.exists({ key: 'b' });
      expect(resp.exists).toBe(true);
      expect(srv.connectionCount()).toBe(2);
      await client.close();
      await srv.close();
    });
  });

  // -------------------------------------------------------------------------
  // timeout
  // -------------------------------------------------------------------------
  describe('timeout', () => {
    it('unix_timeout_throws', async () => {
      // Start a server that accepts connections but never writes a response.
      const socketPath = path.join(
        os.tmpdir(),
        `test-unix-timeout-${Date.now()}.sock`
      );
      try { fs.unlinkSync(socketPath); } catch { /* ignore */ }
      const sockets: net.Socket[] = [];
      const server = net.createServer((socket) => {
        sockets.push(socket);
        // Intentionally never write a response.
      });
      await new Promise<void>((res) => server.listen(socketPath, res));

      const client = new UnixClient({ socketPath, timeout: 200 });
      await expect(client.health()).rejects.toThrow('Unix RPC timeout');

      // Destroy all open connections so server.close() resolves immediately.
      for (const s of sockets) { s.destroy(); }
      await new Promise<void>((res) => server.close(() => res()));
      try { fs.unlinkSync(socketPath); } catch { /* ignore */ }
    }, 5000);
  });
});
