import nock from 'nock';
import { McpClient } from '../../src/clients/mcp-client';
import { HealthStatus, ReplicationMode } from '../../src/types';
import {
  ObjectNotFoundError,
  AuthenticationError,
  AuthorizationError,
  ValidationError,
  AlreadyExistsError,
  RateLimitError,
  ServerError,
} from '../../src/errors';

/**
 * McpClient unit-test matrix.
 *
 * The MCP transport is HTTP POST JSON-RPC 2.0 to "/". Each test mocks the
 * HTTP layer with nock. Success cases verify that the client sends the right
 * tool name + arguments and correctly parses result.content[0].text. Error
 * cases verify that MCP-level errors and HTTP errors are surfaced as thrown
 * exceptions. No live server is required.
 */

const BASE = 'http://localhost:9090';

/** Build a nock interceptor that expects the named tool and replies with text. */
function mockTool(
  scope: nock.Scope,
  toolName: string,
  textResult: object,
  opts?: { statusCode?: number; rpcError?: { code: number; message: string } }
): void {
  scope.post('/').reply(opts?.statusCode ?? 200, (_uri: string, body: unknown) => {
    const req = body as { params?: { name?: string } };
    if (req?.params?.name !== toolName) {
      // Wrong tool name → reply with error so the test fails clearly.
      return {
        jsonrpc: '2.0',
        error: { code: -32601, message: `unexpected tool: ${req?.params?.name}` },
        id: 1,
      };
    }
    if (opts?.rpcError) {
      return { jsonrpc: '2.0', error: opts.rpcError, id: 1 };
    }
    return {
      jsonrpc: '2.0',
      result: {
        content: [{ type: 'text', text: JSON.stringify(textResult) }],
      },
      id: 1,
    };
  });
}

describe('McpClient', () => {
  let client: McpClient;

  beforeEach(() => {
    client = new McpClient({ baseUrl: BASE });
  });

  afterEach(() => {
    nock.cleanAll();
  });

  // -------------------------------------------------------------------------
  // put
  // -------------------------------------------------------------------------
  describe('put', () => {
    it('mcp_put_success', async () => {
      const scope = nock(BASE);
      mockTool(scope, 'objstore_put', { success: true, key: 'k', size: 4 });
      const resp = await client.put({ key: 'k', data: Buffer.from('data') });
      expect(resp.success).toBe(true);
    });

    it('mcp_put_rpc_error', async () => {
      const scope = nock(BASE);
      mockTool(scope, 'objstore_put', {}, { rpcError: { code: -32603, message: 'store failed' } });
      await expect(client.put({ key: 'k', data: Buffer.from('x') })).rejects.toThrow(
        'MCP error (-32603)'
      );
    });
  });

  // -------------------------------------------------------------------------
  // get
  // -------------------------------------------------------------------------
  describe('get', () => {
    it('mcp_get_success', async () => {
      const scope = nock(BASE);
      // The server returns object data base64-encoded in the tool result JSON.
      mockTool(scope, 'objstore_get', {
        success: true,
        key: 'k',
        size: 5,
        data: Buffer.from('hello').toString('base64'),
      });
      const resp = await client.get({ key: 'k' });
      expect(resp.data.toString()).toBe('hello');
    });

    it('mcp_get_binary_round_trip', async () => {
      const payload = Buffer.from([0x00, 0x01, 0xff, 0xfe, 0x80]);
      const scope = nock(BASE);
      mockTool(scope, 'objstore_get', {
        success: true,
        key: 'bin',
        size: payload.length,
        data: payload.toString('base64'),
      });
      const resp = await client.get({ key: 'bin' });
      expect(Buffer.compare(resp.data, payload)).toBe(0);
    });

    it('mcp_get_error', async () => {
      const scope = nock(BASE);
      mockTool(scope, 'objstore_get', {}, { rpcError: { code: -32603, message: 'not found' } });
      await expect(client.get({ key: 'k' })).rejects.toThrow('MCP error');
    });
  });

  // -------------------------------------------------------------------------
  // typed JSON-RPC error mapping
  // -------------------------------------------------------------------------
  describe('typed_errors', () => {
    it('mcp_not_found_maps_to_ObjectNotFoundError', async () => {
      const scope = nock(BASE);
      mockTool(scope, 'objstore_get', {}, { rpcError: { code: -32004, message: 'object not found: missing' } });
      const err = await client.get({ key: 'missing' }).then(
        () => undefined,
        (e) => e
      );
      expect(err).toBeInstanceOf(ObjectNotFoundError);
      expect((err as Error).message).toContain('MCP error (-32004)');
      expect((err as Error).message).toContain('object not found: missing');
    });

    it('mcp_unauthenticated_maps_to_AuthenticationError', async () => {
      const scope = nock(BASE);
      mockTool(scope, 'objstore_get', {}, { rpcError: { code: -32002, message: 'missing credentials' } });
      await expect(client.get({ key: 'k' })).rejects.toBeInstanceOf(AuthenticationError);
    });

    it('mcp_forbidden_maps_to_AuthorizationError', async () => {
      const scope = nock(BASE);
      mockTool(scope, 'objstore_get', {}, { rpcError: { code: -32001, message: 'access denied' } });
      await expect(client.get({ key: 'k' })).rejects.toBeInstanceOf(AuthorizationError);
    });

    it('mcp_invalid_params_maps_to_ValidationError', async () => {
      const scope = nock(BASE);
      mockTool(scope, 'objstore_get', {}, { rpcError: { code: -32602, message: 'key is required' } });
      await expect(client.get({ key: 'k' })).rejects.toBeInstanceOf(ValidationError);
    });

    it('mcp_already_exists_maps_to_AlreadyExistsError', async () => {
      const scope = nock(BASE);
      mockTool(scope, 'objstore_get', {}, { rpcError: { code: -32005, message: 'object already exists' } });
      const err = await client.get({ key: 'k' }).then(
        () => undefined,
        (e) => e
      );
      expect(err).toBeInstanceOf(AlreadyExistsError);
      expect((err as AlreadyExistsError).statusCode).toBe(409);
    });

    it('mcp_rate_limited_maps_to_RateLimitError', async () => {
      const scope = nock(BASE);
      mockTool(scope, 'objstore_get', {}, { rpcError: { code: -32029, message: 'rate limited' } });
      const err = await client.get({ key: 'k' }).then(
        () => undefined,
        (e) => e
      );
      expect(err).toBeInstanceOf(RateLimitError);
      expect((err as RateLimitError).statusCode).toBe(429);
    });

    it('mcp_internal_error_maps_to_ServerError', async () => {
      const scope = nock(BASE);
      mockTool(scope, 'objstore_get', {}, { rpcError: { code: -32603, message: 'internal error' } });
      const err = await client.get({ key: 'k' }).then(
        () => undefined,
        (e) => e
      );
      expect(err).toBeInstanceOf(ServerError);
      expect((err as ServerError).statusCode).toBe(500);
    });
  });

  // -------------------------------------------------------------------------
  // delete
  // -------------------------------------------------------------------------
  describe('delete', () => {
    it('mcp_delete_success', async () => {
      const scope = nock(BASE);
      mockTool(scope, 'objstore_delete', { success: true, key: 'k', deleted: true });
      const resp = await client.delete({ key: 'k' });
      expect(resp.success).toBe(true);
    });

    it('mcp_delete_error', async () => {
      const scope = nock(BASE);
      mockTool(scope, 'objstore_delete', {}, { rpcError: { code: -32603, message: 'failed' } });
      await expect(client.delete({ key: 'k' })).rejects.toThrow('MCP error');
    });
  });

  // -------------------------------------------------------------------------
  // list
  // -------------------------------------------------------------------------
  describe('list', () => {
    it('mcp_list_success', async () => {
      const scope = nock(BASE);
      mockTool(scope, 'objstore_list', {
        success: true,
        keys: ['a', 'b'],
        count: 2,
        truncated: false,
      });
      const resp = await client.list({ prefix: 'a' });
      expect(resp.objects).toHaveLength(2);
      expect(resp.objects[0].key).toBe('a');
    });

    it('mcp_list_error', async () => {
      const scope = nock(BASE);
      mockTool(scope, 'objstore_list', {}, { rpcError: { code: -32603, message: 'err' } });
      await expect(client.list()).rejects.toThrow('MCP error');
    });
  });

  // -------------------------------------------------------------------------
  // exists
  // -------------------------------------------------------------------------
  describe('exists', () => {
    it('mcp_exists_true', async () => {
      const scope = nock(BASE);
      mockTool(scope, 'objstore_exists', { success: true, key: 'k', exists: true });
      const resp = await client.exists({ key: 'k' });
      expect(resp.exists).toBe(true);
    });

    it('mcp_exists_false', async () => {
      const scope = nock(BASE);
      mockTool(scope, 'objstore_exists', { success: true, key: 'k', exists: false });
      const resp = await client.exists({ key: 'k' });
      expect(resp.exists).toBe(false);
    });

    it('mcp_exists_error', async () => {
      const scope = nock(BASE);
      mockTool(scope, 'objstore_exists', {}, { rpcError: { code: -32603, message: 'err' } });
      await expect(client.exists({ key: 'k' })).rejects.toThrow('MCP error');
    });
  });

  // -------------------------------------------------------------------------
  // getMetadata
  // -------------------------------------------------------------------------
  describe('getMetadata', () => {
    it('mcp_getMetadata_success', async () => {
      const scope = nock(BASE);
      mockTool(scope, 'objstore_get_metadata', {
        success: true,
        key: 'k',
        size: 100,
        content_type: 'text/plain',
        etag: '"abc"',
      });
      const resp = await client.getMetadata({ key: 'k' });
      expect(resp.success).toBe(true);
      expect(resp.metadata?.size).toBe(100);
      expect(resp.metadata?.contentType).toBe('text/plain');
    });

    it('mcp_getMetadata_error', async () => {
      const scope = nock(BASE);
      mockTool(scope, 'objstore_get_metadata', {}, { rpcError: { code: -32603, message: 'err' } });
      await expect(client.getMetadata({ key: 'k' })).rejects.toThrow('MCP error');
    });
  });

  // -------------------------------------------------------------------------
  // updateMetadata
  // -------------------------------------------------------------------------
  describe('updateMetadata', () => {
    it('mcp_updateMetadata_success', async () => {
      const scope = nock(BASE);
      mockTool(scope, 'objstore_update_metadata', { success: true, key: 'k', updated: true });
      const resp = await client.updateMetadata({
        key: 'k',
        metadata: { contentType: 'application/json' },
      });
      expect(resp.success).toBe(true);
    });

    it('mcp_updateMetadata_error', async () => {
      const scope = nock(BASE);
      mockTool(scope, 'objstore_update_metadata', {}, { rpcError: { code: -32603, message: 'err' } });
      await expect(
        client.updateMetadata({ key: 'k', metadata: { contentType: 'x' } })
      ).rejects.toThrow('MCP error');
    });
  });

  // -------------------------------------------------------------------------
  // health
  // -------------------------------------------------------------------------
  describe('health', () => {
    it('mcp_health_healthy', async () => {
      const scope = nock(BASE);
      mockTool(scope, 'objstore_health', { status: 'healthy', version: '0.2.0' });
      const resp = await client.health();
      expect(resp.status).toBe(HealthStatus.SERVING);
    });

    it('mcp_health_error', async () => {
      const scope = nock(BASE);
      mockTool(scope, 'objstore_health', {}, { rpcError: { code: -32603, message: 'down' } });
      await expect(client.health()).rejects.toThrow('MCP error');
    });
  });

  // -------------------------------------------------------------------------
  // archive
  // -------------------------------------------------------------------------
  describe('archive', () => {
    it('mcp_archive_success', async () => {
      const scope = nock(BASE);
      mockTool(scope, 'objstore_archive', { success: true, key: 'k', archived: true });
      const resp = await client.archive({ key: 'k', destinationType: 'glacier' });
      expect(resp.success).toBe(true);
    });

    it('mcp_archive_error', async () => {
      const scope = nock(BASE);
      mockTool(scope, 'objstore_archive', {}, { rpcError: { code: -32603, message: 'err' } });
      await expect(client.archive({ key: 'k', destinationType: 'glacier' })).rejects.toThrow(
        'MCP error'
      );
    });
  });

  // -------------------------------------------------------------------------
  // addPolicy
  // -------------------------------------------------------------------------
  describe('addPolicy', () => {
    it('mcp_addPolicy_success', async () => {
      const scope = nock(BASE);
      mockTool(scope, 'objstore_add_policy', { success: true, id: 'p1', added: true });
      const resp = await client.addPolicy({
        policy: { id: 'p1', prefix: 'logs/', retentionSeconds: 86400, action: 'delete' },
      });
      expect(resp.success).toBe(true);
    });

    it('mcp_addPolicy_error', async () => {
      const scope = nock(BASE);
      mockTool(scope, 'objstore_add_policy', {}, { rpcError: { code: -32603, message: 'err' } });
      await expect(
        client.addPolicy({
          policy: { id: 'p1', prefix: '', retentionSeconds: 86400, action: 'delete' },
        })
      ).rejects.toThrow('MCP error');
    });
  });

  // -------------------------------------------------------------------------
  // removePolicy
  // -------------------------------------------------------------------------
  describe('removePolicy', () => {
    it('mcp_removePolicy_success', async () => {
      const scope = nock(BASE);
      mockTool(scope, 'objstore_remove_policy', { success: true, id: 'p1', removed: true });
      const resp = await client.removePolicy({ id: 'p1' });
      expect(resp.success).toBe(true);
    });

    it('mcp_removePolicy_error', async () => {
      const scope = nock(BASE);
      mockTool(scope, 'objstore_remove_policy', {}, { rpcError: { code: -32603, message: 'err' } });
      await expect(client.removePolicy({ id: 'p1' })).rejects.toThrow('MCP error');
    });
  });

  // -------------------------------------------------------------------------
  // getPolicies
  // -------------------------------------------------------------------------
  describe('getPolicies', () => {
    it('mcp_getPolicies_success', async () => {
      const scope = nock(BASE);
      mockTool(scope, 'objstore_get_policies', {
        success: true,
        policies: [{ id: 'p1', prefix: 'logs/', action: 'delete', retention_seconds: 86400 }],
        count: 1,
      });
      const resp = await client.getPolicies();
      expect(resp.policies).toHaveLength(1);
      expect(resp.policies[0].id).toBe('p1');
    });

    it('mcp_getPolicies_error', async () => {
      const scope = nock(BASE);
      mockTool(scope, 'objstore_get_policies', {}, { rpcError: { code: -32603, message: 'err' } });
      await expect(client.getPolicies()).rejects.toThrow('MCP error');
    });
  });

  // -------------------------------------------------------------------------
  // applyPolicies
  // -------------------------------------------------------------------------
  describe('applyPolicies', () => {
    it('mcp_applyPolicies_success', async () => {
      const scope = nock(BASE);
      mockTool(scope, 'objstore_apply_policies', {
        success: true,
        policies_count: 2,
        objects_processed: 5,
      });
      const resp = await client.applyPolicies();
      expect(resp.policiesCount).toBe(2);
      expect(resp.objectsProcessed).toBe(5);
    });

    it('mcp_applyPolicies_error', async () => {
      const scope = nock(BASE);
      mockTool(scope, 'objstore_apply_policies', {}, { rpcError: { code: -32603, message: 'err' } });
      await expect(client.applyPolicies()).rejects.toThrow('MCP error');
    });
  });

  // -------------------------------------------------------------------------
  // addReplicationPolicy
  // -------------------------------------------------------------------------
  describe('addReplicationPolicy', () => {
    it('mcp_addReplicationPolicy_success', async () => {
      const scope = nock(BASE);
      mockTool(scope, 'objstore_add_replication_policy', { success: true, id: 'r1' });
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
    });

    it('mcp_addReplicationPolicy_error', async () => {
      const scope = nock(BASE);
      mockTool(scope, 'objstore_add_replication_policy', {}, {
        rpcError: { code: -32603, message: 'err' },
      });
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
      ).rejects.toThrow('MCP error');
    });
  });

  // -------------------------------------------------------------------------
  // removeReplicationPolicy
  // -------------------------------------------------------------------------
  describe('removeReplicationPolicy', () => {
    it('mcp_removeReplicationPolicy_success', async () => {
      const scope = nock(BASE);
      mockTool(scope, 'objstore_remove_replication_policy', { success: true, id: 'r1' });
      const resp = await client.removeReplicationPolicy({ id: 'r1' });
      expect(resp.success).toBe(true);
    });

    it('mcp_removeReplicationPolicy_error', async () => {
      const scope = nock(BASE);
      mockTool(scope, 'objstore_remove_replication_policy', {}, {
        rpcError: { code: -32603, message: 'err' },
      });
      await expect(client.removeReplicationPolicy({ id: 'r1' })).rejects.toThrow('MCP error');
    });
  });

  // -------------------------------------------------------------------------
  // getReplicationPolicies
  // -------------------------------------------------------------------------
  describe('getReplicationPolicies', () => {
    it('mcp_getReplicationPolicies_success', async () => {
      const scope = nock(BASE);
      mockTool(scope, 'objstore_list_replication_policies', {
        success: true,
        policies: [
          {
            id: 'r1',
            source_backend: 's3',
            destination_backend: 'gcs',
            check_interval: 60,
            enabled: true,
            replication_mode: 'transparent',
          },
        ],
        count: 1,
      });
      const resp = await client.getReplicationPolicies();
      expect(resp.policies).toHaveLength(1);
      expect(resp.policies[0].id).toBe('r1');
    });

    it('mcp_getReplicationPolicies_error', async () => {
      const scope = nock(BASE);
      mockTool(scope, 'objstore_list_replication_policies', {}, {
        rpcError: { code: -32603, message: 'err' },
      });
      await expect(client.getReplicationPolicies()).rejects.toThrow('MCP error');
    });
  });

  // -------------------------------------------------------------------------
  // getReplicationPolicy
  // -------------------------------------------------------------------------
  describe('getReplicationPolicy', () => {
    it('mcp_getReplicationPolicy_success', async () => {
      const scope = nock(BASE);
      mockTool(scope, 'objstore_get_replication_policy', {
        success: true,
        id: 'r1',
        source_backend: 's3',
        destination_backend: 'gcs',
        check_interval: 60,
        enabled: true,
        replication_mode: 'transparent',
      });
      const resp = await client.getReplicationPolicy({ id: 'r1' });
      expect(resp.policy?.id).toBe('r1');
    });

    it('mcp_getReplicationPolicy_error', async () => {
      const scope = nock(BASE);
      mockTool(scope, 'objstore_get_replication_policy', {}, {
        rpcError: { code: -32603, message: 'err' },
      });
      await expect(client.getReplicationPolicy({ id: 'r1' })).rejects.toThrow('MCP error');
    });
  });

  // -------------------------------------------------------------------------
  // triggerReplication
  // -------------------------------------------------------------------------
  describe('triggerReplication', () => {
    it('mcp_triggerReplication_success', async () => {
      const scope = nock(BASE);
      mockTool(scope, 'objstore_trigger_replication', {
        success: true,
        result: { policy_id: 'r1', synced: 3, deleted: 0, failed: 0, bytes_total: 512 },
      });
      const resp = await client.triggerReplication({ policyId: 'r1' });
      expect(resp.success).toBe(true);
      expect(resp.result?.synced).toBe(3);
    });

    it('mcp_triggerReplication_error', async () => {
      const scope = nock(BASE);
      mockTool(scope, 'objstore_trigger_replication', {}, {
        rpcError: { code: -32603, message: 'err' },
      });
      await expect(client.triggerReplication({})).rejects.toThrow('MCP error');
    });
  });

  // -------------------------------------------------------------------------
  // getReplicationStatus
  // -------------------------------------------------------------------------
  describe('getReplicationStatus', () => {
    it('mcp_getReplicationStatus_success', async () => {
      const scope = nock(BASE);
      mockTool(scope, 'objstore_get_replication_status', {
        success: true,
        policy_id: 'r1',
        source_backend: 's3',
        destination_backend: 'gcs',
        enabled: true,
        total_objects_synced: 10,
        sync_count: 2,
      });
      const resp = await client.getReplicationStatus({ id: 'r1' });
      expect(resp.success).toBe(true);
      expect(resp.status?.policyId).toBe('r1');
      expect(resp.status?.totalObjectsSynced).toBe(10);
    });

    it('mcp_getReplicationStatus_error', async () => {
      const scope = nock(BASE);
      mockTool(scope, 'objstore_get_replication_status', {}, {
        rpcError: { code: -32603, message: 'err' },
      });
      await expect(client.getReplicationStatus({ id: 'r1' })).rejects.toThrow('MCP error');
    });
  });

  // -------------------------------------------------------------------------
  // close
  // -------------------------------------------------------------------------
  describe('close', () => {
    it('mcp_close', async () => {
      await expect(client.close()).resolves.toBeUndefined();
    });
  });

  // -------------------------------------------------------------------------
  // auth: token + tenantId headers
  // -------------------------------------------------------------------------
  describe('auth', () => {
    it('mcp_auth_token_and_tenant', async () => {
      const authClient = new McpClient({
        baseUrl: BASE,
        token: 'my-token',
        tenantId: 'tenant-42',
      });
      let capturedHeaders: Record<string, string> = {};
      nock(BASE)
        .post('/')
        .reply(200, function (_uri: string, body: unknown) {
          capturedHeaders = this.req.headers as Record<string, string>;
          const req = body as { params?: { name?: string } };
          return {
            jsonrpc: '2.0',
            result: {
              content: [{ type: 'text', text: JSON.stringify({ status: 'healthy' }) }],
            },
            id: (req as unknown as { id: number }).id ?? 1,
          };
        });
      await authClient.health();
      const auth = Array.isArray(capturedHeaders.authorization)
        ? capturedHeaders.authorization[0]
        : capturedHeaders.authorization;
      const tenant = Array.isArray(capturedHeaders['x-tenant-id'])
        ? capturedHeaders['x-tenant-id'][0]
        : capturedHeaders['x-tenant-id'];
      expect(auth).toBe('Bearer my-token');
      expect(tenant).toBe('tenant-42');
    });
  });

  // -------------------------------------------------------------------------
  // http_error: axios-level error is surfaced
  // -------------------------------------------------------------------------
  describe('http_error', () => {
    it('mcp_http_500_throws', async () => {
      nock(BASE).post('/').reply(500, { message: 'server error' });
      await expect(client.health()).rejects.toThrow('MCP HTTP error (500)');
    });
  });
});
