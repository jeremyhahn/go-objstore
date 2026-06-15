import { QuicClient } from '../../src/clients/quic-client';
import { HealthStatus, ReplicationMode } from '../../src/types';

/**
 * Canonical QUIC/HTTP3 client unit-test matrix.
 *
 * For each of the 19 operations: success + error (HTTP 5xx). Nine operations
 * additionally get a not_found (HTTP 404) case. Plus metadata_round_trip and
 * validation_empty_key. The QUIC client uses the global fetch API, which is
 * mocked here; no live server is required.
 */

interface MockResponseInit {
  ok?: boolean;
  status?: number;
  headers?: Record<string, string>;
  json?: any;
  text?: string;
  arrayBuffer?: Buffer;
}

/** Build a minimal Response-like object that the QUIC client understands. */
function mockResponse(init: MockResponseInit): any {
  const status = init.status ?? 200;
  const ok = init.ok ?? (status >= 200 && status < 300);
  const headerMap = new Map<string, string>();
  for (const [k, v] of Object.entries(init.headers || {})) {
    headerMap.set(k.toLowerCase(), v);
  }
  const headers = {
    get: (name: string) => headerMap.get(name.toLowerCase()) ?? null,
    forEach: (cb: (value: string, name: string) => void) => {
      headerMap.forEach((value, name) => cb(value, name));
    },
  };
  return {
    ok,
    status,
    headers,
    json: async () => init.json ?? {},
    text: async () => init.text ?? '',
    arrayBuffer: async () => {
      const buf = init.arrayBuffer ?? Buffer.alloc(0);
      return buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.byteLength);
    },
  };
}

const jsonHeaders = { 'content-type': 'application/json' };

describe('QuicClient', () => {
  const address = 'localhost:8443';
  let client: QuicClient;
  const mockFetch = jest.fn();

  beforeAll(() => {
    global.fetch = mockFetch as any;
  });

  beforeEach(() => {
    client = new QuicClient({ address, secure: false });
    mockFetch.mockReset();
  });

  /** Resolve fetch with a JSON body (5xx → error path). */
  const resolveJson = (json: any, status = 200) =>
    mockFetch.mockResolvedValue(mockResponse({ status, headers: jsonHeaders, json, text: 'error body' }));

  /** Reject the next request with a 5xx HTTP error. */
  const resolveError = (status = 500) =>
    mockFetch.mockResolvedValue(mockResponse({ status, headers: jsonHeaders, text: 'boom' }));

  // --------------------------------------------------------------------------
  // put
  // --------------------------------------------------------------------------
  describe('put', () => {
    it('quic_put_success', async () => {
      mockFetch.mockResolvedValue(
        mockResponse({ headers: { ...jsonHeaders, etag: '"abc123"' }, json: { message: 'stored' } })
      );

      const response = await client.put({ key: 'test-key', data: Buffer.from('data') });

      expect(response.success).toBe(true);
      expect(response.message).toBe('stored');
      expect(response.etag).toBe('"abc123"');
    });

    it('quic_put_error', async () => {
      resolveError();
      await expect(
        client.put({ key: 'test-key', data: Buffer.from('data') })
      ).rejects.toThrow('QUIC/HTTP3 error (500)');
    });
  });

  // --------------------------------------------------------------------------
  // get
  // --------------------------------------------------------------------------
  describe('get', () => {
    it('quic_get_success', async () => {
      mockFetch.mockResolvedValue(
        mockResponse({
          headers: { 'content-type': 'text/plain', 'content-length': '5' },
          arrayBuffer: Buffer.from('hello'),
        })
      );

      const response = await client.get({ key: 'test-key' });

      expect(response.data.toString()).toBe('hello');
      expect(response.metadata?.contentType).toBe('text/plain');
      expect(response.metadata?.size).toBe(5);
    });

    it('quic_get_error', async () => {
      mockFetch.mockResolvedValue(mockResponse({ status: 500, text: 'boom' }));
      await expect(client.get({ key: 'test-key' })).rejects.toThrow('QUIC/HTTP3 error (500)');
    });

    it('quic_get_not_found', async () => {
      mockFetch.mockResolvedValue(mockResponse({ status: 404, text: 'not found' }));
      await expect(client.get({ key: 'missing' })).rejects.toThrow('QUIC/HTTP3 error (404)');
    });
  });

  // --------------------------------------------------------------------------
  // delete
  // --------------------------------------------------------------------------
  describe('delete', () => {
    it('quic_delete_success_204_no_content', async () => {
      mockFetch.mockResolvedValue(mockResponse({ status: 204, headers: {} }));
      const response = await client.delete({ key: 'test-key' });
      expect(response.success).toBe(true);
      expect(response.message).toBe('Object deleted successfully');
    });

    it('quic_delete_tolerates_legacy_200_body', async () => {
      resolveJson({ message: 'deleted' });
      const response = await client.delete({ key: 'test-key' });
      expect(response.success).toBe(true);
      expect(response.message).toBe('deleted');
    });

    it('quic_delete_error', async () => {
      resolveError();
      await expect(client.delete({ key: 'test-key' })).rejects.toThrow('QUIC/HTTP3 error (500)');
    });

    it('quic_delete_not_found', async () => {
      // Deleting a missing object surfaces the 404 instead of reporting success.
      resolveJson({ message: 'not found' }, 404);
      await expect(client.delete({ key: 'missing' })).rejects.toThrow('QUIC/HTTP3 error (404)');
    });
  });

  // --------------------------------------------------------------------------
  // list
  // --------------------------------------------------------------------------
  describe('list', () => {
    it('quic_list_success', async () => {
      resolveJson({
        objects: [{ key: 'a', content_type: 'text/plain', size: 3 }],
        common_prefixes: ['p/'],
        next_token: 'tok',
        truncated: true,
      });

      const response = await client.list({ prefix: 'a', maxResults: 5, continueFrom: 'c' });

      expect(response.objects).toHaveLength(1);
      expect(response.objects[0].key).toBe('a');
      expect(response.commonPrefixes).toEqual(['p/']);
      expect(response.truncated).toBe(true);
    });

    it('quic_list_error', async () => {
      resolveError();
      await expect(client.list()).rejects.toThrow('QUIC/HTTP3 error (500)');
    });
  });

  // --------------------------------------------------------------------------
  // exists
  // --------------------------------------------------------------------------
  describe('exists', () => {
    it('quic_exists_success', async () => {
      mockFetch.mockResolvedValue(mockResponse({ status: 200 }));
      const response = await client.exists({ key: 'test-key' });
      expect(response.exists).toBe(true);
    });

    it('quic_exists_error', async () => {
      mockFetch.mockResolvedValue(mockResponse({ status: 500, text: 'boom' }));
      await expect(client.exists({ key: 'test-key' })).rejects.toThrow('QUIC/HTTP3 error (500)');
    });

    it('quic_exists_not_found', async () => {
      mockFetch.mockResolvedValue(mockResponse({ status: 404 }));
      const response = await client.exists({ key: 'missing' });
      expect(response.exists).toBe(false);
    });
  });

  // --------------------------------------------------------------------------
  // getMetadata
  // --------------------------------------------------------------------------
  describe('getMetadata', () => {
    it('quic_get_metadata_success', async () => {
      mockFetch.mockResolvedValue(
        mockResponse({
          status: 200,
          headers: { 'content-type': 'text/plain', 'x-meta-author': 'jane' },
        })
      );

      const response = await client.getMetadata({ key: 'test-key' });

      expect(response.success).toBe(true);
      expect(response.metadata?.contentType).toBe('text/plain');
      expect(response.metadata?.custom).toEqual({ author: 'jane' });
    });

    it('quic_get_metadata_error', async () => {
      mockFetch.mockResolvedValue(mockResponse({ status: 500, text: 'boom' }));
      await expect(client.getMetadata({ key: 'test-key' })).rejects.toThrow('QUIC/HTTP3 error (500)');
    });

    it('quic_get_metadata_not_found', async () => {
      mockFetch.mockResolvedValue(mockResponse({ status: 404, text: 'not found' }));
      await expect(client.getMetadata({ key: 'missing' })).rejects.toThrow('QUIC/HTTP3 error (404)');
    });
  });

  // --------------------------------------------------------------------------
  // updateMetadata
  // --------------------------------------------------------------------------
  describe('updateMetadata', () => {
    it('quic_update_metadata_success', async () => {
      resolveJson({ message: 'updated' });
      const response = await client.updateMetadata({
        key: 'test-key',
        metadata: { contentType: 'text/plain' },
      });
      expect(response.success).toBe(true);
      expect(response.message).toBe('updated');
    });

    it('quic_update_metadata_error', async () => {
      resolveError();
      await expect(
        client.updateMetadata({ key: 'test-key', metadata: { contentType: 'text/plain' } })
      ).rejects.toThrow('QUIC/HTTP3 error (500)');
    });

    it('quic_update_metadata_not_found', async () => {
      resolveJson({ message: 'not found' }, 404);
      await expect(
        client.updateMetadata({ key: 'missing', metadata: { contentType: 'text/plain' } })
      ).rejects.toThrow('QUIC/HTTP3 error (404)');
    });
  });

  // --------------------------------------------------------------------------
  // health
  // --------------------------------------------------------------------------
  describe('health', () => {
    it('quic_health_success', async () => {
      resolveJson({ status: 'healthy', message: 'OK' });
      const response = await client.health();
      expect(response.status).toBe(HealthStatus.SERVING);
      expect(response.message).toBe('OK');
    });

    it('quic_health_error', async () => {
      resolveError();
      await expect(client.health()).rejects.toThrow('QUIC/HTTP3 error (500)');
    });
  });

  // --------------------------------------------------------------------------
  // archive
  // --------------------------------------------------------------------------
  describe('archive', () => {
    it('quic_archive_success', async () => {
      resolveJson({ success: true, message: 'archived' });
      const response = await client.archive({ key: 'test-key', destinationType: 'glacier' });
      expect(response.success).toBe(true);
      expect(response.message).toBe('archived');
    });

    it('quic_archive_error', async () => {
      resolveError();
      await expect(
        client.archive({ key: 'test-key', destinationType: 'glacier' })
      ).rejects.toThrow('QUIC/HTTP3 error (500)');
    });
  });

  // --------------------------------------------------------------------------
  // addPolicy
  // --------------------------------------------------------------------------
  describe('addPolicy', () => {
    it('quic_add_policy_success', async () => {
      resolveJson({ success: true, message: 'added' });
      const response = await client.addPolicy({
        policy: { id: 'p1', prefix: 'logs/', retentionSeconds: 86400, action: 'delete' },
      });
      expect(response.success).toBe(true);
      expect(response.message).toBe('added');
    });

    it('quic_add_policy_error', async () => {
      resolveError();
      await expect(
        client.addPolicy({
          policy: { id: 'p1', prefix: 'logs/', retentionSeconds: 1, action: 'delete' },
        })
      ).rejects.toThrow('QUIC/HTTP3 error (500)');
    });
  });

  // --------------------------------------------------------------------------
  // removePolicy
  // --------------------------------------------------------------------------
  describe('removePolicy', () => {
    it('quic_remove_policy_success', async () => {
      resolveJson({ success: true, message: 'removed' });
      const response = await client.removePolicy({ id: 'p1' });
      expect(response.success).toBe(true);
      expect(response.message).toBe('removed');
    });

    it('quic_remove_policy_error', async () => {
      resolveError();
      await expect(client.removePolicy({ id: 'p1' })).rejects.toThrow('QUIC/HTTP3 error (500)');
    });

    it('quic_remove_policy_not_found', async () => {
      // Removing a missing policy surfaces the 404 instead of reporting success.
      resolveJson({ success: true, message: 'not found' }, 404);
      await expect(client.removePolicy({ id: 'missing' })).rejects.toThrow('QUIC/HTTP3 error (404)');
    });
  });

  // --------------------------------------------------------------------------
  // getPolicies
  // --------------------------------------------------------------------------
  describe('getPolicies', () => {
    it('quic_get_policies_success', async () => {
      resolveJson({
        policies: [
          { id: 'p1', prefix: 'logs/', retention_seconds: 86400, action: 'delete' },
        ],
        success: true,
      });

      const response = await client.getPolicies({ prefix: 'logs/' });

      expect(response.policies).toHaveLength(1);
      expect(response.policies[0].id).toBe('p1');
      expect(response.policies[0].retentionSeconds).toBe(86400);
    });

    it('quic_get_policies_error', async () => {
      resolveError();
      await expect(client.getPolicies()).rejects.toThrow('QUIC/HTTP3 error (500)');
    });
  });

  // --------------------------------------------------------------------------
  // applyPolicies
  // --------------------------------------------------------------------------
  describe('applyPolicies', () => {
    it('quic_apply_policies_success', async () => {
      resolveJson({ success: true, policies_count: 2, objects_processed: 7, message: 'applied' });

      const response = await client.applyPolicies();

      expect(response.success).toBe(true);
      expect(response.policiesCount).toBe(2);
      expect(response.objectsProcessed).toBe(7);
    });

    it('quic_apply_policies_error', async () => {
      resolveError();
      await expect(client.applyPolicies()).rejects.toThrow('QUIC/HTTP3 error (500)');
    });
  });

  // --------------------------------------------------------------------------
  // addReplicationPolicy
  // --------------------------------------------------------------------------
  describe('addReplicationPolicy', () => {
    const repPolicy = {
      id: 'r1',
      sourceBackend: 'local',
      sourceSettings: {},
      sourcePrefix: '',
      destinationBackend: 's3',
      destinationSettings: {},
      checkIntervalSeconds: 60,
      enabled: true,
      replicationMode: ReplicationMode.TRANSPARENT,
    };

    it('quic_add_replication_policy_success', async () => {
      resolveJson({ success: true, message: 'added' });
      const response = await client.addReplicationPolicy({ policy: repPolicy });
      expect(response.success).toBe(true);
      expect(response.message).toBe('added');
    });

    it('quic_add_replication_policy_error', async () => {
      resolveError();
      await expect(
        client.addReplicationPolicy({ policy: repPolicy })
      ).rejects.toThrow('QUIC/HTTP3 error (500)');
    });
  });

  // --------------------------------------------------------------------------
  // removeReplicationPolicy
  // --------------------------------------------------------------------------
  describe('removeReplicationPolicy', () => {
    it('quic_remove_replication_policy_success', async () => {
      resolveJson({ success: true, message: 'removed' });
      const response = await client.removeReplicationPolicy({ id: 'r1' });
      expect(response.success).toBe(true);
      expect(response.message).toBe('removed');
    });

    it('quic_remove_replication_policy_error', async () => {
      resolveError();
      await expect(
        client.removeReplicationPolicy({ id: 'r1' })
      ).rejects.toThrow('QUIC/HTTP3 error (500)');
    });

    it('quic_remove_replication_policy_not_found', async () => {
      resolveJson({ success: true, message: 'not found' }, 404);
      await expect(client.removeReplicationPolicy({ id: 'missing' })).rejects.toThrow(
        'QUIC/HTTP3 error (404)'
      );
    });
  });

  // --------------------------------------------------------------------------
  // getReplicationPolicies
  // --------------------------------------------------------------------------
  describe('getReplicationPolicies', () => {
    it('quic_get_replication_policies_success', async () => {
      resolveJson({
        policies: [
          {
            id: 'r1',
            source_backend: 'local',
            destination_backend: 's3',
            check_interval: 60,
            enabled: true,
            replication_mode: 'transparent',
          },
        ],
      });

      const response = await client.getReplicationPolicies();

      expect(response.policies).toHaveLength(1);
      expect(response.policies[0].id).toBe('r1');
      expect(response.policies[0].checkIntervalSeconds).toBe(60);
      expect(response.policies[0].replicationMode).toBe(ReplicationMode.TRANSPARENT);
    });

    it('quic_get_replication_policies_error', async () => {
      resolveError();
      await expect(client.getReplicationPolicies()).rejects.toThrow('QUIC/HTTP3 error (500)');
    });
  });

  // --------------------------------------------------------------------------
  // getReplicationPolicy
  // --------------------------------------------------------------------------
  describe('getReplicationPolicy', () => {
    it('quic_get_replication_policy_success', async () => {
      resolveJson({
        id: 'r1',
        source_backend: 'local',
        destination_backend: 's3',
        check_interval: 60,
        enabled: true,
        replication_mode: 'opaque',
      });

      const response = await client.getReplicationPolicy({ id: 'r1' });

      expect(response.policy?.id).toBe('r1');
      expect(response.policy?.replicationMode).toBe(ReplicationMode.OPAQUE);
    });

    it('quic_get_replication_policy_error', async () => {
      resolveError();
      await expect(
        client.getReplicationPolicy({ id: 'r1' })
      ).rejects.toThrow('QUIC/HTTP3 error (500)');
    });

    it('quic_get_replication_policy_not_found', async () => {
      resolveJson({ id: '' }, 404);
      await expect(client.getReplicationPolicy({ id: 'missing' })).rejects.toThrow(
        'QUIC/HTTP3 error (404)'
      );
    });
  });

  // --------------------------------------------------------------------------
  // triggerReplication
  // --------------------------------------------------------------------------
  describe('triggerReplication', () => {
    it('quic_trigger_replication_success', async () => {
      resolveJson({
        success: true,
        result: {
          policy_id: 'r1',
          synced: 5,
          deleted: 1,
          failed: 0,
          bytes_total: 1024,
          duration_ms: 200,
          errors: [],
        },
      });

      const response = await client.triggerReplication({ policyId: 'r1' });

      expect(response.success).toBe(true);
      expect(response.result?.synced).toBe(5);
      expect(response.result?.bytesTotal).toBe(1024);
    });

    it('quic_trigger_replication_error', async () => {
      resolveError();
      await expect(
        client.triggerReplication({ policyId: 'r1' })
      ).rejects.toThrow('QUIC/HTTP3 error (500)');
    });
  });

  // --------------------------------------------------------------------------
  // getReplicationStatus
  // --------------------------------------------------------------------------
  describe('getReplicationStatus', () => {
    it('quic_get_replication_status_success', async () => {
      resolveJson({
        success: true,
        status: {
          policy_id: 'r1',
          source_backend: 'local',
          destination_backend: 's3',
          enabled: true,
          total_objects_synced: 10,
          total_objects_deleted: 2,
          total_bytes_synced: 2048,
          total_errors: 0,
          average_sync_duration_ms: 150,
          sync_count: 3,
        },
      });

      const response = await client.getReplicationStatus({ id: 'r1' });

      expect(response.success).toBe(true);
      expect(response.status?.totalObjectsSynced).toBe(10);
      expect(response.status?.syncCount).toBe(3);
    });

    it('quic_get_replication_status_error', async () => {
      resolveError();
      await expect(
        client.getReplicationStatus({ id: 'r1' })
      ).rejects.toThrow('QUIC/HTTP3 error (500)');
    });

    it('quic_get_replication_status_not_found', async () => {
      resolveJson({ success: true }, 404);
      await expect(client.getReplicationStatus({ id: 'missing' })).rejects.toThrow(
        'QUIC/HTTP3 error (404)'
      );
    });
  });

  // --------------------------------------------------------------------------
  // metadata_round_trip
  // --------------------------------------------------------------------------
  describe('metadata round trip', () => {
    it('quic_metadata_round_trip', async () => {
      const custom = { author: 'jane', tier: 'gold' };

      // put: assert request sets Content-Type, Content-Encoding and one
      // X-Meta-<key> header per custom entry.
      mockFetch.mockResolvedValueOnce(
        mockResponse({ headers: { ...jsonHeaders, etag: '"e"' }, json: { message: 'stored' } })
      );
      await client.put({
        key: 'doc',
        data: Buffer.from('hello'),
        metadata: { contentType: 'text/plain', contentEncoding: 'gzip', custom },
      });

      const putInit = mockFetch.mock.calls[0][1];
      expect(putInit.headers['Content-Type']).toBe('text/plain');
      expect(putInit.headers['Content-Encoding']).toBe('gzip');
      expect(putInit.headers['X-Meta-author']).toBe('jane');
      expect(putInit.headers['X-Meta-tier']).toBe('gold');

      // get: metadata returned via response headers.
      mockFetch.mockResolvedValueOnce(
        mockResponse({
          headers: {
            'content-type': 'text/plain',
            'content-encoding': 'gzip',
            'content-length': '5',
            'x-meta-author': 'jane',
            'x-meta-tier': 'gold',
          },
          arrayBuffer: Buffer.from('hello'),
        })
      );
      const getResp = await client.get({ key: 'doc' });
      expect(getResp.metadata?.contentType).toBe('text/plain');
      expect(getResp.metadata?.contentEncoding).toBe('gzip');
      expect(getResp.metadata?.custom).toEqual(custom);

      // getMetadata: read via HEAD response headers (incl. X-Meta-*).
      mockFetch.mockResolvedValueOnce(
        mockResponse({
          status: 200,
          headers: {
            'content-type': 'text/plain',
            'content-encoding': 'gzip',
            'x-meta-author': 'jane',
            'x-meta-tier': 'gold',
          },
        })
      );
      const metaResp = await client.getMetadata({ key: 'doc' });
      expect(metaResp.metadata?.contentType).toBe('text/plain');
      expect(metaResp.metadata?.contentEncoding).toBe('gzip');
      expect(metaResp.metadata?.custom).toEqual(custom);
    });
  });

  // --------------------------------------------------------------------------
  // validation_empty_key
  // --------------------------------------------------------------------------
  describe('validation', () => {
    it('quic_validation_empty_key', async () => {
      // The QUIC client has no client-side validation; an empty key hits
      // /objects/ on the wire. Simulate the server rejecting it and assert the
      // call raises (no successful network call succeeds).
      mockFetch.mockResolvedValue(mockResponse({ status: 500, text: 'bad key' }));
      await expect(
        client.get({ key: '' })
      ).rejects.toThrow('QUIC/HTTP3 error');
    });
  });

  // --------------------------------------------------------------------------
  // close
  // --------------------------------------------------------------------------
  describe('close', () => {
    it('quic_close', async () => {
      await expect(client.close()).resolves.toBeUndefined();
    });
  });

  describe('secure', () => {
    it('quic_secure_uses_https', async () => {
      const secureClient = new QuicClient({ address, secure: true });
      mockFetch.mockResolvedValue(mockResponse({ headers: jsonHeaders, json: { status: 'healthy' } }));
      await secureClient.health();
      expect(mockFetch.mock.calls[0][0]).toMatch(/^https:\/\//);
    });
  });

  // --------------------------------------------------------------------------
  // auth: token + tenantId forwarded in requests
  // --------------------------------------------------------------------------
  describe('auth', () => {
    it('quic_auth_token_and_tenant', async () => {
      const authClient = new QuicClient({
        address,
        token: 'tok-123',
        tenantId: 'tenant-99',
      });
      mockFetch.mockResolvedValue(
        mockResponse({ headers: jsonHeaders, json: { status: 'healthy' } })
      );
      await authClient.health();
      const fetchOpts = mockFetch.mock.calls[0][1] as RequestInit;
      const headers = fetchOpts.headers as Record<string, string>;
      expect(headers['Authorization']).toBe('Bearer tok-123');
      expect(headers['X-Tenant-ID']).toBe('tenant-99');
    });
  });

  // --------------------------------------------------------------------------
  // getStream
  // --------------------------------------------------------------------------
  describe('getStream', () => {
    it('quic_getStream_success', async () => {
      // Build a minimal WHATWG ReadableStream from a buffer.
      const data = Buffer.from('streamed');
      let called = false;
      const body = {
        getReader: () => ({
          read: async () => {
            if (!called) {
              called = true;
              return { done: false, value: new Uint8Array(data) };
            }
            return { done: true, value: undefined };
          },
          releaseLock: () => undefined,
        }),
      };
      mockFetch.mockResolvedValue({ ok: true, status: 200, body });

      const stream = await client.getStream('stream-key');
      const chunks: Buffer[] = [];
      for await (const chunk of stream) {
        chunks.push(chunk as Buffer);
      }
      expect(Buffer.concat(chunks).toString()).toBe('streamed');
    });

    it('quic_getStream_no_body', async () => {
      mockFetch.mockResolvedValue({ ok: true, status: 200, body: null });
      const stream = await client.getStream('empty-key');
      const chunks: Buffer[] = [];
      for await (const chunk of stream) {
        chunks.push(chunk as Buffer);
      }
      expect(chunks).toHaveLength(0);
    });

    it('quic_getStream_error', async () => {
      mockFetch.mockResolvedValue(
        mockResponse({ ok: false, status: 404, text: 'not found' })
      );
      await expect(client.getStream('missing')).rejects.toThrow('QUIC/HTTP3 error (404)');
    });
  });

  // --------------------------------------------------------------------------
  // putStream
  // --------------------------------------------------------------------------
  describe('putStream', () => {
    it('quic_putStream_success', async () => {
      mockFetch.mockResolvedValue(
        mockResponse({ headers: jsonHeaders, json: { message: 'stored' } })
      );
      const { Readable: NodeReadable } = require('stream');
      const stream = NodeReadable.from([Buffer.from('hello'), Buffer.from(' world')]);
      const resp = await client.putStream('stream-put', stream);
      expect(resp.success).toBe(true);
    });
  });
});
