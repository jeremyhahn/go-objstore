import { QuicClient } from '../../src/clients/quic-client';
import { HealthStatus } from '../../src/types';

// Mock fetch
global.fetch = jest.fn();

describe('QuicClient', () => {
  let client: QuicClient;

  beforeEach(() => {
    client = new QuicClient({ address: 'localhost:8443', secure: true });
    jest.clearAllMocks();
  });

  const mockFetch = (response: any, status = 200) => {
    (global.fetch as jest.Mock).mockResolvedValueOnce({
      ok: status >= 200 && status < 300,
      status,
      headers: new Map([['content-type', 'application/json']]),
      json: async () => response,
      text: async () => JSON.stringify(response),
    });
  };

  // Mock for get() which uses arrayBuffer() and headers directly
  const mockFetchBinary = (data: Buffer, headers: Record<string, string> = {}, status = 200) => {
    const headerMap = new Map(Object.entries(headers));
    (global.fetch as jest.Mock).mockResolvedValueOnce({
      ok: status >= 200 && status < 300,
      status,
      headers: {
        get: (name: string) => headerMap.get(name.toLowerCase()) || null,
      },
      arrayBuffer: async () => data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength),
      text: async () => data.toString(),
    });
  };

  describe('put', () => {
    it('should upload an object successfully', async () => {
      mockFetch({ message: 'success', etag: 'test-etag' });

      const result = await client.put({
        key: 'test-key',
        data: Buffer.from('test data'),
      });

      expect(result.success).toBe(true);
    });
  });

  describe('get', () => {
    it('should retrieve an object successfully', async () => {
      mockFetchBinary(Buffer.from('test data'), {
        'content-type': 'text/plain',
        'content-length': '9',
      });

      const result = await client.get({ key: 'test-key' });

      expect(result.data.toString()).toBe('test data');
      expect(result.metadata?.contentType).toBe('text/plain');
    });
  });

  describe('delete', () => {
    it('should delete an object successfully', async () => {
      mockFetch({ message: 'deleted' });

      const result = await client.delete({ key: 'test-key' });

      expect(result.success).toBe(true);
    });
  });

  describe('list', () => {
    it('should list objects', async () => {
      mockFetch({
        objects: [
          { key: 'file1.txt', metadata: { size: 100 } },
          { key: 'file2.txt', metadata: { size: 200 } },
        ],
        truncated: false,
      });

      const result = await client.list({ prefix: 'test/' });

      expect(result.objects).toHaveLength(2);
    });
  });

  describe('exists', () => {
    it('should return true when object exists', async () => {
      (global.fetch as jest.Mock).mockResolvedValueOnce({
        ok: true,
        status: 200,
      });

      const result = await client.exists({ key: 'test-key' });

      expect(result.exists).toBe(true);
    });

    it('should return false when object does not exist', async () => {
      (global.fetch as jest.Mock).mockResolvedValueOnce({
        ok: false,
        status: 404,
      });

      const result = await client.exists({ key: 'missing-key' });

      expect(result.exists).toBe(false);
    });
  });

  describe('getMetadata', () => {
    it('should retrieve object metadata', async () => {
      mockFetch({
        content_type: 'text/plain',
        size: 100,
        etag: 'test-etag',
      });

      const result = await client.getMetadata({ key: 'test-key' });

      expect(result.success).toBe(true);
      expect(result.metadata?.contentType).toBe('text/plain');
    });
  });

  describe('updateMetadata', () => {
    it('should update object metadata', async () => {
      mockFetch({ message: 'updated' });

      const result = await client.updateMetadata({
        key: 'test-key',
        metadata: { contentType: 'application/json' },
      });

      expect(result.success).toBe(true);
    });
  });

  describe('health', () => {
    it('should check health successfully', async () => {
      mockFetch({ status: 'healthy' });

      const result = await client.health();

      expect(result.status).toBe(HealthStatus.SERVING);
    });
  });

  describe('archive', () => {
    it('should archive an object', async () => {
      mockFetch({ success: true });

      const result = await client.archive({
        key: 'test-key',
        destinationType: 'glacier',
      });

      expect(result.success).toBe(true);
    });
  });

  describe('lifecycle policies', () => {
    it('should add a policy', async () => {
      mockFetch({ success: true });

      const result = await client.addPolicy({
        policy: {
          id: 'p1',
          prefix: 'logs/',
          retentionSeconds: 86400,
          action: 'delete',
        },
      });

      expect(result.success).toBe(true);
    });

    it('should remove a policy', async () => {
      mockFetch({ success: true });

      const result = await client.removePolicy({ id: 'p1' });

      expect(result.success).toBe(true);
    });

    it('should get policies', async () => {
      mockFetch({
        policies: [{ id: 'p1', prefix: 'logs/', retention_seconds: 86400, action: 'delete' }],
        success: true,
      });

      const result = await client.getPolicies();

      expect(result.policies).toHaveLength(1);
    });

    it('should apply policies', async () => {
      mockFetch({ success: true, policies_count: 5, objects_processed: 100 });

      const result = await client.applyPolicies();

      expect(result.success).toBe(true);
      expect(result.policiesCount).toBe(5);
    });
  });

  describe('replication policies', () => {
    it('should add a replication policy', async () => {
      mockFetch({ success: true });

      const result = await client.addReplicationPolicy({
        policy: {
          id: 'r1',
          sourceBackend: 's3',
          sourceSettings: {},
          sourcePrefix: '',
          destinationBackend: 'gcs',
          destinationSettings: {},
          checkIntervalSeconds: 3600,
          enabled: true,
          replicationMode: 0,
        },
      });

      expect(result.success).toBe(true);
    });

    it('should remove a replication policy', async () => {
      mockFetch({ success: true });

      const result = await client.removeReplicationPolicy({ id: 'r1' });

      expect(result.success).toBe(true);
    });

    it('should get replication policies', async () => {
      mockFetch({
        policies: [
          {
            id: 'r1',
            source_backend: 's3',
            source_settings: {},
            source_prefix: '',
            destination_backend: 'gcs',
            destination_settings: {},
            check_interval_seconds: 3600,
            enabled: true,
            replication_mode: 0,
          },
        ],
      });

      const result = await client.getReplicationPolicies();

      expect(result.policies).toHaveLength(1);
    });

    it('should get a specific replication policy', async () => {
      mockFetch({
        id: 'r1',
        source_backend: 's3',
        source_settings: {},
        source_prefix: '',
        destination_backend: 'gcs',
        destination_settings: {},
        check_interval_seconds: 3600,
        enabled: true,
        replication_mode: 0,
      });

      const result = await client.getReplicationPolicy({ id: 'r1' });

      expect(result.policy?.id).toBe('r1');
    });

    it('should trigger replication', async () => {
      mockFetch({
        success: true,
        result: {
          policy_id: 'r1',
          synced: 10,
          deleted: 2,
          failed: 0,
          bytes_total: 1024,
          duration_ms: 500,
          errors: [],
        },
      });

      const result = await client.triggerReplication({ policyId: 'r1' });

      expect(result.success).toBe(true);
      expect(result.result?.synced).toBe(10);
    });

    it('should get replication status', async () => {
      mockFetch({
        success: true,
        status: {
          policy_id: 'r1',
          source_backend: 's3',
          destination_backend: 'gcs',
          enabled: true,
          total_objects_synced: 100,
          total_objects_deleted: 10,
          total_bytes_synced: 1048576,
          total_errors: 0,
          average_sync_duration_ms: 500,
          sync_count: 5,
        },
      });

      const result = await client.getReplicationStatus({ id: 'r1' });

      expect(result.success).toBe(true);
      expect(result.status?.totalObjectsSynced).toBe(100);
    });
  });

  describe('close', () => {
    it('should close without errors', async () => {
      await expect(client.close()).resolves.toBeUndefined();
    });
  });

  describe('error handling', () => {
    it('should handle fetch errors in put', async () => {
      (global.fetch as jest.Mock).mockRejectedValueOnce(new Error('Network error'));

      await expect(
        client.put({ key: 'test', data: Buffer.from('data') })
      ).rejects.toThrow('Network error');
    });

    it('should handle fetch errors in get', async () => {
      (global.fetch as jest.Mock).mockRejectedValueOnce(new Error('Network error'));

      await expect(client.get({ key: 'test' })).rejects.toThrow('Network error');
    });

    it('should handle fetch errors in delete', async () => {
      (global.fetch as jest.Mock).mockRejectedValueOnce(new Error('Network error'));

      await expect(client.delete({ key: 'test' })).rejects.toThrow('Network error');
    });

    it('should handle fetch errors in list', async () => {
      (global.fetch as jest.Mock).mockRejectedValueOnce(new Error('Network error'));

      await expect(client.list()).rejects.toThrow('Network error');
    });

    it('should handle fetch errors in exists by returning false', async () => {
      (global.fetch as jest.Mock).mockRejectedValueOnce(new Error('Network error'));

      // exists() catches errors and returns { exists: false } by design
      const result = await client.exists({ key: 'test' });
      expect(result.exists).toBe(false);
    });

    it('should handle non-ok responses', async () => {
      (global.fetch as jest.Mock).mockResolvedValueOnce({
        ok: false,
        status: 500,
        json: async () => ({ error: 'Server error' }),
      });

      await expect(client.put({ key: 'test', data: Buffer.from('data') })).rejects.toThrow();
    });

    it('should handle error responses in getMetadata', async () => {
      (global.fetch as jest.Mock).mockResolvedValueOnce({
        ok: false,
        status: 404,
        json: async () => ({ error: 'Not found' }),
      });

      await expect(client.getMetadata({ key: 'test' })).rejects.toThrow();
    });
  });

  describe('additional edge cases', () => {
    it('should handle insecure connection', () => {
      const insecureClient = new QuicClient({ address: 'localhost:8080', secure: false });
      expect(insecureClient).toBeInstanceOf(QuicClient);
    });

    it('should handle get response with metadata', async () => {
      mockFetchBinary(Buffer.from('test data'), {
        'content-type': 'text/plain',
        'content-encoding': 'gzip',
        'content-length': '100',
        'etag': 'abc123',
        'last-modified': '2023-01-01T00:00:00Z',
      });

      const result = await client.get({ key: 'test' });

      expect(result.data.toString()).toBe('test data');
      expect(result.metadata?.contentType).toBe('text/plain');
      expect(result.metadata?.contentEncoding).toBe('gzip');
    });

    it('should handle list with common prefixes', async () => {
      mockFetch({
        objects: [{ key: 'file1.txt', metadata: {} }],
        common_prefixes: ['folder1/', 'folder2/'],
        next_token: 'token123',
        truncated: true,
      });

      const result = await client.list({ delimiter: '/' });

      expect(result.commonPrefixes).toContain('folder1/');
      expect(result.nextToken).toBe('token123');
      expect(result.truncated).toBe(true);
    });

    it('should handle archive with all settings', async () => {
      mockFetch({ success: true, message: 'Archived successfully' });

      const result = await client.archive({
        key: 'test',
        destinationType: 'glacier',
        destinationSettings: { tier: 'deep-archive' },
      });

      expect(result.success).toBe(true);
      expect(result.message).toBe('Archived successfully');
    });

    it('should handle policy with destination settings', async () => {
      mockFetch({ success: true });

      const result = await client.addPolicy({
        policy: {
          id: 'p1',
          prefix: 'logs/',
          retentionSeconds: 86400,
          action: 'archive',
          destinationType: 'glacier',
          destinationSettings: { tier: 'standard' },
        },
      });

      expect(result.success).toBe(true);
    });

    it('should handle replication with encryption', async () => {
      mockFetch({ success: true });

      const result = await client.addReplicationPolicy({
        policy: {
          id: 'r1',
          sourceBackend: 's3',
          sourceSettings: {},
          sourcePrefix: '',
          destinationBackend: 'gcs',
          destinationSettings: {},
          checkIntervalSeconds: 3600,
          enabled: true,
          replicationMode: 0,
          encryption: {
            source: {
              enabled: true,
              provider: 'kms',
              defaultKey: 'key123',
            },
          },
        },
      });

      expect(result.success).toBe(true);
    });

    it('should handle trigger replication with all options', async () => {
      mockFetch({
        success: true,
        result: {
          policy_id: 'r1',
          synced: 10,
          deleted: 2,
          failed: 1,
          bytes_total: 1024,
          duration_ms: 500,
          errors: ['Error 1'],
        },
      });

      const result = await client.triggerReplication({
        policyId: 'r1',
        parallel: true,
        workerCount: 4,
      });

      expect(result.result?.errors).toHaveLength(1);
    });

    it('should handle replication status with last sync time', async () => {
      mockFetch({
        success: true,
        status: {
          policy_id: 'r1',
          source_backend: 's3',
          destination_backend: 'gcs',
          enabled: true,
          total_objects_synced: 100,
          total_objects_deleted: 10,
          total_bytes_synced: 1048576,
          total_errors: 0,
          last_sync_time: '2023-01-01T00:00:00Z',
          average_sync_duration_ms: 500,
          sync_count: 5,
        },
      });

      const result = await client.getReplicationStatus({ id: 'r1' });

      expect(result.status?.lastSyncTime).toBeInstanceOf(Date);
    });
  });
});
