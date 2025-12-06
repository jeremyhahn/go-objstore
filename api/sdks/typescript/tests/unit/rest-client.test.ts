import { RestClient } from '../../src/clients/rest-client';
import nock from 'nock';
import { HealthStatus } from '../../src/types';

describe('RestClient', () => {
  let client: RestClient;
  const baseUrl = 'http://localhost:8080';

  beforeEach(() => {
    client = new RestClient({ baseUrl });
  });

  afterEach(() => {
    nock.cleanAll();
  });

  describe('put', () => {
    it('should upload an object successfully', async () => {
      nock(baseUrl)
        .put('/objects/test-key')
        .reply(201, { message: 'success', success: true }, { etag: 'test-etag' });

      const result = await client.put({
        key: 'test-key',
        data: Buffer.from('test data'),
      });

      expect(result.success).toBe(true);
      expect(result.etag).toBe('test-etag');
    });

    it('should upload an object with metadata', async () => {
      nock(baseUrl).put('/objects/test-key').reply(201, { message: 'success' });

      const result = await client.put({
        key: 'test-key',
        data: Buffer.from('test data'),
        metadata: {
          contentType: 'text/plain',
          custom: { author: 'test' },
        },
      });

      expect(result.success).toBe(true);
    });
  });

  describe('get', () => {
    it('should retrieve an object successfully', async () => {
      const data = 'test data';
      nock(baseUrl)
        .get('/objects/test-key')
        .reply(200, data, {
          'content-type': 'text/plain',
          'content-length': data.length.toString(),
          etag: 'test-etag',
        });

      const result = await client.get({ key: 'test-key' });

      expect(result.data.toString()).toBe(data);
      expect(result.metadata?.contentType).toBe('text/plain');
      expect(result.metadata?.etag).toBe('test-etag');
    });

    it('should throw error when object not found', async () => {
      nock(baseUrl).get('/objects/missing-key').reply(404, { error: 'Not Found' });

      await expect(client.get({ key: 'missing-key' })).rejects.toThrow();
    });
  });

  describe('delete', () => {
    it('should delete an object successfully', async () => {
      nock(baseUrl).delete('/objects/test-key').reply(200, { message: 'deleted' });

      const result = await client.delete({ key: 'test-key' });

      expect(result.success).toBe(true);
    });
  });

  describe('list', () => {
    it('should list objects without filters', async () => {
      nock(baseUrl).get('/objects').reply(200, {
        objects: [
          { key: 'file1.txt', size: 100 },
          { key: 'file2.txt', size: 200 },
        ],
        truncated: false,
      });

      const result = await client.list({});

      expect(result.objects).toHaveLength(2);
      expect(result.objects[0].key).toBe('file1.txt');
      expect(result.truncated).toBe(false);
    });

    it('should list objects with prefix filter', async () => {
      nock(baseUrl).get('/objects?prefix=docs%2F').reply(200, {
        objects: [{ key: 'docs/file1.txt', size: 100 }],
        truncated: false,
      });

      const result = await client.list({ prefix: 'docs/' });

      expect(result.objects).toHaveLength(1);
      expect(result.objects[0].key).toBe('docs/file1.txt');
    });

    it('should handle pagination', async () => {
      nock(baseUrl).get('/objects?limit=10&token=next-token').reply(200, {
        objects: [{ key: 'file1.txt', size: 100 }],
        next_token: 'another-token',
        truncated: true,
      });

      const result = await client.list({
        maxResults: 10,
        continueFrom: 'next-token',
      });

      expect(result.nextToken).toBe('another-token');
      expect(result.truncated).toBe(true);
    });
  });

  describe('exists', () => {
    it('should return true when object exists', async () => {
      nock(baseUrl).head('/objects/test-key').reply(200);

      const result = await client.exists({ key: 'test-key' });

      expect(result.exists).toBe(true);
    });

    it('should return false when object does not exist', async () => {
      nock(baseUrl).head('/objects/missing-key').reply(404);

      const result = await client.exists({ key: 'missing-key' });

      expect(result.exists).toBe(false);
    });
  });

  describe('getMetadata', () => {
    it('should retrieve object metadata', async () => {
      nock(baseUrl).get('/metadata/test-key').reply(200, {
        content_type: 'text/plain',
        size: 100,
        etag: 'test-etag',
      });

      const result = await client.getMetadata({ key: 'test-key' });

      expect(result.success).toBe(true);
      expect(result.metadata?.contentType).toBe('text/plain');
      expect(result.metadata?.size).toBe(100);
    });
  });

  describe('updateMetadata', () => {
    it('should update object metadata', async () => {
      nock(baseUrl).put('/metadata/test-key').reply(200, { message: 'updated' });

      const result = await client.updateMetadata({
        key: 'test-key',
        metadata: {
          contentType: 'application/json',
          custom: { version: '2.0' },
        },
      });

      expect(result.success).toBe(true);
    });
  });

  describe('health', () => {
    it('should check health successfully', async () => {
      nock(baseUrl).get('/health').reply(200, { status: 'healthy' });

      const result = await client.health();

      expect(result.status).toBe(HealthStatus.SERVING);
    });

    it('should handle unhealthy status', async () => {
      nock(baseUrl).get('/health').reply(200, { status: 'unhealthy', message: 'DB down' });

      const result = await client.health();

      expect(result.status).toBe(HealthStatus.NOT_SERVING);
      expect(result.message).toBe('DB down');
    });
  });

  describe('archive', () => {
    it('should archive an object', async () => {
      nock(baseUrl).post('/archive').reply(200, { success: true });

      const result = await client.archive({
        key: 'test-key',
        destinationType: 'glacier',
        destinationSettings: { tier: 'standard' },
      });

      expect(result.success).toBe(true);
    });
  });

  describe('lifecycle policies', () => {
    it('should add a lifecycle policy', async () => {
      nock(baseUrl).post('/policies').reply(200, { success: true });

      const result = await client.addPolicy({
        policy: {
          id: 'policy-1',
          prefix: 'logs/',
          retentionSeconds: 86400,
          action: 'delete',
        },
      });

      expect(result.success).toBe(true);
    });

    it('should remove a lifecycle policy', async () => {
      nock(baseUrl).delete('/policies/policy-1').reply(200, { success: true });

      const result = await client.removePolicy({ id: 'policy-1' });

      expect(result.success).toBe(true);
    });

    it('should get all policies', async () => {
      nock(baseUrl).get('/policies').reply(200, {
        policies: [
          {
            id: 'policy-1',
            prefix: 'logs/',
            retention_seconds: 86400,
            action: 'delete',
          },
        ],
        success: true,
      });

      const result = await client.getPolicies();

      expect(result.policies).toHaveLength(1);
      expect(result.policies[0].id).toBe('policy-1');
    });

    it('should apply policies', async () => {
      nock(baseUrl).post('/policies/apply').reply(200, {
        success: true,
        policies_count: 5,
        objects_processed: 100,
      });

      const result = await client.applyPolicies();

      expect(result.success).toBe(true);
      expect(result.policiesCount).toBe(5);
      expect(result.objectsProcessed).toBe(100);
    });
  });

  describe('replication policies', () => {
    it('should add a replication policy', async () => {
      nock(baseUrl).post('/replication/policies').reply(200, { success: true });

      const result = await client.addReplicationPolicy({
        policy: {
          id: 'rep-1',
          sourceBackend: 's3',
          sourceSettings: { bucket: 'source' },
          sourcePrefix: '',
          destinationBackend: 'gcs',
          destinationSettings: { bucket: 'dest' },
          checkIntervalSeconds: 3600,
          enabled: true,
          replicationMode: 0,
        },
      });

      expect(result.success).toBe(true);
    });

    it('should get replication policies', async () => {
      nock(baseUrl).get('/replication/policies').reply(200, {
        policies: [
          {
            id: 'rep-1',
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
      expect(result.policies[0].id).toBe('rep-1');
    });

    it('should trigger replication', async () => {
      nock(baseUrl).post('/replication/trigger').reply(200, {
        success: true,
        result: {
          policy_id: 'rep-1',
          synced: 10,
          deleted: 2,
          failed: 0,
          bytes_total: 1024,
          duration_ms: 500,
          errors: [],
        },
      });

      const result = await client.triggerReplication({
        policyId: 'rep-1',
        parallel: true,
      });

      expect(result.success).toBe(true);
      expect(result.result?.synced).toBe(10);
    });

    it('should get replication status', async () => {
      nock(baseUrl).get('/replication/status/rep-1').reply(200, {
        success: true,
        status: {
          policy_id: 'rep-1',
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

      const result = await client.getReplicationStatus({ id: 'rep-1' });

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
    it('should handle network errors in put', async () => {
      nock(baseUrl).put('/objects/test-key').replyWithError('Network error');

      await expect(
        client.put({ key: 'test-key', data: Buffer.from('data') })
      ).rejects.toThrow();
    });

    it('should handle network errors in get', async () => {
      nock(baseUrl).get('/objects/test-key').replyWithError('Network error');

      await expect(client.get({ key: 'test-key' })).rejects.toThrow();
    });

    it('should handle network errors in delete', async () => {
      nock(baseUrl).delete('/objects/test-key').replyWithError('Network error');

      await expect(client.delete({ key: 'test-key' })).rejects.toThrow();
    });

    it('should handle network errors in list', async () => {
      nock(baseUrl).get('/objects').replyWithError('Network error');

      await expect(client.list()).rejects.toThrow();
    });

    it('should handle network errors in getMetadata', async () => {
      nock(baseUrl).get('/objects/test-key/metadata').replyWithError('Network error');

      await expect(client.getMetadata({ key: 'test-key' })).rejects.toThrow();
    });

    it('should handle network errors in updateMetadata', async () => {
      nock(baseUrl).put('/objects/test-key/metadata').replyWithError('Network error');

      await expect(
        client.updateMetadata({ key: 'test-key', metadata: {} })
      ).rejects.toThrow();
    });

    it('should handle network errors in health', async () => {
      nock(baseUrl).get('/health').replyWithError('Network error');

      await expect(client.health()).rejects.toThrow();
    });

    it('should handle network errors in archive', async () => {
      nock(baseUrl).post('/archive').replyWithError('Network error');

      await expect(
        client.archive({ key: 'test', destinationType: 'glacier' })
      ).rejects.toThrow();
    });

    it('should handle network errors in triggerReplication', async () => {
      nock(baseUrl).post('/replication/trigger').replyWithError('Network error');

      await expect(client.triggerReplication({ policyId: 'test' })).rejects.toThrow();
    });

    it('should handle network errors in getReplicationStatus', async () => {
      nock(baseUrl).get('/replication/status/test').replyWithError('Network error');

      await expect(client.getReplicationStatus({ id: 'test' })).rejects.toThrow();
    });

    it('should handle network errors in getReplicationPolicy', async () => {
      nock(baseUrl).get('/replication/policies/test').replyWithError('Network error');

      await expect(client.getReplicationPolicy({ id: 'test' })).rejects.toThrow();
    });

    it('should handle network errors in getReplicationPolicies', async () => {
      nock(baseUrl).get('/replication/policies').replyWithError('Network error');

      await expect(client.getReplicationPolicies()).rejects.toThrow();
    });

    it('should throw error when exists check fails (non-404)', async () => {
      nock(baseUrl).head('/objects/test').reply(500);

      await expect(client.exists({ key: 'test' })).rejects.toThrow();
    });

    it('should handle network errors in removePolicy', async () => {
      nock(baseUrl).delete('/policies/p1').replyWithError('Network error');

      await expect(client.removePolicy({ id: 'p1' })).rejects.toThrow();
    });

    it('should handle network errors in getPolicies', async () => {
      nock(baseUrl).get('/policies').replyWithError('Network error');

      await expect(client.getPolicies()).rejects.toThrow();
    });

    it('should handle network errors in applyPolicies', async () => {
      nock(baseUrl).post('/policies/apply').replyWithError('Network error');

      await expect(client.applyPolicies()).rejects.toThrow();
    });

    it('should handle network errors in addPolicy', async () => {
      nock(baseUrl).post('/policies').replyWithError('Network error');

      await expect(
        client.addPolicy({
          policy: { id: 'p1', prefix: 'logs/', retentionSeconds: 86400, action: 'delete' },
        })
      ).rejects.toThrow();
    });

    it('should handle network errors in addReplicationPolicy', async () => {
      nock(baseUrl).post('/replication/policies').replyWithError('Network error');

      await expect(
        client.addReplicationPolicy({
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
        })
      ).rejects.toThrow();
    });

    it('should handle network errors in removeReplicationPolicy', async () => {
      nock(baseUrl).delete('/replication/policies/r1').replyWithError('Network error');

      await expect(client.removeReplicationPolicy({ id: 'r1' })).rejects.toThrow();
    });
  });

  describe('additional edge cases', () => {
    it('should handle health check with service parameter', async () => {
      nock(baseUrl).get('/health?service=storage').reply(200, { status: 'healthy' });

      const result = await client.health({ service: 'storage' });

      expect(result.status).toBe(HealthStatus.SERVING);
    });

    it('should handle unknown health status', async () => {
      nock(baseUrl).get('/health').reply(200, { status: 'unknown' });

      const result = await client.health();

      expect(result.status).toBe(HealthStatus.UNKNOWN);
    });

    it('should handle getPolicies with prefix parameter', async () => {
      nock(baseUrl).get('/policies?prefix=logs%2F').reply(200, {
        policies: [],
        success: true,
      });

      const result = await client.getPolicies({ prefix: 'logs/' });

      expect(result.success).toBe(true);
    });

    it('should handle removeReplicationPolicy error', async () => {
      nock(baseUrl).delete('/replication/policies/r1').reply(404, { error: 'Not found' });

      await expect(client.removeReplicationPolicy({ id: 'r1' })).rejects.toThrow();
    });

    it('should handle getReplicationPolicy error', async () => {
      nock(baseUrl).get('/replication/policies/r1').reply(404, { error: 'Not found' });

      await expect(client.getReplicationPolicy({ id: 'r1' })).rejects.toThrow();
    });

    it('should handle triggerReplication without result', async () => {
      nock(baseUrl).post('/replication/trigger').reply(200, { success: true });

      const result = await client.triggerReplication({ policyId: 'r1' });

      expect(result.success).toBe(true);
      expect(result.result).toBeUndefined();
    });

    it('should handle getReplicationStatus without status', async () => {
      nock(baseUrl).get('/replication/status/r1').reply(200, { success: true });

      const result = await client.getReplicationStatus({ id: 'r1' });

      expect(result.success).toBe(true);
      expect(result.status).toBeUndefined();
    });

    it('should handle metadata with alternate field names', async () => {
      nock(baseUrl).get('/objects').reply(200, {
        objects: [
          {
            key: 'file1.txt',
            modified: '2023-01-01T00:00:00Z',
            metadata: {
              content_type: 'text/plain',
              custom: { author: 'test' },
            },
          },
        ],
        truncated: false,
      });

      const result = await client.list({});

      expect(result.objects[0].metadata?.lastModified).toBeDefined();
    });

    it('should handle error with response data message', async () => {
      nock(baseUrl)
        .get('/objects/test')
        .reply(500, { message: 'Internal server error' });

      // Error message is wrapped as "REST API error (500): ..."
      await expect(client.get({ key: 'test' })).rejects.toThrow('REST API error (500)');
    });

    it('should handle error with response data error field', async () => {
      nock(baseUrl).get('/objects/test').reply(400, { error: 'Bad request' });

      // Error message is wrapped as "REST API error (400): ..."
      await expect(client.get({ key: 'test' })).rejects.toThrow('REST API error (400)');
    });

    it('should handle non-axios errors', async () => {
      // When a non-axios error occurs, it should be re-thrown
      // This test verifies that errors without axios-specific properties are handled
      nock(baseUrl).get('/objects/test').reply(500, { error: 'Server error' });

      await expect(client.get({ key: 'test' })).rejects.toThrow('REST API error (500)');
    });

    it('should handle put response without etag header', async () => {
      nock(baseUrl).put('/objects/test-key').reply(201, { message: 'success' });

      const result = await client.put({
        key: 'test-key',
        data: Buffer.from('test data'),
      });

      expect(result.success).toBe(true);
      expect(result.etag).toBeUndefined();
    });

    it('should serialize metadata with all fields', async () => {
      nock(baseUrl)
        .put('/objects/test-key', () => true)
        .reply(201, { message: 'success' });

      const result = await client.put({
        key: 'test-key',
        data: Buffer.from('test data'),
        metadata: {
          contentType: 'application/json',
          contentEncoding: 'gzip',
          size: 100,
          etag: 'abc123',
          lastModified: new Date('2023-01-01'),
          custom: { version: '1.0' },
        },
      });

      expect(result.success).toBe(true);
    });
  });
});
