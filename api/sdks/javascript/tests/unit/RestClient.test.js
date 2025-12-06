import { describe, it, expect, beforeEach, afterEach, jest } from '@jest/globals';
import { RestClient } from '../../src/clients/RestClient.js';
import axios from 'axios';
import MockAdapter from 'axios-mock-adapter';

describe('RestClient', () => {
  let mock;

  beforeEach(() => {
    // Create a new mock adapter before each test
    mock = new MockAdapter(axios);
  });

  afterEach(() => {
    // Restore axios after each test
    mock.restore();
  });

  describe('constructor', () => {
    it('should create client with valid config', () => {
      const client = new RestClient({ baseURL: 'http://localhost:8080' });
      expect(client).toBeInstanceOf(RestClient);
      expect(client.baseURL).toBe('http://localhost:8080');
      client.close();
    });

    it('should throw error if baseURL is missing', () => {
      expect(() => new RestClient({})).toThrow('baseURL is required');
    });

    it('should strip trailing slash from baseURL', () => {
      const client = new RestClient({ baseURL: 'http://localhost:8080/' });
      expect(client.baseURL).toBe('http://localhost:8080');
      client.close();
    });

    it('should set default timeout', () => {
      const client = new RestClient({ baseURL: 'http://localhost:8080' });
      expect(client.timeout).toBe(30000);
      client.close();
    });

    it('should use custom timeout', () => {
      const client = new RestClient({ baseURL: 'http://localhost:8080', timeout: 5000 });
      expect(client.timeout).toBe(5000);
      client.close();
    });

    it('should accept custom headers', () => {
      const headers = { 'X-Custom': 'value' };
      const client = new RestClient({ baseURL: 'http://localhost:8080', headers });
      expect(client.headers).toEqual(headers);
      client.close();
    });
  });

  describe('API method validation', () => {
    let client;

    beforeEach(() => {
      client = new RestClient({ baseURL: 'http://localhost:8080' });
    });

    afterEach(() => {
      client.close();
    });

    it('should reject put with missing key', async () => {
      await expect(client.put('', Buffer.from('data'))).rejects.toThrow('key is required');
    });

    it('should reject put with missing data', async () => {
      await expect(client.put('key', null)).rejects.toThrow('data is required');
    });

    it('should reject get with missing key', async () => {
      await expect(client.get('')).rejects.toThrow('key is required');
    });

    it('should reject delete with missing key', async () => {
      await expect(client.delete('')).rejects.toThrow('key is required');
    });

    it('should reject exists with missing key', async () => {
      await expect(client.exists('')).rejects.toThrow('key is required');
    });

    it('should reject getMetadata with missing key', async () => {
      await expect(client.getMetadata('')).rejects.toThrow('key is required');
    });

    it('should reject updateMetadata with missing key', async () => {
      await expect(client.updateMetadata('', {})).rejects.toThrow('key is required');
    });

    it('should reject updateMetadata with missing metadata', async () => {
      await expect(client.updateMetadata('key', null)).rejects.toThrow('metadata is required');
    });

    it('should reject archive with missing key', async () => {
      await expect(client.archive('', 'type')).rejects.toThrow('key is required');
    });

    it('should reject archive with missing destinationType', async () => {
      await expect(client.archive('key', '')).rejects.toThrow('destinationType is required');
    });

    it('should reject addPolicy with missing policy', async () => {
      await expect(client.addPolicy(null)).rejects.toThrow('policy is required');
    });

    it('should reject removePolicy with missing id', async () => {
      await expect(client.removePolicy('')).rejects.toThrow('id is required');
    });

    it('should reject addReplicationPolicy with missing policy', async () => {
      await expect(client.addReplicationPolicy(null)).rejects.toThrow('policy is required');
    });

    it('should reject removeReplicationPolicy with missing id', async () => {
      await expect(client.removeReplicationPolicy('')).rejects.toThrow('id is required');
    });

    it('should reject getReplicationPolicy with missing id', async () => {
      await expect(client.getReplicationPolicy('')).rejects.toThrow('id is required');
    });

    it('should reject getReplicationStatus with missing id', async () => {
      await expect(client.getReplicationStatus('')).rejects.toThrow('id is required');
    });
  });

  describe('PUT operation', () => {
    let client;

    beforeEach(() => {
      client = new RestClient({ baseURL: 'http://localhost:8080' });
    });

    afterEach(() => {
      client.close();
    });

    it('should upload object successfully', async () => {
      mock.onPut('/objects/test.txt').reply(200, {
        message: 'Object uploaded successfully',
        data: { etag: 'abc123' },
      });

      const result = await client.put('test.txt', Buffer.from('test data'));
      expect(result.success).toBe(true);
      expect(result.etag).toBe('abc123');
      expect(result.message).toBe('Object uploaded successfully');
    });

    it('should upload object with metadata', async () => {
      mock.onPut('/objects/test.txt').reply(200, {
        message: 'Object uploaded successfully',
        data: { etag: 'def456' },
      });

      const metadata = { contentType: 'text/plain', custom: { author: 'test' } };
      const result = await client.put('test.txt', Buffer.from('test data'), metadata);
      expect(result.success).toBe(true);
      expect(result.etag).toBe('def456');
    });

    it('should handle various data types', async () => {
      mock.onPut(/\/objects\/.*/).reply(200, {
        message: 'Object uploaded successfully',
        data: { etag: 'xyz789' },
      });

      // Buffer
      let result = await client.put('buffer.txt', Buffer.from('data'));
      expect(result.success).toBe(true);

      // Uint8Array
      result = await client.put('uint8.txt', new Uint8Array([1, 2, 3]));
      expect(result.success).toBe(true);

      // String
      result = await client.put('string.txt', 'string data');
      expect(result.success).toBe(true);
    });
  });

  describe('GET operation', () => {
    let client;

    beforeEach(() => {
      client = new RestClient({ baseURL: 'http://localhost:8080' });
    });

    afterEach(() => {
      client.close();
    });

    it('should download object successfully', async () => {
      const testData = Buffer.from('test data');
      mock.onGet('/objects/test.txt').reply(200, testData, {
        'content-type': 'text/plain',
        'content-length': '9',
        'etag': 'abc123',
        'last-modified': 'Mon, 01 Jan 2024 00:00:00 GMT',
      });

      const result = await client.get('test.txt');
      expect(result.data).toBeInstanceOf(Buffer);
      expect(result.data.toString()).toBe('test data');
      expect(result.metadata.contentType).toBe('text/plain');
      expect(result.metadata.contentLength).toBe(9);
      expect(result.metadata.etag).toBe('abc123');
    });

    it('should handle special characters in keys', async () => {
      mock.onGet(/\/objects\/.*/).reply(200, Buffer.from('data'));

      const result = await client.get('file with spaces.txt');
      expect(result.data).toBeDefined();
    });
  });

  describe('DELETE operation', () => {
    let client;

    beforeEach(() => {
      client = new RestClient({ baseURL: 'http://localhost:8080' });
    });

    afterEach(() => {
      client.close();
    });

    it('should delete object successfully', async () => {
      mock.onDelete('/objects/test.txt').reply(200, {
        message: 'Object deleted successfully',
      });

      const result = await client.delete('test.txt');
      expect(result.success).toBe(true);
      expect(result.message).toBe('Object deleted successfully');
    });
  });

  describe('EXISTS operation', () => {
    let client;

    beforeEach(() => {
      client = new RestClient({ baseURL: 'http://localhost:8080' });
    });

    afterEach(() => {
      client.close();
    });

    it('should return true if object exists', async () => {
      mock.onHead('/objects/test.txt').reply(200);

      const result = await client.exists('test.txt');
      expect(result.exists).toBe(true);
    });

    it('should return false if object does not exist', async () => {
      mock.onHead('/objects/test.txt').reply(404);

      const result = await client.exists('test.txt');
      expect(result.exists).toBe(false);
    });
  });

  describe('LIST operation', () => {
    let client;

    beforeEach(() => {
      client = new RestClient({ baseURL: 'http://localhost:8080' });
    });

    afterEach(() => {
      client.close();
    });

    it('should list objects with empty options', async () => {
      mock.onGet('/objects').reply(200, {
        objects: [{ key: 'test1.txt' }, { key: 'test2.txt' }],
        common_prefixes: [],
        next_token: '',
        truncated: false,
      });

      const result = await client.list();
      expect(result.objects).toHaveLength(2);
      expect(result.objects[0].key).toBe('test1.txt');
      expect(result.truncated).toBe(false);
    });

    it('should list objects with prefix', async () => {
      mock.onGet('/objects', { params: { prefix: 'test/' } }).reply(200, {
        objects: [{ key: 'test/file.txt' }],
        common_prefixes: [],
        next_token: '',
        truncated: false,
      });

      const result = await client.list({ prefix: 'test/' });
      expect(result.objects).toHaveLength(1);
    });

    it('should list objects with all options', async () => {
      mock.onGet('/objects').reply(200, {
        objects: [{ key: 'file.txt' }],
        common_prefixes: ['prefix1/', 'prefix2/'],
        next_token: 'token123',
        truncated: true,
      });

      const result = await client.list({
        prefix: 'test/',
        delimiter: '/',
        limit: 10,
        token: 'abc',
      });

      expect(result.objects).toHaveLength(1);
      expect(result.commonPrefixes).toHaveLength(2);
      expect(result.nextToken).toBe('token123');
      expect(result.truncated).toBe(true);
    });
  });

  describe('LIST WITH OPTIONS operation', () => {
    let client;

    beforeEach(() => {
      client = new RestClient({ baseURL: 'http://localhost:8080' });
    });

    afterEach(() => {
      client.close();
    });

    it('should list with maxResults', async () => {
      mock.onGet('/objects').reply(200, {
        objects: [{ key: 'test.txt' }],
        common_prefixes: [],
        next_token: '',
        truncated: false,
      });

      const result = await client.listWithOptions({ maxResults: 10 });
      expect(result.objects).toBeDefined();
    });

    it('should reject invalid prefix type', async () => {
      await expect(client.listWithOptions({ prefix: 123 })).rejects.toThrow(
        'prefix must be a string'
      );
    });

    it('should reject invalid delimiter type', async () => {
      await expect(client.listWithOptions({ delimiter: 123 })).rejects.toThrow(
        'delimiter must be a string'
      );
    });

    it('should reject invalid maxResults type', async () => {
      await expect(client.listWithOptions({ maxResults: '100' })).rejects.toThrow(
        'maxResults must be a number'
      );
    });

    it('should reject invalid continueFrom type', async () => {
      await expect(client.listWithOptions({ continueFrom: 123 })).rejects.toThrow(
        'continueFrom must be a string'
      );
    });
  });

  describe('METADATA operations', () => {
    let client;

    beforeEach(() => {
      client = new RestClient({ baseURL: 'http://localhost:8080' });
    });

    afterEach(() => {
      client.close();
    });

    it('should get metadata successfully', async () => {
      mock.onGet('/metadata/test.txt').reply(200, {
        metadata: {
          contentType: 'text/plain',
          size: 100,
        },
      });

      const result = await client.getMetadata('test.txt');
      expect(result.success).toBe(true);
      expect(result.metadata.contentType).toBe('text/plain');
    });

    it('should update metadata successfully', async () => {
      mock.onPut('/metadata/test.txt').reply(200, {
        message: 'Metadata updated successfully',
      });

      const result = await client.updateMetadata('test.txt', { custom: 'value' });
      expect(result.success).toBe(true);
      expect(result.message).toBe('Metadata updated successfully');
    });
  });

  describe('HEALTH check', () => {
    let client;

    beforeEach(() => {
      client = new RestClient({ baseURL: 'http://localhost:8080' });
    });

    afterEach(() => {
      client.close();
    });

    it('should perform health check successfully', async () => {
      mock.onGet('/health').reply(200, {
        status: 'healthy',
        message: 'Service is running',
      });

      const result = await client.health();
      expect(result.status).toBe('healthy');
      expect(result.message).toBe('Service is running');
    });
  });

  describe('ARCHIVE operation', () => {
    let client;

    beforeEach(() => {
      client = new RestClient({ baseURL: 'http://localhost:8080' });
    });

    afterEach(() => {
      client.close();
    });

    it('should archive object successfully', async () => {
      mock.onPost('/archive').reply(200, {
        message: 'Object archived successfully',
      });

      const result = await client.archive('test.txt', 's3', { bucket: 'archive' });
      expect(result.success).toBe(true);
      expect(result.message).toBe('Object archived successfully');
    });
  });

  describe('LIFECYCLE POLICY operations', () => {
    let client;

    beforeEach(() => {
      client = new RestClient({ baseURL: 'http://localhost:8080' });
    });

    afterEach(() => {
      client.close();
    });

    it('should add policy successfully', async () => {
      mock.onPost('/policies').reply(200, {
        message: 'Policy added successfully',
      });

      const policy = { id: 'p1', action: 'delete', age: 30 };
      const result = await client.addPolicy(policy);
      expect(result.success).toBe(true);
      expect(result.message).toBe('Policy added successfully');
    });

    it('should remove policy successfully', async () => {
      mock.onDelete('/policies/p1').reply(200, {
        message: 'Policy removed successfully',
      });

      const result = await client.removePolicy('p1');
      expect(result.success).toBe(true);
    });

    it('should get all policies', async () => {
      mock.onGet('/policies').reply(200, {
        policies: [
          { id: 'p1', action: 'delete' },
          { id: 'p2', action: 'archive' },
        ],
      });

      const result = await client.getPolicies();
      expect(result.success).toBe(true);
      expect(result.policies).toHaveLength(2);
    });

    it('should get policies with prefix', async () => {
      mock.onGet('/policies', { params: { prefix: 'test/' } }).reply(200, {
        policies: [{ id: 'p1', prefix: 'test/' }],
      });

      const result = await client.getPolicies('test/');
      expect(result.policies).toHaveLength(1);
    });

    it('should apply policies successfully', async () => {
      mock.onPost('/policies/apply').reply(200, {
        policies_count: 5,
        objects_processed: 100,
        message: 'Policies applied',
      });

      const result = await client.applyPolicies();
      expect(result.success).toBe(true);
      expect(result.policiesCount).toBe(5);
      expect(result.objectsProcessed).toBe(100);
    });
  });

  describe('REPLICATION POLICY operations', () => {
    let client;

    beforeEach(() => {
      client = new RestClient({ baseURL: 'http://localhost:8080' });
    });

    afterEach(() => {
      client.close();
    });

    it('should add replication policy successfully', async () => {
      mock.onPost('/replication/policies').reply(200, {
        message: 'Replication policy added successfully',
      });

      const policy = { id: 'r1', destination: 'remote' };
      const result = await client.addReplicationPolicy(policy);
      expect(result.success).toBe(true);
    });

    it('should remove replication policy successfully', async () => {
      mock.onDelete('/replication/policies/r1').reply(200, {
        message: 'Replication policy removed successfully',
      });

      const result = await client.removeReplicationPolicy('r1');
      expect(result.success).toBe(true);
    });

    it('should get all replication policies', async () => {
      mock.onGet('/replication/policies').reply(200, {
        policies: [{ id: 'r1' }, { id: 'r2' }],
      });

      const result = await client.getReplicationPolicies();
      expect(result.policies).toHaveLength(2);
    });

    it('should get specific replication policy', async () => {
      mock.onGet('/replication/policies/r1').reply(200, {
        policy: { id: 'r1', destination: 'remote' },
      });

      const result = await client.getReplicationPolicy('r1');
      expect(result.policy.id).toBe('r1');
    });

    it('should trigger replication', async () => {
      mock.onPost('/replication/trigger').reply(200, {
        result: { synced: 50 },
        message: 'Replication triggered',
      });

      const result = await client.triggerReplication({
        policyId: 'r1',
        parallel: true,
        workerCount: 4,
      });
      expect(result.success).toBe(true);
      expect(result.result.synced).toBe(50);
    });

    it('should get replication status', async () => {
      mock.onGet('/replication/status/r1').reply(200, {
        status: { state: 'active', progress: 75 },
        message: 'Replication in progress',
      });

      const result = await client.getReplicationStatus('r1');
      expect(result.success).toBe(true);
      expect(result.status.state).toBe('active');
    });
  });

  describe('edge cases', () => {
    let client;

    beforeEach(() => {
      client = new RestClient({ baseURL: 'http://localhost:8080' });
    });

    afterEach(() => {
      client.close();
    });

    it('should handle empty string key as invalid', async () => {
      await expect(client.get('')).rejects.toThrow('key is required');
    });

    it('should handle null data as invalid', async () => {
      await expect(client.put('key', null)).rejects.toThrow('data is required');
    });

    it('should handle undefined data as invalid', async () => {
      await expect(client.put('key', undefined)).rejects.toThrow('data is required');
    });
  });

  describe('close method', () => {
    it('should not throw when closing', () => {
      const client = new RestClient({ baseURL: 'http://localhost:8080' });
      expect(() => client.close()).not.toThrow();
    });

    it('should be safe to call close multiple times', () => {
      const client = new RestClient({ baseURL: 'http://localhost:8080' });
      expect(() => {
        client.close();
        client.close();
        client.close();
      }).not.toThrow();
    });
  });
});
