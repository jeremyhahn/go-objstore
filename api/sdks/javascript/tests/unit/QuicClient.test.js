import { describe, it, expect, beforeEach, afterEach, jest } from '@jest/globals';
import { QuicClient } from '../../src/clients/QuicClient.js';

// Simple mock fetch that doesn't use global
const createSimpleMockResponse = (data, ok = true, status = 200) => {
  return Promise.resolve({
    ok,
    status,
    json: () => Promise.resolve(data),
    text: () => Promise.resolve(typeof data === 'string' ? data : JSON.stringify(data)),
    arrayBuffer: () => Promise.resolve(Buffer.from('mock data')),
  });
};

describe('QuicClient', () => {
  describe('constructor', () => {
    it('should create client with valid config', () => {
      const client = new QuicClient({ baseURL: 'http://localhost:8443' });
      expect(client).toBeInstanceOf(QuicClient);
      expect(client.baseURL).toBe('http://localhost:8443');
      client.close();
    });

    it('should throw error if baseURL is missing', () => {
      expect(() => new QuicClient({})).toThrow('baseURL is required');
    });

    it('should throw error if config is missing', () => {
      expect(() => new QuicClient()).toThrow('baseURL is required');
    });

    it('should strip trailing slash from baseURL', () => {
      const client = new QuicClient({ baseURL: 'http://localhost:8443/' });
      expect(client.baseURL).toBe('http://localhost:8443');
      client.close();
    });

    it('should set default timeout', () => {
      const client = new QuicClient({ baseURL: 'http://localhost:8443' });
      expect(client.timeout).toBe(30000);
      client.close();
    });

    it('should use custom timeout', () => {
      const client = new QuicClient({ baseURL: 'http://localhost:8443', timeout: 5000 });
      expect(client.timeout).toBe(5000);
      client.close();
    });

    it('should accept custom headers', () => {
      const headers = { 'X-Custom': 'value' };
      const client = new QuicClient({ baseURL: 'http://localhost:8443', headers });
      expect(client.headers).toEqual(headers);
      client.close();
    });

    it('should have isNode property', () => {
      const client = new QuicClient({ baseURL: 'http://localhost:8443' });
      // isNode should be defined (could be true or false)
      expect(client.isNode).toBeDefined();
      client.close();
    });
  });

  describe('API method validation', () => {
    let client;

    beforeEach(() => {
      client = new QuicClient({ baseURL: 'http://localhost:8443' });
    });

    afterEach(() => {
      client.close();
    });

    it('should reject put with missing key', async () => {
      await expect(client.put('', Buffer.from('data'))).rejects.toThrow('key is required');
    });

    it('should reject put with null key', async () => {
      await expect(client.put(null, Buffer.from('data'))).rejects.toThrow('key is required');
    });

    it('should reject put with missing data', async () => {
      await expect(client.put('key', null)).rejects.toThrow('data is required');
    });

    it('should reject put with undefined data', async () => {
      await expect(client.put('key', undefined)).rejects.toThrow('data is required');
    });

    it('should reject get with missing key', async () => {
      await expect(client.get('')).rejects.toThrow('key is required');
    });

    it('should reject get with null key', async () => {
      await expect(client.get(null)).rejects.toThrow('key is required');
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

  describe('list method', () => {
    let client;

    beforeEach(() => {
      client = new QuicClient({ baseURL: 'http://localhost:8443' });
      // Mock _makeRequest to prevent real network calls
      client._makeRequest = jest.fn().mockResolvedValue({
        ok: true,
        json: () => Promise.resolve({
          objects: [],
          commonPrefixes: [],
          nextToken: '',
          truncated: false
        })
      });
    });

    afterEach(() => {
      client.close();
    });

    it('should accept empty options', () => {
      expect(() => client.list()).not.toThrow();
      expect(() => client.list({})).not.toThrow();
    });

    it('should accept valid options', () => {
      expect(() => client.list({ prefix: 'test/' })).not.toThrow();
      expect(() => client.list({ delimiter: '/' })).not.toThrow();
      expect(() => client.list({ limit: 100 })).not.toThrow();
      expect(() => client.list({ token: 'abc' })).not.toThrow();
    });
  });

  describe('listWithOptions method', () => {
    let client;

    beforeEach(() => {
      client = new QuicClient({ baseURL: 'http://localhost:8443' });
      // Mock _makeRequest to prevent real network calls
      client._makeRequest = jest.fn().mockResolvedValue({
        ok: true,
        json: () => Promise.resolve({
          objects: [],
          commonPrefixes: [],
          nextToken: '',
          truncated: false
        })
      });
    });

    afterEach(() => {
      client.close();
    });

    it('should accept empty options', () => {
      expect(() => client.listWithOptions()).not.toThrow();
      expect(() => client.listWithOptions({})).not.toThrow();
    });

    it('should accept valid options', () => {
      expect(() => client.listWithOptions({ prefix: 'test/' })).not.toThrow();
      expect(() => client.listWithOptions({ delimiter: '/' })).not.toThrow();
      expect(() => client.listWithOptions({ maxResults: 100 })).not.toThrow();
      expect(() => client.listWithOptions({ continueFrom: 'abc' })).not.toThrow();
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

  describe('getPolicies method', () => {
    let client;

    beforeEach(() => {
      client = new QuicClient({ baseURL: 'http://localhost:8443' });
      // Mock _makeRequest to prevent real network calls
      client._makeRequest = jest.fn().mockResolvedValue({
        ok: true,
        json: () => Promise.resolve({
          success: true,
          policies: []
        })
      });
    });

    afterEach(() => {
      client.close();
    });

    it('should accept empty prefix', () => {
      expect(() => client.getPolicies()).not.toThrow();
      expect(() => client.getPolicies('')).not.toThrow();
    });

    it('should accept valid prefix', () => {
      expect(() => client.getPolicies('prefix/')).not.toThrow();
    });
  });

  describe('triggerReplication method', () => {
    let client;

    beforeEach(() => {
      client = new QuicClient({ baseURL: 'http://localhost:8443' });
      // Mock _makeRequest to prevent real network calls
      client._makeRequest = jest.fn().mockResolvedValue({
        ok: true,
        json: () => Promise.resolve({
          success: true,
          result: {},
          message: ''
        })
      });
    });

    afterEach(() => {
      client.close();
    });

    it('should accept empty options', () => {
      expect(() => client.triggerReplication()).not.toThrow();
      expect(() => client.triggerReplication({})).not.toThrow();
    });

    it('should accept valid options', () => {
      expect(() => client.triggerReplication({ policyId: 'r1' })).not.toThrow();
      expect(() => client.triggerReplication({ parallel: true })).not.toThrow();
      expect(() => client.triggerReplication({ workerCount: 8 })).not.toThrow();
    });
  });

  describe('edge cases', () => {
    let client;

    beforeEach(() => {
      client = new QuicClient({ baseURL: 'http://localhost:8443' });
      // Mock _makeRequest to prevent real network calls
      client._makeRequest = jest.fn().mockResolvedValue({
        ok: true,
        json: () => Promise.resolve({ success: true, message: 'OK', data: { etag: 'test-etag' } }),
        arrayBuffer: () => Promise.resolve(Buffer.from('test data')),
        headers: { get: () => 'application/octet-stream' }
      });
    });

    afterEach(() => {
      client.close();
    });

    it('should handle special characters in keys', () => {
      const specialKeys = [
        'file with spaces.txt',
        'file/with/slashes.txt',
        'file-with-dashes.txt',
        'file_with_underscores.txt',
        'file.with.dots.txt',
      ];

      specialKeys.forEach((key) => {
        expect(() => client.get(key)).not.toThrow();
      });
    });

    it('should handle empty string key as invalid', async () => {
      await expect(client.get('')).rejects.toThrow('key is required');
    });

    it('should handle various data types for put', () => {
      expect(() => client.put('key', Buffer.from('data'))).not.toThrow();
      expect(() => client.put('key', new Uint8Array([1, 2, 3]))).not.toThrow();
      expect(() => client.put('key', 'string data')).not.toThrow();
    });
  });

  describe('close method', () => {
    it('should not throw when closing', () => {
      const client = new QuicClient({ baseURL: 'http://localhost:8443' });
      expect(() => client.close()).not.toThrow();
    });

    it('should be safe to call close multiple times', () => {
      const client = new QuicClient({ baseURL: 'http://localhost:8443' });
      expect(() => {
        client.close();
        client.close();
        client.close();
      }).not.toThrow();
    });
  });

  describe('API method success paths', () => {
    let client;

    beforeEach(() => {
      client = new QuicClient({ baseURL: 'http://localhost:8443' });
      // Mock the _makeRequest method to avoid actual HTTP calls
      client._makeRequest = jest.fn();
    });

    afterEach(() => {
      client.close();
    });

    it('should put object successfully', async () => {
      client._makeRequest.mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve({ success: true, etag: 'abc123' })
      });
      const result = await client.put('test.txt', Buffer.from('hello'));
      expect(result.success).toBe(true);
      expect(client._makeRequest).toHaveBeenCalled();
    });

    it('should put object with metadata', async () => {
      client._makeRequest.mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve({ success: true, etag: 'abc123' })
      });
      const result = await client.put('test.txt', Buffer.from('hello'), {
        contentType: 'text/plain',
        contentEncoding: 'gzip',
        custom: { key: 'value' }
      });
      expect(result.success).toBe(true);
    });

    it('should get object successfully', async () => {
      client._makeRequest.mockResolvedValueOnce({
        ok: true,
        status: 200,
        arrayBuffer: () => Promise.resolve(Buffer.from('hello world')),
        headers: new Map([['content-type', 'text/plain'], ['x-metadata', '{}']])
      });
      const result = await client.get('test.txt');
      expect(result).toHaveProperty('data');
    });

    it('should delete object successfully', async () => {
      client._makeRequest.mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve({ success: true })
      });
      const result = await client.delete('test.txt');
      expect(result.success).toBe(true);
    });

    it('should list objects successfully', async () => {
      client._makeRequest.mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve({
          objects: [{ key: 'file1.txt' }],
          commonPrefixes: [],
          nextToken: '',
          truncated: false
        })
      });
      const result = await client.list();
      expect(result).toHaveProperty('objects');
    });

    it('should list objects with options', async () => {
      client._makeRequest.mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve({
          objects: [],
          commonPrefixes: ['prefix/'],
          nextToken: 'token123',
          truncated: true
        })
      });
      const result = await client.list({ prefix: 'test/', delimiter: '/', limit: 100 });
      expect(result).toHaveProperty('objects');
    });

    it('should listWithOptions successfully', async () => {
      client._makeRequest.mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve({
          objects: [],
          commonPrefixes: [],
          nextToken: '',
          truncated: false
        })
      });
      const result = await client.listWithOptions({ prefix: 'test/', maxResults: 100 });
      expect(result).toHaveProperty('objects');
    });

    it('should check exists successfully', async () => {
      client._makeRequest.mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve({ exists: true })
      });
      const result = await client.exists('test.txt');
      expect(result.exists).toBe(true);
    });

    it('should get metadata successfully', async () => {
      client._makeRequest.mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve({
          success: true,
          metadata: { contentType: 'text/plain', size: 100 }
        })
      });
      const result = await client.getMetadata('test.txt');
      expect(result.success).toBe(true);
    });

    it('should update metadata successfully', async () => {
      client._makeRequest.mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve({ success: true })
      });
      const result = await client.updateMetadata('test.txt', { contentType: 'text/plain' });
      expect(result.success).toBe(true);
    });

    it('should check health successfully', async () => {
      client._makeRequest.mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve({ status: 'healthy' })
      });
      const result = await client.health();
      expect(result).toHaveProperty('status');
    });

    it('should archive object successfully', async () => {
      client._makeRequest.mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve({ success: true })
      });
      const result = await client.archive('test.txt', 's3', { bucket: 'archive' });
      expect(result.success).toBe(true);
    });

    it('should add policy successfully', async () => {
      client._makeRequest.mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve({ success: true })
      });
      const result = await client.addPolicy({ id: 'p1', prefix: 'test/', action: 'delete' });
      expect(result.success).toBe(true);
    });

    it('should remove policy successfully', async () => {
      client._makeRequest.mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve({ success: true })
      });
      const result = await client.removePolicy('p1');
      expect(result.success).toBe(true);
    });

    it('should get policies successfully', async () => {
      client._makeRequest.mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve({ success: true, policies: [] })
      });
      const result = await client.getPolicies();
      expect(result).toHaveProperty('policies');
    });

    it('should get policies with prefix', async () => {
      client._makeRequest.mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve({ success: true, policies: [] })
      });
      const result = await client.getPolicies('test/');
      expect(result.success).toBe(true);
    });

    it('should apply policies successfully', async () => {
      client._makeRequest.mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve({
          success: true,
          policiesCount: 2,
          objectsProcessed: 10
        })
      });
      const result = await client.applyPolicies();
      expect(result.success).toBe(true);
    });

    it('should add replication policy successfully', async () => {
      client._makeRequest.mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve({ success: true })
      });
      const result = await client.addReplicationPolicy({ id: 'r1', sourceBackend: 'local' });
      expect(result.success).toBe(true);
    });

    it('should remove replication policy successfully', async () => {
      client._makeRequest.mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve({ success: true })
      });
      const result = await client.removeReplicationPolicy('r1');
      expect(result.success).toBe(true);
    });

    it('should get replication policies successfully', async () => {
      client._makeRequest.mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve({ policies: [] })
      });
      const result = await client.getReplicationPolicies();
      expect(result).toHaveProperty('policies');
    });

    it('should get specific replication policy', async () => {
      client._makeRequest.mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve({ policy: { id: 'r1' } })
      });
      const result = await client.getReplicationPolicy('r1');
      expect(result).toHaveProperty('policy');
    });

    it('should trigger replication successfully', async () => {
      client._makeRequest.mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve({
          success: true,
          result: { synced: 10, failed: 0 }
        })
      });
      const result = await client.triggerReplication();
      expect(result.success).toBe(true);
    });

    it('should trigger replication with options', async () => {
      client._makeRequest.mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve({
          success: true,
          result: { synced: 5, failed: 0 }
        })
      });
      const result = await client.triggerReplication({
        policyId: 'r1',
        parallel: true,
        workerCount: 8
      });
      expect(result.success).toBe(true);
    });

    it('should get replication status successfully', async () => {
      client._makeRequest.mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve({
          success: true,
          status: { policyId: 'r1', synced: 100 }
        })
      });
      const result = await client.getReplicationStatus('r1');
      expect(result.success).toBe(true);
    });
  });

  describe('API error handling', () => {
    let client;

    beforeEach(() => {
      client = new QuicClient({ baseURL: 'http://localhost:8443' });
      client._makeRequest = jest.fn();
    });

    afterEach(() => {
      client.close();
    });

    it('should handle network error', async () => {
      client._makeRequest.mockRejectedValueOnce(new Error('Network error'));
      await expect(client.put('key', Buffer.from('data'))).rejects.toThrow('Network error');
    });

    it('should handle get 404 as not found', async () => {
      client._makeRequest.mockResolvedValueOnce({
        ok: false,
        status: 404,
        arrayBuffer: () => Promise.resolve(Buffer.from('')),
        headers: { get: () => null }
      });
      const result = await client.get('nonexistent.txt');
      expect(result.data).toHaveLength(0);
    });
  });
});
