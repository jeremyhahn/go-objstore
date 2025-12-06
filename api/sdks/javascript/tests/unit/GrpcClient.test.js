import { describe, it, expect, jest, beforeEach, afterEach } from '@jest/globals';
import { GrpcClient } from '../../src/clients/GrpcClient.js';

// Mock the gRPC client to prevent actual connections during tests
const createMockStream = () => {
  const handlers = {};
  return {
    on: jest.fn((event, handler) => {
      handlers[event] = handler;
      // Simulate async streaming behavior
      if (event === 'end') {
        setTimeout(() => {
          if (handlers['data']) {
            handlers['data']({ data: Buffer.from('mock data'), metadata: { content_type: 'text/plain', size: 9 } });
          }
          handlers['end']();
        }, 0);
      }
      return { on: jest.fn() };
    }),
  };
};

const mockGrpcMethods = {
  Put: jest.fn((request, callback) => {
    callback(null, { success: true, message: 'OK', etag: 'abc123' });
  }),
  Get: jest.fn(() => createMockStream()),
  Delete: jest.fn((request, callback) => {
    callback(null, { success: true, message: 'OK' });
  }),
  List: jest.fn((request, callback) => {
    callback(null, { objects: [], common_prefixes: [], next_token: '', truncated: false });
  }),
  Exists: jest.fn((request, callback) => {
    callback(null, { exists: true });
  }),
  GetMetadata: jest.fn((request, callback) => {
    callback(null, { success: true, metadata: { content_type: 'text/plain', size: 100 }, message: 'OK' });
  }),
  UpdateMetadata: jest.fn((request, callback) => {
    callback(null, { success: true, message: 'OK' });
  }),
  Health: jest.fn((request, callback) => {
    callback(null, { status: 1, message: 'OK' });
  }),
  Archive: jest.fn((request, callback) => {
    callback(null, { success: true, message: 'OK' });
  }),
  AddPolicy: jest.fn((request, callback) => {
    callback(null, { success: true, message: 'OK' });
  }),
  RemovePolicy: jest.fn((request, callback) => {
    callback(null, { success: true, message: 'OK' });
  }),
  GetPolicies: jest.fn((request, callback) => {
    callback(null, { success: true, policies: [], message: 'OK' });
  }),
  ApplyPolicies: jest.fn((request, callback) => {
    callback(null, { success: true, policies_count: 0, objects_processed: 0, message: 'OK' });
  }),
  AddReplicationPolicy: jest.fn((request, callback) => {
    callback(null, { success: true, message: 'OK' });
  }),
  RemoveReplicationPolicy: jest.fn((request, callback) => {
    callback(null, { success: true, message: 'OK' });
  }),
  GetReplicationPolicies: jest.fn((request, callback) => {
    callback(null, { policies: [] });
  }),
  GetReplicationPolicy: jest.fn((request, callback) => {
    callback(null, { policy: { id: 'test-policy', source_backend: 'local' } });
  }),
  TriggerReplication: jest.fn((request, callback) => {
    callback(null, { success: true, result: { synced: 10 }, message: 'OK' });
  }),
  GetReplicationStatus: jest.fn((request, callback) => {
    callback(null, { success: true, status: { policy_id: 'test-policy' }, message: 'OK' });
  }),
  close: jest.fn(),
};

// Mock the _ensureClient method to use our mock instead of real gRPC
function mockEnsureClient() {
  const originalEnsureClient = GrpcClient.prototype._ensureClient;

  beforeEach(() => {
    GrpcClient.prototype._ensureClient = function() {
      if (!this.client) {
        this.client = mockGrpcMethods;
      }
    };
  });

  afterEach(() => {
    GrpcClient.prototype._ensureClient = originalEnsureClient;
    // Reset all mocks
    Object.values(mockGrpcMethods).forEach(mock => {
      if (typeof mock.mockClear === 'function') {
        mock.mockClear();
      }
    });
  });
}

describe('GrpcClient', () => {
  describe('constructor', () => {
    it('should throw error if address is missing', () => {
      expect(() => new GrpcClient({})).toThrow('address is required');
    });

    it('should throw error if config is missing', () => {
      expect(() => new GrpcClient()).toThrow('address is required');
    });

    it('should create client with valid config', () => {
      const client = new GrpcClient({ address: 'localhost:50051' });
      expect(client).toBeInstanceOf(GrpcClient);
      expect(client.address).toBe('localhost:50051');
      client.close();
    });

    it('should use insecure by default', () => {
      const client = new GrpcClient({ address: 'localhost:50051' });
      expect(client.insecure).toBe(true);
      client.close();
    });

    it('should respect insecure setting', () => {
      const client = new GrpcClient({ address: 'localhost:50051', insecure: false });
      expect(client.insecure).toBe(false);
      client.close();
    });
  });

  describe('helper methods', () => {
    let client;

    mockEnsureClient();

    beforeEach(() => {
      client = new GrpcClient({ address: 'localhost:50051' });
    });

    afterEach(() => {
      client.close();
    });

    it('should convert metadata from camelCase to snake_case', () => {
      const metadata = {
        contentType: 'text/plain',
        contentEncoding: 'gzip',
        size: 100,
        lastModified: '2025-11-23',
        etag: 'abc123',
        custom: { key: 'value' },
      };

      const converted = client._convertMetadata(metadata);

      expect(converted.content_type).toBe('text/plain');
      expect(converted.content_encoding).toBe('gzip');
      expect(converted.size).toBe(100);
      expect(converted.etag).toBe('abc123');
      expect(converted.custom).toEqual({ key: 'value' });
    });

    it('should handle null metadata', () => {
      const converted = client._convertMetadata(null);
      expect(converted).toBeNull();
    });

    it('should parse metadata from snake_case to camelCase', () => {
      const metadata = {
        content_type: 'text/plain',
        content_encoding: 'gzip',
        size: 100,
        last_modified: '2025-11-23',
        etag: 'abc123',
        custom: { key: 'value' },
      };

      const parsed = client._parseMetadata(metadata);

      expect(parsed.contentType).toBe('text/plain');
      expect(parsed.contentEncoding).toBe('gzip');
      expect(parsed.size).toBe(100);
      expect(parsed.etag).toBe('abc123');
      expect(parsed.custom).toEqual({ key: 'value' });
    });

    it('should handle null metadata in parse', () => {
      const parsed = client._parseMetadata(null);
      expect(parsed).toEqual({});
    });

    it('should get health status string', () => {
      expect(client._getHealthStatus(0)).toBe('UNKNOWN');
      expect(client._getHealthStatus(1)).toBe('SERVING');
      expect(client._getHealthStatus(2)).toBe('NOT_SERVING');
      expect(client._getHealthStatus(999)).toBe('UNKNOWN');
    });

    it('should convert error with details', () => {
      const grpcError = {
        code: 14,
        details: 'Connection failed',
        metadata: {},
      };

      const converted = client._convertError(grpcError);

      expect(converted.message).toBe('Connection failed');
      expect(converted.code).toBe(14);
    });

    it('should convert error with message', () => {
      const grpcError = {
        code: 5,
        message: 'Not found',
        metadata: {},
      };

      const converted = client._convertError(grpcError);

      expect(converted.message).toBe('Not found');
      expect(converted.code).toBe(5);
    });
  });

  describe('API method validation', () => {
    let client;

    mockEnsureClient();

    beforeEach(() => {
      client = new GrpcClient({ address: 'localhost:50051' });
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

    it('should reject put with undefined data', async () => {
      await expect(client.put('key', undefined)).rejects.toThrow('data is required');
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

  describe('list method', () => {
    let client;

    mockEnsureClient();

    beforeEach(() => {
      client = new GrpcClient({ address: 'localhost:50051' });
    });

    afterEach(() => {
      client.close();
    });

    it('should accept empty options', async () => {
      // These methods return promises and should resolve with mocked data
      const result1 = await client.list();
      const result2 = await client.list({});
      expect(result1).toHaveProperty('objects');
      expect(result2).toHaveProperty('objects');
    });

    it('should accept valid options', async () => {
      // These methods return promises and should resolve with mocked data
      await expect(client.list({ prefix: 'test/' })).resolves.toHaveProperty('objects');
      await expect(client.list({ delimiter: '/' })).resolves.toHaveProperty('objects');
      await expect(client.list({ maxResults: 100 })).resolves.toHaveProperty('objects');
      await expect(client.list({ continueFrom: 'abc' })).resolves.toHaveProperty('objects');
    });
  });

  describe('listWithOptions method', () => {
    let client;

    mockEnsureClient();

    beforeEach(() => {
      client = new GrpcClient({ address: 'localhost:50051' });
    });

    afterEach(() => {
      client.close();
    });

    it('should accept empty options', async () => {
      // These methods return promises and should resolve with mocked data
      const result1 = await client.listWithOptions();
      const result2 = await client.listWithOptions({});
      expect(result1).toHaveProperty('objects');
      expect(result2).toHaveProperty('objects');
    });

    it('should accept valid options', async () => {
      // These methods return promises and should resolve with mocked data
      await expect(client.listWithOptions({ prefix: 'test/' })).resolves.toHaveProperty('objects');
      await expect(client.listWithOptions({ delimiter: '/' })).resolves.toHaveProperty('objects');
      await expect(client.listWithOptions({ maxResults: 100 })).resolves.toHaveProperty('objects');
      await expect(client.listWithOptions({ continueFrom: 'abc' })).resolves.toHaveProperty('objects');
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

  describe('edge cases', () => {
    let client;

    mockEnsureClient();

    beforeEach(() => {
      client = new GrpcClient({ address: 'localhost:50051' });
    });

    afterEach(() => {
      client.close();
    });

    it('should handle special characters in keys', async () => {
      const specialKeys = [
        'file with spaces.txt',
        'file/with/slashes.txt',
        'file-with-dashes.txt',
        'file_with_underscores.txt',
        'file.with.dots.txt',
      ];

      // Test each key and await the promises
      for (const key of specialKeys) {
        await expect(client.get(key)).resolves.toHaveProperty('data');
      }
    });

    it('should handle various data types for put', async () => {
      // Test different data types and await the promises
      await expect(client.put('key', Buffer.from('data'))).resolves.toHaveProperty('success');
      await expect(client.put('key', new Uint8Array([1, 2, 3]))).resolves.toHaveProperty('success');
      await expect(client.put('key', 'string data')).resolves.toHaveProperty('success');
    });
  });

  describe('close method', () => {
    it('should not throw when closing', () => {
      const client = new GrpcClient({ address: 'localhost:50051' });
      expect(() => client.close()).not.toThrow();
    });

    it('should be safe to call close multiple times', () => {
      const client = new GrpcClient({ address: 'localhost:50051' });
      expect(() => {
        client.close();
        client.close();
        client.close();
      }).not.toThrow();
    });
  });

  describe('API method success paths', () => {
    let client;

    mockEnsureClient();

    beforeEach(() => {
      client = new GrpcClient({ address: 'localhost:50051' });
    });

    afterEach(() => {
      client.close();
    });

    it('should put object successfully', async () => {
      const result = await client.put('test.txt', Buffer.from('hello'));
      expect(result).toEqual({ success: true, message: 'OK', etag: 'abc123' });
    });

    it('should put object with metadata', async () => {
      const metadata = { contentType: 'text/plain', custom: { key: 'value' } };
      const result = await client.put('test.txt', Buffer.from('hello'), metadata);
      expect(result.success).toBe(true);
    });

    it('should get object successfully', async () => {
      const result = await client.get('test.txt');
      expect(result).toHaveProperty('data');
      expect(result).toHaveProperty('metadata');
    });

    it('should delete object successfully', async () => {
      const result = await client.delete('test.txt');
      expect(result).toEqual({ success: true, message: 'OK' });
    });

    it('should check object existence', async () => {
      const result = await client.exists('test.txt');
      expect(result).toEqual({ exists: true });
    });

    it('should get metadata successfully', async () => {
      const result = await client.getMetadata('test.txt');
      expect(result.success).toBe(true);
      expect(result).toHaveProperty('metadata');
    });

    it('should update metadata successfully', async () => {
      const result = await client.updateMetadata('test.txt', { contentType: 'text/plain' });
      expect(result).toEqual({ success: true, message: 'OK' });
    });

    it('should check health successfully', async () => {
      const result = await client.health();
      expect(result).toEqual({ status: 'SERVING', message: 'OK' });
    });

    it('should check health with service name', async () => {
      const result = await client.health('objstore');
      expect(result.status).toBe('SERVING');
    });

    it('should archive object successfully', async () => {
      const result = await client.archive('test.txt', 's3', { bucket: 'archive' });
      expect(result).toEqual({ success: true, message: 'OK' });
    });

    it('should add policy successfully', async () => {
      const result = await client.addPolicy({ id: 'p1', prefix: 'test/', action: 'delete' });
      expect(result).toEqual({ success: true, message: 'OK' });
    });

    it('should remove policy successfully', async () => {
      const result = await client.removePolicy('p1');
      expect(result).toEqual({ success: true, message: 'OK' });
    });

    it('should get policies successfully', async () => {
      const result = await client.getPolicies();
      expect(result.success).toBe(true);
      expect(result).toHaveProperty('policies');
    });

    it('should get policies with prefix', async () => {
      const result = await client.getPolicies('test/');
      expect(result.success).toBe(true);
    });

    it('should apply policies successfully', async () => {
      const result = await client.applyPolicies();
      expect(result.success).toBe(true);
      expect(result).toHaveProperty('policiesCount');
      expect(result).toHaveProperty('objectsProcessed');
    });

    it('should add replication policy successfully', async () => {
      const result = await client.addReplicationPolicy({ id: 'r1', source_backend: 'local' });
      expect(result).toEqual({ success: true, message: 'OK' });
    });

    it('should remove replication policy successfully', async () => {
      const result = await client.removeReplicationPolicy('r1');
      expect(result).toEqual({ success: true, message: 'OK' });
    });

    it('should get replication policies successfully', async () => {
      const result = await client.getReplicationPolicies();
      expect(result).toHaveProperty('policies');
    });

    it('should get specific replication policy', async () => {
      const result = await client.getReplicationPolicy('r1');
      expect(result).toHaveProperty('policy');
    });

    it('should trigger replication successfully', async () => {
      const result = await client.triggerReplication();
      expect(result.success).toBe(true);
      expect(result).toHaveProperty('result');
    });

    it('should trigger replication with options', async () => {
      const result = await client.triggerReplication({ policyId: 'r1', parallel: true, workerCount: 8 });
      expect(result.success).toBe(true);
    });

    it('should get replication status successfully', async () => {
      const result = await client.getReplicationStatus('r1');
      expect(result.success).toBe(true);
      expect(result).toHaveProperty('status');
    });
  });

  describe('API error handling', () => {
    let client;

    beforeEach(() => {
      GrpcClient.prototype._ensureClient = function() {
        if (!this.client) {
          this.client = {
            Put: jest.fn((req, cb) => cb({ code: 14, details: 'Connection failed' })),
            Get: jest.fn(() => {
              const handlers = {};
              return {
                on: jest.fn((event, handler) => {
                  handlers[event] = handler;
                  if (event === 'error') {
                    setTimeout(() => handler({ code: 5, details: 'Not found' }), 0);
                  }
                  return { on: jest.fn() };
                }),
              };
            }),
            Delete: jest.fn((req, cb) => cb({ code: 5, message: 'Not found' })),
            List: jest.fn((req, cb) => cb({ code: 2, details: 'Unknown error' })),
            Exists: jest.fn((req, cb) => cb({ code: 14, details: 'Connection refused' })),
            GetMetadata: jest.fn((req, cb) => cb({ code: 5, details: 'Not found' })),
            UpdateMetadata: jest.fn((req, cb) => cb({ code: 3, details: 'Invalid argument' })),
            Health: jest.fn((req, cb) => cb({ code: 14, details: 'Connection refused' })),
            Archive: jest.fn((req, cb) => cb({ code: 5, details: 'Not found' })),
            AddPolicy: jest.fn((req, cb) => cb({ code: 6, details: 'Already exists' })),
            RemovePolicy: jest.fn((req, cb) => cb({ code: 5, details: 'Not found' })),
            GetPolicies: jest.fn((req, cb) => cb({ code: 2, details: 'Unknown error' })),
            ApplyPolicies: jest.fn((req, cb) => cb({ code: 13, details: 'Internal error' })),
            AddReplicationPolicy: jest.fn((req, cb) => cb({ code: 6, details: 'Already exists' })),
            RemoveReplicationPolicy: jest.fn((req, cb) => cb({ code: 5, details: 'Not found' })),
            GetReplicationPolicies: jest.fn((req, cb) => cb({ code: 2, details: 'Unknown error' })),
            GetReplicationPolicy: jest.fn((req, cb) => cb({ code: 5, details: 'Not found' })),
            TriggerReplication: jest.fn((req, cb) => cb({ code: 13, details: 'Internal error' })),
            GetReplicationStatus: jest.fn((req, cb) => cb({ code: 5, details: 'Not found' })),
            close: jest.fn(),
          };
        }
      };
      client = new GrpcClient({ address: 'localhost:50051' });
    });

    afterEach(() => {
      client.close();
    });

    it('should handle put error', async () => {
      await expect(client.put('key', Buffer.from('data'))).rejects.toThrow('Connection failed');
    });

    it('should handle get streaming error', async () => {
      await expect(client.get('key')).rejects.toThrow('Not found');
    });

    it('should handle delete error', async () => {
      await expect(client.delete('key')).rejects.toThrow('Not found');
    });

    it('should handle list error', async () => {
      await expect(client.list()).rejects.toThrow('Unknown error');
    });

    it('should handle listWithOptions error', async () => {
      await expect(client.listWithOptions()).rejects.toThrow('Unknown error');
    });

    it('should handle exists error', async () => {
      await expect(client.exists('key')).rejects.toThrow('Connection refused');
    });

    it('should handle getMetadata error', async () => {
      await expect(client.getMetadata('key')).rejects.toThrow('Not found');
    });

    it('should handle updateMetadata error', async () => {
      await expect(client.updateMetadata('key', {})).rejects.toThrow('Invalid argument');
    });

    it('should handle health error', async () => {
      await expect(client.health()).rejects.toThrow('Connection refused');
    });

    it('should handle archive error', async () => {
      await expect(client.archive('key', 's3')).rejects.toThrow('Not found');
    });

    it('should handle addPolicy error', async () => {
      await expect(client.addPolicy({ id: 'p1' })).rejects.toThrow('Already exists');
    });

    it('should handle removePolicy error', async () => {
      await expect(client.removePolicy('p1')).rejects.toThrow('Not found');
    });

    it('should handle getPolicies error', async () => {
      await expect(client.getPolicies()).rejects.toThrow('Unknown error');
    });

    it('should handle applyPolicies error', async () => {
      await expect(client.applyPolicies()).rejects.toThrow('Internal error');
    });

    it('should handle addReplicationPolicy error', async () => {
      await expect(client.addReplicationPolicy({ id: 'r1' })).rejects.toThrow('Already exists');
    });

    it('should handle removeReplicationPolicy error', async () => {
      await expect(client.removeReplicationPolicy('r1')).rejects.toThrow('Not found');
    });

    it('should handle getReplicationPolicies error', async () => {
      await expect(client.getReplicationPolicies()).rejects.toThrow('Unknown error');
    });

    it('should handle getReplicationPolicy error', async () => {
      await expect(client.getReplicationPolicy('r1')).rejects.toThrow('Not found');
    });

    it('should handle triggerReplication error', async () => {
      await expect(client.triggerReplication()).rejects.toThrow('Internal error');
    });

    it('should handle getReplicationStatus error', async () => {
      await expect(client.getReplicationStatus('r1')).rejects.toThrow('Not found');
    });
  });
});
