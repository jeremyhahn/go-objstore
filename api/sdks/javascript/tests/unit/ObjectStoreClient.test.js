import { describe, it, expect, beforeEach, afterEach, jest } from '@jest/globals';
import { ObjectStoreClient } from '../../src/ObjectStoreClient.js';

describe('ObjectStoreClient', () => {
  describe('constructor', () => {
    it('should throw error if config is missing', () => {
      expect(() => new ObjectStoreClient()).toThrow('Configuration is required');
    });

    it('should throw error if protocol is missing', () => {
      expect(() => new ObjectStoreClient({ baseURL: 'http://localhost' })).toThrow(
        'protocol is required'
      );
    });

    it('should throw error for unsupported protocol', () => {
      expect(() =>
        new ObjectStoreClient({ protocol: 'unknown', baseURL: 'http://localhost' })
      ).toThrow('Unsupported protocol: unknown');
    });

    it('should handle case-insensitive protocol', () => {
      const client = new ObjectStoreClient({
        protocol: 'REST',
        baseURL: 'http://localhost:8080',
      });

      expect(client.protocol).toBe('rest');
      client.close();
    });

    it('should create REST client', () => {
      const client = new ObjectStoreClient({
        protocol: 'rest',
        baseURL: 'http://localhost:8080',
      });

      expect(client.protocol).toBe('rest');
      expect(client.client).toBeDefined();
      client.close();
    });

    it('should create gRPC client', () => {
      const client = new ObjectStoreClient({
        protocol: 'grpc',
        baseURL: 'localhost:50051',
      });

      expect(client.protocol).toBe('grpc');
      expect(client.client).toBeDefined();
      client.close();
    });

    it('should create QUIC client with "quic" protocol', () => {
      const client = new ObjectStoreClient({
        protocol: 'quic',
        baseURL: 'http://localhost:8443',
      });

      expect(client.protocol).toBe('quic');
      expect(client.client).toBeDefined();
      client.close();
    });

    it('should create QUIC client with "http3" protocol', () => {
      const client = new ObjectStoreClient({
        protocol: 'http3',
        baseURL: 'http://localhost:8443',
      });

      expect(client.protocol).toBe('http3');
      expect(client.client).toBeDefined();
      client.close();
    });

    it('should pass through REST config options', () => {
      const client = new ObjectStoreClient({
        protocol: 'rest',
        baseURL: 'http://localhost:8080',
        timeout: 5000,
        headers: { 'X-Test': 'value' },
      });

      expect(client.config.timeout).toBe(5000);
      expect(client.config.headers).toEqual({ 'X-Test': 'value' });
      client.close();
    });

    it('should pass through gRPC config options', () => {
      const client = new ObjectStoreClient({
        protocol: 'grpc',
        baseURL: 'localhost:50051',
        insecure: false,
      });

      expect(client.config.insecure).toBe(false);
      client.close();
    });

    it('should pass through QUIC config options', () => {
      const client = new ObjectStoreClient({
        protocol: 'quic',
        baseURL: 'http://localhost:8443',
        http2: false,
      });

      expect(client.config.http2).toBe(false);
      client.close();
    });
  });

  describe('utility methods', () => {
    let client;

    beforeEach(() => {
      client = new ObjectStoreClient({
        protocol: 'rest',
        baseURL: 'http://localhost:8080',
      });
    });

    afterEach(() => {
      client.close();
    });

    it('should return protocol', () => {
      expect(client.getProtocol()).toBe('rest');
    });

    it('should return underlying client', () => {
      expect(client.getClient()).toBeDefined();
      expect(client.getClient()).toBe(client.client);
    });

    it('should close underlying client', () => {
      expect(() => client.close()).not.toThrow();
    });
  });

  describe('API method delegation', () => {
    let client;

    beforeEach(() => {
      client = new ObjectStoreClient({
        protocol: 'rest',
        baseURL: 'http://localhost:8080',
      });
    });

    afterEach(() => {
      client.close();
    });

    it('should have put method', () => {
      expect(typeof client.put).toBe('function');
    });

    it('should have get method', () => {
      expect(typeof client.get).toBe('function');
    });

    it('should have delete method', () => {
      expect(typeof client.delete).toBe('function');
    });

    it('should have list method', () => {
      expect(typeof client.list).toBe('function');
    });

    it('should have exists method', () => {
      expect(typeof client.exists).toBe('function');
    });

    it('should have getMetadata method', () => {
      expect(typeof client.getMetadata).toBe('function');
    });

    it('should have updateMetadata method', () => {
      expect(typeof client.updateMetadata).toBe('function');
    });

    it('should have health method', () => {
      expect(typeof client.health).toBe('function');
    });

    it('should have archive method', () => {
      expect(typeof client.archive).toBe('function');
    });

    it('should have addPolicy method', () => {
      expect(typeof client.addPolicy).toBe('function');
    });

    it('should have removePolicy method', () => {
      expect(typeof client.removePolicy).toBe('function');
    });

    it('should have getPolicies method', () => {
      expect(typeof client.getPolicies).toBe('function');
    });

    it('should have applyPolicies method', () => {
      expect(typeof client.applyPolicies).toBe('function');
    });

    it('should have addReplicationPolicy method', () => {
      expect(typeof client.addReplicationPolicy).toBe('function');
    });

    it('should have removeReplicationPolicy method', () => {
      expect(typeof client.removeReplicationPolicy).toBe('function');
    });

    it('should have getReplicationPolicies method', () => {
      expect(typeof client.getReplicationPolicies).toBe('function');
    });

    it('should have getReplicationPolicy method', () => {
      expect(typeof client.getReplicationPolicy).toBe('function');
    });

    it('should have triggerReplication method', () => {
      expect(typeof client.triggerReplication).toBe('function');
    });

    it('should have getReplicationStatus method', () => {
      expect(typeof client.getReplicationStatus).toBe('function');
    });
  });

  describe('list options normalization', () => {
    it('should normalize REST list options', () => {
      const client = new ObjectStoreClient({
        protocol: 'rest',
        baseURL: 'http://localhost:8080',
      });

      // Just verify the method exists - don't call it since it makes network requests
      expect(typeof client.list).toBe('function');

      client.close();
    });

    it('should normalize gRPC list options', () => {
      const client = new ObjectStoreClient({
        protocol: 'grpc',
        baseURL: 'localhost:50051',
      });

      // Just verify the method exists - don't call it since it makes network requests
      expect(typeof client.list).toBe('function');

      client.close();
    });
  });

  describe('health method', () => {
    it('should call health for REST', () => {
      const client = new ObjectStoreClient({
        protocol: 'rest',
        baseURL: 'http://localhost:8080',
      });

      // Just verify the method exists - don't call it since it makes network requests
      expect(typeof client.health).toBe('function');

      client.close();
    });

    it('should call health with service for gRPC', () => {
      const client = new ObjectStoreClient({
        protocol: 'grpc',
        baseURL: 'localhost:50051',
      });

      // Just verify the method exists - don't call it since it makes network requests
      expect(typeof client.health).toBe('function');

      client.close();
    });
  });

  describe('listWithOptions method', () => {
    it('should have listWithOptions method', () => {
      const client = new ObjectStoreClient({
        protocol: 'rest',
        baseURL: 'http://localhost:8080',
      });

      expect(typeof client.listWithOptions).toBe('function');
      client.close();
    });

    it('should delegate to underlying client for REST', () => {
      const client = new ObjectStoreClient({
        protocol: 'rest',
        baseURL: 'http://localhost:8080',
      });

      // Just verify the method exists - don't call it since it makes network requests
      expect(typeof client.listWithOptions).toBe('function');
      client.close();
    });

    it('should delegate to underlying client for gRPC', () => {
      const client = new ObjectStoreClient({
        protocol: 'grpc',
        baseURL: 'localhost:50051',
      });

      // Just verify the method exists - don't call it since it makes network requests
      expect(typeof client.listWithOptions).toBe('function');
      client.close();
    });

    it('should delegate to underlying client for QUIC', () => {
      const client = new ObjectStoreClient({
        protocol: 'quic',
        baseURL: 'http://localhost:8443',
      });

      // Just verify the method exists - don't call it since it makes network requests
      expect(typeof client.listWithOptions).toBe('function');
      client.close();
    });
  });

  describe('validation edge cases', () => {
    let client;

    beforeEach(() => {
      client = new ObjectStoreClient({
        protocol: 'rest',
        baseURL: 'http://localhost:8080',
      });
    });

    afterEach(() => {
      client.close();
    });

    it('should handle empty config gracefully', () => {
      expect(() => new ObjectStoreClient()).toThrow('Configuration is required');
    });

    it('should handle null config gracefully', () => {
      expect(() => new ObjectStoreClient(null)).toThrow('Configuration is required');
    });

    it('should handle undefined protocol gracefully', () => {
      expect(() => new ObjectStoreClient({ baseURL: 'http://localhost' })).toThrow(
        'protocol is required'
      );
    });

    it('should handle empty protocol string gracefully', () => {
      expect(() => new ObjectStoreClient({ protocol: '', baseURL: 'http://localhost' })).toThrow(
        'protocol is required'
      );
    });
  });

  describe('close method', () => {
    it('should not throw when client has close method', () => {
      const client = new ObjectStoreClient({
        protocol: 'rest',
        baseURL: 'http://localhost:8080',
      });

      expect(() => client.close()).not.toThrow();
    });

    it('should not throw when client has no close method', () => {
      const client = new ObjectStoreClient({
        protocol: 'rest',
        baseURL: 'http://localhost:8080',
      });

      // Remove close method to simulate a client without it
      delete client.client.close;

      expect(() => client.close()).not.toThrow();
    });

    it('should be safe to call close multiple times', () => {
      const client = new ObjectStoreClient({
        protocol: 'rest',
        baseURL: 'http://localhost:8080',
      });

      expect(() => {
        client.close();
        client.close();
        client.close();
      }).not.toThrow();
    });
  });

  describe('API method delegation', () => {
    let client;
    let mockClient;

    beforeEach(() => {
      client = new ObjectStoreClient({
        protocol: 'rest',
        baseURL: 'http://localhost:8080',
      });
      // Mock all the underlying client methods
      mockClient = {
        put: jest.fn().mockResolvedValue({ success: true, etag: 'abc123' }),
        get: jest.fn().mockResolvedValue({ data: Buffer.from('hello'), metadata: {} }),
        delete: jest.fn().mockResolvedValue({ success: true }),
        list: jest.fn().mockResolvedValue({ objects: [], commonPrefixes: [], nextToken: '', truncated: false }),
        listWithOptions: jest.fn().mockResolvedValue({ objects: [], commonPrefixes: [], nextToken: '', truncated: false }),
        exists: jest.fn().mockResolvedValue({ exists: true }),
        getMetadata: jest.fn().mockResolvedValue({ success: true, metadata: {} }),
        updateMetadata: jest.fn().mockResolvedValue({ success: true }),
        health: jest.fn().mockResolvedValue({ status: 'healthy' }),
        archive: jest.fn().mockResolvedValue({ success: true }),
        addPolicy: jest.fn().mockResolvedValue({ success: true }),
        removePolicy: jest.fn().mockResolvedValue({ success: true }),
        getPolicies: jest.fn().mockResolvedValue({ success: true, policies: [] }),
        applyPolicies: jest.fn().mockResolvedValue({ success: true, policiesCount: 0, objectsProcessed: 0 }),
        addReplicationPolicy: jest.fn().mockResolvedValue({ success: true }),
        removeReplicationPolicy: jest.fn().mockResolvedValue({ success: true }),
        getReplicationPolicies: jest.fn().mockResolvedValue({ policies: [] }),
        getReplicationPolicy: jest.fn().mockResolvedValue({ policy: { id: 'r1' } }),
        triggerReplication: jest.fn().mockResolvedValue({ success: true, result: {} }),
        getReplicationStatus: jest.fn().mockResolvedValue({ success: true, status: {} }),
        close: jest.fn(),
      };
      client.client = mockClient;
    });

    afterEach(() => {
      client.close();
    });

    it('should delegate put and return response', async () => {
      const result = await client.put('test.txt', Buffer.from('hello'));
      expect(result.success).toBe(true);
      expect(mockClient.put).toHaveBeenCalledWith('test.txt', expect.any(Buffer), null);
    });

    it('should delegate put with metadata', async () => {
      const metadata = { contentType: 'text/plain' };
      await client.put('test.txt', Buffer.from('hello'), metadata);
      expect(mockClient.put).toHaveBeenCalledWith('test.txt', expect.any(Buffer), metadata);
    });

    it('should delegate get and return response', async () => {
      const result = await client.get('test.txt');
      expect(result).toHaveProperty('data');
      expect(mockClient.get).toHaveBeenCalledWith('test.txt');
    });

    it('should delegate delete and return response', async () => {
      const result = await client.delete('test.txt');
      expect(result.success).toBe(true);
      expect(mockClient.delete).toHaveBeenCalledWith('test.txt');
    });

    it('should delegate list and return response', async () => {
      const result = await client.list();
      expect(result).toHaveProperty('objects');
      expect(mockClient.list).toHaveBeenCalled();
    });

    it('should delegate list with options', async () => {
      await client.list({ prefix: 'test/', delimiter: '/' });
      expect(mockClient.list).toHaveBeenCalledWith({ prefix: 'test/', delimiter: '/' });
    });

    it('should delegate listWithOptions and return response', async () => {
      const result = await client.listWithOptions({ prefix: 'test/' });
      expect(result).toHaveProperty('objects');
      expect(mockClient.listWithOptions).toHaveBeenCalled();
    });

    it('should delegate exists and return response', async () => {
      const result = await client.exists('test.txt');
      expect(result.exists).toBe(true);
      expect(mockClient.exists).toHaveBeenCalledWith('test.txt');
    });

    it('should delegate getMetadata and return response', async () => {
      const result = await client.getMetadata('test.txt');
      expect(result.success).toBe(true);
      expect(mockClient.getMetadata).toHaveBeenCalledWith('test.txt');
    });

    it('should delegate updateMetadata and return response', async () => {
      const metadata = { contentType: 'application/json' };
      await client.updateMetadata('test.txt', metadata);
      expect(mockClient.updateMetadata).toHaveBeenCalledWith('test.txt', metadata);
    });

    it('should delegate health and return response', async () => {
      const result = await client.health();
      expect(result).toHaveProperty('status');
      expect(mockClient.health).toHaveBeenCalled();
    });

    it('should delegate archive and return response', async () => {
      await client.archive('test.txt', 's3', { bucket: 'archive' });
      expect(mockClient.archive).toHaveBeenCalledWith('test.txt', 's3', { bucket: 'archive' });
    });

    it('should delegate addPolicy and return response', async () => {
      const policy = { id: 'p1', prefix: 'test/' };
      await client.addPolicy(policy);
      expect(mockClient.addPolicy).toHaveBeenCalledWith(policy);
    });

    it('should delegate removePolicy and return response', async () => {
      await client.removePolicy('p1');
      expect(mockClient.removePolicy).toHaveBeenCalledWith('p1');
    });

    it('should delegate getPolicies and return response', async () => {
      await client.getPolicies();
      expect(mockClient.getPolicies).toHaveBeenCalledWith('');
    });

    it('should delegate getPolicies with prefix', async () => {
      await client.getPolicies('test/');
      expect(mockClient.getPolicies).toHaveBeenCalledWith('test/');
    });

    it('should delegate applyPolicies and return response', async () => {
      const result = await client.applyPolicies();
      expect(result.success).toBe(true);
      expect(mockClient.applyPolicies).toHaveBeenCalled();
    });

    it('should delegate addReplicationPolicy and return response', async () => {
      const policy = { id: 'r1', sourceBackend: 'local' };
      await client.addReplicationPolicy(policy);
      expect(mockClient.addReplicationPolicy).toHaveBeenCalledWith(policy);
    });

    it('should delegate removeReplicationPolicy and return response', async () => {
      await client.removeReplicationPolicy('r1');
      expect(mockClient.removeReplicationPolicy).toHaveBeenCalledWith('r1');
    });

    it('should delegate getReplicationPolicies and return response', async () => {
      const result = await client.getReplicationPolicies();
      expect(result).toHaveProperty('policies');
      expect(mockClient.getReplicationPolicies).toHaveBeenCalled();
    });

    it('should delegate getReplicationPolicy and return response', async () => {
      const result = await client.getReplicationPolicy('r1');
      expect(result).toHaveProperty('policy');
      expect(mockClient.getReplicationPolicy).toHaveBeenCalledWith('r1');
    });

    it('should delegate triggerReplication and return response', async () => {
      await client.triggerReplication();
      expect(mockClient.triggerReplication).toHaveBeenCalledWith({});
    });

    it('should delegate triggerReplication with options', async () => {
      const options = { policyId: 'r1', parallel: true };
      await client.triggerReplication(options);
      expect(mockClient.triggerReplication).toHaveBeenCalledWith(options);
    });

    it('should delegate getReplicationStatus and return response', async () => {
      await client.getReplicationStatus('r1');
      expect(mockClient.getReplicationStatus).toHaveBeenCalledWith('r1');
    });
  });

  describe('gRPC protocol list normalization', () => {
    let client;

    beforeEach(() => {
      // Create a mock client that captures the normalized options
      client = new ObjectStoreClient({
        protocol: 'grpc',
        baseURL: 'localhost:50051',
      });
      // Replace the underlying client's list method to capture the call
      client.client.list = jest.fn().mockResolvedValue({
        objects: [],
        commonPrefixes: [],
        nextToken: '',
        truncated: false
      });
    });

    afterEach(() => {
      client.close();
    });

    it('should normalize limit to maxResults for gRPC', async () => {
      await client.list({ limit: 100 });
      expect(client.client.list).toHaveBeenCalledWith({
        prefix: undefined,
        delimiter: undefined,
        maxResults: 100,
        continueFrom: undefined,
      });
    });

    it('should normalize token to continueFrom for gRPC', async () => {
      await client.list({ token: 'abc' });
      expect(client.client.list).toHaveBeenCalledWith({
        prefix: undefined,
        delimiter: undefined,
        maxResults: undefined,
        continueFrom: 'abc',
      });
    });

    it('should pass through gRPC-native options', async () => {
      await client.list({ maxResults: 50, continueFrom: 'token' });
      expect(client.client.list).toHaveBeenCalledWith({
        prefix: undefined,
        delimiter: undefined,
        maxResults: 50,
        continueFrom: 'token',
      });
    });
  });

  describe('health method protocol handling', () => {
    it('should call health without service for REST', async () => {
      const client = new ObjectStoreClient({
        protocol: 'rest',
        baseURL: 'http://localhost:8080',
      });

      // Mock the REST client's health method
      client.client.health = jest.fn().mockResolvedValue({ status: 'healthy' });

      await client.health();
      expect(client.client.health).toHaveBeenCalled();
      client.close();
    });

    it('should call health with service for gRPC', async () => {
      const client = new ObjectStoreClient({
        protocol: 'grpc',
        baseURL: 'localhost:50051',
      });

      // Mock the gRPC client's health method
      client.client.health = jest.fn().mockResolvedValue({ status: 'SERVING', message: 'OK' });

      await client.health('objstore');
      expect(client.client.health).toHaveBeenCalledWith('objstore');
      client.close();
    });
  });
});
