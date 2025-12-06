import { GrpcClient } from '../../src/clients/grpc-client';
import { HealthStatus } from '../../src/types';

// Mock grpc-js module
jest.mock('@grpc/grpc-js', () => {
  const mockClient = {
    put: jest.fn(),
    get: jest.fn(),
    delete: jest.fn(),
    list: jest.fn(),
    exists: jest.fn(),
    getMetadata: jest.fn(),
    updateMetadata: jest.fn(),
    health: jest.fn(),
    archive: jest.fn(),
    addPolicy: jest.fn(),
    removePolicy: jest.fn(),
    getPolicies: jest.fn(),
    applyPolicies: jest.fn(),
    addReplicationPolicy: jest.fn(),
    removeReplicationPolicy: jest.fn(),
    getReplicationPolicies: jest.fn(),
    getReplicationPolicy: jest.fn(),
    triggerReplication: jest.fn(),
    getReplicationStatus: jest.fn(),
    close: jest.fn(),
  };

  return {
    credentials: {
      createInsecure: jest.fn(),
      createSsl: jest.fn(),
    },
    loadPackageDefinition: jest.fn(() => ({
      objstore: {
        v1: {
          ObjectStore: jest.fn(() => mockClient),
        },
      },
    })),
    ServiceError: class ServiceError extends Error {},
  };
});

jest.mock('@grpc/proto-loader', () => ({
  loadSync: jest.fn(() => ({})),
}));

describe('GrpcClient', () => {
  let client: GrpcClient;
  let mockGrpcClient: any;

  beforeEach(() => {
    const grpc = require('@grpc/grpc-js');
    client = new GrpcClient({ address: 'localhost:50051', secure: false });
    mockGrpcClient = (grpc.loadPackageDefinition({}) as any).objstore.v1.ObjectStore();
  });

  describe('put', () => {
    it('should upload an object successfully', async () => {
      mockGrpcClient.put.mockImplementation((_req: any, callback: any) => {
        callback(null, { success: true, etag: 'test-etag' });
      });

      const result = await client.put({
        key: 'test-key',
        data: Buffer.from('test data'),
      });

      expect(result.success).toBe(true);
      expect(result.etag).toBe('test-etag');
    });

    it('should handle errors', async () => {
      mockGrpcClient.put.mockImplementation((_req: any, callback: any) => {
        callback(new Error('gRPC error'));
      });

      await expect(
        client.put({ key: 'test-key', data: Buffer.from('data') })
      ).rejects.toThrow('gRPC error');
    });
  });

  describe('get', () => {
    it('should retrieve an object successfully', async () => {
      const mockStream: any = {
        on: jest.fn((event: string, handler: any): any => {
          if (event === 'data') {
            handler({ data: Buffer.from('test'), metadata: { contentType: 'text/plain' } });
          } else if (event === 'end') {
            handler();
          }
          return mockStream;
        }),
      };

      mockGrpcClient.get.mockReturnValue(mockStream);

      const result = await client.get({ key: 'test-key' });

      expect(result.data.toString()).toBe('test');
    });

    it('should handle streaming errors', async () => {
      const mockStream: any = {
        on: jest.fn((event: string, handler: any): any => {
          if (event === 'error') {
            handler(new Error('Stream error'));
          }
          return mockStream;
        }),
      };

      mockGrpcClient.get.mockReturnValue(mockStream);

      await expect(client.get({ key: 'test-key' })).rejects.toThrow('Stream error');
    });
  });

  describe('delete', () => {
    it('should delete an object successfully', async () => {
      mockGrpcClient.delete.mockImplementation((_req: any, callback: any) => {
        callback(null, { success: true });
      });

      const result = await client.delete({ key: 'test-key' });

      expect(result.success).toBe(true);
    });
  });

  describe('list', () => {
    it('should list objects', async () => {
      mockGrpcClient.list.mockImplementation((_req: any, callback: any) => {
        callback(null, {
          objects: [{ key: 'file1.txt', metadata: {} }],
          commonPrefixes: [],
          truncated: false,
        });
      });

      const result = await client.list({ prefix: 'test/' });

      expect(result.objects).toHaveLength(1);
    });
  });

  describe('exists', () => {
    it('should check if object exists', async () => {
      mockGrpcClient.exists.mockImplementation((_req: any, callback: any) => {
        callback(null, { exists: true });
      });

      const result = await client.exists({ key: 'test-key' });

      expect(result.exists).toBe(true);
    });
  });

  describe('getMetadata', () => {
    it('should retrieve object metadata', async () => {
      mockGrpcClient.getMetadata.mockImplementation((_req: any, callback: any) => {
        callback(null, {
          success: true,
          metadata: { contentType: 'text/plain', size: 100 },
        });
      });

      const result = await client.getMetadata({ key: 'test-key' });

      expect(result.success).toBe(true);
      expect(result.metadata?.contentType).toBe('text/plain');
    });
  });

  describe('updateMetadata', () => {
    it('should update object metadata', async () => {
      mockGrpcClient.updateMetadata.mockImplementation((_req: any, callback: any) => {
        callback(null, { success: true });
      });

      const result = await client.updateMetadata({
        key: 'test-key',
        metadata: { contentType: 'application/json' },
      });

      expect(result.success).toBe(true);
    });
  });

  describe('health', () => {
    it('should check health', async () => {
      mockGrpcClient.health.mockImplementation((_req: any, callback: any) => {
        callback(null, { status: HealthStatus.SERVING });
      });

      const result = await client.health();

      expect(result.status).toBe(HealthStatus.SERVING);
    });
  });

  describe('archive', () => {
    it('should archive an object', async () => {
      mockGrpcClient.archive.mockImplementation((_req: any, callback: any) => {
        callback(null, { success: true });
      });

      const result = await client.archive({
        key: 'test-key',
        destinationType: 'glacier',
      });

      expect(result.success).toBe(true);
    });
  });

  describe('lifecycle policies', () => {
    it('should add a policy', async () => {
      mockGrpcClient.addPolicy.mockImplementation((_req: any, callback: any) => {
        callback(null, { success: true });
      });

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
      mockGrpcClient.removePolicy.mockImplementation((_req: any, callback: any) => {
        callback(null, { success: true });
      });

      const result = await client.removePolicy({ id: 'p1' });

      expect(result.success).toBe(true);
    });

    it('should get policies', async () => {
      mockGrpcClient.getPolicies.mockImplementation((_req: any, callback: any) => {
        callback(null, {
          policies: [{ id: 'p1', prefix: 'logs/', retentionSeconds: 86400, action: 'delete' }],
          success: true,
        });
      });

      const result = await client.getPolicies();

      expect(result.policies).toHaveLength(1);
    });

    it('should apply policies', async () => {
      mockGrpcClient.applyPolicies.mockImplementation((_req: any, callback: any) => {
        callback(null, { success: true, policiesCount: 5, objectsProcessed: 100 });
      });

      const result = await client.applyPolicies();

      expect(result.success).toBe(true);
      expect(result.policiesCount).toBe(5);
    });
  });

  describe('replication policies', () => {
    it('should add a replication policy', async () => {
      mockGrpcClient.addReplicationPolicy.mockImplementation((_req: any, callback: any) => {
        callback(null, { success: true });
      });

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
      mockGrpcClient.removeReplicationPolicy.mockImplementation((_req: any, callback: any) => {
        callback(null, { success: true });
      });

      const result = await client.removeReplicationPolicy({ id: 'r1' });

      expect(result.success).toBe(true);
    });

    it('should get replication policies', async () => {
      mockGrpcClient.getReplicationPolicies.mockImplementation((_req: any, callback: any) => {
        callback(null, {
          policies: [
            {
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
          ],
        });
      });

      const result = await client.getReplicationPolicies();

      expect(result.policies).toHaveLength(1);
    });

    it('should get a specific replication policy', async () => {
      mockGrpcClient.getReplicationPolicy.mockImplementation((_req: any, callback: any) => {
        callback(null, {
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
      });

      const result = await client.getReplicationPolicy({ id: 'r1' });

      expect(result.policy?.id).toBe('r1');
    });

    it('should trigger replication', async () => {
      mockGrpcClient.triggerReplication.mockImplementation((_req: any, callback: any) => {
        callback(null, {
          success: true,
          result: {
            policyId: 'r1',
            synced: 10,
            deleted: 2,
            failed: 0,
            bytesTotal: 1024,
            durationMs: 500,
            errors: [],
          },
        });
      });

      const result = await client.triggerReplication({ policyId: 'r1' });

      expect(result.success).toBe(true);
      expect(result.result?.synced).toBe(10);
    });

    it('should get replication status', async () => {
      mockGrpcClient.getReplicationStatus.mockImplementation((_req: any, callback: any) => {
        callback(null, {
          success: true,
          status: {
            policyId: 'r1',
            sourceBackend: 's3',
            destinationBackend: 'gcs',
            enabled: true,
            totalObjectsSynced: 100,
            totalObjectsDeleted: 10,
            totalBytesSynced: 1048576,
            totalErrors: 0,
            averageSyncDurationMs: 500,
            syncCount: 5,
          },
        });
      });

      const result = await client.getReplicationStatus({ id: 'r1' });

      expect(result.success).toBe(true);
      expect(result.status?.totalObjectsSynced).toBe(100);
    });
  });

  describe('close', () => {
    it('should close the client', async () => {
      await expect(client.close()).resolves.toBeUndefined();
    });
  });

  describe('error handling', () => {
    it('should handle delete errors', async () => {
      mockGrpcClient.delete.mockImplementation((_req: any, callback: any) => {
        callback(new Error('Delete failed'));
      });

      await expect(client.delete({ key: 'test' })).rejects.toThrow('Delete failed');
    });

    it('should handle list errors', async () => {
      mockGrpcClient.list.mockImplementation((_req: any, callback: any) => {
        callback(new Error('List failed'));
      });

      await expect(client.list()).rejects.toThrow('List failed');
    });

    it('should handle exists errors', async () => {
      mockGrpcClient.exists.mockImplementation((_req: any, callback: any) => {
        callback(new Error('Exists failed'));
      });

      await expect(client.exists({ key: 'test' })).rejects.toThrow('Exists failed');
    });

    it('should handle getMetadata errors', async () => {
      mockGrpcClient.getMetadata.mockImplementation((_req: any, callback: any) => {
        callback(new Error('GetMetadata failed'));
      });

      await expect(client.getMetadata({ key: 'test' })).rejects.toThrow('GetMetadata failed');
    });

    it('should handle updateMetadata errors', async () => {
      mockGrpcClient.updateMetadata.mockImplementation((_req: any, callback: any) => {
        callback(new Error('UpdateMetadata failed'));
      });

      await expect(
        client.updateMetadata({ key: 'test', metadata: {} })
      ).rejects.toThrow('UpdateMetadata failed');
    });

    it('should handle health errors', async () => {
      mockGrpcClient.health.mockImplementation((_req: any, callback: any) => {
        callback(new Error('Health failed'));
      });

      await expect(client.health()).rejects.toThrow('Health failed');
    });

    it('should handle archive errors', async () => {
      mockGrpcClient.archive.mockImplementation((_req: any, callback: any) => {
        callback(new Error('Archive failed'));
      });

      await expect(
        client.archive({ key: 'test', destinationType: 'glacier' })
      ).rejects.toThrow('Archive failed');
    });

    it('should handle addPolicy errors', async () => {
      mockGrpcClient.addPolicy.mockImplementation((_req: any, callback: any) => {
        callback(new Error('AddPolicy failed'));
      });

      await expect(
        client.addPolicy({
          policy: { id: 'p1', prefix: 'logs/', retentionSeconds: 86400, action: 'delete' },
        })
      ).rejects.toThrow('AddPolicy failed');
    });

    it('should handle removePolicy errors', async () => {
      mockGrpcClient.removePolicy.mockImplementation((_req: any, callback: any) => {
        callback(new Error('RemovePolicy failed'));
      });

      await expect(client.removePolicy({ id: 'p1' })).rejects.toThrow('RemovePolicy failed');
    });

    it('should handle getPolicies errors', async () => {
      mockGrpcClient.getPolicies.mockImplementation((_req: any, callback: any) => {
        callback(new Error('GetPolicies failed'));
      });

      await expect(client.getPolicies()).rejects.toThrow('GetPolicies failed');
    });

    it('should handle applyPolicies errors', async () => {
      mockGrpcClient.applyPolicies.mockImplementation((_req: any, callback: any) => {
        callback(new Error('ApplyPolicies failed'));
      });

      await expect(client.applyPolicies()).rejects.toThrow('ApplyPolicies failed');
    });

    it('should handle addReplicationPolicy errors', async () => {
      mockGrpcClient.addReplicationPolicy.mockImplementation((_req: any, callback: any) => {
        callback(new Error('AddReplicationPolicy failed'));
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
            checkIntervalSeconds: 3600,
            enabled: true,
            replicationMode: 0,
          },
        })
      ).rejects.toThrow('AddReplicationPolicy failed');
    });

    it('should handle removeReplicationPolicy errors', async () => {
      mockGrpcClient.removeReplicationPolicy.mockImplementation((_req: any, callback: any) => {
        callback(new Error('RemoveReplicationPolicy failed'));
      });

      await expect(client.removeReplicationPolicy({ id: 'r1' })).rejects.toThrow(
        'RemoveReplicationPolicy failed'
      );
    });

    it('should handle getReplicationPolicies errors', async () => {
      mockGrpcClient.getReplicationPolicies.mockImplementation((_req: any, callback: any) => {
        callback(new Error('GetReplicationPolicies failed'));
      });

      await expect(client.getReplicationPolicies()).rejects.toThrow(
        'GetReplicationPolicies failed'
      );
    });

    it('should handle getReplicationPolicy errors', async () => {
      mockGrpcClient.getReplicationPolicy.mockImplementation((_req: any, callback: any) => {
        callback(new Error('GetReplicationPolicy failed'));
      });

      await expect(client.getReplicationPolicy({ id: 'r1' })).rejects.toThrow(
        'GetReplicationPolicy failed'
      );
    });

    it('should handle triggerReplication errors', async () => {
      mockGrpcClient.triggerReplication.mockImplementation((_req: any, callback: any) => {
        callback(new Error('TriggerReplication failed'));
      });

      await expect(client.triggerReplication({ policyId: 'r1' })).rejects.toThrow(
        'TriggerReplication failed'
      );
    });

    it('should handle getReplicationStatus errors', async () => {
      mockGrpcClient.getReplicationStatus.mockImplementation((_req: any, callback: any) => {
        callback(new Error('GetReplicationStatus failed'));
      });

      await expect(client.getReplicationStatus({ id: 'r1' })).rejects.toThrow(
        'GetReplicationStatus failed'
      );
    });
  });
});
