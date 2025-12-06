import { ObjectStoreClient } from '../../src/client';
import { RestClient } from '../../src/clients/rest-client';
import { GrpcClient } from '../../src/clients/grpc-client';
import { QuicClient } from '../../src/clients/quic-client';

jest.mock('../../src/clients/rest-client');
jest.mock('../../src/clients/grpc-client');
jest.mock('../../src/clients/quic-client');

describe('ObjectStoreClient', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  describe('constructor', () => {
    it('should create REST client when protocol is rest', () => {
      const config = {
        protocol: 'rest' as const,
        rest: { baseUrl: 'http://localhost:8080' },
      };

      const client = new ObjectStoreClient(config);

      expect(RestClient).toHaveBeenCalledWith(config.rest);
      expect(client).toBeInstanceOf(ObjectStoreClient);
    });

    it('should create gRPC client when protocol is grpc', () => {
      const config = {
        protocol: 'grpc' as const,
        grpc: { address: 'localhost:50051' },
      };

      const client = new ObjectStoreClient(config);

      expect(GrpcClient).toHaveBeenCalledWith(config.grpc);
      expect(client).toBeInstanceOf(ObjectStoreClient);
    });

    it('should create QUIC client when protocol is quic', () => {
      const config = {
        protocol: 'quic' as const,
        quic: { address: 'localhost:8443' },
      };

      const client = new ObjectStoreClient(config);

      expect(QuicClient).toHaveBeenCalledWith(config.quic);
      expect(client).toBeInstanceOf(ObjectStoreClient);
    });

    it('should throw error when REST config is missing', () => {
      const config = {
        protocol: 'rest' as const,
      };

      expect(() => new ObjectStoreClient(config)).toThrow(
        'REST configuration is required when using REST protocol'
      );
    });

    it('should throw error when gRPC config is missing', () => {
      const config = {
        protocol: 'grpc' as const,
      };

      expect(() => new ObjectStoreClient(config)).toThrow(
        'gRPC configuration is required when using gRPC protocol'
      );
    });

    it('should throw error when QUIC config is missing', () => {
      const config = {
        protocol: 'quic' as const,
      };

      expect(() => new ObjectStoreClient(config)).toThrow(
        'QUIC configuration is required when using QUIC protocol'
      );
    });

    it('should throw error for unsupported protocol', () => {
      const config = {
        protocol: 'invalid' as any,
      };

      expect(() => new ObjectStoreClient(config)).toThrow('Unsupported protocol: invalid');
    });
  });

  describe('method delegation', () => {
    let client: ObjectStoreClient;
    let mockClient: any;

    beforeEach(() => {
      mockClient = {
        put: jest.fn().mockResolvedValue({ success: true }),
        get: jest.fn().mockResolvedValue({ data: Buffer.from('test') }),
        delete: jest.fn().mockResolvedValue({ success: true }),
        list: jest.fn().mockResolvedValue({ objects: [], truncated: false }),
        exists: jest.fn().mockResolvedValue({ exists: true }),
        getMetadata: jest.fn().mockResolvedValue({ success: true }),
        updateMetadata: jest.fn().mockResolvedValue({ success: true }),
        health: jest.fn().mockResolvedValue({ status: 1 }),
        archive: jest.fn().mockResolvedValue({ success: true }),
        addPolicy: jest.fn().mockResolvedValue({ success: true }),
        removePolicy: jest.fn().mockResolvedValue({ success: true }),
        getPolicies: jest.fn().mockResolvedValue({ policies: [], success: true }),
        applyPolicies: jest.fn().mockResolvedValue({ success: true, policiesCount: 0, objectsProcessed: 0 }),
        addReplicationPolicy: jest.fn().mockResolvedValue({ success: true }),
        removeReplicationPolicy: jest.fn().mockResolvedValue({ success: true }),
        getReplicationPolicies: jest.fn().mockResolvedValue({ policies: [] }),
        getReplicationPolicy: jest.fn().mockResolvedValue({ policy: undefined }),
        triggerReplication: jest.fn().mockResolvedValue({ success: true }),
        getReplicationStatus: jest.fn().mockResolvedValue({ success: true }),
        close: jest.fn().mockResolvedValue(undefined),
      };

      (RestClient as jest.Mock).mockImplementation(() => mockClient);

      client = new ObjectStoreClient({
        protocol: 'rest',
        rest: { baseUrl: 'http://localhost:8080' },
      });
    });

    it('should delegate put calls', async () => {
      const request = { key: 'test', data: Buffer.from('data') };
      await client.put(request);
      expect(mockClient.put).toHaveBeenCalledWith(request);
    });

    it('should delegate get calls', async () => {
      const request = { key: 'test' };
      await client.get(request);
      expect(mockClient.get).toHaveBeenCalledWith(request);
    });

    it('should delegate delete calls', async () => {
      const request = { key: 'test' };
      await client.delete(request);
      expect(mockClient.delete).toHaveBeenCalledWith(request);
    });

    it('should delegate list calls', async () => {
      const request = { prefix: 'test/' };
      await client.list(request);
      expect(mockClient.list).toHaveBeenCalledWith(request);
    });

    it('should delegate exists calls', async () => {
      const request = { key: 'test' };
      await client.exists(request);
      expect(mockClient.exists).toHaveBeenCalledWith(request);
    });

    it('should delegate getMetadata calls', async () => {
      const request = { key: 'test' };
      await client.getMetadata(request);
      expect(mockClient.getMetadata).toHaveBeenCalledWith(request);
    });

    it('should delegate updateMetadata calls', async () => {
      const request = { key: 'test', metadata: { contentType: 'text/plain' } };
      await client.updateMetadata(request);
      expect(mockClient.updateMetadata).toHaveBeenCalledWith(request);
    });

    it('should delegate health calls', async () => {
      await client.health();
      expect(mockClient.health).toHaveBeenCalledWith({});
    });

    it('should delegate archive calls', async () => {
      const request = { key: 'test', destinationType: 'glacier' };
      await client.archive(request);
      expect(mockClient.archive).toHaveBeenCalledWith(request);
    });

    it('should delegate lifecycle policy calls', async () => {
      const policy = { id: 'p1', prefix: 'logs/', retentionSeconds: 86400, action: 'delete' };

      await client.addPolicy({ policy });
      expect(mockClient.addPolicy).toHaveBeenCalledWith({ policy });

      await client.removePolicy({ id: 'p1' });
      expect(mockClient.removePolicy).toHaveBeenCalledWith({ id: 'p1' });

      await client.getPolicies();
      expect(mockClient.getPolicies).toHaveBeenCalledWith({});

      await client.applyPolicies();
      expect(mockClient.applyPolicies).toHaveBeenCalledWith({});
    });

    it('should delegate replication policy calls', async () => {
      const policy = {
        id: 'r1',
        sourceBackend: 's3',
        sourceSettings: {},
        sourcePrefix: '',
        destinationBackend: 'gcs',
        destinationSettings: {},
        checkIntervalSeconds: 3600,
        enabled: true,
        replicationMode: 0,
      };

      await client.addReplicationPolicy({ policy });
      expect(mockClient.addReplicationPolicy).toHaveBeenCalledWith({ policy });

      await client.removeReplicationPolicy({ id: 'r1' });
      expect(mockClient.removeReplicationPolicy).toHaveBeenCalledWith({ id: 'r1' });

      await client.getReplicationPolicies();
      expect(mockClient.getReplicationPolicies).toHaveBeenCalledWith({});

      await client.getReplicationPolicy({ id: 'r1' });
      expect(mockClient.getReplicationPolicy).toHaveBeenCalledWith({ id: 'r1' });

      await client.triggerReplication({ policyId: 'r1' });
      expect(mockClient.triggerReplication).toHaveBeenCalledWith({ policyId: 'r1' });

      await client.getReplicationStatus({ id: 'r1' });
      expect(mockClient.getReplicationStatus).toHaveBeenCalledWith({ id: 'r1' });
    });

    it('should delegate close calls', async () => {
      await client.close();
      expect(mockClient.close).toHaveBeenCalled();
    });
  });
});
