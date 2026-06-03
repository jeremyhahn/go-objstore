import { ObjectStoreClient } from '../../src/client';
import { RestClient } from '../../src/clients/rest-client';
import { GrpcClient } from '../../src/clients/grpc-client';
import { QuicClient } from '../../src/clients/quic-client';
import { McpClient } from '../../src/clients/mcp-client';
import { UnixClient } from '../../src/clients/unix-client';
import { HealthStatus } from '../../src/types';

jest.mock('../../src/clients/rest-client');
jest.mock('../../src/clients/grpc-client');
jest.mock('../../src/clients/quic-client');
jest.mock('../../src/clients/mcp-client');
jest.mock('../../src/clients/unix-client');

const MockRestClient = RestClient as jest.MockedClass<typeof RestClient>;
const MockGrpcClient = GrpcClient as jest.MockedClass<typeof GrpcClient>;
const MockQuicClient = QuicClient as jest.MockedClass<typeof QuicClient>;
const MockMcpClient = McpClient as jest.MockedClass<typeof McpClient>;
const MockUnixClient = UnixClient as jest.MockedClass<typeof UnixClient>;

/**
 * Unified client tests: construction per protocol, delegation of a
 * representative call to the right protocol client, and close().
 */
describe('ObjectStoreClient', () => {
  afterEach(() => {
    jest.clearAllMocks();
  });

  describe('constructor', () => {
    it('unified_constructs_rest', () => {
      new ObjectStoreClient({ protocol: 'rest', rest: { baseUrl: 'http://localhost:8080' } });
      expect(MockRestClient).toHaveBeenCalledWith({ baseUrl: 'http://localhost:8080' });
    });

    it('unified_constructs_grpc', () => {
      new ObjectStoreClient({ protocol: 'grpc', grpc: { address: 'localhost:50051' } });
      expect(MockGrpcClient).toHaveBeenCalledWith({ address: 'localhost:50051' });
    });

    it('unified_constructs_quic', () => {
      new ObjectStoreClient({ protocol: 'quic', quic: { address: 'localhost:8443' } });
      expect(MockQuicClient).toHaveBeenCalledWith({ address: 'localhost:8443' });
    });

    it('unified_rest_requires_config', () => {
      expect(() => new ObjectStoreClient({ protocol: 'rest' })).toThrow(
        'REST configuration is required'
      );
    });

    it('unified_grpc_requires_config', () => {
      expect(() => new ObjectStoreClient({ protocol: 'grpc' })).toThrow(
        'gRPC configuration is required'
      );
    });

    it('unified_quic_requires_config', () => {
      expect(() => new ObjectStoreClient({ protocol: 'quic' })).toThrow(
        'QUIC configuration is required'
      );
    });

    it('unified_constructs_mcp', () => {
      new ObjectStoreClient({ protocol: 'mcp', mcp: { baseUrl: 'http://localhost:9090' } });
      expect(MockMcpClient).toHaveBeenCalledWith({ baseUrl: 'http://localhost:9090' });
    });

    it('unified_mcp_requires_config', () => {
      expect(() => new ObjectStoreClient({ protocol: 'mcp' })).toThrow(
        'MCP configuration is required'
      );
    });

    it('unified_constructs_unix', () => {
      new ObjectStoreClient({
        protocol: 'unix',
        unix: { socketPath: '/tmp/objstore.sock' },
      });
      expect(MockUnixClient).toHaveBeenCalledWith({ socketPath: '/tmp/objstore.sock' });
    });

    it('unified_unix_requires_config', () => {
      expect(() => new ObjectStoreClient({ protocol: 'unix' })).toThrow(
        'Unix configuration is required'
      );
    });

    it('unified_unsupported_protocol', () => {
      expect(() => new ObjectStoreClient({ protocol: 'invalid' as any })).toThrow(
        'Unsupported protocol'
      );
    });
  });

  describe('delegation', () => {
    it('unified_delegates_rest', async () => {
      const health = jest.fn().mockResolvedValue({ status: HealthStatus.SERVING });
      MockRestClient.prototype.health = health as any;

      const client = new ObjectStoreClient({
        protocol: 'rest',
        rest: { baseUrl: 'http://localhost:8080' },
      });
      const resp = await client.health();

      expect(health).toHaveBeenCalled();
      expect(resp.status).toBe(HealthStatus.SERVING);
    });

    it('unified_delegates_grpc', async () => {
      const put = jest.fn().mockResolvedValue({ success: true });
      MockGrpcClient.prototype.put = put as any;

      const client = new ObjectStoreClient({
        protocol: 'grpc',
        grpc: { address: 'localhost:50051' },
      });
      const req = { key: 'k', data: Buffer.from('d') };
      const resp = await client.put(req);

      expect(put).toHaveBeenCalledWith(req);
      expect(resp.success).toBe(true);
    });

    it('unified_delegates_quic', async () => {
      const health = jest.fn().mockResolvedValue({ status: HealthStatus.SERVING });
      MockQuicClient.prototype.health = health as any;

      const client = new ObjectStoreClient({
        protocol: 'quic',
        quic: { address: 'localhost:8443' },
      });
      const resp = await client.health();

      expect(health).toHaveBeenCalled();
      expect(resp.status).toBe(HealthStatus.SERVING);
    });

    it('unified_delegates_mcp', async () => {
      const health = jest.fn().mockResolvedValue({ status: HealthStatus.SERVING });
      MockMcpClient.prototype.health = health as any;

      const client = new ObjectStoreClient({
        protocol: 'mcp',
        mcp: { baseUrl: 'http://localhost:9090' },
      });
      const resp = await client.health();

      expect(health).toHaveBeenCalled();
      expect(resp.status).toBe(HealthStatus.SERVING);
    });

    it('unified_delegates_unix', async () => {
      const health = jest.fn().mockResolvedValue({ status: HealthStatus.SERVING });
      MockUnixClient.prototype.health = health as any;

      const client = new ObjectStoreClient({
        protocol: 'unix',
        unix: { socketPath: '/tmp/objstore.sock' },
      });
      const resp = await client.health();

      expect(health).toHaveBeenCalled();
      expect(resp.status).toBe(HealthStatus.SERVING);
    });

    it('unified_delegates_all_ops', async () => {
      // Each unified method forwards to the underlying protocol client.
      const stub: any = {};
      const methods = [
        'put',
        'get',
        'delete',
        'list',
        'exists',
        'getMetadata',
        'updateMetadata',
        'health',
        'archive',
        'addPolicy',
        'removePolicy',
        'getPolicies',
        'applyPolicies',
        'addReplicationPolicy',
        'removeReplicationPolicy',
        'getReplicationPolicies',
        'getReplicationPolicy',
        'triggerReplication',
        'getReplicationStatus',
        'close',
      ];
      for (const m of methods) {
        stub[m] = jest.fn().mockResolvedValue({ ok: true });
        (MockRestClient.prototype as any)[m] = stub[m];
      }

      const client = new ObjectStoreClient({
        protocol: 'rest',
        rest: { baseUrl: 'http://localhost:8080' },
      });

      const arg: any = { key: 'k', id: 'i', policyId: 'p', data: Buffer.from('d'), metadata: {}, policy: { id: 'p' } };
      await (client as any).put(arg);
      await (client as any).get(arg);
      await (client as any).delete(arg);
      await (client as any).list(arg);
      await (client as any).exists(arg);
      await (client as any).getMetadata(arg);
      await (client as any).updateMetadata(arg);
      await (client as any).health(arg);
      await (client as any).archive(arg);
      await (client as any).addPolicy(arg);
      await (client as any).removePolicy(arg);
      await (client as any).getPolicies(arg);
      await (client as any).applyPolicies(arg);
      await (client as any).addReplicationPolicy(arg);
      await (client as any).removeReplicationPolicy(arg);
      await (client as any).getReplicationPolicies(arg);
      await (client as any).getReplicationPolicy(arg);
      await (client as any).triggerReplication(arg);
      await (client as any).getReplicationStatus(arg);

      for (const m of methods.filter((x) => x !== 'close')) {
        expect(stub[m]).toHaveBeenCalledWith(arg);
      }
    });

    it('unified_delegates_optional_arg_defaults', async () => {
      // The methods with an optional request argument default it to {} and
      // forward that. Calling them with no argument exercises those defaults.
      const stub: any = {};
      for (const m of ['list', 'getPolicies', 'applyPolicies', 'getReplicationPolicies', 'health']) {
        stub[m] = jest.fn().mockResolvedValue({ ok: true });
        (MockRestClient.prototype as any)[m] = stub[m];
      }

      const client = new ObjectStoreClient({
        protocol: 'rest',
        rest: { baseUrl: 'http://localhost:8080' },
      });

      await client.list();
      await client.getPolicies();
      await client.applyPolicies();
      await client.getReplicationPolicies();
      await client.health();

      expect(stub.list).toHaveBeenCalledWith({});
      expect(stub.getPolicies).toHaveBeenCalledWith({});
      expect(stub.applyPolicies).toHaveBeenCalledWith({});
      expect(stub.getReplicationPolicies).toHaveBeenCalledWith({});
      expect(stub.health).toHaveBeenCalledWith({});
    });
  });

  describe('close', () => {
    it('unified_close', async () => {
      const close = jest.fn().mockResolvedValue(undefined);
      MockRestClient.prototype.close = close as any;

      const client = new ObjectStoreClient({
        protocol: 'rest',
        rest: { baseUrl: 'http://localhost:8080' },
      });

      await expect(client.close()).resolves.toBeUndefined();
      expect(close).toHaveBeenCalled();
      // Safe to call again.
      await expect(client.close()).resolves.toBeUndefined();
    });
  });
});
