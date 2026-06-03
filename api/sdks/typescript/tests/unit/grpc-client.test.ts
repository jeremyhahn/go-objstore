import { GrpcClient } from '../../src/clients/grpc-client';
import { HealthStatus, ReplicationMode } from '../../src/types';
import { ConnectionError, ValidationError } from '../../src/errors';

// Mock the grpc-js module so no real proto loading or connections occur.
jest.mock('@grpc/grpc-js', () => {
  const actual = jest.requireActual('@grpc/grpc-js');
  return {
    ...actual,
    loadPackageDefinition: jest.fn(),
    credentials: {
      createInsecure: jest.fn(() => ({})),
      createSsl: jest.fn(() => ({})),
    },
  };
});

jest.mock('@grpc/proto-loader', () => ({
  loadSync: jest.fn(() => ({})),
}));

import * as grpc from '@grpc/grpc-js';

const EventEmitter = require('events');

/**
 * Canonical gRPC client unit-test matrix.
 *
 * For each of the 19 operations: success + error (gRPC error status). Nine
 * operations additionally get a not_found (gRPC NOT_FOUND / code 5) case. Plus
 * metadata_round_trip and validation_empty_key. The service stub is mocked; no
 * live server is required.
 */
describe('GrpcClient', () => {
  let client: GrpcClient;
  let mockServiceClient: any;

  /** Make a unary stub method resolve with the given response.
   *  Accepts both (req, callback) and (req, metadata, callback) call forms. */
  const unarySuccess = (method: string, response: any) => {
    mockServiceClient[method].mockImplementation((_req: any, metaOrCb: any, maybeCb?: any) => {
      const callback = typeof metaOrCb === 'function' ? metaOrCb : maybeCb;
      callback(null, response);
    });
  };

  /** Make a unary stub method fail with the given gRPC status code.
   *  Accepts both (req, callback) and (req, metadata, callback) call forms. */
  const unaryError = (method: string, code = 13, message = 'failed') => {
    mockServiceClient[method].mockImplementation((_req: any, metaOrCb: any, maybeCb?: any) => {
      const callback = typeof metaOrCb === 'function' ? metaOrCb : maybeCb;
      const error: any = new Error(message);
      error.code = code;
      error.details = message;
      callback(error, null);
    });
  };

  beforeEach(() => {
    mockServiceClient = {
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

    (grpc.loadPackageDefinition as jest.Mock).mockReturnValue({
      objstore: {
        v1: { ObjectStore: jest.fn(() => mockServiceClient) },
      },
    });

    client = new GrpcClient({ address: 'localhost:50051' });
  });

  // --------------------------------------------------------------------------
  // put
  // --------------------------------------------------------------------------
  describe('put', () => {
    it('grpc_put_success', async () => {
      unarySuccess('put', { success: true, message: 'stored', etag: '"abc"' });
      const response = await client.put({ key: 'test-key', data: Buffer.from('data') });
      expect(response.success).toBe(true);
      expect(response.etag).toBe('"abc"');
    });

    it('grpc_put_error', async () => {
      unaryError('put');
      await expect(
        client.put({ key: 'test-key', data: Buffer.from('data') })
      ).rejects.toThrow(ConnectionError);
    });
  });

  // --------------------------------------------------------------------------
  // get (server-streaming)
  // --------------------------------------------------------------------------
  describe('get', () => {
    it('grpc_get_success', async () => {
      mockServiceClient.get.mockImplementation(() => {
        const emitter = new EventEmitter();
        setImmediate(() => {
          emitter.emit('data', {
            metadata: { contentType: 'text/plain', size: 4 },
            data: Buffer.from('data'),
          });
          emitter.emit('end');
        });
        return emitter;
      });

      const response = await client.get({ key: 'test-key' });
      expect(response.data.toString()).toBe('data');
      expect(response.metadata?.contentType).toBe('text/plain');
    });

    it('grpc_get_error', async () => {
      mockServiceClient.get.mockImplementation(() => {
        const emitter = new EventEmitter();
        setImmediate(() => {
          const error: any = new Error('failed');
          error.code = 13;
          emitter.emit('error', error);
        });
        return emitter;
      });

      await expect(client.get({ key: 'test-key' })).rejects.toThrow(ConnectionError);
    });

    it('grpc_get_not_found', async () => {
      mockServiceClient.get.mockImplementation(() => {
        const emitter = new EventEmitter();
        setImmediate(() => {
          const error: any = new Error('missing');
          error.code = 5; // NOT_FOUND
          error.details = 'missing';
          emitter.emit('error', error);
        });
        return emitter;
      });

      await expect(client.get({ key: 'missing' })).rejects.toThrow('Not found');
    });
  });

  // --------------------------------------------------------------------------
  // delete
  // --------------------------------------------------------------------------
  describe('delete', () => {
    it('grpc_delete_success', async () => {
      unarySuccess('delete', { success: true, message: 'deleted' });
      const response = await client.delete({ key: 'test-key' });
      expect(response.success).toBe(true);
    });

    it('grpc_delete_error', async () => {
      unaryError('delete');
      await expect(client.delete({ key: 'test-key' })).rejects.toThrow(ConnectionError);
    });

    it('grpc_delete_not_found', async () => {
      unaryError('delete', 5, 'missing');
      await expect(client.delete({ key: 'missing' })).rejects.toThrow('Not found');
    });
  });

  // --------------------------------------------------------------------------
  // list
  // --------------------------------------------------------------------------
  describe('list', () => {
    it('grpc_list_success', async () => {
      unarySuccess('list', {
        objects: [{ key: 'a', metadata: { contentType: 'text/plain', size: 3 } }],
        commonPrefixes: ['p/'],
        nextToken: 'tok',
        truncated: true,
      });

      const response = await client.list({ prefix: 'a' });
      expect(response.objects).toHaveLength(1);
      expect(response.objects[0].key).toBe('a');
      expect(response.commonPrefixes).toEqual(['p/']);
      expect(response.truncated).toBe(true);
    });

    it('grpc_list_error', async () => {
      unaryError('list');
      await expect(client.list()).rejects.toThrow(ConnectionError);
    });
  });

  // --------------------------------------------------------------------------
  // exists
  // --------------------------------------------------------------------------
  describe('exists', () => {
    it('grpc_exists_success', async () => {
      unarySuccess('exists', { exists: true });
      const response = await client.exists({ key: 'test-key' });
      expect(response.exists).toBe(true);
    });

    it('grpc_exists_error', async () => {
      unaryError('exists');
      await expect(client.exists({ key: 'test-key' })).rejects.toThrow(ConnectionError);
    });

    it('grpc_exists_not_found', async () => {
      // NOT_FOUND for exists surfaces as exists:false from the server response.
      unarySuccess('exists', { exists: false });
      const response = await client.exists({ key: 'missing' });
      expect(response.exists).toBe(false);
    });
  });

  // --------------------------------------------------------------------------
  // getMetadata
  // --------------------------------------------------------------------------
  describe('getMetadata', () => {
    it('grpc_get_metadata_success', async () => {
      unarySuccess('getMetadata', {
        metadata: { contentType: 'text/plain', size: 9, custom: { author: 'jane' } },
        success: true,
      });

      const response = await client.getMetadata({ key: 'test-key' });
      expect(response.success).toBe(true);
      expect(response.metadata?.contentType).toBe('text/plain');
      expect(response.metadata?.custom).toEqual({ author: 'jane' });
    });

    it('grpc_get_metadata_error', async () => {
      unaryError('getMetadata');
      await expect(client.getMetadata({ key: 'test-key' })).rejects.toThrow(ConnectionError);
    });

    it('grpc_get_metadata_not_found', async () => {
      unaryError('getMetadata', 5, 'missing');
      await expect(client.getMetadata({ key: 'missing' })).rejects.toThrow('Not found');
    });
  });

  // --------------------------------------------------------------------------
  // updateMetadata
  // --------------------------------------------------------------------------
  describe('updateMetadata', () => {
    it('grpc_update_metadata_success', async () => {
      unarySuccess('updateMetadata', { success: true, message: 'updated' });
      const response = await client.updateMetadata({
        key: 'test-key',
        metadata: { contentType: 'text/plain' },
      });
      expect(response.success).toBe(true);
    });

    it('grpc_update_metadata_error', async () => {
      unaryError('updateMetadata');
      await expect(
        client.updateMetadata({ key: 'test-key', metadata: { contentType: 'text/plain' } })
      ).rejects.toThrow(ConnectionError);
    });

    it('grpc_update_metadata_not_found', async () => {
      unaryError('updateMetadata', 5, 'missing');
      await expect(
        client.updateMetadata({ key: 'missing', metadata: { contentType: 'text/plain' } })
      ).rejects.toThrow('Not found');
    });
  });

  // --------------------------------------------------------------------------
  // health
  // --------------------------------------------------------------------------
  describe('health', () => {
    it('grpc_health_success', async () => {
      unarySuccess('health', { status: 'SERVING', message: 'OK' });
      const response = await client.health();
      expect(response.status).toBe(HealthStatus.SERVING);
    });

    it('grpc_health_error', async () => {
      unaryError('health', 14, 'unavailable');
      await expect(client.health()).rejects.toThrow(ConnectionError);
    });
  });

  // --------------------------------------------------------------------------
  // archive
  // --------------------------------------------------------------------------
  describe('archive', () => {
    it('grpc_archive_success', async () => {
      unarySuccess('archive', { success: true, message: 'archived' });
      const response = await client.archive({ key: 'test-key', destinationType: 'glacier' });
      expect(response.success).toBe(true);
    });

    it('grpc_archive_error', async () => {
      unaryError('archive');
      await expect(
        client.archive({ key: 'test-key', destinationType: 'glacier' })
      ).rejects.toThrow(ConnectionError);
    });
  });

  // --------------------------------------------------------------------------
  // addPolicy
  // --------------------------------------------------------------------------
  describe('addPolicy', () => {
    it('grpc_add_policy_success', async () => {
      unarySuccess('addPolicy', { success: true, message: 'added' });
      const response = await client.addPolicy({
        policy: { id: 'p1', prefix: 'logs/', retentionSeconds: 86400, action: 'delete' },
      });
      expect(response.success).toBe(true);
    });

    it('grpc_add_policy_error', async () => {
      unaryError('addPolicy');
      await expect(
        client.addPolicy({
          policy: { id: 'p1', prefix: 'logs/', retentionSeconds: 1, action: 'delete' },
        })
      ).rejects.toThrow(ConnectionError);
    });
  });

  // --------------------------------------------------------------------------
  // removePolicy
  // --------------------------------------------------------------------------
  describe('removePolicy', () => {
    it('grpc_remove_policy_success', async () => {
      unarySuccess('removePolicy', { success: true, message: 'removed' });
      const response = await client.removePolicy({ id: 'p1' });
      expect(response.success).toBe(true);
    });

    it('grpc_remove_policy_error', async () => {
      unaryError('removePolicy');
      await expect(client.removePolicy({ id: 'p1' })).rejects.toThrow(ConnectionError);
    });

    it('grpc_remove_policy_not_found', async () => {
      unaryError('removePolicy', 5, 'missing');
      await expect(client.removePolicy({ id: 'missing' })).rejects.toThrow('Not found');
    });
  });

  // --------------------------------------------------------------------------
  // getPolicies
  // --------------------------------------------------------------------------
  describe('getPolicies', () => {
    it('grpc_get_policies_success', async () => {
      unarySuccess('getPolicies', {
        policies: [
          {
            id: 'p1',
            prefix: 'logs/',
            retentionSeconds: 86400,
            action: 'delete',
          },
        ],
        success: true,
      });

      const response = await client.getPolicies({ prefix: 'logs/' });
      expect(response.policies).toHaveLength(1);
      expect(response.policies[0].id).toBe('p1');
    });

    it('grpc_get_policies_error', async () => {
      unaryError('getPolicies');
      await expect(client.getPolicies()).rejects.toThrow(ConnectionError);
    });
  });

  // --------------------------------------------------------------------------
  // applyPolicies
  // --------------------------------------------------------------------------
  describe('applyPolicies', () => {
    it('grpc_apply_policies_success', async () => {
      unarySuccess('applyPolicies', {
        success: true,
        policiesCount: 2,
        objectsProcessed: 7,
      });

      const response = await client.applyPolicies();
      expect(response.success).toBe(true);
      expect(response.policiesCount).toBe(2);
      expect(response.objectsProcessed).toBe(7);
    });

    it('grpc_apply_policies_error', async () => {
      unaryError('applyPolicies');
      await expect(client.applyPolicies()).rejects.toThrow(ConnectionError);
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

    it('grpc_add_replication_policy_success', async () => {
      unarySuccess('addReplicationPolicy', { success: true, message: 'added' });
      const response = await client.addReplicationPolicy({ policy: repPolicy });
      expect(response.success).toBe(true);
    });

    it('grpc_add_replication_policy_error', async () => {
      unaryError('addReplicationPolicy');
      await expect(
        client.addReplicationPolicy({ policy: repPolicy })
      ).rejects.toThrow(ConnectionError);
    });
  });

  // --------------------------------------------------------------------------
  // removeReplicationPolicy
  // --------------------------------------------------------------------------
  describe('removeReplicationPolicy', () => {
    it('grpc_remove_replication_policy_success', async () => {
      unarySuccess('removeReplicationPolicy', { success: true, message: 'removed' });
      const response = await client.removeReplicationPolicy({ id: 'r1' });
      expect(response.success).toBe(true);
    });

    it('grpc_remove_replication_policy_error', async () => {
      unaryError('removeReplicationPolicy');
      await expect(
        client.removeReplicationPolicy({ id: 'r1' })
      ).rejects.toThrow(ConnectionError);
    });

    it('grpc_remove_replication_policy_not_found', async () => {
      unaryError('removeReplicationPolicy', 5, 'missing');
      await expect(
        client.removeReplicationPolicy({ id: 'missing' })
      ).rejects.toThrow('Not found');
    });
  });

  // --------------------------------------------------------------------------
  // getReplicationPolicies
  // --------------------------------------------------------------------------
  describe('getReplicationPolicies', () => {
    it('grpc_get_replication_policies_success', async () => {
      unarySuccess('getReplicationPolicies', {
        policies: [
          {
            id: 'r1',
            sourceBackend: 'local',
            destinationBackend: 's3',
            checkIntervalSeconds: 60,
            enabled: true,
            replicationMode: ReplicationMode.TRANSPARENT,
          },
        ],
      });

      const response = await client.getReplicationPolicies();
      expect(response.policies).toHaveLength(1);
      expect(response.policies[0].id).toBe('r1');
      expect(response.policies[0].checkIntervalSeconds).toBe(60);
    });

    it('grpc_get_replication_policies_error', async () => {
      unaryError('getReplicationPolicies');
      await expect(client.getReplicationPolicies()).rejects.toThrow(ConnectionError);
    });
  });

  // --------------------------------------------------------------------------
  // getReplicationPolicy
  // --------------------------------------------------------------------------
  describe('getReplicationPolicy', () => {
    it('grpc_get_replication_policy_success', async () => {
      unarySuccess('getReplicationPolicy', {
        policy: {
          id: 'r1',
          sourceBackend: 'local',
          destinationBackend: 's3',
          checkIntervalSeconds: 60,
          enabled: true,
          replicationMode: ReplicationMode.OPAQUE,
        },
      });

      const response = await client.getReplicationPolicy({ id: 'r1' });
      expect(response.policy?.id).toBe('r1');
      expect(response.policy?.replicationMode).toBe(ReplicationMode.OPAQUE);
    });

    it('grpc_get_replication_policy_error', async () => {
      unaryError('getReplicationPolicy');
      await expect(
        client.getReplicationPolicy({ id: 'r1' })
      ).rejects.toThrow(ConnectionError);
    });

    it('grpc_get_replication_policy_not_found', async () => {
      unaryError('getReplicationPolicy', 5, 'missing');
      await expect(
        client.getReplicationPolicy({ id: 'missing' })
      ).rejects.toThrow('Not found');
    });
  });

  // --------------------------------------------------------------------------
  // triggerReplication
  // --------------------------------------------------------------------------
  describe('triggerReplication', () => {
    it('grpc_trigger_replication_success', async () => {
      unarySuccess('triggerReplication', {
        success: true,
        result: {
          policyId: 'r1',
          synced: 5,
          deleted: 1,
          failed: 0,
          bytesTotal: 1024,
          durationMs: 200,
          errors: [],
        },
      });

      const response = await client.triggerReplication({ policyId: 'r1' });
      expect(response.success).toBe(true);
      expect(response.result?.synced).toBe(5);
      expect(response.result?.bytesTotal).toBe(1024);
    });

    it('grpc_trigger_replication_error', async () => {
      unaryError('triggerReplication');
      await expect(
        client.triggerReplication({ policyId: 'r1' })
      ).rejects.toThrow(ConnectionError);
    });
  });

  // --------------------------------------------------------------------------
  // getReplicationStatus
  // --------------------------------------------------------------------------
  describe('getReplicationStatus', () => {
    it('grpc_get_replication_status_success', async () => {
      unarySuccess('getReplicationStatus', {
        success: true,
        status: {
          policyId: 'r1',
          sourceBackend: 'local',
          destinationBackend: 's3',
          enabled: true,
          totalObjectsSynced: 10,
          totalObjectsDeleted: 2,
          totalBytesSynced: 2048,
          totalErrors: 0,
          averageSyncDurationMs: 150,
          syncCount: 3,
        },
      });

      const response = await client.getReplicationStatus({ id: 'r1' });
      expect(response.success).toBe(true);
      expect(response.status?.totalObjectsSynced).toBe(10);
      expect(response.status?.syncCount).toBe(3);
    });

    it('grpc_get_replication_status_error', async () => {
      unaryError('getReplicationStatus');
      await expect(
        client.getReplicationStatus({ id: 'r1' })
      ).rejects.toThrow(ConnectionError);
    });

    it('grpc_get_replication_status_not_found', async () => {
      unaryError('getReplicationStatus', 5, 'missing');
      await expect(
        client.getReplicationStatus({ id: 'missing' })
      ).rejects.toThrow('Not found');
    });
  });

  // --------------------------------------------------------------------------
  // metadata_round_trip
  // --------------------------------------------------------------------------
  describe('metadata round trip', () => {
    it('grpc_metadata_round_trip', async () => {
      const custom = { author: 'jane', tier: 'gold' };
      const metadata = {
        contentType: 'text/plain',
        contentEncoding: 'gzip',
        size: 5,
        custom,
      };

      // put: assert metadata travels in the proto message fields.
      let putReq: any;
      mockServiceClient.put.mockImplementation((req: any, metaOrCb: any, maybeCb?: any) => {
        putReq = req;
        const callback = typeof metaOrCb === 'function' ? metaOrCb : maybeCb;
        callback(null, { success: true, message: 'stored', etag: '"e"' });
      });
      await client.put({ key: 'doc', data: Buffer.from('hello'), metadata });
      expect(putReq.metadata.contentType).toBe('text/plain');
      expect(putReq.metadata.contentEncoding).toBe('gzip');
      expect(putReq.metadata.custom).toEqual(custom);

      // get: metadata comes back via the proto message.
      mockServiceClient.get.mockImplementation(() => {
        const emitter = new EventEmitter();
        setImmediate(() => {
          emitter.emit('data', {
            metadata: { contentType: 'text/plain', contentEncoding: 'gzip', custom },
            data: Buffer.from('hello'),
          });
          emitter.emit('end');
        });
        return emitter;
      });
      const getResp = await client.get({ key: 'doc' });
      expect(getResp.metadata?.contentType).toBe('text/plain');
      expect(getResp.metadata?.contentEncoding).toBe('gzip');
      expect(getResp.metadata?.custom).toEqual(custom);

      // getMetadata: metadata in the proto message fields.
      unarySuccess('getMetadata', {
        metadata: { contentType: 'text/plain', contentEncoding: 'gzip', custom },
        success: true,
      });
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
    it('grpc_validation_empty_key', async () => {
      // The gRPC client validates client-side; an empty key throws before any
      // network call is attempted.
      await expect(
        client.put({ key: '', data: Buffer.from('data') })
      ).rejects.toThrow(ValidationError);
      expect(mockServiceClient.put).not.toHaveBeenCalled();
    });
  });

  // --------------------------------------------------------------------------
  // close
  // --------------------------------------------------------------------------
  describe('close', () => {
    it('grpc_close', async () => {
      await expect(client.close()).resolves.toBeUndefined();
      expect(mockServiceClient.close).toHaveBeenCalled();
    });
  });

  // --------------------------------------------------------------------------
  // auth: callMeta forwarded to every RPC
  // --------------------------------------------------------------------------
  describe('auth', () => {
    it('grpc_auth_metadata_forwarded', async () => {
      // Re-create the client with token + tenantId so callMeta is populated.
      // Capture the second arg (metadata) from the first unary call (put).
      const authClient = new GrpcClient({
        address: 'localhost:50051',
        token: 'my-token',
        tenantId: 'tenant-1',
        headers: { 'x-custom': 'val' },
      });

      let capturedMeta: any;
      mockServiceClient.put.mockImplementation((_req: any, meta: any, cb: any) => {
        capturedMeta = meta;
        cb(null, { success: true, message: 'ok', etag: '' });
      });

      await authClient.put({ key: 'k', data: Buffer.from('d') });

      expect(capturedMeta).toBeDefined();
      // grpc.Metadata stores values as arrays.
      const auth = capturedMeta.get('authorization');
      expect(auth[0]).toBe('Bearer my-token');
      const tenant = capturedMeta.get('x-tenant-id');
      expect(tenant[0]).toBe('tenant-1');
    });
  });

  // --------------------------------------------------------------------------
  // getStream
  // --------------------------------------------------------------------------
  describe('getStream', () => {
    it('grpc_getStream_success', async () => {
      const emitter = new EventEmitter();
      mockServiceClient.get.mockImplementation(() => emitter);

      const stream = client.getStream('stream-key');
      const chunks: Buffer[] = [];
      const done = new Promise<void>((resolve, reject) => {
        stream.on('data', (chunk: Buffer) => chunks.push(chunk));
        stream.on('end', resolve);
        stream.on('error', reject);
      });

      setImmediate(() => {
        emitter.emit('data', { data: Buffer.from('chunk1') });
        emitter.emit('data', { data: Buffer.from('chunk2') });
        emitter.emit('end');
      });

      await done;
      expect(Buffer.concat(chunks).toString()).toBe('chunk1chunk2');
    });

    it('grpc_getStream_error', async () => {
      const emitter = new EventEmitter();
      mockServiceClient.get.mockImplementation(() => emitter);

      const stream = client.getStream('bad-key');
      const errorPromise = new Promise<Error>((resolve) => {
        stream.on('error', resolve);
      });

      const grpcErr: any = new Error('not found');
      grpcErr.code = 5;
      grpcErr.details = 'not found';
      setImmediate(() => emitter.emit('error', grpcErr));

      const err = await errorPromise;
      expect(err.message).toMatch(/Not found/);
    });
  });

  // --------------------------------------------------------------------------
  // putStream
  // --------------------------------------------------------------------------
  describe('putStream', () => {
    it('grpc_putStream_success', async () => {
      unarySuccess('put', { success: true, message: 'ok', etag: '' });
      const { Readable: NodeReadable } = require('stream');
      const stream = NodeReadable.from([Buffer.from('hello'), Buffer.from(' world')]);
      const resp = await client.putStream('stream-key', stream);
      expect(resp.success).toBe(true);
    });
  });
});
