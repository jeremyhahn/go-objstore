import nock from 'nock';
import { RestClient } from '../../src/clients/rest-client';
import { HealthStatus, ReplicationMode } from '../../src/types';
import {
  ObjectNotFoundError,
  AuthenticationError,
  AuthorizationError,
  ValidationError,
  AlreadyExistsError,
  RateLimitError,
  ServerError,
  ConnectionError,
} from '../../src/errors';

/**
 * Canonical REST client unit-test matrix.
 *
 * For each of the 19 operations: success + error (HTTP 5xx). Nine operations
 * additionally get a not_found (HTTP 404) case. Plus metadata_round_trip and
 * validation_empty_key cross-cutting cases. The transport is mocked with nock;
 * no live server is required.
 */
describe('RestClient', () => {
  const baseUrl = 'http://localhost:8080';
  let client: RestClient;

  beforeEach(() => {
    client = new RestClient({ baseUrl });
  });

  afterEach(() => {
    nock.cleanAll();
  });

  // --------------------------------------------------------------------------
  // put
  // --------------------------------------------------------------------------
  describe('put', () => {
    it('rest_put_success', async () => {
      const scope = nock(baseUrl)
        .put('/objects/test-key')
        .reply(200, { message: 'Object stored successfully' }, { etag: '"abc123"' });

      const response = await client.put({ key: 'test-key', data: Buffer.from('test data') });

      expect(response.success).toBe(true);
      expect(response.message).toBe('Object stored successfully');
      expect(response.etag).toBe('"abc123"');
      scope.done();
    });

    it('rest_put_error', async () => {
      nock(baseUrl).put('/objects/test-key').reply(500, { message: 'Internal server error' });

      await expect(
        client.put({ key: 'test-key', data: Buffer.from('test data') })
      ).rejects.toThrow('REST API error (500)');
    });
  });

  // --------------------------------------------------------------------------
  // get
  // --------------------------------------------------------------------------
  describe('get', () => {
    it('rest_get_success', async () => {
      const scope = nock(baseUrl)
        .get('/objects/test-key')
        .reply(200, 'test data', { 'content-type': 'text/plain', etag: '"abc123"' });

      const response = await client.get({ key: 'test-key' });

      expect(response.data.toString()).toBe('test data');
      expect(response.metadata?.contentType).toBe('text/plain');
      scope.done();
    });

    it('rest_get_error', async () => {
      nock(baseUrl).get('/objects/test-key').reply(500, 'boom');
      await expect(client.get({ key: 'test-key' })).rejects.toThrow('REST API error (500)');
    });

    it('rest_get_not_found', async () => {
      nock(baseUrl).get('/objects/missing').reply(404, 'not found');
      await expect(client.get({ key: 'missing' })).rejects.toThrow('REST API error (404)');
    });
  });

  // --------------------------------------------------------------------------
  // typed HTTP error mapping
  // --------------------------------------------------------------------------
  describe('typed_errors', () => {
    it('rest_404_maps_to_ObjectNotFoundError', async () => {
      nock(baseUrl).get('/objects/missing').reply(404, { message: 'not found' });
      const err = await client.get({ key: 'missing' }).then(
        () => undefined,
        (e) => e
      );
      expect(err).toBeInstanceOf(ObjectNotFoundError);
      expect((err as Error).message).toContain('REST API error (404)');
      expect((err as Error).message).toContain('not found');
    });

    it('rest_401_maps_to_AuthenticationError', async () => {
      nock(baseUrl).get('/objects/k').reply(401, { message: 'unauthenticated' });
      await expect(client.get({ key: 'k' })).rejects.toBeInstanceOf(AuthenticationError);
    });

    it('rest_403_maps_to_AuthorizationError', async () => {
      nock(baseUrl).get('/objects/k').reply(403, { message: 'forbidden' });
      await expect(client.get({ key: 'k' })).rejects.toBeInstanceOf(AuthorizationError);
    });

    it('rest_400_maps_to_ValidationError', async () => {
      nock(baseUrl).get('/objects/k').reply(400, { message: 'bad request' });
      await expect(client.get({ key: 'k' })).rejects.toBeInstanceOf(ValidationError);
    });

    it('rest_409_maps_to_AlreadyExistsError', async () => {
      nock(baseUrl).get('/objects/k').reply(409, { message: 'already exists' });
      const err = await client.get({ key: 'k' }).then(
        () => undefined,
        (e) => e
      );
      expect(err).toBeInstanceOf(AlreadyExistsError);
      expect((err as AlreadyExistsError).statusCode).toBe(409);
    });

    it('rest_429_maps_to_RateLimitError', async () => {
      nock(baseUrl).get('/objects/k').reply(429, { message: 'rate limited' });
      const err = await client.get({ key: 'k' }).then(
        () => undefined,
        (e) => e
      );
      expect(err).toBeInstanceOf(RateLimitError);
      expect((err as RateLimitError).statusCode).toBe(429);
    });

    it('rest_500_maps_to_ServerError', async () => {
      nock(baseUrl).get('/objects/k').reply(500, { message: 'boom' });
      const err = await client.get({ key: 'k' }).then(
        () => undefined,
        (e) => e
      );
      expect(err).toBeInstanceOf(ServerError);
      expect((err as ServerError).statusCode).toBe(500);
    });

    it('rest_network_failure_maps_to_ConnectionError', async () => {
      nock(baseUrl).get('/objects/k').replyWithError('ECONNREFUSED');
      await expect(client.get({ key: 'k' })).rejects.toBeInstanceOf(ConnectionError);
    });
  });

  // --------------------------------------------------------------------------
  // delete
  // --------------------------------------------------------------------------
  describe('delete', () => {
    it('rest_delete_success_204_no_content', async () => {
      const scope = nock(baseUrl).delete('/objects/test-key').reply(204);

      const response = await client.delete({ key: 'test-key' });

      expect(response.success).toBe(true);
      expect(response.message).toBe('Object deleted successfully');
      scope.done();
    });

    it('rest_delete_tolerates_legacy_200_body', async () => {
      const scope = nock(baseUrl).delete('/objects/test-key').reply(200, { message: 'deleted' });

      const response = await client.delete({ key: 'test-key' });

      expect(response.success).toBe(true);
      expect(response.message).toBe('deleted');
      scope.done();
    });

    it('rest_delete_error', async () => {
      nock(baseUrl).delete('/objects/test-key').reply(500, { message: 'boom' });
      await expect(client.delete({ key: 'test-key' })).rejects.toThrow('REST API error (500)');
    });

    it('rest_delete_not_found', async () => {
      nock(baseUrl).delete('/objects/missing').reply(404, { message: 'not found' });
      await expect(client.delete({ key: 'missing' })).rejects.toThrow('REST API error (404)');
    });
  });

  // --------------------------------------------------------------------------
  // list
  // --------------------------------------------------------------------------
  describe('list', () => {
    it('rest_list_success', async () => {
      const scope = nock(baseUrl)
        .get('/objects')
        .query(true)
        .reply(200, {
          objects: [{ key: 'a', content_type: 'text/plain', size: 3 }],
          common_prefixes: ['p/'],
          next_token: 'tok',
          truncated: true,
        });

      const response = await client.list({ prefix: 'a', maxResults: 10, continueFrom: 'x' });

      expect(response.objects).toHaveLength(1);
      expect(response.objects[0].key).toBe('a');
      expect(response.commonPrefixes).toEqual(['p/']);
      expect(response.nextToken).toBe('tok');
      expect(response.truncated).toBe(true);
      scope.done();
    });

    it('rest_list_error', async () => {
      nock(baseUrl).get('/objects').query(true).reply(500, { message: 'boom' });
      await expect(client.list()).rejects.toThrow('REST API error (500)');
    });
  });

  // --------------------------------------------------------------------------
  // exists
  // --------------------------------------------------------------------------
  describe('exists', () => {
    it('rest_exists_success', async () => {
      const scope = nock(baseUrl).head('/objects/test-key').reply(200);
      const response = await client.exists({ key: 'test-key' });
      expect(response.exists).toBe(true);
      scope.done();
    });

    it('rest_exists_error', async () => {
      nock(baseUrl).head('/objects/test-key').reply(500);
      await expect(client.exists({ key: 'test-key' })).rejects.toThrow('REST API error (500)');
    });

    it('rest_exists_not_found', async () => {
      const scope = nock(baseUrl).head('/objects/missing').reply(404);
      const response = await client.exists({ key: 'missing' });
      expect(response.exists).toBe(false);
      scope.done();
    });
  });

  // --------------------------------------------------------------------------
  // getMetadata
  // --------------------------------------------------------------------------
  describe('getMetadata', () => {
    it('rest_get_metadata_success', async () => {
      const scope = nock(baseUrl)
        .get('/metadata/test-key')
        .reply(
          200,
          { content_type: 'text/plain', size: 9, etag: '"e"' },
          { 'x-object-metadata': JSON.stringify({ author: 'jane' }) }
        );

      const response = await client.getMetadata({ key: 'test-key' });

      expect(response.success).toBe(true);
      expect(response.metadata?.contentType).toBe('text/plain');
      expect(response.metadata?.custom).toEqual({ author: 'jane' });
      scope.done();
    });

    it('rest_get_metadata_error', async () => {
      nock(baseUrl).get('/metadata/test-key').reply(500, { message: 'boom' });
      await expect(client.getMetadata({ key: 'test-key' })).rejects.toThrow('REST API error (500)');
    });

    it('rest_get_metadata_not_found', async () => {
      nock(baseUrl).get('/metadata/missing').reply(404, { message: 'not found' });
      await expect(client.getMetadata({ key: 'missing' })).rejects.toThrow('REST API error (404)');
    });
  });

  // --------------------------------------------------------------------------
  // updateMetadata
  // --------------------------------------------------------------------------
  describe('updateMetadata', () => {
    it('rest_update_metadata_success', async () => {
      const scope = nock(baseUrl).put('/metadata/test-key').reply(200, { message: 'updated' });

      const response = await client.updateMetadata({
        key: 'test-key',
        metadata: { contentType: 'text/plain' },
      });

      expect(response.success).toBe(true);
      expect(response.message).toBe('updated');
      scope.done();
    });

    it('rest_update_metadata_error', async () => {
      nock(baseUrl).put('/metadata/test-key').reply(500, { message: 'boom' });
      await expect(
        client.updateMetadata({ key: 'test-key', metadata: { contentType: 'text/plain' } })
      ).rejects.toThrow('REST API error (500)');
    });

    it('rest_update_metadata_not_found', async () => {
      nock(baseUrl).put('/metadata/missing').reply(404, { message: 'not found' });
      await expect(
        client.updateMetadata({ key: 'missing', metadata: { contentType: 'text/plain' } })
      ).rejects.toThrow('REST API error (404)');
    });
  });

  // --------------------------------------------------------------------------
  // health
  // --------------------------------------------------------------------------
  describe('health', () => {
    it('rest_health_success', async () => {
      const scope = nock(baseUrl).get('/health').query(true).reply(200, { status: 'healthy', message: 'OK' });

      const response = await client.health();

      expect(response.status).toBe(HealthStatus.SERVING);
      expect(response.message).toBe('OK');
      scope.done();
    });

    it('rest_health_error', async () => {
      nock(baseUrl).get('/health').query(true).reply(500, { message: 'boom' });
      await expect(client.health()).rejects.toThrow('REST API error (500)');
    });
  });

  // --------------------------------------------------------------------------
  // archive
  // --------------------------------------------------------------------------
  describe('archive', () => {
    it('rest_archive_success', async () => {
      const scope = nock(baseUrl)
        .post('/archive')
        .reply(200, { success: true, message: 'archived' });

      const response = await client.archive({ key: 'test-key', destinationType: 'glacier' });

      expect(response.success).toBe(true);
      expect(response.message).toBe('archived');
      scope.done();
    });

    it('rest_archive_error', async () => {
      nock(baseUrl).post('/archive').reply(500, { message: 'boom' });
      await expect(
        client.archive({ key: 'test-key', destinationType: 'glacier' })
      ).rejects.toThrow('REST API error (500)');
    });
  });

  // --------------------------------------------------------------------------
  // addPolicy
  // --------------------------------------------------------------------------
  describe('addPolicy', () => {
    it('rest_add_policy_success', async () => {
      const scope = nock(baseUrl).post('/policies').reply(200, { success: true, message: 'added' });

      const response = await client.addPolicy({
        policy: { id: 'p1', prefix: 'logs/', retentionSeconds: 86400, action: 'delete' },
      });

      expect(response.success).toBe(true);
      expect(response.message).toBe('added');
      scope.done();
    });

    it('rest_add_policy_error', async () => {
      nock(baseUrl).post('/policies').reply(500, { message: 'boom' });
      await expect(
        client.addPolicy({
          policy: { id: 'p1', prefix: 'logs/', retentionSeconds: 1, action: 'delete' },
        })
      ).rejects.toThrow('REST API error (500)');
    });
  });

  // --------------------------------------------------------------------------
  // removePolicy
  // --------------------------------------------------------------------------
  describe('removePolicy', () => {
    it('rest_remove_policy_success', async () => {
      const scope = nock(baseUrl).delete('/policies/p1').reply(200, { success: true, message: 'removed' });

      const response = await client.removePolicy({ id: 'p1' });

      expect(response.success).toBe(true);
      expect(response.message).toBe('removed');
      scope.done();
    });

    it('rest_remove_policy_error', async () => {
      nock(baseUrl).delete('/policies/p1').reply(500, { message: 'boom' });
      await expect(client.removePolicy({ id: 'p1' })).rejects.toThrow('REST API error (500)');
    });

    it('rest_remove_policy_not_found', async () => {
      nock(baseUrl).delete('/policies/missing').reply(404, { message: 'not found' });
      await expect(client.removePolicy({ id: 'missing' })).rejects.toThrow('REST API error (404)');
    });
  });

  // --------------------------------------------------------------------------
  // getPolicies
  // --------------------------------------------------------------------------
  describe('getPolicies', () => {
    it('rest_get_policies_success', async () => {
      const scope = nock(baseUrl)
        .get('/policies')
        .query(true)
        .reply(200, {
          policies: [
            {
              id: 'p1',
              prefix: 'logs/',
              retention_seconds: 86400,
              action: 'delete',
              destination_type: '',
            },
          ],
          success: true,
        });

      const response = await client.getPolicies({ prefix: 'logs/' });

      expect(response.policies).toHaveLength(1);
      expect(response.policies[0].id).toBe('p1');
      expect(response.policies[0].retentionSeconds).toBe(86400);
      scope.done();
    });

    it('rest_get_policies_error', async () => {
      nock(baseUrl).get('/policies').query(true).reply(500, { message: 'boom' });
      await expect(client.getPolicies()).rejects.toThrow('REST API error (500)');
    });
  });

  // --------------------------------------------------------------------------
  // applyPolicies
  // --------------------------------------------------------------------------
  describe('applyPolicies', () => {
    it('rest_apply_policies_success', async () => {
      const scope = nock(baseUrl)
        .post('/policies/apply')
        .reply(200, { success: true, policies_count: 2, objects_processed: 7, message: 'applied' });

      const response = await client.applyPolicies();

      expect(response.success).toBe(true);
      expect(response.policiesCount).toBe(2);
      expect(response.objectsProcessed).toBe(7);
      scope.done();
    });

    it('rest_apply_policies_error', async () => {
      nock(baseUrl).post('/policies/apply').reply(500, { message: 'boom' });
      await expect(client.applyPolicies()).rejects.toThrow('REST API error (500)');
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

    it('rest_add_replication_policy_success', async () => {
      const scope = nock(baseUrl)
        .post('/replication/policies')
        .reply(200, { success: true, message: 'added' });

      const response = await client.addReplicationPolicy({ policy: repPolicy });

      expect(response.success).toBe(true);
      expect(response.message).toBe('added');
      scope.done();
    });

    it('rest_add_replication_policy_error', async () => {
      nock(baseUrl).post('/replication/policies').reply(500, { message: 'boom' });
      await expect(
        client.addReplicationPolicy({ policy: repPolicy })
      ).rejects.toThrow('REST API error (500)');
    });
  });

  // --------------------------------------------------------------------------
  // removeReplicationPolicy
  // --------------------------------------------------------------------------
  describe('removeReplicationPolicy', () => {
    it('rest_remove_replication_policy_success', async () => {
      const scope = nock(baseUrl)
        .delete('/replication/policies/r1')
        .reply(200, { success: true, message: 'removed' });

      const response = await client.removeReplicationPolicy({ id: 'r1' });

      expect(response.success).toBe(true);
      expect(response.message).toBe('removed');
      scope.done();
    });

    it('rest_remove_replication_policy_error', async () => {
      nock(baseUrl).delete('/replication/policies/r1').reply(500, { message: 'boom' });
      await expect(
        client.removeReplicationPolicy({ id: 'r1' })
      ).rejects.toThrow('REST API error (500)');
    });

    it('rest_remove_replication_policy_not_found', async () => {
      nock(baseUrl).delete('/replication/policies/missing').reply(404, { message: 'not found' });
      await expect(
        client.removeReplicationPolicy({ id: 'missing' })
      ).rejects.toThrow('REST API error (404)');
    });
  });

  // --------------------------------------------------------------------------
  // getReplicationPolicies
  // --------------------------------------------------------------------------
  describe('getReplicationPolicies', () => {
    it('rest_get_replication_policies_success', async () => {
      const scope = nock(baseUrl)
        .get('/replication/policies')
        .reply(200, {
          policies: [
            {
              id: 'r1',
              source_backend: 'local',
              destination_backend: 's3',
              check_interval_seconds: 60,
              enabled: true,
              replication_mode: 'transparent',
            },
          ],
        });

      const response = await client.getReplicationPolicies();

      expect(response.policies).toHaveLength(1);
      expect(response.policies[0].id).toBe('r1');
      expect(response.policies[0].replicationMode).toBe(ReplicationMode.TRANSPARENT);
      scope.done();
    });

    it('rest_get_replication_policies_error', async () => {
      nock(baseUrl).get('/replication/policies').reply(500, { message: 'boom' });
      await expect(client.getReplicationPolicies()).rejects.toThrow('REST API error (500)');
    });
  });

  // --------------------------------------------------------------------------
  // getReplicationPolicy
  // --------------------------------------------------------------------------
  describe('getReplicationPolicy', () => {
    it('rest_get_replication_policy_success', async () => {
      const scope = nock(baseUrl)
        .get('/replication/policies/r1')
        .reply(200, {
          id: 'r1',
          source_backend: 'local',
          destination_backend: 's3',
          check_interval_seconds: 60,
          enabled: true,
          replication_mode: 'opaque',
        });

      const response = await client.getReplicationPolicy({ id: 'r1' });

      expect(response.policy?.id).toBe('r1');
      expect(response.policy?.replicationMode).toBe(ReplicationMode.OPAQUE);
      scope.done();
    });

    it('rest_get_replication_policy_error', async () => {
      nock(baseUrl).get('/replication/policies/r1').reply(500, { message: 'boom' });
      await expect(
        client.getReplicationPolicy({ id: 'r1' })
      ).rejects.toThrow('REST API error (500)');
    });

    it('rest_get_replication_policy_not_found', async () => {
      nock(baseUrl).get('/replication/policies/missing').reply(404, { message: 'not found' });
      await expect(
        client.getReplicationPolicy({ id: 'missing' })
      ).rejects.toThrow('REST API error (404)');
    });
  });

  // --------------------------------------------------------------------------
  // triggerReplication
  // --------------------------------------------------------------------------
  describe('triggerReplication', () => {
    it('rest_trigger_replication_success', async () => {
      const scope = nock(baseUrl)
        .post('/replication/trigger')
        .reply(200, {
          success: true,
          result: {
            policy_id: 'r1',
            synced: 5,
            deleted: 1,
            failed: 0,
            bytes_total: 1024,
            duration_ms: 200,
            errors: [],
          },
        });

      const response = await client.triggerReplication({ policyId: 'r1' });

      expect(response.success).toBe(true);
      expect(response.result?.synced).toBe(5);
      expect(response.result?.bytesTotal).toBe(1024);
      scope.done();
    });

    it('rest_trigger_replication_error', async () => {
      nock(baseUrl).post('/replication/trigger').reply(500, { message: 'boom' });
      await expect(
        client.triggerReplication({ policyId: 'r1' })
      ).rejects.toThrow('REST API error (500)');
    });
  });

  // --------------------------------------------------------------------------
  // getReplicationStatus
  // --------------------------------------------------------------------------
  describe('getReplicationStatus', () => {
    it('rest_get_replication_status_success', async () => {
      // The server sends ReplicationStatusResponse fields at the top level of
      // the response body (no "status" wrapper key), and average_sync_duration
      // is a Go time.Duration string (e.g. "150ms"), not a numeric _ms field.
      const scope = nock(baseUrl)
        .get('/replication/status/r1')
        .reply(200, {
          policy_id: 'r1',
          source_backend: 'local',
          destination_backend: 's3',
          enabled: true,
          total_objects_synced: 10,
          total_objects_deleted: 2,
          total_bytes_synced: 2048,
          total_errors: 0,
          average_sync_duration: '150ms',
          sync_count: 3,
        });

      const response = await client.getReplicationStatus({ id: 'r1' });

      expect(response.success).toBe(true);
      expect(response.status?.totalObjectsSynced).toBe(10);
      expect(response.status?.syncCount).toBe(3);
      expect(response.status?.averageSyncDurationMs).toBe(150);
      scope.done();
    });

    it('rest_get_replication_status_error', async () => {
      nock(baseUrl).get('/replication/status/r1').reply(500, { message: 'boom' });
      await expect(
        client.getReplicationStatus({ id: 'r1' })
      ).rejects.toThrow('REST API error (500)');
    });

    it('rest_get_replication_status_not_found', async () => {
      nock(baseUrl).get('/replication/status/missing').reply(404, { message: 'not found' });
      await expect(
        client.getReplicationStatus({ id: 'missing' })
      ).rejects.toThrow('REST API error (404)');
    });
  });

  // --------------------------------------------------------------------------
  // metadata_round_trip
  // --------------------------------------------------------------------------
  describe('metadata round trip', () => {
    it('rest_metadata_round_trip', async () => {
      const custom = { author: 'jane', tier: 'gold' };

      // put: assert request sets Content-Type, Content-Encoding and
      // X-Object-Metadata = JSON(custom map only).
      const putScope = nock(baseUrl)
        .matchHeader('content-type', 'text/plain')
        .matchHeader('content-encoding', 'gzip')
        .matchHeader('x-object-metadata', JSON.stringify(custom))
        .put('/objects/doc')
        .reply(200, { message: 'stored' }, { etag: '"e"' });

      await client.put({
        key: 'doc',
        data: Buffer.from('hello'),
        metadata: { contentType: 'text/plain', contentEncoding: 'gzip', custom },
      });
      putScope.done();

      // get: response carries content-type + X-Object-Metadata. (The
      // content-encoding round-trip is asserted as a PUT request header above;
      // the HTTP layer consumes the content-encoding response header on the
      // arraybuffer GET / JSON getMetadata body paths, so it is not re-asserted
      // on those responses.)
      const getScope = nock(baseUrl)
        .get('/objects/doc')
        .reply(200, 'hello', {
          'content-type': 'text/plain',
          'x-object-metadata': JSON.stringify(custom),
        });

      const getResp = await client.get({ key: 'doc' });
      expect(getResp.metadata?.contentType).toBe('text/plain');
      expect(getResp.metadata?.custom).toEqual(custom);
      getScope.done();

      // getMetadata: content_type from the JSON body, custom parsed back from
      // the X-Object-Metadata response header.
      const metaScope = nock(baseUrl)
        .get('/metadata/doc')
        .reply(200, JSON.stringify({ content_type: 'text/plain' }), {
          'content-type': 'application/json',
          'x-object-metadata': JSON.stringify(custom),
        });

      const metaResp = await client.getMetadata({ key: 'doc' });
      expect(metaResp.metadata?.contentType).toBe('text/plain');
      expect(metaResp.metadata?.custom).toEqual(custom);
      metaScope.done();
    });
  });

  // --------------------------------------------------------------------------
  // validation_empty_key
  // --------------------------------------------------------------------------
  describe('validation', () => {
    it('rest_validation_empty_key', async () => {
      // The REST client has no client-side validation, so an empty key would
      // hit /objects/ on the wire. With NO nock interceptor registered, the
      // request is refused (no successful network call) and the call rejects.
      nock.disableNetConnect();
      try {
        await expect(
          client.put({ key: '', data: Buffer.from('data') })
        ).rejects.toThrow();
      } finally {
        nock.enableNetConnect();
      }
    });
  });

  // --------------------------------------------------------------------------
  // auth: token + tenantId forwarded as headers
  // --------------------------------------------------------------------------
  describe('auth', () => {
    it('rest_auth_token_and_tenant', async () => {
      const authClient = new RestClient({
        baseUrl,
        token: 'my-bearer',
        tenantId: 'ten-1',
      });
      const scope = nock(baseUrl)
        .matchHeader('authorization', 'Bearer my-bearer')
        .matchHeader('x-tenant-id', 'ten-1')
        .get('/health')
        .reply(200, { status: 'healthy' });

      await authClient.health();
      scope.done();
    });
  });

  // --------------------------------------------------------------------------
  // getStream
  // --------------------------------------------------------------------------
  describe('getStream', () => {
    it('rest_getStream_success', async () => {
      nock(baseUrl).get('/objects/stream-key').reply(200, 'streamed content');

      const stream = await client.getStream('stream-key');
      const chunks: Buffer[] = [];
      await new Promise<void>((resolve, reject) => {
        stream.on('data', (chunk: Buffer) => chunks.push(chunk));
        stream.on('end', resolve);
        stream.on('error', reject);
      });
      expect(Buffer.concat(chunks).toString()).toContain('streamed content');
    });

    it('rest_getStream_error', async () => {
      nock(baseUrl).get('/objects/missing-stream').reply(404, 'not found');
      await expect(client.getStream('missing-stream')).rejects.toThrow();
    });
  });

  // --------------------------------------------------------------------------
  // putStream
  // --------------------------------------------------------------------------
  describe('putStream', () => {
    it('rest_putStream_success', async () => {
      nock(baseUrl)
        .put('/objects/stream-put')
        .reply(200, { message: 'stored' }, { etag: '"stream1"' });

      const { Readable } = require('stream');
      const stream = Readable.from([Buffer.from('hello'), Buffer.from(' world')]);
      const resp = await client.putStream('stream-put', stream);
      expect(resp.success).toBe(true);
    });

    it('rest_putStream_asyncIterable', async () => {
      nock(baseUrl)
        .put('/objects/stream-async')
        .reply(200, { message: 'ok' });

      async function* gen(): AsyncIterable<Buffer> {
        yield Buffer.from('a');
        yield Buffer.from('b');
      }
      const resp = await client.putStream('stream-async', gen());
      expect(resp.success).toBe(true);
    });

    it('rest_putStream_error', async () => {
      nock(baseUrl).put('/objects/stream-err').reply(500, { message: 'fail' });

      const { Readable } = require('stream');
      const stream = Readable.from([Buffer.from('data')]);
      await expect(client.putStream('stream-err', stream)).rejects.toThrow('REST API error (500)');
    });
  });
});
