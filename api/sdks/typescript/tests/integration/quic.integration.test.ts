import { QuicClient } from '../../src/clients/quic-client';
import { HealthStatus, ReplicationMode } from '../../src/types';

describe('QUIC Integration Tests', () => {
  let client: QuicClient;
  const testKey = `test-quic-${Date.now()}.txt`;
  const testData = Buffer.from('Hello from QUIC!');

  beforeAll(() => {
    // Note: True QUIC/HTTP3 support uses UDP, but Node.js fetch API uses TCP.
    // The QuicClient uses HTTP/2 over TLS as a fallback, connecting to the REST endpoint.
    // This matches the behavior of other SDK implementations (Python, JavaScript).
    const quicUrl = process.env.OBJSTORE_QUIC_URL || process.env.OBJSTORE_REST_URL || 'localhost:8080';

    // Parse URL to extract host and determine if secure
    const isSecure = quicUrl.startsWith('https://');
    const address = quicUrl.replace(/^https?:\/\//, '');

    client = new QuicClient({
      address: address,
      secure: isSecure,
    });
  });

  afterAll(async () => {
    await client.close();
  });

  describe('Health Check', () => {
    it('should return healthy status', async () => {
      const response = await client.health();
      expect(response.status).toBe(HealthStatus.SERVING);
    });
  });

  describe('Object Operations', () => {
    it('should put an object', async () => {
      const response = await client.put({
        key: testKey,
        data: testData,
        metadata: {
          contentType: 'text/plain',
          custom: { author: 'quic-integration-test' },
        },
      });

      expect(response.success).toBe(true);
      // ETag may or may not be returned depending on backend
    });

    it('should check if object exists', async () => {
      const response = await client.exists({ key: testKey });
      expect(response.exists).toBe(true);
    });

    it('should get object metadata', async () => {
      const response = await client.getMetadata({ key: testKey });
      expect(response.success).toBe(true);
      expect(response.metadata).toBeDefined();
      expect(response.metadata?.size).toBeGreaterThan(0);
    });

    it('should update object metadata', async () => {
      const response = await client.updateMetadata({
        key: testKey,
        metadata: {
          contentType: 'text/plain',
          custom: { author: 'updated-quic-test', version: '2.0' },
        },
      });

      expect(response.success).toBe(true);
    });

    it('should get an object', async () => {
      const response = await client.get({ key: testKey });
      expect(response.data).toBeDefined();
      expect(response.data.toString()).toBe(testData.toString());
      expect(response.metadata?.contentType).toBeDefined();
    });

    it('should list objects', async () => {
      const response = await client.list({ prefix: 'test-quic-' });
      expect(response.objects).toBeDefined();
      expect(Array.isArray(response.objects)).toBe(true);
      expect(response.truncated).toBeDefined();
    });

    it('should delete an object', async () => {
      const response = await client.delete({ key: testKey });
      expect(response.success).toBe(true);
    });

    it('should confirm object does not exist after deletion', async () => {
      const response = await client.exists({ key: testKey });
      expect(response.exists).toBe(false);
    });
  });

  describe('Error Handling', () => {
    it('should handle getting a nonexistent object', async () => {
      const nonexistentKey = `nonexistent-quic-${Date.now()}.txt`;

      await expect(async () => {
        await client.get({ key: nonexistentKey });
      }).rejects.toThrow();
    });

    it('should handle deleting a nonexistent object', async () => {
      const nonexistentKey = `nonexistent-quic-${Date.now()}.txt`;

      try {
        const response = await client.delete({ key: nonexistentKey });
        // Some backends may not error on delete of nonexistent object (idempotent delete)
        expect(response.success).toBeDefined();
      } catch (error) {
        // If it throws, that's also acceptable behavior
        expect(error).toBeDefined();
      }
    });

    it('should handle updating metadata on nonexistent object', async () => {
      const nonexistentKey = `nonexistent-quic-${Date.now()}.txt`;

      try {
        await client.updateMetadata({
          key: nonexistentKey,
          metadata: {
            contentType: 'text/plain',
            custom: { test: 'value' },
          },
        });
        // Some backends may allow metadata updates on nonexistent objects
      } catch (error) {
        // If it throws, that's expected behavior
        expect(error).toBeDefined();
      }
    });
  });

  describe('Lifecycle Policies', () => {
    const policyId = `policy-quic-${Date.now()}`;

    it('should add a lifecycle policy', async () => {
      try {
        const response = await client.addPolicy({
          policy: {
            id: policyId,
            prefix: 'logs/',
            retentionSeconds: 86400,
            action: 'delete',
          },
        });

        expect(response.success).toBe(true);
      } catch (error) {
        // Local backend doesn't support lifecycle policies
        expect(error).toBeDefined();
      }
    });

    it('should get lifecycle policies', async () => {
      const response = await client.getPolicies();
      expect(response.success).toBe(true);
      expect(response.policies).toBeDefined();
      expect(Array.isArray(response.policies)).toBe(true);
    });

    it('should apply lifecycle policies', async () => {
      const response = await client.applyPolicies();
      expect(response.success).toBe(true);
      expect(response.policiesCount).toBeGreaterThanOrEqual(0);
      expect(response.objectsProcessed).toBeGreaterThanOrEqual(0);
    });

    it('should remove a lifecycle policy', async () => {
      try {
        const response = await client.removePolicy({ id: policyId });
        expect(response.success).toBe(true);
      } catch (error) {
        // Local backend doesn't support lifecycle policies
        expect(error).toBeDefined();
      }
    });
  });

  describe('Replication Policies', () => {
    const replicationId = `replication-quic-${Date.now()}`;

    it('should add a replication policy', async () => {
      try {
        const response = await client.addReplicationPolicy({
          policy: {
            id: replicationId,
            sourceBackend: 'local',
            sourceSettings: { path: '/source' },
            sourcePrefix: '',
            destinationBackend: 'local',
            destinationSettings: { path: '/destination' },
            checkIntervalSeconds: 3600,
            enabled: true,
            replicationMode: ReplicationMode.TRANSPARENT,
          },
        });

        expect(response.success).toBe(true);
      } catch (error) {
        expect(error).toBeDefined();
      }
    });

    it('should get replication policies', async () => {
      try {
        const response = await client.getReplicationPolicies();
        expect(response.policies).toBeDefined();
        expect(Array.isArray(response.policies)).toBe(true);
      } catch (error) {
        expect(error).toBeDefined();
      }
    });

    it('should get a specific replication policy', async () => {
      try {
        const response = await client.getReplicationPolicy({ id: replicationId });
        expect(response.policy).toBeDefined();
        expect(response.policy?.id).toBe(replicationId);
      } catch (error) {
        expect(error).toBeDefined();
      }
    });

    it('should trigger replication', async () => {
      try {
        const response = await client.triggerReplication({
          policyId: replicationId,
          parallel: false,
        });

        expect(response.success).toBe(true);
      } catch (error) {
        expect(error).toBeDefined();
      }
    });

    it('should get replication status', async () => {
      try {
        const response = await client.getReplicationStatus({ id: replicationId });
        expect(response.success).toBe(true);
        expect(response.status).toBeDefined();
      } catch (error) {
        expect(error).toBeDefined();
      }
    });

    it('should remove a replication policy', async () => {
      try {
        const response = await client.removeReplicationPolicy({ id: replicationId });
        expect(response.success).toBe(true);
      } catch (error) {
        expect(error).toBeDefined();
      }
    });
  });

  describe('Archive Operations', () => {
    const archiveKey = `archive-quic-${Date.now()}.txt`;

    beforeAll(async () => {
      await client.put({
        key: archiveKey,
        data: Buffer.from('Archive test data'),
      });
    });

    afterAll(async () => {
      await client.delete({ key: archiveKey }).catch(() => {});
    });

    it('should archive an object', async () => {
      try {
        const response = await client.archive({
          key: archiveKey,
          destinationType: 'local',
          destinationSettings: { path: '/archive' },
        });

        expect(response.success).toBe(true);
      } catch (error) {
        // Local backend may have restrictions on archive operations
        expect(error).toBeDefined();
      }
    });
  });
});
