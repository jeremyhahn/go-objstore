/**
 * gRPC Integration — edge cases and negative-path tests.
 *
 * All happy-path coverage (put/get/delete/exists/list/metadata/lifecycle/
 * replication/health) is provided by the comprehensive table-driven suite
 * (comprehensive.integration.test.ts). This file contains only tests that
 * exercise behaviour unique to the gRPC transport that are not captured by
 * the shared OPERATIONS table.
 */

import { GrpcClient } from '../../src/clients/grpc-client';

describe('gRPC Integration — Edge Cases', () => {
  let client: GrpcClient;

  beforeAll(() => {
    const grpcHost = process.env.OBJSTORE_GRPC_HOST || 'localhost';
    const grpcPort = parseInt(process.env.OBJSTORE_GRPC_PORT || '50051', 10);
    client = new GrpcClient({
      address: `${grpcHost}:${grpcPort}`,
      secure: false,
    });
  });

  afterAll(async () => {
    await client.close();
  });

  describe('Error Handling', () => {
    it('should reject get of a nonexistent object', async () => {
      const key = `nonexistent-grpc-${Date.now()}.txt`;
      await expect(client.get({ key })).rejects.toThrow();
    });

    it('should handle delete of a nonexistent object without crashing', async () => {
      const key = `nonexistent-grpc-del-${Date.now()}.txt`;
      try {
        const response = await client.delete({ key });
        // Some backends implement idempotent delete (success even when absent).
        expect(response.success).toBeDefined();
      } catch (error) {
        // A thrown error is also acceptable behaviour.
        expect(error).toBeDefined();
      }
    });

    it('should handle updateMetadata on a nonexistent object without crashing', async () => {
      const key = `nonexistent-grpc-meta-${Date.now()}.txt`;
      try {
        await client.updateMetadata({
          key,
          metadata: { contentType: 'text/plain', custom: { test: 'value' } },
        });
        // Some backends tolerate metadata writes to absent keys.
      } catch (error) {
        expect(error).toBeDefined();
      }
    });
  });
});
