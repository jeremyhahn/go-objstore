/**
 * REST Integration — edge cases and negative-path tests.
 *
 * All happy-path coverage (put/get/delete/exists/list/metadata/lifecycle/
 * replication/health) is provided by the comprehensive table-driven suite
 * (comprehensive.integration.test.ts). This file contains only tests that
 * exercise behaviour specific to the REST transport that are not captured by
 * the shared OPERATIONS table.
 */

import { RestClient } from '../../src/clients/rest-client';

describe('REST Integration — Edge Cases', () => {
  let client: RestClient;

  beforeAll(() => {
    client = new RestClient({
      baseUrl: process.env.OBJSTORE_REST_URL || 'http://localhost:8080',
      timeout: 30000,
    });
  });

  afterAll(async () => {
    await client.close();
  });

  describe('Error Handling', () => {
    it('should reject get of a nonexistent object', async () => {
      const key = `nonexistent-rest-${Date.now()}.txt`;
      await expect(client.get({ key })).rejects.toThrow();
    });

    it('should handle delete of a nonexistent object without crashing', async () => {
      const key = `nonexistent-rest-del-${Date.now()}.txt`;
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
      const key = `nonexistent-rest-meta-${Date.now()}.txt`;
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
