/**
 * E2E smoke test: exercises the MCP and Unix transports against a live
 * server. Skipped unless SMOKE_MCP_ADDR / SMOKE_UNIX_SOCK are set; launch a
 * server with scripts/start-test-server.sh first (or use `make sdk-smoke`).
 */
import { McpClient } from '../../src/clients/mcp-client';
import { UnixClient } from '../../src/clients/unix-client';
import { IObjectStoreClient } from '../../src/types';

const MCP_ADDR = process.env.SMOKE_MCP_ADDR ?? '';
const UNIX_SOCK = process.env.SMOKE_UNIX_SOCK ?? '';

const maybeDescribe = MCP_ADDR && UNIX_SOCK ? describe : describe.skip;

maybeDescribe('e2e smoke', () => {
  const roundTrip = async (name: string, client: IObjectStoreClient) => {
    const key = `smoke/typescript/${name}/obj.bin`;
    const payload = Buffer.concat([
      Buffer.from([0x00, 0x01]),
      Buffer.from(`hello from typescript ${name}`),
      Buffer.from([0xff, 0xfe]),
    ]);

    const put = await client.put({ key, data: payload });
    expect(put.success).toBe(true);

    const exists = await client.exists({ key });
    expect(exists.exists).toBe(true);

    const got = await client.get({ key });
    expect(Buffer.compare(got.data, payload)).toBe(0);

    const listing = await client.list({ prefix: `smoke/typescript/${name}` });
    expect(listing.objects.map((o) => o.key)).toContain(key);

    const del = await client.delete({ key });
    expect(del.success).toBe(true);

    const gone = await client.exists({ key });
    expect(gone.exists).toBe(false);
  };

  it('mcp transport round trip', async () => {
    const client = new McpClient({ baseUrl: `http://${MCP_ADDR}` });
    await roundTrip('mcp', client);
  });

  it('unix transport round trip', async () => {
    const client = new UnixClient({ socketPath: UNIX_SOCK });
    await roundTrip('unix', client);
  });
});
