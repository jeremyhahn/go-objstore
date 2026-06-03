using System.Text;
using ObjStore.SDK;
using ObjStore.SDK.Clients;
using Xunit;

namespace ObjStore.SDK.Tests.E2E;

/// <summary>
/// E2E smoke tests: exercise the MCP and Unix transports against a live
/// server. They pass trivially (no-op) unless SMOKE_MCP_ADDR /
/// SMOKE_UNIX_SOCK are set; launch a server with
/// scripts/start-test-server.sh first (or use `make sdk-smoke`).
/// </summary>
[Trait("Category", "E2E")]
public class SmokeTests
{
    private static readonly string? McpAddr = Environment.GetEnvironmentVariable("SMOKE_MCP_ADDR");
    private static readonly string? UnixSock = Environment.GetEnvironmentVariable("SMOKE_UNIX_SOCK");

    private static bool Enabled => !string.IsNullOrEmpty(McpAddr) && !string.IsNullOrEmpty(UnixSock);

    private static async Task RoundTripAsync(string name, IObjectStoreClient client)
    {
        var key = $"smoke/csharp/{name}/obj.bin";
        var payload = new byte[] { 0x00, 0x01 }
            .Concat(Encoding.UTF8.GetBytes($"hello from csharp {name}"))
            .Concat(new byte[] { 0xFF, 0xFE })
            .ToArray();

        await client.PutAsync(key, payload);

        Assert.True(await client.ExistsAsync(key), $"{name}: object should exist");

        var (data, _) = await client.GetAsync(key);
        Assert.Equal(payload, data);

        var listing = await client.ListAsync(prefix: $"smoke/csharp/{name}");
        Assert.Contains(listing.Objects, o => o.Key == key);

        Assert.True(await client.DeleteAsync(key), $"{name}: delete failed");
        Assert.False(await client.ExistsAsync(key), $"{name}: object should be gone");
    }

    [Fact]
    public async Task Mcp_Transport_RoundTrip()
    {
        if (!Enabled)
        {
            return; // SMOKE_MCP_ADDR / SMOKE_UNIX_SOCK not set
        }
        using var client = new McpClient($"http://{McpAddr}");
        await RoundTripAsync("mcp", client);
    }

    [Fact]
    public async Task Unix_Transport_RoundTrip()
    {
        if (!Enabled)
        {
            return; // SMOKE_MCP_ADDR / SMOKE_UNIX_SOCK not set
        }
        using var client = new UnixClient(UnixSock!);
        await RoundTripAsync("unix", client);
    }
}
