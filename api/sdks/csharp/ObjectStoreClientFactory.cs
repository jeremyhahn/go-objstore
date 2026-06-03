using ObjStore.SDK.Clients;
using Grpc.Net.Client;
using Microsoft.Extensions.Logging;

namespace ObjStore.SDK;

/// <summary>
/// Protocol types supported by the SDK.
/// </summary>
public enum Protocol
{
    REST,
    GRPC,
    QUIC,
    /// <summary>
    /// MCP (Model Context Protocol) transport — HTTP POST JSON-RPC 2.0 to the base URL.
    /// </summary>
    MCP,
    /// <summary>
    /// Unix domain socket transport — newline-delimited JSON-RPC 2.0 over a local socket path.
    /// </summary>
    Unix
}

/// <summary>
/// Factory for creating ObjectStore clients with different protocols.
/// </summary>
public static class ObjectStoreClientFactory
{
    /// <summary>
    /// Creates a client with the specified protocol.
    /// </summary>
    /// <param name="baseUrl">
    /// Base URL for the service (e.g., "http://localhost:8080") for network transports;
    /// for the Unix transport this value is treated as the socket file path.
    /// </param>
    /// <param name="protocol">Protocol to use.</param>
    /// <param name="logger">Optional logger.</param>
    /// <returns>IObjectStoreClient instance.</returns>
    public static IObjectStoreClient Create(string baseUrl, Protocol protocol, ILogger? logger = null)
    {
        ArgumentNullException.ThrowIfNull(baseUrl);

        return protocol switch
        {
            Protocol.REST => new RestClient(baseUrl, logger as ILogger<RestClient>),
            Protocol.GRPC => new GrpcClient(baseUrl, logger as ILogger<GrpcClient>),
            Protocol.QUIC => new QuicClient(baseUrl, logger as ILogger<QuicClient>),
            Protocol.MCP => new McpClient(baseUrl, logger as ILogger<McpClient>),
            Protocol.Unix => new UnixClient(baseUrl, logger as ILogger<UnixClient>),
            _ => throw new ArgumentException($"Unsupported protocol: {protocol}", nameof(protocol))
        };
    }

    /// <summary>
    /// Creates a REST client.
    /// </summary>
    public static IObjectStoreClient CreateRestClient(string baseUrl, ILogger<RestClient>? logger = null)
    {
        return new RestClient(baseUrl, logger);
    }

    /// <summary>
    /// Creates a gRPC client.
    /// </summary>
    public static IObjectStoreClient CreateGrpcClient(string address, ILogger<GrpcClient>? logger = null)
    {
        return new GrpcClient(address, logger);
    }

    /// <summary>
    /// Creates a gRPC client with custom channel.
    /// </summary>
    public static IObjectStoreClient CreateGrpcClient(GrpcChannel channel, ILogger<GrpcClient>? logger = null)
    {
        return new GrpcClient(channel, logger);
    }

    /// <summary>
    /// Creates a QUIC/HTTP3 client.
    /// </summary>
    public static IObjectStoreClient CreateQuicClient(string baseUrl, ILogger<QuicClient>? logger = null)
    {
        return new QuicClient(baseUrl, logger);
    }

    /// <summary>
    /// Creates an MCP (Model Context Protocol) client.
    /// </summary>
    /// <param name="baseUrl">Base URL of the MCP HTTP endpoint (e.g., "http://localhost:8081").</param>
    /// <param name="logger">Optional logger.</param>
    public static IObjectStoreClient CreateMcpClient(string baseUrl, ILogger<McpClient>? logger = null)
    {
        return new McpClient(baseUrl, logger);
    }

    /// <summary>
    /// Creates a Unix domain socket client.
    /// </summary>
    /// <param name="socketPath">Filesystem path of the Unix socket (e.g., "/var/run/objstore.sock").</param>
    /// <param name="logger">Optional logger.</param>
    public static IObjectStoreClient CreateUnixClient(string socketPath, ILogger<UnixClient>? logger = null)
    {
        return new UnixClient(socketPath, logger);
    }
}
