using ObjStore.SDK.Clients;
using Grpc.Net.Client;
using Microsoft.Extensions.Logging;

namespace ObjStore.SDK;

/// <summary>
/// Protocol types supported by the SDK
/// </summary>
public enum Protocol
{
    REST,
    GRPC,
    QUIC
}

/// <summary>
/// Factory for creating ObjectStore clients with different protocols
/// </summary>
public static class ObjectStoreClientFactory
{
    /// <summary>
    /// Creates a client with the specified protocol
    /// </summary>
    /// <param name="baseUrl">Base URL for the service (e.g., "http://localhost:8080")</param>
    /// <param name="protocol">Protocol to use</param>
    /// <param name="logger">Optional logger</param>
    /// <returns>IObjectStoreClient instance</returns>
    public static IObjectStoreClient Create(string baseUrl, Protocol protocol, ILogger? logger = null)
    {
        ArgumentNullException.ThrowIfNull(baseUrl);

        return protocol switch
        {
            Protocol.REST => new RestClient(baseUrl, logger as ILogger<RestClient>),
            Protocol.GRPC => new GrpcClient(baseUrl, logger as ILogger<GrpcClient>),
            Protocol.QUIC => new QuicClient(baseUrl, logger as ILogger<QuicClient>),
            _ => throw new ArgumentException($"Unsupported protocol: {protocol}", nameof(protocol))
        };
    }

    /// <summary>
    /// Creates a REST client
    /// </summary>
    public static IObjectStoreClient CreateRestClient(string baseUrl, ILogger<RestClient>? logger = null)
    {
        return new RestClient(baseUrl, logger);
    }

    /// <summary>
    /// Creates a gRPC client
    /// </summary>
    public static IObjectStoreClient CreateGrpcClient(string address, ILogger<GrpcClient>? logger = null)
    {
        return new GrpcClient(address, logger);
    }

    /// <summary>
    /// Creates a gRPC client with custom channel
    /// </summary>
    public static IObjectStoreClient CreateGrpcClient(GrpcChannel channel, ILogger<GrpcClient>? logger = null)
    {
        return new GrpcClient(channel, logger);
    }

    /// <summary>
    /// Creates a QUIC/HTTP3 client
    /// </summary>
    public static IObjectStoreClient CreateQuicClient(string baseUrl, ILogger<QuicClient>? logger = null)
    {
        return new QuicClient(baseUrl, logger);
    }
}
