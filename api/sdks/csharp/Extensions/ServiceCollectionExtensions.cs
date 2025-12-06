using ObjStore.SDK.Clients;
using Grpc.Net.Client;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace ObjStore.SDK.Extensions;

/// <summary>
/// Extension methods for configuring ObjectStore services in DI container
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Adds ObjectStore REST client to the service collection with typed HttpClient
    /// </summary>
    /// <param name="services">The service collection</param>
    /// <param name="configureOptions">Configuration action for client options</param>
    /// <returns>IHttpClientBuilder for further configuration</returns>
    public static IHttpClientBuilder AddObjectStoreRestClient(
        this IServiceCollection services,
        Action<ObjectStoreClientOptions> configureOptions)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configureOptions);

        services.Configure(configureOptions);

        return services.AddHttpClient<IObjectStoreClient, RestClient>((serviceProvider, client) =>
        {
            var options = serviceProvider.GetRequiredService<IOptions<ObjectStoreClientOptions>>().Value;
            client.BaseAddress = new Uri(options.BaseUrl);

            if (options.Timeout.HasValue)
            {
                client.Timeout = options.Timeout.Value;
            }
        });
    }

    /// <summary>
    /// Adds ObjectStore REST client to the service collection from configuration
    /// </summary>
    /// <param name="services">The service collection</param>
    /// <param name="configuration">Configuration section containing ObjectStore settings</param>
    /// <returns>IHttpClientBuilder for further configuration</returns>
    public static IHttpClientBuilder AddObjectStoreRestClient(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configuration);

        services.Configure<ObjectStoreClientOptions>(configuration);

        return services.AddHttpClient<IObjectStoreClient, RestClient>((serviceProvider, client) =>
        {
            var options = serviceProvider.GetRequiredService<IOptions<ObjectStoreClientOptions>>().Value;
            client.BaseAddress = new Uri(options.BaseUrl);

            if (options.Timeout.HasValue)
            {
                client.Timeout = options.Timeout.Value;
            }
        });
    }

    /// <summary>
    /// Adds ObjectStore gRPC client to the service collection
    /// </summary>
    /// <param name="services">The service collection</param>
    /// <param name="configureOptions">Configuration action for client options</param>
    /// <returns>The service collection</returns>
    public static IServiceCollection AddObjectStoreGrpcClient(
        this IServiceCollection services,
        Action<ObjectStoreClientOptions> configureOptions)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configureOptions);

        services.Configure(configureOptions);

        services.AddSingleton<IObjectStoreClient>(serviceProvider =>
        {
            var options = serviceProvider.GetRequiredService<IOptions<ObjectStoreClientOptions>>().Value;
            var logger = serviceProvider.GetService<ILogger<GrpcClient>>();

            var channelOptions = new GrpcChannelOptions();
            if (options.MaxMessageSize.HasValue)
            {
                channelOptions.MaxReceiveMessageSize = options.MaxMessageSize;
                channelOptions.MaxSendMessageSize = options.MaxMessageSize;
            }

            var channel = GrpcChannel.ForAddress(options.BaseUrl, channelOptions);
            return new GrpcClient(channel, logger);
        });

        return services;
    }

    /// <summary>
    /// Adds ObjectStore gRPC client to the service collection from configuration
    /// </summary>
    /// <param name="services">The service collection</param>
    /// <param name="configuration">Configuration section containing ObjectStore settings</param>
    /// <returns>The service collection</returns>
    public static IServiceCollection AddObjectStoreGrpcClient(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configuration);

        services.Configure<ObjectStoreClientOptions>(configuration);

        services.AddSingleton<IObjectStoreClient>(serviceProvider =>
        {
            var options = serviceProvider.GetRequiredService<IOptions<ObjectStoreClientOptions>>().Value;
            var logger = serviceProvider.GetService<ILogger<GrpcClient>>();

            var channelOptions = new GrpcChannelOptions();
            if (options.MaxMessageSize.HasValue)
            {
                channelOptions.MaxReceiveMessageSize = options.MaxMessageSize;
                channelOptions.MaxSendMessageSize = options.MaxMessageSize;
            }

            var channel = GrpcChannel.ForAddress(options.BaseUrl, channelOptions);
            return new GrpcClient(channel, logger);
        });

        return services;
    }

    /// <summary>
    /// Adds ObjectStore QUIC/HTTP3 client to the service collection with typed HttpClient
    /// </summary>
    /// <param name="services">The service collection</param>
    /// <param name="configureOptions">Configuration action for client options</param>
    /// <returns>IHttpClientBuilder for further configuration</returns>
    public static IHttpClientBuilder AddObjectStoreQuicClient(
        this IServiceCollection services,
        Action<ObjectStoreClientOptions> configureOptions)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configureOptions);

        services.Configure(configureOptions);

        return services.AddHttpClient<IObjectStoreClient, QuicClient>((serviceProvider, client) =>
        {
            var options = serviceProvider.GetRequiredService<IOptions<ObjectStoreClientOptions>>().Value;
            client.BaseAddress = new Uri(options.BaseUrl);
            client.DefaultRequestVersion = System.Net.HttpVersion.Version30;
            client.DefaultVersionPolicy = System.Net.Http.HttpVersionPolicy.RequestVersionOrHigher;

            if (options.Timeout.HasValue)
            {
                client.Timeout = options.Timeout.Value;
            }
        })
        .ConfigurePrimaryHttpMessageHandler(() => new System.Net.Http.SocketsHttpHandler
        {
            EnableMultipleHttp2Connections = true,
            PooledConnectionLifetime = TimeSpan.FromMinutes(5)
        });
    }

    /// <summary>
    /// Adds ObjectStore QUIC/HTTP3 client to the service collection from configuration
    /// </summary>
    /// <param name="services">The service collection</param>
    /// <param name="configuration">Configuration section containing ObjectStore settings</param>
    /// <returns>IHttpClientBuilder for further configuration</returns>
    public static IHttpClientBuilder AddObjectStoreQuicClient(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configuration);

        services.Configure<ObjectStoreClientOptions>(configuration);

        return services.AddHttpClient<IObjectStoreClient, QuicClient>((serviceProvider, client) =>
        {
            var options = serviceProvider.GetRequiredService<IOptions<ObjectStoreClientOptions>>().Value;
            client.BaseAddress = new Uri(options.BaseUrl);
            client.DefaultRequestVersion = System.Net.HttpVersion.Version30;
            client.DefaultVersionPolicy = System.Net.Http.HttpVersionPolicy.RequestVersionOrHigher;

            if (options.Timeout.HasValue)
            {
                client.Timeout = options.Timeout.Value;
            }
        })
        .ConfigurePrimaryHttpMessageHandler(() => new System.Net.Http.SocketsHttpHandler
        {
            EnableMultipleHttp2Connections = true,
            PooledConnectionLifetime = TimeSpan.FromMinutes(5)
        });
    }
}
