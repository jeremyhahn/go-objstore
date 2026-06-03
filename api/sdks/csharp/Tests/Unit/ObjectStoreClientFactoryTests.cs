using System.Net;
using System.Text.Json;
using FluentAssertions;
using ObjStore.SDK;
using ObjStore.SDK.Clients;
using ObjStore.SDK.Models;
using Moq;
using Xunit;

namespace ObjStore.SDK.Tests.Unit;

/// <summary>
/// Canonical unified-client / factory tests. Confirms the factory delegates to the correct
/// protocol client for each of REST/gRPC/QUIC, that a representative call routes through the
/// chosen protocol, and that Dispose releases resources safely (and is idempotent).
/// </summary>
public class ObjectStoreClientFactoryTests
{
    // ---------------------------------------------------------------- delegation by protocol

    [Fact]
    public void Unified_Delegates_Rest()
    {
        using var client = ObjectStoreClientFactory.Create("http://localhost:8080", Protocol.REST);
        client.Should().BeOfType<RestClient>();
    }

    [Fact]
    public void Unified_Delegates_Grpc()
    {
        using var client = ObjectStoreClientFactory.Create("http://localhost:9090", Protocol.GRPC);
        client.Should().BeOfType<GrpcClient>();
    }

    [Fact]
    public void Unified_Delegates_Quic()
    {
        using var client = ObjectStoreClientFactory.Create("https://localhost:8443", Protocol.QUIC);
        client.Should().BeOfType<QuicClient>();
    }

    [Fact]
    public void Unified_CreateRestClient_ReturnsRest()
    {
        using var client = ObjectStoreClientFactory.CreateRestClient("http://localhost:8080");
        client.Should().BeOfType<RestClient>();
    }

    [Fact]
    public void Unified_CreateGrpcClient_ReturnsGrpc()
    {
        using var client = ObjectStoreClientFactory.CreateGrpcClient("http://localhost:9090");
        client.Should().BeOfType<GrpcClient>();
    }

    [Fact]
    public void Unified_CreateQuicClient_ReturnsQuic()
    {
        using var client = ObjectStoreClientFactory.CreateQuicClient("https://localhost:8443");
        client.Should().BeOfType<QuicClient>();
    }

    [Fact]
    public void Unified_NullBaseUrl_Throws()
    {
        Assert.Throws<ArgumentNullException>(() => ObjectStoreClientFactory.Create(null!, Protocol.REST));
    }

    // ---------------------------------------------------------------- representative call routes through

    [Fact]
    public async Task Unified_Rest_RoutesHealthCall()
    {
        // A representative call (health) issued through a REST-backed unified client reaches the
        // HTTP transport and returns the parsed result.
        var handler = new Mock<HttpMessageHandler>();
        handler.SetupJson(
            req => req.RequestUri!.ToString().Contains("/health"),
            JsonSerializer.Serialize(new { status = "healthy", version = "1.0.0" }));
        var http = new HttpClient(handler.Object) { BaseAddress = new Uri("http://localhost:8080") };

        IObjectStoreClient client = new RestClient(http);
        var health = await client.HealthAsync();

        health.Status.Should().Be(HealthStatus.Serving);
    }

    // ---------------------------------------------------------------- close / dispose

    [Fact]
    public void Unified_Close_Rest()
    {
        var client = ObjectStoreClientFactory.Create("http://localhost:8080", Protocol.REST);
        client.Dispose();
        client.Dispose(); // idempotent
    }

    [Fact]
    public void Unified_Close_Grpc()
    {
        var client = ObjectStoreClientFactory.Create("http://localhost:9090", Protocol.GRPC);
        client.Dispose();
        client.Dispose(); // idempotent
    }

    [Fact]
    public void Unified_Close_Quic()
    {
        var client = ObjectStoreClientFactory.Create("https://localhost:8443", Protocol.QUIC);
        client.Dispose();
        client.Dispose(); // idempotent
    }

    [Fact]
    public void Unified_Constructor_NullHttpClient_Throws()
    {
        Assert.Throws<ArgumentNullException>(() => new RestClient((HttpClient)null!));
        Assert.Throws<ArgumentNullException>(() => new QuicClient((HttpClient)null!));
        Assert.Throws<ArgumentNullException>(() => new GrpcClient((Grpc.Net.Client.GrpcChannel)null!));
    }

    [Fact]
    public void Unified_CreateGrpcClient_FromChannel_ReturnsGrpc()
    {
        // The channel-based gRPC factory overload returns a GrpcClient bound to that channel.
        using var channel = Grpc.Net.Client.GrpcChannel.ForAddress("http://localhost:9090");
        using var client = ObjectStoreClientFactory.CreateGrpcClient(channel);
        client.Should().BeOfType<GrpcClient>();
    }

    [Fact]
    public void Unified_UnsupportedProtocol_Throws()
    {
        // An out-of-range protocol value hits the factory's default switch arm.
        Assert.Throws<ArgumentException>(() =>
            ObjectStoreClientFactory.Create("http://localhost:8080", (Protocol)999));
    }
}
