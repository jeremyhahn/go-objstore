using FluentAssertions;
using ObjStore.SDK;
using ObjStore.SDK.Clients;
using Xunit;

namespace ObjStore.SDK.Tests.Unit;

public class ObjectStoreClientFactoryTests
{
    [Fact]
    public void Create_ShouldReturnRestClient_WhenProtocolIsREST()
    {
        // Act
        using var client = ObjectStoreClientFactory.Create("http://localhost:8080", Protocol.REST);

        // Assert
        client.Should().NotBeNull();
        client.Should().BeOfType<RestClient>();
    }

    [Fact]
    public void Create_ShouldReturnGrpcClient_WhenProtocolIsGRPC()
    {
        // Act
        using var client = ObjectStoreClientFactory.Create("http://localhost:9090", Protocol.GRPC);

        // Assert
        client.Should().NotBeNull();
        client.Should().BeOfType<GrpcClient>();
    }

    [Fact]
    public void Create_ShouldReturnQuicClient_WhenProtocolIsQUIC()
    {
        // Act
        using var client = ObjectStoreClientFactory.Create("https://localhost:8443", Protocol.QUIC);

        // Assert
        client.Should().NotBeNull();
        client.Should().BeOfType<QuicClient>();
    }

    [Fact]
    public void CreateRestClient_ShouldReturnRestClient()
    {
        // Act
        using var client = ObjectStoreClientFactory.CreateRestClient("http://localhost:8080");

        // Assert
        client.Should().NotBeNull();
        client.Should().BeOfType<RestClient>();
    }

    [Fact]
    public void CreateGrpcClient_ShouldReturnGrpcClient()
    {
        // Act
        using var client = ObjectStoreClientFactory.CreateGrpcClient("http://localhost:9090");

        // Assert
        client.Should().NotBeNull();
        client.Should().BeOfType<GrpcClient>();
    }

    [Fact]
    public void CreateQuicClient_ShouldReturnQuicClient()
    {
        // Act
        using var client = ObjectStoreClientFactory.CreateQuicClient("https://localhost:8443");

        // Assert
        client.Should().NotBeNull();
        client.Should().BeOfType<QuicClient>();
    }

    [Fact]
    public void Create_ShouldThrowArgumentNullException_WhenBaseUrlIsNull()
    {
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() =>
            ObjectStoreClientFactory.Create(null!, Protocol.REST));
    }
}
