using FluentAssertions;
using ObjStore.SDK.Clients;
using ObjStore.SDK.Exceptions;
using ObjStore.SDK.Models;
using Grpc.Core;
using Grpc.Net.Client;
using Moq;
using Objstore.V1;
using Xunit;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

namespace ObjStore.SDK.Tests.Unit;

public class GrpcClientTests : IDisposable
{
    private readonly Mock<ObjectStore.ObjectStoreClient> _mockGrpcClient;
    private bool _disposed;

    public GrpcClientTests()
    {
        _mockGrpcClient = new Mock<ObjectStore.ObjectStoreClient>();
    }

    #region Constructor Tests

    [Fact]
    public void Constructor_WithNullChannel_ThrowsArgumentNullException()
    {
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => new GrpcClient((GrpcChannel)null!));
    }

    [Fact]
    public void Constructor_WithValidAddress_CreatesClient()
    {
        // Arrange & Act
        using var client = new GrpcClient("http://localhost:50051");

        // Assert - no exception means success
        client.Should().NotBeNull();
    }

    #endregion

    #region PutAsync Tests

    [Fact]
    public async Task PutAsync_WithNullKey_ThrowsArgumentNullException()
    {
        // Arrange
        using var client = new GrpcClient("http://localhost:50051");

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() => client.PutAsync(null!, new byte[] { 1, 2, 3 }));
    }

    [Fact]
    public async Task PutAsync_WithNullData_ThrowsArgumentNullException()
    {
        // Arrange
        using var client = new GrpcClient("http://localhost:50051");

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() => client.PutAsync("test-key", null!));
    }

    #endregion

    #region GetAsync Tests

    [Fact]
    public async Task GetAsync_WithNullKey_ThrowsArgumentNullException()
    {
        // Arrange
        using var client = new GrpcClient("http://localhost:50051");

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() => client.GetAsync(null!));
    }

    #endregion

    #region DeleteAsync Tests

    [Fact]
    public async Task DeleteAsync_WithNullKey_ThrowsArgumentNullException()
    {
        // Arrange
        using var client = new GrpcClient("http://localhost:50051");

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() => client.DeleteAsync(null!));
    }

    #endregion

    #region ExistsAsync Tests

    [Fact]
    public async Task ExistsAsync_WithNullKey_ThrowsArgumentNullException()
    {
        // Arrange
        using var client = new GrpcClient("http://localhost:50051");

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() => client.ExistsAsync(null!));
    }

    #endregion

    #region GetMetadataAsync Tests

    [Fact]
    public async Task GetMetadataAsync_WithNullKey_ThrowsArgumentNullException()
    {
        // Arrange
        using var client = new GrpcClient("http://localhost:50051");

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() => client.GetMetadataAsync(null!));
    }

    #endregion

    #region UpdateMetadataAsync Tests

    [Fact]
    public async Task UpdateMetadataAsync_WithNullKey_ThrowsArgumentNullException()
    {
        // Arrange
        using var client = new GrpcClient("http://localhost:50051");
        var metadata = new ObjectMetadata { ContentType = "text/plain" };

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() => client.UpdateMetadataAsync(null!, metadata));
    }

    [Fact]
    public async Task UpdateMetadataAsync_WithNullMetadata_ThrowsArgumentNullException()
    {
        // Arrange
        using var client = new GrpcClient("http://localhost:50051");

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() => client.UpdateMetadataAsync("test-key", null!));
    }

    #endregion

    #region ArchiveAsync Tests

    [Fact]
    public async Task ArchiveAsync_WithNullKey_ThrowsArgumentNullException()
    {
        // Arrange
        using var client = new GrpcClient("http://localhost:50051");

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() => client.ArchiveAsync(null!, "glacier"));
    }

    [Fact]
    public async Task ArchiveAsync_WithNullDestinationType_ThrowsArgumentNullException()
    {
        // Arrange
        using var client = new GrpcClient("http://localhost:50051");

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() => client.ArchiveAsync("test-key", null!));
    }

    #endregion

    #region AddPolicyAsync Tests

    [Fact]
    public async Task AddPolicyAsync_WithNullPolicy_ThrowsArgumentNullException()
    {
        // Arrange
        using var client = new GrpcClient("http://localhost:50051");

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() => client.AddPolicyAsync(null!));
    }

    #endregion

    #region RemovePolicyAsync Tests

    [Fact]
    public async Task RemovePolicyAsync_WithNullId_ThrowsArgumentNullException()
    {
        // Arrange
        using var client = new GrpcClient("http://localhost:50051");

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() => client.RemovePolicyAsync(null!));
    }

    #endregion

    #region AddReplicationPolicyAsync Tests

    [Fact]
    public async Task AddReplicationPolicyAsync_WithNullPolicy_ThrowsArgumentNullException()
    {
        // Arrange
        using var client = new GrpcClient("http://localhost:50051");

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() => client.AddReplicationPolicyAsync(null!));
    }

    #endregion

    #region RemoveReplicationPolicyAsync Tests

    [Fact]
    public async Task RemoveReplicationPolicyAsync_WithNullId_ThrowsArgumentNullException()
    {
        // Arrange
        using var client = new GrpcClient("http://localhost:50051");

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() => client.RemoveReplicationPolicyAsync(null!));
    }

    #endregion

    #region GetReplicationPolicyAsync Tests

    [Fact]
    public async Task GetReplicationPolicyAsync_WithNullId_ThrowsArgumentNullException()
    {
        // Arrange
        using var client = new GrpcClient("http://localhost:50051");

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() => client.GetReplicationPolicyAsync(null!));
    }

    #endregion

    #region GetReplicationStatusAsync Tests

    [Fact]
    public async Task GetReplicationStatusAsync_WithNullId_ThrowsArgumentNullException()
    {
        // Arrange
        using var client = new GrpcClient("http://localhost:50051");

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() => client.GetReplicationStatusAsync(null!));
    }

    #endregion

    #region PutWithMetadataAsync Tests

    [Fact]
    public async Task PutWithMetadataAsync_WithNullMetadata_ThrowsArgumentNullException()
    {
        // Arrange
        using var client = new GrpcClient("http://localhost:50051");

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            client.PutWithMetadataAsync("test-key", new byte[] { 1, 2, 3 }, null!));
    }

    #endregion

    #region Dispose Tests

    [Fact]
    public void Dispose_CanBeCalledMultipleTimes()
    {
        // Arrange
        var client = new GrpcClient("http://localhost:50051");

        // Act & Assert - should not throw
        client.Dispose();
        client.Dispose();
    }

    #endregion

    public void Dispose()
    {
        if (_disposed)
            return;
        _disposed = true;
    }
}
