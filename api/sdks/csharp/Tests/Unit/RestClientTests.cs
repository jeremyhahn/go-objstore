using System.Net;
using System.Text;
using System.Text.Json;
using FluentAssertions;
using ObjStore.SDK.Clients;
using ObjStore.SDK.Models;
using Moq;
using Moq.Protected;
using Xunit;

namespace ObjStore.SDK.Tests.Unit;

public class RestClientTests : IDisposable
{
    private readonly Mock<HttpMessageHandler> _mockHandler;
    private readonly HttpClient _httpClient;
    private readonly RestClient _client;

    public RestClientTests()
    {
        _mockHandler = new Mock<HttpMessageHandler>();
        _httpClient = new HttpClient(_mockHandler.Object)
        {
            BaseAddress = new Uri("http://localhost:8080")
        };
        _client = new RestClient(_httpClient);
    }

    [Fact]
    public async Task PutAsync_ShouldUploadObject_Successfully()
    {
        // Arrange
        var key = "test/file.txt";
        var data = Encoding.UTF8.GetBytes("test content");
        var metadata = new ObjectMetadata { ContentType = "text/plain" };

        var response = new HttpResponseMessage
        {
            StatusCode = HttpStatusCode.Created,
            Content = new StringContent("", Encoding.UTF8, "application/json")
        };
        response.Headers.ETag = new System.Net.Http.Headers.EntityTagHeaderValue("\"test-etag\"");

        _mockHandler.Protected()
            .Setup<Task<HttpResponseMessage>>(
                "SendAsync",
                ItExpr.Is<HttpRequestMessage>(req =>
                    req.Method == HttpMethod.Put &&
                    req.RequestUri!.ToString().Contains(Uri.EscapeDataString(key))),
                ItExpr.IsAny<CancellationToken>())
            .ReturnsAsync(response);

        // Act
        var etag = await _client.PutAsync(key, data, metadata);

        // Assert
        etag.Should().Be("\"test-etag\"");
    }

    [Fact]
    public async Task GetAsync_ShouldRetrieveObject_Successfully()
    {
        // Arrange
        var key = "test/file.txt";
        var expectedData = Encoding.UTF8.GetBytes("test content");

        var response = new HttpResponseMessage
        {
            StatusCode = HttpStatusCode.OK,
            Content = new ByteArrayContent(expectedData)
        };
        response.Content.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue("text/plain");
        response.Headers.ETag = new System.Net.Http.Headers.EntityTagHeaderValue("\"test-etag\"");

        _mockHandler.Protected()
            .Setup<Task<HttpResponseMessage>>(
                "SendAsync",
                ItExpr.Is<HttpRequestMessage>(req =>
                    req.Method == HttpMethod.Get &&
                    req.RequestUri!.ToString().Contains(Uri.EscapeDataString(key))),
                ItExpr.IsAny<CancellationToken>())
            .ReturnsAsync(response);

        // Act
        var (data, metadata) = await _client.GetAsync(key);

        // Assert
        data.Should().BeEquivalentTo(expectedData);
        metadata.Should().NotBeNull();
        metadata!.ContentType.Should().Be("text/plain");
        metadata.ETag.Should().Be("\"test-etag\"");
    }

    [Fact]
    public async Task DeleteAsync_ShouldDeleteObject_Successfully()
    {
        // Arrange
        var key = "test/file.txt";

        _mockHandler.Protected()
            .Setup<Task<HttpResponseMessage>>(
                "SendAsync",
                ItExpr.Is<HttpRequestMessage>(req =>
                    req.Method == HttpMethod.Delete &&
                    req.RequestUri!.ToString().Contains(Uri.EscapeDataString(key))),
                ItExpr.IsAny<CancellationToken>())
            .ReturnsAsync(new HttpResponseMessage
            {
                StatusCode = HttpStatusCode.OK
            });

        // Act
        var result = await _client.DeleteAsync(key);

        // Assert
        result.Should().BeTrue();
    }

    [Fact]
    public async Task ListAsync_ShouldReturnObjects_Successfully()
    {
        // Arrange
        var prefix = "test/";
        var responseData = new ListObjectsResponse
        {
            Objects = new List<ObjectInfo>
            {
                new() { Key = "test/file1.txt", Metadata = new ObjectMetadata { Size = 100 } },
                new() { Key = "test/file2.txt", Metadata = new ObjectMetadata { Size = 200 } }
            },
            Truncated = false
        };

        var responseContent = JsonSerializer.Serialize(responseData);

        _mockHandler.Protected()
            .Setup<Task<HttpResponseMessage>>(
                "SendAsync",
                ItExpr.Is<HttpRequestMessage>(req =>
                    req.Method == HttpMethod.Get &&
                    req.RequestUri!.ToString().Contains("prefix=")),
                ItExpr.IsAny<CancellationToken>())
            .ReturnsAsync(new HttpResponseMessage
            {
                StatusCode = HttpStatusCode.OK,
                Content = new StringContent(responseContent, Encoding.UTF8, "application/json")
            });

        // Act
        var result = await _client.ListAsync(prefix);

        // Assert
        result.Should().NotBeNull();
        result.Objects.Should().HaveCount(2);
        result.Objects[0].Key.Should().Be("test/file1.txt");
    }

    [Fact]
    public async Task ExistsAsync_ShouldReturnTrue_WhenObjectExists()
    {
        // Arrange
        var key = "test/file.txt";

        _mockHandler.Protected()
            .Setup<Task<HttpResponseMessage>>(
                "SendAsync",
                ItExpr.Is<HttpRequestMessage>(req =>
                    req.Method == HttpMethod.Head &&
                    req.RequestUri!.ToString().Contains(Uri.EscapeDataString(key))),
                ItExpr.IsAny<CancellationToken>())
            .ReturnsAsync(new HttpResponseMessage
            {
                StatusCode = HttpStatusCode.OK
            });

        // Act
        var exists = await _client.ExistsAsync(key);

        // Assert
        exists.Should().BeTrue();
    }

    [Fact]
    public async Task ExistsAsync_ShouldReturnFalse_WhenObjectDoesNotExist()
    {
        // Arrange
        var key = "test/nonexistent.txt";

        _mockHandler.Protected()
            .Setup<Task<HttpResponseMessage>>(
                "SendAsync",
                ItExpr.Is<HttpRequestMessage>(req =>
                    req.Method == HttpMethod.Head),
                ItExpr.IsAny<CancellationToken>())
            .ReturnsAsync(new HttpResponseMessage
            {
                StatusCode = HttpStatusCode.NotFound
            });

        // Act
        var exists = await _client.ExistsAsync(key);

        // Assert
        exists.Should().BeFalse();
    }

    [Fact]
    public async Task HealthAsync_ShouldReturnHealthy_WhenServiceIsUp()
    {
        // Arrange
        var responseData = new { status = "healthy", version = "1.0.0" };
        var responseContent = JsonSerializer.Serialize(responseData);

        _mockHandler.Protected()
            .Setup<Task<HttpResponseMessage>>(
                "SendAsync",
                ItExpr.Is<HttpRequestMessage>(req =>
                    req.Method == HttpMethod.Get &&
                    req.RequestUri!.ToString().Contains("/health")),
                ItExpr.IsAny<CancellationToken>())
            .ReturnsAsync(new HttpResponseMessage
            {
                StatusCode = HttpStatusCode.OK,
                Content = new StringContent(responseContent, Encoding.UTF8, "application/json")
            });

        // Act
        var health = await _client.HealthAsync();

        // Assert
        health.Should().NotBeNull();
        health.Status.Should().Be(HealthStatus.Serving);
    }

    [Fact]
    public async Task UpdateMetadataAsync_ShouldUpdateMetadata_Successfully()
    {
        // Arrange
        var key = "test/file.txt";
        var metadata = new ObjectMetadata
        {
            ContentType = "application/json",
            Custom = new Dictionary<string, string> { ["author"] = "test" }
        };

        _mockHandler.Protected()
            .Setup<Task<HttpResponseMessage>>(
                "SendAsync",
                ItExpr.Is<HttpRequestMessage>(req =>
                    req.Method == HttpMethod.Put &&
                    req.RequestUri!.ToString().Contains("/metadata")),
                ItExpr.IsAny<CancellationToken>())
            .ReturnsAsync(new HttpResponseMessage
            {
                StatusCode = HttpStatusCode.OK
            });

        // Act
        var result = await _client.UpdateMetadataAsync(key, metadata);

        // Assert
        result.Should().BeTrue();
    }

    [Fact]
    public async Task AddPolicyAsync_ShouldAddLifecyclePolicy_Successfully()
    {
        // Arrange
        var policy = new LifecyclePolicy
        {
            Id = "test-policy",
            Prefix = "archive/",
            RetentionSeconds = 86400,
            Action = "delete"
        };

        _mockHandler.Protected()
            .Setup<Task<HttpResponseMessage>>(
                "SendAsync",
                ItExpr.Is<HttpRequestMessage>(req =>
                    req.Method == HttpMethod.Post &&
                    req.RequestUri!.ToString().Contains("/policies")),
                ItExpr.IsAny<CancellationToken>())
            .ReturnsAsync(new HttpResponseMessage
            {
                StatusCode = HttpStatusCode.Created
            });

        // Act
        var result = await _client.AddPolicyAsync(policy);

        // Assert
        result.Should().BeTrue();
    }

    [Fact]
    public async Task PutAsync_ShouldThrowArgumentNullException_WhenKeyIsNull()
    {
        // Arrange
        string key = null!;
        var data = new byte[] { 1, 2, 3 };

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() => _client.PutAsync(key, data));
    }

    [Fact]
    public async Task PutAsync_ShouldThrowArgumentNullException_WhenDataIsNull()
    {
        // Arrange
        var key = "test";
        byte[] data = null!;

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() => _client.PutAsync(key, data));
    }

    #region Replication Tests

    [Fact]
    public async Task AddReplicationPolicyAsync_ShouldAddPolicy_Successfully()
    {
        // Arrange
        var policy = new ReplicationPolicy
        {
            Id = "repl-1",
            SourceBackend = "s3",
            DestinationBackend = "gcs",
            CheckIntervalSeconds = 300,
            Enabled = true
        };

        _mockHandler.Protected()
            .Setup<Task<HttpResponseMessage>>(
                "SendAsync",
                ItExpr.Is<HttpRequestMessage>(req =>
                    req.Method == HttpMethod.Post &&
                    req.RequestUri!.ToString().Contains("/replication/policies")),
                ItExpr.IsAny<CancellationToken>())
            .ReturnsAsync(new HttpResponseMessage
            {
                StatusCode = HttpStatusCode.Created
            });

        // Act
        var result = await _client.AddReplicationPolicyAsync(policy);

        // Assert
        result.Should().BeTrue();
    }

    [Fact]
    public async Task AddReplicationPolicyAsync_WithNullPolicy_ThrowsArgumentNullException()
    {
        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() => _client.AddReplicationPolicyAsync(null!));
    }

    [Fact]
    public async Task RemoveReplicationPolicyAsync_ShouldRemovePolicy_Successfully()
    {
        // Arrange
        var policyId = "repl-1";

        _mockHandler.Protected()
            .Setup<Task<HttpResponseMessage>>(
                "SendAsync",
                ItExpr.Is<HttpRequestMessage>(req =>
                    req.Method == HttpMethod.Delete &&
                    req.RequestUri!.ToString().Contains($"/replication/policies/{policyId}")),
                ItExpr.IsAny<CancellationToken>())
            .ReturnsAsync(new HttpResponseMessage
            {
                StatusCode = HttpStatusCode.OK
            });

        // Act
        var result = await _client.RemoveReplicationPolicyAsync(policyId);

        // Assert
        result.Should().BeTrue();
    }

    [Fact]
    public async Task RemoveReplicationPolicyAsync_WithNullId_ThrowsArgumentNullException()
    {
        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() => _client.RemoveReplicationPolicyAsync(null!));
    }

    [Fact]
    public async Task GetReplicationPoliciesAsync_ShouldReturnPolicies_Successfully()
    {
        // Arrange
        var responseContent = JsonSerializer.Serialize(new
        {
            policies = new[]
            {
                new { id = "repl-1", source_backend = "s3", destination_backend = "gcs", enabled = true, check_interval_seconds = 300 }
            }
        });

        _mockHandler.Protected()
            .Setup<Task<HttpResponseMessage>>(
                "SendAsync",
                ItExpr.Is<HttpRequestMessage>(req =>
                    req.Method == HttpMethod.Get &&
                    req.RequestUri!.ToString().Contains("/replication/policies") &&
                    !req.RequestUri!.ToString().Contains("/replication/policies/")),
                ItExpr.IsAny<CancellationToken>())
            .ReturnsAsync(new HttpResponseMessage
            {
                StatusCode = HttpStatusCode.OK,
                Content = new StringContent(responseContent, Encoding.UTF8, "application/json")
            });

        // Act
        var result = await _client.GetReplicationPoliciesAsync();

        // Assert
        result.Should().NotBeNull();
        result.Should().HaveCount(1);
        result[0].Id.Should().Be("repl-1");
    }

    [Fact]
    public async Task GetReplicationPolicyAsync_ShouldReturnPolicy_Successfully()
    {
        // Arrange
        var policy = new ReplicationPolicy
        {
            Id = "repl-1",
            SourceBackend = "s3",
            DestinationBackend = "gcs",
            Enabled = true
        };
        var responseContent = JsonSerializer.Serialize(policy);

        _mockHandler.Protected()
            .Setup<Task<HttpResponseMessage>>(
                "SendAsync",
                ItExpr.Is<HttpRequestMessage>(req =>
                    req.Method == HttpMethod.Get &&
                    req.RequestUri!.ToString().Contains("/replication/policies/repl-1")),
                ItExpr.IsAny<CancellationToken>())
            .ReturnsAsync(new HttpResponseMessage
            {
                StatusCode = HttpStatusCode.OK,
                Content = new StringContent(responseContent, Encoding.UTF8, "application/json")
            });

        // Act
        var result = await _client.GetReplicationPolicyAsync("repl-1");

        // Assert
        result.Should().NotBeNull();
        result!.Id.Should().Be("repl-1");
    }

    [Fact]
    public async Task GetReplicationPolicyAsync_WithNullId_ThrowsArgumentNullException()
    {
        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() => _client.GetReplicationPolicyAsync(null!));
    }

    [Fact]
    public async Task TriggerReplicationAsync_ShouldTrigger_Successfully()
    {
        // Arrange
        _mockHandler.Protected()
            .Setup<Task<HttpResponseMessage>>(
                "SendAsync",
                ItExpr.Is<HttpRequestMessage>(req =>
                    req.Method == HttpMethod.Post &&
                    req.RequestUri!.ToString().Contains("/replication/trigger")),
                ItExpr.IsAny<CancellationToken>())
            .ReturnsAsync(new HttpResponseMessage
            {
                StatusCode = HttpStatusCode.OK
            });

        // Act
        var result = await _client.TriggerReplicationAsync("repl-1");

        // Assert
        result.Should().BeTrue();
    }

    [Fact]
    public async Task TriggerReplicationAsync_WithParallelOptions_ShouldTrigger_Successfully()
    {
        // Arrange
        _mockHandler.Protected()
            .Setup<Task<HttpResponseMessage>>(
                "SendAsync",
                ItExpr.Is<HttpRequestMessage>(req =>
                    req.Method == HttpMethod.Post &&
                    req.RequestUri!.ToString().Contains("/replication/trigger")),
                ItExpr.IsAny<CancellationToken>())
            .ReturnsAsync(new HttpResponseMessage
            {
                StatusCode = HttpStatusCode.OK
            });

        // Act
        var result = await _client.TriggerReplicationAsync("repl-1", true, 8);

        // Assert
        result.Should().BeTrue();
    }

    [Fact]
    public async Task GetReplicationStatusAsync_ShouldReturnStatus_Successfully()
    {
        // Arrange
        var status = new ReplicationStatus
        {
            PolicyId = "repl-1",
            SourceBackend = "s3",
            DestinationBackend = "gcs",
            TotalObjectsSynced = 100,
            TotalBytesSynced = 1048576,
            Enabled = true
        };
        var responseContent = JsonSerializer.Serialize(status);

        _mockHandler.Protected()
            .Setup<Task<HttpResponseMessage>>(
                "SendAsync",
                ItExpr.Is<HttpRequestMessage>(req =>
                    req.Method == HttpMethod.Get &&
                    req.RequestUri!.ToString().Contains("/replication/status/repl-1")),
                ItExpr.IsAny<CancellationToken>())
            .ReturnsAsync(new HttpResponseMessage
            {
                StatusCode = HttpStatusCode.OK,
                Content = new StringContent(responseContent, Encoding.UTF8, "application/json")
            });

        // Act
        var result = await _client.GetReplicationStatusAsync("repl-1");

        // Assert
        result.Should().NotBeNull();
        result!.PolicyId.Should().Be("repl-1");
        result.TotalObjectsSynced.Should().Be(100);
    }

    [Fact]
    public async Task GetReplicationStatusAsync_WithNullId_ThrowsArgumentNullException()
    {
        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() => _client.GetReplicationStatusAsync(null!));
    }

    [Fact]
    public async Task GetReplicationStatusAsync_ShouldReturnNull_WhenNotFound()
    {
        // Arrange
        _mockHandler.Protected()
            .Setup<Task<HttpResponseMessage>>(
                "SendAsync",
                ItExpr.Is<HttpRequestMessage>(req =>
                    req.Method == HttpMethod.Get &&
                    req.RequestUri!.ToString().Contains("/replication/status/")),
                ItExpr.IsAny<CancellationToken>())
            .ReturnsAsync(new HttpResponseMessage
            {
                StatusCode = HttpStatusCode.NotFound
            });

        // Act
        var result = await _client.GetReplicationStatusAsync("nonexistent");

        // Assert
        result.Should().BeNull();
    }

    #endregion

    #region Additional REST Client Tests

    [Fact]
    public async Task GetAsync_WithNullKey_ThrowsArgumentNullException()
    {
        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() => _client.GetAsync(null!));
    }

    [Fact]
    public async Task DeleteAsync_WithNullKey_ThrowsArgumentNullException()
    {
        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() => _client.DeleteAsync(null!));
    }

    [Fact]
    public async Task ExistsAsync_WithNullKey_ThrowsArgumentNullException()
    {
        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() => _client.ExistsAsync(null!));
    }

    [Fact]
    public async Task GetMetadataAsync_WithNullKey_ThrowsArgumentNullException()
    {
        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() => _client.GetMetadataAsync(null!));
    }

    [Fact]
    public async Task UpdateMetadataAsync_WithNullKey_ThrowsArgumentNullException()
    {
        // Arrange
        var metadata = new ObjectMetadata { ContentType = "text/plain" };

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() => _client.UpdateMetadataAsync(null!, metadata));
    }

    [Fact]
    public async Task UpdateMetadataAsync_WithNullMetadata_ThrowsArgumentNullException()
    {
        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() => _client.UpdateMetadataAsync("test-key", null!));
    }

    [Fact]
    public async Task ArchiveAsync_WithNullKey_ThrowsArgumentNullException()
    {
        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() => _client.ArchiveAsync(null!, "glacier"));
    }

    [Fact]
    public async Task ArchiveAsync_WithNullDestinationType_ThrowsArgumentNullException()
    {
        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() => _client.ArchiveAsync("test-key", null!));
    }

    [Fact]
    public async Task AddPolicyAsync_WithNullPolicy_ThrowsArgumentNullException()
    {
        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() => _client.AddPolicyAsync(null!));
    }

    [Fact]
    public async Task RemovePolicyAsync_WithNullId_ThrowsArgumentNullException()
    {
        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() => _client.RemovePolicyAsync(null!));
    }

    [Fact]
    public async Task RemovePolicyAsync_ShouldRemovePolicy_Successfully()
    {
        // Arrange
        var policyId = "test-policy";

        _mockHandler.Protected()
            .Setup<Task<HttpResponseMessage>>(
                "SendAsync",
                ItExpr.Is<HttpRequestMessage>(req =>
                    req.Method == HttpMethod.Delete &&
                    req.RequestUri!.ToString().Contains($"/policies/{policyId}")),
                ItExpr.IsAny<CancellationToken>())
            .ReturnsAsync(new HttpResponseMessage
            {
                StatusCode = HttpStatusCode.OK
            });

        // Act
        var result = await _client.RemovePolicyAsync(policyId);

        // Assert
        result.Should().BeTrue();
    }

    [Fact]
    public async Task GetPoliciesAsync_ShouldReturnPolicies_Successfully()
    {
        // Arrange
        var responseContent = JsonSerializer.Serialize(new
        {
            policies = new[]
            {
                new { id = "policy-1", prefix = "test/", action = "delete", retention_seconds = 86400 }
            }
        });

        _mockHandler.Protected()
            .Setup<Task<HttpResponseMessage>>(
                "SendAsync",
                ItExpr.Is<HttpRequestMessage>(req =>
                    req.Method == HttpMethod.Get &&
                    req.RequestUri!.ToString().EndsWith("/policies")),
                ItExpr.IsAny<CancellationToken>())
            .ReturnsAsync(new HttpResponseMessage
            {
                StatusCode = HttpStatusCode.OK,
                Content = new StringContent(responseContent, Encoding.UTF8, "application/json")
            });

        // Act
        var result = await _client.GetPoliciesAsync();

        // Assert
        result.Should().NotBeNull();
        result.Should().HaveCount(1);
        result[0].Id.Should().Be("policy-1");
    }

    [Fact]
    public async Task ApplyPoliciesAsync_ShouldApplyPolicies_Successfully()
    {
        // Arrange
        var responseContent = JsonSerializer.Serialize(new
        {
            policies_count = 2,
            objects_processed = 10
        });

        _mockHandler.Protected()
            .Setup<Task<HttpResponseMessage>>(
                "SendAsync",
                ItExpr.Is<HttpRequestMessage>(req =>
                    req.Method == HttpMethod.Post &&
                    req.RequestUri!.ToString().Contains("/policies/apply")),
                ItExpr.IsAny<CancellationToken>())
            .ReturnsAsync(new HttpResponseMessage
            {
                StatusCode = HttpStatusCode.OK,
                Content = new StringContent(responseContent, Encoding.UTF8, "application/json")
            });

        // Act
        var (success, policiesCount, objectsProcessed) = await _client.ApplyPoliciesAsync();

        // Assert
        success.Should().BeTrue();
        policiesCount.Should().Be(2);
        objectsProcessed.Should().Be(10);
    }

    [Fact]
    public async Task ArchiveAsync_ShouldArchive_Successfully()
    {
        // Arrange
        var key = "test/file.txt";

        _mockHandler.Protected()
            .Setup<Task<HttpResponseMessage>>(
                "SendAsync",
                ItExpr.Is<HttpRequestMessage>(req =>
                    req.Method == HttpMethod.Post &&
                    req.RequestUri!.ToString().Contains("/archive")),
                ItExpr.IsAny<CancellationToken>())
            .ReturnsAsync(new HttpResponseMessage
            {
                StatusCode = HttpStatusCode.OK
            });

        // Act
        var result = await _client.ArchiveAsync(key, "glacier");

        // Assert
        result.Should().BeTrue();
    }

    [Fact]
    public async Task PutWithMetadataAsync_WithNullMetadata_ThrowsArgumentNullException()
    {
        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            _client.PutWithMetadataAsync("test-key", new byte[] { 1, 2, 3 }, null!));
    }

    [Fact]
    public void Dispose_CanBeCalledMultipleTimes()
    {
        // Arrange
        var handler = new Mock<HttpMessageHandler>();
        var httpClient = new HttpClient(handler.Object) { BaseAddress = new Uri("http://localhost:8080") };
        using var client = new RestClient(httpClient);

        // Act & Assert - should not throw
        client.Dispose();
        client.Dispose();
    }

    #endregion

    public void Dispose()
    {
        _client?.Dispose();
        _httpClient?.Dispose();
    }
}
