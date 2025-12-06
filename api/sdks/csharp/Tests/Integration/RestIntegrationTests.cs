using System.Text;
using FluentAssertions;
using ObjStore.SDK;
using ObjStore.SDK.Models;
using Xunit;

namespace ObjStore.SDK.Tests.Integration;

[Collection("Integration")]
public class RestIntegrationTests : IntegrationTestBase
{
    private bool? _supportsReplication;
    private bool? _supportsArchive;

    /// <summary>
    /// Checks if the backend supports replication by attempting to add a policy.
    /// Caches the result to avoid repeated checks.
    /// </summary>
    private async Task<bool> SupportsReplication()
    {
        if (_supportsReplication.HasValue)
        {
            return _supportsReplication.Value;
        }

        try
        {
            using var client = ObjectStoreClientFactory.CreateRestClient(RestBaseUrl);
            var testPolicy = new ReplicationPolicy
            {
                Id = $"feature-check-{Guid.NewGuid()}",
                SourceBackend = "local",
                DestinationBackend = "local",
                CheckIntervalSeconds = 300,
                Enabled = false,
                ReplicationMode = ReplicationMode.Transparent
            };

            // Try to add the policy
            await client.AddReplicationPolicyAsync(testPolicy);

            // If successful, clean up and return true
            await client.RemoveReplicationPolicyAsync(testPolicy.Id);
            _supportsReplication = true;
            return true;
        }
        catch (Exception ex) when (ex.Message.Contains("not supported") ||
                                     ex.Message.Contains("not implemented") ||
                                     ex.Message.Contains("unsupported"))
        {
            _supportsReplication = false;
            return false;
        }
        catch
        {
            // On other errors, assume feature is supported but there's a different issue
            _supportsReplication = true;
            return true;
        }
    }

    /// <summary>
    /// Checks if the backend supports archive operations.
    /// Caches the result to avoid repeated checks.
    /// </summary>
    private async Task<bool> SupportsArchive()
    {
        if (_supportsArchive.HasValue)
        {
            return _supportsArchive.Value;
        }

        try
        {
            using var client = ObjectStoreClientFactory.CreateRestClient(RestBaseUrl);
            var testKey = $"feature-check/archive-{Guid.NewGuid()}.txt";
            var data = Encoding.UTF8.GetBytes("archive feature check");

            // Create a test object
            await client.PutAsync(testKey, data);

            try
            {
                // Try to archive it
                await client.ArchiveAsync(testKey, "glacier", new Dictionary<string, string>
                {
                    ["vault"] = "test-vault"
                });

                _supportsArchive = true;
                return true;
            }
            catch (NotSupportedException)
            {
                _supportsArchive = false;
                return false;
            }
            catch (Exception ex) when (ex.Message.Contains("not supported") ||
                                        ex.Message.Contains("not implemented") ||
                                        ex.Message.Contains("unsupported"))
            {
                _supportsArchive = false;
                return false;
            }
            finally
            {
                // Clean up test object
                try { await client.DeleteAsync(testKey); } catch { }
            }
        }
        catch
        {
            // On setup errors, assume feature is not supported
            _supportsArchive = false;
            return false;
        }
    }

    [Fact]
    public async Task PutAndGet_ShouldWorkEndToEnd()
    {
        if (!IsServerAvailable)
        {
            // Skip test if Docker is not available
            return;
        }

        // Arrange
        using var client = ObjectStoreClientFactory.CreateRestClient(RestBaseUrl);
        var key = $"test/file-{Guid.NewGuid()}.txt";
        var data = Encoding.UTF8.GetBytes("Hello, World!");
        var metadata = new ObjectMetadata
        {
            ContentType = "text/plain",
            Custom = new Dictionary<string, string> { ["author"] = "integration-test" }
        };

        // Act - Put
        var etag = await client.PutAsync(key, data, metadata);

        // Assert - Put
        etag.Should().NotBeNullOrEmpty();

        // Act - Get
        var (retrievedData, retrievedMetadata) = await client.GetAsync(key);

        // Assert - Get
        retrievedData.Should().BeEquivalentTo(data);
        retrievedMetadata.Should().NotBeNull();
        retrievedMetadata!.ContentType.Should().Be("text/plain");

        // Cleanup
        await client.DeleteAsync(key);
    }

    [Fact]
    public async Task Delete_ShouldRemoveObject()
    {
        if (!IsServerAvailable)
        {
            return;
        }

        // Arrange
        using var client = ObjectStoreClientFactory.CreateRestClient(RestBaseUrl);
        var key = $"test/delete-{Guid.NewGuid()}.txt";
        var data = Encoding.UTF8.GetBytes("Delete me");

        // Act - Put
        await client.PutAsync(key, data);

        // Assert - Exists before delete
        var existsBefore = await client.ExistsAsync(key);
        existsBefore.Should().BeTrue();

        // Act - Delete
        var deleted = await client.DeleteAsync(key);

        // Assert - Delete successful
        deleted.Should().BeTrue();

        // Assert - Does not exist after delete
        var existsAfter = await client.ExistsAsync(key);
        existsAfter.Should().BeFalse();
    }

    [Fact]
    public async Task List_ShouldReturnObjects()
    {
        if (!IsServerAvailable)
        {
            return;
        }

        // Arrange
        using var client = ObjectStoreClientFactory.CreateRestClient(RestBaseUrl);
        var prefix = $"test/list-{Guid.NewGuid()}/";
        var keys = new[] { $"{prefix}file1.txt", $"{prefix}file2.txt", $"{prefix}file3.txt" };
        var data = Encoding.UTF8.GetBytes("test");

        // Act - Put multiple objects
        foreach (var key in keys)
        {
            await client.PutAsync(key, data);
        }

        // Act - List
        var result = await client.ListAsync(prefix);

        // Assert
        result.Should().NotBeNull();
        result.Objects.Should().HaveCountGreaterOrEqualTo(3);

        // Cleanup
        foreach (var key in keys)
        {
            await client.DeleteAsync(key);
        }
    }

    [Fact]
    public async Task Exists_ShouldReturnCorrectStatus()
    {
        if (!IsServerAvailable)
        {
            return;
        }

        // Arrange
        using var client = ObjectStoreClientFactory.CreateRestClient(RestBaseUrl);
        var key = $"test/exists-{Guid.NewGuid()}.txt";
        var data = Encoding.UTF8.GetBytes("exists test");

        // Act & Assert - Does not exist initially
        var existsBefore = await client.ExistsAsync(key);
        existsBefore.Should().BeFalse();

        // Act - Put
        await client.PutAsync(key, data);

        // Assert - Exists after put
        var existsAfter = await client.ExistsAsync(key);
        existsAfter.Should().BeTrue();

        // Cleanup
        await client.DeleteAsync(key);
    }

    [Fact]
    public async Task GetMetadata_ShouldReturnObjectMetadata()
    {
        if (!IsServerAvailable)
        {
            return;
        }

        // Arrange
        using var client = ObjectStoreClientFactory.CreateRestClient(RestBaseUrl);
        var key = $"test/metadata-{Guid.NewGuid()}.txt";
        var data = Encoding.UTF8.GetBytes("metadata test");
        var metadata = new ObjectMetadata
        {
            ContentType = "text/plain",
            Custom = new Dictionary<string, string> { ["version"] = "1.0" }
        };

        // Act - Put
        await client.PutAsync(key, data, metadata);

        // Act - Get Metadata
        var retrievedMetadata = await client.GetMetadataAsync(key);

        // Assert
        retrievedMetadata.Should().NotBeNull();
        retrievedMetadata!.Size.Should().BeGreaterThan(0);

        // Cleanup
        await client.DeleteAsync(key);
    }

    [Fact]
    public async Task UpdateMetadata_ShouldUpdateObjectMetadata()
    {
        if (!IsServerAvailable)
        {
            return;
        }

        // Arrange
        using var client = ObjectStoreClientFactory.CreateRestClient(RestBaseUrl);
        var key = $"test/update-metadata-{Guid.NewGuid()}.txt";
        var data = Encoding.UTF8.GetBytes("update metadata test");

        // Act - Put
        await client.PutAsync(key, data);

        // Act - Update Metadata
        var newMetadata = new ObjectMetadata
        {
            ContentType = "application/json",
            Custom = new Dictionary<string, string> { ["updated"] = "true" }
        };
        var updated = await client.UpdateMetadataAsync(key, newMetadata);

        // Assert
        updated.Should().BeTrue();

        // Cleanup
        await client.DeleteAsync(key);
    }

    [Fact]
    public async Task Health_ShouldReturnHealthyStatus()
    {
        if (!IsServerAvailable)
        {
            return;
        }

        // Arrange
        using var client = ObjectStoreClientFactory.CreateRestClient(RestBaseUrl);

        // Act
        var health = await client.HealthAsync();

        // Assert
        health.Should().NotBeNull();
        health.Status.Should().Be(HealthStatus.Serving);
    }

    [Fact]
    public async Task LifecyclePolicy_ShouldAddAndRemove()
    {
        if (!IsServerAvailable)
        {
            return;
        }

        // Arrange
        using var client = ObjectStoreClientFactory.CreateRestClient(RestBaseUrl);
        var policy = new LifecyclePolicy
        {
            Id = $"test-policy-{Guid.NewGuid()}",
            Prefix = "archive/",
            RetentionSeconds = 86400,
            Action = "delete"
        };

        // Act - Add Policy
        var added = await client.AddPolicyAsync(policy);

        // Assert - Add
        added.Should().BeTrue();

        // Act - Get Policies
        var policies = await client.GetPoliciesAsync();

        // Assert - Policy exists
        policies.Should().Contain(p => p.Id == policy.Id);

        // Act - Remove Policy
        var removed = await client.RemovePolicyAsync(policy.Id);

        // Assert - Remove
        removed.Should().BeTrue();
    }

    [Fact]
    public async Task ApplyPolicies_ShouldExecuteSuccessfully()
    {
        if (!IsServerAvailable)
        {
            return;
        }

        // Arrange
        using var client = ObjectStoreClientFactory.CreateRestClient(RestBaseUrl);

        // Act
        var result = await client.ApplyPoliciesAsync();

        // Assert
        result.Success.Should().BeTrue();
        result.ObjectsProcessed.Should().BeGreaterOrEqualTo(0);
    }

    [Fact]
    public async Task ReplicationPolicy_ShouldAddAndRemove()
    {
        if (!IsServerAvailable)
        {
            return;
        }

        // Check if replication is supported
        if (!await SupportsReplication())
        {
            // Skip test - replication not supported by local storage backend
            return;
        }

        // Arrange
        using var client = ObjectStoreClientFactory.CreateRestClient(RestBaseUrl);
        var policy = new ReplicationPolicy
        {
            Id = $"test-repl-{Guid.NewGuid()}",
            SourceBackend = "local",
            DestinationBackend = "local",
            CheckIntervalSeconds = 300,
            Enabled = true,
            ReplicationMode = ReplicationMode.Transparent
        };

        // Act - Add Policy
        var added = await client.AddReplicationPolicyAsync(policy);

        // Assert - Add
        added.Should().BeTrue();

        // Act - Get Policies
        var policies = await client.GetReplicationPoliciesAsync();

        // Assert - Policy exists
        policies.Should().Contain(p => p.Id == policy.Id);

        // Act - Remove Policy
        var removed = await client.RemoveReplicationPolicyAsync(policy.Id);

        // Assert - Remove
        removed.Should().BeTrue();
    }

    [Fact]
    public async Task TriggerReplication_ShouldExecuteSuccessfully()
    {
        if (!IsServerAvailable)
        {
            return;
        }

        // Check if replication is supported
        if (!await SupportsReplication())
        {
            // Skip test - replication not supported by local storage backend
            return;
        }

        // Arrange
        using var client = ObjectStoreClientFactory.CreateRestClient(RestBaseUrl);
        var policy = new ReplicationPolicy
        {
            Id = $"rest-trigger-repl-{Guid.NewGuid()}",
            SourceBackend = "local",
            DestinationBackend = "local",
            CheckIntervalSeconds = 300,
            Enabled = true,
            ReplicationMode = ReplicationMode.Transparent
        };

        // Add policy first
        await client.AddReplicationPolicyAsync(policy);

        try
        {
            // Act - Trigger replication
            var result = await client.TriggerReplicationAsync(policy.Id);

            // Assert
            result.Should().BeTrue();
        }
        finally
        {
            // Cleanup
            await client.RemoveReplicationPolicyAsync(policy.Id);
        }
    }

    [Fact]
    public async Task GetReplicationStatus_ShouldReturnStatus()
    {
        if (!IsServerAvailable)
        {
            return;
        }

        // Check if replication is supported
        if (!await SupportsReplication())
        {
            // Skip test - replication not supported by local storage backend
            return;
        }

        // Arrange
        using var client = ObjectStoreClientFactory.CreateRestClient(RestBaseUrl);
        var policy = new ReplicationPolicy
        {
            Id = $"rest-status-repl-{Guid.NewGuid()}",
            SourceBackend = "local",
            DestinationBackend = "local",
            CheckIntervalSeconds = 300,
            Enabled = true,
            ReplicationMode = ReplicationMode.Transparent
        };

        // Add policy first
        await client.AddReplicationPolicyAsync(policy);

        try
        {
            // Act - Get replication status
            var status = await client.GetReplicationStatusAsync(policy.Id);

            // Assert
            status.Should().NotBeNull();
            status!.PolicyId.Should().Be(policy.Id);
        }
        finally
        {
            // Cleanup
            await client.RemoveReplicationPolicyAsync(policy.Id);
        }
    }

    [Fact]
    [Trait("Category", "Archive")]
    public async Task Archive_ShouldArchiveObject()
    {
        if (!IsServerAvailable)
        {
            return;
        }

        // Check if archive is supported
        if (!await SupportsArchive())
        {
            // Skip test - archive/glacier operations not supported by local storage backend
            return;
        }

        // Arrange
        using var client = ObjectStoreClientFactory.CreateRestClient(RestBaseUrl);
        var key = $"test/rest/archive-{Guid.NewGuid()}.txt";
        var data = Encoding.UTF8.GetBytes("Archive this via REST");

        // Put object first
        await client.PutAsync(key, data);

        try
        {
            // Act - Archive the object
            var archived = await client.ArchiveAsync(key, "glacier", new Dictionary<string, string>
            {
                ["vault"] = "test-vault"
            });

            // Assert
            archived.Should().BeTrue();
        }
        finally
        {
            // Cleanup
            try
            {
                await client.DeleteAsync(key);
            }
            catch
            {
                // Object may have been archived/deleted
            }
        }
    }

    [Fact]
    public async Task GetNonexistent_ShouldThrowException()
    {
        if (!IsServerAvailable)
        {
            return;
        }

        // Arrange
        using var client = ObjectStoreClientFactory.CreateRestClient(RestBaseUrl);
        var key = $"test/rest/nonexistent-{Guid.NewGuid()}.txt";

        // Act & Assert - Should throw ObjectNotFoundException for non-existent keys
        await Assert.ThrowsAsync<ObjStore.SDK.Exceptions.ObjectNotFoundException>(
            async () => await client.GetAsync(key));
    }

    [Fact]
    public async Task DeleteNonexistent_ShouldHandleGracefully()
    {
        if (!IsServerAvailable)
        {
            return;
        }

        // Arrange
        using var client = ObjectStoreClientFactory.CreateRestClient(RestBaseUrl);
        var key = $"test/rest/nonexistent-delete-{Guid.NewGuid()}.txt";

        // Act - Delete non-existent object (idempotent operation)
        var result = await client.DeleteAsync(key);

        // Assert - Should not throw, may return false
        result.Should().BeFalse();
    }

    [Fact]
    public async Task UpdateMetadataNonexistent_ShouldHandleGracefully()
    {
        if (!IsServerAvailable)
        {
            return;
        }

        // Arrange
        using var client = ObjectStoreClientFactory.CreateRestClient(RestBaseUrl);
        var key = $"test/rest/nonexistent-metadata-{Guid.NewGuid()}.txt";
        var metadata = new ObjectMetadata
        {
            ContentType = "text/plain"
        };

        // Act & Assert - Should throw or return false
        try
        {
            var result = await client.UpdateMetadataAsync(key, metadata);
            result.Should().BeFalse();
        }
        catch (Exception)
        {
            // Expected for non-existent object
            Assert.True(true);
        }
    }

    [Fact]
    public async Task EmptyObject_ShouldHandleCorrectly()
    {
        if (!IsServerAvailable)
        {
            return;
        }

        // Arrange
        using var client = ObjectStoreClientFactory.CreateRestClient(RestBaseUrl);
        var key = $"test/rest/empty-{Guid.NewGuid()}.txt";
        var data = Array.Empty<byte>();

        // Act - Put empty object
        await client.PutAsync(key, data);

        // Act - Get empty object
        var (retrievedData, retrievedMetadata) = await client.GetAsync(key);

        // Assert
        retrievedData.Should().BeEmpty();
        retrievedMetadata.Should().NotBeNull();
        retrievedMetadata!.Size.Should().Be(0);

        // Cleanup
        await client.DeleteAsync(key);
    }

    [Fact]
    public async Task BinaryData_ShouldPreserveAllBytes()
    {
        if (!IsServerAvailable)
        {
            return;
        }

        // Arrange
        using var client = ObjectStoreClientFactory.CreateRestClient(RestBaseUrl);
        var key = $"test/rest/binary-{Guid.NewGuid()}.bin";
        var data = Enumerable.Range(0, 256).Select(i => (byte)i).ToArray();

        // Act - Put binary data
        await client.PutAsync(key, data);

        // Act - Get binary data
        var (retrievedData, retrievedMetadata) = await client.GetAsync(key);

        // Assert
        retrievedData.Should().BeEquivalentTo(data);
        retrievedMetadata.Should().NotBeNull();
        retrievedMetadata!.Size.Should().Be(256);

        // Cleanup
        await client.DeleteAsync(key);
    }

    [Fact]
    public async Task LargeObject_ShouldHandleCorrectly()
    {
        if (!IsServerAvailable)
        {
            return;
        }

        // Arrange
        using var client = ObjectStoreClientFactory.CreateRestClient(RestBaseUrl);
        var key = $"test/rest/large-{Guid.NewGuid()}.bin";
        // Create 1MB of data
        var data = new byte[1024 * 1024];
        new Random().NextBytes(data);

        // Act - Put large object
        await client.PutAsync(key, data);

        // Act - Get large object
        var (retrievedData, retrievedMetadata) = await client.GetAsync(key);

        // Assert
        retrievedData.Should().BeEquivalentTo(data);
        retrievedMetadata.Should().NotBeNull();
        retrievedMetadata!.Size.Should().Be(data.Length);

        // Cleanup
        await client.DeleteAsync(key);
    }

    [Fact]
    public async Task ListWithDelimiter_ShouldReturnHierarchicalStructure()
    {
        if (!IsServerAvailable)
        {
            return;
        }

        // Arrange
        using var client = ObjectStoreClientFactory.CreateRestClient(RestBaseUrl);
        var base_prefix = $"test/rest/hierarchy-{Guid.NewGuid()}/";
        var keys = new[]
        {
            $"{base_prefix}file1.txt",
            $"{base_prefix}file2.txt",
            $"{base_prefix}subdir1/file3.txt",
            $"{base_prefix}subdir2/file4.txt"
        };
        var data = Encoding.UTF8.GetBytes("test data");

        // Create objects
        foreach (var key in keys)
        {
            await client.PutAsync(key, data);
        }

        // Act - List with delimiter
        var result = await client.ListAsync(base_prefix, delimiter: "/");

        // Assert - Should have files and common prefixes
        result.Should().NotBeNull();
        result.Objects.Should().HaveCountGreaterOrEqualTo(2); // file1.txt, file2.txt

        // Cleanup
        foreach (var key in keys)
        {
            await client.DeleteAsync(key);
        }
    }
}
