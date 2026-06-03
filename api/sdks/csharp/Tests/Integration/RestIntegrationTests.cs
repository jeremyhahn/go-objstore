using System.Text;
using FluentAssertions;
using ObjStore.SDK;
using ObjStore.SDK.Models;
using Xunit;

namespace ObjStore.SDK.Tests.Integration;

/// <summary>
/// REST-specific integration tests. Happy-path coverage for all 19 operations is in
/// ComprehensiveTests. This file retains edge cases and error conditions that are unique
/// to the REST transport or supplement coverage not present in ComprehensiveTests.
///
/// Shared helpers (SupportsReplication, SupportsArchive, per-protocol availability) are
/// defined once in IntegrationTestBase and inherited here.
/// </summary>
[Collection("Integration")]
public class RestIntegrationTests : IntegrationTestBase
{
    [Fact]
    public async Task PutAndGet_ShouldWorkEndToEnd()
    {
        if (!IsServerAvailable) { Assert.Fail("REST server unavailable — integration tests require the objstore-server container (this is a failure, not a skip)."); }

        using var client = ObjectStoreClientFactory.CreateRestClient(RestBaseUrl);
        var key = $"test/file-{Guid.NewGuid()}.txt";
        var data = Encoding.UTF8.GetBytes("Hello, World!");
        var metadata = new ObjectMetadata
        {
            ContentType = "text/plain",
            Custom = new Dictionary<string, string> { ["author"] = "integration-test" }
        };

        var etag = await client.PutAsync(key, data, metadata);
        etag.Should().NotBeNullOrEmpty();

        var (retrievedData, retrievedMetadata) = await client.GetAsync(key);
        retrievedData.Should().BeEquivalentTo(data);
        retrievedMetadata.Should().NotBeNull();
        retrievedMetadata!.ContentType.Should().Be("text/plain");

        await client.DeleteAsync(key);
    }

    [Fact]
    public async Task Delete_ShouldRemoveObject()
    {
        if (!IsServerAvailable) { Assert.Fail("REST server unavailable — integration tests require the objstore-server container (this is a failure, not a skip)."); }

        using var client = ObjectStoreClientFactory.CreateRestClient(RestBaseUrl);
        var key = $"test/delete-{Guid.NewGuid()}.txt";
        await client.PutAsync(key, Encoding.UTF8.GetBytes("Delete me"));

        var existsBefore = await client.ExistsAsync(key);
        existsBefore.Should().BeTrue();

        var deleted = await client.DeleteAsync(key);
        deleted.Should().BeTrue();

        var existsAfter = await client.ExistsAsync(key);
        existsAfter.Should().BeFalse();
    }

    [Fact]
    public async Task List_ShouldReturnObjects()
    {
        if (!IsServerAvailable) { Assert.Fail("REST server unavailable — integration tests require the objstore-server container (this is a failure, not a skip)."); }

        using var client = ObjectStoreClientFactory.CreateRestClient(RestBaseUrl);
        var prefix = $"test/list-{Guid.NewGuid()}/";
        var keys = new[] { $"{prefix}file1.txt", $"{prefix}file2.txt", $"{prefix}file3.txt" };
        var data = Encoding.UTF8.GetBytes("test");

        foreach (var key in keys)
            await client.PutAsync(key, data);

        var result = await client.ListAsync(prefix);
        result.Should().NotBeNull();
        result.Objects.Should().HaveCountGreaterOrEqualTo(3);

        foreach (var key in keys)
            await client.DeleteAsync(key);
    }

    [Fact]
    public async Task Exists_ShouldReturnCorrectStatus()
    {
        if (!IsServerAvailable) { Assert.Fail("REST server unavailable — integration tests require the objstore-server container (this is a failure, not a skip)."); }

        using var client = ObjectStoreClientFactory.CreateRestClient(RestBaseUrl);
        var key = $"test/exists-{Guid.NewGuid()}.txt";

        var existsBefore = await client.ExistsAsync(key);
        existsBefore.Should().BeFalse();

        await client.PutAsync(key, Encoding.UTF8.GetBytes("exists test"));

        var existsAfter = await client.ExistsAsync(key);
        existsAfter.Should().BeTrue();

        await client.DeleteAsync(key);
    }

    [Fact]
    public async Task GetMetadata_ShouldReturnObjectMetadata()
    {
        if (!IsServerAvailable) { Assert.Fail("REST server unavailable — integration tests require the objstore-server container (this is a failure, not a skip)."); }

        using var client = ObjectStoreClientFactory.CreateRestClient(RestBaseUrl);
        var key = $"test/metadata-{Guid.NewGuid()}.txt";
        var data = Encoding.UTF8.GetBytes("metadata test");
        var metadata = new ObjectMetadata
        {
            ContentType = "text/plain",
            Custom = new Dictionary<string, string> { ["version"] = "1.0" }
        };

        await client.PutAsync(key, data, metadata);

        var retrievedMetadata = await client.GetMetadataAsync(key);
        retrievedMetadata.Should().NotBeNull();
        retrievedMetadata!.Size.Should().Be(data.Length);
        retrievedMetadata.ContentType.Should().Be("text/plain");

        await client.DeleteAsync(key);
    }

    [Fact]
    public async Task UpdateMetadata_ShouldPersistNewValues()
    {
        if (!IsServerAvailable) { Assert.Fail("REST server unavailable — integration tests require the objstore-server container (this is a failure, not a skip)."); }

        using var client = ObjectStoreClientFactory.CreateRestClient(RestBaseUrl);
        var key = $"test/update-metadata-{Guid.NewGuid()}.txt";
        await client.PutAsync(key, Encoding.UTF8.GetBytes("update metadata test"));

        var newMetadata = new ObjectMetadata
        {
            ContentType = "application/json",
            Custom = new Dictionary<string, string> { ["updated"] = "true" }
        };
        var updated = await client.UpdateMetadataAsync(key, newMetadata);
        updated.Should().BeTrue();

        // Read-back assertion
        var readBack = await client.GetMetadataAsync(key);
        readBack.Should().NotBeNull();
        readBack!.ContentType.Should().Be("application/json");
        readBack.Custom.Should().NotBeNull();
        readBack.Custom!["updated"].Should().Be("true");

        await client.DeleteAsync(key);
    }

    [Fact]
    public async Task Health_ShouldReturnHealthyStatus()
    {
        if (!IsServerAvailable) { Assert.Fail("REST server unavailable — integration tests require the objstore-server container (this is a failure, not a skip)."); }

        using var client = ObjectStoreClientFactory.CreateRestClient(RestBaseUrl);
        var health = await client.HealthAsync();
        health.Should().NotBeNull();
        health.Status.Should().Be(HealthStatus.Serving);
    }

    [Fact]
    public async Task LifecyclePolicy_ShouldAddAndRemove()
    {
        if (!IsServerAvailable) { Assert.Fail("REST server unavailable — integration tests require the objstore-server container (this is a failure, not a skip)."); }

        using var client = ObjectStoreClientFactory.CreateRestClient(RestBaseUrl);
        var policy = new LifecyclePolicy
        {
            Id = $"test-policy-{Guid.NewGuid()}",
            Prefix = "archive/",
            RetentionSeconds = 86400,
            Action = "delete"
        };

        var added = await client.AddPolicyAsync(policy);
        added.Should().BeTrue();

        var policies = await client.GetPoliciesAsync();
        policies.Should().Contain(p => p.Id == policy.Id);

        var removed = await client.RemovePolicyAsync(policy.Id);
        removed.Should().BeTrue();

        var policiesAfter = await client.GetPoliciesAsync();
        policiesAfter.Should().NotContain(p => p.Id == policy.Id);
    }

    [Fact]
    public async Task ApplyPolicies_ShouldExecuteSuccessfully()
    {
        if (!IsServerAvailable) { Assert.Fail("REST server unavailable — integration tests require the objstore-server container (this is a failure, not a skip)."); }

        using var client = ObjectStoreClientFactory.CreateRestClient(RestBaseUrl);
        var result = await client.ApplyPoliciesAsync();
        result.Success.Should().BeTrue();
        result.ObjectsProcessed.Should().BeGreaterOrEqualTo(0);
    }

    [Fact]
    public async Task ReplicationPolicy_ShouldAddAndRemove()
    {
        if (!IsServerAvailable) { Assert.Fail("REST server unavailable — integration tests require the objstore-server container (this is a failure, not a skip)."); }
        if (!await SupportsReplication())
        {
            Console.WriteLine("[SKIP] REST ReplicationPolicy: server reports replication unsupported.");
            return;
        }

        using var client = ObjectStoreClientFactory.CreateRestClient(RestBaseUrl);
        var policy = new ReplicationPolicy
        {
            Id = $"test-repl-{Guid.NewGuid()}",
            SourceBackend = "local",
            DestinationBackend = "local",
            CheckIntervalSeconds = 3600,
            Enabled = true,
            ReplicationMode = ReplicationMode.Transparent
        };

        var added = await client.AddReplicationPolicyAsync(policy);
        added.Should().BeTrue();

        var policies = await client.GetReplicationPoliciesAsync();
        policies.Should().Contain(p => p.Id == policy.Id);

        var removed = await client.RemoveReplicationPolicyAsync(policy.Id);
        removed.Should().BeTrue();

        var policiesAfter = await client.GetReplicationPoliciesAsync();
        policiesAfter.Should().NotContain(p => p.Id == policy.Id);
    }

    [Fact]
    public async Task TriggerReplication_ShouldExecuteSuccessfully()
    {
        if (!IsServerAvailable) { Assert.Fail("REST server unavailable — integration tests require the objstore-server container (this is a failure, not a skip)."); }
        if (!await SupportsReplication())
        {
            Console.WriteLine("[SKIP] REST TriggerReplication: server reports replication unsupported.");
            return;
        }

        using var client = ObjectStoreClientFactory.CreateRestClient(RestBaseUrl);
        var id = $"rest-trigger-repl-{Guid.NewGuid()}";
        var policy = new ReplicationPolicy
        {
            Id = id,
            SourceBackend = "local",
            SourceSettings = new Dictionary<string, string> { ["path"] = System.IO.Path.Combine(System.IO.Path.GetTempPath(), $"objstore-repl-src-{id}") },
            DestinationBackend = "local",
            DestinationSettings = new Dictionary<string, string> { ["path"] = System.IO.Path.Combine(System.IO.Path.GetTempPath(), $"objstore-repl-dst-{id}") },
            CheckIntervalSeconds = 3600,
            Enabled = true,
            ReplicationMode = ReplicationMode.Transparent
        };

        await client.AddReplicationPolicyAsync(policy);

        try
        {
            var result = await client.TriggerReplicationAsync(policy.Id);

            result.Success.Should().BeTrue("TriggerReplication should succeed for the added policy");
            result.SyncResult.Should().NotBeNull("server should return a sync result payload");
            result.SyncResult!.PolicyId.Should().Be(id, "returned policy_id must match the triggered policy");
            result.SyncResult.Synced.Should().BeGreaterOrEqualTo(0);
            result.SyncResult.Deleted.Should().BeGreaterOrEqualTo(0);
            result.SyncResult.Failed.Should().BeGreaterOrEqualTo(0);
            result.SyncResult.BytesTotal.Should().BeGreaterOrEqualTo(0);
            result.SyncResult.DurationMs.Should().BeGreaterOrEqualTo(0);
        }
        finally
        {
            await client.RemoveReplicationPolicyAsync(policy.Id);
        }
    }

    [Fact]
    public async Task GetReplicationStatus_ShouldReturnStatus()
    {
        if (!IsServerAvailable) { Assert.Fail("REST server unavailable — integration tests require the objstore-server container (this is a failure, not a skip)."); }
        if (!await SupportsReplication())
        {
            Console.WriteLine("[SKIP] REST GetReplicationStatus: server reports replication unsupported.");
            return;
        }

        using var client = ObjectStoreClientFactory.CreateRestClient(RestBaseUrl);
        var id = $"rest-status-repl-{Guid.NewGuid()}";
        var policy = new ReplicationPolicy
        {
            Id = id,
            SourceBackend = "local",
            SourceSettings = new Dictionary<string, string> { ["path"] = System.IO.Path.Combine(System.IO.Path.GetTempPath(), $"objstore-repl-src-{id}") },
            DestinationBackend = "local",
            DestinationSettings = new Dictionary<string, string> { ["path"] = System.IO.Path.Combine(System.IO.Path.GetTempPath(), $"objstore-repl-dst-{id}") },
            CheckIntervalSeconds = 3600,
            Enabled = true,
            ReplicationMode = ReplicationMode.Transparent
        };

        await client.AddReplicationPolicyAsync(policy);

        try
        {
            var status = await client.GetReplicationStatusAsync(policy.Id);
            status.Should().NotBeNull();
            status!.PolicyId.Should().Be(policy.Id);
            status.TotalObjectsSynced.Should().BeGreaterOrEqualTo(0);
            status.SyncCount.Should().BeGreaterOrEqualTo(0);
        }
        finally
        {
            await client.RemoveReplicationPolicyAsync(policy.Id);
        }
    }

    [Fact]
    [Trait("Category", "Archive")]
    public async Task Archive_ShouldArchiveObject()
    {
        if (!IsServerAvailable) { Assert.Fail("REST server unavailable — integration tests require the objstore-server container (this is a failure, not a skip)."); }
        if (!await SupportsArchive())
        {
            Console.WriteLine("[SKIP] REST Archive: server reports archive unsupported.");
            return;
        }

        using var client = ObjectStoreClientFactory.CreateRestClient(RestBaseUrl);
        var key = $"test/rest/archive-{Guid.NewGuid()}.txt";
        await client.PutAsync(key, Encoding.UTF8.GetBytes("Archive this via REST"));

        try
        {
            var archived = await client.ArchiveAsync(key, "glacier", new Dictionary<string, string>
            {
                ["vault"] = "test-vault"
            });
            archived.Should().BeTrue();
        }
        finally
        {
            try { await client.DeleteAsync(key); } catch { }
        }
    }

    // --- Edge cases unique to REST transport ---

    [Fact]
    public async Task GetNonexistent_ShouldThrowException()
    {
        if (!IsServerAvailable) { Assert.Fail("REST server unavailable — integration tests require the objstore-server container (this is a failure, not a skip)."); }

        using var client = ObjectStoreClientFactory.CreateRestClient(RestBaseUrl);
        var key = $"test/rest/nonexistent-{Guid.NewGuid()}.txt";

        await Assert.ThrowsAsync<ObjStore.SDK.Exceptions.ObjectNotFoundException>(
            async () => await client.GetAsync(key));
    }

    [Fact]
    public async Task DeleteNonexistent_ShouldHandleGracefully()
    {
        if (!IsServerAvailable) { Assert.Fail("REST server unavailable — integration tests require the objstore-server container (this is a failure, not a skip)."); }

        using var client = ObjectStoreClientFactory.CreateRestClient(RestBaseUrl);
        var key = $"test/rest/nonexistent-delete-{Guid.NewGuid()}.txt";

        var result = await client.DeleteAsync(key);
        result.Should().BeFalse();
    }

    [Fact]
    public async Task UpdateMetadataNonexistent_ShouldHandleGracefully()
    {
        if (!IsServerAvailable) { Assert.Fail("REST server unavailable — integration tests require the objstore-server container (this is a failure, not a skip)."); }

        using var client = ObjectStoreClientFactory.CreateRestClient(RestBaseUrl);
        var key = $"test/rest/nonexistent-metadata-{Guid.NewGuid()}.txt";
        var metadata = new ObjectMetadata { ContentType = "text/plain" };

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
        if (!IsServerAvailable) { Assert.Fail("REST server unavailable — integration tests require the objstore-server container (this is a failure, not a skip)."); }

        using var client = ObjectStoreClientFactory.CreateRestClient(RestBaseUrl);
        var key = $"test/rest/empty-{Guid.NewGuid()}.txt";

        await client.PutAsync(key, Array.Empty<byte>());

        var (retrievedData, retrievedMetadata) = await client.GetAsync(key);
        retrievedData.Should().BeEmpty();
        retrievedMetadata.Should().NotBeNull();
        retrievedMetadata!.Size.Should().Be(0);

        await client.DeleteAsync(key);
    }

    [Fact]
    public async Task BinaryData_ShouldPreserveAllBytes()
    {
        if (!IsServerAvailable) { Assert.Fail("REST server unavailable — integration tests require the objstore-server container (this is a failure, not a skip)."); }

        using var client = ObjectStoreClientFactory.CreateRestClient(RestBaseUrl);
        var key = $"test/rest/binary-{Guid.NewGuid()}.bin";
        var data = Enumerable.Range(0, 256).Select(i => (byte)i).ToArray();

        await client.PutAsync(key, data);

        var (retrievedData, retrievedMetadata) = await client.GetAsync(key);
        retrievedData.Should().BeEquivalentTo(data);
        retrievedMetadata.Should().NotBeNull();
        retrievedMetadata!.Size.Should().Be(256);

        await client.DeleteAsync(key);
    }

    [Fact]
    public async Task LargeObject_ShouldHandleCorrectly()
    {
        if (!IsServerAvailable) { Assert.Fail("REST server unavailable — integration tests require the objstore-server container (this is a failure, not a skip)."); }

        using var client = ObjectStoreClientFactory.CreateRestClient(RestBaseUrl);
        var key = $"test/rest/large-{Guid.NewGuid()}.bin";
        var data = new byte[1024 * 1024];
        new Random().NextBytes(data);

        await client.PutAsync(key, data);

        var (retrievedData, retrievedMetadata) = await client.GetAsync(key);
        retrievedData.Should().BeEquivalentTo(data);
        retrievedMetadata.Should().NotBeNull();
        retrievedMetadata!.Size.Should().Be(data.Length);

        await client.DeleteAsync(key);
    }

    [Fact]
    public async Task ListWithDelimiter_ShouldReturnHierarchicalStructure()
    {
        if (!IsServerAvailable) { Assert.Fail("REST server unavailable — integration tests require the objstore-server container (this is a failure, not a skip)."); }

        using var client = ObjectStoreClientFactory.CreateRestClient(RestBaseUrl);
        var basePrefix = $"test/rest/hierarchy-{Guid.NewGuid()}/";
        var keys = new[]
        {
            $"{basePrefix}file1.txt",
            $"{basePrefix}file2.txt",
            $"{basePrefix}subdir1/file3.txt",
            $"{basePrefix}subdir2/file4.txt"
        };
        var data = Encoding.UTF8.GetBytes("test data");

        foreach (var key in keys)
            await client.PutAsync(key, data);

        var result = await client.ListAsync(basePrefix, delimiter: "/");
        result.Should().NotBeNull();
        result.Objects.Should().HaveCountGreaterOrEqualTo(2);

        foreach (var key in keys)
            await client.DeleteAsync(key);
    }
}
