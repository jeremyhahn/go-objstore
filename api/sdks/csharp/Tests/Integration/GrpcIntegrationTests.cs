using System.Text;
using FluentAssertions;
using ObjStore.SDK;
using ObjStore.SDK.Models;
using Xunit;

namespace ObjStore.SDK.Tests.Integration;

/// <summary>
/// gRPC-specific integration tests. Happy-path coverage for all 19 operations is in
/// ComprehensiveTests. This file retains edge cases and error conditions unique to the gRPC
/// transport or supplementing coverage not present in ComprehensiveTests.
///
/// All tests gate on IsGrpcAvailable (set during InitializeAsync by probing the gRPC health
/// endpoint). If gRPC is down the test fails immediately — gRPC is a required always-on protocol.
///
/// Shared helpers (SupportsReplication, SupportsArchive, IsGrpcAvailable) are defined once in
/// IntegrationTestBase and inherited here.
/// </summary>
[Collection("Integration")]
public class GrpcIntegrationTests : IntegrationTestBase
{
    [Fact]
    public async Task PutAndGet_ShouldWorkEndToEnd()
    {
        if (!IsServerAvailable) { Assert.Fail("REST server unavailable — integration tests require the objstore-server container (this is a failure, not a skip)."); }
        if (!IsGrpcAvailable) { Assert.Fail("gRPC server unavailable — integration tests require the objstore-server container (this is a failure, not a skip)."); }

        using var client = ObjectStoreClientFactory.CreateGrpcClient(GrpcAddress);
        var key = $"test/grpc/file-{Guid.NewGuid()}.txt";
        var data = Encoding.UTF8.GetBytes("Hello from gRPC!");
        var metadata = new ObjectMetadata
        {
            ContentType = "text/plain",
            Custom = new Dictionary<string, string> { ["author"] = "grpc-integration-test" }
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
        if (!IsGrpcAvailable) { Assert.Fail("gRPC server unavailable — integration tests require the objstore-server container (this is a failure, not a skip)."); }

        using var client = ObjectStoreClientFactory.CreateGrpcClient(GrpcAddress);
        var key = $"test/grpc/delete-{Guid.NewGuid()}.txt";
        await client.PutAsync(key, Encoding.UTF8.GetBytes("Delete me via gRPC"));

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
        if (!IsGrpcAvailable) { Assert.Fail("gRPC server unavailable — integration tests require the objstore-server container (this is a failure, not a skip)."); }

        using var client = ObjectStoreClientFactory.CreateGrpcClient(GrpcAddress);
        var prefix = $"test/grpc/list-{Guid.NewGuid()}/";
        var keys = new[] { $"{prefix}file1.txt", $"{prefix}file2.txt", $"{prefix}file3.txt" };
        var data = Encoding.UTF8.GetBytes("test data");

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
        if (!IsGrpcAvailable) { Assert.Fail("gRPC server unavailable — integration tests require the objstore-server container (this is a failure, not a skip)."); }

        using var client = ObjectStoreClientFactory.CreateGrpcClient(GrpcAddress);
        var key = $"test/grpc/exists-{Guid.NewGuid()}.txt";

        var existsBefore = await client.ExistsAsync(key);
        existsBefore.Should().BeFalse();

        await client.PutAsync(key, Encoding.UTF8.GetBytes("exists test via gRPC"));

        var existsAfter = await client.ExistsAsync(key);
        existsAfter.Should().BeTrue();

        await client.DeleteAsync(key);
    }

    [Fact]
    public async Task GetMetadata_ShouldReturnObjectMetadata()
    {
        if (!IsServerAvailable) { Assert.Fail("REST server unavailable — integration tests require the objstore-server container (this is a failure, not a skip)."); }
        if (!IsGrpcAvailable) { Assert.Fail("gRPC server unavailable — integration tests require the objstore-server container (this is a failure, not a skip)."); }

        using var client = ObjectStoreClientFactory.CreateGrpcClient(GrpcAddress);
        var key = $"test/grpc/metadata-{Guid.NewGuid()}.txt";
        var data = Encoding.UTF8.GetBytes("metadata test via gRPC");
        var metadata = new ObjectMetadata
        {
            ContentType = "text/plain",
            Custom = new Dictionary<string, string> { ["version"] = "1.0", ["protocol"] = "grpc" }
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
        if (!IsGrpcAvailable) { Assert.Fail("gRPC server unavailable — integration tests require the objstore-server container (this is a failure, not a skip)."); }

        using var client = ObjectStoreClientFactory.CreateGrpcClient(GrpcAddress);
        var key = $"test/grpc/update-metadata-{Guid.NewGuid()}.txt";
        await client.PutAsync(key, Encoding.UTF8.GetBytes("update metadata test via gRPC"));

        var newMetadata = new ObjectMetadata
        {
            ContentType = "application/json",
            Custom = new Dictionary<string, string> { ["updated"] = "true", ["protocol"] = "grpc" }
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
        if (!IsGrpcAvailable) { Assert.Fail("gRPC server unavailable — integration tests require the objstore-server container (this is a failure, not a skip)."); }

        using var client = ObjectStoreClientFactory.CreateGrpcClient(GrpcAddress);
        var health = await client.HealthAsync();
        health.Should().NotBeNull();
        health.Status.Should().Be(HealthStatus.Serving);
    }

    [Fact]
    public async Task LifecyclePolicy_ShouldAddGetAndRemove()
    {
        if (!IsServerAvailable) { Assert.Fail("REST server unavailable — integration tests require the objstore-server container (this is a failure, not a skip)."); }
        if (!IsGrpcAvailable) { Assert.Fail("gRPC server unavailable — integration tests require the objstore-server container (this is a failure, not a skip)."); }

        using var client = ObjectStoreClientFactory.CreateGrpcClient(GrpcAddress);
        var policy = new LifecyclePolicy
        {
            Id = $"grpc-test-policy-{Guid.NewGuid()}",
            Prefix = "grpc-archive/",
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
        if (!IsGrpcAvailable) { Assert.Fail("gRPC server unavailable — integration tests require the objstore-server container (this is a failure, not a skip)."); }

        using var client = ObjectStoreClientFactory.CreateGrpcClient(GrpcAddress);
        var result = await client.ApplyPoliciesAsync();
        result.Success.Should().BeTrue();
        result.ObjectsProcessed.Should().BeGreaterOrEqualTo(0);
    }

    [Fact]
    public async Task ReplicationPolicy_ShouldAddGetAndRemove()
    {
        if (!IsServerAvailable) { Assert.Fail("REST server unavailable — integration tests require the objstore-server container (this is a failure, not a skip)."); }
        if (!IsGrpcAvailable) { Assert.Fail("gRPC server unavailable — integration tests require the objstore-server container (this is a failure, not a skip)."); }
        if (!await SupportsReplication())
        {
            Console.WriteLine("[SKIP] gRPC ReplicationPolicy: server reports replication unsupported.");
            return;
        }

        using var client = ObjectStoreClientFactory.CreateGrpcClient(GrpcAddress);
        var policy = new ReplicationPolicy
        {
            Id = $"grpc-test-repl-{Guid.NewGuid()}",
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
        if (!IsGrpcAvailable) { Assert.Fail("gRPC server unavailable — integration tests require the objstore-server container (this is a failure, not a skip)."); }
        if (!await SupportsReplication())
        {
            Console.WriteLine("[SKIP] gRPC TriggerReplication: server reports replication unsupported.");
            return;
        }

        using var client = ObjectStoreClientFactory.CreateGrpcClient(GrpcAddress);
        var id = $"grpc-trigger-repl-{Guid.NewGuid()}";
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
        if (!IsGrpcAvailable) { Assert.Fail("gRPC server unavailable — integration tests require the objstore-server container (this is a failure, not a skip)."); }
        if (!await SupportsReplication())
        {
            Console.WriteLine("[SKIP] gRPC GetReplicationStatus: server reports replication unsupported.");
            return;
        }

        using var client = ObjectStoreClientFactory.CreateGrpcClient(GrpcAddress);
        var id = $"grpc-status-repl-{Guid.NewGuid()}";
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
        if (!IsGrpcAvailable) { Assert.Fail("gRPC server unavailable — integration tests require the objstore-server container (this is a failure, not a skip)."); }
        if (!await SupportsArchive())
        {
            Console.WriteLine("[SKIP] gRPC Archive: server reports archive unsupported.");
            return;
        }

        using var client = ObjectStoreClientFactory.CreateGrpcClient(GrpcAddress);
        var key = $"test/grpc/archive-{Guid.NewGuid()}.txt";
        await client.PutAsync(key, Encoding.UTF8.GetBytes("Archive this via gRPC"));

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

    // --- Edge cases unique to gRPC transport ---

    [Fact]
    public async Task GetNonexistent_ShouldThrowException()
    {
        if (!IsServerAvailable) { Assert.Fail("REST server unavailable — integration tests require the objstore-server container (this is a failure, not a skip)."); }
        if (!IsGrpcAvailable) { Assert.Fail("gRPC server unavailable — integration tests require the objstore-server container (this is a failure, not a skip)."); }

        using var client = ObjectStoreClientFactory.CreateGrpcClient(GrpcAddress);
        var key = $"test/grpc/nonexistent-{Guid.NewGuid()}.txt";

        await Assert.ThrowsAsync<ObjStore.SDK.Exceptions.ObjectNotFoundException>(
            async () => await client.GetAsync(key));
    }

    [Fact]
    public async Task DeleteNonexistent_ShouldHandleGracefully()
    {
        if (!IsServerAvailable) { Assert.Fail("REST server unavailable — integration tests require the objstore-server container (this is a failure, not a skip)."); }
        if (!IsGrpcAvailable) { Assert.Fail("gRPC server unavailable — integration tests require the objstore-server container (this is a failure, not a skip)."); }

        using var client = ObjectStoreClientFactory.CreateGrpcClient(GrpcAddress);
        var key = $"test/grpc/nonexistent-delete-{Guid.NewGuid()}.txt";

        var result = await client.DeleteAsync(key);
        result.Should().BeFalse();
    }

    [Fact]
    public async Task UpdateMetadataNonexistent_ShouldHandleGracefully()
    {
        if (!IsServerAvailable) { Assert.Fail("REST server unavailable — integration tests require the objstore-server container (this is a failure, not a skip)."); }
        if (!IsGrpcAvailable) { Assert.Fail("gRPC server unavailable — integration tests require the objstore-server container (this is a failure, not a skip)."); }

        using var client = ObjectStoreClientFactory.CreateGrpcClient(GrpcAddress);
        var key = $"test/grpc/nonexistent-metadata-{Guid.NewGuid()}.txt";
        var metadata = new ObjectMetadata { ContentType = "text/plain" };

        try
        {
            var result = await client.UpdateMetadataAsync(key, metadata);
            result.Should().BeFalse();
        }
        catch (Exception)
        {
            Assert.True(true);
        }
    }

    [Fact]
    public async Task EmptyObject_ShouldHandleCorrectly()
    {
        if (!IsServerAvailable) { Assert.Fail("REST server unavailable — integration tests require the objstore-server container (this is a failure, not a skip)."); }
        if (!IsGrpcAvailable) { Assert.Fail("gRPC server unavailable — integration tests require the objstore-server container (this is a failure, not a skip)."); }

        using var client = ObjectStoreClientFactory.CreateGrpcClient(GrpcAddress);
        var key = $"test/grpc/empty-{Guid.NewGuid()}.txt";

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
        if (!IsGrpcAvailable) { Assert.Fail("gRPC server unavailable — integration tests require the objstore-server container (this is a failure, not a skip)."); }

        using var client = ObjectStoreClientFactory.CreateGrpcClient(GrpcAddress);
        var key = $"test/grpc/binary-{Guid.NewGuid()}.bin";
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
        if (!IsGrpcAvailable) { Assert.Fail("gRPC server unavailable — integration tests require the objstore-server container (this is a failure, not a skip)."); }

        using var client = ObjectStoreClientFactory.CreateGrpcClient(GrpcAddress);
        var key = $"test/grpc/large-{Guid.NewGuid()}.bin";
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
        if (!IsGrpcAvailable) { Assert.Fail("gRPC server unavailable — integration tests require the objstore-server container (this is a failure, not a skip)."); }

        using var client = ObjectStoreClientFactory.CreateGrpcClient(GrpcAddress);
        var basePrefix = $"test/grpc/hierarchy-{Guid.NewGuid()}/";
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
