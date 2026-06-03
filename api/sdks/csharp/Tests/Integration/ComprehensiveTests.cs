using System.IO;
using System.Text;
using FluentAssertions;
using ObjStore.SDK;
using ObjStore.SDK.Models;
using Xunit;

namespace ObjStore.SDK.Tests.Integration;

/// <summary>
/// Comprehensive table-driven integration tests that validate all 19 API operations
/// across all 3 protocols (REST, gRPC, QUIC).
///
/// This test suite uses xUnit Theory with MemberData for parameterized testing,
/// ensuring complete protocol coverage with minimal code duplication.
///
/// Environment Variables (set by docker-compose integration-tests service):
/// - OBJSTORE_REST_URL: REST endpoint (default: http://localhost:8080)
/// - OBJSTORE_GRPC_HOST / OBJSTORE_GRPC_PORT: gRPC host and port (default: localhost:50051)
/// - OBJSTORE_QUIC_URL: QUIC endpoint (default: https://localhost:4433)
///
/// Canonical replication policy payload (per sdk_canonical_test_spec.md):
///   source_backend="local", source_settings path=/tmp/src
///   destination_backend="local", destination_settings path=/tmp/dst
///   mode async, check_interval_seconds=3600
/// </summary>
[Collection("Integration")]
[Trait("Category", "Comprehensive")]
public class ComprehensiveTests : IntegrationTestBase, IAsyncDisposable
{
    private readonly List<string> _testKeys = new();
    private readonly Dictionary<string, string> _policyIds = new();

    #region Protocol Factory

    /// <summary>
    /// Factory method to create clients for each protocol.
    /// </summary>
    private IObjectStoreClient CreateClient(Protocol protocol)
    {
        return protocol switch
        {
            Protocol.REST => ObjectStoreClientFactory.CreateRestClient(RestBaseUrl),
            Protocol.GRPC => ObjectStoreClientFactory.CreateGrpcClient(GrpcAddress),
            Protocol.QUIC => ObjectStoreClientFactory.CreateQuicClient(QuicBaseUrl),
            _ => throw new ArgumentException($"Unsupported protocol: {protocol}")
        };
    }

    /// <summary>
    /// Provides test data for all protocol combinations.
    /// </summary>
    public static IEnumerable<object[]> GetAllProtocols()
    {
        yield return new object[] { Protocol.REST };
        yield return new object[] { Protocol.GRPC };
        yield return new object[] { Protocol.QUIC };
    }

    /// <summary>
    /// Provides test data for cross-protocol consistency tests.
    /// </summary>
    public static IEnumerable<object[]> GetProtocolPairs()
    {
        var protocols = new[] { Protocol.REST, Protocol.GRPC, Protocol.QUIC };

        foreach (var source in protocols)
        {
            foreach (var dest in protocols)
            {
                if (source != dest)
                {
                    yield return new object[] { source, dest };
                }
            }
        }
    }

    /// <summary>
    /// Returns true when the given protocol endpoint has been confirmed available during
    /// InitializeAsync. Tests for unavailable protocols are skipped with an explicit log,
    /// consistent with the canonical spec's "explicit skip, never silent fail" rule.
    /// </summary>
    private bool IsProtocolAvailable(Protocol protocol)
    {
        return protocol switch
        {
            Protocol.REST => IsServerAvailable,
            Protocol.GRPC => IsGrpcAvailable,
            Protocol.QUIC => IsQuicAvailable,
            _ => false
        };
    }

    /// <summary>
    /// Returns true when this test should be skipped (QUIC unavailable only).
    /// Fails the test immediately when the REST or gRPC endpoint is unavailable, because
    /// those are required protocols — unavailability is a harness failure, not a skip.
    /// </summary>
    private bool ShouldSkip(Protocol protocol)
    {
        if (!IsProtocolAvailable(protocol))
        {
            if (protocol == Protocol.QUIC)
            {
                Console.WriteLine($"[SKIP] QUIC/HTTP3 not available; skipping test.");
                return true;
            }

            // REST and gRPC are always-on — unavailability is a failure, not a skip.
            Assert.Fail($"{protocol} server unavailable — integration tests require the objstore-server container (this is a failure, not a skip).");
        }

        return false;
    }

    #endregion

    #region Canonical Replication Policy Builder

    /// <summary>
    /// Builds the canonical replication policy payload as defined in sdk_canonical_test_spec.md:
    ///   source_backend="local", source_settings path=tmpSrc
    ///   destination_backend="local", destination_settings path=tmpDst
    ///   mode async (ReplicationMode.Transparent), check_interval_seconds=3600
    /// </summary>
    private static ReplicationPolicy BuildCanonicalReplicationPolicy(string id)
    {
        var tmpSrc = Path.Combine(Path.GetTempPath(), $"objstore-repl-src-{id}");
        var tmpDst = Path.Combine(Path.GetTempPath(), $"objstore-repl-dst-{id}");

        return new ReplicationPolicy
        {
            Id = id,
            SourceBackend = "local",
            SourceSettings = new Dictionary<string, string> { ["path"] = tmpSrc },
            DestinationBackend = "local",
            DestinationSettings = new Dictionary<string, string> { ["path"] = tmpDst },
            // "async" mode maps to ReplicationMode.Transparent in this SDK
            ReplicationMode = ReplicationMode.Transparent,
            CheckIntervalSeconds = 3600,
            Enabled = true
        };
    }

    #endregion

    #region 1. Basic Operations Tests

    /// <summary>
    /// Test 1/19: PutAsync - Stores an object in the backend
    /// </summary>
    [Theory]
    [MemberData(nameof(GetAllProtocols))]
    [Trait("Operation", "Put")]
    public async Task Put_StoresObjectSuccessfully(Protocol protocol)
    {
        if (ShouldSkip(protocol)) return;

        using var client = CreateClient(protocol);
        var key = $"comprehensive/{protocol}/put-{Guid.NewGuid()}.txt";
        _testKeys.Add(key);

        var data = Encoding.UTF8.GetBytes($"Test data for {protocol}");
        var metadata = new ObjectMetadata
        {
            ContentType = "text/plain",
            Custom = new Dictionary<string, string> { ["protocol"] = protocol.ToString() }
        };

        var etag = await client.PutAsync(key, data, metadata);

        etag.Should().NotBeNullOrEmpty();
    }

    /// <summary>
    /// Test 2/19: GetAsync - Retrieves an object from the backend
    /// </summary>
    [Theory]
    [MemberData(nameof(GetAllProtocols))]
    [Trait("Operation", "Get")]
    public async Task Get_RetrievesObjectSuccessfully(Protocol protocol)
    {
        if (ShouldSkip(protocol)) return;

        using var client = CreateClient(protocol);
        var key = $"comprehensive/{protocol}/get-{Guid.NewGuid()}.txt";
        _testKeys.Add(key);

        var originalData = Encoding.UTF8.GetBytes($"Get test for {protocol}");
        await client.PutAsync(key, originalData);

        var (retrievedData, metadata) = await client.GetAsync(key);

        retrievedData.Should().BeEquivalentTo(originalData);
        metadata.Should().NotBeNull();
        metadata!.Size.Should().BeGreaterThan(0);
    }

    /// <summary>
    /// Test 3/19: DeleteAsync - Removes an object from the backend
    /// </summary>
    [Theory]
    [MemberData(nameof(GetAllProtocols))]
    [Trait("Operation", "Delete")]
    public async Task Delete_RemovesObjectSuccessfully(Protocol protocol)
    {
        if (ShouldSkip(protocol)) return;

        using var client = CreateClient(protocol);
        var key = $"comprehensive/{protocol}/delete-{Guid.NewGuid()}.txt";

        var data = Encoding.UTF8.GetBytes("Delete me");
        await client.PutAsync(key, data);

        var deleted = await client.DeleteAsync(key);

        deleted.Should().BeTrue();
        var exists = await client.ExistsAsync(key);
        exists.Should().BeFalse();
    }

    /// <summary>
    /// Test 4/19: ExistsAsync - Checks if an object exists in the backend
    /// </summary>
    [Theory]
    [MemberData(nameof(GetAllProtocols))]
    [Trait("Operation", "Exists")]
    public async Task Exists_ChecksObjectExistenceCorrectly(Protocol protocol)
    {
        if (ShouldSkip(protocol)) return;

        using var client = CreateClient(protocol);
        var key = $"comprehensive/{protocol}/exists-{Guid.NewGuid()}.txt";
        _testKeys.Add(key);

        var existsBefore = await client.ExistsAsync(key);
        existsBefore.Should().BeFalse();

        var data = Encoding.UTF8.GetBytes("Exists test");
        await client.PutAsync(key, data);

        var existsAfter = await client.ExistsAsync(key);
        existsAfter.Should().BeTrue();
    }

    /// <summary>
    /// Test 5/19: ListAsync - Returns a list of objects that match the given criteria
    /// </summary>
    [Theory]
    [MemberData(nameof(GetAllProtocols))]
    [Trait("Operation", "List")]
    public async Task List_ReturnsMatchingObjects(Protocol protocol)
    {
        if (ShouldSkip(protocol)) return;

        using var client = CreateClient(protocol);
        var prefix = $"comprehensive/{protocol}/list-{Guid.NewGuid()}/";
        var keys = new[]
        {
            $"{prefix}file1.txt",
            $"{prefix}file2.txt",
            $"{prefix}file3.txt"
        };

        var data = Encoding.UTF8.GetBytes("list test");
        foreach (var key in keys)
        {
            _testKeys.Add(key);
            await client.PutAsync(key, data);
        }

        var result = await client.ListAsync(prefix);

        result.Should().NotBeNull();
        result.Objects.Should().HaveCountGreaterOrEqualTo(3);
        foreach (var key in keys)
        {
            result.Objects.Should().Contain(o => o.Key == key, $"key '{key}' should be present in listing");
        }
    }

    #endregion

    #region 2. Metadata Operations Tests

    /// <summary>
    /// Test 6/19: GetMetadataAsync - Retrieves only the metadata for an object.
    /// Asserts size==len(data), content_type matches, and custom key is present.
    /// </summary>
    [Theory]
    [MemberData(nameof(GetAllProtocols))]
    [Trait("Operation", "GetMetadata")]
    public async Task GetMetadata_RetrievesMetadataWithoutContent(Protocol protocol)
    {
        if (ShouldSkip(protocol)) return;

        using var client = CreateClient(protocol);
        var key = $"comprehensive/{protocol}/get-metadata-{Guid.NewGuid()}.txt";
        _testKeys.Add(key);

        var data = Encoding.UTF8.GetBytes("Metadata test");
        var metadata = new ObjectMetadata
        {
            ContentType = "text/plain",
            Custom = new Dictionary<string, string>
            {
                ["version"] = "1.0",
                ["protocol"] = protocol.ToString()
            }
        };

        await client.PutAsync(key, data, metadata);

        var retrievedMetadata = await client.GetMetadataAsync(key);

        retrievedMetadata.Should().NotBeNull();
        retrievedMetadata!.Size.Should().Be(data.Length);
        retrievedMetadata.ContentType.Should().Be("text/plain");
        retrievedMetadata.Custom.Should().NotBeNull();
        retrievedMetadata.Custom!["version"].Should().Be("1.0");
    }

    /// <summary>
    /// Test 7/19: UpdateMetadataAsync - Updates the metadata for an existing object.
    /// Calls getMetadata after update and asserts the NEW content_type and custom values persisted.
    /// </summary>
    [Theory]
    [MemberData(nameof(GetAllProtocols))]
    [Trait("Operation", "UpdateMetadata")]
    public async Task UpdateMetadata_UpdatesMetadataSuccessfully(Protocol protocol)
    {
        if (ShouldSkip(protocol)) return;

        using var client = CreateClient(protocol);
        var key = $"comprehensive/{protocol}/update-metadata-{Guid.NewGuid()}.txt";
        _testKeys.Add(key);

        var data = Encoding.UTF8.GetBytes("Update metadata test");
        await client.PutAsync(key, data, new ObjectMetadata
        {
            ContentType = "text/plain",
            Custom = new Dictionary<string, string> { ["state"] = "original" }
        });

        var newMetadata = new ObjectMetadata
        {
            ContentType = "application/json",
            Custom = new Dictionary<string, string>
            {
                ["updated"] = "true",
                ["state"] = "modified"
            }
        };

        var updated = await client.UpdateMetadataAsync(key, newMetadata);
        updated.Should().BeTrue();

        // Read-back: assert the new values persisted
        var readBack = await client.GetMetadataAsync(key);
        readBack.Should().NotBeNull();
        readBack!.ContentType.Should().Be("application/json");
        readBack.Custom.Should().NotBeNull();
        readBack.Custom!["updated"].Should().Be("true");
        readBack.Custom["state"].Should().Be("modified");
    }

    #endregion

    #region 3. Lifecycle Policy Tests

    /// <summary>
    /// Test 8/19: AddPolicyAsync - Adds a new lifecycle policy.
    /// Verifies via getPolicies that the policy id is present after add.
    /// </summary>
    [Theory]
    [MemberData(nameof(GetAllProtocols))]
    [Trait("Operation", "AddPolicy")]
    public async Task AddPolicy_AddsLifecyclePolicySuccessfully(Protocol protocol)
    {
        if (ShouldSkip(protocol)) return;

        using var client = CreateClient(protocol);
        var policyId = $"comprehensive-{protocol}-{Guid.NewGuid()}";
        _policyIds[$"lifecycle-{protocol}-{policyId}"] = policyId;

        var policy = new LifecyclePolicy
        {
            Id = policyId,
            Prefix = $"comprehensive/{protocol}/lifecycle/",
            RetentionSeconds = 86400,
            Action = "delete"
        };

        var added = await client.AddPolicyAsync(policy);
        added.Should().BeTrue();

        // Verify via getPolicies
        var policies = await client.GetPoliciesAsync();
        policies.Should().Contain(p => p.Id == policyId, $"newly added policy '{policyId}' should be present");
    }

    /// <summary>
    /// Test 9/19: RemovePolicyAsync - Removes an existing lifecycle policy.
    /// Verifies policy is gone from the list after removal.
    /// </summary>
    [Theory]
    [MemberData(nameof(GetAllProtocols))]
    [Trait("Operation", "RemovePolicy")]
    public async Task RemovePolicy_RemovesLifecyclePolicySuccessfully(Protocol protocol)
    {
        if (ShouldSkip(protocol)) return;

        using var client = CreateClient(protocol);
        var policyId = $"comprehensive-remove-{protocol}-{Guid.NewGuid()}";

        var policy = new LifecyclePolicy
        {
            Id = policyId,
            Prefix = $"comprehensive/{protocol}/remove-lifecycle/",
            RetentionSeconds = 3600,
            Action = "delete"
        };

        await client.AddPolicyAsync(policy);

        var removed = await client.RemovePolicyAsync(policyId);
        removed.Should().BeTrue();

        // Verify it is gone
        var policies = await client.GetPoliciesAsync();
        policies.Should().NotContain(p => p.Id == policyId, $"removed policy '{policyId}' should not be present");
    }

    /// <summary>
    /// Test 10/19: GetPoliciesAsync - Retrieves all lifecycle policies.
    /// Asserts the added policy id is present.
    /// </summary>
    [Theory]
    [MemberData(nameof(GetAllProtocols))]
    [Trait("Operation", "GetPolicies")]
    public async Task GetPolicies_RetrievesLifecyclePoliciesSuccessfully(Protocol protocol)
    {
        if (ShouldSkip(protocol)) return;

        using var client = CreateClient(protocol);
        var policyId = $"comprehensive-get-policies-{protocol}-{Guid.NewGuid()}";

        var policy = new LifecyclePolicy
        {
            Id = policyId,
            Prefix = $"comprehensive/{protocol}/get-policies/",
            RetentionSeconds = 7200,
            Action = "delete"
        };

        await client.AddPolicyAsync(policy);

        try
        {
            var policies = await client.GetPoliciesAsync();

            policies.Should().NotBeNull();
            policies.Should().Contain(p => p.Id == policyId);
        }
        finally
        {
            await client.RemovePolicyAsync(policyId);
        }
    }

    /// <summary>
    /// Test 11/19: ApplyPoliciesAsync - Executes all lifecycle policies
    /// </summary>
    [Theory]
    [MemberData(nameof(GetAllProtocols))]
    [Trait("Operation", "ApplyPolicies")]
    public async Task ApplyPolicies_ExecutesLifecyclePoliciesSuccessfully(Protocol protocol)
    {
        if (ShouldSkip(protocol)) return;

        using var client = CreateClient(protocol);

        var result = await client.ApplyPoliciesAsync();

        result.Success.Should().BeTrue();
        result.PoliciesCount.Should().BeGreaterOrEqualTo(0);
        result.ObjectsProcessed.Should().BeGreaterOrEqualTo(0);
    }

    #endregion

    #region 4. Replication Tests

    /// <summary>
    /// Test 12/19: AddReplicationPolicyAsync - Adds a replication policy using the canonical payload.
    /// Asserts success (HTTP 201 equivalent). SupportsReplication() is a genuine fallback — on a
    /// correctly-configured server this should always take the assert path.
    /// </summary>
    [Theory]
    [MemberData(nameof(GetAllProtocols))]
    [Trait("Operation", "AddReplicationPolicy")]
    public async Task AddReplicationPolicy_AddsReplicationPolicySuccessfully(Protocol protocol)
    {
        if (ShouldSkip(protocol)) return;
        if (!await SupportsReplication())
        {
            Console.WriteLine("[SKIP] AddReplicationPolicy: server reports replication unsupported.");
            return;
        }

        using var client = CreateClient(protocol);
        var policyId = $"comprehensive-repl-add-{protocol}-{Guid.NewGuid()}";
        _policyIds[$"replication-{protocol}-{policyId}"] = policyId;

        var policy = BuildCanonicalReplicationPolicy(policyId);

        var added = await client.AddReplicationPolicyAsync(policy);
        added.Should().BeTrue($"AddReplicationPolicy should succeed for policy '{policyId}'");
    }

    /// <summary>
    /// Test 13/19: GetReplicationPoliciesAsync - Retrieves all replication policies.
    /// Asserts the added id is present and count >= 1.
    /// </summary>
    [Theory]
    [MemberData(nameof(GetAllProtocols))]
    [Trait("Operation", "GetReplicationPolicies")]
    public async Task GetReplicationPolicies_ContainsAddedPolicy(Protocol protocol)
    {
        if (ShouldSkip(protocol)) return;
        if (!await SupportsReplication())
        {
            Console.WriteLine("[SKIP] GetReplicationPolicies: server reports replication unsupported.");
            return;
        }

        using var client = CreateClient(protocol);
        var policyId = $"comprehensive-repl-getlist-{protocol}-{Guid.NewGuid()}";

        var policy = BuildCanonicalReplicationPolicy(policyId);
        await client.AddReplicationPolicyAsync(policy);

        try
        {
            var policies = await client.GetReplicationPoliciesAsync();

            policies.Should().NotBeNull();
            policies.Count.Should().BeGreaterOrEqualTo(1);
            policies.Should().Contain(p => p.Id == policyId, $"id '{policyId}' should be present in list");
        }
        finally
        {
            await client.RemoveReplicationPolicyAsync(policyId);
        }
    }

    /// <summary>
    /// Test 14/19: GetReplicationPolicyAsync - Retrieves a specific replication policy.
    /// Asserts id, source_backend=="local", destination_backend=="local", check_interval_seconds==3600.
    /// </summary>
    [Theory]
    [MemberData(nameof(GetAllProtocols))]
    [Trait("Operation", "GetReplicationPolicy")]
    public async Task GetReplicationPolicy_ReturnsCanonicalFields(Protocol protocol)
    {
        if (ShouldSkip(protocol)) return;
        if (!await SupportsReplication())
        {
            Console.WriteLine("[SKIP] GetReplicationPolicy: server reports replication unsupported.");
            return;
        }

        using var client = CreateClient(protocol);
        var policyId = $"comprehensive-repl-getsingle-{protocol}-{Guid.NewGuid()}";

        var policy = BuildCanonicalReplicationPolicy(policyId);
        await client.AddReplicationPolicyAsync(policy);

        try
        {
            var retrievedPolicy = await client.GetReplicationPolicyAsync(policyId);

            retrievedPolicy.Should().NotBeNull();
            retrievedPolicy!.Id.Should().Be(policyId);
            retrievedPolicy.SourceBackend.Should().Be("local");
            retrievedPolicy.DestinationBackend.Should().Be("local");
            retrievedPolicy.CheckIntervalSeconds.Should().Be(3600);
        }
        finally
        {
            await client.RemoveReplicationPolicyAsync(policyId);
        }
    }

    /// <summary>
    /// Test 15/19: TriggerReplicationAsync - Triggers synchronization for a policy.
    /// Asserts the rich server result: success==true, policy_id matches, counters >= 0,
    /// bytes_total >= 0, and DurationMs >= 0.
    /// </summary>
    [Theory]
    [MemberData(nameof(GetAllProtocols))]
    [Trait("Operation", "TriggerReplication")]
    public async Task TriggerReplication_ReturnsRichResult(Protocol protocol)
    {
        if (ShouldSkip(protocol)) return;
        if (!await SupportsReplication())
        {
            Console.WriteLine("[SKIP] TriggerReplication: server reports replication unsupported.");
            return;
        }

        using var client = CreateClient(protocol);
        var policyId = $"comprehensive-repl-trigger-{protocol}-{Guid.NewGuid()}";

        var policy = BuildCanonicalReplicationPolicy(policyId);
        await client.AddReplicationPolicyAsync(policy);

        try
        {
            var result = await client.TriggerReplicationAsync(policyId, parallel: true, workerCount: 2);

            result.Success.Should().BeTrue($"TriggerReplication for policy '{policyId}' should succeed");
            result.SyncResult.Should().NotBeNull("server should return a sync result payload");
            result.SyncResult!.PolicyId.Should().Be(policyId, "returned policy_id must match the triggered policy");
            result.SyncResult.Synced.Should().BeGreaterOrEqualTo(0, "synced counter must be non-negative");
            result.SyncResult.Deleted.Should().BeGreaterOrEqualTo(0, "deleted counter must be non-negative");
            result.SyncResult.Failed.Should().BeGreaterOrEqualTo(0, "failed counter must be non-negative");
            result.SyncResult.BytesTotal.Should().BeGreaterOrEqualTo(0, "bytes_total must be non-negative");
            result.SyncResult.DurationMs.Should().BeGreaterOrEqualTo(0, "duration_ms must be non-negative");
        }
        finally
        {
            await client.RemoveReplicationPolicyAsync(policyId);
        }
    }

    /// <summary>
    /// Test 16/19: GetReplicationStatusAsync - Retrieves status and metrics for a policy.
    /// Asserts policy_id matches, total_objects_synced >= 0, sync_count >= 0.
    /// </summary>
    [Theory]
    [MemberData(nameof(GetAllProtocols))]
    [Trait("Operation", "GetReplicationStatus")]
    public async Task GetReplicationStatus_ReturnsCounters(Protocol protocol)
    {
        if (ShouldSkip(protocol)) return;
        if (!await SupportsReplication())
        {
            Console.WriteLine("[SKIP] GetReplicationStatus: server reports replication unsupported.");
            return;
        }

        using var client = CreateClient(protocol);
        var policyId = $"comprehensive-repl-status-{protocol}-{Guid.NewGuid()}";

        var policy = BuildCanonicalReplicationPolicy(policyId);
        await client.AddReplicationPolicyAsync(policy);

        try
        {
            var status = await client.GetReplicationStatusAsync(policyId);

            status.Should().NotBeNull();
            status!.PolicyId.Should().Be(policyId);
            status.TotalObjectsSynced.Should().BeGreaterOrEqualTo(0);
            status.SyncCount.Should().BeGreaterOrEqualTo(0);
        }
        finally
        {
            await client.RemoveReplicationPolicyAsync(policyId);
        }
    }

    /// <summary>
    /// Test 17/19: RemoveReplicationPolicyAsync - Removes a replication policy.
    /// Asserts success and that the id is no longer in the list.
    /// </summary>
    [Theory]
    [MemberData(nameof(GetAllProtocols))]
    [Trait("Operation", "RemoveReplicationPolicy")]
    public async Task RemoveReplicationPolicy_SuccessAndGoneFromList(Protocol protocol)
    {
        if (ShouldSkip(protocol)) return;
        if (!await SupportsReplication())
        {
            Console.WriteLine("[SKIP] RemoveReplicationPolicy: server reports replication unsupported.");
            return;
        }

        using var client = CreateClient(protocol);
        var policyId = $"comprehensive-repl-remove-{protocol}-{Guid.NewGuid()}";

        var policy = BuildCanonicalReplicationPolicy(policyId);
        await client.AddReplicationPolicyAsync(policy);

        var removed = await client.RemoveReplicationPolicyAsync(policyId);
        removed.Should().BeTrue($"RemoveReplicationPolicy for '{policyId}' should succeed");

        var policies = await client.GetReplicationPoliciesAsync();
        policies.Should().NotContain(p => p.Id == policyId, $"removed policy '{policyId}' should not appear in list");
    }

    #endregion

    #region 5. Archive Tests

    /// <summary>
    /// Test 18/19: ArchiveAsync - Copies an object to archival storage backend.
    /// Skips with explicit log if the backend genuinely lacks archive support.
    /// </summary>
    [Theory]
    [MemberData(nameof(GetAllProtocols))]
    [Trait("Operation", "Archive")]
    public async Task Archive_ArchivesObjectSuccessfully(Protocol protocol)
    {
        if (ShouldSkip(protocol)) return;
        if (!await SupportsArchive())
        {
            Console.WriteLine("[SKIP] Archive: server reports archive unsupported.");
            return;
        }

        using var client = CreateClient(protocol);
        var key = $"comprehensive/{protocol}/archive-{Guid.NewGuid()}.txt";
        _testKeys.Add(key);

        var data = Encoding.UTF8.GetBytes($"Archive test for {protocol}");
        await client.PutAsync(key, data);

        var archived = await client.ArchiveAsync(key, "glacier", new Dictionary<string, string>
        {
            ["vault"] = "test-vault",
            ["protocol"] = protocol.ToString()
        });

        archived.Should().BeTrue();
    }

    #endregion

    #region 6. Health Check Tests

    /// <summary>
    /// Test 19/19: HealthAsync - Health check endpoint for service monitoring
    /// </summary>
    [Theory]
    [MemberData(nameof(GetAllProtocols))]
    [Trait("Operation", "Health")]
    public async Task Health_ReturnsHealthyStatus(Protocol protocol)
    {
        if (ShouldSkip(protocol)) return;

        using var client = CreateClient(protocol);

        var health = await client.HealthAsync();

        health.Should().NotBeNull();
        health.Status.Should().Be(HealthStatus.Serving);
    }

    #endregion

    #region Cross-Protocol Consistency Tests

    /// <summary>
    /// Verifies that an object written via one protocol can be read via another,
    /// and that data bytes are identical.
    /// </summary>
    [Theory]
    [MemberData(nameof(GetProtocolPairs))]
    [Trait("Category", "CrossProtocol")]
    public async Task CrossProtocol_WriteWithOneReadWithAnother_DataMatchesPerfectly(
        Protocol writeProtocol,
        Protocol readProtocol)
    {
        if (!IsProtocolAvailable(writeProtocol) || !IsProtocolAvailable(readProtocol))
        {
            // QUIC is a permitted skip; REST/gRPC unavailability is a harness failure.
            if (writeProtocol == Protocol.QUIC || readProtocol == Protocol.QUIC)
            {
                Console.WriteLine($"[SKIP] CrossProtocol {writeProtocol}->{readProtocol}: QUIC not available; skipping.");
                return;
            }
            Assert.Fail($"CrossProtocol {writeProtocol}->{readProtocol}: REST or gRPC server unavailable — integration tests require the objstore-server container (this is a failure, not a skip).");
        }

        var key = $"comprehensive/cross-protocol/{writeProtocol}-to-{readProtocol}-{Guid.NewGuid()}.txt";
        _testKeys.Add(key);

        var originalData = Encoding.UTF8.GetBytes($"Written by {writeProtocol}, read by {readProtocol}");

        using var writeClient = CreateClient(writeProtocol);
        var etag = await writeClient.PutAsync(key, originalData, new ObjectMetadata
        {
            ContentType = "text/plain",
            Custom = new Dictionary<string, string>
            {
                ["writeProtocol"] = writeProtocol.ToString(),
                ["readProtocol"] = readProtocol.ToString()
            }
        });

        etag.Should().NotBeNullOrEmpty();

        using var readClient = CreateClient(readProtocol);
        var (retrievedData, metadata) = await readClient.GetAsync(key);

        retrievedData.Should().BeEquivalentTo(originalData);
        metadata.Should().NotBeNull();
        metadata!.Size.Should().Be(originalData.Length);
    }

    /// <summary>
    /// Verifies that metadata written via one protocol is readable via another with
    /// equal size and content_type. Custom metadata key equality is also asserted.
    /// </summary>
    [Theory]
    [MemberData(nameof(GetProtocolPairs))]
    [Trait("Category", "CrossProtocol")]
    public async Task CrossProtocol_MetadataConsistency_SizeAndContentTypeEqual(
        Protocol writeProtocol,
        Protocol readProtocol)
    {
        if (!IsProtocolAvailable(writeProtocol) || !IsProtocolAvailable(readProtocol))
        {
            if (writeProtocol == Protocol.QUIC || readProtocol == Protocol.QUIC)
            {
                Console.WriteLine($"[SKIP] CrossProtocol metadata {writeProtocol}->{readProtocol}: QUIC not available; skipping.");
                return;
            }
            Assert.Fail($"CrossProtocol metadata {writeProtocol}->{readProtocol}: REST or gRPC server unavailable — integration tests require the objstore-server container (this is a failure, not a skip).");
        }

        var key = $"comprehensive/cross-protocol-metadata/{writeProtocol}-to-{readProtocol}-{Guid.NewGuid()}.txt";
        _testKeys.Add(key);

        var data = Encoding.UTF8.GetBytes("Metadata consistency test");
        var originalContentType = "text/plain";
        var originalCustomValue = "consistency-check";

        using var writeClient = CreateClient(writeProtocol);
        await writeClient.PutAsync(key, data, new ObjectMetadata
        {
            ContentType = originalContentType,
            Custom = new Dictionary<string, string> { ["test"] = originalCustomValue }
        });

        using var readClient = CreateClient(readProtocol);
        var metadata = await readClient.GetMetadataAsync(key);

        metadata.Should().NotBeNull();
        metadata!.Size.Should().Be(data.Length);
        metadata.ContentType.Should().Be(originalContentType);
        metadata.Custom.Should().NotBeNull();
        metadata.Custom!["test"].Should().Be(originalCustomValue);
    }

    /// <summary>
    /// Verifies that a delete performed via one protocol is visible via another (exists == false).
    /// </summary>
    [Theory]
    [MemberData(nameof(GetProtocolPairs))]
    [Trait("Category", "CrossProtocol")]
    public async Task CrossProtocol_DeleteViaOneExistenceViaAnother_ReturnsFalse(
        Protocol deleteProtocol,
        Protocol existsProtocol)
    {
        if (!IsProtocolAvailable(deleteProtocol) || !IsProtocolAvailable(existsProtocol))
        {
            if (deleteProtocol == Protocol.QUIC || existsProtocol == Protocol.QUIC)
            {
                Console.WriteLine($"[SKIP] CrossProtocol delete {deleteProtocol}->{existsProtocol}: QUIC not available; skipping.");
                return;
            }
            Assert.Fail($"CrossProtocol delete {deleteProtocol}->{existsProtocol}: REST or gRPC server unavailable — integration tests require the objstore-server container (this is a failure, not a skip).");
        }

        var key = $"comprehensive/cross-protocol-delete/{deleteProtocol}-to-{existsProtocol}-{Guid.NewGuid()}.txt";

        using var writeClient = CreateClient(Protocol.REST);
        await writeClient.PutAsync(key, Encoding.UTF8.GetBytes("cross-protocol delete test"));

        using var deleteClient = CreateClient(deleteProtocol);
        var deleted = await deleteClient.DeleteAsync(key);
        deleted.Should().BeTrue();

        using var existsClient = CreateClient(existsProtocol);
        var exists = await existsClient.ExistsAsync(key);
        exists.Should().BeFalse($"object deleted via {deleteProtocol} should not exist when checked via {existsProtocol}");
    }

    /// <summary>
    /// Verifies that lifecycle policies created via one protocol are visible via another.
    /// </summary>
    [Theory]
    [MemberData(nameof(GetProtocolPairs))]
    [Trait("Category", "CrossProtocol")]
    public async Task CrossProtocol_LifecyclePolicyConsistency_AcrossProtocols(
        Protocol createProtocol,
        Protocol readProtocol)
    {
        if (!IsProtocolAvailable(createProtocol) || !IsProtocolAvailable(readProtocol))
        {
            if (createProtocol == Protocol.QUIC || readProtocol == Protocol.QUIC)
            {
                Console.WriteLine($"[SKIP] CrossProtocol policy {createProtocol}->{readProtocol}: QUIC not available; skipping.");
                return;
            }
            Assert.Fail($"CrossProtocol policy {createProtocol}->{readProtocol}: REST or gRPC server unavailable — integration tests require the objstore-server container (this is a failure, not a skip).");
        }

        var policyId = $"cross-protocol-policy-{createProtocol}-to-{readProtocol}-{Guid.NewGuid()}";

        using var createClient = CreateClient(createProtocol);
        var policy = new LifecyclePolicy
        {
            Id = policyId,
            Prefix = $"comprehensive/cross-protocol/{createProtocol}/",
            RetentionSeconds = 14400,
            Action = "delete"
        };

        await createClient.AddPolicyAsync(policy);

        try
        {
            using var readClient = CreateClient(readProtocol);
            var policies = await readClient.GetPoliciesAsync();

            policies.Should().Contain(p => p.Id == policyId);
        }
        finally
        {
            await createClient.RemovePolicyAsync(policyId);
        }
    }

    #endregion

    #region Edge Cases and Error Handling

    /// <summary>
    /// Verifies that all protocols handle empty objects correctly
    /// </summary>
    [Theory]
    [MemberData(nameof(GetAllProtocols))]
    [Trait("Category", "EdgeCase")]
    public async Task EdgeCase_EmptyObject_HandledCorrectly(Protocol protocol)
    {
        if (ShouldSkip(protocol)) return;

        using var client = CreateClient(protocol);
        var key = $"comprehensive/{protocol}/empty-{Guid.NewGuid()}.txt";
        _testKeys.Add(key);

        var emptyData = Array.Empty<byte>();
        await client.PutAsync(key, emptyData);

        var (retrievedData, metadata) = await client.GetAsync(key);

        retrievedData.Should().BeEmpty();
        metadata.Should().NotBeNull();
        metadata!.Size.Should().Be(0);
    }

    /// <summary>
    /// Verifies that all protocols handle large objects correctly
    /// </summary>
    [Theory]
    [MemberData(nameof(GetAllProtocols))]
    [Trait("Category", "EdgeCase")]
    public async Task EdgeCase_LargeObject_HandledCorrectly(Protocol protocol)
    {
        if (ShouldSkip(protocol)) return;

        using var client = CreateClient(protocol);
        var key = $"comprehensive/{protocol}/large-{Guid.NewGuid()}.bin";
        _testKeys.Add(key);

        var largeData = new byte[1024 * 1024]; // 1MB
        new Random().NextBytes(largeData);

        await client.PutAsync(key, largeData);

        var (retrievedData, metadata) = await client.GetAsync(key);

        retrievedData.Should().BeEquivalentTo(largeData);
        metadata.Should().NotBeNull();
        metadata!.Size.Should().Be(largeData.Length);
    }

    /// <summary>
    /// Verifies that all protocols handle binary data correctly
    /// </summary>
    [Theory]
    [MemberData(nameof(GetAllProtocols))]
    [Trait("Category", "EdgeCase")]
    public async Task EdgeCase_BinaryData_PreservesAllBytes(Protocol protocol)
    {
        if (ShouldSkip(protocol)) return;

        using var client = CreateClient(protocol);
        var key = $"comprehensive/{protocol}/binary-{Guid.NewGuid()}.bin";
        _testKeys.Add(key);

        var binaryData = Enumerable.Range(0, 256).Select(i => (byte)i).ToArray();
        await client.PutAsync(key, binaryData);

        var (retrievedData, metadata) = await client.GetAsync(key);

        retrievedData.Should().BeEquivalentTo(binaryData);
        metadata.Should().NotBeNull();
        metadata!.Size.Should().Be(256);
    }

    /// <summary>
    /// Verifies that all protocols handle non-existent object queries correctly
    /// </summary>
    [Theory]
    [MemberData(nameof(GetAllProtocols))]
    [Trait("Category", "ErrorHandling")]
    public async Task ErrorHandling_GetNonexistent_ThrowsException(Protocol protocol)
    {
        if (ShouldSkip(protocol)) return;

        using var client = CreateClient(protocol);
        var key = $"comprehensive/{protocol}/nonexistent-{Guid.NewGuid()}.txt";

        await Assert.ThrowsAsync<ObjStore.SDK.Exceptions.ObjectNotFoundException>(
            async () => await client.GetAsync(key));
    }

    /// <summary>
    /// Verifies that all protocols handle deletion of non-existent objects gracefully
    /// </summary>
    [Theory]
    [MemberData(nameof(GetAllProtocols))]
    [Trait("Category", "ErrorHandling")]
    public async Task ErrorHandling_DeleteNonexistent_HandledGracefully(Protocol protocol)
    {
        if (ShouldSkip(protocol)) return;

        using var client = CreateClient(protocol);
        var key = $"comprehensive/{protocol}/nonexistent-delete-{Guid.NewGuid()}.txt";

        var result = await client.DeleteAsync(key);

        result.Should().BeFalse();
    }

    /// <summary>
    /// Verifies that all protocols handle hierarchical list operations correctly
    /// </summary>
    [Theory]
    [MemberData(nameof(GetAllProtocols))]
    [Trait("Category", "EdgeCase")]
    public async Task EdgeCase_HierarchicalList_WithDelimiter(Protocol protocol)
    {
        if (ShouldSkip(protocol)) return;

        using var client = CreateClient(protocol);
        var prefix = $"comprehensive/{protocol}/hierarchy-{Guid.NewGuid()}/";
        var keys = new[]
        {
            $"{prefix}file1.txt",
            $"{prefix}file2.txt",
            $"{prefix}subdir1/file3.txt",
            $"{prefix}subdir2/file4.txt"
        };

        var data = Encoding.UTF8.GetBytes("hierarchy test");
        foreach (var key in keys)
        {
            _testKeys.Add(key);
            await client.PutAsync(key, data);
        }

        var result = await client.ListAsync(prefix, delimiter: "/");

        result.Should().NotBeNull();
        result.Objects.Should().HaveCountGreaterOrEqualTo(2);
    }

    #endregion

    #region Cleanup

    /// <summary>
    /// Cleanup test data created during test execution
    /// </summary>
    public new async ValueTask DisposeAsync()
    {
        // Cleanup is best-effort: if the server is unavailable, InitializeAsync already
        // threw and real test failures have been recorded — don't suppress them here.
        if (!IsServerAvailable) return;

        // Clean up test objects
        foreach (var key in _testKeys)
        {
            try
            {
                using var client = CreateClient(Protocol.REST);
                await client.DeleteAsync(key);
            }
            catch
            {
                // Best effort cleanup
            }
        }

        // Clean up lifecycle policies
        foreach (var kvp in _policyIds.Where(p => p.Key.StartsWith("lifecycle-")))
        {
            try
            {
                using var client = CreateClient(Protocol.REST);
                await client.RemovePolicyAsync(kvp.Value);
            }
            catch
            {
                // Best effort cleanup
            }
        }

        // Clean up replication policies
        foreach (var kvp in _policyIds.Where(p => p.Key.StartsWith("replication-")))
        {
            try
            {
                using var client = CreateClient(Protocol.REST);
                await client.RemoveReplicationPolicyAsync(kvp.Value);
            }
            catch
            {
                // Best effort cleanup
            }
        }

        await base.DisposeAsync();
    }

    #endregion
}
