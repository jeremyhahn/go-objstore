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
/// Environment Variables:
/// - REST_BASE_URL: REST endpoint (default: http://localhost:8080)
/// - GRPC_ADDRESS: gRPC endpoint (default: http://localhost:9090)
/// - QUIC_BASE_URL: QUIC endpoint (default: https://localhost:8443)
/// </summary>
[Collection("Integration")]
[Trait("Category", "Comprehensive")]
public class ComprehensiveTests : IntegrationTestBase, IAsyncDisposable
{
    private readonly List<string> _testKeys = new();
    private readonly Dictionary<string, string> _policyIds = new();
    private bool? _supportsReplication;
    private bool? _supportsArchive;

    #region Protocol Factory

    /// <summary>
    /// Factory method to create clients for each protocol
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
    /// Provides test data for all protocol combinations
    /// </summary>
    public static IEnumerable<object[]> GetAllProtocols()
    {
        yield return new object[] { Protocol.REST };
        yield return new object[] { Protocol.GRPC };
        yield return new object[] { Protocol.QUIC };
    }

    /// <summary>
    /// Provides test data for cross-protocol consistency tests
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

    #endregion

    #region Feature Detection

    /// <summary>
    /// Checks if the backend supports replication by attempting to add a policy.
    /// Caches the result to avoid repeated checks.
    /// </summary>
    private async Task<bool> SupportsReplication()
    {
        if (_supportsReplication.HasValue)
            return _supportsReplication.Value;

        try
        {
            using var client = CreateClient(Protocol.REST);
            var testPolicy = new ReplicationPolicy
            {
                Id = $"feature-check-{Guid.NewGuid()}",
                SourceBackend = "local",
                DestinationBackend = "local",
                CheckIntervalSeconds = 300,
                Enabled = false,
                ReplicationMode = ReplicationMode.Transparent
            };

            await client.AddReplicationPolicyAsync(testPolicy);
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
            return _supportsArchive.Value;

        try
        {
            using var client = CreateClient(Protocol.REST);
            var testKey = $"feature-check/archive-{Guid.NewGuid()}.txt";
            var data = Encoding.UTF8.GetBytes("archive feature check");

            await client.PutAsync(testKey, data);

            try
            {
                await client.ArchiveAsync(testKey, "glacier", new Dictionary<string, string>
                {
                    ["vault"] = "test-vault"
                });

                _supportsArchive = true;
                return true;
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
                try { await client.DeleteAsync(testKey); } catch { }
            }
        }
        catch
        {
            _supportsArchive = false;
            return false;
        }
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
        if (!IsServerAvailable) return;

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
        if (!IsServerAvailable) return;

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
        if (!IsServerAvailable) return;

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
        if (!IsServerAvailable) return;

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
        if (!IsServerAvailable) return;

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
    }

    #endregion

    #region 2. Metadata Operations Tests

    /// <summary>
    /// Test 6/19: GetMetadataAsync - Retrieves only the metadata for an object
    /// </summary>
    [Theory]
    [MemberData(nameof(GetAllProtocols))]
    [Trait("Operation", "GetMetadata")]
    public async Task GetMetadata_RetrievesMetadataWithoutContent(Protocol protocol)
    {
        if (!IsServerAvailable) return;

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
        retrievedMetadata!.Size.Should().BeGreaterThan(0);
        retrievedMetadata.ContentType.Should().NotBeNullOrEmpty();
    }

    /// <summary>
    /// Test 7/19: UpdateMetadataAsync - Updates the metadata for an existing object
    /// </summary>
    [Theory]
    [MemberData(nameof(GetAllProtocols))]
    [Trait("Operation", "UpdateMetadata")]
    public async Task UpdateMetadata_UpdatesMetadataSuccessfully(Protocol protocol)
    {
        if (!IsServerAvailable) return;

        using var client = CreateClient(protocol);
        var key = $"comprehensive/{protocol}/update-metadata-{Guid.NewGuid()}.txt";
        _testKeys.Add(key);

        var data = Encoding.UTF8.GetBytes("Update metadata test");
        await client.PutAsync(key, data);

        var newMetadata = new ObjectMetadata
        {
            ContentType = "application/json",
            Custom = new Dictionary<string, string>
            {
                ["updated"] = "true",
                ["timestamp"] = DateTime.UtcNow.ToString("O")
            }
        };

        var updated = await client.UpdateMetadataAsync(key, newMetadata);

        updated.Should().BeTrue();
    }

    #endregion

    #region 3. Lifecycle Policy Tests

    /// <summary>
    /// Test 8/19: AddPolicyAsync - Adds a new lifecycle policy
    /// </summary>
    [Theory]
    [MemberData(nameof(GetAllProtocols))]
    [Trait("Operation", "AddPolicy")]
    public async Task AddPolicy_AddsLifecyclePolicySuccessfully(Protocol protocol)
    {
        if (!IsServerAvailable) return;

        using var client = CreateClient(protocol);
        var policyId = $"comprehensive-{protocol}-{Guid.NewGuid()}";
        _policyIds[$"lifecycle-{protocol}"] = policyId;

        var policy = new LifecyclePolicy
        {
            Id = policyId,
            Prefix = $"comprehensive/{protocol}/lifecycle/",
            RetentionSeconds = 86400,
            Action = "delete"
        };

        var added = await client.AddPolicyAsync(policy);

        added.Should().BeTrue();
    }

    /// <summary>
    /// Test 9/19: RemovePolicyAsync - Removes an existing lifecycle policy
    /// </summary>
    [Theory]
    [MemberData(nameof(GetAllProtocols))]
    [Trait("Operation", "RemovePolicy")]
    public async Task RemovePolicy_RemovesLifecyclePolicySuccessfully(Protocol protocol)
    {
        if (!IsServerAvailable) return;

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
    }

    /// <summary>
    /// Test 10/19: GetPoliciesAsync - Retrieves all lifecycle policies
    /// </summary>
    [Theory]
    [MemberData(nameof(GetAllProtocols))]
    [Trait("Operation", "GetPolicies")]
    public async Task GetPolicies_RetrievesLifecyclePoliciesSuccessfully(Protocol protocol)
    {
        if (!IsServerAvailable) return;

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
        if (!IsServerAvailable) return;

        using var client = CreateClient(protocol);

        var result = await client.ApplyPoliciesAsync();

        result.Success.Should().BeTrue();
        result.PoliciesCount.Should().BeGreaterOrEqualTo(0);
        result.ObjectsProcessed.Should().BeGreaterOrEqualTo(0);
    }

    #endregion

    #region 4. Replication Tests

    /// <summary>
    /// Test 12/19: AddReplicationPolicyAsync - Adds a new replication policy
    /// </summary>
    [Theory]
    [MemberData(nameof(GetAllProtocols))]
    [Trait("Operation", "AddReplicationPolicy")]
    public async Task AddReplicationPolicy_AddsReplicationPolicySuccessfully(Protocol protocol)
    {
        if (!IsServerAvailable) return;
        if (!await SupportsReplication()) return;

        using var client = CreateClient(protocol);
        var policyId = $"comprehensive-repl-{protocol}-{Guid.NewGuid()}";
        _policyIds[$"replication-{protocol}"] = policyId;

        var policy = new ReplicationPolicy
        {
            Id = policyId,
            SourceBackend = "local",
            DestinationBackend = "local",
            CheckIntervalSeconds = 300,
            Enabled = true,
            ReplicationMode = ReplicationMode.Transparent
        };

        var added = await client.AddReplicationPolicyAsync(policy);

        added.Should().BeTrue();
    }

    /// <summary>
    /// Test 13/19: RemoveReplicationPolicyAsync - Removes an existing replication policy
    /// </summary>
    [Theory]
    [MemberData(nameof(GetAllProtocols))]
    [Trait("Operation", "RemoveReplicationPolicy")]
    public async Task RemoveReplicationPolicy_RemovesReplicationPolicySuccessfully(Protocol protocol)
    {
        if (!IsServerAvailable) return;
        if (!await SupportsReplication()) return;

        using var client = CreateClient(protocol);
        var policyId = $"comprehensive-remove-repl-{protocol}-{Guid.NewGuid()}";

        var policy = new ReplicationPolicy
        {
            Id = policyId,
            SourceBackend = "local",
            DestinationBackend = "local",
            CheckIntervalSeconds = 300,
            Enabled = false,
            ReplicationMode = ReplicationMode.Transparent
        };

        await client.AddReplicationPolicyAsync(policy);

        var removed = await client.RemoveReplicationPolicyAsync(policyId);

        removed.Should().BeTrue();
    }

    /// <summary>
    /// Test 14/19: GetReplicationPoliciesAsync - Retrieves all replication policies
    /// </summary>
    [Theory]
    [MemberData(nameof(GetAllProtocols))]
    [Trait("Operation", "GetReplicationPolicies")]
    public async Task GetReplicationPolicies_RetrievesReplicationPoliciesSuccessfully(Protocol protocol)
    {
        if (!IsServerAvailable) return;
        if (!await SupportsReplication()) return;

        using var client = CreateClient(protocol);
        var policyId = $"comprehensive-get-repl-policies-{protocol}-{Guid.NewGuid()}";

        var policy = new ReplicationPolicy
        {
            Id = policyId,
            SourceBackend = "local",
            DestinationBackend = "local",
            CheckIntervalSeconds = 300,
            Enabled = true,
            ReplicationMode = ReplicationMode.Transparent
        };

        await client.AddReplicationPolicyAsync(policy);

        try
        {
            var policies = await client.GetReplicationPoliciesAsync();

            policies.Should().NotBeNull();
            policies.Should().Contain(p => p.Id == policyId);
        }
        finally
        {
            await client.RemoveReplicationPolicyAsync(policyId);
        }
    }

    /// <summary>
    /// Test 15/19: GetReplicationPolicyAsync - Retrieves a specific replication policy
    /// </summary>
    [Theory]
    [MemberData(nameof(GetAllProtocols))]
    [Trait("Operation", "GetReplicationPolicy")]
    public async Task GetReplicationPolicy_RetrievesSpecificPolicySuccessfully(Protocol protocol)
    {
        if (!IsServerAvailable) return;
        if (!await SupportsReplication()) return;

        using var client = CreateClient(protocol);
        var policyId = $"comprehensive-get-single-repl-{protocol}-{Guid.NewGuid()}";

        var policy = new ReplicationPolicy
        {
            Id = policyId,
            SourceBackend = "local",
            DestinationBackend = "local",
            CheckIntervalSeconds = 600,
            Enabled = true,
            ReplicationMode = ReplicationMode.Opaque
        };

        await client.AddReplicationPolicyAsync(policy);

        try
        {
            var retrievedPolicy = await client.GetReplicationPolicyAsync(policyId);

            retrievedPolicy.Should().NotBeNull();
            retrievedPolicy!.Id.Should().Be(policyId);
            retrievedPolicy.SourceBackend.Should().Be("local");
            retrievedPolicy.DestinationBackend.Should().Be("local");
        }
        finally
        {
            await client.RemoveReplicationPolicyAsync(policyId);
        }
    }

    /// <summary>
    /// Test 16/19: TriggerReplicationAsync - Triggers synchronization for policies
    /// </summary>
    [Theory]
    [MemberData(nameof(GetAllProtocols))]
    [Trait("Operation", "TriggerReplication")]
    public async Task TriggerReplication_TriggersReplicationSuccessfully(Protocol protocol)
    {
        if (!IsServerAvailable) return;
        if (!await SupportsReplication()) return;

        using var client = CreateClient(protocol);
        var policyId = $"comprehensive-trigger-repl-{protocol}-{Guid.NewGuid()}";

        var policy = new ReplicationPolicy
        {
            Id = policyId,
            SourceBackend = "local",
            DestinationBackend = "local",
            CheckIntervalSeconds = 300,
            Enabled = true,
            ReplicationMode = ReplicationMode.Transparent
        };

        await client.AddReplicationPolicyAsync(policy);

        try
        {
            var result = await client.TriggerReplicationAsync(policyId, parallel: true, workerCount: 2);

            result.Should().BeTrue();
        }
        finally
        {
            await client.RemoveReplicationPolicyAsync(policyId);
        }
    }

    /// <summary>
    /// Test 17/19: GetReplicationStatusAsync - Retrieves status and metrics for a policy
    /// </summary>
    [Theory]
    [MemberData(nameof(GetAllProtocols))]
    [Trait("Operation", "GetReplicationStatus")]
    public async Task GetReplicationStatus_RetrievesStatusSuccessfully(Protocol protocol)
    {
        if (!IsServerAvailable) return;
        if (!await SupportsReplication()) return;

        using var client = CreateClient(protocol);
        var policyId = $"comprehensive-status-repl-{protocol}-{Guid.NewGuid()}";

        var policy = new ReplicationPolicy
        {
            Id = policyId,
            SourceBackend = "local",
            DestinationBackend = "local",
            CheckIntervalSeconds = 300,
            Enabled = true,
            ReplicationMode = ReplicationMode.Transparent
        };

        await client.AddReplicationPolicyAsync(policy);

        try
        {
            var status = await client.GetReplicationStatusAsync(policyId);

            status.Should().NotBeNull();
            status!.PolicyId.Should().Be(policyId);
            status.TotalObjectsSynced.Should().BeGreaterOrEqualTo(0);
            status.TotalErrors.Should().BeGreaterOrEqualTo(0);
        }
        finally
        {
            await client.RemoveReplicationPolicyAsync(policyId);
        }
    }

    #endregion

    #region 5. Archive Tests

    /// <summary>
    /// Test 18/19: ArchiveAsync - Copies an object to archival storage backend
    /// </summary>
    [Theory]
    [MemberData(nameof(GetAllProtocols))]
    [Trait("Operation", "Archive")]
    public async Task Archive_ArchivesObjectSuccessfully(Protocol protocol)
    {
        if (!IsServerAvailable) return;
        if (!await SupportsArchive()) return;

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
        if (!IsServerAvailable) return;

        using var client = CreateClient(protocol);

        var health = await client.HealthAsync();

        health.Should().NotBeNull();
        health.Status.Should().Be(HealthStatus.Serving);
    }

    #endregion

    #region Cross-Protocol Consistency Tests

    /// <summary>
    /// Verifies that an object written via one protocol can be read via another
    /// </summary>
    [Theory]
    [MemberData(nameof(GetProtocolPairs))]
    [Trait("Category", "CrossProtocol")]
    public async Task CrossProtocol_WriteWithOneReadWithAnother_DataMatchesPerfectly(
        Protocol writeProtocol,
        Protocol readProtocol)
    {
        if (!IsServerAvailable) return;

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
    /// Verifies that metadata operations work consistently across protocols
    /// </summary>
    [Theory]
    [MemberData(nameof(GetProtocolPairs))]
    [Trait("Category", "CrossProtocol")]
    public async Task CrossProtocol_MetadataConsistency_AcrossProtocols(
        Protocol protocol1,
        Protocol protocol2)
    {
        if (!IsServerAvailable) return;

        var key = $"comprehensive/cross-protocol-metadata/{protocol1}-to-{protocol2}-{Guid.NewGuid()}.txt";
        _testKeys.Add(key);

        using var client1 = CreateClient(protocol1);
        var data = Encoding.UTF8.GetBytes("Metadata consistency test");
        await client1.PutAsync(key, data, new ObjectMetadata
        {
            ContentType = "text/plain",
            Custom = new Dictionary<string, string> { ["test"] = "value" }
        });

        using var client2 = CreateClient(protocol2);
        var metadata = await client2.GetMetadataAsync(key);

        metadata.Should().NotBeNull();
        metadata!.Size.Should().BeGreaterThan(0);
        metadata.ContentType.Should().NotBeNullOrEmpty();
    }

    /// <summary>
    /// Verifies that lifecycle policies created via one protocol are visible via another
    /// </summary>
    [Theory]
    [MemberData(nameof(GetProtocolPairs))]
    [Trait("Category", "CrossProtocol")]
    public async Task CrossProtocol_LifecyclePolicyConsistency_AcrossProtocols(
        Protocol createProtocol,
        Protocol readProtocol)
    {
        if (!IsServerAvailable) return;

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
        if (!IsServerAvailable) return;

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
        if (!IsServerAvailable) return;

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
        if (!IsServerAvailable) return;

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
        if (!IsServerAvailable) return;

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
        if (!IsServerAvailable) return;

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
        if (!IsServerAvailable) return;

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
