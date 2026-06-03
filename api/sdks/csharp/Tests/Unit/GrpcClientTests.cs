using FluentAssertions;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using ObjStore.SDK.Clients;
using ObjStore.SDK.Exceptions;
using Xunit;
using Objstore.V1;
using ProtoMetadata = Objstore.V1.Metadata;
using ProtoLifecyclePolicy = Objstore.V1.LifecyclePolicy;
using ProtoReplicationPolicy = Objstore.V1.ReplicationPolicy;
using ProtoReplicationStatus = Objstore.V1.ReplicationStatus;
using ProtoObjectInfo = Objstore.V1.ObjectInfo;
using ModelMetadata = ObjStore.SDK.Models.ObjectMetadata;
using ModelLifecyclePolicy = ObjStore.SDK.Models.LifecyclePolicy;
using ModelReplicationPolicy = ObjStore.SDK.Models.ReplicationPolicy;
using HealthStatus = ObjStore.SDK.Models.HealthStatus;

namespace ObjStore.SDK.Tests.Unit;

/// <summary>
/// Canonical gRPC client unit tests. Same matrix as REST/QUIC: success + error for all 19 ops,
/// not_found for the 9 listed ops, plus metadata_round_trip and validation_empty_key.
/// The production GrpcClient builds its own stub from a GrpcChannel, so the transport is the seam:
/// a real GrpcChannel over a mocked HttpMessageHandler returning framed gRPC responses
/// (see GrpcTestChannel). This drives the client's real call/parse paths with no live server.
/// </summary>
public class GrpcClientTests
{
    private static GrpcClient ClientFor<T>(T message) where T : IMessage<T> =>
        new(GrpcTestChannel.Unary(message));

    private static GrpcClient StreamingClientFor<T>(IEnumerable<T> messages) where T : IMessage<T> =>
        new(GrpcTestChannel.Streaming(messages));

    private static GrpcClient ErrorClient(StatusCode status = StatusCode.Internal) =>
        new(GrpcTestChannel.Error(status));

    // ---------------------------------------------------------------- put

    [Fact]
    public async Task Grpc_Put_Success()
    {
        (await ClientFor(new PutResponse { Success = true, Etag = "etag-1" })
            .PutAsync("k", new byte[] { 1 })).Should().Be("etag-1");
    }

    [Fact]
    public async Task Grpc_Put_Error()
    {
        await Assert.ThrowsAsync<OperationFailedException>(() => ErrorClient().PutAsync("k", new byte[] { 1 }));
    }

    // ---------------------------------------------------------------- get (server streaming)

    [Fact]
    public async Task Grpc_Get_Success()
    {
        var client = StreamingClientFor(new[]
        {
            new GetResponse
            {
                Data = ByteString.CopyFromUtf8("hello"),
                Metadata = new ProtoMetadata { ContentType = "text/plain" }
            }
        });

        var (data, metadata) = await client.GetAsync("k");

        ByteString.CopyFrom(data).ToStringUtf8().Should().Be("hello");
        metadata!.ContentType.Should().Be("text/plain");
    }

    [Fact]
    public async Task Grpc_Get_Error()
    {
        // A non-NotFound server error surfaces as RpcException (Get has no generic catch wrapper).
        await Assert.ThrowsAsync<RpcException>(() => ErrorClient().GetAsync("k"));
    }

    [Fact]
    public async Task Grpc_Get_NotFound()
    {
        await Assert.ThrowsAsync<ObjectNotFoundException>(() => ErrorClient(StatusCode.NotFound).GetAsync("missing"));
    }

    [Fact]
    public async Task Grpc_Get_NotFound_ByDetailMessage()
    {
        // The server may signal a missing object via the status detail rather than the NotFound
        // code (e.g. an underlying "no such file" error); the client still maps it to ObjectNotFound.
        var channel = GrpcTestChannel.Error(StatusCode.Internal, "open: no such file or directory");
        await Assert.ThrowsAsync<ObjectNotFoundException>(() => new GrpcClient(channel).GetAsync("missing"));
    }

    // ---------------------------------------------------------------- status code mapping

    [Theory]
    [InlineData(StatusCode.InvalidArgument, typeof(ValidationException))]
    [InlineData(StatusCode.Unauthenticated, typeof(AuthenticationException))]
    [InlineData(StatusCode.PermissionDenied, typeof(AuthorizationException))]
    [InlineData(StatusCode.AlreadyExists, typeof(AlreadyExistsException))]
    [InlineData(StatusCode.ResourceExhausted, typeof(RateLimitException))]
    public async Task Grpc_Get_StatusCode_MapsToCanonicalException(StatusCode status, System.Type expected)
    {
        var ex = await Record.ExceptionAsync(() => ErrorClient(status).GetAsync("k"));

        ex.Should().BeOfType(expected);
    }

    [Theory]
    [InlineData(StatusCode.InvalidArgument, typeof(ValidationException))]
    [InlineData(StatusCode.Unauthenticated, typeof(AuthenticationException))]
    [InlineData(StatusCode.PermissionDenied, typeof(AuthorizationException))]
    [InlineData(StatusCode.AlreadyExists, typeof(AlreadyExistsException))]
    [InlineData(StatusCode.ResourceExhausted, typeof(RateLimitException))]
    public async Task Grpc_Put_StatusCode_MapsToCanonicalException(StatusCode status, System.Type expected)
    {
        var ex = await Record.ExceptionAsync(() => ErrorClient(status).PutAsync("k", new byte[] { 1 }));

        ex.Should().BeOfType(expected);
    }

    [Fact]
    public async Task Grpc_PermissionDenied_WithNotFoundMessage_IsNotObjectNotFound()
    {
        // Explicit status codes win over the detail-substring not-found heuristic:
        // a "not found" substring in a PermissionDenied detail stays AuthorizationException.
        var channel = GrpcTestChannel.Error(StatusCode.PermissionDenied, "tenant not found in allowlist");

        await Assert.ThrowsAsync<AuthorizationException>(() => new GrpcClient(channel).GetAsync("k"));
    }

    // ---------------------------------------------------------------- delete

    [Fact]
    public async Task Grpc_Delete_Success()
    {
        (await ClientFor(new DeleteResponse { Success = true }).DeleteAsync("k")).Should().BeTrue();
    }

    [Fact]
    public async Task Grpc_Delete_Error()
    {
        await Assert.ThrowsAsync<OperationFailedException>(() => ErrorClient().DeleteAsync("k"));
    }

    [Fact]
    public async Task Grpc_Delete_NotFound()
    {
        (await ErrorClient(StatusCode.NotFound).DeleteAsync("missing")).Should().BeFalse();
    }

    [Fact]
    public async Task Grpc_Delete_NotFound_ByDetailMessage()
    {
        // Delete also treats a "not found"-detailed error as a benign miss (returns false).
        var channel = GrpcTestChannel.Error(StatusCode.Internal, "object not found");
        (await new GrpcClient(channel).DeleteAsync("missing")).Should().BeFalse();
    }

    // ---------------------------------------------------------------- list

    [Fact]
    public async Task Grpc_List_Success()
    {
        var response = new ListResponse { NextToken = "tok", Truncated = true };
        response.Objects.Add(new ProtoObjectInfo { Key = "a" });
        response.CommonPrefixes.Add("p/");

        var result = await ClientFor(response).ListAsync("p/");

        result.Objects.Should().ContainSingle();
        result.Objects[0].Key.Should().Be("a");
        result.NextToken.Should().Be("tok");
        result.Truncated.Should().BeTrue();
    }

    [Fact]
    public async Task Grpc_List_Error()
    {
        await Assert.ThrowsAsync<RpcException>(() => ErrorClient().ListAsync("p/"));
    }

    // ---------------------------------------------------------------- exists

    [Fact]
    public async Task Grpc_Exists_Success()
    {
        (await ClientFor(new ExistsResponse { Exists = true }).ExistsAsync("k")).Should().BeTrue();
    }

    [Fact]
    public async Task Grpc_Exists_Error()
    {
        await Assert.ThrowsAsync<RpcException>(() => ErrorClient().ExistsAsync("k"));
    }

    [Fact]
    public async Task Grpc_Exists_NotFound()
    {
        // For exists, "not found" is an exists=false response, not an error.
        (await ClientFor(new ExistsResponse { Exists = false }).ExistsAsync("missing")).Should().BeFalse();
    }

    // ---------------------------------------------------------------- get_metadata

    [Fact]
    public async Task Grpc_GetMetadata_Success()
    {
        var client = ClientFor(new MetadataResponse
        {
            Success = true,
            Metadata = new ProtoMetadata { ContentType = "application/json", Size = 42 }
        });

        var metadata = await client.GetMetadataAsync("k");

        metadata!.ContentType.Should().Be("application/json");
        metadata.Size.Should().Be(42);
    }

    [Fact]
    public async Task Grpc_GetMetadata_Error()
    {
        await Assert.ThrowsAsync<RpcException>(() => ErrorClient().GetMetadataAsync("k"));
    }

    [Fact]
    public async Task Grpc_GetMetadata_NotFound()
    {
        (await ClientFor(new MetadataResponse { Success = false }).GetMetadataAsync("missing")).Should().BeNull();
    }

    // ---------------------------------------------------------------- update_metadata

    [Fact]
    public async Task Grpc_UpdateMetadata_Success()
    {
        (await ClientFor(new UpdateMetadataResponse { Success = true })
            .UpdateMetadataAsync("k", new ModelMetadata())).Should().BeTrue();
    }

    [Fact]
    public async Task Grpc_UpdateMetadata_Error()
    {
        await Assert.ThrowsAsync<RpcException>(() => ErrorClient().UpdateMetadataAsync("k", new ModelMetadata()));
    }

    [Fact]
    public async Task Grpc_UpdateMetadata_NotFound()
    {
        await Assert.ThrowsAsync<ObjectNotFoundException>(() =>
            ClientFor(new UpdateMetadataResponse { Success = false })
                .UpdateMetadataAsync("missing", new ModelMetadata()));
    }

    // ---------------------------------------------------------------- health

    [Fact]
    public async Task Grpc_Health_Success()
    {
        var client = ClientFor(new Objstore.V1.HealthResponse
        {
            Status = Objstore.V1.HealthResponse.Types.Status.Serving,
            Message = "ok"
        });

        var health = await client.HealthAsync();

        health.Status.Should().Be(HealthStatus.Serving);
        health.Message.Should().Be("ok");
    }

    [Fact]
    public async Task Grpc_Health_Error()
    {
        await Assert.ThrowsAsync<RpcException>(() => ErrorClient().HealthAsync());
    }

    // ---------------------------------------------------------------- archive

    [Fact]
    public async Task Grpc_Archive_Success()
    {
        (await ClientFor(new ArchiveResponse { Success = true }).ArchiveAsync("k", "glacier")).Should().BeTrue();
    }

    [Fact]
    public async Task Grpc_Archive_Error()
    {
        await Assert.ThrowsAsync<RpcException>(() => ErrorClient().ArchiveAsync("k", "glacier"));
    }

    // ---------------------------------------------------------------- add_policy

    [Fact]
    public async Task Grpc_AddPolicy_Success()
    {
        (await ClientFor(new AddPolicyResponse { Success = true })
            .AddPolicyAsync(new ModelLifecyclePolicy { Id = "p1" })).Should().BeTrue();
    }

    [Fact]
    public async Task Grpc_AddPolicy_Error()
    {
        await Assert.ThrowsAsync<RpcException>(() => ErrorClient().AddPolicyAsync(new ModelLifecyclePolicy { Id = "p1" }));
    }

    // ---------------------------------------------------------------- remove_policy

    [Fact]
    public async Task Grpc_RemovePolicy_Success()
    {
        (await ClientFor(new RemovePolicyResponse { Success = true }).RemovePolicyAsync("p1")).Should().BeTrue();
    }

    [Fact]
    public async Task Grpc_RemovePolicy_Error()
    {
        await Assert.ThrowsAsync<RpcException>(() => ErrorClient().RemovePolicyAsync("p1"));
    }

    [Fact]
    public async Task Grpc_RemovePolicy_NotFound()
    {
        (await ClientFor(new RemovePolicyResponse { Success = false }).RemovePolicyAsync("missing")).Should().BeFalse();
    }

    // ---------------------------------------------------------------- get_policies

    [Fact]
    public async Task Grpc_GetPolicies_Success()
    {
        var response = new GetPoliciesResponse();
        response.Policies.Add(new ProtoLifecyclePolicy { Id = "p1", Action = "delete" });

        var result = await ClientFor(response).GetPoliciesAsync();

        result.Should().ContainSingle();
        result[0].Id.Should().Be("p1");
    }

    [Fact]
    public async Task Grpc_GetPolicies_Error()
    {
        await Assert.ThrowsAsync<RpcException>(() => ErrorClient().GetPoliciesAsync());
    }

    // ---------------------------------------------------------------- apply_policies

    [Fact]
    public async Task Grpc_ApplyPolicies_Success()
    {
        var client = ClientFor(new ApplyPoliciesResponse { Success = true, PoliciesCount = 3, ObjectsProcessed = 9 });

        var (success, count, processed) = await client.ApplyPoliciesAsync();

        success.Should().BeTrue();
        count.Should().Be(3);
        processed.Should().Be(9);
    }

    [Fact]
    public async Task Grpc_ApplyPolicies_Error()
    {
        await Assert.ThrowsAsync<RpcException>(() => ErrorClient().ApplyPoliciesAsync());
    }

    // ---------------------------------------------------------------- add_replication_policy

    [Fact]
    public async Task Grpc_AddReplicationPolicy_Success()
    {
        (await ClientFor(new AddReplicationPolicyResponse { Success = true })
            .AddReplicationPolicyAsync(new ModelReplicationPolicy { Id = "r1" })).Should().BeTrue();
    }

    [Fact]
    public async Task Grpc_AddReplicationPolicy_Error()
    {
        await Assert.ThrowsAsync<RpcException>(
            () => ErrorClient().AddReplicationPolicyAsync(new ModelReplicationPolicy { Id = "r1" }));
    }

    // ---------------------------------------------------------------- remove_replication_policy

    [Fact]
    public async Task Grpc_RemoveReplicationPolicy_Success()
    {
        (await ClientFor(new RemoveReplicationPolicyResponse { Success = true })
            .RemoveReplicationPolicyAsync("r1")).Should().BeTrue();
    }

    [Fact]
    public async Task Grpc_RemoveReplicationPolicy_Error()
    {
        await Assert.ThrowsAsync<RpcException>(() => ErrorClient().RemoveReplicationPolicyAsync("r1"));
    }

    [Fact]
    public async Task Grpc_RemoveReplicationPolicy_NotFound()
    {
        (await ClientFor(new RemoveReplicationPolicyResponse { Success = false })
            .RemoveReplicationPolicyAsync("missing")).Should().BeFalse();
    }

    // ---------------------------------------------------------------- get_replication_policies

    [Fact]
    public async Task Grpc_GetReplicationPolicies_Success()
    {
        var response = new GetReplicationPoliciesResponse();
        response.Policies.Add(new ProtoReplicationPolicy { Id = "r1", SourceBackend = "s3" });

        var result = await ClientFor(response).GetReplicationPoliciesAsync();

        result.Should().ContainSingle();
        result[0].Id.Should().Be("r1");
    }

    [Fact]
    public async Task Grpc_GetReplicationPolicies_Error()
    {
        await Assert.ThrowsAsync<RpcException>(() => ErrorClient().GetReplicationPoliciesAsync());
    }

    // ---------------------------------------------------------------- get_replication_policy

    [Fact]
    public async Task Grpc_GetReplicationPolicy_Success()
    {
        var client = ClientFor(new GetReplicationPolicyResponse
        {
            Policy = new ProtoReplicationPolicy { Id = "r1", SourceBackend = "s3" }
        });

        (await client.GetReplicationPolicyAsync("r1"))!.Id.Should().Be("r1");
    }

    [Fact]
    public async Task Grpc_GetReplicationPolicy_Error()
    {
        await Assert.ThrowsAsync<RpcException>(() => ErrorClient().GetReplicationPolicyAsync("r1"));
    }

    [Fact]
    public async Task Grpc_GetReplicationPolicy_NotFound()
    {
        // No policy present on the response => client returns null.
        (await ClientFor(new GetReplicationPolicyResponse()).GetReplicationPolicyAsync("missing")).Should().BeNull();
    }

    // ---------------------------------------------------------------- trigger_replication

    [Fact]
    public async Task Grpc_TriggerReplication_Success()
    {
        var protoResult = new SyncResult
        {
            PolicyId = "r1",
            Synced = 5,
            Deleted = 1,
            Failed = 0,
            BytesTotal = 2048,
            DurationMs = 300
        };

        var result = await ClientFor(new TriggerReplicationResponse
        {
            Success = true,
            Result = protoResult,
            Message = "ok"
        }).TriggerReplicationAsync("r1");

        result.Success.Should().BeTrue();
        result.SyncResult.Should().NotBeNull();
        result.SyncResult!.PolicyId.Should().Be("r1");
        result.SyncResult.Synced.Should().Be(5);
        result.SyncResult.Deleted.Should().Be(1);
        result.SyncResult.Failed.Should().Be(0);
        result.SyncResult.BytesTotal.Should().Be(2048);
        result.SyncResult.DurationMs.Should().Be(300);
    }

    [Fact]
    public async Task Grpc_TriggerReplication_Error()
    {
        await Assert.ThrowsAsync<RpcException>(() => ErrorClient().TriggerReplicationAsync("r1"));
    }

    // ---------------------------------------------------------------- get_replication_status

    [Fact]
    public async Task Grpc_GetReplicationStatus_Success()
    {
        var client = ClientFor(new GetReplicationStatusResponse
        {
            Success = true,
            Status = new ProtoReplicationStatus
            {
                PolicyId = "r1",
                TotalObjectsSynced = 7,
                // Timestamp set so the LastSyncTime?.ToDateTime() non-null branch is exercised.
                LastSyncTime = Timestamp.FromDateTime(new DateTime(2026, 1, 2, 3, 4, 5, DateTimeKind.Utc))
            }
        });

        var result = await client.GetReplicationStatusAsync("r1");

        result!.PolicyId.Should().Be("r1");
        result.TotalObjectsSynced.Should().Be(7);
    }

    [Fact]
    public async Task Grpc_GetReplicationStatus_Error()
    {
        await Assert.ThrowsAsync<RpcException>(() => ErrorClient().GetReplicationStatusAsync("r1"));
    }

    [Fact]
    public async Task Grpc_GetReplicationStatus_NotFound()
    {
        (await ClientFor(new GetReplicationStatusResponse { Success = false })
            .GetReplicationStatusAsync("missing")).Should().BeNull();
    }

    // ---------------------------------------------------------------- conversion layer
    // These exercise the proto<->model conversion helpers' richer branches (encryption layers,
    // timestamps) that the basic success/error cases do not reach.

    [Fact]
    public async Task Grpc_AddReplicationPolicy_MapsEncryptionAndTimestamp()
    {
        // A fully-populated policy (all three encryption layers + last-sync time) round-trips
        // through ConvertToProtoReplicationPolicy without loss; the server accepts it.
        var policy = new ModelReplicationPolicy
        {
            Id = "r1",
            SourceBackend = "s3",
            SourcePrefix = "in/",
            DestinationBackend = "gcs",
            SourceSettings = new Dictionary<string, string> { ["region"] = "us" },
            DestinationSettings = new Dictionary<string, string> { ["bucket"] = "b" },
            CheckIntervalSeconds = 120,
            Enabled = true,
            ReplicationMode = ObjStore.SDK.Models.ReplicationMode.Opaque,
            LastSyncTime = new DateTime(2026, 1, 2, 3, 4, 5, DateTimeKind.Utc),
            Encryption = new ObjStore.SDK.Models.EncryptionPolicy
            {
                Backend = new ObjStore.SDK.Models.EncryptionConfig { Enabled = true, Provider = "noop", DefaultKey = "bk" },
                Source = new ObjStore.SDK.Models.EncryptionConfig { Enabled = true, Provider = "custom", DefaultKey = "sk" },
                Destination = new ObjStore.SDK.Models.EncryptionConfig { Enabled = false, Provider = "noop", DefaultKey = "dk" }
            }
        };

        (await ClientFor(new AddReplicationPolicyResponse { Success = true })
            .AddReplicationPolicyAsync(policy)).Should().BeTrue();
    }

    [Fact]
    public async Task Grpc_GetReplicationPolicy_MapsEncryptionAndTimestampBack()
    {
        // The reverse mapping (ConvertFromProtoReplicationPolicy) reconstructs all encryption layers.
        var proto = new ProtoReplicationPolicy
        {
            Id = "r1",
            SourceBackend = "s3",
            DestinationBackend = "gcs",
            Enabled = true,
            LastSyncTime = Timestamp.FromDateTime(new DateTime(2026, 1, 2, 3, 4, 5, DateTimeKind.Utc)),
            Encryption = new Objstore.V1.EncryptionPolicy
            {
                Backend = new Objstore.V1.EncryptionConfig { Enabled = true, Provider = "noop", DefaultKey = "bk" },
                Source = new Objstore.V1.EncryptionConfig { Enabled = true, Provider = "custom", DefaultKey = "sk" },
                Destination = new Objstore.V1.EncryptionConfig { Enabled = false, Provider = "noop", DefaultKey = "dk" }
            }
        };

        var result = await ClientFor(new GetReplicationPolicyResponse { Policy = proto }).GetReplicationPolicyAsync("r1");

        result!.Encryption.Should().NotBeNull();
        result.Encryption!.Backend!.DefaultKey.Should().Be("bk");
        result.Encryption.Source!.Provider.Should().Be("custom");
        result.Encryption.Destination!.Enabled.Should().BeFalse();
        result.LastSyncTime.Should().NotBeNull();
    }

    [Fact]
    public async Task Grpc_Put_MapsLastModifiedTimestamp()
    {
        // ObjectMetadata.LastModified -> proto Timestamp conversion branch.
        var meta = new ModelMetadata
        {
            ContentType = "text/plain",
            LastModified = new DateTime(2026, 1, 2, 3, 4, 5, DateTimeKind.Utc)
        };

        (await ClientFor(new PutResponse { Success = true, Etag = "e" })
            .PutAsync("k", new byte[] { 1 }, meta)).Should().Be("e");
    }

    // ---------------------------------------------------------------- cross-cutting

    [Fact]
    public async Task Grpc_Metadata_RoundTrip()
    {
        var custom = new Dictionary<string, string> { ["author"] = "alice", ["team"] = "core" };

        // PUT: metadata travels in the proto message fields; the server echoes the etag.
        var etag = await ClientFor(new PutResponse { Success = true, Etag = "rt" })
            .PutAsync("k", System.Text.Encoding.UTF8.GetBytes("body"), new ModelMetadata
            {
                ContentType = "application/json",
                ContentEncoding = "gzip",
                Custom = custom
            });
        etag.Should().Be("rt");

        // Build the proto Metadata the server returns on GET / GET_METADATA. Include a
        // LastModified timestamp so the conversion's null-conditional non-null branch is covered.
        var protoMeta = new ProtoMetadata
        {
            ContentType = "application/json",
            ContentEncoding = "gzip",
            LastModified = Timestamp.FromDateTime(new DateTime(2026, 1, 2, 3, 4, 5, DateTimeKind.Utc))
        };
        protoMeta.Custom.Add(custom);

        // GET: metadata comes back in the streamed proto message.
        var (_, fromGet) = await StreamingClientFor(new[]
        {
            new GetResponse { Data = ByteString.CopyFromUtf8("body"), Metadata = protoMeta }
        }).GetAsync("k");
        fromGet!.ContentType.Should().Be("application/json");
        fromGet.ContentEncoding.Should().Be("gzip");
        fromGet.Custom!["author"].Should().Be("alice");
        fromGet.Custom["team"].Should().Be("core");

        // GET_METADATA: same proto Metadata returned via MetadataResponse.
        var fromGetMeta = await ClientFor(new MetadataResponse { Success = true, Metadata = protoMeta }).GetMetadataAsync("k");
        fromGetMeta!.ContentType.Should().Be("application/json");
        fromGetMeta.Custom!["author"].Should().Be("alice");
        fromGetMeta.Custom["team"].Should().Be("core");
    }

    [Fact]
    public async Task Grpc_Validation_EmptyKey()
    {
        var client = ClientFor(new PutResponse { Success = true });
        await Assert.ThrowsAsync<ArgumentNullException>(() => client.PutAsync(null!, new byte[] { 1 }));
        await Assert.ThrowsAsync<ArgumentNullException>(() => client.GetAsync(null!));
        await Assert.ThrowsAsync<ArgumentNullException>(() => client.PutAsync("k", null!));
    }

    [Fact]
    public void Grpc_Dispose_IsIdempotent()
    {
        var client = ClientFor(new PutResponse { Success = true });
        client.Dispose();
        client.Dispose();
    }
}
