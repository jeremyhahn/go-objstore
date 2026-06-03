using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using FluentAssertions;
using ObjStore.SDK.Clients;
using ObjStore.SDK.Exceptions;
using ObjStore.SDK.Models;
using Xunit;

namespace ObjStore.SDK.Tests.Unit;

/// <summary>
/// Unix domain socket client tests. A minimal in-process mock server is started as a background
/// task for each test using a temporary socket path. The server reads one JSON-RPC 2.0 request
/// line and writes one JSON-RPC 2.0 response line, then closes the connection. UnixClient holds
/// a persistent connection and reconnects on the next call after the server closes it.
/// </summary>
public class UnixClientTests : IAsyncDisposable
{
    // Temporary socket directory reused across this instance; each test gets a unique file name.
    private readonly string _socketDir = Path.GetTempPath();

    // Creates a unique temp socket path.
    private string TempSocket() => Path.Combine(_socketDir, $"objstore_test_{Guid.NewGuid():N}.sock");

    // Starts a mock Unix domain socket server that accepts one connection, replies with the
    // given JSON-RPC 2.0 result JSON, and exits.  Returns the socket path.
    private static Task<string> StartMockServer(string resultJson)
    {
        return StartMockServerRaw(resultJson, isError: false, errorCode: 0, errorMessage: null);
    }

    private static Task<string> StartMockServerError(int code, string message)
    {
        return StartMockServerRaw(null, isError: true, errorCode: code, errorMessage: message);
    }

    private static Task<string> StartMockServerRaw(string? resultJson, bool isError, int errorCode, string? errorMessage)
    {
        var socketPath = Path.Combine(Path.GetTempPath(), $"objstore_test_{Guid.NewGuid():N}.sock");
        var ready = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);

        Task.Run(async () =>
        {
            if (File.Exists(socketPath))
                File.Delete(socketPath);

            using var server = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
            server.Bind(new UnixDomainSocketEndPoint(socketPath));
            server.Listen(1);

            ready.SetResult(socketPath);

            using var conn = await server.AcceptAsync();

            // Read one newline-delimited request.
            var buffer = new byte[65536];
            var received = new List<byte>();
            while (true)
            {
                int n = await conn.ReceiveAsync(buffer.AsMemory(0, buffer.Length));
                if (n == 0)
                    break;
                for (int i = 0; i < n; i++)
                {
                    if (buffer[i] == (byte)'\n')
                        goto done;
                    received.Add(buffer[i]);
                }
            }
            done:

            // Parse the request to extract the id.
            var requestDoc = JsonDocument.Parse(Encoding.UTF8.GetString(received.ToArray()));
            var id = requestDoc.RootElement.TryGetProperty("id", out var idProp) ? idProp.GetInt32() : 1;

            // Build the response.
            string response;
            if (isError)
            {
                response = JsonSerializer.Serialize(new
                {
                    jsonrpc = "2.0",
                    id,
                    error = new { code = errorCode, message = errorMessage }
                }) + "\n";
            }
            else
            {
                // The result field holds the already-serialized JSON object for the operation.
                var resultNode = JsonNode.Parse(resultJson!);
                response = JsonSerializer.Serialize(new
                {
                    jsonrpc = "2.0",
                    id,
                    result = resultNode
                }) + "\n";
            }

            await conn.SendAsync(Encoding.UTF8.GetBytes(response));
            conn.Shutdown(SocketShutdown.Both);
        });

        return ready.Task;
    }

    // Starts a mock server that captures the parsed JSON-RPC request, replies with the given
    // result JSON, and exits. Returns the socket path; the request is published via the TCS.
    private static Task<string> StartCaptureServer(string resultJson, TaskCompletionSource<JsonNode> requestCapture)
    {
        var socketPath = Path.Combine(Path.GetTempPath(), $"objstore_test_{Guid.NewGuid():N}.sock");
        var ready = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);

        Task.Run(async () =>
        {
            if (File.Exists(socketPath))
                File.Delete(socketPath);

            using var server = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
            server.Bind(new UnixDomainSocketEndPoint(socketPath));
            server.Listen(1);

            ready.SetResult(socketPath);

            using var conn = await server.AcceptAsync();

            var buffer = new byte[65536];
            var received = new List<byte>();
            while (true)
            {
                int n = await conn.ReceiveAsync(buffer.AsMemory(0, buffer.Length));
                if (n == 0)
                    break;
                for (int i = 0; i < n; i++)
                {
                    if (buffer[i] == (byte)'\n')
                        goto done;
                    received.Add(buffer[i]);
                }
            }
            done:

            var requestNode = JsonNode.Parse(Encoding.UTF8.GetString(received.ToArray()))!;
            requestCapture.TrySetResult(requestNode);

            var id = requestNode["id"]?.GetValue<int>() ?? 1;
            var response = JsonSerializer.Serialize(new
            {
                jsonrpc = "2.0",
                id,
                result = JsonNode.Parse(resultJson)
            }) + "\n";

            await conn.SendAsync(Encoding.UTF8.GetBytes(response));
            conn.Shutdown(SocketShutdown.Both);
        });

        return ready.Task;
    }

    // ---------------------------------------------------------------- put

    [Fact]
    public async Task Unix_Put_Success()
    {
        var socketPath = await StartMockServer(JsonSerializer.Serialize(new { status = "ok" }));
        using var client = new UnixClient(socketPath);

        // The Unix server returns {"status":"ok"} with no etag field; result should be null.
        var etag = await client.PutAsync("k", Encoding.UTF8.GetBytes("hello"));

        etag.Should().BeNull();
    }

    [Fact]
    public async Task Unix_Put_Error()
    {
        var socketPath = await StartMockServerError(-32000, "internal error");
        using var client = new UnixClient(socketPath);

        await Assert.ThrowsAsync<OperationFailedException>(() => client.PutAsync("k", new byte[] { 1 }));
    }

    // ---------------------------------------------------------------- get

    [Fact]
    public async Task Unix_Get_Success()
    {
        var payload = Encoding.UTF8.GetBytes("world");
        var socketPath = await StartMockServer(JsonSerializer.Serialize(new
        {
            data = Convert.ToBase64String(payload),
            metadata = new { content_type = "text/plain" }
        }));
        using var client = new UnixClient(socketPath);

        var (data, metadata) = await client.GetAsync("k");

        data.Should().BeEquivalentTo(payload);
        metadata!.ContentType.Should().Be("text/plain");
    }

    [Fact]
    public async Task Unix_Get_NotFound()
    {
        // -32004 is the server's not-found code; mapping is by code, not message.
        var socketPath = await StartMockServerError(-32004, "object not found");
        using var client = new UnixClient(socketPath);

        await Assert.ThrowsAsync<ObjectNotFoundException>(() => client.GetAsync("missing"));
    }

    [Fact]
    public async Task Unix_Error_ForbiddenWithNotFoundMessage_IsNotObjectNotFound()
    {
        // -32001 is the authorization-denied code; a "not found" substring in
        // the message must not turn it into ObjectNotFoundException.
        var socketPath = await StartMockServerError(-32001, "tenant not found in allowlist");
        using var client = new UnixClient(socketPath);

        var ex = await Assert.ThrowsAsync<AuthorizationException>(() => client.GetAsync("k"));
        ex.StatusCode.Should().Be(403);
    }

    [Fact]
    public async Task Unix_Error_Unauthenticated_MapsToAuthenticationException()
    {
        var socketPath = await StartMockServerError(-32002, "missing credentials");
        using var client = new UnixClient(socketPath);

        var ex = await Assert.ThrowsAsync<AuthenticationException>(() => client.GetAsync("k"));
        ex.StatusCode.Should().Be(401);
    }

    [Fact]
    public async Task Unix_Error_AlreadyExists_MapsToAlreadyExistsException()
    {
        var socketPath = await StartMockServerError(-32005, "object already exists");
        using var client = new UnixClient(socketPath);

        var ex = await Assert.ThrowsAsync<AlreadyExistsException>(() => client.GetAsync("k"));
        ex.StatusCode.Should().Be(409);
    }

    [Fact]
    public async Task Unix_Error_RateLimited_MapsToRateLimitException()
    {
        var socketPath = await StartMockServerError(-32029, "rate limited");
        using var client = new UnixClient(socketPath);

        var ex = await Assert.ThrowsAsync<RateLimitException>(() => client.GetAsync("k"));
        ex.StatusCode.Should().Be(429);
    }

    [Fact]
    public async Task Unix_Error_InvalidParams_MapsToValidationException()
    {
        var socketPath = await StartMockServerError(-32602, "invalid params");
        using var client = new UnixClient(socketPath);

        var ex = await Assert.ThrowsAsync<ValidationException>(() => client.GetAsync("k"));
        ex.StatusCode.Should().Be(400);
    }

    [Fact]
    public async Task Unix_Get_Error()
    {
        // Connecting to a non-existent socket path throws ConnectionException.
        using var client = new UnixClient("/tmp/does_not_exist_objstore_test.sock");

        await Assert.ThrowsAsync<ConnectionException>(() => client.GetAsync("k"));
    }

    // ---------------------------------------------------------------- get_stream

    [Fact]
    public async Task Unix_GetStream_ReturnsStream()
    {
        var payload = Encoding.UTF8.GetBytes("stream data");
        var socketPath = await StartMockServer(JsonSerializer.Serialize(new
        {
            data = Convert.ToBase64String(payload)
        }));
        using var client = new UnixClient(socketPath);

        var (stream, _) = await client.GetStreamAsync("k");
        using var ms = new MemoryStream();
        await stream.CopyToAsync(ms);

        ms.ToArray().Should().BeEquivalentTo(payload);
    }

    // ---------------------------------------------------------------- put_stream

    [Fact]
    public async Task Unix_PutStream_Success()
    {
        var socketPath = await StartMockServer(JsonSerializer.Serialize(new { status = "ok" }));
        using var client = new UnixClient(socketPath);

        // The Unix server returns {"status":"ok"} with no etag field; result should be null.
        var etag = await client.PutStreamAsync("k", new MemoryStream(Encoding.UTF8.GetBytes("streamed")));

        etag.Should().BeNull();
    }

    // ---------------------------------------------------------------- delete

    [Fact]
    public async Task Unix_Delete_Success()
    {
        var socketPath = await StartMockServer(JsonSerializer.Serialize(new { deleted = true }));
        using var client = new UnixClient(socketPath);

        (await client.DeleteAsync("k")).Should().BeTrue();
    }

    [Fact]
    public async Task Unix_Delete_Error()
    {
        var socketPath = await StartMockServerError(-32000, "internal");
        using var client = new UnixClient(socketPath);

        await Assert.ThrowsAsync<OperationFailedException>(() => client.DeleteAsync("k"));
    }

    // ---------------------------------------------------------------- list

    [Fact]
    public async Task Unix_List_Success()
    {
        var socketPath = await StartMockServer(JsonSerializer.Serialize(new
        {
            objects = new[]
            {
                new { key = "a", size = 10L, last_modified = "2024-01-01T00:00:00Z", etag = "\"e1\"" },
                new { key = "b", size = 20L, last_modified = "2024-01-02T00:00:00Z", etag = "\"e2\"" }
            },
            is_truncated = false
        }));
        using var client = new UnixClient(socketPath);

        var result = await client.ListAsync("p/");

        result.Objects.Should().HaveCount(2);
        result.Objects[0].Key.Should().Be("a");
    }

    [Fact]
    public async Task Unix_List_Error()
    {
        var socketPath = await StartMockServerError(-32000, "error");
        using var client = new UnixClient(socketPath);

        await Assert.ThrowsAsync<OperationFailedException>(() => client.ListAsync("p"));
    }

    // ---------------------------------------------------------------- exists

    [Fact]
    public async Task Unix_Exists_True()
    {
        var socketPath = await StartMockServer(JsonSerializer.Serialize(new { exists = true }));
        using var client = new UnixClient(socketPath);

        (await client.ExistsAsync("k")).Should().BeTrue();
    }

    [Fact]
    public async Task Unix_Exists_False()
    {
        var socketPath = await StartMockServer(JsonSerializer.Serialize(new { exists = false }));
        using var client = new UnixClient(socketPath);

        (await client.ExistsAsync("k")).Should().BeFalse();
    }

    [Fact]
    public async Task Unix_Exists_Error()
    {
        var socketPath = await StartMockServerError(-32000, "boom");
        using var client = new UnixClient(socketPath);

        await Assert.ThrowsAsync<OperationFailedException>(() => client.ExistsAsync("k"));
    }

    // ---------------------------------------------------------------- get_metadata

    [Fact]
    public async Task Unix_GetMetadata_Success()
    {
        // Server returns metadata fields directly on the result object (not nested under "metadata").
        var socketPath = await StartMockServer(JsonSerializer.Serialize(new
        {
            content_type = "application/json",
            custom = new Dictionary<string, string> { ["team"] = "core" }
        }));
        using var client = new UnixClient(socketPath);

        var meta = await client.GetMetadataAsync("k");

        meta!.ContentType.Should().Be("application/json");
        meta.Custom!["team"].Should().Be("core");
    }

    [Fact]
    public async Task Unix_GetMetadata_Error()
    {
        var socketPath = await StartMockServerError(-32000, "error");
        using var client = new UnixClient(socketPath);

        await Assert.ThrowsAsync<OperationFailedException>(() => client.GetMetadataAsync("k"));
    }

    // ---------------------------------------------------------------- update_metadata

    [Fact]
    public async Task Unix_UpdateMetadata_Success()
    {
        var socketPath = await StartMockServer(JsonSerializer.Serialize(new { status = "ok" }));
        using var client = new UnixClient(socketPath);

        (await client.UpdateMetadataAsync("k", new ObjectMetadata { ContentType = "text/plain" })).Should().BeTrue();
    }

    [Fact]
    public async Task Unix_UpdateMetadata_Error()
    {
        var socketPath = await StartMockServerError(-32000, "error");
        using var client = new UnixClient(socketPath);

        await Assert.ThrowsAsync<OperationFailedException>(() =>
            client.UpdateMetadataAsync("k", new ObjectMetadata()));
    }

    // ---------------------------------------------------------------- health

    [Fact]
    public async Task Unix_Health_Serving()
    {
        var socketPath = await StartMockServer(JsonSerializer.Serialize(new { status = "healthy", version = "1.0" }));
        using var client = new UnixClient(socketPath);

        var health = await client.HealthAsync();

        health.Status.Should().Be(HealthStatus.Serving);
    }

    [Fact]
    public async Task Unix_Health_Error()
    {
        using var client = new UnixClient("/tmp/no_such_objstore.sock");

        await Assert.ThrowsAsync<ConnectionException>(() => client.HealthAsync());
    }

    // ---------------------------------------------------------------- archive

    [Fact]
    public async Task Unix_Archive_Success()
    {
        var socketPath = await StartMockServer(JsonSerializer.Serialize(new { status = "ok" }));
        using var client = new UnixClient(socketPath);

        (await client.ArchiveAsync("k", "glacier")).Should().BeTrue();
    }

    [Fact]
    public async Task Unix_Archive_Error()
    {
        var socketPath = await StartMockServerError(-32000, "error");
        using var client = new UnixClient(socketPath);

        await Assert.ThrowsAsync<OperationFailedException>(() => client.ArchiveAsync("k", "glacier"));
    }

    // ---------------------------------------------------------------- add_policy

    [Fact]
    public async Task Unix_AddPolicy_Success()
    {
        var socketPath = await StartMockServer(JsonSerializer.Serialize(new { status = "ok" }));
        using var client = new UnixClient(socketPath);

        (await client.AddPolicyAsync(new LifecyclePolicy { Id = "p1" })).Should().BeTrue();
    }

    [Fact]
    public async Task Unix_AddPolicy_Error()
    {
        var socketPath = await StartMockServerError(-32000, "error");
        using var client = new UnixClient(socketPath);

        await Assert.ThrowsAsync<OperationFailedException>(() =>
            client.AddPolicyAsync(new LifecyclePolicy { Id = "p1" }));
    }

    [Fact]
    public async Task Unix_AddPolicy_SendsRetentionSeconds()
    {
        // Sub-day retention must be transmitted as retention_seconds, not
        // rejected or truncated to whole days.
        var capture = new TaskCompletionSource<JsonNode>(TaskCreationOptions.RunContinuationsAsynchronously);
        var socketPath = await StartCaptureServer(JsonSerializer.Serialize(new { status = "ok" }), capture);
        using var client = new UnixClient(socketPath);

        await client.AddPolicyAsync(new LifecyclePolicy
        {
            Id = "p1",
            Prefix = "x/",
            Action = "delete",
            RetentionSeconds = 3600
        });

        var p = (await capture.Task)["params"]!;
        p["retention_seconds"]!.GetValue<long>().Should().Be(3600);
    }

    // ---------------------------------------------------------------- remove_policy

    [Fact]
    public async Task Unix_RemovePolicy_Success()
    {
        var socketPath = await StartMockServer(JsonSerializer.Serialize(new { status = "ok" }));
        using var client = new UnixClient(socketPath);

        (await client.RemovePolicyAsync("p1")).Should().BeTrue();
    }

    [Fact]
    public async Task Unix_RemovePolicy_Error()
    {
        var socketPath = await StartMockServerError(-32000, "error");
        using var client = new UnixClient(socketPath);

        await Assert.ThrowsAsync<OperationFailedException>(() => client.RemovePolicyAsync("p1"));
    }

    // ---------------------------------------------------------------- get_policies

    [Fact]
    public async Task Unix_GetPolicies_Success()
    {
        // Server returns a top-level JSON array as the result value.
        var socketPath = await StartMockServer(JsonSerializer.Serialize(new[]
        {
            new { id = "p1", prefix = "x/", action = "delete", retention_seconds = 86400L }
        }));
        using var client = new UnixClient(socketPath);

        var result = await client.GetPoliciesAsync();

        result.Should().ContainSingle();
        result[0].Id.Should().Be("p1");
    }

    [Fact]
    public async Task Unix_GetPolicies_PrefersRetentionSeconds()
    {
        // When both fields are present, retention_seconds wins.
        var socketPath = await StartMockServer(JsonSerializer.Serialize(new[]
        {
            new { id = "p1", prefix = "x/", action = "delete", retention_seconds = 3600L, after_days = 2 }
        }));
        using var client = new UnixClient(socketPath);

        var result = await client.GetPoliciesAsync();

        result[0].RetentionSeconds.Should().Be(3600);
    }

    [Fact]
    public async Task Unix_GetPolicies_FallsBackToAfterDays()
    {
        // Older servers only return after_days; convert to seconds.
        var socketPath = await StartMockServer(JsonSerializer.Serialize(new[]
        {
            new { id = "p1", prefix = "x/", action = "delete", after_days = 2 }
        }));
        using var client = new UnixClient(socketPath);

        var result = await client.GetPoliciesAsync();

        result[0].RetentionSeconds.Should().Be(2L * 86400);
    }

    [Fact]
    public async Task Unix_GetPolicies_Error()
    {
        var socketPath = await StartMockServerError(-32000, "error");
        using var client = new UnixClient(socketPath);

        await Assert.ThrowsAsync<OperationFailedException>(() => client.GetPoliciesAsync());
    }

    // ---------------------------------------------------------------- apply_policies

    [Fact]
    public async Task Unix_ApplyPolicies_Success()
    {
        var socketPath = await StartMockServer(JsonSerializer.Serialize(new
        {
            success = true,
            policies_count = 3L,
            objects_processed = 15L
        }));
        using var client = new UnixClient(socketPath);

        var (success, count, processed) = await client.ApplyPoliciesAsync();

        success.Should().BeTrue();
        count.Should().Be(3);
        processed.Should().Be(15);
    }

    [Fact]
    public async Task Unix_ApplyPolicies_Error()
    {
        var socketPath = await StartMockServerError(-32000, "error");
        using var client = new UnixClient(socketPath);

        await Assert.ThrowsAsync<OperationFailedException>(() => client.ApplyPoliciesAsync());
    }

    // ---------------------------------------------------------------- add_replication_policy

    [Fact]
    public async Task Unix_AddReplicationPolicy_Success()
    {
        var socketPath = await StartMockServer(JsonSerializer.Serialize(new { status = "ok" }));
        using var client = new UnixClient(socketPath);

        (await client.AddReplicationPolicyAsync(new ReplicationPolicy { Id = "r1" })).Should().BeTrue();
    }

    [Fact]
    public async Task Unix_AddReplicationPolicy_Error()
    {
        var socketPath = await StartMockServerError(-32000, "error");
        using var client = new UnixClient(socketPath);

        await Assert.ThrowsAsync<OperationFailedException>(() =>
            client.AddReplicationPolicyAsync(new ReplicationPolicy { Id = "r1" }));
    }

    [Fact]
    public async Task Unix_AddReplicationPolicy_SendsDestinationAndSchedule()
    {
        // DestinationSettings map to "destination" and CheckIntervalSeconds to a
        // Go duration "schedule" string; without them the policy is non-functional.
        var capture = new TaskCompletionSource<JsonNode>(TaskCreationOptions.RunContinuationsAsynchronously);
        var socketPath = await StartCaptureServer(JsonSerializer.Serialize(new { status = "ok" }), capture);
        using var client = new UnixClient(socketPath);

        await client.AddReplicationPolicyAsync(new ReplicationPolicy
        {
            Id = "r1",
            SourcePrefix = "data/",
            DestinationBackend = "s3",
            DestinationSettings = new Dictionary<string, string> { ["bucket"] = "backup" },
            CheckIntervalSeconds = 300,
            Enabled = true
        });

        var p = (await capture.Task)["params"]!;
        p["id"]!.GetValue<string>().Should().Be("r1");
        p["source_prefix"]!.GetValue<string>().Should().Be("data/");
        p["destination_type"]!.GetValue<string>().Should().Be("s3");
        p["destination"]!["bucket"]!.GetValue<string>().Should().Be("backup");
        p["schedule"]!.GetValue<string>().Should().Be("300s");
        p["enabled"]!.GetValue<bool>().Should().BeTrue();
    }

    // ---------------------------------------------------------------- remove_replication_policy

    [Fact]
    public async Task Unix_RemoveReplicationPolicy_Success()
    {
        var socketPath = await StartMockServer(JsonSerializer.Serialize(new { status = "ok" }));
        using var client = new UnixClient(socketPath);

        (await client.RemoveReplicationPolicyAsync("r1")).Should().BeTrue();
    }

    [Fact]
    public async Task Unix_RemoveReplicationPolicy_Error()
    {
        var socketPath = await StartMockServerError(-32000, "error");
        using var client = new UnixClient(socketPath);

        await Assert.ThrowsAsync<OperationFailedException>(() => client.RemoveReplicationPolicyAsync("r1"));
    }

    // ---------------------------------------------------------------- get_replication_policies

    [Fact]
    public async Task Unix_GetReplicationPolicies_Success()
    {
        // Server returns a top-level JSON array; each element uses source_prefix and destination_type.
        var socketPath = await StartMockServer(JsonSerializer.Serialize(new[]
        {
            new { id = "r1", source_prefix = "data/", destination_type = "s3", enabled = true }
        }));
        using var client = new UnixClient(socketPath);

        var result = await client.GetReplicationPoliciesAsync();

        result.Should().ContainSingle();
        result[0].Id.Should().Be("r1");
    }

    [Fact]
    public async Task Unix_GetReplicationPolicies_Error()
    {
        var socketPath = await StartMockServerError(-32000, "error");
        using var client = new UnixClient(socketPath);

        await Assert.ThrowsAsync<OperationFailedException>(() => client.GetReplicationPoliciesAsync());
    }

    // ---------------------------------------------------------------- get_replication_policy

    [Fact]
    public async Task Unix_GetReplicationPolicy_Success()
    {
        var socketPath = await StartMockServer(JsonSerializer.Serialize(new
        {
            id = "r1",
            source_prefix = "data/",
            destination_type = "s3",
            enabled = true
        }));
        using var client = new UnixClient(socketPath);

        var result = await client.GetReplicationPolicyAsync("r1");

        result!.Id.Should().Be("r1");
    }

    [Fact]
    public async Task Unix_GetReplicationPolicy_NotFound()
    {
        var socketPath = await StartMockServerError(-32004, "policy not found");
        using var client = new UnixClient(socketPath);

        await Assert.ThrowsAsync<ObjectNotFoundException>(() => client.GetReplicationPolicyAsync("missing"));
    }

    [Fact]
    public async Task Unix_GetReplicationPolicy_Error()
    {
        var socketPath = await StartMockServerError(-32000, "error");
        using var client = new UnixClient(socketPath);

        await Assert.ThrowsAsync<OperationFailedException>(() => client.GetReplicationPolicyAsync("r1"));
    }

    // ---------------------------------------------------------------- trigger_replication

    [Fact]
    public async Task Unix_TriggerReplication_Success()
    {
        var socketPath = await StartMockServer(JsonSerializer.Serialize(new
        {
            objects_synced = 7,
            objects_failed = 0,
            bytes_transferred = 1024L,
            errors = Array.Empty<string>()
        }));
        using var client = new UnixClient(socketPath);

        var result = await client.TriggerReplicationAsync("r1");

        result.Success.Should().BeTrue();
    }

    [Fact]
    public async Task Unix_TriggerReplication_Error()
    {
        var socketPath = await StartMockServerError(-32000, "error");
        using var client = new UnixClient(socketPath);

        await Assert.ThrowsAsync<OperationFailedException>(() => client.TriggerReplicationAsync("r1"));
    }

    // ---------------------------------------------------------------- get_replication_status

    [Fact]
    public async Task Unix_GetReplicationStatus_Success()
    {
        var socketPath = await StartMockServer(JsonSerializer.Serialize(new
        {
            policy_id = "r1",
            status = "active",
            objects_synced = 42,
            objects_failed = 0,
            objects_pending = 3
        }));
        using var client = new UnixClient(socketPath);

        var result = await client.GetReplicationStatusAsync("r1");

        result!.PolicyId.Should().Be("r1");
        result.TotalObjectsSynced.Should().Be(42);
    }

    [Fact]
    public async Task Unix_GetReplicationStatus_NotFound()
    {
        var socketPath = await StartMockServerError(-32004, "policy not found");
        using var client = new UnixClient(socketPath);

        await Assert.ThrowsAsync<ObjectNotFoundException>(() => client.GetReplicationStatusAsync("missing"));
    }

    [Fact]
    public async Task Unix_GetReplicationStatus_Error()
    {
        var socketPath = await StartMockServerError(-32000, "error");
        using var client = new UnixClient(socketPath);

        await Assert.ThrowsAsync<OperationFailedException>(() => client.GetReplicationStatusAsync("r1"));
    }

    // ---------------------------------------------------------------- wire format: method names

    [Fact]
    public async Task Unix_WireFormat_PutMethodName()
    {
        // Intercept the raw JSON line from the client and check the method name.
        string? receivedMethod = null;
        var socketPath = Path.Combine(Path.GetTempPath(), $"objstore_method_{Guid.NewGuid():N}.sock");

        var serverTask = Task.Run(async () =>
        {
            using var server = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
            server.Bind(new UnixDomainSocketEndPoint(socketPath));
            server.Listen(1);

            using var conn = await server.AcceptAsync();

            var buf = new byte[4096];
            var acc = new List<byte>();
            while (true)
            {
                int n = await conn.ReceiveAsync(buf.AsMemory(0, buf.Length));
                if (n == 0) break;
                for (int i = 0; i < n; i++)
                {
                    if (buf[i] == (byte)'\n') goto done;
                    acc.Add(buf[i]);
                }
            }
            done:
            var doc = JsonDocument.Parse(Encoding.UTF8.GetString(acc.ToArray()));
            receivedMethod = doc.RootElement.GetProperty("method").GetString();
            var id = doc.RootElement.GetProperty("id").GetInt32();
            var resp = JsonSerializer.Serialize(new
            {
                jsonrpc = "2.0",
                id,
                result = new { key = "k" }
            }) + "\n";
            await conn.SendAsync(Encoding.UTF8.GetBytes(resp));
            conn.Shutdown(SocketShutdown.Both);
        });

        // Brief delay for server to bind and listen
        await Task.Delay(50);

        using var client = new UnixClient(socketPath);
        await client.PutAsync("k", Encoding.UTF8.GetBytes("data"));
        await serverTask;

        receivedMethod.Should().Be("put");
    }

    // ---------------------------------------------------------------- max buffer size

    [Fact]
    public async Task Unix_PutStream_ExceedsMaxBufferSize_Throws()
    {
        // The size check fires while buffering, before any socket IO, so no
        // mock server is needed.
        using var client = new UnixClient("/tmp/objstore_unused.sock") { MaxBufferSize = 16 };

        var ex = await Assert.ThrowsAsync<OperationFailedException>(() =>
            client.PutStreamAsync("k", new MemoryStream(new byte[32])));

        ex.Message.Should().Contain("MaxBufferSize");
    }

    [Fact]
    public async Task Unix_PutStream_WithinMaxBufferSize_Succeeds()
    {
        var socketPath = await StartMockServer(JsonSerializer.Serialize(new { status = "ok" }));
        using var client = new UnixClient(socketPath) { MaxBufferSize = 1024 };

        var etag = await client.PutStreamAsync("k", new MemoryStream(new byte[16]));

        etag.Should().BeNull();
    }

    // ---------------------------------------------------------------- persistent connection

    [Fact]
    public async Task Unix_PersistentConnection_ReusesSingleConnection()
    {
        var socketPath = Path.Combine(Path.GetTempPath(), $"objstore_persist_{Guid.NewGuid():N}.sock");
        int connectionsAccepted = 0;

        var serverTask = Task.Run(async () =>
        {
            using var server = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
            server.Bind(new UnixDomainSocketEndPoint(socketPath));
            server.Listen(1);

            using var conn = await server.AcceptAsync();
            Interlocked.Increment(ref connectionsAccepted);

            // Serve two requests on the same connection.
            for (int r = 0; r < 2; r++)
            {
                var acc = new List<byte>();
                var buf = new byte[4096];
                var lineComplete = false;
                while (!lineComplete)
                {
                    int n = await conn.ReceiveAsync(buf.AsMemory(0, buf.Length));
                    if (n == 0)
                        return;
                    for (int i = 0; i < n; i++)
                    {
                        if (buf[i] == (byte)'\n')
                        {
                            lineComplete = true;
                            break;
                        }
                        acc.Add(buf[i]);
                    }
                }

                var doc = JsonDocument.Parse(Encoding.UTF8.GetString(acc.ToArray()));
                var id = doc.RootElement.GetProperty("id").GetInt32();
                var resp = JsonSerializer.Serialize(new
                {
                    jsonrpc = "2.0",
                    id,
                    result = new { exists = true }
                }) + "\n";
                await conn.SendAsync(Encoding.UTF8.GetBytes(resp));
            }
        });

        // Brief delay for server to bind and listen
        await Task.Delay(50);

        using var client = new UnixClient(socketPath);
        (await client.ExistsAsync("a")).Should().BeTrue();
        (await client.ExistsAsync("b")).Should().BeTrue();
        await serverTask;

        connectionsAccepted.Should().Be(1);
    }

    [Fact]
    public async Task Unix_ResponseIdMismatch_Throws()
    {
        var socketPath = Path.Combine(Path.GetTempPath(), $"objstore_mismatch_{Guid.NewGuid():N}.sock");

        var serverTask = Task.Run(async () =>
        {
            using var server = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
            server.Bind(new UnixDomainSocketEndPoint(socketPath));
            server.Listen(1);

            using var conn = await server.AcceptAsync();

            var acc = new List<byte>();
            var buf = new byte[4096];
            var lineComplete = false;
            while (!lineComplete)
            {
                int n = await conn.ReceiveAsync(buf.AsMemory(0, buf.Length));
                if (n == 0)
                    return;
                for (int i = 0; i < n; i++)
                {
                    if (buf[i] == (byte)'\n')
                    {
                        lineComplete = true;
                        break;
                    }
                    acc.Add(buf[i]);
                }
            }

            var doc = JsonDocument.Parse(Encoding.UTF8.GetString(acc.ToArray()));
            var id = doc.RootElement.GetProperty("id").GetInt32();
            var resp = JsonSerializer.Serialize(new
            {
                jsonrpc = "2.0",
                id = id + 999, // deliberately wrong response id
                result = new { exists = true }
            }) + "\n";
            await conn.SendAsync(Encoding.UTF8.GetBytes(resp));
            conn.Shutdown(SocketShutdown.Both);
        });

        // Brief delay for server to bind and listen
        await Task.Delay(50);

        using var client = new UnixClient(socketPath);
        var ex = await Assert.ThrowsAsync<OperationFailedException>(() => client.ExistsAsync("k"));
        ex.Message.Should().Contain("does not match");
        await serverTask;
    }

    // ---------------------------------------------------------------- IDisposable

    [Fact]
    public void Unix_Dispose_Idempotent()
    {
        var client = new UnixClient("/tmp/objstore_test.sock");
        client.Dispose();
        client.Dispose(); // must not throw
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
