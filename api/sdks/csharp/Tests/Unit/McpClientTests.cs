using System.Net;
using System.Text;
using System.Text.Json;
using FluentAssertions;
using ObjStore.SDK.Clients;
using ObjStore.SDK.Exceptions;
using ObjStore.SDK.Models;
using Moq;
using Xunit;

namespace ObjStore.SDK.Tests.Unit;

/// <summary>
/// MCP client unit tests. Each of the 19 operations gets a success case and an error case.
/// Selected operations get a not_found case. Auth header wiring is verified for token,
/// tenant-id and custom headers. Transport is a mocked HttpMessageHandler; no live server.
/// </summary>
public class McpClientTests : IDisposable
{
    private readonly Mock<HttpMessageHandler> _handler;
    private readonly HttpClient _httpClient;
    private readonly McpClient _client;

    public McpClientTests()
    {
        _handler = new Mock<HttpMessageHandler>();
        _httpClient = new HttpClient(_handler.Object) { BaseAddress = new Uri("http://localhost:8081") };
        _client = new McpClient(_httpClient);
    }

    // A fresh client whose handler throws a transport exception on every request.
    private McpClient ErrorClient()
    {
        var handler = new Mock<HttpMessageHandler>();
        handler.SetupThrow();
        var http = new HttpClient(handler.Object) { BaseAddress = new Uri("http://localhost:8081") };
        return new McpClient(http);
    }

    // Builds a valid MCP JSON-RPC 2.0 success envelope with the given tool-result JSON string.
    private static string McpSuccess(string resultJson) =>
        JsonSerializer.Serialize(new
        {
            jsonrpc = "2.0",
            id = 1,
            result = new
            {
                content = new[] { new { type = "text", text = resultJson } }
            }
        });

    // Builds a valid MCP JSON-RPC 2.0 error envelope.
    private static string McpError(int code, string message) =>
        JsonSerializer.Serialize(new
        {
            jsonrpc = "2.0",
            id = 1,
            error = new { code, message }
        });

    // Sets up the handler for every HTTP POST to "/" returning the provided MCP envelope.
    private void SetupMcpResponse(string mcpEnvelope, HttpStatusCode status = HttpStatusCode.OK)
    {
        _handler.SetupJson(_ => true, mcpEnvelope, status);
    }

    // ---------------------------------------------------------------- put

    [Fact]
    public async Task Mcp_Put_Success()
    {
        SetupMcpResponse(McpSuccess(JsonSerializer.Serialize(new { key = "k" })));

        var key = await _client.PutAsync("k", Encoding.UTF8.GetBytes("data"));

        key.Should().Be("k");
    }

    [Fact]
    public async Task Mcp_Put_Error()
    {
        await Assert.ThrowsAsync<ConnectionException>(() =>
            ErrorClient().PutAsync("k", new byte[] { 1 }));
    }

    // ---------------------------------------------------------------- get

    [Fact]
    public async Task Mcp_Get_Success()
    {
        var payload = Encoding.UTF8.GetBytes("hello world");
        SetupMcpResponse(McpSuccess(JsonSerializer.Serialize(new { data = Convert.ToBase64String(payload) })));

        var (data, _) = await _client.GetAsync("k");

        data.Should().BeEquivalentTo(payload);
    }

    [Fact]
    public async Task Mcp_Get_BinaryRoundTrip()
    {
        var payload = new byte[] { 0x00, 0x01, 0xFF, 0xFE, 0x80 };
        SetupMcpResponse(McpSuccess(JsonSerializer.Serialize(new { data = Convert.ToBase64String(payload) })));

        var (data, _) = await _client.GetAsync("k");

        data.Should().BeEquivalentTo(payload);
    }

    [Fact]
    public async Task Mcp_Get_InvalidBase64_Throws()
    {
        // Non-base64 data is a protocol violation, never a silent fallback.
        SetupMcpResponse(McpSuccess(JsonSerializer.Serialize(new { data = "not valid base64!!!" })));

        await Assert.ThrowsAsync<OperationFailedException>(() => _client.GetAsync("k"));
    }

    [Fact]
    public async Task Mcp_Get_NotFound()
    {
        // -32004 is the server's not-found code; mapping is by code, not message.
        SetupMcpResponse(McpError(-32004, "object not found"));

        await Assert.ThrowsAsync<ObjectNotFoundException>(() => _client.GetAsync("missing"));
    }

    [Fact]
    public async Task Mcp_Error_ForbiddenWithNotFoundMessage_IsNotObjectNotFound()
    {
        // -32001 is the authorization-denied code; a "not found" substring in
        // the message must not turn it into ObjectNotFoundException.
        SetupMcpResponse(McpError(-32001, "tenant not found in allowlist"));

        var ex = await Assert.ThrowsAsync<AuthorizationException>(() => _client.GetAsync("k"));
        ex.StatusCode.Should().Be(403);
    }

    [Fact]
    public async Task Mcp_Error_Unauthenticated_MapsToAuthenticationException()
    {
        SetupMcpResponse(McpError(-32002, "missing credentials"));

        var ex = await Assert.ThrowsAsync<AuthenticationException>(() => _client.GetAsync("k"));
        ex.StatusCode.Should().Be(401);
    }

    [Fact]
    public async Task Mcp_Error_AlreadyExists_MapsToAlreadyExistsException()
    {
        SetupMcpResponse(McpError(-32005, "object already exists"));

        var ex = await Assert.ThrowsAsync<AlreadyExistsException>(() => _client.GetAsync("k"));
        ex.StatusCode.Should().Be(409);
    }

    [Fact]
    public async Task Mcp_Error_RateLimited_MapsToRateLimitException()
    {
        SetupMcpResponse(McpError(-32029, "rate limited"));

        var ex = await Assert.ThrowsAsync<RateLimitException>(() => _client.GetAsync("k"));
        ex.StatusCode.Should().Be(429);
    }

    [Fact]
    public async Task Mcp_Error_InvalidParams_MapsToValidationException()
    {
        SetupMcpResponse(McpError(-32602, "invalid params"));

        var ex = await Assert.ThrowsAsync<ValidationException>(() => _client.GetAsync("k"));
        ex.StatusCode.Should().Be(400);
    }

    [Fact]
    public async Task Mcp_Error_UnknownCode_MapsToOperationFailedException()
    {
        SetupMcpResponse(McpError(-32000, "internal error"));

        await Assert.ThrowsAsync<OperationFailedException>(() => _client.GetAsync("k"));
    }

    [Fact]
    public async Task Mcp_Get_Error()
    {
        await Assert.ThrowsAsync<ConnectionException>(() => ErrorClient().GetAsync("k"));
    }

    // ---------------------------------------------------------------- get_stream

    [Fact]
    public async Task Mcp_GetStream_ReturnsStream()
    {
        var payload = Encoding.UTF8.GetBytes("stream data");
        SetupMcpResponse(McpSuccess(JsonSerializer.Serialize(new { data = Convert.ToBase64String(payload) })));

        var (stream, _) = await _client.GetStreamAsync("k");
        using var ms = new MemoryStream();
        await stream.CopyToAsync(ms);

        ms.ToArray().Should().BeEquivalentTo(payload);
    }

    // ---------------------------------------------------------------- put_stream

    [Fact]
    public async Task Mcp_PutStream_Success()
    {
        SetupMcpResponse(McpSuccess(JsonSerializer.Serialize(new { key = "k" })));

        var payload = Encoding.UTF8.GetBytes("streamed content");
        var key = await _client.PutStreamAsync("k", new MemoryStream(payload));

        key.Should().Be("k");
    }

    [Fact]
    public async Task Mcp_PutStream_ExceedsMaxBufferSize_Throws()
    {
        // The size check fires while buffering, before any HTTP IO.
        _client.MaxBufferSize = 16;

        var ex = await Assert.ThrowsAsync<OperationFailedException>(() =>
            _client.PutStreamAsync("k", new MemoryStream(new byte[32])));

        ex.Message.Should().Contain("MaxBufferSize");
    }

    // ---------------------------------------------------------------- delete

    [Fact]
    public async Task Mcp_Delete_Success()
    {
        SetupMcpResponse(McpSuccess(JsonSerializer.Serialize(new { deleted = true })));

        (await _client.DeleteAsync("k")).Should().BeTrue();
    }

    [Fact]
    public async Task Mcp_Delete_Error()
    {
        await Assert.ThrowsAsync<ConnectionException>(() => ErrorClient().DeleteAsync("k"));
    }

    // ---------------------------------------------------------------- list

    [Fact]
    public async Task Mcp_List_Success()
    {
        SetupMcpResponse(McpSuccess(JsonSerializer.Serialize(new
        {
            keys = new[] { "a", "b", "c" },
            truncated = false
        })));

        var result = await _client.ListAsync("prefix/");

        result.Objects.Should().HaveCount(3);
        result.Objects[0].Key.Should().Be("a");
    }

    [Fact]
    public async Task Mcp_List_Error()
    {
        await Assert.ThrowsAsync<ConnectionException>(() => ErrorClient().ListAsync("p"));
    }

    // ---------------------------------------------------------------- exists

    [Fact]
    public async Task Mcp_Exists_True()
    {
        SetupMcpResponse(McpSuccess(JsonSerializer.Serialize(new { exists = true })));

        (await _client.ExistsAsync("k")).Should().BeTrue();
    }

    [Fact]
    public async Task Mcp_Exists_False()
    {
        SetupMcpResponse(McpSuccess(JsonSerializer.Serialize(new { exists = false })));

        (await _client.ExistsAsync("k")).Should().BeFalse();
    }

    [Fact]
    public async Task Mcp_Exists_Error()
    {
        await Assert.ThrowsAsync<ConnectionException>(() => ErrorClient().ExistsAsync("k"));
    }

    // ---------------------------------------------------------------- get_metadata

    [Fact]
    public async Task Mcp_GetMetadata_Success()
    {
        SetupMcpResponse(McpSuccess(JsonSerializer.Serialize(new
        {
            content_type = "application/json",
            size = 42L,
            etag = "\"m\"",
            custom = new Dictionary<string, string> { ["author"] = "alice" }
        })));

        var metadata = await _client.GetMetadataAsync("k");

        metadata!.ContentType.Should().Be("application/json");
        metadata.Size.Should().Be(42L);
        metadata.Custom!["author"].Should().Be("alice");
    }

    [Fact]
    public async Task Mcp_GetMetadata_Error()
    {
        await Assert.ThrowsAsync<ConnectionException>(() => ErrorClient().GetMetadataAsync("k"));
    }

    // ---------------------------------------------------------------- update_metadata

    [Fact]
    public async Task Mcp_UpdateMetadata_Success()
    {
        SetupMcpResponse(McpSuccess(JsonSerializer.Serialize(new { updated = true })));

        (await _client.UpdateMetadataAsync("k", new ObjectMetadata { ContentType = "text/plain" })).Should().BeTrue();
    }

    [Fact]
    public async Task Mcp_UpdateMetadata_Error()
    {
        await Assert.ThrowsAsync<ConnectionException>(() =>
            ErrorClient().UpdateMetadataAsync("k", new ObjectMetadata()));
    }

    // ---------------------------------------------------------------- health

    [Fact]
    public async Task Mcp_Health_Serving()
    {
        SetupMcpResponse(McpSuccess(JsonSerializer.Serialize(new { status = "healthy", version = "1.2.3" })));

        var health = await _client.HealthAsync();

        health.Status.Should().Be(HealthStatus.Serving);
        health.Message.Should().Be("1.2.3");
    }

    [Fact]
    public async Task Mcp_Health_Error()
    {
        await Assert.ThrowsAsync<ConnectionException>(() => ErrorClient().HealthAsync());
    }

    // ---------------------------------------------------------------- archive

    [Fact]
    public async Task Mcp_Archive_Success()
    {
        SetupMcpResponse(McpSuccess(JsonSerializer.Serialize(new { archived = true })));

        (await _client.ArchiveAsync("k", "glacier")).Should().BeTrue();
    }

    [Fact]
    public async Task Mcp_Archive_Error()
    {
        await Assert.ThrowsAsync<ConnectionException>(() => ErrorClient().ArchiveAsync("k", "glacier"));
    }

    // ---------------------------------------------------------------- add_policy

    [Fact]
    public async Task Mcp_AddPolicy_Success()
    {
        SetupMcpResponse(McpSuccess(JsonSerializer.Serialize(new { added = true })));

        (await _client.AddPolicyAsync(new LifecyclePolicy { Id = "p1" })).Should().BeTrue();
    }

    [Fact]
    public async Task Mcp_AddPolicy_Error()
    {
        await Assert.ThrowsAsync<ConnectionException>(() =>
            ErrorClient().AddPolicyAsync(new LifecyclePolicy { Id = "p1" }));
    }

    // ---------------------------------------------------------------- remove_policy

    [Fact]
    public async Task Mcp_RemovePolicy_Success()
    {
        SetupMcpResponse(McpSuccess(JsonSerializer.Serialize(new { removed = true })));

        (await _client.RemovePolicyAsync("p1")).Should().BeTrue();
    }

    [Fact]
    public async Task Mcp_RemovePolicy_Error()
    {
        await Assert.ThrowsAsync<ConnectionException>(() => ErrorClient().RemovePolicyAsync("p1"));
    }

    // ---------------------------------------------------------------- get_policies

    [Fact]
    public async Task Mcp_GetPolicies_Success()
    {
        SetupMcpResponse(McpSuccess(JsonSerializer.Serialize(new
        {
            policies = new[] { new { id = "p1", prefix = "x/", action = "delete", retention_seconds = 86400 } }
        })));

        var result = await _client.GetPoliciesAsync();

        result.Should().ContainSingle();
        result[0].Id.Should().Be("p1");
    }

    [Fact]
    public async Task Mcp_GetPolicies_Error()
    {
        await Assert.ThrowsAsync<ConnectionException>(() => ErrorClient().GetPoliciesAsync());
    }

    // ---------------------------------------------------------------- apply_policies

    [Fact]
    public async Task Mcp_ApplyPolicies_Success()
    {
        SetupMcpResponse(McpSuccess(JsonSerializer.Serialize(new
        {
            success = true,
            policies_count = 2,
            objects_processed = 10
        })));

        var (success, count, processed) = await _client.ApplyPoliciesAsync();

        success.Should().BeTrue();
        count.Should().Be(2);
        processed.Should().Be(10);
    }

    [Fact]
    public async Task Mcp_ApplyPolicies_Error()
    {
        await Assert.ThrowsAsync<ConnectionException>(() => ErrorClient().ApplyPoliciesAsync());
    }

    // ---------------------------------------------------------------- add_replication_policy

    [Fact]
    public async Task Mcp_AddReplicationPolicy_Success()
    {
        SetupMcpResponse(McpSuccess(JsonSerializer.Serialize(new { success = true })));

        (await _client.AddReplicationPolicyAsync(new ReplicationPolicy { Id = "r1" })).Should().BeTrue();
    }

    [Fact]
    public async Task Mcp_AddReplicationPolicy_Error()
    {
        await Assert.ThrowsAsync<ConnectionException>(() =>
            ErrorClient().AddReplicationPolicyAsync(new ReplicationPolicy { Id = "r1" }));
    }

    // ---------------------------------------------------------------- remove_replication_policy

    [Fact]
    public async Task Mcp_RemoveReplicationPolicy_Success()
    {
        SetupMcpResponse(McpSuccess(JsonSerializer.Serialize(new { success = true })));

        (await _client.RemoveReplicationPolicyAsync("r1")).Should().BeTrue();
    }

    [Fact]
    public async Task Mcp_RemoveReplicationPolicy_Error()
    {
        await Assert.ThrowsAsync<ConnectionException>(() => ErrorClient().RemoveReplicationPolicyAsync("r1"));
    }

    // ---------------------------------------------------------------- get_replication_policies

    [Fact]
    public async Task Mcp_GetReplicationPolicies_Success()
    {
        SetupMcpResponse(McpSuccess(JsonSerializer.Serialize(new
        {
            policies = new[] { new { id = "r1", source_backend = "s3", destination_backend = "gcs", enabled = true } }
        })));

        var result = await _client.GetReplicationPoliciesAsync();

        result.Should().ContainSingle();
        result[0].Id.Should().Be("r1");
        result[0].SourceBackend.Should().Be("s3");
    }

    [Fact]
    public async Task Mcp_GetReplicationPolicies_Error()
    {
        await Assert.ThrowsAsync<ConnectionException>(() => ErrorClient().GetReplicationPoliciesAsync());
    }

    // ---------------------------------------------------------------- get_replication_policy

    [Fact]
    public async Task Mcp_GetReplicationPolicy_Success()
    {
        SetupMcpResponse(McpSuccess(JsonSerializer.Serialize(new
        {
            id = "r1",
            source_backend = "local",
            destination_backend = "s3",
            enabled = true
        })));

        var result = await _client.GetReplicationPolicyAsync("r1");

        result!.Id.Should().Be("r1");
        result.SourceBackend.Should().Be("local");
    }

    [Fact]
    public async Task Mcp_GetReplicationPolicy_NotFound()
    {
        SetupMcpResponse(McpError(-32004, "policy not found"));

        await Assert.ThrowsAsync<ObjectNotFoundException>(() => _client.GetReplicationPolicyAsync("missing"));
    }

    [Fact]
    public async Task Mcp_GetReplicationPolicy_Error()
    {
        await Assert.ThrowsAsync<ConnectionException>(() => ErrorClient().GetReplicationPolicyAsync("r1"));
    }

    // ---------------------------------------------------------------- trigger_replication

    [Fact]
    public async Task Mcp_TriggerReplication_Success()
    {
        SetupMcpResponse(McpSuccess(JsonSerializer.Serialize(new
        {
            success = true,
            message = "triggered",
            result = new
            {
                policy_id = "r1",
                synced = 5,
                deleted = 0,
                failed = 0,
                bytes_total = 2048L
            }
        })));

        var result = await _client.TriggerReplicationAsync("r1");

        result.Success.Should().BeTrue();
        result.SyncResult.Should().NotBeNull();
        result.SyncResult!.PolicyId.Should().Be("r1");
        result.SyncResult.Synced.Should().Be(5);
        result.SyncResult.BytesTotal.Should().Be(2048);
    }

    [Fact]
    public async Task Mcp_TriggerReplication_Error()
    {
        await Assert.ThrowsAsync<ConnectionException>(() => ErrorClient().TriggerReplicationAsync("r1"));
    }

    // ---------------------------------------------------------------- get_replication_status

    [Fact]
    public async Task Mcp_GetReplicationStatus_Success()
    {
        SetupMcpResponse(McpSuccess(JsonSerializer.Serialize(new
        {
            policy_id = "r1",
            source_backend = "local",
            destination_backend = "s3",
            enabled = true,
            total_objects_synced = 99L,
            sync_count = 12L
        })));

        var result = await _client.GetReplicationStatusAsync("r1");

        result!.PolicyId.Should().Be("r1");
        result.TotalObjectsSynced.Should().Be(99);
        result.SyncCount.Should().Be(12);
    }

    [Fact]
    public async Task Mcp_GetReplicationStatus_Error()
    {
        await Assert.ThrowsAsync<ConnectionException>(() => ErrorClient().GetReplicationStatusAsync("r1"));
    }

    // ---------------------------------------------------------------- auth header wiring

    [Fact]
    public async Task Mcp_Auth_NoTokenDoesNotSendAuthorizationHeader()
    {
        // When no token is configured the Authorization header must not be sent.
        HttpRequestMessage? captured = null;
        _handler.SetupCapture(
            _ => true,
            req => captured = req,
            () => new HttpResponseMessage
            {
                StatusCode = HttpStatusCode.OK,
                Content = new System.Net.Http.StringContent(
                    McpSuccess(JsonSerializer.Serialize(new { exists = true })),
                    Encoding.UTF8, "application/json")
            });

        await _client.ExistsAsync("k");

        captured.Should().NotBeNull();
        captured!.Headers.TryGetValues("Authorization", out _).Should().BeFalse();
    }

    [Fact]
    public async Task Mcp_Auth_RequestTargetsRootPath()
    {
        // All MCP calls must POST to "/".
        HttpRequestMessage? captured = null;
        _handler.SetupCapture(
            _ => true,
            req => captured = req,
            () => new HttpResponseMessage
            {
                StatusCode = HttpStatusCode.OK,
                Content = new System.Net.Http.StringContent(
                    McpSuccess(JsonSerializer.Serialize(new { exists = true })),
                    Encoding.UTF8, "application/json")
            });

        await _client.ExistsAsync("k");

        captured!.RequestUri!.ToString().Should().EndWith("/");
        captured.Method.Should().Be(HttpMethod.Post);
    }

    // ---------------------------------------------------------------- wire format validation

    [Fact]
    public async Task Mcp_JsonRpc_MethodIsToolsCall()
    {
        // Verify the outgoing JSON-RPC method is exactly "tools/call"
        string? requestBody = null;
        var captureHandler = new Mock<HttpMessageHandler>();
        captureHandler.SetupCapture(
            _ => true,
            req =>
            {
                requestBody = req.Content!.ReadAsStringAsync().GetAwaiter().GetResult();
            },
            () => new HttpResponseMessage
            {
                StatusCode = HttpStatusCode.OK,
                Content = new System.Net.Http.StringContent(
                    McpSuccess(JsonSerializer.Serialize(new { exists = true })),
                    Encoding.UTF8, "application/json")
            });

        var http = new HttpClient(captureHandler.Object) { BaseAddress = new Uri("http://localhost:8081") };
        await new McpClient(http).ExistsAsync("k");

        requestBody.Should().NotBeNullOrEmpty();
        var doc = JsonDocument.Parse(requestBody!).RootElement;
        doc.GetProperty("method").GetString().Should().Be("tools/call");
        doc.GetProperty("params").GetProperty("name").GetString().Should().Be("objstore_exists");
    }

    // ---------------------------------------------------------------- HTTP error mapping

    [Theory]
    [InlineData(HttpStatusCode.BadRequest, typeof(ValidationException))]
    [InlineData(HttpStatusCode.Unauthorized, typeof(AuthenticationException))]
    [InlineData(HttpStatusCode.Forbidden, typeof(AuthorizationException))]
    [InlineData(HttpStatusCode.NotFound, typeof(ObjectNotFoundException))]
    [InlineData(HttpStatusCode.Conflict, typeof(AlreadyExistsException))]
    [InlineData(HttpStatusCode.TooManyRequests, typeof(RateLimitException))]
    [InlineData(HttpStatusCode.InternalServerError, typeof(OperationFailedException))]
    public async Task Mcp_HttpStatus_MapsToCanonicalException(HttpStatusCode status, Type expected)
    {
        _handler.SetupStatus(_ => true, status);

        var ex = await Record.ExceptionAsync(() => _client.ExistsAsync("k"));

        ex.Should().BeOfType(expected);
    }

    [Fact]
    public async Task Mcp_HttpStatus_5xx_CarriesStatusCode()
    {
        _handler.SetupStatus(_ => true, HttpStatusCode.ServiceUnavailable);

        var ex = await Assert.ThrowsAsync<OperationFailedException>(() => _client.ExistsAsync("k"));
        ex.StatusCode.Should().Be(503);
    }

    // ---------------------------------------------------------------- validation

    [Fact]
    public async Task Mcp_Validation_NullKey()
    {
        await Assert.ThrowsAsync<ArgumentNullException>(() => _client.PutAsync(null!, new byte[] { 1 }));
        await Assert.ThrowsAsync<ArgumentNullException>(() => _client.GetAsync(null!));
    }

    // ---------------------------------------------------------------- IDisposable

    [Fact]
    public void Mcp_Dispose_Idempotent()
    {
        var h = new Mock<HttpMessageHandler>();
        var client = new McpClient(new HttpClient(h.Object) { BaseAddress = new Uri("http://localhost:8081") });
        client.Dispose();
        client.Dispose(); // must not throw
    }

    public void Dispose()
    {
        _client.Dispose();
        _httpClient.Dispose();
    }
}
