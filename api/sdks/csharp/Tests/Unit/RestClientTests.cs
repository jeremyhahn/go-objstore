using System.Net;
using System.Net.Http.Headers;
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
/// Canonical REST client unit tests. For each of the 19 operations: a success case and an
/// error case. The 9 not-found-aware operations additionally get a not_found case. Two
/// cross-cutting tests (metadata_round_trip, validation_empty_key) close out the matrix.
/// Transport is a mocked HttpMessageHandler — no live server.
/// </summary>
public class RestClientTests : IDisposable
{
    private readonly Mock<HttpMessageHandler> _handler;
    private readonly HttpClient _httpClient;
    private readonly RestClient _client;

    public RestClientTests()
    {
        _handler = new Mock<HttpMessageHandler>();
        _httpClient = new HttpClient(_handler.Object) { BaseAddress = new Uri("http://localhost:8080") };
        _client = new RestClient(_httpClient);
    }

    // A fresh client whose handler throws a transport exception on every request, used for the
    // canonical _error cases (every op wraps HttpRequestException into its SDK error type).
    private RestClient ErrorClient()
    {
        var handler = new Mock<HttpMessageHandler>();
        handler.SetupThrow();
        var http = new HttpClient(handler.Object) { BaseAddress = new Uri("http://localhost:8080") };
        return new RestClient(http);
    }

    // ---------------------------------------------------------------- put

    [Fact]
    public async Task Rest_Put_Success()
    {
        _handler.SetupResponse(
            req => req.Method == HttpMethod.Put,
            () =>
            {
                var r = new HttpResponseMessage { StatusCode = HttpStatusCode.Created };
                r.Headers.ETag = new EntityTagHeaderValue("\"etag-1\"");
                return r;
            });

        var etag = await _client.PutAsync("k", Encoding.UTF8.GetBytes("data"));

        etag.Should().Be("\"etag-1\"");
    }

    [Fact]
    public async Task Rest_Put_Error()
    {
        await Assert.ThrowsAsync<OperationFailedException>(() => ErrorClient().PutAsync("k", new byte[] { 1 }));
    }

    [Fact]
    public async Task Rest_Put_ReadsEtagFromRawHeader()
    {
        // When the server returns a weak/non-strong ETag the strongly-typed Headers.ETag is null,
        // so the client falls back to the raw "ETag" header value.
        _handler.SetupResponse(
            req => req.Method == HttpMethod.Put,
            () =>
            {
                var r = new HttpResponseMessage { StatusCode = HttpStatusCode.Created };
                r.Headers.TryAddWithoutValidation("ETag", "raw-etag");
                return r;
            });

        (await _client.PutAsync("k", new byte[] { 1 })).Should().Be("raw-etag");
    }

    // ---------------------------------------------------------------- get

    [Fact]
    public async Task Rest_Get_Success()
    {
        var data = Encoding.UTF8.GetBytes("hello");
        _handler.SetupResponse(
            req => req.Method == HttpMethod.Get,
            () =>
            {
                var r = new HttpResponseMessage { StatusCode = HttpStatusCode.OK, Content = new ByteArrayContent(data) };
                r.Content.Headers.ContentType = new MediaTypeHeaderValue("text/plain");
                return r;
            });

        var (body, metadata) = await _client.GetAsync("k");

        body.Should().BeEquivalentTo(data);
        metadata!.ContentType.Should().Be("text/plain");
    }

    [Fact]
    public async Task Rest_Get_Error()
    {
        await Assert.ThrowsAsync<OperationFailedException>(() => ErrorClient().GetAsync("k"));
    }

    [Fact]
    public async Task Rest_Get_ParsesLastModified()
    {
        // LastModified header populates the metadata's LastModified field.
        _handler.SetupResponse(
            req => req.Method == HttpMethod.Get,
            () =>
            {
                var r = new HttpResponseMessage { StatusCode = HttpStatusCode.OK, Content = new ByteArrayContent(new byte[] { 1 }) };
                r.Content.Headers.ContentType = new MediaTypeHeaderValue("text/plain");
                r.Content.Headers.LastModified = new DateTimeOffset(2026, 1, 2, 3, 4, 5, TimeSpan.Zero);
                return r;
            });

        var (_, metadata) = await _client.GetAsync("k");

        metadata!.LastModified.Should().NotBeNull();
    }

    [Fact]
    public async Task Rest_Get_NotFound()
    {
        _handler.SetupStatus(req => req.Method == HttpMethod.Get, HttpStatusCode.NotFound);

        await Assert.ThrowsAsync<ObjectNotFoundException>(() => _client.GetAsync("missing"));
    }

    // ---------------------------------------------------------------- delete

    [Fact]
    public async Task Rest_Delete_Success()
    {
        _handler.SetupStatus(req => req.Method == HttpMethod.Delete, HttpStatusCode.OK);

        (await _client.DeleteAsync("k")).Should().BeTrue();
    }

    [Fact]
    public async Task Rest_Delete_Error()
    {
        await Assert.ThrowsAsync<OperationFailedException>(() => ErrorClient().DeleteAsync("k"));
    }

    [Fact]
    public async Task Rest_Delete_NotFound()
    {
        _handler.SetupStatus(req => req.Method == HttpMethod.Delete, HttpStatusCode.NotFound);

        (await _client.DeleteAsync("missing")).Should().BeFalse();
    }

    // ---------------------------------------------------------------- list

    [Fact]
    public async Task Rest_List_Success()
    {
        var json = JsonSerializer.Serialize(new ListObjectsResponse
        {
            Objects = new List<ObjectInfo> { new() { Key = "a" }, new() { Key = "b" } }
        });
        _handler.SetupJson(req => req.Method == HttpMethod.Get, json);

        var result = await _client.ListAsync("prefix/");

        result.Objects.Should().HaveCount(2);
        result.Objects[0].Key.Should().Be("a");
    }

    [Fact]
    public async Task Rest_List_Error()
    {
        await Assert.ThrowsAsync<OperationFailedException>(() => ErrorClient().ListAsync("p"));
    }

    // ---------------------------------------------------------------- exists

    [Fact]
    public async Task Rest_Exists_Success()
    {
        _handler.SetupStatus(req => req.Method == HttpMethod.Head, HttpStatusCode.OK);

        (await _client.ExistsAsync("k")).Should().BeTrue();
    }

    [Fact]
    public async Task Rest_Exists_Error()
    {
        await Assert.ThrowsAsync<OperationFailedException>(() => ErrorClient().ExistsAsync("k"));
    }

    [Fact]
    public async Task Rest_Exists_NotFound()
    {
        _handler.SetupStatus(req => req.Method == HttpMethod.Head, HttpStatusCode.NotFound);

        (await _client.ExistsAsync("missing")).Should().BeFalse();
    }

    [Fact]
    public async Task Rest_Exists_ServerError_Throws()
    {
        // A 5xx must surface as an error, never a silent false (only 404 maps to false).
        _handler.SetupStatus(req => req.Method == HttpMethod.Head, HttpStatusCode.InternalServerError);

        await Assert.ThrowsAsync<OperationFailedException>(() => _client.ExistsAsync("boom"));
    }

    // ---------------------------------------------------------------- get_metadata

    [Fact]
    public async Task Rest_GetMetadata_Success()
    {
        var json = JsonSerializer.Serialize(new
        {
            size = 42L,
            etag = "\"m\"",
            content_type = "application/json",
            metadata = new Dictionary<string, string> { ["author"] = "alice" }
        });
        _handler.SetupJson(req => req.RequestUri!.ToString().Contains("/metadata/"), json);

        var metadata = await _client.GetMetadataAsync("k");

        metadata!.Size.Should().Be(42L);
        metadata.Custom!["author"].Should().Be("alice");
    }

    [Fact]
    public async Task Rest_GetMetadata_Error()
    {
        await Assert.ThrowsAsync<OperationFailedException>(() => ErrorClient().GetMetadataAsync("k"));
    }

    [Fact]
    public async Task Rest_GetMetadata_NotFound()
    {
        _handler.SetupStatus(req => req.RequestUri!.ToString().Contains("/metadata/"), HttpStatusCode.NotFound);

        (await _client.GetMetadataAsync("missing")).Should().BeNull();
    }

    // ---------------------------------------------------------------- update_metadata

    [Fact]
    public async Task Rest_UpdateMetadata_Success()
    {
        _handler.SetupStatus(
            req => req.Method == HttpMethod.Put && req.RequestUri!.ToString().Contains("/metadata/"),
            HttpStatusCode.OK);

        (await _client.UpdateMetadataAsync("k", new ObjectMetadata { ContentType = "text/plain" })).Should().BeTrue();
    }

    [Fact]
    public async Task Rest_UpdateMetadata_Error()
    {
        await Assert.ThrowsAsync<OperationFailedException>(
            () => ErrorClient().UpdateMetadataAsync("k", new ObjectMetadata()));
    }

    [Fact]
    public async Task Rest_UpdateMetadata_NotFound()
    {
        _handler.SetupStatus(
            req => req.Method == HttpMethod.Put && req.RequestUri!.ToString().Contains("/metadata/"),
            HttpStatusCode.NotFound);

        // A 404 on update must surface as ObjectNotFoundException, consistent with
        // GetAsync/GetMetadataAsync and the Go/Rust SDKs (404 -> not-found error).
        await Assert.ThrowsAsync<ObjectNotFoundException>(
            () => _client.UpdateMetadataAsync("missing", new ObjectMetadata()));
    }

    // ---------------------------------------------------------------- health

    [Fact]
    public async Task Rest_Health_Success()
    {
        _handler.SetupJson(
            req => req.RequestUri!.ToString().Contains("/health"),
            JsonSerializer.Serialize(new { status = "healthy", version = "1.0.0" }));

        var health = await _client.HealthAsync();

        health.Status.Should().Be(HealthStatus.Serving);
        health.Message.Should().Be("1.0.0");
    }

    [Fact]
    public async Task Rest_Health_Error()
    {
        await Assert.ThrowsAsync<ConnectionException>(() => ErrorClient().HealthAsync());
    }

    // ---------------------------------------------------------------- archive

    [Fact]
    public async Task Rest_Archive_Success()
    {
        _handler.SetupStatus(
            req => req.Method == HttpMethod.Post && req.RequestUri!.ToString().Contains("/archive"),
            HttpStatusCode.OK);

        (await _client.ArchiveAsync("k", "glacier")).Should().BeTrue();
    }

    [Fact]
    public async Task Rest_Archive_Error()
    {
        await Assert.ThrowsAsync<OperationFailedException>(() => ErrorClient().ArchiveAsync("k", "glacier"));
    }

    // ---------------------------------------------------------------- add_policy

    [Fact]
    public async Task Rest_AddPolicy_Success()
    {
        _handler.SetupStatus(
            req => req.Method == HttpMethod.Post && req.RequestUri!.ToString().EndsWith("/policies"),
            HttpStatusCode.Created);

        (await _client.AddPolicyAsync(new LifecyclePolicy { Id = "p1" })).Should().BeTrue();
    }

    [Fact]
    public async Task Rest_AddPolicy_Error()
    {
        await Assert.ThrowsAsync<OperationFailedException>(
            () => ErrorClient().AddPolicyAsync(new LifecyclePolicy { Id = "p1" }));
    }

    // ---------------------------------------------------------------- remove_policy

    [Fact]
    public async Task Rest_RemovePolicy_Success()
    {
        _handler.SetupStatus(
            req => req.Method == HttpMethod.Delete && req.RequestUri!.ToString().Contains("/policies/"),
            HttpStatusCode.OK);

        (await _client.RemovePolicyAsync("p1")).Should().BeTrue();
    }

    [Fact]
    public async Task Rest_RemovePolicy_Error()
    {
        await Assert.ThrowsAsync<OperationFailedException>(() => ErrorClient().RemovePolicyAsync("p1"));
    }

    [Fact]
    public async Task Rest_RemovePolicy_NotFound()
    {
        _handler.SetupStatus(
            req => req.Method == HttpMethod.Delete && req.RequestUri!.ToString().Contains("/policies/"),
            HttpStatusCode.NotFound);

        (await _client.RemovePolicyAsync("missing")).Should().BeFalse();
    }

    // ---------------------------------------------------------------- get_policies

    [Fact]
    public async Task Rest_GetPolicies_Success()
    {
        var json = JsonSerializer.Serialize(new
        {
            policies = new[] { new { id = "p1", prefix = "x/", action = "delete", retention_seconds = 60 } }
        });
        _handler.SetupJson(req => req.Method == HttpMethod.Get && req.RequestUri!.ToString().EndsWith("/policies"), json);

        var result = await _client.GetPoliciesAsync();

        result.Should().ContainSingle();
        result[0].Id.Should().Be("p1");
    }

    [Fact]
    public async Task Rest_GetPolicies_Error()
    {
        await Assert.ThrowsAsync<OperationFailedException>(() => ErrorClient().GetPoliciesAsync());
    }

    // ---------------------------------------------------------------- apply_policies

    [Fact]
    public async Task Rest_ApplyPolicies_Success()
    {
        _handler.SetupJson(
            req => req.Method == HttpMethod.Post && req.RequestUri!.ToString().Contains("/policies/apply"),
            JsonSerializer.Serialize(new { policies_count = 3, objects_processed = 9 }));

        var (success, count, processed) = await _client.ApplyPoliciesAsync();

        success.Should().BeTrue();
        count.Should().Be(3);
        processed.Should().Be(9);
    }

    [Fact]
    public async Task Rest_ApplyPolicies_Error()
    {
        await Assert.ThrowsAsync<OperationFailedException>(() => ErrorClient().ApplyPoliciesAsync());
    }

    // ---------------------------------------------------------------- add_replication_policy

    [Fact]
    public async Task Rest_AddReplicationPolicy_Success()
    {
        _handler.SetupStatus(
            req => req.Method == HttpMethod.Post && req.RequestUri!.ToString().Contains("/replication/policies"),
            HttpStatusCode.Created);

        (await _client.AddReplicationPolicyAsync(new ReplicationPolicy { Id = "r1" })).Should().BeTrue();
    }

    [Fact]
    public async Task Rest_AddReplicationPolicy_Error()
    {
        await Assert.ThrowsAsync<OperationFailedException>(
            () => ErrorClient().AddReplicationPolicyAsync(new ReplicationPolicy { Id = "r1" }));
    }

    // ---------------------------------------------------------------- remove_replication_policy

    [Fact]
    public async Task Rest_RemoveReplicationPolicy_Success()
    {
        _handler.SetupStatus(
            req => req.Method == HttpMethod.Delete && req.RequestUri!.ToString().Contains("/replication/policies/"),
            HttpStatusCode.OK);

        (await _client.RemoveReplicationPolicyAsync("r1")).Should().BeTrue();
    }

    [Fact]
    public async Task Rest_RemoveReplicationPolicy_Error()
    {
        await Assert.ThrowsAsync<OperationFailedException>(() => ErrorClient().RemoveReplicationPolicyAsync("r1"));
    }

    [Fact]
    public async Task Rest_RemoveReplicationPolicy_NotFound()
    {
        _handler.SetupStatus(
            req => req.Method == HttpMethod.Delete && req.RequestUri!.ToString().Contains("/replication/policies/"),
            HttpStatusCode.NotFound);

        (await _client.RemoveReplicationPolicyAsync("missing")).Should().BeFalse();
    }

    // ---------------------------------------------------------------- get_replication_policies

    [Fact]
    public async Task Rest_GetReplicationPolicies_Success()
    {
        var json = JsonSerializer.Serialize(new
        {
            policies = new[] { new { id = "r1", source_backend = "s3", destination_backend = "gcs", enabled = true } }
        });
        _handler.SetupJson(
            req => req.Method == HttpMethod.Get && req.RequestUri!.ToString().EndsWith("/replication/policies"),
            json);

        var result = await _client.GetReplicationPoliciesAsync();

        result.Should().ContainSingle();
        result[0].Id.Should().Be("r1");
    }

    [Fact]
    public async Task Rest_GetReplicationPolicies_Error()
    {
        await Assert.ThrowsAsync<OperationFailedException>(() => ErrorClient().GetReplicationPoliciesAsync());
    }

    // ---------------------------------------------------------------- get_replication_policy

    [Fact]
    public async Task Rest_GetReplicationPolicy_Success()
    {
        var json = JsonSerializer.Serialize(new ReplicationPolicy { Id = "r1", SourceBackend = "s3" });
        _handler.SetupJson(
            req => req.Method == HttpMethod.Get && req.RequestUri!.ToString().Contains("/replication/policies/r1"),
            json);

        var result = await _client.GetReplicationPolicyAsync("r1");

        result!.Id.Should().Be("r1");
    }

    [Fact]
    public async Task Rest_GetReplicationPolicy_Error()
    {
        await Assert.ThrowsAsync<OperationFailedException>(() => ErrorClient().GetReplicationPolicyAsync("r1"));
    }

    [Fact]
    public async Task Rest_GetReplicationPolicy_NotFound()
    {
        _handler.SetupStatus(
            req => req.RequestUri!.ToString().Contains("/replication/policies/"),
            HttpStatusCode.NotFound);

        await Assert.ThrowsAsync<PolicyNotFoundException>(() => _client.GetReplicationPolicyAsync("missing"));
    }

    // ---------------------------------------------------------------- trigger_replication

    [Fact]
    public async Task Rest_TriggerReplication_Success()
    {
        var json = JsonSerializer.Serialize(new
        {
            success = true,
            message = "replication triggered",
            result = new
            {
                policy_id = "r1",
                synced = 3,
                deleted = 0,
                failed = 0,
                bytes_total = 1024L,
                duration = "1.5s"
            }
        });
        _handler.SetupJson(
            req => req.Method == HttpMethod.Post && req.RequestUri!.ToString().Contains("/replication/trigger"),
            json);

        var result = await _client.TriggerReplicationAsync("r1");

        result.Success.Should().BeTrue();
        result.SyncResult.Should().NotBeNull();
        result.SyncResult!.PolicyId.Should().Be("r1");
        result.SyncResult.Synced.Should().Be(3);
        result.SyncResult.Deleted.Should().Be(0);
        result.SyncResult.Failed.Should().Be(0);
        result.SyncResult.BytesTotal.Should().Be(1024);
        result.SyncResult.DurationMs.Should().Be(1500);
    }

    [Fact]
    public async Task Rest_TriggerReplication_Error()
    {
        await Assert.ThrowsAsync<OperationFailedException>(() => ErrorClient().TriggerReplicationAsync("r1"));
    }

    // ---------------------------------------------------------------- get_replication_status

    [Fact]
    public async Task Rest_GetReplicationStatus_Success()
    {
        var json = JsonSerializer.Serialize(new ReplicationStatus { PolicyId = "r1", TotalObjectsSynced = 7 });
        _handler.SetupJson(
            req => req.RequestUri!.ToString().Contains("/replication/status/r1"),
            json);

        var result = await _client.GetReplicationStatusAsync("r1");

        result!.PolicyId.Should().Be("r1");
        result.TotalObjectsSynced.Should().Be(7);
    }

    [Fact]
    public async Task Rest_GetReplicationStatus_Error()
    {
        await Assert.ThrowsAsync<OperationFailedException>(() => ErrorClient().GetReplicationStatusAsync("r1"));
    }

    [Fact]
    public async Task Rest_GetReplicationStatus_NotFound()
    {
        _handler.SetupStatus(
            req => req.RequestUri!.ToString().Contains("/replication/status/"),
            HttpStatusCode.NotFound);

        (await _client.GetReplicationStatusAsync("missing")).Should().BeNull();
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
    public async Task Rest_Put_HttpStatus_MapsToCanonicalException(HttpStatusCode status, Type expected)
    {
        _handler.SetupStatus(req => req.Method == HttpMethod.Put, status);

        var ex = await Record.ExceptionAsync(() => _client.PutAsync("k", new byte[] { 1 }));

        ex.Should().BeOfType(expected);
        ((ObjectStoreException)ex!).StatusCode.Should().Be((int)status);
    }

    [Fact]
    public async Task Rest_HttpError_ExtractsServerErrorMessage()
    {
        // The server's {"error": "..."} body becomes the exception message.
        _handler.SetupJson(
            req => req.Method == HttpMethod.Put,
            JsonSerializer.Serialize(new { error = "quota exceeded" }),
            HttpStatusCode.TooManyRequests);

        var ex = await Assert.ThrowsAsync<RateLimitException>(() => _client.PutAsync("k", new byte[] { 1 }));
        ex.Message.Should().Contain("quota exceeded");
    }

    // ---------------------------------------------------------------- cross-cutting

    [Fact]
    public async Task Rest_Metadata_RoundTrip()
    {
        var meta = new ObjectMetadata
        {
            ContentType = "application/json",
            ContentEncoding = "gzip",
            Custom = new Dictionary<string, string> { ["author"] = "alice", ["team"] = "core" }
        };

        // PUT: assert wire scheme — Content-Type, Content-Encoding, X-Object-Metadata = JSON(custom only).
        HttpRequestMessage? putReq = null;
        _handler.SetupCapture(
            req => req.Method == HttpMethod.Put,
            req => putReq = req,
            () =>
            {
                var r = new HttpResponseMessage { StatusCode = HttpStatusCode.Created };
                r.Headers.ETag = new EntityTagHeaderValue("\"rt\"");
                return r;
            });

        await _client.PutAsync("k", Encoding.UTF8.GetBytes("body"), meta);

        putReq!.Content!.Headers.ContentType!.MediaType.Should().Be("application/json");
        putReq.Content.Headers.ContentEncoding.Should().Contain("gzip");
        putReq.Headers.TryGetValues("X-Object-Metadata", out var customValues).Should().BeTrue();
        var customJson = customValues!.First();
        var parsed = JsonSerializer.Deserialize<Dictionary<string, string>>(customJson)!;
        parsed["author"].Should().Be("alice");
        parsed["team"].Should().Be("core");
        customJson.Should().NotContain("content_type");
        customJson.Should().NotContain("content_encoding");

        // GET: custom returned via X-Object-Metadata response header.
        _handler.SetupResponse(
            req => req.Method == HttpMethod.Get,
            () =>
            {
                var r = new HttpResponseMessage { StatusCode = HttpStatusCode.OK, Content = new ByteArrayContent(Encoding.UTF8.GetBytes("body")) };
                r.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
                r.Content.Headers.ContentEncoding.Add("gzip");
                r.Headers.TryAddWithoutValidation("X-Object-Metadata", JsonSerializer.Serialize(meta.Custom));
                return r;
            });

        var (_, getMeta) = await _client.GetAsync("k");
        getMeta!.ContentType.Should().Be("application/json");
        getMeta.ContentEncoding.Should().Be("gzip");
        getMeta.Custom!["author"].Should().Be("alice");
        getMeta.Custom["team"].Should().Be("core");

        // GET_METADATA: custom returned in the JSON body's metadata object.
        _handler.SetupJson(
            req => req.RequestUri!.ToString().Contains("/metadata/"),
            JsonSerializer.Serialize(new
            {
                content_type = "application/json",
                metadata = meta.Custom
            }));

        var headMeta = await _client.GetMetadataAsync("k");
        headMeta!.ContentType.Should().Be("application/json");
        headMeta.Custom!["author"].Should().Be("alice");
        headMeta.Custom["team"].Should().Be("core");
    }

    [Fact]
    public async Task Rest_Validation_EmptyKey()
    {
        // Empty key is rejected before any network call: the handler is set to throw if ever hit.
        await Assert.ThrowsAsync<ArgumentNullException>(() => _client.PutAsync(null!, new byte[] { 1 }));
        await Assert.ThrowsAsync<ArgumentNullException>(() => _client.GetAsync(null!));
        await Assert.ThrowsAsync<ArgumentNullException>(() => _client.PutAsync("k", null!));
    }

    public void Dispose()
    {
        _client.Dispose();
        _httpClient.Dispose();
    }
}
