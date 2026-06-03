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
/// Canonical QUIC/HTTP3 client unit tests. Same matrix as REST: success + error for all 19 ops,
/// not_found for the 9 listed ops, plus metadata_round_trip and validation_empty_key.
/// Transport is a mocked HttpMessageHandler. The QUIC wire scheme differs from REST: custom
/// metadata travels as one X-Meta-&lt;key&gt; header per entry and is read back from HEAD response headers.
/// For the _error cases a transport failure is raised; the implementation surfaces it as
/// OperationFailedException (Put) or HttpRequestException (the rest) — never a silent zero.
/// </summary>
public class QuicClientTests : IDisposable
{
    private readonly Mock<HttpMessageHandler> _handler;
    private readonly HttpClient _httpClient;
    private readonly QuicClient _client;

    public QuicClientTests()
    {
        _handler = new Mock<HttpMessageHandler>();
        _httpClient = new HttpClient(_handler.Object)
        {
            BaseAddress = new Uri("https://localhost:4433"),
            DefaultRequestVersion = HttpVersion.Version30,
            DefaultVersionPolicy = HttpVersionPolicy.RequestVersionOrHigher
        };
        _client = new QuicClient(_httpClient);
    }

    private QuicClient ErrorClient()
    {
        var handler = new Mock<HttpMessageHandler>();
        handler.SetupThrow();
        var http = new HttpClient(handler.Object) { BaseAddress = new Uri("https://localhost:4433") };
        return new QuicClient(http);
    }

    // ---------------------------------------------------------------- put

    [Fact]
    public async Task Quic_Put_Success()
    {
        _handler.SetupResponse(
            req => req.Method == HttpMethod.Put,
            () =>
            {
                var r = new HttpResponseMessage { StatusCode = HttpStatusCode.Created };
                r.Headers.ETag = new EntityTagHeaderValue("\"etag-1\"");
                return r;
            });

        (await _client.PutAsync("k", Encoding.UTF8.GetBytes("data"))).Should().Be("\"etag-1\"");
    }

    [Fact]
    public async Task Quic_Put_Error()
    {
        await Assert.ThrowsAsync<OperationFailedException>(() => ErrorClient().PutAsync("k", new byte[] { 1 }));
    }

    // ---------------------------------------------------------------- get

    [Fact]
    public async Task Quic_Get_Success()
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
    public async Task Quic_Get_Error()
    {
        await Assert.ThrowsAsync<HttpRequestException>(() => ErrorClient().GetAsync("k"));
    }

    [Fact]
    public async Task Quic_Get_NotFound()
    {
        _handler.SetupStatus(req => req.Method == HttpMethod.Get, HttpStatusCode.NotFound);

        await Assert.ThrowsAsync<ObjectNotFoundException>(() => _client.GetAsync("missing"));
    }

    [Fact]
    public async Task Quic_Get_ParsesCustomMetadata()
    {
        // GET must collect X-Meta-<key> response headers into Custom, same as GetMetadata.
        _handler.SetupResponse(
            req => req.Method == HttpMethod.Get,
            () =>
            {
                var r = new HttpResponseMessage { StatusCode = HttpStatusCode.OK, Content = new ByteArrayContent(Encoding.UTF8.GetBytes("body")) };
                r.Content.Headers.ContentType = new MediaTypeHeaderValue("text/plain");
                r.Headers.TryAddWithoutValidation("X-Meta-author", "alice");
                r.Headers.TryAddWithoutValidation("X-Meta-team", "core");
                return r;
            });

        var (_, metadata) = await _client.GetAsync("k");

        metadata!.Custom!["author"].Should().Be("alice");
        metadata.Custom["team"].Should().Be("core");
    }

    // ---------------------------------------------------------------- delete

    [Fact]
    public async Task Quic_Delete_Success()
    {
        _handler.SetupStatus(req => req.Method == HttpMethod.Delete, HttpStatusCode.OK);

        (await _client.DeleteAsync("k")).Should().BeTrue();
    }

    [Fact]
    public async Task Quic_Delete_Error()
    {
        await Assert.ThrowsAsync<HttpRequestException>(() => ErrorClient().DeleteAsync("k"));
    }

    [Fact]
    public async Task Quic_Delete_NotFound()
    {
        _handler.SetupStatus(req => req.Method == HttpMethod.Delete, HttpStatusCode.NotFound);

        (await _client.DeleteAsync("missing")).Should().BeFalse();
    }

    // ---------------------------------------------------------------- list

    [Fact]
    public async Task Quic_List_Success()
    {
        var json = JsonSerializer.Serialize(new ListObjectsResponse
        {
            Objects = new List<ObjectInfo> { new() { Key = "a" } }
        });
        _handler.SetupJson(req => req.Method == HttpMethod.Get, json);

        (await _client.ListAsync("p/")).Objects.Should().ContainSingle();
    }

    [Fact]
    public async Task Quic_List_Error()
    {
        await Assert.ThrowsAsync<HttpRequestException>(() => ErrorClient().ListAsync("p"));
    }

    // ---------------------------------------------------------------- exists

    [Fact]
    public async Task Quic_Exists_Success()
    {
        _handler.SetupStatus(req => req.Method == HttpMethod.Head, HttpStatusCode.OK);

        (await _client.ExistsAsync("k")).Should().BeTrue();
    }

    [Fact]
    public async Task Quic_Exists_Error()
    {
        await Assert.ThrowsAsync<HttpRequestException>(() => ErrorClient().ExistsAsync("k"));
    }

    [Fact]
    public async Task Quic_Exists_NotFound()
    {
        _handler.SetupStatus(req => req.Method == HttpMethod.Head, HttpStatusCode.NotFound);

        (await _client.ExistsAsync("missing")).Should().BeFalse();
    }

    [Fact]
    public async Task Quic_Exists_ServerError_Throws()
    {
        // A 5xx must surface as an error, never a silent false (only 404 maps to false).
        _handler.SetupStatus(req => req.Method == HttpMethod.Head, HttpStatusCode.InternalServerError);

        await Assert.ThrowsAsync<OperationFailedException>(() => _client.ExistsAsync("boom"));
    }

    // ---------------------------------------------------------------- get_metadata

    [Fact]
    public async Task Quic_GetMetadata_Success()
    {
        _handler.SetupResponse(
            req => req.Method == HttpMethod.Head,
            () =>
            {
                var r = new HttpResponseMessage { StatusCode = HttpStatusCode.OK, Content = new ByteArrayContent(Array.Empty<byte>()) };
                r.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
                r.Headers.TryAddWithoutValidation("X-Meta-author", "alice");
                return r;
            });

        var metadata = await _client.GetMetadataAsync("k");

        metadata!.ContentType.Should().Be("application/json");
        metadata.Custom!["author"].Should().Be("alice");
    }

    [Fact]
    public async Task Quic_GetMetadata_Error()
    {
        await Assert.ThrowsAsync<HttpRequestException>(() => ErrorClient().GetMetadataAsync("k"));
    }

    [Fact]
    public async Task Quic_GetMetadata_NotFound()
    {
        _handler.SetupStatus(req => req.Method == HttpMethod.Head, HttpStatusCode.NotFound);

        (await _client.GetMetadataAsync("missing")).Should().BeNull();
    }

    // ---------------------------------------------------------------- update_metadata

    [Fact]
    public async Task Quic_UpdateMetadata_Success()
    {
        _handler.SetupStatus(req => req.Method == HttpMethod.Patch, HttpStatusCode.OK);

        (await _client.UpdateMetadataAsync("k", new ObjectMetadata { ContentType = "text/plain" })).Should().BeTrue();
    }

    [Fact]
    public async Task Quic_UpdateMetadata_Error()
    {
        await Assert.ThrowsAsync<HttpRequestException>(
            () => ErrorClient().UpdateMetadataAsync("k", new ObjectMetadata()));
    }

    [Fact]
    public async Task Quic_UpdateMetadata_NotFound()
    {
        _handler.SetupStatus(req => req.Method == HttpMethod.Patch, HttpStatusCode.NotFound);

        await Assert.ThrowsAsync<ObjectNotFoundException>(
            () => _client.UpdateMetadataAsync("missing", new ObjectMetadata()));
    }

    // ---------------------------------------------------------------- health

    [Fact]
    public async Task Quic_Health_Success()
    {
        _handler.SetupJson(
            req => req.RequestUri!.ToString().Contains("/health"),
            JsonSerializer.Serialize(new { status = "healthy", version = "1.0.0" }));

        (await _client.HealthAsync()).Status.Should().Be(HealthStatus.Serving);
    }

    [Fact]
    public async Task Quic_Health_Error()
    {
        await Assert.ThrowsAsync<HttpRequestException>(() => ErrorClient().HealthAsync());
    }

    // ---------------------------------------------------------------- archive

    [Fact]
    public async Task Quic_Archive_Success()
    {
        _handler.SetupStatus(
            req => req.Method == HttpMethod.Post && req.RequestUri!.ToString().Contains("/archive"),
            HttpStatusCode.OK);

        (await _client.ArchiveAsync("k", "glacier")).Should().BeTrue();
    }

    [Fact]
    public async Task Quic_Archive_Error()
    {
        await Assert.ThrowsAsync<HttpRequestException>(() => ErrorClient().ArchiveAsync("k", "glacier"));
    }

    // ---------------------------------------------------------------- add_policy

    [Fact]
    public async Task Quic_AddPolicy_Success()
    {
        _handler.SetupStatus(
            req => req.Method == HttpMethod.Post && req.RequestUri!.ToString().EndsWith("/policies"),
            HttpStatusCode.Created);

        (await _client.AddPolicyAsync(new LifecyclePolicy { Id = "p1" })).Should().BeTrue();
    }

    [Fact]
    public async Task Quic_AddPolicy_Error()
    {
        await Assert.ThrowsAsync<HttpRequestException>(
            () => ErrorClient().AddPolicyAsync(new LifecyclePolicy { Id = "p1" }));
    }

    // ---------------------------------------------------------------- remove_policy

    [Fact]
    public async Task Quic_RemovePolicy_Success()
    {
        _handler.SetupStatus(
            req => req.Method == HttpMethod.Delete && req.RequestUri!.ToString().Contains("/policies/"),
            HttpStatusCode.OK);

        (await _client.RemovePolicyAsync("p1")).Should().BeTrue();
    }

    [Fact]
    public async Task Quic_RemovePolicy_Error()
    {
        await Assert.ThrowsAsync<HttpRequestException>(() => ErrorClient().RemovePolicyAsync("p1"));
    }

    [Fact]
    public async Task Quic_RemovePolicy_NotFound()
    {
        _handler.SetupStatus(
            req => req.Method == HttpMethod.Delete && req.RequestUri!.ToString().Contains("/policies/"),
            HttpStatusCode.NotFound);

        (await _client.RemovePolicyAsync("missing")).Should().BeFalse();
    }

    // ---------------------------------------------------------------- get_policies

    [Fact]
    public async Task Quic_GetPolicies_Success()
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
    public async Task Quic_GetPolicies_Error()
    {
        await Assert.ThrowsAsync<HttpRequestException>(() => ErrorClient().GetPoliciesAsync());
    }

    // ---------------------------------------------------------------- apply_policies

    [Fact]
    public async Task Quic_ApplyPolicies_Success()
    {
        _handler.SetupJson(
            req => req.Method == HttpMethod.Post && req.RequestUri!.ToString().Contains("/policies/apply"),
            JsonSerializer.Serialize(new { policies_count = 2, objects_processed = 5 }));

        var (success, count, processed) = await _client.ApplyPoliciesAsync();

        success.Should().BeTrue();
        count.Should().Be(2);
        processed.Should().Be(5);
    }

    [Fact]
    public async Task Quic_ApplyPolicies_Error()
    {
        await Assert.ThrowsAsync<HttpRequestException>(() => ErrorClient().ApplyPoliciesAsync());
    }

    // ---------------------------------------------------------------- add_replication_policy

    [Fact]
    public async Task Quic_AddReplicationPolicy_Success()
    {
        _handler.SetupStatus(
            req => req.Method == HttpMethod.Post && req.RequestUri!.ToString().Contains("/replication/policies"),
            HttpStatusCode.Created);

        (await _client.AddReplicationPolicyAsync(new ReplicationPolicy { Id = "r1" })).Should().BeTrue();
    }

    [Fact]
    public async Task Quic_AddReplicationPolicy_Error()
    {
        await Assert.ThrowsAsync<HttpRequestException>(
            () => ErrorClient().AddReplicationPolicyAsync(new ReplicationPolicy { Id = "r1" }));
    }

    // ---------------------------------------------------------------- remove_replication_policy

    [Fact]
    public async Task Quic_RemoveReplicationPolicy_Success()
    {
        _handler.SetupStatus(
            req => req.Method == HttpMethod.Delete && req.RequestUri!.ToString().Contains("/replication/policies/"),
            HttpStatusCode.OK);

        (await _client.RemoveReplicationPolicyAsync("r1")).Should().BeTrue();
    }

    [Fact]
    public async Task Quic_RemoveReplicationPolicy_Error()
    {
        await Assert.ThrowsAsync<HttpRequestException>(() => ErrorClient().RemoveReplicationPolicyAsync("r1"));
    }

    [Fact]
    public async Task Quic_RemoveReplicationPolicy_NotFound()
    {
        _handler.SetupStatus(
            req => req.Method == HttpMethod.Delete && req.RequestUri!.ToString().Contains("/replication/policies/"),
            HttpStatusCode.NotFound);

        (await _client.RemoveReplicationPolicyAsync("missing")).Should().BeFalse();
    }

    // ---------------------------------------------------------------- get_replication_policies

    [Fact]
    public async Task Quic_GetReplicationPolicies_Success()
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
    public async Task Quic_GetReplicationPolicies_Error()
    {
        await Assert.ThrowsAsync<HttpRequestException>(() => ErrorClient().GetReplicationPoliciesAsync());
    }

    // ---------------------------------------------------------------- get_replication_policy

    [Fact]
    public async Task Quic_GetReplicationPolicy_Success()
    {
        var json = JsonSerializer.Serialize(new ReplicationPolicy { Id = "r1", SourceBackend = "s3" });
        _handler.SetupJson(
            req => req.Method == HttpMethod.Get && req.RequestUri!.ToString().Contains("/replication/policies/r1"),
            json);

        (await _client.GetReplicationPolicyAsync("r1"))!.Id.Should().Be("r1");
    }

    [Fact]
    public async Task Quic_GetReplicationPolicy_Error()
    {
        await Assert.ThrowsAsync<HttpRequestException>(() => ErrorClient().GetReplicationPolicyAsync("r1"));
    }

    [Fact]
    public async Task Quic_GetReplicationPolicy_NotFound()
    {
        _handler.SetupStatus(
            req => req.RequestUri!.ToString().Contains("/replication/policies/"),
            HttpStatusCode.NotFound);

        (await _client.GetReplicationPolicyAsync("missing")).Should().BeNull();
    }

    // ---------------------------------------------------------------- trigger_replication

    [Fact]
    public async Task Quic_TriggerReplication_Success()
    {
        var json = JsonSerializer.Serialize(new
        {
            success = true,
            message = "replication triggered",
            result = new
            {
                policy_id = "r1",
                synced = 2,
                deleted = 0,
                failed = 0,
                bytes_total = 512L,
                duration = "300ms"
            }
        });
        _handler.SetupJson(
            req => req.Method == HttpMethod.Post && req.RequestUri!.ToString().Contains("/replication/trigger"),
            json);

        var result = await _client.TriggerReplicationAsync("r1");

        result.Success.Should().BeTrue();
        result.SyncResult.Should().NotBeNull();
        result.SyncResult!.PolicyId.Should().Be("r1");
        result.SyncResult.Synced.Should().Be(2);
        result.SyncResult.Deleted.Should().Be(0);
        result.SyncResult.Failed.Should().Be(0);
        result.SyncResult.BytesTotal.Should().Be(512);
        result.SyncResult.DurationMs.Should().Be(300);
    }

    [Fact]
    public async Task Quic_TriggerReplication_Error()
    {
        await Assert.ThrowsAsync<HttpRequestException>(() => ErrorClient().TriggerReplicationAsync("r1"));
    }

    // ---------------------------------------------------------------- get_replication_status

    [Fact]
    public async Task Quic_GetReplicationStatus_Success()
    {
        var json = JsonSerializer.Serialize(new ReplicationStatus { PolicyId = "r1", TotalObjectsSynced = 7 });
        _handler.SetupJson(req => req.RequestUri!.ToString().Contains("/replication/status/r1"), json);

        var result = await _client.GetReplicationStatusAsync("r1");

        result!.PolicyId.Should().Be("r1");
        result.TotalObjectsSynced.Should().Be(7);
    }

    [Fact]
    public async Task Quic_GetReplicationStatus_Error()
    {
        await Assert.ThrowsAsync<HttpRequestException>(() => ErrorClient().GetReplicationStatusAsync("r1"));
    }

    [Fact]
    public async Task Quic_GetReplicationStatus_NotFound()
    {
        _handler.SetupStatus(
            req => req.RequestUri!.ToString().Contains("/replication/status/"),
            HttpStatusCode.NotFound);

        (await _client.GetReplicationStatusAsync("missing")).Should().BeNull();
    }

    // ---------------------------------------------------------------- cross-cutting

    [Fact]
    public async Task Quic_Metadata_RoundTrip()
    {
        var meta = new ObjectMetadata
        {
            ContentType = "application/json",
            ContentEncoding = "gzip",
            Custom = new Dictionary<string, string> { ["author"] = "alice", ["team"] = "core" }
        };

        // PUT: assert wire scheme — Content-Type, Content-Encoding, one X-Meta-<key> per custom entry.
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
        putReq.Headers.GetValues("X-Meta-author").First().Should().Be("alice");
        putReq.Headers.GetValues("X-Meta-team").First().Should().Be("core");

        // Metadata read back via HEAD response headers.
        _handler.SetupResponse(
            req => req.Method == HttpMethod.Head,
            () =>
            {
                var r = new HttpResponseMessage { StatusCode = HttpStatusCode.OK, Content = new ByteArrayContent(Array.Empty<byte>()) };
                r.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
                r.Content.Headers.ContentEncoding.Add("gzip");
                r.Headers.TryAddWithoutValidation("X-Meta-author", "alice");
                r.Headers.TryAddWithoutValidation("X-Meta-team", "core");
                return r;
            });

        var headMeta = await _client.GetMetadataAsync("k");
        headMeta!.ContentType.Should().Be("application/json");
        headMeta.ContentEncoding.Should().Be("gzip");
        headMeta.Custom!["author"].Should().Be("alice");
        headMeta.Custom["team"].Should().Be("core");
    }

    [Fact]
    public async Task Quic_Validation_EmptyKey()
    {
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
