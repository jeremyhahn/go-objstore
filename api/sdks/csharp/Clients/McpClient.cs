using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using ObjStore.SDK.Exceptions;
using ObjStore.SDK.Extensions;
using ObjStore.SDK.Internal;
using ObjStore.SDK.Models;

namespace ObjStore.SDK.Clients;

/// <summary>
/// MCP (Model Context Protocol) client for go-objstore.
/// Wire protocol: HTTP POST JSON-RPC 2.0 to base URL "/".
/// Each operation maps to a tools/call request with name "objstore_&lt;op&gt;" and matching arguments.
/// Binary data is base64-encoded per the server protocol.
/// </summary>
public class McpClient : IObjectStoreClient
{
    private readonly HttpClient _httpClient;
    private readonly bool _disposeHttpClient;
    private readonly ILogger<McpClient>? _logger;
    private readonly string? _token;
    private readonly IDictionary<string, string>? _extraHeaders;
    private readonly string? _tenantId;
    private int _nextId;
    private bool _disposed;

    /// <summary>
    /// Maximum number of bytes PutStreamAsync may buffer in memory. The MCP
    /// transport carries object data base64-encoded inside JSON, so streams
    /// must be fully buffered; larger payloads should use REST or gRPC.
    /// Default: 64 MiB.
    /// </summary>
    public long MaxBufferSize { get; set; } = 64L * 1024 * 1024;

    /// <summary>
    /// Creates a new McpClient with a new HttpClient instance.
    /// </summary>
    /// <param name="baseUrl">Base URL of the MCP HTTP endpoint (e.g. "http://localhost:8081")</param>
    /// <param name="logger">Optional logger</param>
    /// <param name="token">Optional bearer token for Authorization header</param>
    /// <param name="headers">Optional additional HTTP headers</param>
    /// <param name="tenantId">Optional tenant ID for X-Tenant-ID header</param>
    public McpClient(string baseUrl, ILogger<McpClient>? logger = null, string? token = null, IDictionary<string, string>? headers = null, string? tenantId = null)
        : this(new HttpClient { BaseAddress = new Uri(baseUrl) }, logger, disposeHttpClient: true, token, headers, tenantId)
    {
    }

    /// <summary>
    /// Creates a new McpClient with an injected HttpClient (recommended for production use via IHttpClientFactory).
    /// </summary>
    /// <param name="httpClient">The HttpClient instance to use</param>
    /// <param name="logger">Optional logger</param>
    public McpClient(HttpClient httpClient, ILogger<McpClient>? logger = null)
        : this(httpClient, logger, disposeHttpClient: false, null, null, null)
    {
    }

    /// <summary>
    /// Creates a new McpClient from DI with client options (auth headers, tenant,
    /// MaxBufferSize). Preferred constructor for the typed-HttpClient registration.
    /// </summary>
    /// <param name="httpClient">The HttpClient instance to use</param>
    /// <param name="options">ObjectStore client options</param>
    /// <param name="logger">Optional logger</param>
    public McpClient(HttpClient httpClient, IOptions<ObjectStoreClientOptions> options, ILogger<McpClient>? logger = null)
        : this(httpClient, logger, disposeHttpClient: false, options.Value.Token, options.Value.Headers, options.Value.TenantId)
    {
        MaxBufferSize = options.Value.MaxBufferSize;
    }

    private McpClient(HttpClient httpClient, ILogger<McpClient>? logger, bool disposeHttpClient, string? token, IDictionary<string, string>? extraHeaders, string? tenantId)
    {
        _httpClient = httpClient ?? throw new ArgumentNullException(nameof(httpClient));
        _logger = logger;
        _disposeHttpClient = disposeHttpClient;
        _token = token;
        _extraHeaders = extraHeaders;
        _tenantId = tenantId;
    }

    // ---------------------------------------------------------------- IObjectStoreClient

    public async Task<string?> PutAsync(string key, byte[] data, ObjectMetadata? metadata = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(data);

        _logger?.LogDebug("MCP Put: key={Key} size={Size}", key, data.Length);

        var args = new JsonObject
        {
            ["key"] = key,
            ["data"] = Convert.ToBase64String(data)
        };

        if (metadata != null)
            args["metadata"] = BuildMetadataArgs(metadata);

        var result = await CallToolAsync("objstore_put", args, cancellationToken).ConfigureAwait(false);
        return result?["key"]?.GetValue<string>();
    }

    public Task<string?> PutWithMetadataAsync(string key, byte[] data, ObjectMetadata metadata, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(metadata);
        return PutAsync(key, data, metadata, cancellationToken);
    }

    public async Task<string?> PutStreamAsync(string key, Stream data, ObjectMetadata? metadata = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(data);

        // Buffer the stream; MCP transport does not support true streaming.
        // Enforce MaxBufferSize so an oversized stream fails fast instead of
        // exhausting memory.
        using var ms = new MemoryStream();
        var buffer = new byte[81920];
        long total = 0;
        int read;
        while ((read = await data.ReadAsync(buffer, cancellationToken).ConfigureAwait(false)) > 0)
        {
            total += read;
            if (total > MaxBufferSize)
            {
                throw new OperationFailedException(
                    "PutStream",
                    $"stream exceeds MaxBufferSize ({MaxBufferSize} bytes); use the REST or gRPC transport for large objects");
            }
            await ms.WriteAsync(buffer.AsMemory(0, read), cancellationToken).ConfigureAwait(false);
        }
        return await PutAsync(key, ms.ToArray(), metadata, cancellationToken).ConfigureAwait(false);
    }

    public async Task<(byte[] Data, ObjectMetadata? Metadata)> GetAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);

        _logger?.LogDebug("MCP Get: key={Key}", key);

        var result = await CallToolAsync("objstore_get", new JsonObject { ["key"] = key }, cancellationToken).ConfigureAwait(false)
            ?? throw new ObjectNotFoundException(key);

        var dataStr = result["data"]?.GetValue<string>() ?? string.Empty;
        byte[] bytes;

        // Object data is base64-encoded on the MCP transport; anything else is
        // a protocol violation and must surface as an error.
        try
        {
            bytes = string.IsNullOrEmpty(dataStr) ? Array.Empty<byte>() : Convert.FromBase64String(dataStr);
        }
        catch (FormatException ex)
        {
            throw new OperationFailedException("Get", $"Invalid base64 data in MCP response for key '{key}'", ex);
        }

        return (bytes, null);
    }

    public async Task<(Stream Data, ObjectMetadata? Metadata)> GetStreamAsync(string key, CancellationToken cancellationToken = default)
    {
        var (data, metadata) = await GetAsync(key, cancellationToken).ConfigureAwait(false);
        return (new MemoryStream(data), metadata);
    }

    public async Task<bool> DeleteAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);

        _logger?.LogDebug("MCP Delete: key={Key}", key);

        var result = await CallToolAsync("objstore_delete", new JsonObject { ["key"] = key }, cancellationToken).ConfigureAwait(false);
        return result?["deleted"]?.GetValue<bool>() ?? result?["success"]?.GetValue<bool>() ?? false;
    }

    public async Task<ListObjectsResponse> ListAsync(string? prefix = null, string? delimiter = null, int? maxResults = null, string? continueFrom = null, CancellationToken cancellationToken = default)
    {
        _logger?.LogDebug("MCP List: prefix={Prefix}", prefix);

        var args = new JsonObject();
        if (!string.IsNullOrEmpty(prefix))
            args["prefix"] = prefix;
        if (maxResults.HasValue)
            args["max_results"] = maxResults.Value;
        if (!string.IsNullOrEmpty(continueFrom))
            args["continue_from"] = continueFrom;

        var result = await CallToolAsync("objstore_list", args, cancellationToken).ConfigureAwait(false);
        if (result == null)
            return new ListObjectsResponse();

        var response = new ListObjectsResponse
        {
            Truncated = result["truncated"]?.GetValue<bool>() ?? false,
            NextToken = result["next_token"]?.GetValue<string>()
        };

        if (result["keys"] is JsonArray keys)
        {
            response.Objects = keys
                .Where(k => k != null)
                .Select(k => new ObjectInfo { Key = k!.GetValue<string>() })
                .ToList();
        }

        return response;
    }

    public async Task<bool> ExistsAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);

        _logger?.LogDebug("MCP Exists: key={Key}", key);

        var result = await CallToolAsync("objstore_exists", new JsonObject { ["key"] = key }, cancellationToken).ConfigureAwait(false);
        return result?["exists"]?.GetValue<bool>() ?? false;
    }

    public async Task<ObjectMetadata?> GetMetadataAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);

        _logger?.LogDebug("MCP GetMetadata: key={Key}", key);

        var result = await CallToolAsync("objstore_get_metadata", new JsonObject { ["key"] = key }, cancellationToken).ConfigureAwait(false);
        if (result == null)
            return null;

        return new ObjectMetadata
        {
            ContentType = result["content_type"]?.GetValue<string>(),
            ContentEncoding = result["content_encoding"]?.GetValue<string>(),
            Size = TryGetLong(result, "size"),
            ETag = result["etag"]?.GetValue<string>(),
            Custom = JsonHelpers.ParseCustomMap(result["custom"])
        };
    }

    public async Task<bool> UpdateMetadataAsync(string key, ObjectMetadata metadata, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(metadata);

        _logger?.LogDebug("MCP UpdateMetadata: key={Key}", key);

        var args = new JsonObject
        {
            ["key"] = key,
            ["metadata"] = BuildMetadataArgs(metadata)
        };

        var result = await CallToolAsync("objstore_update_metadata", args, cancellationToken).ConfigureAwait(false);
        return result?["updated"]?.GetValue<bool>() ?? result?["success"]?.GetValue<bool>() ?? false;
    }

    public async Task<HealthResponse> HealthAsync(string? service = null, CancellationToken cancellationToken = default)
    {
        _logger?.LogDebug("MCP Health");

        var result = await CallToolAsync("objstore_health", new JsonObject(), cancellationToken).ConfigureAwait(false);
        if (result == null)
            return new HealthResponse { Status = HealthStatus.Unknown };

        var status = result["status"]?.GetValue<string>()?.ToLowerInvariant();
        return new HealthResponse
        {
            Status = status switch
            {
                "healthy" or "serving" => HealthStatus.Serving,
                _ => HealthStatus.Unknown
            },
            Message = result["version"]?.GetValue<string>()
        };
    }

    public async Task<bool> ArchiveAsync(string key, string destinationType, Dictionary<string, string>? destinationSettings = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(destinationType);

        _logger?.LogDebug("MCP Archive: key={Key} dest={DestType}", key, destinationType);

        var args = new JsonObject
        {
            ["key"] = key,
            ["destination_type"] = destinationType
        };

        if (destinationSettings != null)
        {
            var settingsNode = new JsonObject();
            foreach (var kvp in destinationSettings)
                settingsNode[kvp.Key] = kvp.Value;
            args["destination_settings"] = settingsNode;
        }

        var result = await CallToolAsync("objstore_archive", args, cancellationToken).ConfigureAwait(false);
        return result?["archived"]?.GetValue<bool>() ?? result?["success"]?.GetValue<bool>() ?? false;
    }

    public async Task<bool> AddPolicyAsync(LifecyclePolicy policy, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(policy);

        _logger?.LogDebug("MCP AddPolicy: id={PolicyId}", policy.Id);

        var args = new JsonObject
        {
            ["id"] = policy.Id,
            ["prefix"] = policy.Prefix,
            ["retention_seconds"] = policy.RetentionSeconds,
            ["action"] = policy.Action
        };

        if (!string.IsNullOrEmpty(policy.DestinationType))
            args["destination_type"] = policy.DestinationType;

        var result = await CallToolAsync("objstore_add_policy", args, cancellationToken).ConfigureAwait(false);
        return result?["added"]?.GetValue<bool>() ?? result?["success"]?.GetValue<bool>() ?? false;
    }

    public async Task<bool> RemovePolicyAsync(string id, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(id);

        _logger?.LogDebug("MCP RemovePolicy: id={PolicyId}", id);

        var result = await CallToolAsync("objstore_remove_policy", new JsonObject { ["id"] = id }, cancellationToken).ConfigureAwait(false);
        return result?["removed"]?.GetValue<bool>() ?? result?["success"]?.GetValue<bool>() ?? false;
    }

    public async Task<List<LifecyclePolicy>> GetPoliciesAsync(string? prefix = null, CancellationToken cancellationToken = default)
    {
        _logger?.LogDebug("MCP GetPolicies");

        var args = new JsonObject();
        if (!string.IsNullOrEmpty(prefix))
            args["prefix"] = prefix;

        var result = await CallToolAsync("objstore_get_policies", args, cancellationToken).ConfigureAwait(false);
        if (result == null)
            return new List<LifecyclePolicy>();

        var list = new List<LifecyclePolicy>();
        if (result["policies"] is JsonArray policies)
        {
            foreach (var p in policies.Where(p => p != null))
            {
                list.Add(new LifecyclePolicy
                {
                    Id = p!["id"]?.GetValue<string>() ?? string.Empty,
                    Prefix = p["prefix"]?.GetValue<string>() ?? string.Empty,
                    Action = p["action"]?.GetValue<string>() ?? string.Empty,
                    RetentionSeconds = TryGetLong(p, "retention_seconds")
                });
            }
        }

        return list;
    }

    public async Task<(bool Success, int PoliciesCount, int ObjectsProcessed)> ApplyPoliciesAsync(CancellationToken cancellationToken = default)
    {
        _logger?.LogDebug("MCP ApplyPolicies");

        var result = await CallToolAsync("objstore_apply_policies", new JsonObject(), cancellationToken).ConfigureAwait(false);
        if (result == null)
            return (false, 0, 0);

        return (
            result["success"]?.GetValue<bool>() ?? false,
            (int)(result["policies_count"]?.GetValue<long>() ?? 0),
            (int)(result["objects_processed"]?.GetValue<long>() ?? 0)
        );
    }

    public async Task<bool> AddReplicationPolicyAsync(ReplicationPolicy policy, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(policy);

        _logger?.LogDebug("MCP AddReplicationPolicy: id={PolicyId}", policy.Id);

        var args = new JsonObject
        {
            ["id"] = policy.Id,
            ["source_backend"] = policy.SourceBackend,
            ["destination_backend"] = policy.DestinationBackend,
            ["check_interval"] = policy.CheckIntervalSeconds,
            ["enabled"] = policy.Enabled
        };

        var result = await CallToolAsync("objstore_add_replication_policy", args, cancellationToken).ConfigureAwait(false);
        return result?["success"]?.GetValue<bool>() ?? false;
    }

    public async Task<bool> RemoveReplicationPolicyAsync(string id, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(id);

        _logger?.LogDebug("MCP RemoveReplicationPolicy: id={PolicyId}", id);

        var result = await CallToolAsync("objstore_remove_replication_policy", new JsonObject { ["id"] = id }, cancellationToken).ConfigureAwait(false);
        return result?["success"]?.GetValue<bool>() ?? false;
    }

    public async Task<List<ReplicationPolicy>> GetReplicationPoliciesAsync(CancellationToken cancellationToken = default)
    {
        _logger?.LogDebug("MCP GetReplicationPolicies");

        var result = await CallToolAsync("objstore_list_replication_policies", new JsonObject(), cancellationToken).ConfigureAwait(false);
        if (result == null)
            return new List<ReplicationPolicy>();

        var list = new List<ReplicationPolicy>();
        if (result["policies"] is JsonArray policies)
        {
            foreach (var p in policies.Where(p => p != null))
            {
                list.Add(new ReplicationPolicy
                {
                    Id = p!["id"]?.GetValue<string>() ?? string.Empty,
                    SourceBackend = p["source_backend"]?.GetValue<string>() ?? string.Empty,
                    DestinationBackend = p["destination_backend"]?.GetValue<string>() ?? string.Empty,
                    CheckIntervalSeconds = TryGetLong(p, "check_interval"),
                    Enabled = p["enabled"]?.GetValue<bool>() ?? false
                });
            }
        }

        return list;
    }

    public async Task<ReplicationPolicy?> GetReplicationPolicyAsync(string id, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(id);

        _logger?.LogDebug("MCP GetReplicationPolicy: id={PolicyId}", id);

        var result = await CallToolAsync("objstore_get_replication_policy", new JsonObject { ["id"] = id }, cancellationToken).ConfigureAwait(false);
        if (result == null)
            return null;

        return new ReplicationPolicy
        {
            Id = result["id"]?.GetValue<string>() ?? string.Empty,
            SourceBackend = result["source_backend"]?.GetValue<string>() ?? string.Empty,
            DestinationBackend = result["destination_backend"]?.GetValue<string>() ?? string.Empty,
            CheckIntervalSeconds = TryGetLong(result, "check_interval"),
            Enabled = result["enabled"]?.GetValue<bool>() ?? false
        };
    }

    public async Task<TriggerReplicationResult> TriggerReplicationAsync(string? policyId = null, bool parallel = false, int workerCount = 4, CancellationToken cancellationToken = default)
    {
        _logger?.LogDebug("MCP TriggerReplication: policy={PolicyId}", policyId ?? "all");

        var args = new JsonObject();
        if (!string.IsNullOrEmpty(policyId))
            args["policy_id"] = policyId;

        var result = await CallToolAsync("objstore_trigger_replication", args, cancellationToken).ConfigureAwait(false);
        if (result == null)
            return new TriggerReplicationResult { Success = false };

        SyncResult? syncResult = null;
        if (result["result"] is JsonNode resultNode)
        {
            syncResult = new SyncResult
            {
                PolicyId = resultNode["policy_id"]?.GetValue<string>() ?? policyId ?? string.Empty,
                Synced = (int)(resultNode["synced"]?.GetValue<long>() ?? 0),
                Deleted = (int)(resultNode["deleted"]?.GetValue<long>() ?? 0),
                Failed = (int)(resultNode["failed"]?.GetValue<long>() ?? 0),
                BytesTotal = TryGetLong(resultNode, "bytes_total")
            };
        }

        return new TriggerReplicationResult
        {
            Success = result["success"]?.GetValue<bool>() ?? false,
            SyncResult = syncResult,
            Message = result["message"]?.GetValue<string>()
        };
    }

    public async Task<ReplicationStatus?> GetReplicationStatusAsync(string id, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(id);

        _logger?.LogDebug("MCP GetReplicationStatus: id={PolicyId}", id);

        var result = await CallToolAsync("objstore_get_replication_status", new JsonObject { ["policy_id"] = id }, cancellationToken).ConfigureAwait(false);
        if (result == null)
            return null;

        return new ReplicationStatus
        {
            PolicyId = result["policy_id"]?.GetValue<string>() ?? id,
            SourceBackend = result["source_backend"]?.GetValue<string>() ?? string.Empty,
            DestinationBackend = result["destination_backend"]?.GetValue<string>() ?? string.Empty,
            Enabled = result["enabled"]?.GetValue<bool>() ?? false,
            TotalObjectsSynced = TryGetLong(result, "total_objects_synced"),
            TotalObjectsDeleted = TryGetLong(result, "total_objects_deleted"),
            TotalBytesSynced = TryGetLong(result, "total_bytes_synced"),
            TotalErrors = TryGetLong(result, "total_errors"),
            SyncCount = TryGetLong(result, "sync_count")
        };
    }

    // ---------------------------------------------------------------- transport

    /// <summary>
    /// Applies auth headers to the outgoing request.
    /// </summary>
    private void ApplyAuthHeaders(HttpRequestMessage request) =>
        AuthHeaders.Apply(request, _token, _tenantId, _extraHeaders);

    /// <summary>
    /// Sends a JSON-RPC 2.0 tools/call request and returns the parsed tool-result JSON node.
    /// The MCP server wraps the result as result.content[0].text (JSON string) which this method parses.
    /// </summary>
    private async Task<JsonNode?> CallToolAsync(string toolName, JsonObject arguments, CancellationToken cancellationToken)
    {
        var id = Interlocked.Increment(ref _nextId);

        var rpcRequest = new JsonObject
        {
            ["jsonrpc"] = "2.0",
            ["method"] = "tools/call",
            ["id"] = id,
            ["params"] = new JsonObject
            {
                ["name"] = toolName,
                ["arguments"] = arguments
            }
        };

        using var httpRequest = new HttpRequestMessage(HttpMethod.Post, "/")
        {
            Content = new StringContent(rpcRequest.ToJsonString(), Encoding.UTF8, "application/json")
        };
        ApplyAuthHeaders(httpRequest);

        HttpResponseMessage? httpResponse = null;
        try
        {
            try
            {
                httpResponse = await _httpClient.SendAsync(httpRequest, cancellationToken).ConfigureAwait(false);
            }
            catch (HttpRequestException ex)
            {
                throw new ConnectionException(
                    _httpClient.BaseAddress?.ToString() ?? "unknown",
                    $"Failed to connect to MCP endpoint for tool '{toolName}'",
                    ex);
            }

            return await ParseToolResponseAsync(toolName, httpResponse, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            httpResponse?.Dispose();
        }
    }

    private static async Task<JsonNode?> ParseToolResponseAsync(string toolName, HttpResponseMessage httpResponse, CancellationToken cancellationToken)
    {
        // Non-success HTTP statuses (auth middleware, rate limiting, proxies) map to the
        // same canonical exception types as the REST and QUIC transports.
        await HttpErrorMapper.EnsureSuccessAsync(httpResponse, toolName, cancellationToken: cancellationToken).ConfigureAwait(false);

        var responseJson = await httpResponse.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
        var responseDoc = JsonNode.Parse(responseJson);
        if (responseDoc == null)
            throw new OperationFailedException(toolName, $"MCP tool '{toolName}' returned empty response");

        if (responseDoc["error"] is JsonNode errorNode)
        {
            // Classification is by JSON-RPC error code, never by message substring.
            var code = errorNode["code"]?.GetValue<int>() ?? 0;
            var message = errorNode["message"]?.GetValue<string>() ?? "Unknown error";
            throw JsonRpcErrorMapper.ToException(toolName, code, message);
        }

        // MCP result shape: result.content[0].text = JSON string of tool output
        var text = responseDoc["result"]?["content"]?[0]?["text"]?.GetValue<string>();
        if (string.IsNullOrEmpty(text))
            return null;

        return JsonNode.Parse(text);
    }

    // ---------------------------------------------------------------- helpers

    private static JsonObject BuildMetadataArgs(ObjectMetadata metadata)
    {
        var node = new JsonObject();
        if (!string.IsNullOrEmpty(metadata.ContentType))
            node["content_type"] = metadata.ContentType;
        if (!string.IsNullOrEmpty(metadata.ContentEncoding))
            node["content_encoding"] = metadata.ContentEncoding;
        if (metadata.Custom is { Count: > 0 })
        {
            var customNode = new JsonObject();
            foreach (var kvp in metadata.Custom)
                customNode[kvp.Key] = kvp.Value;
            node["custom"] = customNode;
        }
        return node;
    }

    private static long TryGetLong(JsonNode? node, string key)
    {
        if (node == null)
            return 0;

        var val = node[key];
        if (val == null)
            return 0;

        try { return val.GetValue<long>(); }
        catch { return 0; }
    }

    // ---------------------------------------------------------------- IDisposable

    /// <inheritdoc/>
    public void Dispose()
    {
        if (_disposed)
            return;

        if (_disposeHttpClient)
            _httpClient?.Dispose();

        _disposed = true;
        GC.SuppressFinalize(this);
    }
}
