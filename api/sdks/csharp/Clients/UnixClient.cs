using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using ObjStore.SDK.Exceptions;
using ObjStore.SDK.Internal;
using ObjStore.SDK.Models;
using Microsoft.Extensions.Logging;

namespace ObjStore.SDK.Clients;

/// <summary>
/// Unix-domain socket client for go-objstore.
/// Wire protocol: newline-delimited JSON-RPC 2.0 over a persistent Unix socket
/// connection; request/response pairs are serialized on the single connection.
/// Authentication is peer-credential based on the server side; the client just connects.
/// Binary object data is base64-encoded per the server protocol.
/// </summary>
public class UnixClient : IObjectStoreClient
{
    private readonly string _socketPath;
    private readonly ILogger<UnixClient>? _logger;
    private readonly JsonSerializerOptions _jsonOptions;
    private readonly SemaphoreSlim _connectionLock = new(1, 1);
    private Socket? _socket;
    private int _nextId;
    private bool _disposed;

    /// <summary>
    /// Maximum number of bytes PutStreamAsync may buffer in memory. The Unix
    /// transport carries object data base64-encoded inside a single JSON-RPC
    /// request line, so streams must be fully buffered; larger payloads should
    /// use REST or gRPC. Default: 64 MiB. Note the server caps request lines
    /// at 10 MiB and base64 expands payloads by ~4/3, so raw payloads above
    /// roughly 7.5 MiB are rejected server-side regardless of this setting.
    /// </summary>
    public long MaxBufferSize { get; set; } = 64L * 1024 * 1024;

    /// <summary>
    /// Creates a new UnixClient.
    /// </summary>
    /// <param name="socketPath">Path to the Unix-domain socket (e.g. "/run/objstore/objstore.sock")</param>
    /// <param name="logger">Optional logger</param>
    public UnixClient(string socketPath, ILogger<UnixClient>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(socketPath);
        _socketPath = socketPath;
        _logger = logger;
        _jsonOptions = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };
    }

    // ---------------------------------------------------------------- IObjectStoreClient

    public async Task<string?> PutAsync(string key, byte[] data, ObjectMetadata? metadata = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(data);

        _logger?.LogDebug("Unix Put: key={Key} size={Size}", key, data.Length);

        var meta = metadata != null ? BuildMetadataNode(metadata) : null;
        var paramsNode = new JsonObject
        {
            ["key"] = key,
            ["data"] = Convert.ToBase64String(data)
        };
        if (meta != null)
            paramsNode["metadata"] = meta;

        var result = await CallAsync("put", paramsNode, cancellationToken).ConfigureAwait(false);
        return result?["etag"]?.GetValue<string>();
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

        // Buffer the stream; Unix transport does not support true streaming.
        // Enforce MaxBufferSize so an oversized stream fails fast instead of
        // exhausting memory (the server caps request lines at 10 MiB and the
        // payload is base64-expanded by ~4/3 on the wire).
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

        _logger?.LogDebug("Unix Get: key={Key}", key);

        var result = await CallAsync("get", new JsonObject { ["key"] = key }, cancellationToken).ConfigureAwait(false)
            ?? throw new ObjectNotFoundException(key);

        var b64 = result["data"]?.GetValue<string>() ?? string.Empty;
        var bytes = string.IsNullOrEmpty(b64) ? Array.Empty<byte>() : Convert.FromBase64String(b64);

        ObjectMetadata? meta = null;
        if (result["metadata"] is JsonNode metaNode)
            meta = ParseMetadataNode(metaNode);

        return (bytes, meta);
    }

    public async Task<(Stream Data, ObjectMetadata? Metadata)> GetStreamAsync(string key, CancellationToken cancellationToken = default)
    {
        var (data, metadata) = await GetAsync(key, cancellationToken).ConfigureAwait(false);
        return (new MemoryStream(data), metadata);
    }

    public async Task<bool> DeleteAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);

        _logger?.LogDebug("Unix Delete: key={Key}", key);

        // Unix protocol has no explicit delete result; reaching here without an exception means success.
        await CallAsync("delete", new JsonObject { ["key"] = key }, cancellationToken).ConfigureAwait(false);
        return true;
    }

    public async Task<ListObjectsResponse> ListAsync(string? prefix = null, string? delimiter = null, int? maxResults = null, string? continueFrom = null, CancellationToken cancellationToken = default)
    {
        _logger?.LogDebug("Unix List: prefix={Prefix}", prefix);

        var paramsNode = new JsonObject();
        if (!string.IsNullOrEmpty(prefix))
            paramsNode["prefix"] = prefix;
        if (!string.IsNullOrEmpty(delimiter))
            paramsNode["delimiter"] = delimiter;
        if (maxResults.HasValue)
            paramsNode["max_results"] = maxResults.Value;
        if (!string.IsNullOrEmpty(continueFrom))
            paramsNode["continue_from"] = continueFrom;

        var result = await CallAsync("list", paramsNode, cancellationToken).ConfigureAwait(false);
        if (result == null)
            return new ListObjectsResponse();

        var response = new ListObjectsResponse
        {
            Truncated = result["is_truncated"]?.GetValue<bool>() ?? false,
            NextToken = result["next_cursor"]?.GetValue<string>()
        };

        if (result["objects"] is JsonArray objects)
        {
            response.Objects = objects
                .Where(o => o != null)
                .Select(o => new ObjectInfo
                {
                    Key = o!["key"]?.GetValue<string>() ?? string.Empty,
                    Metadata = new ObjectMetadata
                    {
                        Size = o["size"]?.GetValue<long>() ?? 0,
                        ETag = o["etag"]?.GetValue<string>()
                    }
                })
                .ToList();
        }

        return response;
    }

    public async Task<bool> ExistsAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);

        _logger?.LogDebug("Unix Exists: key={Key}", key);

        var result = await CallAsync("exists", new JsonObject { ["key"] = key }, cancellationToken).ConfigureAwait(false);
        return result?["exists"]?.GetValue<bool>() ?? false;
    }

    public async Task<ObjectMetadata?> GetMetadataAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);

        _logger?.LogDebug("Unix GetMetadata: key={Key}", key);

        var result = await CallAsync("get_metadata", new JsonObject { ["key"] = key }, cancellationToken).ConfigureAwait(false);
        if (result == null)
            return null;

        // Server returns metadata fields directly on the result object: content_type, content_encoding, custom.
        return new ObjectMetadata
        {
            ContentType = result["content_type"]?.GetValue<string>(),
            ContentEncoding = result["content_encoding"]?.GetValue<string>(),
            Size = result["size"]?.GetValue<long>() ?? 0,
            ETag = result["etag"]?.GetValue<string>(),
            Custom = JsonHelpers.ParseCustomMap(result["custom"])
        };
    }

    public async Task<bool> UpdateMetadataAsync(string key, ObjectMetadata metadata, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(metadata);

        _logger?.LogDebug("Unix UpdateMetadata: key={Key}", key);

        var paramsNode = new JsonObject
        {
            ["key"] = key,
            ["metadata"] = BuildMetadataNode(metadata)
        };

        await CallAsync("update_metadata", paramsNode, cancellationToken).ConfigureAwait(false);
        return true;
    }

    public async Task<HealthResponse> HealthAsync(string? service = null, CancellationToken cancellationToken = default)
    {
        _logger?.LogDebug("Unix Health");

        var result = await CallAsync("health", new JsonObject(), cancellationToken).ConfigureAwait(false);
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

        _logger?.LogDebug("Unix Archive: key={Key} dest={DestType}", key, destinationType);

        var settingsNode = new JsonObject();
        if (destinationSettings != null)
        {
            foreach (var kvp in destinationSettings)
                settingsNode[kvp.Key] = kvp.Value;
        }

        var paramsNode = new JsonObject
        {
            ["key"] = key,
            ["destination_type"] = destinationType,
            ["destination_settings"] = settingsNode
        };

        await CallAsync("archive", paramsNode, cancellationToken).ConfigureAwait(false);
        return true;
    }

    public async Task<bool> AddPolicyAsync(LifecyclePolicy policy, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(policy);

        _logger?.LogDebug("Unix AddPolicy: id={PolicyId}", policy.Id);

        // The unix server accepts retention_seconds directly (it takes
        // precedence over after_days), so sub-day retention is supported.
        var paramsNode = new JsonObject
        {
            ["id"] = policy.Id,
            ["prefix"] = policy.Prefix,
            ["action"] = policy.Action,
            ["retention_seconds"] = policy.RetentionSeconds
        };

        await CallAsync("add_policy", paramsNode, cancellationToken).ConfigureAwait(false);
        return true;
    }

    public async Task<bool> RemovePolicyAsync(string id, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(id);

        _logger?.LogDebug("Unix RemovePolicy: id={PolicyId}", id);

        await CallAsync("remove_policy", new JsonObject { ["id"] = id }, cancellationToken).ConfigureAwait(false);
        return true;
    }

    public async Task<List<LifecyclePolicy>> GetPoliciesAsync(string? prefix = null, CancellationToken cancellationToken = default)
    {
        _logger?.LogDebug("Unix GetPolicies");

        var paramsNode = new JsonObject();
        if (!string.IsNullOrEmpty(prefix))
            paramsNode["prefix"] = prefix;

        var result = await CallAsync("get_policies", paramsNode, cancellationToken).ConfigureAwait(false);
        if (result == null)
            return new List<LifecyclePolicy>();

        // Server returns a top-level JSON array as the result value.
        var policies = result as JsonArray;
        if (policies == null)
            return new List<LifecyclePolicy>();

        var list = new List<LifecyclePolicy>();
        foreach (var p in policies.Where(p => p != null))
        {
            // Prefer retention_seconds; fall back to after_days for older servers.
            list.Add(new LifecyclePolicy
            {
                Id = p!["id"]?.GetValue<string>() ?? string.Empty,
                Prefix = p["prefix"]?.GetValue<string>() ?? string.Empty,
                Action = p["action"]?.GetValue<string>() ?? string.Empty,
                RetentionSeconds = p["retention_seconds"]?.GetValue<long>()
                    ?? (p["after_days"]?.GetValue<long>() ?? 0) * 86400
            });
        }

        return list;
    }

    public async Task<(bool Success, int PoliciesCount, int ObjectsProcessed)> ApplyPoliciesAsync(CancellationToken cancellationToken = default)
    {
        _logger?.LogDebug("Unix ApplyPolicies");

        var result = await CallAsync("apply_policies", new JsonObject(), cancellationToken).ConfigureAwait(false);
        if (result == null)
            return (true, 0, 0);

        // ApplyPoliciesResult: { policies_count, objects_processed } — no explicit success field.
        // Reaching here without an exception means success.
        return (
            true,
            result["policies_count"]?.GetValue<int>() ?? 0,
            result["objects_processed"]?.GetValue<int>() ?? 0
        );
    }

    public async Task<bool> AddReplicationPolicyAsync(ReplicationPolicy policy, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(policy);

        _logger?.LogDebug("Unix AddReplicationPolicy: id={PolicyId}", policy.Id);

        var destinationNode = new JsonObject();
        if (policy.DestinationSettings != null)
        {
            foreach (var kvp in policy.DestinationSettings)
                destinationNode[kvp.Key] = kvp.Value;
        }

        var paramsNode = new JsonObject
        {
            ["id"] = policy.Id,
            ["source_prefix"] = policy.SourcePrefix ?? string.Empty,
            ["destination_type"] = policy.DestinationBackend,
            ["destination"] = destinationNode,
            ["enabled"] = policy.Enabled
        };

        // The server parses schedule as a Go duration string (maps to CheckInterval).
        if (policy.CheckIntervalSeconds > 0)
            paramsNode["schedule"] = $"{policy.CheckIntervalSeconds}s";

        await CallAsync("add_replication_policy", paramsNode, cancellationToken).ConfigureAwait(false);
        return true;
    }

    public async Task<bool> RemoveReplicationPolicyAsync(string id, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(id);

        _logger?.LogDebug("Unix RemoveReplicationPolicy: id={PolicyId}", id);

        await CallAsync("remove_replication_policy", new JsonObject { ["id"] = id }, cancellationToken).ConfigureAwait(false);
        return true;
    }

    public async Task<List<ReplicationPolicy>> GetReplicationPoliciesAsync(CancellationToken cancellationToken = default)
    {
        _logger?.LogDebug("Unix GetReplicationPolicies");

        var result = await CallAsync("get_replication_policies", new JsonObject(), cancellationToken).ConfigureAwait(false);
        if (result == null)
            return new List<ReplicationPolicy>();

        // Server returns a top-level JSON array as the result value.
        var policies = result as JsonArray;
        if (policies == null)
            return new List<ReplicationPolicy>();

        var list = new List<ReplicationPolicy>();
        foreach (var p in policies.Where(p => p != null))
        {
            list.Add(new ReplicationPolicy
            {
                Id = p!["id"]?.GetValue<string>() ?? string.Empty,
                SourcePrefix = p["source_prefix"]?.GetValue<string>(),
                DestinationBackend = p["destination_type"]?.GetValue<string>() ?? string.Empty,
                Enabled = p["enabled"]?.GetValue<bool>() ?? false
            });
        }

        return list;
    }

    public async Task<ReplicationPolicy?> GetReplicationPolicyAsync(string id, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(id);

        _logger?.LogDebug("Unix GetReplicationPolicy: id={PolicyId}", id);

        var result = await CallAsync("get_replication_policy", new JsonObject { ["id"] = id }, cancellationToken).ConfigureAwait(false);
        if (result == null)
            return null;

        return new ReplicationPolicy
        {
            Id = result["id"]?.GetValue<string>() ?? string.Empty,
            SourcePrefix = result["source_prefix"]?.GetValue<string>(),
            DestinationBackend = result["destination_type"]?.GetValue<string>() ?? string.Empty,
            Enabled = result["enabled"]?.GetValue<bool>() ?? false
        };
    }

    public async Task<TriggerReplicationResult> TriggerReplicationAsync(string? policyId = null, bool parallel = false, int workerCount = 4, CancellationToken cancellationToken = default)
    {
        _logger?.LogDebug("Unix TriggerReplication: policy={PolicyId}", policyId ?? "all");

        var paramsNode = new JsonObject();
        if (!string.IsNullOrEmpty(policyId))
            paramsNode["id"] = policyId;

        var result = await CallAsync("trigger_replication", paramsNode, cancellationToken).ConfigureAwait(false);
        if (result == null)
            return new TriggerReplicationResult { Success = false };

        // Unix protocol returns: objects_synced, objects_failed, bytes_transferred, errors
        // There is no explicit "success" field; infer success from objects_failed == 0 and no errors.
        var objectsFailed = result["objects_failed"]?.GetValue<int>() ?? 0;
        var errors = result["errors"] as JsonArray;
        var success = objectsFailed == 0 && (errors == null || errors.Count == 0);

        return new TriggerReplicationResult
        {
            Success = success,
            SyncResult = new SyncResult
            {
                PolicyId = policyId ?? string.Empty,
                Synced = result["objects_synced"]?.GetValue<int>() ?? 0,
                Failed = objectsFailed,
                BytesTotal = result["bytes_transferred"]?.GetValue<long>() ?? 0
            }
        };
    }

    public async Task<ReplicationStatus?> GetReplicationStatusAsync(string id, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(id);

        _logger?.LogDebug("Unix GetReplicationStatus: id={PolicyId}", id);

        var result = await CallAsync("get_replication_status", new JsonObject { ["id"] = id }, cancellationToken).ConfigureAwait(false);
        if (result == null)
            return null;

        // ReplicationStatusResult: { policy_id, status, last_sync_time, objects_synced, objects_pending, objects_failed }
        return new ReplicationStatus
        {
            PolicyId = result["policy_id"]?.GetValue<string>() ?? string.Empty,
            TotalObjectsSynced = result["objects_synced"]?.GetValue<long>() ?? 0,
            TotalErrors = result["objects_failed"]?.GetValue<long>() ?? 0,
            Enabled = (result["status"]?.GetValue<string>() ?? string.Empty)
                      .Equals("active", StringComparison.OrdinalIgnoreCase)
        };
    }

    // ---------------------------------------------------------------- transport

    /// <summary>
    /// Sends one JSON-RPC 2.0 request over the persistent Unix socket connection
    /// and reads the matching response. A SemaphoreSlim serializes request/response
    /// pairs so multiple callers can share one UnixClient safely. The connection is
    /// established lazily and re-established on the next call after an IO error or
    /// response-id mismatch (the server closes idle connections after ~30s).
    /// </summary>
    private async Task<JsonNode?> CallAsync(string method, JsonNode paramsNode, CancellationToken cancellationToken)
    {
        var id = Interlocked.Increment(ref _nextId);

        var requestObj = new JsonObject
        {
            ["jsonrpc"] = "2.0",
            ["method"] = method,
            ["params"] = paramsNode,
            ["id"] = id
        };
        var requestJson = requestObj.ToJsonString() + "\n";
        var requestBytes = Encoding.UTF8.GetBytes(requestJson);

        await _connectionLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var socket = await GetConnectedSocketAsync(cancellationToken).ConfigureAwait(false);

            try
            {
                // Send request
                await socket.SendAsync(requestBytes, SocketFlags.None, cancellationToken).ConfigureAwait(false);

                // Read response until newline
                var responseJson = await ReadLineAsync(socket, cancellationToken).ConfigureAwait(false);

                var responseDoc = JsonNode.Parse(responseJson);
                if (responseDoc == null)
                    throw new OperationFailedException(method, "Received empty response from Unix socket");

                // The response id must match the request id; a mismatch means the
                // connection state is corrupted and it cannot be reused.
                var responseId = responseDoc["id"]?.GetValue<int>();
                if (responseId != id)
                {
                    DisposeSocket();
                    throw new OperationFailedException(
                        method,
                        $"Unix RPC '{method}' response id {responseId?.ToString() ?? "<null>"} does not match request id {id}");
                }

                // Check for JSON-RPC error; classification is by error code.
                if (responseDoc["error"] is JsonNode errorNode)
                {
                    var code = errorNode["code"]?.GetValue<int>() ?? 0;
                    var message = errorNode["message"]?.GetValue<string>() ?? "Unknown error";
                    throw JsonRpcErrorMapper.ToException(method, code, message);
                }

                return responseDoc["result"];
            }
            catch (ObjectStoreException)
            {
                throw;
            }
            catch (Exception ex)
            {
                // IO or protocol failure: the connection can no longer be trusted.
                // Dispose it so the next call reconnects.
                DisposeSocket();
                throw new OperationFailedException(method, $"Unix RPC '{method}' failed: {ex.Message}", ex);
            }
        }
        finally
        {
            _connectionLock.Release();
        }
    }

    /// <summary>
    /// Returns the persistent socket, dialing a new connection when none exists
    /// or the previous one was closed by the server while idle.
    /// Must be called while holding the connection lock.
    /// </summary>
    private async Task<Socket> GetConnectedSocketAsync(CancellationToken cancellationToken)
    {
        if (_socket != null && IsSocketAlive(_socket))
            return _socket;

        DisposeSocket();

        var socket = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
        try
        {
            var endpoint = new UnixDomainSocketEndPoint(_socketPath);
            await socket.ConnectAsync(endpoint, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is SocketException or IOException)
        {
            socket.Dispose();
            throw new ConnectionException(_socketPath, $"Failed to connect to Unix socket '{_socketPath}'", ex);
        }

        _socket = socket;
        return socket;
    }

    /// <summary>
    /// Detects whether the held connection is still usable. A socket that is
    /// readable with zero bytes available was closed by the peer (the server
    /// drops idle connections after ~30s).
    /// </summary>
    private static bool IsSocketAlive(Socket socket)
    {
        try
        {
            return socket.Connected && !(socket.Poll(0, SelectMode.SelectRead) && socket.Available == 0);
        }
        catch (Exception ex) when (ex is SocketException or ObjectDisposedException)
        {
            return false;
        }
    }

    /// <summary>
    /// Disposes the held connection so the next call dials a fresh one.
    /// Must be called while holding the connection lock (or from Dispose).
    /// </summary>
    private void DisposeSocket()
    {
        _socket?.Dispose();
        _socket = null;
    }

    /// <summary>
    /// Reads bytes from the socket until a newline byte is encountered and
    /// decodes the accumulated bytes as UTF-8 (multi-byte sequences may span
    /// receive chunks, so decoding happens once at the end).
    /// </summary>
    private static async Task<string> ReadLineAsync(Socket socket, CancellationToken cancellationToken)
    {
        var buffer = new byte[4096];
        using var line = new MemoryStream();

        while (true)
        {
            var received = await socket.ReceiveAsync(buffer, SocketFlags.None, cancellationToken).ConfigureAwait(false);
            if (received == 0)
                throw new IOException("Unix socket connection closed by server before a complete response was received");

            var newlineIndex = Array.IndexOf(buffer, (byte)'\n', 0, received);
            if (newlineIndex >= 0)
            {
                line.Write(buffer, 0, newlineIndex);
                break;
            }

            line.Write(buffer, 0, received);
        }

        return Encoding.UTF8.GetString(line.GetBuffer(), 0, (int)line.Length);
    }

    // ---------------------------------------------------------------- helpers

    private static JsonObject BuildMetadataNode(ObjectMetadata metadata)
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

    private static ObjectMetadata ParseMetadataNode(JsonNode metaNode)
    {
        return new ObjectMetadata
        {
            ContentType = metaNode["content_type"]?.GetValue<string>(),
            ContentEncoding = metaNode["content_encoding"]?.GetValue<string>(),
            Custom = JsonHelpers.ParseCustomMap(metaNode["custom"])
        };
    }

    // ---------------------------------------------------------------- IDisposable

    /// <inheritdoc/>
    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;
        DisposeSocket();
        _connectionLock.Dispose();
        GC.SuppressFinalize(this);
    }
}
