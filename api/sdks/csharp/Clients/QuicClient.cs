using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using ObjStore.SDK.Exceptions;
using ObjStore.SDK.Internal;
using ObjStore.SDK.Models;

namespace ObjStore.SDK.Clients;

/// <summary>
/// QUIC/HTTP3 client implementation for go-objstore
/// </summary>
public class QuicClient : IObjectStoreClient
{
    private readonly HttpClient _httpClient;
    private readonly bool _disposeHttpClient;
    private readonly ILogger<QuicClient>? _logger;
    private readonly JsonSerializerOptions _jsonOptions;
    private readonly string? _token;
    private readonly IDictionary<string, string>? _extraHeaders;
    private readonly string? _tenantId;
    private bool _disposed;

    /// <summary>
    /// Creates a new QuicClient with a new HttpClient instance (for backward compatibility)
    /// Note: When using this constructor, the HttpClient will be disposed when this client is disposed.
    /// For production use, prefer using IHttpClientFactory via dependency injection.
    /// </summary>
    /// <param name="baseUrl">The base URL of the object store service</param>
    /// <param name="logger">Optional logger instance</param>
    /// <param name="token">Optional bearer token for Authorization header</param>
    /// <param name="headers">Optional additional HTTP headers</param>
    /// <param name="tenantId">Optional tenant ID for X-Tenant-ID header</param>
    public QuicClient(string baseUrl, ILogger<QuicClient>? logger = null, string? token = null, IDictionary<string, string>? headers = null, string? tenantId = null)
        : this(CreateHttpClient(baseUrl), logger, disposeHttpClient: true, token, headers, tenantId)
    {
        _logger?.LogInformation("QuicClient initialized with HTTP/3 support for {BaseUrl}", baseUrl);
    }

    /// <summary>
    /// Creates a new QuicClient with an injected HttpClient (recommended for production use)
    /// When using IHttpClientFactory, the HttpClient should NOT be disposed by this class.
    /// </summary>
    /// <param name="httpClient">The HttpClient instance to use</param>
    /// <param name="logger">Optional logger instance</param>
    public QuicClient(HttpClient httpClient, ILogger<QuicClient>? logger = null)
        : this(httpClient, logger, disposeHttpClient: false, null, null, null)
    {
    }

    private QuicClient(HttpClient httpClient, ILogger<QuicClient>? logger, bool disposeHttpClient, string? token, IDictionary<string, string>? extraHeaders, string? tenantId)
    {
        _httpClient = httpClient ?? throw new ArgumentNullException(nameof(httpClient));
        _logger = logger;
        _disposeHttpClient = disposeHttpClient;
        _token = token;
        _extraHeaders = extraHeaders;
        _tenantId = tenantId;
        _jsonOptions = new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true
        };

        // Ensure HTTP/3 is configured
        if (_httpClient.DefaultRequestVersion < HttpVersion.Version30)
        {
            _httpClient.DefaultRequestVersion = HttpVersion.Version30;
            _httpClient.DefaultVersionPolicy = HttpVersionPolicy.RequestVersionOrHigher;
        }
    }

    /// <summary>
    /// Applies auth headers (Authorization, X-Tenant-ID, and any extra headers) to the request.
    /// </summary>
    private void ApplyAuthHeaders(HttpRequestMessage request) =>
        AuthHeaders.Apply(request, _token, _tenantId, _extraHeaders);

    private static HttpClient CreateHttpClient(string baseUrl)
    {
        var handler = new SocketsHttpHandler
        {
            EnableMultipleHttp2Connections = true,
            PooledConnectionLifetime = TimeSpan.FromMinutes(5)
        };

        return new HttpClient(handler)
        {
            BaseAddress = new Uri(baseUrl),
            DefaultRequestVersion = HttpVersion.Version30,
            DefaultVersionPolicy = HttpVersionPolicy.RequestVersionOrHigher
        };
    }

    public async Task<string?> PutAsync(string key, byte[] data, ObjectMetadata? metadata = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(data);

        _logger?.LogDebug("Putting object with key: {Key}, size: {Size} via HTTP/3", key, data.Length);

        try
        {
            // The QUIC server expects a RAW request body with metadata supplied via headers
            // (Content-Type, Content-Encoding, and per custom-key X-Meta-<key>).
            var content = new ByteArrayContent(data);

            if (metadata?.ContentType != null)
            {
                content.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue(metadata.ContentType);
            }
            if (!string.IsNullOrEmpty(metadata?.ContentEncoding))
            {
                content.Headers.ContentEncoding.Add(metadata.ContentEncoding);
            }

            using var request = new HttpRequestMessage(HttpMethod.Put, $"/objects/{Uri.EscapeDataString(key)}")
            {
                Content = content,
                Version = HttpVersion.Version30,
                VersionPolicy = HttpVersionPolicy.RequestVersionOrHigher
            };

            if (metadata?.Custom != null)
            {
                foreach (var kvp in metadata.Custom)
                {
                    request.Headers.TryAddWithoutValidation($"X-Meta-{kvp.Key}", kvp.Value);
                }
            }

            ApplyAuthHeaders(request);

            using var response = await _httpClient.SendAsync(request, cancellationToken).ConfigureAwait(false);
            await HttpErrorMapper.EnsureSuccessAsync(response, "Put", key, cancellationToken).ConfigureAwait(false);

            _logger?.LogDebug("PUT request used HTTP version: {Version}", response.Version);

            // The QUIC server returns {key, message} with no etag in the body.
            // The ETag is returned in the response header instead.
            if (response.Headers.ETag != null)
            {
                return response.Headers.ETag.Tag;
            }
            if (response.Headers.TryGetValues("ETag", out var etagValues))
            {
                return etagValues.FirstOrDefault();
            }
            return null;
        }
        catch (HttpRequestException ex)
        {
            throw new OperationFailedException("Put", $"Failed to put object with key '{key}' via HTTP/3", ex);
        }
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

        _logger?.LogDebug("Putting object stream with key: {Key} via HTTP/3", key);

        try
        {
            // NonDisposingStream keeps ownership of the caller's stream with the
            // caller; StreamContent would otherwise dispose it with the request.
            using var content = new StreamContent(new NonDisposingStream(data));

            if (metadata?.ContentType != null)
                content.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue(metadata.ContentType);
            if (!string.IsNullOrEmpty(metadata?.ContentEncoding))
                content.Headers.ContentEncoding.Add(metadata.ContentEncoding);

            using var request = new HttpRequestMessage(HttpMethod.Put, $"/objects/{Uri.EscapeDataString(key)}")
            {
                Content = content,
                Version = HttpVersion.Version30,
                VersionPolicy = HttpVersionPolicy.RequestVersionOrHigher
            };

            if (metadata?.Custom != null)
            {
                foreach (var kvp in metadata.Custom)
                    request.Headers.TryAddWithoutValidation($"X-Meta-{kvp.Key}", kvp.Value);
            }

            ApplyAuthHeaders(request);

            using var response = await _httpClient.SendAsync(request, cancellationToken).ConfigureAwait(false);
            await HttpErrorMapper.EnsureSuccessAsync(response, "PutStream", key, cancellationToken).ConfigureAwait(false);

            if (response.Headers.ETag != null)
                return response.Headers.ETag.Tag;
            if (response.Headers.TryGetValues("ETag", out var etagValues))
                return etagValues.FirstOrDefault();
            return null;
        }
        catch (HttpRequestException ex)
        {
            throw new OperationFailedException("PutStream", $"Failed to put object stream with key '{key}' via HTTP/3", ex);
        }
    }

    public async Task<(byte[] Data, ObjectMetadata? Metadata)> GetAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);

        _logger?.LogDebug("Getting object with key: {Key} via HTTP/3", key);

        using var request = new HttpRequestMessage(HttpMethod.Get, $"/objects/{Uri.EscapeDataString(key)}")
        {
            Version = HttpVersion.Version30,
            VersionPolicy = HttpVersionPolicy.RequestVersionOrHigher
        };

        ApplyAuthHeaders(request);

        using var response = await _httpClient.SendAsync(request, cancellationToken);

        if (response.StatusCode == HttpStatusCode.NotFound)
        {
            throw new ObjectNotFoundException(key);
        }

        await HttpErrorMapper.EnsureSuccessAsync(response, "Get", key, cancellationToken).ConfigureAwait(false);

        _logger?.LogDebug("GET request used HTTP version: {Version}", response.Version);

        var data = await response.Content.ReadAsByteArrayAsync(cancellationToken);

        var metadata = new ObjectMetadata
        {
            ContentType = response.Content.Headers.ContentType?.MediaType,
            Size = response.Content.Headers.ContentLength ?? data.Length,
            ETag = response.Headers.ETag?.Tag
        };

        if (response.Content.Headers.LastModified.HasValue)
        {
            metadata.LastModified = response.Content.Headers.LastModified.Value.DateTime;
        }

        // Custom metadata is returned as X-Meta-<key> response headers.
        var custom = new Dictionary<string, string>();
        foreach (var header in response.Headers)
        {
            if (header.Key.StartsWith("X-Meta-", StringComparison.OrdinalIgnoreCase))
            {
                custom[header.Key.Substring("X-Meta-".Length)] = string.Join(",", header.Value);
            }
        }
        if (custom.Count > 0)
        {
            metadata.Custom = custom;
        }

        return (data, metadata);
    }

    public async Task<(Stream Data, ObjectMetadata? Metadata)> GetStreamAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);

        _logger?.LogDebug("Getting object stream with key: {Key} via HTTP/3", key);

        using var request = new HttpRequestMessage(HttpMethod.Get, $"/objects/{Uri.EscapeDataString(key)}")
        {
            Version = HttpVersion.Version30,
            VersionPolicy = HttpVersionPolicy.RequestVersionOrHigher
        };

        ApplyAuthHeaders(request);

        var response = await _httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false);
        try
        {
            if (response.StatusCode == HttpStatusCode.NotFound)
                throw new ObjectNotFoundException(key);

            await HttpErrorMapper.EnsureSuccessAsync(response, "GetStream", key, cancellationToken).ConfigureAwait(false);

            var metadata = new ObjectMetadata
            {
                ContentType = response.Content.Headers.ContentType?.MediaType,
                Size = response.Content.Headers.ContentLength ?? 0,
                ETag = response.Headers.ETag?.Tag
            };

            if (response.Content.Headers.LastModified.HasValue)
                metadata.LastModified = response.Content.Headers.LastModified.Value.DateTime;

            var custom = new Dictionary<string, string>();
            foreach (var header in response.Headers)
            {
                if (header.Key.StartsWith("X-Meta-", StringComparison.OrdinalIgnoreCase))
                    custom[header.Key.Substring("X-Meta-".Length)] = string.Join(",", header.Value);
            }
            if (custom.Count > 0)
                metadata.Custom = custom;

            var stream = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
            // The wrapper owns the response: disposing the returned stream
            // disposes the response too.
            return (new ResponseOwningStream(stream, response), metadata);
        }
        catch
        {
            response.Dispose();
            throw;
        }
    }

    public async Task<bool> DeleteAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);

        _logger?.LogDebug("Deleting object with key: {Key} via HTTP/3", key);

        using var request = new HttpRequestMessage(HttpMethod.Delete, $"/objects/{Uri.EscapeDataString(key)}")
        {
            Version = HttpVersion.Version30,
            VersionPolicy = HttpVersionPolicy.RequestVersionOrHigher
        };

        ApplyAuthHeaders(request);

        using var response = await _httpClient.SendAsync(request, cancellationToken);
        _logger?.LogDebug("DELETE request used HTTP version: {Version}", response.Version);

        return response.IsSuccessStatusCode;
    }

    public async Task<ListObjectsResponse> ListAsync(string? prefix = null, string? delimiter = null, int? maxResults = null, string? continueFrom = null, CancellationToken cancellationToken = default)
    {
        _logger?.LogDebug("Listing objects with prefix: {Prefix} via HTTP/3", prefix);

        var queryParams = new List<string>();
        if (!string.IsNullOrEmpty(prefix))
            queryParams.Add($"prefix={Uri.EscapeDataString(prefix)}");
        if (!string.IsNullOrEmpty(delimiter))
            queryParams.Add($"delimiter={Uri.EscapeDataString(delimiter)}");
        if (maxResults.HasValue)
            queryParams.Add($"max={maxResults.Value}");
        if (!string.IsNullOrEmpty(continueFrom))
            queryParams.Add($"continue={Uri.EscapeDataString(continueFrom)}");

        var query = queryParams.Count > 0 ? "?" + string.Join("&", queryParams) : "";

        using var request = new HttpRequestMessage(HttpMethod.Get, $"/objects{query}")
        {
            Version = HttpVersion.Version30,
            VersionPolicy = HttpVersionPolicy.RequestVersionOrHigher
        };

        ApplyAuthHeaders(request);

        using var response = await _httpClient.SendAsync(request, cancellationToken);
        await HttpErrorMapper.EnsureSuccessAsync(response, "List", cancellationToken: cancellationToken).ConfigureAwait(false);

        _logger?.LogDebug("LIST request used HTTP version: {Version}", response.Version);

        return await response.Content.ReadFromJsonAsync<ListObjectsResponse>(_jsonOptions, cancellationToken)
            ?? new ListObjectsResponse();
    }

    public async Task<bool> ExistsAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);

        _logger?.LogDebug("Checking existence of object with key: {Key} via HTTP/3", key);

        using var request = new HttpRequestMessage(HttpMethod.Head, $"/objects/{Uri.EscapeDataString(key)}")
        {
            Version = HttpVersion.Version30,
            VersionPolicy = HttpVersionPolicy.RequestVersionOrHigher
        };

        ApplyAuthHeaders(request);

        using var response = await _httpClient.SendAsync(request, cancellationToken);
        _logger?.LogDebug("HEAD request used HTTP version: {Version}", response.Version);

        if (response.StatusCode == HttpStatusCode.NotFound)
        {
            return false;
        }

        await HttpErrorMapper.EnsureSuccessAsync(response, "Exists", key, cancellationToken).ConfigureAwait(false);

        return true;
    }

    public async Task<ObjectMetadata?> GetMetadataAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);

        _logger?.LogDebug("Getting metadata for object with key: {Key} via HTTP/3", key);

        // The QUIC server exposes metadata via HEAD /objects/{key}; there is no
        // dedicated /metadata route. Metadata is carried in the response headers.
        using var request = new HttpRequestMessage(HttpMethod.Head, $"/objects/{Uri.EscapeDataString(key)}")
        {
            Version = HttpVersion.Version30,
            VersionPolicy = HttpVersionPolicy.RequestVersionOrHigher
        };

        ApplyAuthHeaders(request);

        using var response = await _httpClient.SendAsync(request, cancellationToken);
        if (!response.IsSuccessStatusCode)
            return null;

        var metadata = new ObjectMetadata
        {
            ContentType = response.Content.Headers.ContentType?.MediaType,
            ContentEncoding = response.Content.Headers.ContentEncoding.FirstOrDefault(),
            Size = response.Content.Headers.ContentLength ?? 0,
            ETag = response.Headers.ETag?.Tag
        };

        if (response.Content.Headers.LastModified.HasValue)
        {
            metadata.LastModified = response.Content.Headers.LastModified.Value.DateTime;
        }

        // Custom metadata is returned as X-Meta-<key> response headers.
        var custom = new Dictionary<string, string>();
        foreach (var header in response.Headers)
        {
            if (header.Key.StartsWith("X-Meta-", StringComparison.OrdinalIgnoreCase))
            {
                custom[header.Key.Substring("X-Meta-".Length)] = string.Join(",", header.Value);
            }
        }
        if (custom.Count > 0)
        {
            metadata.Custom = custom;
        }

        return metadata;
    }

    public async Task<bool> UpdateMetadataAsync(string key, ObjectMetadata metadata, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(metadata);

        _logger?.LogDebug("Updating metadata for object with key: {Key} via HTTP/3", key);

        // The QUIC server updates metadata via PATCH /objects/{key} with a JSON body
        // of {content_type, content_encoding, custom}. A PUT to /objects/{key}/metadata
        // would instead store a NEW object at key "{key}/metadata".
        var requestData = new
        {
            content_type = metadata.ContentType,
            content_encoding = metadata.ContentEncoding,
            custom = metadata.Custom
        };

        using var request = new HttpRequestMessage(HttpMethod.Patch, $"/objects/{Uri.EscapeDataString(key)}")
        {
            Content = JsonContent.Create(requestData, options: _jsonOptions),
            Version = HttpVersion.Version30,
            VersionPolicy = HttpVersionPolicy.RequestVersionOrHigher
        };

        ApplyAuthHeaders(request);

        using var response = await _httpClient.SendAsync(request, cancellationToken);

        if (response.StatusCode == HttpStatusCode.NotFound)
        {
            throw new ObjectNotFoundException(key);
        }

        return response.IsSuccessStatusCode;
    }

    public async Task<HealthResponse> HealthAsync(string? service = null, CancellationToken cancellationToken = default)
    {
        _logger?.LogDebug("Checking health via HTTP/3");

        var query = !string.IsNullOrEmpty(service) ? $"?service={Uri.EscapeDataString(service)}" : "";

        using var request = new HttpRequestMessage(HttpMethod.Get, $"/health{query}")
        {
            Version = HttpVersion.Version30,
            VersionPolicy = HttpVersionPolicy.RequestVersionOrHigher
        };

        ApplyAuthHeaders(request);

        using var response = await _httpClient.SendAsync(request, cancellationToken);

        if (!response.IsSuccessStatusCode)
        {
            return new HealthResponse { Status = HealthStatus.NotServing };
        }

        var result = await response.Content.ReadFromJsonAsync<Dictionary<string, string>>(_jsonOptions, cancellationToken);
        var status = result?.GetValueOrDefault("status", "unknown");

        return new HealthResponse
        {
            Status = status?.ToLower() switch
            {
                "healthy" or "serving" => HealthStatus.Serving,
                _ => HealthStatus.Unknown
            },
            Message = result?.GetValueOrDefault("version")
        };
    }

    public async Task<bool> ArchiveAsync(string key, string destinationType, Dictionary<string, string>? destinationSettings = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(destinationType);

        _logger?.LogDebug("Archiving object with key: {Key} to {DestinationType} via HTTP/3", key, destinationType);

        var requestData = new
        {
            key,
            destination_type = destinationType,
            destination_settings = destinationSettings
        };

        using var request = new HttpRequestMessage(HttpMethod.Post, "/archive")
        {
            Content = JsonContent.Create(requestData, options: _jsonOptions),
            Version = HttpVersion.Version30,
            VersionPolicy = HttpVersionPolicy.RequestVersionOrHigher
        };

        ApplyAuthHeaders(request);

        using var response = await _httpClient.SendAsync(request, cancellationToken);
        return response.IsSuccessStatusCode;
    }

    public async Task<bool> AddPolicyAsync(LifecyclePolicy policy, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(policy);

        _logger?.LogDebug("Adding lifecycle policy: {PolicyId} via HTTP/3", policy.Id);

        using var request = new HttpRequestMessage(HttpMethod.Post, "/policies")
        {
            Content = JsonContent.Create(policy, options: _jsonOptions),
            Version = HttpVersion.Version30,
            VersionPolicy = HttpVersionPolicy.RequestVersionOrHigher
        };

        ApplyAuthHeaders(request);

        using var response = await _httpClient.SendAsync(request, cancellationToken);
        return response.IsSuccessStatusCode;
    }

    public async Task<bool> RemovePolicyAsync(string id, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(id);

        _logger?.LogDebug("Removing lifecycle policy: {PolicyId} via HTTP/3", id);

        using var request = new HttpRequestMessage(HttpMethod.Delete, $"/policies/{Uri.EscapeDataString(id)}")
        {
            Version = HttpVersion.Version30,
            VersionPolicy = HttpVersionPolicy.RequestVersionOrHigher
        };

        ApplyAuthHeaders(request);

        using var response = await _httpClient.SendAsync(request, cancellationToken);
        return response.IsSuccessStatusCode;
    }

    public async Task<List<LifecyclePolicy>> GetPoliciesAsync(string? prefix = null, CancellationToken cancellationToken = default)
    {
        _logger?.LogDebug("Getting lifecycle policies via HTTP/3");

        var query = !string.IsNullOrEmpty(prefix) ? $"?prefix={Uri.EscapeDataString(prefix)}" : "";

        using var request = new HttpRequestMessage(HttpMethod.Get, $"/policies{query}")
        {
            Version = HttpVersion.Version30,
            VersionPolicy = HttpVersionPolicy.RequestVersionOrHigher
        };

        ApplyAuthHeaders(request);

        using var response = await _httpClient.SendAsync(request, cancellationToken);

        if (!response.IsSuccessStatusCode)
            return new List<LifecyclePolicy>();

        var result = await response.Content.ReadFromJsonAsync<Dictionary<string, List<LifecyclePolicy>>>(_jsonOptions, cancellationToken);
        return result?.GetValueOrDefault("policies") ?? new List<LifecyclePolicy>();
    }

    public async Task<(bool Success, int PoliciesCount, int ObjectsProcessed)> ApplyPoliciesAsync(CancellationToken cancellationToken = default)
    {
        _logger?.LogDebug("Applying lifecycle policies via HTTP/3");

        using var request = new HttpRequestMessage(HttpMethod.Post, "/policies/apply")
        {
            Version = HttpVersion.Version30,
            VersionPolicy = HttpVersionPolicy.RequestVersionOrHigher
        };

        ApplyAuthHeaders(request);

        using var response = await _httpClient.SendAsync(request, cancellationToken);
        if (!response.IsSuccessStatusCode)
            return (false, 0, 0);

        var result = await response.Content.ReadFromJsonAsync<Dictionary<string, JsonElement>>(_jsonOptions, cancellationToken);
        if (result == null)
            return (false, 0, 0);

        return (
            true,
            result.GetValueOrDefault("policies_count").GetInt32(),
            result.GetValueOrDefault("objects_processed").GetInt32()
        );
    }

    public async Task<bool> AddReplicationPolicyAsync(ReplicationPolicy policy, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(policy);

        _logger?.LogDebug("Adding replication policy: {PolicyId} via HTTP/3", policy.Id);

        // The QUIC server expects the interval field as "check_interval" (seconds),
        // whereas the shared model serializes it as "check_interval_seconds" (REST).
        var requestData = new
        {
            id = policy.Id,
            source_backend = policy.SourceBackend,
            source_settings = policy.SourceSettings,
            source_prefix = policy.SourcePrefix,
            destination_backend = policy.DestinationBackend,
            destination_settings = policy.DestinationSettings,
            check_interval = policy.CheckIntervalSeconds,
            enabled = policy.Enabled,
            replication_mode = policy.ReplicationMode == ReplicationMode.Opaque ? "opaque" : "transparent",
            encryption = policy.Encryption
        };

        using var request = new HttpRequestMessage(HttpMethod.Post, "/replication/policies")
        {
            Content = JsonContent.Create(requestData, options: _jsonOptions),
            Version = HttpVersion.Version30,
            VersionPolicy = HttpVersionPolicy.RequestVersionOrHigher
        };

        ApplyAuthHeaders(request);

        using var response = await _httpClient.SendAsync(request, cancellationToken);
        return response.IsSuccessStatusCode;
    }

    public async Task<bool> RemoveReplicationPolicyAsync(string id, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(id);

        _logger?.LogDebug("Removing replication policy: {PolicyId} via HTTP/3", id);

        using var request = new HttpRequestMessage(HttpMethod.Delete, $"/replication/policies/{Uri.EscapeDataString(id)}")
        {
            Version = HttpVersion.Version30,
            VersionPolicy = HttpVersionPolicy.RequestVersionOrHigher
        };

        ApplyAuthHeaders(request);

        using var response = await _httpClient.SendAsync(request, cancellationToken);
        return response.IsSuccessStatusCode;
    }

    public async Task<List<ReplicationPolicy>> GetReplicationPoliciesAsync(CancellationToken cancellationToken = default)
    {
        _logger?.LogDebug("Getting replication policies via HTTP/3");

        using var request = new HttpRequestMessage(HttpMethod.Get, "/replication/policies")
        {
            Version = HttpVersion.Version30,
            VersionPolicy = HttpVersionPolicy.RequestVersionOrHigher
        };

        ApplyAuthHeaders(request);

        using var response = await _httpClient.SendAsync(request, cancellationToken);
        if (!response.IsSuccessStatusCode)
            return new List<ReplicationPolicy>();

        var result = await response.Content.ReadFromJsonAsync<Dictionary<string, List<ReplicationPolicy>>>(_jsonOptions, cancellationToken);
        return result?.GetValueOrDefault("policies") ?? new List<ReplicationPolicy>();
    }

    public async Task<ReplicationPolicy?> GetReplicationPolicyAsync(string id, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(id);

        _logger?.LogDebug("Getting replication policy: {PolicyId} via HTTP/3", id);

        using var request = new HttpRequestMessage(HttpMethod.Get, $"/replication/policies/{Uri.EscapeDataString(id)}")
        {
            Version = HttpVersion.Version30,
            VersionPolicy = HttpVersionPolicy.RequestVersionOrHigher
        };

        ApplyAuthHeaders(request);

        using var response = await _httpClient.SendAsync(request, cancellationToken);
        if (!response.IsSuccessStatusCode)
            return null;

        // The QUIC server returns the policy fields flat at the top level
        // (alongside "success"), not wrapped under a "policy" key.
        return await response.Content.ReadFromJsonAsync<ReplicationPolicy>(_jsonOptions, cancellationToken);
    }

    public async Task<TriggerReplicationResult> TriggerReplicationAsync(string? policyId = null, bool parallel = false, int workerCount = 4, CancellationToken cancellationToken = default)
    {
        _logger?.LogDebug("Triggering replication for policy: {PolicyId} via HTTP/3", policyId ?? "all");

        // The QUIC server takes policy_id as a QUERY param (empty = sync all), not a JSON body.
        var query = !string.IsNullOrEmpty(policyId) ? $"?policy_id={Uri.EscapeDataString(policyId)}" : "";

        using var request = new HttpRequestMessage(HttpMethod.Post, $"/replication/trigger{query}")
        {
            Version = HttpVersion.Version30,
            VersionPolicy = HttpVersionPolicy.RequestVersionOrHigher
        };

        ApplyAuthHeaders(request);

        using var response = await _httpClient.SendAsync(request, cancellationToken);

        if (!response.IsSuccessStatusCode)
            return new TriggerReplicationResult { Success = false };

        var body = await response.Content.ReadFromJsonAsync<JsonElement>(_jsonOptions, cancellationToken).ConfigureAwait(false);

        var success = body.TryGetProperty("success", out var successEl) && successEl.GetBoolean();
        var message = body.TryGetProperty("message", out var msgEl) ? msgEl.GetString() : null;

        SyncResult? syncResult = null;
        if (body.TryGetProperty("result", out var resultEl) && resultEl.ValueKind == JsonValueKind.Object)
        {
            syncResult = new SyncResult
            {
                PolicyId = resultEl.TryGetProperty("policy_id", out var pidEl) ? pidEl.GetString() ?? string.Empty : string.Empty,
                Synced = resultEl.TryGetProperty("synced", out var syncedEl) ? syncedEl.GetInt32() : 0,
                Deleted = resultEl.TryGetProperty("deleted", out var deletedEl) ? deletedEl.GetInt32() : 0,
                Failed = resultEl.TryGetProperty("failed", out var failedEl) ? failedEl.GetInt32() : 0,
                BytesTotal = resultEl.TryGetProperty("bytes_total", out var bytesEl) ? bytesEl.GetInt64() : 0,
                DurationMs = ParseGoDurationToMs(resultEl.TryGetProperty("duration", out var durEl) ? durEl.GetString() : null)
            };
        }

        return new TriggerReplicationResult
        {
            Success = success,
            SyncResult = syncResult,
            Message = message
        };
    }

    /// <summary>
    /// Parses a Go duration string (e.g. "5.2s", "300ms", "1m30s") into milliseconds.
    /// Returns 0 if the input is null or cannot be parsed.
    /// </summary>
    private static long ParseGoDurationToMs(string? duration)
    {
        if (string.IsNullOrEmpty(duration))
            return 0;

        double totalNs = 0;
        var s = duration.AsSpan();

        while (!s.IsEmpty)
        {
            int numEnd = 0;
            while (numEnd < s.Length && (char.IsDigit(s[numEnd]) || s[numEnd] == '.'))
                numEnd++;

            if (numEnd == 0)
                return 0;

            if (!double.TryParse(s[..numEnd], System.Globalization.NumberStyles.Float,
                    System.Globalization.CultureInfo.InvariantCulture, out var value))
                return 0;

            s = s[numEnd..];

            int unitEnd = 0;
            while (unitEnd < s.Length && !char.IsDigit(s[unitEnd]) && s[unitEnd] != '.')
                unitEnd++;

            var unit = s[..unitEnd].ToString();
            s = s[unitEnd..];

            totalNs += unit switch
            {
                "ns" => value,
                "us" or "µs" => value * 1_000,
                "ms" => value * 1_000_000,
                "s" => value * 1_000_000_000,
                "m" => value * 60_000_000_000,
                "h" => value * 3_600_000_000_000,
                _ => 0
            };
        }

        return (long)(totalNs / 1_000_000);
    }

    public async Task<ReplicationStatus?> GetReplicationStatusAsync(string id, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(id);

        _logger?.LogDebug("Getting replication status for policy: {PolicyId} via HTTP/3", id);

        using var request = new HttpRequestMessage(HttpMethod.Get, $"/replication/status/{Uri.EscapeDataString(id)}")
        {
            Version = HttpVersion.Version30,
            VersionPolicy = HttpVersionPolicy.RequestVersionOrHigher
        };

        ApplyAuthHeaders(request);

        using var response = await _httpClient.SendAsync(request, cancellationToken);
        if (!response.IsSuccessStatusCode)
            return null;

        // The QUIC server returns the status fields flat at the top level
        // (alongside "success"), not wrapped under a "status" key.
        return await response.Content.ReadFromJsonAsync<ReplicationStatus>(_jsonOptions, cancellationToken);
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        // Only dispose HttpClient if we created it (backward compatibility mode)
        if (_disposeHttpClient)
        {
            _httpClient?.Dispose();
        }

        _disposed = true;
        GC.SuppressFinalize(this);
    }
}
