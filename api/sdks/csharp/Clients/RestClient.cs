using System.Net.Http.Json;
using System.Text.Json;
using ObjStore.SDK.Exceptions;
using ObjStore.SDK.Internal;
using ObjStore.SDK.Models;
using Microsoft.Extensions.Logging;

namespace ObjStore.SDK.Clients;

/// <summary>
/// REST client implementation for go-objstore
/// </summary>
public class RestClient : IObjectStoreClient
{
    private readonly HttpClient _httpClient;
    private readonly bool _disposeHttpClient;
    private readonly ILogger<RestClient>? _logger;
    private readonly JsonSerializerOptions _jsonOptions;
    private readonly string? _token;
    private readonly IDictionary<string, string>? _extraHeaders;
    private readonly string? _tenantId;
    private bool _disposed;

    /// <summary>
    /// Creates a new RestClient with a new HttpClient instance (for backward compatibility)
    /// Note: When using this constructor, the HttpClient will be disposed when this client is disposed.
    /// For production use, prefer using IHttpClientFactory via dependency injection.
    /// </summary>
    /// <param name="baseUrl">The base URL of the object store service</param>
    /// <param name="logger">Optional logger instance</param>
    /// <param name="token">Optional bearer token for Authorization header</param>
    /// <param name="headers">Optional additional HTTP headers</param>
    /// <param name="tenantId">Optional tenant ID for X-Tenant-ID header</param>
    public RestClient(string baseUrl, ILogger<RestClient>? logger = null, string? token = null, IDictionary<string, string>? headers = null, string? tenantId = null)
        : this(new HttpClient { BaseAddress = new Uri(baseUrl) }, logger, disposeHttpClient: true, token, headers, tenantId)
    {
    }

    /// <summary>
    /// Creates a new RestClient with an injected HttpClient (recommended for production use)
    /// When using IHttpClientFactory, the HttpClient should NOT be disposed by this class.
    /// </summary>
    /// <param name="httpClient">The HttpClient instance to use</param>
    /// <param name="logger">Optional logger instance</param>
    public RestClient(HttpClient httpClient, ILogger<RestClient>? logger = null)
        : this(httpClient, logger, disposeHttpClient: false, null, null, null)
    {
    }

    private RestClient(HttpClient httpClient, ILogger<RestClient>? logger, bool disposeHttpClient, string? token, IDictionary<string, string>? extraHeaders, string? tenantId)
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
    }

    /// <summary>
    /// Applies auth headers (Authorization, X-Tenant-ID, and any extra headers) to the request.
    /// </summary>
    private void ApplyAuthHeaders(HttpRequestMessage request) =>
        AuthHeaders.Apply(request, _token, _tenantId, _extraHeaders);

    public async Task<string?> PutAsync(string key, byte[] data, ObjectMetadata? metadata = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(data);

        _logger?.LogDebug("Putting object with key: {Key}, size: {Size}", key, data.Length);

        try
        {
            using var content = new ByteArrayContent(data);

            // Content-Type header carries the object's MIME type; default to octet-stream.
            var contentType = !string.IsNullOrEmpty(metadata?.ContentType)
                ? metadata!.ContentType!
                : "application/octet-stream";
            content.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue(contentType);

            using var request = new HttpRequestMessage(HttpMethod.Put, $"/objects/{Uri.EscapeDataString(key)}")
            {
                Content = content
            };

            // Content-Encoding header only when present.
            if (!string.IsNullOrEmpty(metadata?.ContentEncoding))
            {
                content.Headers.ContentEncoding.Add(metadata!.ContentEncoding!);
            }

            // Custom metadata (string->string map only) travels as JSON in the
            // X-Object-Metadata request header. Omit the header when empty.
            if (metadata?.Custom is { Count: > 0 })
            {
                var customJson = JsonSerializer.Serialize(metadata.Custom, _jsonOptions);
                request.Headers.TryAddWithoutValidation("X-Object-Metadata", customJson);
            }

            ApplyAuthHeaders(request);

            var response = await _httpClient.SendAsync(request, cancellationToken).ConfigureAwait(false);
            await HttpErrorMapper.EnsureSuccessAsync(response, "Put", key, cancellationToken).ConfigureAwait(false);

            // ETag is returned in the response header, not the body
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
            throw new OperationFailedException("Put", $"Failed to put object with key '{key}'", ex);
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

        _logger?.LogDebug("Putting object stream with key: {Key}", key);

        try
        {
            // NonDisposingStream keeps ownership of the caller's stream with the
            // caller; StreamContent would otherwise dispose it with the request.
            using var content = new StreamContent(new NonDisposingStream(data));

            var contentType = !string.IsNullOrEmpty(metadata?.ContentType)
                ? metadata!.ContentType!
                : "application/octet-stream";
            content.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue(contentType);

            if (!string.IsNullOrEmpty(metadata?.ContentEncoding))
                content.Headers.ContentEncoding.Add(metadata!.ContentEncoding!);

            using var request = new HttpRequestMessage(HttpMethod.Put, $"/objects/{Uri.EscapeDataString(key)}")
            {
                Content = content
            };

            if (metadata?.Custom is { Count: > 0 })
            {
                var customJson = JsonSerializer.Serialize(metadata.Custom, _jsonOptions);
                request.Headers.TryAddWithoutValidation("X-Object-Metadata", customJson);
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
            throw new OperationFailedException("PutStream", $"Failed to put object stream with key '{key}'", ex);
        }
    }

    public async Task<(byte[] Data, ObjectMetadata? Metadata)> GetAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);

        _logger?.LogDebug("Getting object with key: {Key}", key);

        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Get, $"/objects/{Uri.EscapeDataString(key)}");
            ApplyAuthHeaders(request);
            using var response = await _httpClient.SendAsync(request, cancellationToken).ConfigureAwait(false);

            if (response.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                throw new ObjectNotFoundException(key);
            }

            await HttpErrorMapper.EnsureSuccessAsync(response, "Get", key, cancellationToken).ConfigureAwait(false);

            var data = await response.Content.ReadAsByteArrayAsync(cancellationToken).ConfigureAwait(false);

            var metadata = new ObjectMetadata
            {
                ContentType = response.Content.Headers.ContentType?.MediaType,
                ContentEncoding = response.Content.Headers.ContentEncoding.FirstOrDefault(),
                Size = response.Content.Headers.ContentLength ?? data.Length,
                ETag = response.Headers.ETag?.Tag
            };

            if (response.Content.Headers.LastModified.HasValue)
            {
                metadata.LastModified = response.Content.Headers.LastModified.Value.DateTime;
            }

            // Custom metadata is returned as a JSON object in the X-Object-Metadata header.
            if (response.Headers.TryGetValues("X-Object-Metadata", out var customValues))
            {
                var customJson = customValues.FirstOrDefault();
                if (!string.IsNullOrEmpty(customJson))
                {
                    metadata.Custom = JsonSerializer.Deserialize<Dictionary<string, string>>(customJson, _jsonOptions);
                }
            }

            return (data, metadata);
        }
        catch (ObjectNotFoundException)
        {
            throw;
        }
        catch (HttpRequestException ex)
        {
            throw new OperationFailedException("Get", $"Failed to get object with key '{key}'", ex);
        }
    }

    public async Task<(Stream Data, ObjectMetadata? Metadata)> GetStreamAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);

        _logger?.LogDebug("Getting object stream with key: {Key}", key);

        HttpResponseMessage? response = null;
        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Get, $"/objects/{Uri.EscapeDataString(key)}");
            ApplyAuthHeaders(request);

            // ResponseHeadersRead lets the caller stream the body without buffering it first.
            response = await _httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false);

            if (response.StatusCode == System.Net.HttpStatusCode.NotFound)
                throw new ObjectNotFoundException(key);

            await HttpErrorMapper.EnsureSuccessAsync(response, "GetStream", key, cancellationToken).ConfigureAwait(false);

            var metadata = new ObjectMetadata
            {
                ContentType = response.Content.Headers.ContentType?.MediaType,
                ContentEncoding = response.Content.Headers.ContentEncoding.FirstOrDefault(),
                Size = response.Content.Headers.ContentLength ?? 0,
                ETag = response.Headers.ETag?.Tag
            };

            if (response.Content.Headers.LastModified.HasValue)
                metadata.LastModified = response.Content.Headers.LastModified.Value.DateTime;

            if (response.Headers.TryGetValues("X-Object-Metadata", out var customValues))
            {
                var customJson = customValues.FirstOrDefault();
                if (!string.IsNullOrEmpty(customJson))
                    metadata.Custom = JsonSerializer.Deserialize<Dictionary<string, string>>(customJson, _jsonOptions);
            }

            var stream = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
            // The wrapper owns the response from here: disposing the returned
            // stream disposes the response too.
            var owned = new ResponseOwningStream(stream, response);
            response = null;
            return (owned, metadata);
        }
        catch (ObjectNotFoundException)
        {
            throw;
        }
        catch (HttpRequestException ex)
        {
            throw new OperationFailedException("GetStream", $"Failed to get object stream with key '{key}'", ex);
        }
        finally
        {
            response?.Dispose();
        }
    }

    public async Task<bool> DeleteAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);

        _logger?.LogDebug("Deleting object with key: {Key}", key);

        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Delete, $"/objects/{Uri.EscapeDataString(key)}");
            ApplyAuthHeaders(request);
            using var response = await _httpClient.SendAsync(request, cancellationToken).ConfigureAwait(false);
            return response.IsSuccessStatusCode;
        }
        catch (HttpRequestException ex)
        {
            throw new OperationFailedException("Delete", $"Failed to delete object with key '{key}'", ex);
        }
    }

    public async Task<ListObjectsResponse> ListAsync(string? prefix = null, string? delimiter = null, int? maxResults = null, string? continueFrom = null, CancellationToken cancellationToken = default)
    {
        _logger?.LogDebug("Listing objects with prefix: {Prefix}", prefix);

        try
        {
            var queryParams = new List<string>();
            if (!string.IsNullOrEmpty(prefix))
                queryParams.Add($"prefix={Uri.EscapeDataString(prefix)}");
            if (!string.IsNullOrEmpty(delimiter))
                queryParams.Add($"delimiter={Uri.EscapeDataString(delimiter)}");
            if (maxResults.HasValue)
                queryParams.Add($"limit={maxResults.Value}");
            if (!string.IsNullOrEmpty(continueFrom))
                queryParams.Add($"token={Uri.EscapeDataString(continueFrom)}");

            var query = queryParams.Count > 0 ? "?" + string.Join("&", queryParams) : "";
            using var request = new HttpRequestMessage(HttpMethod.Get, $"/objects{query}");
            ApplyAuthHeaders(request);
            var response = await _httpClient.SendAsync(request, cancellationToken).ConfigureAwait(false);
            await HttpErrorMapper.EnsureSuccessAsync(response, "List", cancellationToken: cancellationToken).ConfigureAwait(false);

            return await response.Content.ReadFromJsonAsync<ListObjectsResponse>(_jsonOptions, cancellationToken).ConfigureAwait(false)
                ?? new ListObjectsResponse();
        }
        catch (HttpRequestException ex)
        {
            throw new OperationFailedException("List", "Failed to list objects", ex);
        }
    }

    public async Task<bool> ExistsAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);

        _logger?.LogDebug("Checking existence of object with key: {Key}", key);

        try
        {
            var request = new HttpRequestMessage(HttpMethod.Head, $"/objects/{Uri.EscapeDataString(key)}");
            ApplyAuthHeaders(request);
            var response = await _httpClient.SendAsync(request, cancellationToken).ConfigureAwait(false);

            if (response.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                return false;
            }

            await HttpErrorMapper.EnsureSuccessAsync(response, "Exists", key, cancellationToken).ConfigureAwait(false);

            return true;
        }
        catch (HttpRequestException ex)
        {
            throw new OperationFailedException("Exists", $"Failed to check existence of object with key '{key}'", ex);
        }
    }

    public async Task<ObjectMetadata?> GetMetadataAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);

        _logger?.LogDebug("Getting metadata for object with key: {Key}", key);

        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Get, $"/metadata/{Uri.EscapeDataString(key)}");
            ApplyAuthHeaders(request);
            var response = await _httpClient.SendAsync(request, cancellationToken).ConfigureAwait(false);
            if (!response.IsSuccessStatusCode)
                return null;

            var objectResponse = await response.Content.ReadFromJsonAsync<Dictionary<string, JsonElement>>(_jsonOptions, cancellationToken).ConfigureAwait(false);
            if (objectResponse == null)
                return null;

            var metadata = new ObjectMetadata
            {
                Size = objectResponse.TryGetValue("size", out var sizeEl) ? sizeEl.GetInt64() : 0,
                ETag = objectResponse.TryGetValue("etag", out var etagEl) ? etagEl.GetString() : null,
                ContentType = objectResponse.TryGetValue("content_type", out var ctEl) ? ctEl.GetString() : null
            };

            if (objectResponse.TryGetValue("modified", out var modifiedEl) &&
                DateTime.TryParse(modifiedEl.GetString(), out var modified))
            {
                metadata.LastModified = modified;
            }

            // Parse custom metadata if present
            if (objectResponse.TryGetValue("metadata", out var customEl) && customEl.ValueKind == JsonValueKind.Object)
            {
                metadata.Custom = new Dictionary<string, string>();
                foreach (var prop in customEl.EnumerateObject())
                {
                    metadata.Custom[prop.Name] = prop.Value.GetString() ?? string.Empty;
                }
            }

            return metadata;
        }
        catch (HttpRequestException ex)
        {
            throw new OperationFailedException("GetMetadata", $"Failed to get metadata for object with key '{key}'", ex);
        }
    }

    public async Task<bool> UpdateMetadataAsync(string key, ObjectMetadata metadata, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(metadata);

        _logger?.LogDebug("Updating metadata for object with key: {Key}", key);

        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Put, $"/metadata/{Uri.EscapeDataString(key)}")
            {
                Content = JsonContent.Create(metadata, options: _jsonOptions)
            };
            ApplyAuthHeaders(request);
            var response = await _httpClient.SendAsync(request, cancellationToken).ConfigureAwait(false);

            if (response.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                throw new ObjectNotFoundException(key);
            }

            return response.IsSuccessStatusCode;
        }
        catch (ObjectNotFoundException)
        {
            throw;
        }
        catch (HttpRequestException ex)
        {
            throw new OperationFailedException("UpdateMetadata", $"Failed to update metadata for object with key '{key}'", ex);
        }
    }

    public async Task<HealthResponse> HealthAsync(string? service = null, CancellationToken cancellationToken = default)
    {
        _logger?.LogDebug("Checking health");

        try
        {
            var query = !string.IsNullOrEmpty(service) ? $"?service={Uri.EscapeDataString(service)}" : "";
            using var request = new HttpRequestMessage(HttpMethod.Get, $"/health{query}");
            ApplyAuthHeaders(request);
            var response = await _httpClient.SendAsync(request, cancellationToken).ConfigureAwait(false);

            if (!response.IsSuccessStatusCode)
            {
                return new HealthResponse { Status = HealthStatus.NotServing };
            }

            var result = await response.Content.ReadFromJsonAsync<Dictionary<string, string>>(_jsonOptions, cancellationToken).ConfigureAwait(false);
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
        catch (HttpRequestException ex)
        {
            throw new ConnectionException(_httpClient.BaseAddress?.ToString() ?? "unknown", "Failed to check health", ex);
        }
    }

    public async Task<bool> ArchiveAsync(string key, string destinationType, Dictionary<string, string>? destinationSettings = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(destinationType);

        _logger?.LogDebug("Archiving object with key: {Key} to {DestinationType}", key, destinationType);

        try
        {
            var requestBody = new
            {
                key,
                destination_type = destinationType,
                destination_settings = destinationSettings
            };

            using var httpRequest = new HttpRequestMessage(HttpMethod.Post, "/archive")
            {
                Content = JsonContent.Create(requestBody, options: _jsonOptions)
            };
            ApplyAuthHeaders(httpRequest);
            var response = await _httpClient.SendAsync(httpRequest, cancellationToken).ConfigureAwait(false);
            return response.IsSuccessStatusCode;
        }
        catch (HttpRequestException ex)
        {
            throw new OperationFailedException("Archive", $"Failed to archive object with key '{key}'", ex);
        }
    }

    public async Task<bool> AddPolicyAsync(LifecyclePolicy policy, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(policy);

        _logger?.LogDebug("Adding lifecycle policy: {PolicyId}", policy.Id);

        try
        {
            using var httpRequest = new HttpRequestMessage(HttpMethod.Post, "/policies")
            {
                Content = JsonContent.Create(policy, options: _jsonOptions)
            };
            ApplyAuthHeaders(httpRequest);
            var response = await _httpClient.SendAsync(httpRequest, cancellationToken).ConfigureAwait(false);
            return response.IsSuccessStatusCode;
        }
        catch (HttpRequestException ex)
        {
            throw new OperationFailedException("AddPolicy", $"Failed to add lifecycle policy '{policy.Id}'", ex);
        }
    }

    public async Task<bool> RemovePolicyAsync(string id, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(id);

        _logger?.LogDebug("Removing lifecycle policy: {PolicyId}", id);

        try
        {
            using var httpRequest = new HttpRequestMessage(HttpMethod.Delete, $"/policies/{Uri.EscapeDataString(id)}");
            ApplyAuthHeaders(httpRequest);
            var response = await _httpClient.SendAsync(httpRequest, cancellationToken).ConfigureAwait(false);
            return response.IsSuccessStatusCode;
        }
        catch (HttpRequestException ex)
        {
            throw new OperationFailedException("RemovePolicy", $"Failed to remove lifecycle policy '{id}'", ex);
        }
    }

    public async Task<List<LifecyclePolicy>> GetPoliciesAsync(string? prefix = null, CancellationToken cancellationToken = default)
    {
        _logger?.LogDebug("Getting lifecycle policies");

        try
        {
            var query = !string.IsNullOrEmpty(prefix) ? $"?prefix={Uri.EscapeDataString(prefix)}" : "";
            using var httpRequest = new HttpRequestMessage(HttpMethod.Get, $"/policies{query}");
            ApplyAuthHeaders(httpRequest);
            var response = await _httpClient.SendAsync(httpRequest, cancellationToken).ConfigureAwait(false);

            if (!response.IsSuccessStatusCode)
                return new List<LifecyclePolicy>();

            var jsonDoc = await response.Content.ReadFromJsonAsync<JsonElement>(_jsonOptions, cancellationToken).ConfigureAwait(false);

            if (jsonDoc.TryGetProperty("policies", out var policiesEl) && policiesEl.ValueKind == JsonValueKind.Array)
            {
                var policies = new List<LifecyclePolicy>();
                foreach (var policyEl in policiesEl.EnumerateArray())
                {
                    policies.Add(new LifecyclePolicy
                    {
                        Id = policyEl.TryGetProperty("id", out var idEl) ? idEl.GetString() ?? "" : "",
                        Prefix = policyEl.TryGetProperty("prefix", out var prefixEl) ? prefixEl.GetString() ?? "" : "",
                        RetentionSeconds = policyEl.TryGetProperty("retention_seconds", out var retEl) ? retEl.GetInt64() : 0,
                        Action = policyEl.TryGetProperty("action", out var actionEl) ? actionEl.GetString() ?? "" : "",
                        DestinationType = policyEl.TryGetProperty("destination_type", out var destEl) ? destEl.GetString() : null
                    });
                }
                return policies;
            }

            return new List<LifecyclePolicy>();
        }
        catch (HttpRequestException ex)
        {
            throw new OperationFailedException("GetPolicies", "Failed to get lifecycle policies", ex);
        }
    }

    public async Task<(bool Success, int PoliciesCount, int ObjectsProcessed)> ApplyPoliciesAsync(CancellationToken cancellationToken = default)
    {
        _logger?.LogDebug("Applying lifecycle policies");

        try
        {
            using var httpRequest = new HttpRequestMessage(HttpMethod.Post, "/policies/apply");
            ApplyAuthHeaders(httpRequest);
            var response = await _httpClient.SendAsync(httpRequest, cancellationToken).ConfigureAwait(false);
            if (!response.IsSuccessStatusCode)
                return (false, 0, 0);

            var result = await response.Content.ReadFromJsonAsync<Dictionary<string, JsonElement>>(_jsonOptions, cancellationToken).ConfigureAwait(false);
            if (result == null)
                return (false, 0, 0);

            return (
                true,
                result.GetValueOrDefault("policies_count").GetInt32(),
                result.GetValueOrDefault("objects_processed").GetInt32()
            );
        }
        catch (HttpRequestException ex)
        {
            throw new OperationFailedException("ApplyPolicies", "Failed to apply lifecycle policies", ex);
        }
    }

    public async Task<bool> AddReplicationPolicyAsync(ReplicationPolicy policy, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(policy);

        _logger?.LogDebug("Adding replication policy: {PolicyId}", policy.Id);

        try
        {
            using var httpRequest = new HttpRequestMessage(HttpMethod.Post, "/replication/policies")
            {
                Content = JsonContent.Create(policy, options: _jsonOptions)
            };
            ApplyAuthHeaders(httpRequest);
            var response = await _httpClient.SendAsync(httpRequest, cancellationToken).ConfigureAwait(false);
            return response.IsSuccessStatusCode;
        }
        catch (HttpRequestException ex)
        {
            throw new OperationFailedException("AddReplicationPolicy", $"Failed to add replication policy '{policy.Id}'", ex);
        }
    }

    public async Task<bool> RemoveReplicationPolicyAsync(string id, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(id);

        _logger?.LogDebug("Removing replication policy: {PolicyId}", id);

        try
        {
            using var httpRequest = new HttpRequestMessage(HttpMethod.Delete, $"/replication/policies/{Uri.EscapeDataString(id)}");
            ApplyAuthHeaders(httpRequest);
            var response = await _httpClient.SendAsync(httpRequest, cancellationToken).ConfigureAwait(false);
            return response.IsSuccessStatusCode;
        }
        catch (HttpRequestException ex)
        {
            throw new OperationFailedException("RemoveReplicationPolicy", $"Failed to remove replication policy '{id}'", ex);
        }
    }

    public async Task<List<ReplicationPolicy>> GetReplicationPoliciesAsync(CancellationToken cancellationToken = default)
    {
        _logger?.LogDebug("Getting replication policies");

        try
        {
            using var httpRequest = new HttpRequestMessage(HttpMethod.Get, "/replication/policies");
            ApplyAuthHeaders(httpRequest);
            var response = await _httpClient.SendAsync(httpRequest, cancellationToken).ConfigureAwait(false);
            if (!response.IsSuccessStatusCode)
                return new List<ReplicationPolicy>();

            var jsonDoc = await response.Content.ReadFromJsonAsync<JsonElement>(_jsonOptions, cancellationToken).ConfigureAwait(false);

            if (jsonDoc.TryGetProperty("policies", out var policiesEl) && policiesEl.ValueKind == JsonValueKind.Array)
            {
                var policies = new List<ReplicationPolicy>();
                foreach (var policyEl in policiesEl.EnumerateArray())
                {
                    policies.Add(new ReplicationPolicy
                    {
                        Id = policyEl.TryGetProperty("id", out var idEl) ? idEl.GetString() ?? "" : "",
                        SourceBackend = policyEl.TryGetProperty("source_backend", out var srcEl) ? srcEl.GetString() ?? "" : "",
                        DestinationBackend = policyEl.TryGetProperty("destination_backend", out var dstEl) ? dstEl.GetString() ?? "" : "",
                        CheckIntervalSeconds = policyEl.TryGetProperty("check_interval_seconds", out var intEl) ? intEl.GetInt64() : 0,
                        Enabled = policyEl.TryGetProperty("enabled", out var enEl) && enEl.GetBoolean(),
                        ReplicationMode = policyEl.TryGetProperty("replication_mode", out var modeEl)
                            ? (modeEl.GetString()?.ToLowerInvariant() == "opaque" ? ReplicationMode.Opaque : ReplicationMode.Transparent)
                            : ReplicationMode.Transparent
                    });
                }
                return policies;
            }

            return new List<ReplicationPolicy>();
        }
        catch (HttpRequestException ex)
        {
            throw new OperationFailedException("GetReplicationPolicies", "Failed to get replication policies", ex);
        }
    }

    public async Task<ReplicationPolicy?> GetReplicationPolicyAsync(string id, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(id);

        _logger?.LogDebug("Getting replication policy: {PolicyId}", id);

        try
        {
            using var httpRequest = new HttpRequestMessage(HttpMethod.Get, $"/replication/policies/{Uri.EscapeDataString(id)}");
            ApplyAuthHeaders(httpRequest);
            var response = await _httpClient.SendAsync(httpRequest, cancellationToken).ConfigureAwait(false);
            if (response.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                throw new PolicyNotFoundException(id, "replication policy");
            }

            if (!response.IsSuccessStatusCode)
                return null;

            // Server returns the policy directly, not wrapped
            return await response.Content.ReadFromJsonAsync<ReplicationPolicy>(_jsonOptions, cancellationToken).ConfigureAwait(false);
        }
        catch (PolicyNotFoundException)
        {
            throw;
        }
        catch (HttpRequestException ex)
        {
            throw new OperationFailedException("GetReplicationPolicy", $"Failed to get replication policy '{id}'", ex);
        }
    }

    public async Task<TriggerReplicationResult> TriggerReplicationAsync(string? policyId = null, bool parallel = false, int workerCount = 4, CancellationToken cancellationToken = default)
    {
        _logger?.LogDebug("Triggering replication for policy: {PolicyId}", policyId ?? "all");

        try
        {
            var requestBody = new
            {
                policy_id = policyId,
                parallel,
                worker_count = workerCount
            };

            using var httpRequest = new HttpRequestMessage(HttpMethod.Post, "/replication/trigger")
            {
                Content = JsonContent.Create(requestBody, options: _jsonOptions)
            };
            ApplyAuthHeaders(httpRequest);
            var response = await _httpClient.SendAsync(httpRequest, cancellationToken).ConfigureAwait(false);

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
        catch (HttpRequestException ex)
        {
            throw new OperationFailedException("TriggerReplication", $"Failed to trigger replication for policy '{policyId ?? "all"}'", ex);
        }
    }

    /// <summary>
    /// Parses a Go duration string (e.g. "5.2s", "300ms", "1m30s") into milliseconds.
    /// Returns 0 if the input is null or cannot be parsed.
    /// </summary>
    private static long ParseGoDurationToMs(string? duration)
    {
        if (string.IsNullOrEmpty(duration))
            return 0;

        // Go duration strings are composed of decimal numbers with a unit suffix.
        // Supported units: ns, us (or µs), ms, s, m, h.
        // Strategy: accumulate total nanoseconds, then convert to ms.
        double totalNs = 0;
        var s = duration.AsSpan();

        while (!s.IsEmpty)
        {
            // Read the numeric part (digits and optional decimal point)
            int numEnd = 0;
            while (numEnd < s.Length && (char.IsDigit(s[numEnd]) || s[numEnd] == '.'))
                numEnd++;

            if (numEnd == 0)
                return 0;

            if (!double.TryParse(s[..numEnd], System.Globalization.NumberStyles.Float,
                    System.Globalization.CultureInfo.InvariantCulture, out var value))
                return 0;

            s = s[numEnd..];

            // Read the unit suffix
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

        _logger?.LogDebug("Getting replication status for policy: {PolicyId}", id);

        try
        {
            using var httpRequest = new HttpRequestMessage(HttpMethod.Get, $"/replication/status/{Uri.EscapeDataString(id)}");
            ApplyAuthHeaders(httpRequest);
            var response = await _httpClient.SendAsync(httpRequest, cancellationToken).ConfigureAwait(false);
            if (!response.IsSuccessStatusCode)
                return null;

            // Server returns the status directly, not wrapped
            return await response.Content.ReadFromJsonAsync<ReplicationStatus>(_jsonOptions, cancellationToken).ConfigureAwait(false);
        }
        catch (HttpRequestException ex)
        {
            throw new OperationFailedException("GetReplicationStatus", $"Failed to get replication status for policy '{id}'", ex);
        }
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
