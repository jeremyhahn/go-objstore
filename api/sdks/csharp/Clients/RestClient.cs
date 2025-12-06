using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using ObjStore.SDK.Exceptions;
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
    private bool _disposed;

    /// <summary>
    /// Creates a new RestClient with a new HttpClient instance (for backward compatibility)
    /// Note: When using this constructor, the HttpClient will be disposed when this client is disposed.
    /// For production use, prefer using IHttpClientFactory via dependency injection.
    /// </summary>
    /// <param name="baseUrl">The base URL of the object store service</param>
    /// <param name="logger">Optional logger instance</param>
    public RestClient(string baseUrl, ILogger<RestClient>? logger = null)
        : this(new HttpClient { BaseAddress = new Uri(baseUrl) }, logger, disposeHttpClient: true)
    {
    }

    /// <summary>
    /// Creates a new RestClient with an injected HttpClient (recommended for production use)
    /// When using IHttpClientFactory, the HttpClient should NOT be disposed by this class.
    /// </summary>
    /// <param name="httpClient">The HttpClient instance to use</param>
    /// <param name="logger">Optional logger instance</param>
    public RestClient(HttpClient httpClient, ILogger<RestClient>? logger = null)
        : this(httpClient, logger, disposeHttpClient: false)
    {
    }

    private RestClient(HttpClient httpClient, ILogger<RestClient>? logger, bool disposeHttpClient)
    {
        _httpClient = httpClient ?? throw new ArgumentNullException(nameof(httpClient));
        _logger = logger;
        _disposeHttpClient = disposeHttpClient;
        _jsonOptions = new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true
        };
    }

    public async Task<string?> PutAsync(string key, byte[] data, ObjectMetadata? metadata = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(data);

        _logger?.LogDebug("Putting object with key: {Key}, size: {Size}", key, data.Length);

        try
        {
            using var content = new MultipartFormDataContent();
            content.Add(new ByteArrayContent(data), "file", "file");

            if (metadata != null)
            {
                var metadataJson = JsonSerializer.Serialize(metadata, _jsonOptions);
                content.Add(new StringContent(metadataJson, Encoding.UTF8, "application/json"), "metadata");
            }

            var response = await _httpClient.PutAsync($"/objects/{Uri.EscapeDataString(key)}", content, cancellationToken).ConfigureAwait(false);
            response.EnsureSuccessStatusCode();

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

    public async Task<(byte[] Data, ObjectMetadata? Metadata)> GetAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);

        _logger?.LogDebug("Getting object with key: {Key}", key);

        try
        {
            var response = await _httpClient.GetAsync($"/objects/{Uri.EscapeDataString(key)}", cancellationToken).ConfigureAwait(false);

            if (response.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                throw new ObjectNotFoundException(key);
            }

            response.EnsureSuccessStatusCode();

            var data = await response.Content.ReadAsByteArrayAsync(cancellationToken).ConfigureAwait(false);

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

    public async Task<bool> DeleteAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);

        _logger?.LogDebug("Deleting object with key: {Key}", key);

        try
        {
            var response = await _httpClient.DeleteAsync($"/objects/{Uri.EscapeDataString(key)}", cancellationToken).ConfigureAwait(false);
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
            var response = await _httpClient.GetAsync($"/objects{query}", cancellationToken).ConfigureAwait(false);
            response.EnsureSuccessStatusCode();

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
            var response = await _httpClient.SendAsync(request, cancellationToken).ConfigureAwait(false);
            return response.IsSuccessStatusCode;
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
            var response = await _httpClient.GetAsync($"/metadata/{Uri.EscapeDataString(key)}", cancellationToken).ConfigureAwait(false);
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
            var response = await _httpClient.PutAsJsonAsync($"/metadata/{Uri.EscapeDataString(key)}", metadata, _jsonOptions, cancellationToken).ConfigureAwait(false);
            return response.IsSuccessStatusCode;
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
            var response = await _httpClient.GetAsync($"/health{query}", cancellationToken).ConfigureAwait(false);

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
            var request = new
            {
                key,
                destination_type = destinationType,
                destination_settings = destinationSettings
            };

            var response = await _httpClient.PostAsJsonAsync("/archive", request, _jsonOptions, cancellationToken).ConfigureAwait(false);
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
            var response = await _httpClient.PostAsJsonAsync("/policies", policy, _jsonOptions, cancellationToken).ConfigureAwait(false);
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
            var response = await _httpClient.DeleteAsync($"/policies/{Uri.EscapeDataString(id)}", cancellationToken).ConfigureAwait(false);
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
            var response = await _httpClient.GetAsync($"/policies{query}", cancellationToken).ConfigureAwait(false);

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
            var response = await _httpClient.PostAsync("/policies/apply", null, cancellationToken).ConfigureAwait(false);
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
            var response = await _httpClient.PostAsJsonAsync("/replication/policies", policy, _jsonOptions, cancellationToken).ConfigureAwait(false);
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
            var response = await _httpClient.DeleteAsync($"/replication/policies/{Uri.EscapeDataString(id)}", cancellationToken).ConfigureAwait(false);
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
            var response = await _httpClient.GetAsync("/replication/policies", cancellationToken).ConfigureAwait(false);
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
            var response = await _httpClient.GetAsync($"/replication/policies/{Uri.EscapeDataString(id)}", cancellationToken).ConfigureAwait(false);
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

    public async Task<bool> TriggerReplicationAsync(string? policyId = null, bool parallel = false, int workerCount = 4, CancellationToken cancellationToken = default)
    {
        _logger?.LogDebug("Triggering replication for policy: {PolicyId}", policyId ?? "all");

        try
        {
            var request = new
            {
                policy_id = policyId,
                parallel,
                worker_count = workerCount
            };

            var response = await _httpClient.PostAsJsonAsync("/replication/trigger", request, _jsonOptions, cancellationToken).ConfigureAwait(false);
            return response.IsSuccessStatusCode;
        }
        catch (HttpRequestException ex)
        {
            throw new OperationFailedException("TriggerReplication", $"Failed to trigger replication for policy '{policyId ?? "all"}'", ex);
        }
    }

    public async Task<ReplicationStatus?> GetReplicationStatusAsync(string id, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(id);

        _logger?.LogDebug("Getting replication status for policy: {PolicyId}", id);

        try
        {
            var response = await _httpClient.GetAsync($"/replication/status/{Uri.EscapeDataString(id)}", cancellationToken).ConfigureAwait(false);
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
