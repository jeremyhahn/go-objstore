using System.Net;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using ObjStore.SDK.Exceptions;
using ObjStore.SDK.Models;
using Microsoft.Extensions.Logging;

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
    private bool _disposed;

    /// <summary>
    /// Creates a new QuicClient with a new HttpClient instance (for backward compatibility)
    /// Note: When using this constructor, the HttpClient will be disposed when this client is disposed.
    /// For production use, prefer using IHttpClientFactory via dependency injection.
    /// </summary>
    /// <param name="baseUrl">The base URL of the object store service</param>
    /// <param name="logger">Optional logger instance</param>
    public QuicClient(string baseUrl, ILogger<QuicClient>? logger = null)
        : this(CreateHttpClient(baseUrl), logger, disposeHttpClient: true)
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
        : this(httpClient, logger, disposeHttpClient: false)
    {
    }

    private QuicClient(HttpClient httpClient, ILogger<QuicClient>? logger, bool disposeHttpClient)
    {
        _httpClient = httpClient ?? throw new ArgumentNullException(nameof(httpClient));
        _logger = logger;
        _disposeHttpClient = disposeHttpClient;
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
            using var content = new MultipartFormDataContent();
            content.Add(new ByteArrayContent(data), "file", "file");

            if (metadata != null)
            {
                var metadataJson = JsonSerializer.Serialize(metadata, _jsonOptions);
                content.Add(new StringContent(metadataJson, Encoding.UTF8, "application/json"), "metadata");
            }

            var request = new HttpRequestMessage(HttpMethod.Put, $"/objects/{Uri.EscapeDataString(key)}")
            {
                Content = content,
                Version = HttpVersion.Version30,
                VersionPolicy = HttpVersionPolicy.RequestVersionOrHigher
            };

            var response = await _httpClient.SendAsync(request, cancellationToken).ConfigureAwait(false);
            response.EnsureSuccessStatusCode();

            _logger?.LogDebug("PUT request used HTTP version: {Version}", response.Version);

            var result = await response.Content.ReadFromJsonAsync<Dictionary<string, JsonElement>>(_jsonOptions, cancellationToken).ConfigureAwait(false);
            return result?.GetValueOrDefault("data").GetProperty("etag").GetString();
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

    public async Task<(byte[] Data, ObjectMetadata? Metadata)> GetAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);

        _logger?.LogDebug("Getting object with key: {Key} via HTTP/3", key);

        var request = new HttpRequestMessage(HttpMethod.Get, $"/objects/{Uri.EscapeDataString(key)}")
        {
            Version = HttpVersion.Version30,
            VersionPolicy = HttpVersionPolicy.RequestVersionOrHigher
        };

        var response = await _httpClient.SendAsync(request, cancellationToken);
        response.EnsureSuccessStatusCode();

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

        return (data, metadata);
    }

    public async Task<bool> DeleteAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);

        _logger?.LogDebug("Deleting object with key: {Key} via HTTP/3", key);

        var request = new HttpRequestMessage(HttpMethod.Delete, $"/objects/{Uri.EscapeDataString(key)}")
        {
            Version = HttpVersion.Version30,
            VersionPolicy = HttpVersionPolicy.RequestVersionOrHigher
        };

        var response = await _httpClient.SendAsync(request, cancellationToken);
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
            queryParams.Add($"limit={maxResults.Value}");
        if (!string.IsNullOrEmpty(continueFrom))
            queryParams.Add($"token={Uri.EscapeDataString(continueFrom)}");

        var query = queryParams.Count > 0 ? "?" + string.Join("&", queryParams) : "";

        var request = new HttpRequestMessage(HttpMethod.Get, $"/objects{query}")
        {
            Version = HttpVersion.Version30,
            VersionPolicy = HttpVersionPolicy.RequestVersionOrHigher
        };

        var response = await _httpClient.SendAsync(request, cancellationToken);
        response.EnsureSuccessStatusCode();

        _logger?.LogDebug("LIST request used HTTP version: {Version}", response.Version);

        return await response.Content.ReadFromJsonAsync<ListObjectsResponse>(_jsonOptions, cancellationToken)
            ?? new ListObjectsResponse();
    }

    public async Task<bool> ExistsAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);

        _logger?.LogDebug("Checking existence of object with key: {Key} via HTTP/3", key);

        var request = new HttpRequestMessage(HttpMethod.Head, $"/objects/{Uri.EscapeDataString(key)}")
        {
            Version = HttpVersion.Version30,
            VersionPolicy = HttpVersionPolicy.RequestVersionOrHigher
        };

        var response = await _httpClient.SendAsync(request, cancellationToken);
        _logger?.LogDebug("HEAD request used HTTP version: {Version}", response.Version);

        return response.IsSuccessStatusCode;
    }

    public async Task<ObjectMetadata?> GetMetadataAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);

        _logger?.LogDebug("Getting metadata for object with key: {Key} via HTTP/3", key);

        var request = new HttpRequestMessage(HttpMethod.Get, $"/objects/{Uri.EscapeDataString(key)}/metadata")
        {
            Version = HttpVersion.Version30,
            VersionPolicy = HttpVersionPolicy.RequestVersionOrHigher
        };

        var response = await _httpClient.SendAsync(request, cancellationToken);
        if (!response.IsSuccessStatusCode)
            return null;

        var objectResponse = await response.Content.ReadFromJsonAsync<Dictionary<string, JsonElement>>(_jsonOptions, cancellationToken);
        if (objectResponse == null)
            return null;

        return new ObjectMetadata
        {
            ContentType = objectResponse.GetValueOrDefault("metadata")
                .GetProperty("content_type").GetString(),
            Size = objectResponse.GetValueOrDefault("size").GetInt64(),
            ETag = objectResponse.GetValueOrDefault("etag").GetString(),
            LastModified = DateTime.TryParse(
                objectResponse.GetValueOrDefault("modified").GetString(),
                out var modified) ? modified : null
        };
    }

    public async Task<bool> UpdateMetadataAsync(string key, ObjectMetadata metadata, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(metadata);

        _logger?.LogDebug("Updating metadata for object with key: {Key} via HTTP/3", key);

        var request = new HttpRequestMessage(HttpMethod.Put, $"/objects/{Uri.EscapeDataString(key)}/metadata")
        {
            Content = JsonContent.Create(metadata, options: _jsonOptions),
            Version = HttpVersion.Version30,
            VersionPolicy = HttpVersionPolicy.RequestVersionOrHigher
        };

        var response = await _httpClient.SendAsync(request, cancellationToken);
        return response.IsSuccessStatusCode;
    }

    public async Task<HealthResponse> HealthAsync(string? service = null, CancellationToken cancellationToken = default)
    {
        _logger?.LogDebug("Checking health via HTTP/3");

        var query = !string.IsNullOrEmpty(service) ? $"?service={Uri.EscapeDataString(service)}" : "";

        var request = new HttpRequestMessage(HttpMethod.Get, $"/health{query}")
        {
            Version = HttpVersion.Version30,
            VersionPolicy = HttpVersionPolicy.RequestVersionOrHigher
        };

        var response = await _httpClient.SendAsync(request, cancellationToken);

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

        var request = new HttpRequestMessage(HttpMethod.Post, "/archive")
        {
            Content = JsonContent.Create(requestData, options: _jsonOptions),
            Version = HttpVersion.Version30,
            VersionPolicy = HttpVersionPolicy.RequestVersionOrHigher
        };

        var response = await _httpClient.SendAsync(request, cancellationToken);
        return response.IsSuccessStatusCode;
    }

    public async Task<bool> AddPolicyAsync(LifecyclePolicy policy, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(policy);

        _logger?.LogDebug("Adding lifecycle policy: {PolicyId} via HTTP/3", policy.Id);

        var request = new HttpRequestMessage(HttpMethod.Post, "/policies")
        {
            Content = JsonContent.Create(policy, options: _jsonOptions),
            Version = HttpVersion.Version30,
            VersionPolicy = HttpVersionPolicy.RequestVersionOrHigher
        };

        var response = await _httpClient.SendAsync(request, cancellationToken);
        return response.IsSuccessStatusCode;
    }

    public async Task<bool> RemovePolicyAsync(string id, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(id);

        _logger?.LogDebug("Removing lifecycle policy: {PolicyId} via HTTP/3", id);

        var request = new HttpRequestMessage(HttpMethod.Delete, $"/policies/{Uri.EscapeDataString(id)}")
        {
            Version = HttpVersion.Version30,
            VersionPolicy = HttpVersionPolicy.RequestVersionOrHigher
        };

        var response = await _httpClient.SendAsync(request, cancellationToken);
        return response.IsSuccessStatusCode;
    }

    public async Task<List<LifecyclePolicy>> GetPoliciesAsync(string? prefix = null, CancellationToken cancellationToken = default)
    {
        _logger?.LogDebug("Getting lifecycle policies via HTTP/3");

        var query = !string.IsNullOrEmpty(prefix) ? $"?prefix={Uri.EscapeDataString(prefix)}" : "";

        var request = new HttpRequestMessage(HttpMethod.Get, $"/policies{query}")
        {
            Version = HttpVersion.Version30,
            VersionPolicy = HttpVersionPolicy.RequestVersionOrHigher
        };

        var response = await _httpClient.SendAsync(request, cancellationToken);

        if (!response.IsSuccessStatusCode)
            return new List<LifecyclePolicy>();

        var result = await response.Content.ReadFromJsonAsync<Dictionary<string, List<LifecyclePolicy>>>(_jsonOptions, cancellationToken);
        return result?.GetValueOrDefault("policies") ?? new List<LifecyclePolicy>();
    }

    public async Task<(bool Success, int PoliciesCount, int ObjectsProcessed)> ApplyPoliciesAsync(CancellationToken cancellationToken = default)
    {
        _logger?.LogDebug("Applying lifecycle policies via HTTP/3");

        var request = new HttpRequestMessage(HttpMethod.Post, "/policies/apply")
        {
            Version = HttpVersion.Version30,
            VersionPolicy = HttpVersionPolicy.RequestVersionOrHigher
        };

        var response = await _httpClient.SendAsync(request, cancellationToken);
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

        var request = new HttpRequestMessage(HttpMethod.Post, "/replication/policies")
        {
            Content = JsonContent.Create(policy, options: _jsonOptions),
            Version = HttpVersion.Version30,
            VersionPolicy = HttpVersionPolicy.RequestVersionOrHigher
        };

        var response = await _httpClient.SendAsync(request, cancellationToken);
        return response.IsSuccessStatusCode;
    }

    public async Task<bool> RemoveReplicationPolicyAsync(string id, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(id);

        _logger?.LogDebug("Removing replication policy: {PolicyId} via HTTP/3", id);

        var request = new HttpRequestMessage(HttpMethod.Delete, $"/replication/policies/{Uri.EscapeDataString(id)}")
        {
            Version = HttpVersion.Version30,
            VersionPolicy = HttpVersionPolicy.RequestVersionOrHigher
        };

        var response = await _httpClient.SendAsync(request, cancellationToken);
        return response.IsSuccessStatusCode;
    }

    public async Task<List<ReplicationPolicy>> GetReplicationPoliciesAsync(CancellationToken cancellationToken = default)
    {
        _logger?.LogDebug("Getting replication policies via HTTP/3");

        var request = new HttpRequestMessage(HttpMethod.Get, "/replication/policies")
        {
            Version = HttpVersion.Version30,
            VersionPolicy = HttpVersionPolicy.RequestVersionOrHigher
        };

        var response = await _httpClient.SendAsync(request, cancellationToken);
        if (!response.IsSuccessStatusCode)
            return new List<ReplicationPolicy>();

        var result = await response.Content.ReadFromJsonAsync<Dictionary<string, List<ReplicationPolicy>>>(_jsonOptions, cancellationToken);
        return result?.GetValueOrDefault("policies") ?? new List<ReplicationPolicy>();
    }

    public async Task<ReplicationPolicy?> GetReplicationPolicyAsync(string id, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(id);

        _logger?.LogDebug("Getting replication policy: {PolicyId} via HTTP/3", id);

        var request = new HttpRequestMessage(HttpMethod.Get, $"/replication/policies/{Uri.EscapeDataString(id)}")
        {
            Version = HttpVersion.Version30,
            VersionPolicy = HttpVersionPolicy.RequestVersionOrHigher
        };

        var response = await _httpClient.SendAsync(request, cancellationToken);
        if (!response.IsSuccessStatusCode)
            return null;

        var result = await response.Content.ReadFromJsonAsync<Dictionary<string, ReplicationPolicy>>(_jsonOptions, cancellationToken);
        return result?.GetValueOrDefault("policy");
    }

    public async Task<bool> TriggerReplicationAsync(string? policyId = null, bool parallel = false, int workerCount = 4, CancellationToken cancellationToken = default)
    {
        _logger?.LogDebug("Triggering replication for policy: {PolicyId} via HTTP/3", policyId ?? "all");

        var requestData = new
        {
            policy_id = policyId,
            parallel,
            worker_count = workerCount
        };

        var request = new HttpRequestMessage(HttpMethod.Post, "/replication/trigger")
        {
            Content = JsonContent.Create(requestData, options: _jsonOptions),
            Version = HttpVersion.Version30,
            VersionPolicy = HttpVersionPolicy.RequestVersionOrHigher
        };

        var response = await _httpClient.SendAsync(request, cancellationToken);
        return response.IsSuccessStatusCode;
    }

    public async Task<ReplicationStatus?> GetReplicationStatusAsync(string id, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(id);

        _logger?.LogDebug("Getting replication status for policy: {PolicyId} via HTTP/3", id);

        var request = new HttpRequestMessage(HttpMethod.Get, $"/replication/status/{Uri.EscapeDataString(id)}")
        {
            Version = HttpVersion.Version30,
            VersionPolicy = HttpVersionPolicy.RequestVersionOrHigher
        };

        var response = await _httpClient.SendAsync(request, cancellationToken);
        if (!response.IsSuccessStatusCode)
            return null;

        var result = await response.Content.ReadFromJsonAsync<Dictionary<string, ReplicationStatus>>(_jsonOptions, cancellationToken);
        return result?.GetValueOrDefault("status");
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
