using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Grpc.Net.Client;
using ObjStore.SDK.Exceptions;
using ObjStore.SDK.Models;
using Microsoft.Extensions.Logging;
using Objstore.V1;

namespace ObjStore.SDK.Clients;

/// <summary>
/// gRPC client implementation for go-objstore
/// </summary>
public class GrpcClient : IObjectStoreClient
{
    private readonly GrpcChannel _channel;
    private readonly ObjectStore.ObjectStoreClient _client;
    private readonly ILogger<GrpcClient>? _logger;
    private readonly string? _token;
    private readonly IDictionary<string, string>? _extraHeaders;
    private readonly string? _tenantId;
    private bool _disposed;

    /// <summary>
    /// Creates a gRPC client using the supplied address with optional TLS configuration and auth credentials.
    /// </summary>
    /// <param name="address">Server address, e.g. "https://localhost:9090"</param>
    /// <param name="logger">Optional logger</param>
    /// <param name="allowInsecureTls">When true, skips TLS certificate validation (development only)</param>
    /// <param name="caCertPath">Path to a PEM CA certificate for custom TLS verification</param>
    /// <param name="token">Optional bearer token sent as gRPC authorization metadata</param>
    /// <param name="headers">Optional additional metadata entries sent with every call</param>
    /// <param name="tenantId">Optional tenant ID sent as x-tenant-id metadata</param>
    public GrpcClient(
        string address,
        ILogger<GrpcClient>? logger = null,
        bool allowInsecureTls = false,
        string? caCertPath = null,
        string? token = null,
        IDictionary<string, string>? headers = null,
        string? tenantId = null)
    {
        _logger = logger;
        _token = token;
        _extraHeaders = headers;
        _tenantId = tenantId;

        var channelOptions = BuildChannelOptions(allowInsecureTls, caCertPath);
        _channel = GrpcChannel.ForAddress(address, channelOptions);
        _client = new ObjectStore.ObjectStoreClient(_channel);
    }

    public GrpcClient(GrpcChannel channel, ILogger<GrpcClient>? logger = null)
    {
        _channel = channel ?? throw new ArgumentNullException(nameof(channel));
        _client = new ObjectStore.ObjectStoreClient(_channel);
        _logger = logger;
    }

    /// <summary>
    /// Builds GrpcChannelOptions with the requested TLS configuration.
    /// </summary>
    private static GrpcChannelOptions BuildChannelOptions(bool allowInsecureTls, string? caCertPath)
    {
        if (!allowInsecureTls && string.IsNullOrEmpty(caCertPath))
            return new GrpcChannelOptions();

        var handler = new System.Net.Http.HttpClientHandler();
        try
        {
            if (allowInsecureTls)
            {
                handler.ServerCertificateCustomValidationCallback =
                    HttpClientHandler.DangerousAcceptAnyServerCertificateValidator;
            }
            else if (!string.IsNullOrEmpty(caCertPath))
            {
                var caCert = new X509Certificate2(caCertPath);
                handler.ServerCertificateCustomValidationCallback = (_, cert, chain, errors) =>
                {
                    if (errors == SslPolicyErrors.None)
                        return true;

                    // Accept when the cert chains to our trusted CA.
                    chain!.ChainPolicy.ExtraStore.Add(caCert);
                    chain.ChainPolicy.VerificationFlags = X509VerificationFlags.AllowUnknownCertificateAuthority;
                    return chain.Build(cert!);
                };
            }
        }
        catch
        {
            // The handler is only owned by a channel after this method returns;
            // dispose it when configuration throws (e.g. unreadable CA cert).
            handler.Dispose();
            throw;
        }

        return new GrpcChannelOptions
        {
            HttpHandler = handler
        };
    }

    /// <summary>
    /// Builds per-call metadata for auth headers.
    /// Returns null when no auth is configured (avoids allocating an empty Metadata).
    /// </summary>
    private Grpc.Core.Metadata? BuildCallMetadata()
    {
        if (string.IsNullOrEmpty(_token) && string.IsNullOrEmpty(_tenantId) &&
            (_extraHeaders == null || _extraHeaders.Count == 0))
            return null;

        var metadata = new Grpc.Core.Metadata();

        if (!string.IsNullOrEmpty(_token))
            metadata.Add("authorization", $"Bearer {_token}");

        if (!string.IsNullOrEmpty(_tenantId))
            metadata.Add("x-tenant-id", _tenantId);

        if (_extraHeaders != null)
        {
            foreach (var kvp in _extraHeaders)
                metadata.Add(kvp.Key.ToLowerInvariant(), kvp.Value);
        }

        return metadata;
    }

    /// <summary>
    /// Builds CallOptions carrying the auth metadata when configured.
    /// </summary>
    private CallOptions CallOptions(CancellationToken cancellationToken = default)
    {
        var meta = BuildCallMetadata();
        return meta != null
            ? new CallOptions(headers: meta, cancellationToken: cancellationToken)
            : new CallOptions(cancellationToken: cancellationToken);
    }

    /// <summary>
    /// Returns true when the gRPC status code has a canonical SDK exception mapping
    /// (validation, auth, already-exists, rate-limit). NotFound is handled per call site.
    /// </summary>
    private static bool HasCanonicalMapping(Grpc.Core.StatusCode statusCode) => statusCode is
        Grpc.Core.StatusCode.InvalidArgument or
        Grpc.Core.StatusCode.Unauthenticated or
        Grpc.Core.StatusCode.PermissionDenied or
        Grpc.Core.StatusCode.AlreadyExists or
        Grpc.Core.StatusCode.ResourceExhausted;

    /// <summary>
    /// Maps a gRPC status code to the canonical SDK exception:
    /// InvalidArgument→ValidationException, Unauthenticated→AuthenticationException,
    /// PermissionDenied→AuthorizationException, AlreadyExists→AlreadyExistsException,
    /// ResourceExhausted→RateLimitException. Classification is by status code,
    /// never by message substring.
    /// </summary>
    private static ObjectStoreException MapRpcException(Grpc.Core.RpcException ex, string operation)
    {
        var message = $"{operation} failed ({ex.StatusCode}): {ex.Status.Detail}";
        return ex.StatusCode switch
        {
            Grpc.Core.StatusCode.InvalidArgument => new ValidationException(message, ex),
            Grpc.Core.StatusCode.Unauthenticated => new AuthenticationException(message, ex),
            Grpc.Core.StatusCode.PermissionDenied => new AuthorizationException(message, ex),
            Grpc.Core.StatusCode.AlreadyExists => new AlreadyExistsException(message, ex),
            Grpc.Core.StatusCode.ResourceExhausted => new RateLimitException(message, ex),
            _ => new OperationFailedException(operation, message, ex)
        };
    }

    public async Task<string?> PutAsync(string key, byte[] data, ObjectMetadata? metadata = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(data);

        _logger?.LogDebug("Putting object with key: {Key}, size: {Size}", key, data.Length);

        try
        {
            var request = new PutRequest
            {
                Key = key,
                // ByteString.CopyFrom handles empty arrays correctly
                Data = data.Length > 0 ? ByteString.CopyFrom(data) : ByteString.Empty
            };

            if (metadata != null)
            {
                request.Metadata = ConvertToProtoMetadata(metadata);
            }

            var response = await _client.PutAsync(request, CallOptions(cancellationToken)).ConfigureAwait(false);
            return response.Success ? response.Etag : null;
        }
        catch (Grpc.Core.RpcException ex) when (ex.StatusCode == Grpc.Core.StatusCode.NotFound)
        {
            throw new ObjectNotFoundException(key);
        }
        catch (Grpc.Core.RpcException ex) when (HasCanonicalMapping(ex.StatusCode))
        {
            throw MapRpcException(ex, "Put");
        }
        catch (Grpc.Core.RpcException ex)
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

        // Buffer the stream into a byte array and delegate to PutAsync.
        // True client-streaming gRPC upload can be added when the proto adds a streaming Put RPC.
        _logger?.LogDebug("Putting object stream with key: {Key}", key);
        using var ms = new MemoryStream();
        await data.CopyToAsync(ms, cancellationToken).ConfigureAwait(false);
        return await PutAsync(key, ms.ToArray(), metadata, cancellationToken).ConfigureAwait(false);
    }

    public async Task<(byte[] Data, ObjectMetadata? Metadata)> GetAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);

        _logger?.LogDebug("Getting object with key: {Key}", key);

        try
        {
            var request = new GetRequest { Key = key };
            using var call = _client.Get(request, CallOptions(cancellationToken));

            var chunks = new List<byte[]>();
            Objstore.V1.Metadata? protoMetadata = null;

            while (await call.ResponseStream.MoveNext(cancellationToken))
            {
                var response = call.ResponseStream.Current;
                if (response.Data.Length > 0)
                {
                    chunks.Add(response.Data.ToByteArray());
                }

                if (response.Metadata != null && protoMetadata == null)
                {
                    protoMetadata = response.Metadata;
                }
            }

            var data = chunks.SelectMany(x => x).ToArray();
            var objectMetadata = protoMetadata != null ? ConvertFromProtoMetadata(protoMetadata) : null;

            return (data, objectMetadata);
        }
        catch (Grpc.Core.RpcException ex) when (HasCanonicalMapping(ex.StatusCode))
        {
            // Explicit status codes win over the detail-substring not-found heuristic below.
            throw MapRpcException(ex, "Get");
        }
        catch (Grpc.Core.RpcException ex) when (ex.StatusCode == Grpc.Core.StatusCode.NotFound ||
            ex.Status.Detail.Contains("no such file") ||
            ex.Status.Detail.Contains("not found", StringComparison.OrdinalIgnoreCase))
        {
            throw new ObjectNotFoundException(key);
        }
    }

    public async Task<(Stream Data, ObjectMetadata? Metadata)> GetStreamAsync(string key, CancellationToken cancellationToken = default)
    {
        // Collect all server-streaming chunks then expose the result as a MemoryStream.
        // True incremental streaming is available via the gRPC call directly when needed.
        var (data, metadata) = await GetAsync(key, cancellationToken).ConfigureAwait(false);
        return (new MemoryStream(data), metadata);
    }

    public async Task<bool> DeleteAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);

        _logger?.LogDebug("Deleting object with key: {Key}", key);

        try
        {
            var request = new DeleteRequest { Key = key };
            var response = await _client.DeleteAsync(request, CallOptions(cancellationToken));
            return response.Success;
        }
        catch (Grpc.Core.RpcException ex) when (HasCanonicalMapping(ex.StatusCode))
        {
            // Explicit status codes win over the detail-substring not-found heuristic below.
            throw MapRpcException(ex, "Delete");
        }
        catch (Grpc.Core.RpcException ex) when (ex.StatusCode == Grpc.Core.StatusCode.NotFound ||
            ex.Status.Detail.Contains("no such file") ||
            ex.Status.Detail.Contains("not found", StringComparison.OrdinalIgnoreCase))
        {
            _logger?.LogDebug("Object not found for deletion: {Key}", key);
            return false;
        }
        catch (Grpc.Core.RpcException ex)
        {
            throw new OperationFailedException("Delete", $"Failed to delete object with key '{key}'", ex);
        }
    }

    public async Task<ListObjectsResponse> ListAsync(string? prefix = null, string? delimiter = null, int? maxResults = null, string? continueFrom = null, CancellationToken cancellationToken = default)
    {
        _logger?.LogDebug("Listing objects with prefix: {Prefix}", prefix);

        var request = new ListRequest
        {
            Prefix = prefix ?? string.Empty,
            Delimiter = delimiter ?? string.Empty,
            MaxResults = maxResults ?? 100,
            ContinueFrom = continueFrom ?? string.Empty
        };

        var response = await _client.ListAsync(request, CallOptions(cancellationToken));

        return new ListObjectsResponse
        {
            Objects = response.Objects.Select(obj => new Models.ObjectInfo
            {
                Key = obj.Key,
                Metadata = obj.Metadata != null ? ConvertFromProtoMetadata(obj.Metadata) : null
            }).ToList(),
            CommonPrefixes = response.CommonPrefixes.ToList(),
            NextToken = response.NextToken,
            Truncated = response.Truncated
        };
    }

    public async Task<bool> ExistsAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);

        _logger?.LogDebug("Checking existence of object with key: {Key}", key);

        var request = new ExistsRequest { Key = key };
        var response = await _client.ExistsAsync(request, CallOptions(cancellationToken));
        return response.Exists;
    }

    public async Task<ObjectMetadata?> GetMetadataAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);

        _logger?.LogDebug("Getting metadata for object with key: {Key}", key);

        var request = new GetMetadataRequest { Key = key };
        var response = await _client.GetMetadataAsync(request, CallOptions(cancellationToken));

        return response.Success && response.Metadata != null
            ? ConvertFromProtoMetadata(response.Metadata)
            : null;
    }

    public async Task<bool> UpdateMetadataAsync(string key, ObjectMetadata metadata, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(metadata);

        _logger?.LogDebug("Updating metadata for object with key: {Key}", key);

        try
        {
            var request = new UpdateMetadataRequest
            {
                Key = key,
                Metadata = ConvertToProtoMetadata(metadata)
            };

            var response = await _client.UpdateMetadataAsync(request, CallOptions(cancellationToken));

            if (!response.Success)
            {
                throw new ObjectNotFoundException(key);
            }

            return true;
        }
        catch (ObjectNotFoundException)
        {
            throw;
        }
        catch (Grpc.Core.RpcException ex) when (HasCanonicalMapping(ex.StatusCode))
        {
            // Explicit status codes win over the detail-substring not-found heuristic below.
            throw MapRpcException(ex, "UpdateMetadata");
        }
        catch (Grpc.Core.RpcException ex) when (ex.StatusCode == Grpc.Core.StatusCode.NotFound ||
            ex.Status.Detail.Contains("no such file") ||
            ex.Status.Detail.Contains("not found", StringComparison.OrdinalIgnoreCase))
        {
            throw new ObjectNotFoundException(key);
        }
    }

    public async Task<Models.HealthResponse> HealthAsync(string? service = null, CancellationToken cancellationToken = default)
    {
        _logger?.LogDebug("Checking health");

        var request = new HealthRequest { Service = service ?? string.Empty };
        var response = await _client.HealthAsync(request, CallOptions(cancellationToken));

        return new Models.HealthResponse
        {
            Status = response.Status switch
            {
                Objstore.V1.HealthResponse.Types.Status.Serving => HealthStatus.Serving,
                Objstore.V1.HealthResponse.Types.Status.NotServing => HealthStatus.NotServing,
                _ => HealthStatus.Unknown
            },
            Message = response.Message
        };
    }

    public async Task<bool> ArchiveAsync(string key, string destinationType, Dictionary<string, string>? destinationSettings = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(destinationType);

        _logger?.LogDebug("Archiving object with key: {Key} to {DestinationType}", key, destinationType);

        var request = new ArchiveRequest
        {
            Key = key,
            DestinationType = destinationType
        };

        if (destinationSettings != null)
        {
            request.DestinationSettings.Add(destinationSettings);
        }

        var response = await _client.ArchiveAsync(request, CallOptions(cancellationToken));
        return response.Success;
    }

    public async Task<bool> AddPolicyAsync(Models.LifecyclePolicy policy, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(policy);

        _logger?.LogDebug("Adding lifecycle policy: {PolicyId}", policy.Id);

        var request = new AddPolicyRequest
        {
            Policy = new Objstore.V1.LifecyclePolicy
            {
                Id = policy.Id,
                Prefix = policy.Prefix,
                RetentionSeconds = policy.RetentionSeconds,
                Action = policy.Action,
                DestinationType = policy.DestinationType ?? string.Empty
            }
        };

        if (policy.DestinationSettings != null)
        {
            request.Policy.DestinationSettings.Add(policy.DestinationSettings);
        }

        var response = await _client.AddPolicyAsync(request, CallOptions(cancellationToken));
        return response.Success;
    }

    public async Task<bool> RemovePolicyAsync(string id, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(id);

        _logger?.LogDebug("Removing lifecycle policy: {PolicyId}", id);

        var request = new RemovePolicyRequest { Id = id };
        var response = await _client.RemovePolicyAsync(request, CallOptions(cancellationToken));
        return response.Success;
    }

    public async Task<List<Models.LifecyclePolicy>> GetPoliciesAsync(string? prefix = null, CancellationToken cancellationToken = default)
    {
        _logger?.LogDebug("Getting lifecycle policies");

        var request = new GetPoliciesRequest { Prefix = prefix ?? string.Empty };
        var response = await _client.GetPoliciesAsync(request, CallOptions(cancellationToken));

        return response.Policies.Select(p => new Models.LifecyclePolicy
        {
            Id = p.Id,
            Prefix = p.Prefix,
            RetentionSeconds = p.RetentionSeconds,
            Action = p.Action,
            DestinationType = p.DestinationType,
            DestinationSettings = p.DestinationSettings.ToDictionary(kvp => kvp.Key, kvp => kvp.Value)
        }).ToList();
    }

    public async Task<(bool Success, int PoliciesCount, int ObjectsProcessed)> ApplyPoliciesAsync(CancellationToken cancellationToken = default)
    {
        _logger?.LogDebug("Applying lifecycle policies");

        var request = new ApplyPoliciesRequest();
        var response = await _client.ApplyPoliciesAsync(request, CallOptions(cancellationToken));

        return (response.Success, response.PoliciesCount, response.ObjectsProcessed);
    }

    public async Task<bool> AddReplicationPolicyAsync(Models.ReplicationPolicy policy, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(policy);

        _logger?.LogDebug("Adding replication policy: {PolicyId}", policy.Id);

        var request = new AddReplicationPolicyRequest
        {
            Policy = ConvertToProtoReplicationPolicy(policy)
        };

        var response = await _client.AddReplicationPolicyAsync(request, CallOptions(cancellationToken));
        return response.Success;
    }

    public async Task<bool> RemoveReplicationPolicyAsync(string id, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(id);

        _logger?.LogDebug("Removing replication policy: {PolicyId}", id);

        var request = new RemoveReplicationPolicyRequest { Id = id };
        var response = await _client.RemoveReplicationPolicyAsync(request, CallOptions(cancellationToken));
        return response.Success;
    }

    public async Task<List<Models.ReplicationPolicy>> GetReplicationPoliciesAsync(CancellationToken cancellationToken = default)
    {
        _logger?.LogDebug("Getting replication policies");

        var request = new GetReplicationPoliciesRequest();
        var response = await _client.GetReplicationPoliciesAsync(request, CallOptions(cancellationToken));

        return response.Policies.Select(ConvertFromProtoReplicationPolicy).ToList();
    }

    public async Task<Models.ReplicationPolicy?> GetReplicationPolicyAsync(string id, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(id);

        _logger?.LogDebug("Getting replication policy: {PolicyId}", id);

        var request = new GetReplicationPolicyRequest { Id = id };
        var response = await _client.GetReplicationPolicyAsync(request, CallOptions(cancellationToken));

        return response.Policy != null ? ConvertFromProtoReplicationPolicy(response.Policy) : null;
    }

    public async Task<Models.TriggerReplicationResult> TriggerReplicationAsync(string? policyId = null, bool parallel = false, int workerCount = 4, CancellationToken cancellationToken = default)
    {
        _logger?.LogDebug("Triggering replication for policy: {PolicyId}", policyId ?? "all");

        var request = new TriggerReplicationRequest
        {
            PolicyId = policyId ?? string.Empty,
            Parallel = parallel,
            WorkerCount = workerCount
        };

        var response = await _client.TriggerReplicationAsync(request, CallOptions(cancellationToken));

        Models.SyncResult? syncResult = null;
        if (response.Result != null)
        {
            syncResult = new Models.SyncResult
            {
                PolicyId = response.Result.PolicyId,
                Synced = response.Result.Synced,
                Deleted = response.Result.Deleted,
                Failed = response.Result.Failed,
                BytesTotal = response.Result.BytesTotal,
                // gRPC returns duration_ms as an integer directly
                DurationMs = response.Result.DurationMs
            };
        }

        return new Models.TriggerReplicationResult
        {
            Success = response.Success,
            SyncResult = syncResult,
            Message = response.Message
        };
    }

    public async Task<Models.ReplicationStatus?> GetReplicationStatusAsync(string id, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(id);

        _logger?.LogDebug("Getting replication status for policy: {PolicyId}", id);

        var request = new GetReplicationStatusRequest { Id = id };
        var response = await _client.GetReplicationStatusAsync(request, CallOptions(cancellationToken));

        return response.Success && response.Status != null
            ? ConvertFromProtoReplicationStatus(response.Status)
            : null;
    }

    private static Objstore.V1.Metadata ConvertToProtoMetadata(ObjectMetadata metadata)
    {
        var protoMetadata = new Objstore.V1.Metadata
        {
            ContentType = metadata.ContentType ?? string.Empty,
            ContentEncoding = metadata.ContentEncoding ?? string.Empty,
            Size = metadata.Size,
            Etag = metadata.ETag ?? string.Empty
        };

        if (metadata.LastModified.HasValue)
        {
            protoMetadata.LastModified = Timestamp.FromDateTime(metadata.LastModified.Value.ToUniversalTime());
        }

        if (metadata.Custom != null)
        {
            protoMetadata.Custom.Add(metadata.Custom);
        }

        return protoMetadata;
    }

    private static ObjectMetadata ConvertFromProtoMetadata(Objstore.V1.Metadata protoMetadata)
    {
        return new ObjectMetadata
        {
            ContentType = protoMetadata.ContentType,
            ContentEncoding = protoMetadata.ContentEncoding,
            Size = protoMetadata.Size,
            ETag = protoMetadata.Etag,
            LastModified = protoMetadata.LastModified?.ToDateTime(),
            Custom = protoMetadata.Custom.ToDictionary(kvp => kvp.Key, kvp => kvp.Value)
        };
    }

    private static Objstore.V1.ReplicationPolicy ConvertToProtoReplicationPolicy(Models.ReplicationPolicy policy)
    {
        var proto = new Objstore.V1.ReplicationPolicy
        {
            Id = policy.Id,
            SourceBackend = policy.SourceBackend,
            SourcePrefix = policy.SourcePrefix ?? string.Empty,
            DestinationBackend = policy.DestinationBackend,
            CheckIntervalSeconds = policy.CheckIntervalSeconds,
            Enabled = policy.Enabled,
            ReplicationMode = (Objstore.V1.ReplicationMode)policy.ReplicationMode
        };

        if (policy.SourceSettings != null)
            proto.SourceSettings.Add(policy.SourceSettings);

        if (policy.DestinationSettings != null)
            proto.DestinationSettings.Add(policy.DestinationSettings);

        if (policy.LastSyncTime.HasValue)
            proto.LastSyncTime = Timestamp.FromDateTime(policy.LastSyncTime.Value.ToUniversalTime());

        if (policy.Encryption != null)
        {
            proto.Encryption = new Objstore.V1.EncryptionPolicy();
            if (policy.Encryption.Backend != null)
            {
                proto.Encryption.Backend = new Objstore.V1.EncryptionConfig
                {
                    Enabled = policy.Encryption.Backend.Enabled,
                    Provider = policy.Encryption.Backend.Provider,
                    DefaultKey = policy.Encryption.Backend.DefaultKey
                };
            }
            if (policy.Encryption.Source != null)
            {
                proto.Encryption.Source = new Objstore.V1.EncryptionConfig
                {
                    Enabled = policy.Encryption.Source.Enabled,
                    Provider = policy.Encryption.Source.Provider,
                    DefaultKey = policy.Encryption.Source.DefaultKey
                };
            }
            if (policy.Encryption.Destination != null)
            {
                proto.Encryption.Destination = new Objstore.V1.EncryptionConfig
                {
                    Enabled = policy.Encryption.Destination.Enabled,
                    Provider = policy.Encryption.Destination.Provider,
                    DefaultKey = policy.Encryption.Destination.DefaultKey
                };
            }
        }

        return proto;
    }

    private static Models.ReplicationPolicy ConvertFromProtoReplicationPolicy(Objstore.V1.ReplicationPolicy proto)
    {
        return new Models.ReplicationPolicy
        {
            Id = proto.Id,
            SourceBackend = proto.SourceBackend,
            SourceSettings = proto.SourceSettings.ToDictionary(kvp => kvp.Key, kvp => kvp.Value),
            SourcePrefix = proto.SourcePrefix,
            DestinationBackend = proto.DestinationBackend,
            DestinationSettings = proto.DestinationSettings.ToDictionary(kvp => kvp.Key, kvp => kvp.Value),
            CheckIntervalSeconds = proto.CheckIntervalSeconds,
            LastSyncTime = proto.LastSyncTime?.ToDateTime(),
            Enabled = proto.Enabled,
            ReplicationMode = (Models.ReplicationMode)proto.ReplicationMode,
            Encryption = proto.Encryption != null ? new Models.EncryptionPolicy
            {
                Backend = proto.Encryption.Backend != null ? new Models.EncryptionConfig
                {
                    Enabled = proto.Encryption.Backend.Enabled,
                    Provider = proto.Encryption.Backend.Provider,
                    DefaultKey = proto.Encryption.Backend.DefaultKey
                } : null,
                Source = proto.Encryption.Source != null ? new Models.EncryptionConfig
                {
                    Enabled = proto.Encryption.Source.Enabled,
                    Provider = proto.Encryption.Source.Provider,
                    DefaultKey = proto.Encryption.Source.DefaultKey
                } : null,
                Destination = proto.Encryption.Destination != null ? new Models.EncryptionConfig
                {
                    Enabled = proto.Encryption.Destination.Enabled,
                    Provider = proto.Encryption.Destination.Provider,
                    DefaultKey = proto.Encryption.Destination.DefaultKey
                } : null
            } : null
        };
    }

    private static Models.ReplicationStatus ConvertFromProtoReplicationStatus(Objstore.V1.ReplicationStatus proto)
    {
        return new Models.ReplicationStatus
        {
            PolicyId = proto.PolicyId,
            SourceBackend = proto.SourceBackend,
            DestinationBackend = proto.DestinationBackend,
            Enabled = proto.Enabled,
            TotalObjectsSynced = proto.TotalObjectsSynced,
            TotalObjectsDeleted = proto.TotalObjectsDeleted,
            TotalBytesSynced = proto.TotalBytesSynced,
            TotalErrors = proto.TotalErrors,
            LastSyncTime = proto.LastSyncTime?.ToDateTime(),
            AverageSyncDurationMs = proto.AverageSyncDurationMs,
            SyncCount = proto.SyncCount
        };
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        _channel?.Dispose();
        _disposed = true;
        GC.SuppressFinalize(this);
    }
}
