using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
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
    private bool _disposed;

    public GrpcClient(string address, ILogger<GrpcClient>? logger = null)
    {
        _channel = GrpcChannel.ForAddress(address);
        _client = new ObjectStore.ObjectStoreClient(_channel);
        _logger = logger;
    }

    public GrpcClient(GrpcChannel channel, ILogger<GrpcClient>? logger = null)
    {
        _channel = channel ?? throw new ArgumentNullException(nameof(channel));
        _client = new ObjectStore.ObjectStoreClient(_channel);
        _logger = logger;
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

            var response = await _client.PutAsync(request, cancellationToken: cancellationToken).ConfigureAwait(false);
            return response.Success ? response.Etag : null;
        }
        catch (Grpc.Core.RpcException ex) when (ex.StatusCode == Grpc.Core.StatusCode.NotFound)
        {
            throw new ObjectNotFoundException(key);
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

    public async Task<(byte[] Data, ObjectMetadata? Metadata)> GetAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);

        _logger?.LogDebug("Getting object with key: {Key}", key);

        try
        {
            var request = new GetRequest { Key = key };
            using var call = _client.Get(request, cancellationToken: cancellationToken);

            var chunks = new List<byte[]>();
            Metadata? protoMetadata = null;

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
            var metadata = protoMetadata != null ? ConvertFromProtoMetadata(protoMetadata) : null;

            return (data, metadata);
        }
        catch (Grpc.Core.RpcException ex) when (ex.StatusCode == Grpc.Core.StatusCode.NotFound ||
            ex.Status.Detail.Contains("no such file") ||
            ex.Status.Detail.Contains("not found", StringComparison.OrdinalIgnoreCase))
        {
            throw new ObjectNotFoundException(key);
        }
    }

    public async Task<bool> DeleteAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);

        _logger?.LogDebug("Deleting object with key: {Key}", key);

        try
        {
            var request = new DeleteRequest { Key = key };
            var response = await _client.DeleteAsync(request, cancellationToken: cancellationToken);
            return response.Success;
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

        var response = await _client.ListAsync(request, cancellationToken: cancellationToken);

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
        var response = await _client.ExistsAsync(request, cancellationToken: cancellationToken);
        return response.Exists;
    }

    public async Task<ObjectMetadata?> GetMetadataAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);

        _logger?.LogDebug("Getting metadata for object with key: {Key}", key);

        var request = new GetMetadataRequest { Key = key };
        var response = await _client.GetMetadataAsync(request, cancellationToken: cancellationToken);

        return response.Success && response.Metadata != null
            ? ConvertFromProtoMetadata(response.Metadata)
            : null;
    }

    public async Task<bool> UpdateMetadataAsync(string key, ObjectMetadata metadata, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(metadata);

        _logger?.LogDebug("Updating metadata for object with key: {Key}", key);

        var request = new UpdateMetadataRequest
        {
            Key = key,
            Metadata = ConvertToProtoMetadata(metadata)
        };

        var response = await _client.UpdateMetadataAsync(request, cancellationToken: cancellationToken);
        return response.Success;
    }

    public async Task<Models.HealthResponse> HealthAsync(string? service = null, CancellationToken cancellationToken = default)
    {
        _logger?.LogDebug("Checking health");

        var request = new HealthRequest { Service = service ?? string.Empty };
        var response = await _client.HealthAsync(request, cancellationToken: cancellationToken);

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

        var response = await _client.ArchiveAsync(request, cancellationToken: cancellationToken);
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

        var response = await _client.AddPolicyAsync(request, cancellationToken: cancellationToken);
        return response.Success;
    }

    public async Task<bool> RemovePolicyAsync(string id, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(id);

        _logger?.LogDebug("Removing lifecycle policy: {PolicyId}", id);

        var request = new RemovePolicyRequest { Id = id };
        var response = await _client.RemovePolicyAsync(request, cancellationToken: cancellationToken);
        return response.Success;
    }

    public async Task<List<Models.LifecyclePolicy>> GetPoliciesAsync(string? prefix = null, CancellationToken cancellationToken = default)
    {
        _logger?.LogDebug("Getting lifecycle policies");

        var request = new GetPoliciesRequest { Prefix = prefix ?? string.Empty };
        var response = await _client.GetPoliciesAsync(request, cancellationToken: cancellationToken);

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
        var response = await _client.ApplyPoliciesAsync(request, cancellationToken: cancellationToken);

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

        var response = await _client.AddReplicationPolicyAsync(request, cancellationToken: cancellationToken);
        return response.Success;
    }

    public async Task<bool> RemoveReplicationPolicyAsync(string id, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(id);

        _logger?.LogDebug("Removing replication policy: {PolicyId}", id);

        var request = new RemoveReplicationPolicyRequest { Id = id };
        var response = await _client.RemoveReplicationPolicyAsync(request, cancellationToken: cancellationToken);
        return response.Success;
    }

    public async Task<List<Models.ReplicationPolicy>> GetReplicationPoliciesAsync(CancellationToken cancellationToken = default)
    {
        _logger?.LogDebug("Getting replication policies");

        var request = new GetReplicationPoliciesRequest();
        var response = await _client.GetReplicationPoliciesAsync(request, cancellationToken: cancellationToken);

        return response.Policies.Select(ConvertFromProtoReplicationPolicy).ToList();
    }

    public async Task<Models.ReplicationPolicy?> GetReplicationPolicyAsync(string id, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(id);

        _logger?.LogDebug("Getting replication policy: {PolicyId}", id);

        var request = new GetReplicationPolicyRequest { Id = id };
        var response = await _client.GetReplicationPolicyAsync(request, cancellationToken: cancellationToken);

        return response.Policy != null ? ConvertFromProtoReplicationPolicy(response.Policy) : null;
    }

    public async Task<bool> TriggerReplicationAsync(string? policyId = null, bool parallel = false, int workerCount = 4, CancellationToken cancellationToken = default)
    {
        _logger?.LogDebug("Triggering replication for policy: {PolicyId}", policyId ?? "all");

        var request = new TriggerReplicationRequest
        {
            PolicyId = policyId ?? string.Empty,
            Parallel = parallel,
            WorkerCount = workerCount
        };

        var response = await _client.TriggerReplicationAsync(request, cancellationToken: cancellationToken);
        return response.Success;
    }

    public async Task<Models.ReplicationStatus?> GetReplicationStatusAsync(string id, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(id);

        _logger?.LogDebug("Getting replication status for policy: {PolicyId}", id);

        var request = new GetReplicationStatusRequest { Id = id };
        var response = await _client.GetReplicationStatusAsync(request, cancellationToken: cancellationToken);

        return response.Success && response.Status != null
            ? ConvertFromProtoReplicationStatus(response.Status)
            : null;
    }

    private static Metadata ConvertToProtoMetadata(ObjectMetadata metadata)
    {
        var protoMetadata = new Metadata
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

    private static ObjectMetadata ConvertFromProtoMetadata(Metadata protoMetadata)
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
