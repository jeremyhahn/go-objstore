using ObjStore.SDK.Models;

namespace ObjStore.SDK;

/// <summary>
/// Unified interface for object storage operations across all protocols
/// </summary>
public interface IObjectStoreClient : IDisposable
{
    /// <summary>
    /// Stores an object in the backend
    /// </summary>
    Task<string?> PutAsync(string key, byte[] data, ObjectMetadata? metadata = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Stores an object in the backend with explicit metadata (for feature parity with other SDKs)
    /// </summary>
    Task<string?> PutWithMetadataAsync(string key, byte[] data, ObjectMetadata metadata, CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves an object from the backend
    /// </summary>
    Task<(byte[] Data, ObjectMetadata? Metadata)> GetAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes an object from the backend
    /// </summary>
    Task<bool> DeleteAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns a list of objects that match the given criteria
    /// </summary>
    Task<ListObjectsResponse> ListAsync(string? prefix = null, string? delimiter = null, int? maxResults = null, string? continueFrom = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Checks if an object exists in the backend
    /// </summary>
    Task<bool> ExistsAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves only the metadata for an object without its content
    /// </summary>
    Task<ObjectMetadata?> GetMetadataAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>
    /// Updates the metadata for an existing object
    /// </summary>
    Task<bool> UpdateMetadataAsync(string key, ObjectMetadata metadata, CancellationToken cancellationToken = default);

    /// <summary>
    /// Health check endpoint for service health monitoring
    /// </summary>
    Task<HealthResponse> HealthAsync(string? service = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Copies an object to an archival storage backend
    /// </summary>
    Task<bool> ArchiveAsync(string key, string destinationType, Dictionary<string, string>? destinationSettings = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Adds a new lifecycle policy
    /// </summary>
    Task<bool> AddPolicyAsync(LifecyclePolicy policy, CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes an existing lifecycle policy
    /// </summary>
    Task<bool> RemovePolicyAsync(string id, CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves all lifecycle policies
    /// </summary>
    Task<List<LifecyclePolicy>> GetPoliciesAsync(string? prefix = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Executes all lifecycle policies
    /// </summary>
    Task<(bool Success, int PoliciesCount, int ObjectsProcessed)> ApplyPoliciesAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Adds a new replication policy
    /// </summary>
    Task<bool> AddReplicationPolicyAsync(ReplicationPolicy policy, CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes an existing replication policy
    /// </summary>
    Task<bool> RemoveReplicationPolicyAsync(string id, CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves all replication policies
    /// </summary>
    Task<List<ReplicationPolicy>> GetReplicationPoliciesAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves a specific replication policy
    /// </summary>
    Task<ReplicationPolicy?> GetReplicationPolicyAsync(string id, CancellationToken cancellationToken = default);

    /// <summary>
    /// Triggers synchronization for one or all policies
    /// </summary>
    Task<bool> TriggerReplicationAsync(string? policyId = null, bool parallel = false, int workerCount = 4, CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves status and metrics for a specific replication policy
    /// </summary>
    Task<ReplicationStatus?> GetReplicationStatusAsync(string id, CancellationToken cancellationToken = default);
}
