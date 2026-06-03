using System.Text.Json.Serialization;

namespace ObjStore.SDK.Models;

/// <summary>
/// Result of a single replication trigger operation.
/// Fields mirror the Go SDK SyncResult and the REST/gRPC server payload.
/// </summary>
public class SyncResult
{
    /// <summary>
    /// Policy ID that was triggered
    /// </summary>
    [JsonPropertyName("policy_id")]
    public string PolicyId { get; set; } = string.Empty;

    /// <summary>
    /// Number of objects successfully synced
    /// </summary>
    [JsonPropertyName("synced")]
    public int Synced { get; set; }

    /// <summary>
    /// Number of objects deleted at the destination
    /// </summary>
    [JsonPropertyName("deleted")]
    public int Deleted { get; set; }

    /// <summary>
    /// Number of objects that failed to sync
    /// </summary>
    [JsonPropertyName("failed")]
    public int Failed { get; set; }

    /// <summary>
    /// Total bytes transferred
    /// </summary>
    [JsonPropertyName("bytes_total")]
    public long BytesTotal { get; set; }

    /// <summary>
    /// Sync duration in milliseconds.
    /// For REST, the server returns a Go duration string (e.g. "5.2s") which is parsed here.
    /// For gRPC, the server returns an integer duration_ms directly.
    /// </summary>
    [JsonPropertyName("duration_ms")]
    public long DurationMs { get; set; }
}

/// <summary>
/// Rich result returned by TriggerReplicationAsync, containing the server's full trigger payload.
/// </summary>
public class TriggerReplicationResult
{
    /// <summary>
    /// Whether the trigger operation succeeded
    /// </summary>
    public bool Success { get; set; }

    /// <summary>
    /// The sync result from the server, containing policy_id, counters, bytes, and duration.
    /// May be null if the server returned no result body (e.g. on failure).
    /// </summary>
    public SyncResult? SyncResult { get; set; }

    /// <summary>
    /// Optional message from the server
    /// </summary>
    public string? Message { get; set; }
}
