using System.Text.Json;
using System.Text.Json.Serialization;

namespace ObjStore.SDK.Models;

/// <summary>
/// Custom JSON converter for ReplicationMode that serializes to lowercase strings
/// </summary>
public class ReplicationModeConverter : JsonConverter<ReplicationMode>
{
    public override ReplicationMode Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        var value = reader.GetString();
        return value?.ToLowerInvariant() switch
        {
            "transparent" => ReplicationMode.Transparent,
            "opaque" => ReplicationMode.Opaque,
            _ => ReplicationMode.Transparent
        };
    }

    public override void Write(Utf8JsonWriter writer, ReplicationMode value, JsonSerializerOptions options)
    {
        var stringValue = value switch
        {
            ReplicationMode.Transparent => "transparent",
            ReplicationMode.Opaque => "opaque",
            _ => "transparent"
        };
        writer.WriteStringValue(stringValue);
    }
}

/// <summary>
/// Replication mode enumeration
/// </summary>
[JsonConverter(typeof(ReplicationModeConverter))]
public enum ReplicationMode
{
    /// <summary>
    /// Decrypts at source and re-encrypts at destination
    /// </summary>
    Transparent = 0,

    /// <summary>
    /// Copies encrypted blobs as-is (no DEK operations)
    /// </summary>
    Opaque = 1
}

/// <summary>
/// Encryption configuration for a single layer
/// </summary>
public class EncryptionConfig
{
    /// <summary>
    /// Whether this encryption layer is enabled
    /// </summary>
    [JsonPropertyName("enabled")]
    public bool Enabled { get; set; }

    /// <summary>
    /// Encryption provider ("noop", "custom")
    /// </summary>
    [JsonPropertyName("provider")]
    public string Provider { get; set; } = string.Empty;

    /// <summary>
    /// Provider-agnostic key identifier
    /// </summary>
    [JsonPropertyName("default_key")]
    public string DefaultKey { get; set; } = string.Empty;
}

/// <summary>
/// Encryption policy for all three layers
/// </summary>
public class EncryptionPolicy
{
    /// <summary>
    /// Backend at-rest encryption configuration
    /// </summary>
    [JsonPropertyName("backend")]
    public EncryptionConfig? Backend { get; set; }

    /// <summary>
    /// Source client-side DEK configuration
    /// </summary>
    [JsonPropertyName("source")]
    public EncryptionConfig? Source { get; set; }

    /// <summary>
    /// Destination client-side DEK configuration
    /// </summary>
    [JsonPropertyName("destination")]
    public EncryptionConfig? Destination { get; set; }
}

/// <summary>
/// Replication policy configuration
/// </summary>
public class ReplicationPolicy
{
    /// <summary>
    /// Unique identifier for the policy
    /// </summary>
    [JsonPropertyName("id")]
    public string Id { get; set; } = string.Empty;

    /// <summary>
    /// Source backend type (e.g., "local", "s3", "gcs")
    /// </summary>
    [JsonPropertyName("source_backend")]
    public string SourceBackend { get; set; } = string.Empty;

    /// <summary>
    /// Source backend-specific configuration
    /// </summary>
    [JsonPropertyName("source_settings")]
    public Dictionary<string, string>? SourceSettings { get; set; }

    /// <summary>
    /// Source object key prefix filter (empty means all)
    /// </summary>
    [JsonPropertyName("source_prefix")]
    public string? SourcePrefix { get; set; }

    /// <summary>
    /// Destination backend type
    /// </summary>
    [JsonPropertyName("destination_backend")]
    public string DestinationBackend { get; set; } = string.Empty;

    /// <summary>
    /// Destination backend-specific configuration
    /// </summary>
    [JsonPropertyName("destination_settings")]
    public Dictionary<string, string>? DestinationSettings { get; set; }

    /// <summary>
    /// Check interval in seconds
    /// </summary>
    [JsonPropertyName("check_interval_seconds")]
    public long CheckIntervalSeconds { get; set; }

    /// <summary>
    /// Last sync timestamp
    /// </summary>
    [JsonPropertyName("last_sync_time")]
    public DateTime? LastSyncTime { get; set; }

    /// <summary>
    /// Whether this policy is active
    /// </summary>
    [JsonPropertyName("enabled")]
    public bool Enabled { get; set; }

    /// <summary>
    /// Encryption configuration
    /// </summary>
    [JsonPropertyName("encryption")]
    public EncryptionPolicy? Encryption { get; set; }

    /// <summary>
    /// Replication mode
    /// </summary>
    [JsonPropertyName("replication_mode")]
    public ReplicationMode ReplicationMode { get; set; }
}

/// <summary>
/// Replication status and metrics
/// </summary>
public class ReplicationStatus
{
    /// <summary>
    /// Policy ID
    /// </summary>
    [JsonPropertyName("policy_id")]
    public string PolicyId { get; set; } = string.Empty;

    /// <summary>
    /// Source backend type
    /// </summary>
    [JsonPropertyName("source_backend")]
    public string SourceBackend { get; set; } = string.Empty;

    /// <summary>
    /// Destination backend type
    /// </summary>
    [JsonPropertyName("destination_backend")]
    public string DestinationBackend { get; set; } = string.Empty;

    /// <summary>
    /// Whether the policy is enabled
    /// </summary>
    [JsonPropertyName("enabled")]
    public bool Enabled { get; set; }

    /// <summary>
    /// Total objects synced (all-time)
    /// </summary>
    [JsonPropertyName("total_objects_synced")]
    public long TotalObjectsSynced { get; set; }

    /// <summary>
    /// Total objects deleted (all-time)
    /// </summary>
    [JsonPropertyName("total_objects_deleted")]
    public long TotalObjectsDeleted { get; set; }

    /// <summary>
    /// Total bytes synced (all-time)
    /// </summary>
    [JsonPropertyName("total_bytes_synced")]
    public long TotalBytesSynced { get; set; }

    /// <summary>
    /// Total errors encountered (all-time)
    /// </summary>
    [JsonPropertyName("total_errors")]
    public long TotalErrors { get; set; }

    /// <summary>
    /// Last sync timestamp
    /// </summary>
    [JsonPropertyName("last_sync_time")]
    public DateTime? LastSyncTime { get; set; }

    /// <summary>
    /// Average sync duration in milliseconds
    /// </summary>
    [JsonPropertyName("average_sync_duration_ms")]
    public long AverageSyncDurationMs { get; set; }

    /// <summary>
    /// Number of syncs performed
    /// </summary>
    [JsonPropertyName("sync_count")]
    public long SyncCount { get; set; }
}
