using System.Text.Json.Serialization;

namespace ObjStore.SDK.Models;

/// <summary>
/// Lifecycle policy for objects
/// </summary>
public class LifecyclePolicy
{
    /// <summary>
    /// Unique identifier for the policy
    /// </summary>
    [JsonPropertyName("id")]
    public string Id { get; set; } = string.Empty;

    /// <summary>
    /// Prefix of objects to which the policy applies
    /// </summary>
    [JsonPropertyName("prefix")]
    public string Prefix { get; set; } = string.Empty;

    /// <summary>
    /// Retention duration in seconds
    /// </summary>
    [JsonPropertyName("retention_seconds")]
    public long RetentionSeconds { get; set; }

    /// <summary>
    /// Action to take after retention period ("delete" or "archive")
    /// </summary>
    [JsonPropertyName("action")]
    public string Action { get; set; } = string.Empty;

    /// <summary>
    /// Destination backend type for archive action
    /// </summary>
    [JsonPropertyName("destination_type")]
    public string? DestinationType { get; set; }

    /// <summary>
    /// Destination backend settings for archive action
    /// </summary>
    [JsonPropertyName("destination_settings")]
    public Dictionary<string, string>? DestinationSettings { get; set; }
}
