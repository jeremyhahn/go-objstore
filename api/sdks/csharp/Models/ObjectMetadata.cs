using System.Text.Json.Serialization;

namespace ObjStore.SDK.Models;

/// <summary>
/// Metadata associated with an object in storage
/// </summary>
public class ObjectMetadata
{
    /// <summary>
    /// MIME type of the object (e.g., "application/json")
    /// </summary>
    [JsonPropertyName("content_type")]
    public string? ContentType { get; set; }

    /// <summary>
    /// Encoding applied to the object (e.g., "gzip")
    /// </summary>
    [JsonPropertyName("content_encoding")]
    public string? ContentEncoding { get; set; }

    /// <summary>
    /// Size of the object in bytes
    /// </summary>
    [JsonPropertyName("size")]
    public long Size { get; set; }

    /// <summary>
    /// Timestamp when the object was last modified
    /// </summary>
    [JsonPropertyName("last_modified")]
    public DateTime? LastModified { get; set; }

    /// <summary>
    /// Entity tag for the object (used for versioning/caching)
    /// </summary>
    [JsonPropertyName("etag")]
    public string? ETag { get; set; }

    /// <summary>
    /// Custom metadata key-value pairs
    /// </summary>
    [JsonPropertyName("custom")]
    public Dictionary<string, string>? Custom { get; set; }
}
