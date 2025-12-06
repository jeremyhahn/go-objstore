using System.Text.Json.Serialization;

namespace ObjStore.SDK.Models;

/// <summary>
/// Complete information about a stored object
/// </summary>
public class ObjectInfo
{
    /// <summary>
    /// Object's storage key/path
    /// </summary>
    [JsonPropertyName("key")]
    public string Key { get; set; } = string.Empty;

    /// <summary>
    /// Object's metadata
    /// </summary>
    [JsonPropertyName("metadata")]
    public ObjectMetadata? Metadata { get; set; }
}
