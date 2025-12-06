using System.Text.Json.Serialization;

namespace ObjStore.SDK.Models;

/// <summary>
/// Response from a List operation
/// </summary>
public class ListObjectsResponse
{
    /// <summary>
    /// List of objects matching the criteria
    /// </summary>
    [JsonPropertyName("objects")]
    public List<ObjectInfo> Objects { get; set; } = new();

    /// <summary>
    /// Common prefixes when using delimiter
    /// </summary>
    [JsonPropertyName("common_prefixes")]
    public List<string> CommonPrefixes { get; set; } = new();

    /// <summary>
    /// Pagination token for the next page
    /// </summary>
    [JsonPropertyName("next_token")]
    public string? NextToken { get; set; }

    /// <summary>
    /// Whether more results are available
    /// </summary>
    [JsonPropertyName("truncated")]
    public bool Truncated { get; set; }
}
