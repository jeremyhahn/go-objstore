using System.Text.Json.Nodes;

namespace ObjStore.SDK.Internal;

/// <summary>
/// Shared JSON parsing helpers for the JSON-RPC transports (MCP, Unix).
/// </summary>
internal static class JsonHelpers
{
    /// <summary>
    /// Parses a JSON object node into a string/string dictionary of custom
    /// metadata. Returns null when the node is absent, not an object, or empty.
    /// </summary>
    /// <param name="node">The JSON node holding the custom metadata map</param>
    internal static Dictionary<string, string>? ParseCustomMap(JsonNode? node)
    {
        if (node is not JsonObject obj)
            return null;

        var dict = new Dictionary<string, string>();
        foreach (var kvp in obj)
        {
            if (kvp.Value != null)
                dict[kvp.Key] = kvp.Value.GetValue<string>();
        }

        return dict.Count > 0 ? dict : null;
    }
}
