namespace ObjStore.SDK.Internal;

/// <summary>
/// Applies app-layer authentication headers (bearer token, tenant ID, custom
/// headers) to outgoing HTTP requests. Shared by the REST, QUIC, and MCP
/// transports.
/// </summary>
internal static class AuthHeaders
{
    /// <summary>
    /// Applies auth headers to the outgoing request.
    /// </summary>
    /// <param name="request">The request to decorate</param>
    /// <param name="token">Optional bearer token for the Authorization header</param>
    /// <param name="tenantId">Optional tenant ID for the X-Tenant-ID header</param>
    /// <param name="extraHeaders">Optional additional headers forwarded verbatim</param>
    internal static void Apply(HttpRequestMessage request, string? token, string? tenantId, IDictionary<string, string>? extraHeaders)
    {
        if (!string.IsNullOrEmpty(token))
            request.Headers.TryAddWithoutValidation("Authorization", $"Bearer {token}");

        if (!string.IsNullOrEmpty(tenantId))
            request.Headers.TryAddWithoutValidation("X-Tenant-ID", tenantId);

        if (extraHeaders != null)
        {
            foreach (var kvp in extraHeaders)
                request.Headers.TryAddWithoutValidation(kvp.Key, kvp.Value);
        }
    }
}
