using ObjStore.SDK.Exceptions;

namespace ObjStore.SDK.Internal;

/// <summary>
/// Maps JSON-RPC 2.0 error codes returned by the go-objstore servers to SDK
/// exception types. The codes mirror pkg/server/jsonrpc on the server side:
/// -32001 Forbidden, -32002 Unauthenticated, -32004 NotFound,
/// -32005 AlreadyExists, -32029 RateLimited, -32602 InvalidParams.
/// Shared by the MCP and Unix JSON-RPC transports.
/// </summary>
internal static class JsonRpcErrorMapper
{
    /// <summary>Authorization denied (HTTP 403 equivalent).</summary>
    internal const int Forbidden = -32001;

    /// <summary>Missing or invalid credentials (HTTP 401 equivalent).</summary>
    internal const int Unauthenticated = -32002;

    /// <summary>Object or policy not found (HTTP 404 equivalent).</summary>
    internal const int NotFound = -32004;

    /// <summary>Resource already exists (HTTP 409 equivalent).</summary>
    internal const int AlreadyExists = -32005;

    /// <summary>Request rejected by rate limiting (HTTP 429 equivalent).</summary>
    internal const int RateLimited = -32029;

    /// <summary>Invalid request parameters (HTTP 400 equivalent).</summary>
    internal const int InvalidParams = -32602;

    /// <summary>
    /// Creates the SDK exception for a JSON-RPC error response. Classification
    /// is by error code, never by message substring.
    /// </summary>
    /// <param name="operation">The operation (method or tool name) that failed</param>
    /// <param name="code">The JSON-RPC error code</param>
    /// <param name="message">The server-provided error message</param>
    internal static ObjectStoreException ToException(string operation, int code, string message)
    {
        return code switch
        {
            NotFound => new ObjectNotFoundException(operation, message),
            Forbidden => new AuthorizationException($"RPC error {code} (forbidden): {message}"),
            Unauthenticated => new AuthenticationException($"RPC error {code} (unauthenticated): {message}"),
            AlreadyExists => new AlreadyExistsException($"RPC error {code} (already exists): {message}"),
            RateLimited => new RateLimitException($"RPC error {code} (rate limited): {message}"),
            InvalidParams => new ValidationException($"RPC error {code} (invalid params): {message}"),
            _ => new OperationFailedException(operation, $"RPC error {code}: {message}")
        };
    }
}
