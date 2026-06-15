using System.Text.Json;
using ObjStore.SDK.Exceptions;

namespace ObjStore.SDK.Internal;

/// <summary>
/// Maps non-success HTTP status codes returned by the go-objstore HTTP transports
/// (REST, QUIC and MCP) to SDK exception types:
/// 400 ValidationException, 401 AuthenticationException, 403 AuthorizationException,
/// 404 ObjectNotFoundException, 409 AlreadyExistsException, 429 RateLimitException,
/// anything else (including 5xx) OperationFailedException carrying the status code.
/// </summary>
internal static class HttpErrorMapper
{
    /// <summary>
    /// Throws the mapped SDK exception when the response indicates failure.
    /// Reads the response body to surface the server-provided error message.
    /// </summary>
    /// <param name="response">The HTTP response to inspect</param>
    /// <param name="operation">The operation that produced the response</param>
    /// <param name="key">Optional object key for 404 mapping to ObjectNotFoundException</param>
    /// <param name="cancellationToken">Cancellation token</param>
    internal static async Task EnsureSuccessAsync(
        HttpResponseMessage response,
        string operation,
        string? key = null,
        CancellationToken cancellationToken = default)
    {
        if (response.IsSuccessStatusCode)
            return;

        var message = await ExtractErrorMessageAsync(response, cancellationToken).ConfigureAwait(false);
        throw ToException(operation, (int)response.StatusCode, message, key);
    }

    /// <summary>
    /// Creates the SDK exception for a non-success HTTP status code. Classification
    /// is by status code, never by message substring.
    /// </summary>
    /// <param name="operation">The operation that failed</param>
    /// <param name="statusCode">The HTTP status code</param>
    /// <param name="message">The server-provided error message</param>
    /// <param name="key">Optional object key for 404 mapping to ObjectNotFoundException</param>
    internal static ObjectStoreException ToException(string operation, int statusCode, string message, string? key = null)
    {
        return statusCode switch
        {
            400 => new ValidationException($"{operation} failed (HTTP 400): {message}"),
            401 => new AuthenticationException($"{operation} failed (HTTP 401): {message}"),
            403 => new AuthorizationException($"{operation} failed (HTTP 403): {message}"),
            404 => new ObjectNotFoundException(key ?? operation, message),
            409 => new AlreadyExistsException($"{operation} failed (HTTP 409): {message}"),
            429 => new RateLimitException($"{operation} failed (HTTP 429): {message}"),
            _ => new OperationFailedException(operation, message, statusCode)
        };
    }

    /// <summary>
    /// Extracts the error message from a response body. The go-objstore servers
    /// return {"error": "..."} JSON; falls back to the raw body, then to the
    /// status code and reason phrase when the body is empty or unreadable.
    /// </summary>
    private static async Task<string> ExtractErrorMessageAsync(HttpResponseMessage response, CancellationToken cancellationToken)
    {
        string body;
        try
        {
            body = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (HttpRequestException)
        {
            body = string.Empty;
        }

        if (!string.IsNullOrWhiteSpace(body))
        {
            try
            {
                using var doc = JsonDocument.Parse(body);
                if (doc.RootElement.ValueKind == JsonValueKind.Object)
                {
                    if (doc.RootElement.TryGetProperty("error", out var errorEl) && errorEl.ValueKind == JsonValueKind.String)
                        return errorEl.GetString()!;
                    if (doc.RootElement.TryGetProperty("message", out var messageEl) && messageEl.ValueKind == JsonValueKind.String)
                        return messageEl.GetString()!;
                }
            }
            catch (JsonException)
            {
                // Not JSON — fall through to the raw body.
            }
            return body;
        }

        return $"HTTP {(int)response.StatusCode} {response.ReasonPhrase}";
    }
}
