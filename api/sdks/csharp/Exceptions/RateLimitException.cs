namespace ObjStore.SDK.Exceptions;

/// <summary>
/// Exception thrown when the request is rejected by server-side rate limiting (HTTP 429)
/// </summary>
public class RateLimitException : ObjectStoreException
{
    /// <summary>
    /// Creates a new instance of RateLimitException
    /// </summary>
    /// <param name="message">The error message</param>
    public RateLimitException(string message)
        : base(message, 429)
    {
    }

    /// <summary>
    /// Creates a new instance of RateLimitException with an inner exception
    /// </summary>
    /// <param name="message">The error message</param>
    /// <param name="innerException">The inner exception</param>
    public RateLimitException(string message, Exception innerException)
        : base(message, 429, innerException)
    {
    }
}
