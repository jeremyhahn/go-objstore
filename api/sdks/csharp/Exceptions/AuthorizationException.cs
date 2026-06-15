namespace ObjStore.SDK.Exceptions;

/// <summary>
/// Exception thrown when the caller is authenticated but not permitted to perform the operation (HTTP 403)
/// </summary>
public class AuthorizationException : ObjectStoreException
{
    /// <summary>
    /// Creates a new instance of AuthorizationException
    /// </summary>
    /// <param name="message">The error message</param>
    public AuthorizationException(string message)
        : base(message, 403)
    {
    }

    /// <summary>
    /// Creates a new instance of AuthorizationException with an inner exception
    /// </summary>
    /// <param name="message">The error message</param>
    /// <param name="innerException">The inner exception</param>
    public AuthorizationException(string message, Exception innerException)
        : base(message, 403, innerException)
    {
    }
}
