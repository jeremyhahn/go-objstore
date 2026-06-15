namespace ObjStore.SDK.Exceptions;

/// <summary>
/// Exception thrown when authentication fails due to missing or invalid credentials (HTTP 401)
/// </summary>
public class AuthenticationException : ObjectStoreException
{
    /// <summary>
    /// Creates a new instance of AuthenticationException
    /// </summary>
    /// <param name="message">The error message</param>
    public AuthenticationException(string message)
        : base(message, 401)
    {
    }

    /// <summary>
    /// Creates a new instance of AuthenticationException with an inner exception
    /// </summary>
    /// <param name="message">The error message</param>
    /// <param name="innerException">The inner exception</param>
    public AuthenticationException(string message, Exception innerException)
        : base(message, 401, innerException)
    {
    }
}
