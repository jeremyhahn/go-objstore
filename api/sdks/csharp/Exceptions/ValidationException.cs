namespace ObjStore.SDK.Exceptions;

/// <summary>
/// Exception thrown when the server rejects a request as invalid (HTTP 400)
/// </summary>
public class ValidationException : ObjectStoreException
{
    /// <summary>
    /// Creates a new instance of ValidationException
    /// </summary>
    /// <param name="message">The error message</param>
    public ValidationException(string message)
        : base(message, 400)
    {
    }

    /// <summary>
    /// Creates a new instance of ValidationException with an inner exception
    /// </summary>
    /// <param name="message">The error message</param>
    /// <param name="innerException">The inner exception</param>
    public ValidationException(string message, Exception innerException)
        : base(message, 400, innerException)
    {
    }
}
