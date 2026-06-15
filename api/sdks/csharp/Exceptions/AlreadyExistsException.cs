namespace ObjStore.SDK.Exceptions;

/// <summary>
/// Exception thrown when a resource being created already exists (HTTP 409)
/// </summary>
public class AlreadyExistsException : ObjectStoreException
{
    /// <summary>
    /// Creates a new instance of AlreadyExistsException
    /// </summary>
    /// <param name="message">The error message</param>
    public AlreadyExistsException(string message)
        : base(message, 409)
    {
    }

    /// <summary>
    /// Creates a new instance of AlreadyExistsException with an inner exception
    /// </summary>
    /// <param name="message">The error message</param>
    /// <param name="innerException">The inner exception</param>
    public AlreadyExistsException(string message, Exception innerException)
        : base(message, 409, innerException)
    {
    }
}
