namespace ObjStore.SDK.Exceptions;

/// <summary>
/// Base exception for all object store operations
/// </summary>
public class ObjectStoreException : Exception
{
    /// <summary>
    /// Gets the HTTP status code associated with this exception, if applicable
    /// </summary>
    public int? StatusCode { get; }

    /// <summary>
    /// Creates a new instance of ObjectStoreException
    /// </summary>
    public ObjectStoreException()
    {
    }

    /// <summary>
    /// Creates a new instance of ObjectStoreException with a message
    /// </summary>
    /// <param name="message">The error message</param>
    public ObjectStoreException(string message) : base(message)
    {
    }

    /// <summary>
    /// Creates a new instance of ObjectStoreException with a message and inner exception
    /// </summary>
    /// <param name="message">The error message</param>
    /// <param name="innerException">The inner exception</param>
    public ObjectStoreException(string message, Exception innerException) : base(message, innerException)
    {
    }

    /// <summary>
    /// Creates a new instance of ObjectStoreException with a message and status code
    /// </summary>
    /// <param name="message">The error message</param>
    /// <param name="statusCode">The HTTP status code</param>
    public ObjectStoreException(string message, int statusCode) : base(message)
    {
        StatusCode = statusCode;
    }

    /// <summary>
    /// Creates a new instance of ObjectStoreException with a message, status code, and inner exception
    /// </summary>
    /// <param name="message">The error message</param>
    /// <param name="statusCode">The HTTP status code</param>
    /// <param name="innerException">The inner exception</param>
    public ObjectStoreException(string message, int statusCode, Exception innerException) : base(message, innerException)
    {
        StatusCode = statusCode;
    }
}
