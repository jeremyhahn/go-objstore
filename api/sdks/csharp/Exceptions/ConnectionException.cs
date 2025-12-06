namespace ObjStore.SDK.Exceptions;

/// <summary>
/// Exception thrown when a connection to the object store fails
/// </summary>
public class ConnectionException : ObjectStoreException
{
    /// <summary>
    /// Gets the endpoint that the connection attempt was made to
    /// </summary>
    public string? Endpoint { get; }

    /// <summary>
    /// Creates a new instance of ConnectionException
    /// </summary>
    /// <param name="message">The error message</param>
    public ConnectionException(string message) : base(message)
    {
    }

    /// <summary>
    /// Creates a new instance of ConnectionException with an endpoint
    /// </summary>
    /// <param name="endpoint">The endpoint that failed</param>
    /// <param name="message">The error message</param>
    public ConnectionException(string endpoint, string message) : base(message)
    {
        Endpoint = endpoint;
    }

    /// <summary>
    /// Creates a new instance of ConnectionException with an inner exception
    /// </summary>
    /// <param name="message">The error message</param>
    /// <param name="innerException">The inner exception</param>
    public ConnectionException(string message, Exception innerException) : base(message, innerException)
    {
    }

    /// <summary>
    /// Creates a new instance of ConnectionException with an endpoint and inner exception
    /// </summary>
    /// <param name="endpoint">The endpoint that failed</param>
    /// <param name="message">The error message</param>
    /// <param name="innerException">The inner exception</param>
    public ConnectionException(string endpoint, string message, Exception innerException) : base(message, innerException)
    {
        Endpoint = endpoint;
    }
}
