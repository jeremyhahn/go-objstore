namespace ObjStore.SDK.Exceptions;

/// <summary>
/// Exception thrown when an object store operation fails
/// </summary>
public class OperationFailedException : ObjectStoreException
{
    /// <summary>
    /// Gets the operation that failed
    /// </summary>
    public string Operation { get; }

    /// <summary>
    /// Creates a new instance of OperationFailedException
    /// </summary>
    /// <param name="operation">The operation that failed</param>
    /// <param name="message">The error message</param>
    public OperationFailedException(string operation, string message) : base(message)
    {
        Operation = operation;
    }

    /// <summary>
    /// Creates a new instance of OperationFailedException with a status code
    /// </summary>
    /// <param name="operation">The operation that failed</param>
    /// <param name="message">The error message</param>
    /// <param name="statusCode">The HTTP status code</param>
    public OperationFailedException(string operation, string message, int statusCode)
        : base(message, statusCode)
    {
        Operation = operation;
    }

    /// <summary>
    /// Creates a new instance of OperationFailedException with an inner exception
    /// </summary>
    /// <param name="operation">The operation that failed</param>
    /// <param name="message">The error message</param>
    /// <param name="innerException">The inner exception</param>
    public OperationFailedException(string operation, string message, Exception innerException)
        : base(message, innerException)
    {
        Operation = operation;
    }

    /// <summary>
    /// Creates a new instance of OperationFailedException with a status code and inner exception
    /// </summary>
    /// <param name="operation">The operation that failed</param>
    /// <param name="message">The error message</param>
    /// <param name="statusCode">The HTTP status code</param>
    /// <param name="innerException">The inner exception</param>
    public OperationFailedException(string operation, string message, int statusCode, Exception innerException)
        : base(message, statusCode, innerException)
    {
        Operation = operation;
    }
}
