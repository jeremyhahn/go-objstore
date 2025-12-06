namespace ObjStore.SDK.Exceptions;

/// <summary>
/// Exception thrown when a requested object is not found
/// </summary>
public class ObjectNotFoundException : ObjectStoreException
{
    /// <summary>
    /// Gets the key of the object that was not found
    /// </summary>
    public string Key { get; }

    /// <summary>
    /// Creates a new instance of ObjectNotFoundException
    /// </summary>
    /// <param name="key">The key of the object that was not found</param>
    public ObjectNotFoundException(string key)
        : base($"Object with key '{key}' was not found", 404)
    {
        Key = key;
    }

    /// <summary>
    /// Creates a new instance of ObjectNotFoundException with a custom message
    /// </summary>
    /// <param name="key">The key of the object that was not found</param>
    /// <param name="message">The error message</param>
    public ObjectNotFoundException(string key, string message)
        : base(message, 404)
    {
        Key = key;
    }

    /// <summary>
    /// Creates a new instance of ObjectNotFoundException with an inner exception
    /// </summary>
    /// <param name="key">The key of the object that was not found</param>
    /// <param name="message">The error message</param>
    /// <param name="innerException">The inner exception</param>
    public ObjectNotFoundException(string key, string message, Exception innerException)
        : base(message, 404, innerException)
    {
        Key = key;
    }
}
