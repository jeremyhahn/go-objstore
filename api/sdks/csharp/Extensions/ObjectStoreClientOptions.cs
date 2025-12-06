namespace ObjStore.SDK.Extensions;

/// <summary>
/// Configuration options for ObjectStore clients
/// </summary>
public class ObjectStoreClientOptions
{
    /// <summary>
    /// The base URL of the ObjectStore service
    /// </summary>
    public string BaseUrl { get; set; } = string.Empty;

    /// <summary>
    /// The protocol to use (REST, GRPC, QUIC)
    /// </summary>
    public Protocol Protocol { get; set; } = Protocol.REST;

    /// <summary>
    /// Request timeout duration
    /// </summary>
    public TimeSpan? Timeout { get; set; }

    /// <summary>
    /// Maximum message size for gRPC (in bytes)
    /// </summary>
    public int? MaxMessageSize { get; set; }

    /// <summary>
    /// Number of retry attempts for failed requests
    /// </summary>
    public int RetryCount { get; set; } = 3;

    /// <summary>
    /// Delay between retry attempts
    /// </summary>
    public TimeSpan RetryDelay { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Whether to use exponential backoff for retries
    /// </summary>
    public bool UseExponentialBackoff { get; set; } = true;
}
