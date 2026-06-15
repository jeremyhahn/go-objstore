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
    /// The protocol to use (REST, GRPC, QUIC, MCP, Unix)
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

    /// <summary>
    /// Maximum number of bytes PutStreamAsync may buffer in memory for
    /// transports without true streaming (MCP, Unix). Streams larger than this
    /// throw an OperationFailedException; use the REST or gRPC transports for
    /// large objects. Default: 64 MiB.
    /// </summary>
    public long MaxBufferSize { get; set; } = 64L * 1024 * 1024;

    // ---------------------------------------------------------------- auth / multi-tenancy

    /// <summary>
    /// Bearer token transmitted as Authorization: Bearer &lt;Token&gt;.
    /// Applies to REST, QUIC, and MCP transports via HTTP header; to gRPC via call metadata.
    /// Not used for the Unix transport (authentication is peer-credential based on the server side).
    /// </summary>
    public string? Token { get; set; }

    /// <summary>
    /// Additional HTTP headers forwarded verbatim with every request.
    /// Applies to REST, QUIC, and MCP transports. For gRPC each entry becomes a metadata entry.
    /// </summary>
    public IDictionary<string, string>? Headers { get; set; }

    /// <summary>
    /// Tenant identifier transmitted as X-Tenant-ID.
    /// Applies to REST, QUIC, and MCP transports via HTTP header; to gRPC via call metadata.
    /// </summary>
    public string? TenantId { get; set; }

    // ---------------------------------------------------------------- TLS (gRPC)

    /// <summary>
    /// When true, TLS certificate validation is skipped. For development / self-signed certs only.
    /// Applies to gRPC channels created by the factory and DI helpers.
    /// </summary>
    public bool AllowInsecureTls { get; set; }

    /// <summary>
    /// Path to a PEM-encoded CA certificate file used for gRPC TLS verification.
    /// When set, the certificate is loaded and added as a trusted root.
    /// </summary>
    public string? CaCertificatePath { get; set; }
}
