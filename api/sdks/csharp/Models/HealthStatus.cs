namespace ObjStore.SDK.Models;

/// <summary>
/// Health status enumeration
/// </summary>
public enum HealthStatus
{
    Unknown = 0,
    Serving = 1,
    NotServing = 2
}

/// <summary>
/// Health check response
/// </summary>
public class HealthResponse
{
    /// <summary>
    /// Health status
    /// </summary>
    public HealthStatus Status { get; set; }

    /// <summary>
    /// Optional message
    /// </summary>
    public string? Message { get; set; }
}
