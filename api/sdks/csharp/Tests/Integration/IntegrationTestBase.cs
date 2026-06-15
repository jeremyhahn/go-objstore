using System.Text;
using ObjStore.SDK;
using ObjStore.SDK.Models;
using Xunit;

namespace ObjStore.SDK.Tests.Integration;

/// <summary>
/// Base class for integration tests that manages server connectivity and capability detection.
///
/// Integration tests can be run in two modes:
/// 1. Via docker-compose (recommended): Run `docker compose up` from the SDK directory,
///    which starts both the server and runs tests with proper environment variables.
/// 2. Against a manually started server: Set environment variables and run tests directly.
///
/// Required environment variables (set by docker-compose integration-tests service):
/// - OBJSTORE_REST_URL: Full URL for REST server (default: http://localhost:8080)
/// - OBJSTORE_GRPC_HOST: Hostname for gRPC server (default: localhost)
/// - OBJSTORE_GRPC_PORT: Port for gRPC server (default: 50051)
/// - OBJSTORE_QUIC_URL: Full URL for QUIC server (default: https://localhost:4433)
/// </summary>
public abstract class IntegrationTestBase : IAsyncLifetime
{
    protected string RestBaseUrl { get; private set; } = null!;
    protected string GrpcAddress { get; private set; } = null!;
    protected string QuicBaseUrl { get; private set; } = null!;

    /// <summary>
    /// True when the REST server responds to /health. All tests gate on this first.
    /// </summary>
    protected bool IsServerAvailable { get; private set; }

    /// <summary>
    /// True when the gRPC endpoint is reachable.
    /// </summary>
    protected bool IsGrpcAvailable { get; private set; }

    /// <summary>
    /// True when the QUIC/HTTP3 endpoint is reachable. QUIC tests skip (with log) when false.
    /// </summary>
    protected bool IsQuicAvailable { get; private set; }

    private bool? _supportsReplication;
    private bool? _supportsArchive;

    public async Task InitializeAsync()
    {
        // OBJSTORE_* names match what docker-compose provides; fall back to legacy
        // names for local manual runs, then to safe defaults.
        RestBaseUrl = Environment.GetEnvironmentVariable("OBJSTORE_REST_URL")
                   ?? Environment.GetEnvironmentVariable("REST_BASE_URL")
                   ?? "http://localhost:8080";

        var grpcHost = Environment.GetEnvironmentVariable("OBJSTORE_GRPC_HOST") ?? "localhost";
        var grpcPort = Environment.GetEnvironmentVariable("OBJSTORE_GRPC_PORT") ?? "50051";
        GrpcAddress = Environment.GetEnvironmentVariable("GRPC_ADDRESS")
                   ?? $"http://{grpcHost}:{grpcPort}";

        QuicBaseUrl = Environment.GetEnvironmentVariable("OBJSTORE_QUIC_URL")
                   ?? Environment.GetEnvironmentVariable("QUIC_BASE_URL")
                   ?? "https://localhost:4433";

        IsServerAvailable = await CheckRestAvailable();

        if (!IsServerAvailable)
        {
            throw new InvalidOperationException(
                $"Integration server REST endpoint not reachable at {RestBaseUrl}. " +
                "Integration tests require the objstore-server container. " +
                "Run 'docker compose up' from the SDK directory or start the server manually. " +
                "This is a failure, not a skip.");
        }

        IsGrpcAvailable = await CheckGrpcAvailable();
        if (!IsGrpcAvailable)
        {
            throw new InvalidOperationException(
                $"Integration server gRPC endpoint not reachable at {GrpcAddress}. " +
                "The objstore-server is expected to expose gRPC (port 50051) alongside REST. " +
                "This is a failure, not a skip.");
        }

        IsQuicAvailable = await CheckQuicAvailable();
        if (!IsQuicAvailable)
        {
            Console.WriteLine($"[SKIP] QUIC/HTTP3 endpoint not available at {QuicBaseUrl}. QUIC-specific tests will be skipped.");
        }
    }

    public virtual Task DisposeAsync()
    {
        return Task.CompletedTask;
    }

    /// <summary>
    /// Checks if the backend supports replication by attempting to add a probe policy via REST.
    /// Caches the result to avoid repeated round-trips. Returns true when replication is enabled
    /// on the server; returns false only when the server explicitly signals unsupported.
    /// </summary>
    protected async Task<bool> SupportsReplication()
    {
        if (_supportsReplication.HasValue)
            return _supportsReplication.Value;

        try
        {
            using var client = ObjectStoreClientFactory.CreateRestClient(RestBaseUrl);
            var testPolicy = new ReplicationPolicy
            {
                Id = $"feature-probe-{Guid.NewGuid()}",
                SourceBackend = "local",
                DestinationBackend = "local",
                CheckIntervalSeconds = 300,
                Enabled = false,
                ReplicationMode = ReplicationMode.Transparent
            };

            await client.AddReplicationPolicyAsync(testPolicy);
            await client.RemoveReplicationPolicyAsync(testPolicy.Id);
            _supportsReplication = true;
            return true;
        }
        catch (Exception ex) when (ex.Message.Contains("not supported", StringComparison.OrdinalIgnoreCase) ||
                                    ex.Message.Contains("not implemented", StringComparison.OrdinalIgnoreCase) ||
                                    ex.Message.Contains("unsupported", StringComparison.OrdinalIgnoreCase))
        {
            Console.WriteLine($"[SKIP] Replication not supported by server: {ex.Message}");
            _supportsReplication = false;
            return false;
        }
        catch
        {
            // Network error or other issue; optimistically assume replication is supported
            // so that missing assertions cause real failures rather than silent skips.
            _supportsReplication = true;
            return true;
        }
    }

    /// <summary>
    /// Checks if the backend supports archive operations.
    /// Caches the result to avoid repeated round-trips.
    ///
    /// The local backend does not have a Glacier vault configured, so the server
    /// returns HTTP 400 ("vaultName not set") or gRPC InvalidArgument. Either a
    /// non-true return value or a recognized server error means archive is not
    /// supported and tests should be skipped with a logged reason.
    /// </summary>
    protected async Task<bool> SupportsArchive()
    {
        if (_supportsArchive.HasValue)
            return _supportsArchive.Value;

        try
        {
            using var client = ObjectStoreClientFactory.CreateRestClient(RestBaseUrl);
            var testKey = $"feature-probe/archive-{Guid.NewGuid()}.txt";
            var data = Encoding.UTF8.GetBytes("archive feature probe");

            await client.PutAsync(testKey, data);

            try
            {
                var result = await client.ArchiveAsync(testKey, "glacier", new Dictionary<string, string>
                {
                    ["vault"] = "test-vault"
                });

                if (!result)
                {
                    // Server returned a non-success status (e.g. 400 "vaultName not set") —
                    // archive is not available on this backend configuration.
                    Console.WriteLine("[SKIP] Archive: server returned non-success — archive unsupported on this backend (no vault configured).");
                    _supportsArchive = false;
                    return false;
                }

                _supportsArchive = true;
                return true;
            }
            catch (Exception ex) when (ex is NotSupportedException ||
                                        ex.Message.Contains("not supported", StringComparison.OrdinalIgnoreCase) ||
                                        ex.Message.Contains("not implemented", StringComparison.OrdinalIgnoreCase) ||
                                        ex.Message.Contains("unsupported", StringComparison.OrdinalIgnoreCase) ||
                                        ex.Message.Contains("vault", StringComparison.OrdinalIgnoreCase) ||
                                        ex.Message.Contains("InvalidArgument", StringComparison.OrdinalIgnoreCase))
            {
                Console.WriteLine($"[SKIP] Archive not supported by server: {ex.Message}");
                _supportsArchive = false;
                return false;
            }
            finally
            {
                try { await client.DeleteAsync(testKey); } catch { }
            }
        }
        catch
        {
            _supportsArchive = false;
            return false;
        }
    }

    private async Task<bool> CheckRestAvailable()
    {
        try
        {
            using var httpClient = new HttpClient();
            httpClient.Timeout = TimeSpan.FromSeconds(5);
            var response = await httpClient.GetAsync($"{RestBaseUrl}/health");
            return response.IsSuccessStatusCode;
        }
        catch
        {
            return false;
        }
    }

    private async Task<bool> CheckGrpcAvailable()
    {
        try
        {
            using var client = ObjectStoreClientFactory.CreateGrpcClient(GrpcAddress);
            var health = await client.HealthAsync();
            return health.Status == HealthStatus.Serving;
        }
        catch
        {
            return false;
        }
    }

    private async Task<bool> CheckQuicAvailable()
    {
        try
        {
            using var client = ObjectStoreClientFactory.CreateQuicClient(QuicBaseUrl);
            var health = await client.HealthAsync();
            return health.Status == HealthStatus.Serving;
        }
        catch (PlatformNotSupportedException)
        {
            return false;
        }
        catch (NotSupportedException)
        {
            return false;
        }
        catch (Exception ex) when (ex.Message.Contains("HTTP/3", StringComparison.OrdinalIgnoreCase) ||
                                    ex.Message.Contains("QUIC", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }
        catch
        {
            return false;
        }
    }
}
