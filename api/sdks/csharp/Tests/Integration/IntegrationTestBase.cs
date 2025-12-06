using System.Diagnostics;
using Xunit;

namespace ObjStore.SDK.Tests.Integration;

/// <summary>
/// Base class for integration tests that manages server connectivity.
///
/// Integration tests can be run in two modes:
/// 1. Via docker-compose (recommended): Run `docker compose -f docker-compose.test.yml up`
///    which starts both the server and runs tests with proper environment variables.
/// 2. Against a manually started server: Set environment variables and run tests directly.
///
/// Required environment variables:
/// - REST_BASE_URL: URL for REST server (default: http://localhost:8080)
/// - GRPC_ADDRESS: Address for gRPC server (default: http://localhost:9090)
/// - QUIC_BASE_URL: URL for QUIC server (default: https://localhost:8443)
/// </summary>
public abstract class IntegrationTestBase : IAsyncLifetime
{
    protected string RestBaseUrl { get; private set; } = null!;
    protected string GrpcAddress { get; private set; } = null!;
    protected string QuicBaseUrl { get; private set; } = null!;

    protected bool IsServerAvailable { get; private set; }

    public async Task InitializeAsync()
    {
        // Read URLs from environment variables (set by docker-compose or manually)
        RestBaseUrl = Environment.GetEnvironmentVariable("REST_BASE_URL") ?? "http://localhost:8080";
        GrpcAddress = Environment.GetEnvironmentVariable("GRPC_ADDRESS") ?? "http://localhost:9090";
        QuicBaseUrl = Environment.GetEnvironmentVariable("QUIC_BASE_URL") ?? "https://localhost:8443";

        // Check if server is available
        IsServerAvailable = await CheckServerAvailable();

        if (!IsServerAvailable)
        {
            Console.WriteLine($"Warning: Server not available at {RestBaseUrl}. Integration tests will be skipped.");
            Console.WriteLine("To run integration tests, either:");
            Console.WriteLine("  1. Run 'docker compose -f docker-compose.test.yml up' from the SDK directory");
            Console.WriteLine("  2. Start the go-objstore server manually and set REST_BASE_URL environment variable");
        }
    }

    public Task DisposeAsync()
    {
        // Nothing to dispose - we don't manage the server lifecycle
        return Task.CompletedTask;
    }

    private async Task<bool> CheckServerAvailable()
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
}
