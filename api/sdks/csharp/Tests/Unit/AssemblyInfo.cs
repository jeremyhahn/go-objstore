using Xunit;

// The gRPC client tests drive a real GrpcChannel over a mocked HTTP transport. Running test
// classes in parallel lets concurrent channels race on HTTP/2 protocol negotiation, which
// intermittently surfaces as "Response protocol downgraded to HTTP/1.1". The full unit suite
// runs in well under a second, so disabling cross-class parallelization keeps the gRPC
// transport mock deterministic.
[assembly: CollectionBehavior(DisableTestParallelization = true)]
