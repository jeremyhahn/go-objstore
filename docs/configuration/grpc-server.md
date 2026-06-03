# gRPC Server Configuration

Configuration reference for the gRPC server.

The gRPC server is configured with command-line flags. There is no configuration
file. Two binaries serve gRPC:

- `objstore-grpc-server` - standalone gRPC server
- `objstore-server` - combined server (gRPC + REST + QUIC + MCP)

## Standalone Server Flags (objstore-grpc-server)

| Flag | Default | Description |
|------|---------|-------------|
| `--addr` | `:50051` | gRPC server address |
| `--backend` | `local` | Storage backend (`local`, `s3`, `gcs`, `azure`) |
| `--path` | `/tmp/objstore` | Storage path for the local backend |
| `--tls-cert` | (none) | TLS certificate file |
| `--tls-key` | (none) | TLS key file |

```bash
objstore-grpc-server --addr :50051 --backend local --path /var/lib/objstore

# With TLS
objstore-grpc-server \
  --addr :50051 \
  --tls-cert /etc/objstore/tls/server.crt \
  --tls-key /etc/objstore/tls/server.key
```

TLS is enabled when both `--tls-cert` and `--tls-key` are provided; otherwise
the server listens in plaintext.

## Combined Server Flags (objstore-server)

| Flag | Default | Description |
|------|---------|-------------|
| `--grpc` | `true` | Enable the gRPC server |
| `--grpc-addr` | `:50051` | gRPC server address |
| `--rate-limit` | `false` | Enable rate limiting on all transports |
| `--rate-limit-rps` | `100` | Rate limit requests per second |
| `--rate-limit-burst` | `200` | Rate limit burst size |
| `--audit` | `true` | Enable audit logging on all transports |

## Built-in Defaults

The server runs with these defaults (`pkg/server/grpc` options):

- `address`: `:50051`
- `max_receive_message_size`: 10MB (10485760 bytes)
- `max_send_message_size`: 10MB (10485760 bytes)
- Server reflection: disabled
- Health check service: registered (see below)

## Health Checks

The standard gRPC health service (`grpc.health.v1.Health`) is always
registered and reports `SERVING` for the server and the
`objstore.ObjectStore` service. Compatible with `grpc_health_probe`:

```bash
grpc_health_probe -addr=localhost:50051
```

## Advanced Settings (Programmatic)

The binaries do not expose flags for message limits, mTLS, reflection, or auth
adapters. When embedding the server, configure these through
`grpcserver.ServerOption` values (see `pkg/server/grpc`):

```go
server, err := grpcserver.NewServer(
    grpcserver.WithAddress(":50051"),
    grpcserver.WithMaxMessageSize(10*1024*1024),
    grpcserver.WithReflection(true), // enables grpcurl and service discovery
    grpcserver.WithRateLimit(true, rateLimitConfig),
)
```

mTLS and custom authentication are configured through the TLS and adapter
options in the same package.
