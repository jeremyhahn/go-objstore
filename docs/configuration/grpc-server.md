# gRPC Server Configuration

> **Status: NORMATIVE** · Scope: go-objstore · Last verified: 2026-10-09
> Conflicts with this document are defects. Fix them here first.

Configuration reference for the gRPC server.

The gRPC server is configured with command-line flags, `OBJSTORE_*` environment
variables and an optional YAML file (precedence: flags > environment > file >
defaults); see [Server Configuration](servers.md) for the config keys and
environment variables behind every flag below. Two binaries serve gRPC:

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
| `--max-recv-msg-size` | `33554432` (32 MiB) | Largest message the server accepts, in bytes |
| `--max-send-msg-size` | `10485760` (10 MiB) | Largest message the server sends, in bytes |

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
| `--grpc-tls-cert` | — | gRPC TLS certificate file (set together with `--grpc-tls-key`) |
| `--grpc-tls-key` | — | gRPC TLS key file (set together with `--grpc-tls-cert`) |
| `--grpc-max-recv-msg-size` | `33554432` (32 MiB) | Largest message the gRPC server accepts, in bytes |
| `--grpc-max-send-msg-size` | `10485760` (10 MiB) | Largest message the gRPC server sends, in bytes |
| `--rate-limit` | `false` | Enable rate limiting on all transports |
| `--rate-limit-rps` | `100` | Rate limit requests per second |
| `--rate-limit-burst` | `200` | Rate limit burst size |
| `--audit` | `true` | Enable audit logging on all transports |

## Built-in Defaults

The server runs with these defaults (`pkg/server/grpc` options):

- `address`: `:50051`
- `max_receive_message_size`: 32 MiB (33554432 bytes), twice the backup agent's
  default 16 MiB pack, so a pack upload fits with its framing
- `max_send_message_size`: 10 MiB (10485760 bytes); `Get` streams objects in
  64 KiB chunks, so no single response approaches it

Both limits are settable as `grpc.max_recv_msg_size` and
`grpc.max_send_msg_size` (see [servers.md](servers.md)). A value below 1 MiB
(1048576) or above 2147483647 is a configuration error.
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

The binaries do not expose flags for mTLS, reflection, or auth
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
