# Server Configuration (file, environment, flags)

> **Status: NORMATIVE** · Scope: go-objstore · Last verified: 2026-10-09
> Conflicts with this document are defects. Fix them here first.

Every server binary — `objstore-server`, `objstore-grpc-server`,
`objstore-rest-server`, `objstore-quic-server` and `objstore-mcp-server` —
resolves its settings through one configuration layer
(`internal/serverconfig`, built on viper). Every setting, including every
listen host, port, address and socket path, can be set three ways:

| Layer | How | Example (REST bind host) |
|-------|-----|--------------------------|
| Flag | the binary's command-line flag | `objstore-server -rest-host 10.0.0.5` |
| Environment | `OBJSTORE_` + key path, upper case, `.` → `_` | `OBJSTORE_REST_HOST=10.0.0.5` |
| Config file | YAML named by `-config <file>` or `OBJSTORE_CONFIG` | `rest:` / `  host: 10.0.0.5` |

Precedence is **flags > environment > config file > built-in defaults**. A
flag overrides the lower layers only when it is given on the command line, so a
flag's default never masks a value from the environment or the file. `-config`
wins over `OBJSTORE_CONFIG`. The file is optional; when one is named it must
exist, and unknown keys are rejected at startup, as are invalid values
(for example a port outside 0-65535, an address that is not `host:port`, a TLS
certificate without its key or a key without its certificate, a gRPC message
limit outside 1048576-2147483647 bytes, or an MCP mode
other than `stdio`/`http`). A configured TLS key pair that cannot be loaded
stops that transport; it never falls back to plaintext.

`rest.host` may be an IPv6 literal without brackets (`::1`); the listener
address is joined as `[::1]:8080`. Every problem is reported at once.

The built-in defaults of the listen addresses are the transport packages' own
defaults (`restserver.DefaultServerConfig()`, `grpcserver.DefaultServerOptions()`,
`quicserver.DefaultOptions()`, `mcpserver.DefaultServerConfig()`,
`unixserver.DefaultConfig()`), so the value lives in one place.

A commented template of every key and its default ships as
[`configs/objstore-server.yaml.example`](../../configs/objstore-server.yaml.example).
Each binary reads the sections for the transports it serves.

## Keys

The defaults are safe to start as is: there is no default storage path, so
the `local` backend refuses to start until `path` is set; gRPC, REST and MCP
listen on loopback only; QUIC and MCP are off. To serve other machines, set
the address explicitly (for example `-grpc-addr 0.0.0.0:50051`,
`-rest-host 0.0.0.0`) and enable QUIC or MCP with `-quic=true` or
`-mcp=true`. The single-transport binaries keep their own defaults where the
table notes one.

| Key | Environment | Default | objstore-server flag | Single-transport flag |
|-----|-------------|---------|----------------------|-----------------------|
| `backend` | `OBJSTORE_BACKEND` | `local` | `-backend` | `-backend` |
| `path` | `OBJSTORE_PATH` | none, required for the `local` backend (`./data` for objstore-quic-server) | `-path` | `-path` |
| `grpc.enabled` | `OBJSTORE_GRPC_ENABLED` | `true` | `-grpc` | — |
| `grpc.addr` | `OBJSTORE_GRPC_ADDR` | `127.0.0.1:50051` | `-grpc-addr` | `-addr` (grpc) |
| `grpc.tls_cert` | `OBJSTORE_GRPC_TLS_CERT` | — | `-grpc-tls-cert` | `-tls-cert` (grpc) |
| `grpc.tls_key` | `OBJSTORE_GRPC_TLS_KEY` | — | `-grpc-tls-key` | `-tls-key` (grpc) |
| `grpc.max_recv_msg_size` | `OBJSTORE_GRPC_MAX_RECV_MSG_SIZE` | `33554432` | `-grpc-max-recv-msg-size` | `-max-recv-msg-size` (grpc) |
| `grpc.max_send_msg_size` | `OBJSTORE_GRPC_MAX_SEND_MSG_SIZE` | `10485760` | `-grpc-max-send-msg-size` | `-max-send-msg-size` (grpc) |
| `rest.enabled` | `OBJSTORE_REST_ENABLED` | `true` | `-rest` | — |
| `rest.host` | `OBJSTORE_REST_HOST` | `127.0.0.1` (`localhost` for objstore-rest-server) | `-rest-host` | `-host` (rest) |
| `rest.port` | `OBJSTORE_REST_PORT` | `8080` | `-rest-port` | `-port` (rest) |
| `rest.metrics_public` | `OBJSTORE_REST_METRICS_PUBLIC` | `false` | `-metrics-public` | `-metrics-public` (rest) |
| `quic.enabled` | `OBJSTORE_QUIC_ENABLED` | `false` | `-quic` | — |
| `quic.addr` | `OBJSTORE_QUIC_ADDR` | `127.0.0.1:4433` | `-quic-addr` | `-addr` (quic) |
| `quic.tls_cert` | `OBJSTORE_QUIC_TLS_CERT` | — | `-quic-tls-cert` | `-tlscert` (quic) |
| `quic.tls_key` | `OBJSTORE_QUIC_TLS_KEY` | — | `-quic-tls-key` | `-tlskey` (quic) |
| `quic.self_signed` | `OBJSTORE_QUIC_SELF_SIGNED` | `false` | `-quic-self-signed` | `-selfsigned` (quic) |
| `quic.max_body_size` | `OBJSTORE_QUIC_MAX_BODY_SIZE` | `104857600` | — | `-maxbodysize` (quic) |
| `quic.read_timeout` | `OBJSTORE_QUIC_READ_TIMEOUT` | `30s` | — | `-readtimeout` (quic) |
| `quic.write_timeout` | `OBJSTORE_QUIC_WRITE_TIMEOUT` | `30s` | — | `-writetimeout` (quic) |
| `quic.idle_timeout` | `OBJSTORE_QUIC_IDLE_TIMEOUT` | `60s` | — | `-idletimeout` (quic) |
| `quic.max_streams` | `OBJSTORE_QUIC_MAX_STREAMS` | `100` | — | `-maxstreams` (quic) |
| `mcp.enabled` | `OBJSTORE_MCP_ENABLED` | `false` | `-mcp` | — |
| `mcp.mode` | `OBJSTORE_MCP_MODE` | `http` | `-mcp-mode` | `-mode` (mcp) |
| `mcp.addr` | `OBJSTORE_MCP_ADDR` | `127.0.0.1:8081` | `-mcp-addr` | `-addr` (mcp) |
| `unix.enabled` | `OBJSTORE_UNIX_ENABLED` | `false` | `-unix` | — |
| `unix.socket` | `OBJSTORE_UNIX_SOCKET` | `/var/run/objstore.sock` | `-unix-socket` | — |
| `rate_limit.enabled` | `OBJSTORE_RATE_LIMIT_ENABLED` | `false` | `-rate-limit` | `-rate-limit` (all) |
| `rate_limit.rps` | `OBJSTORE_RATE_LIMIT_RPS` | `100` | `-rate-limit-rps` | `-rate-limit-rps` (all) |
| `rate_limit.burst` | `OBJSTORE_RATE_LIMIT_BURST` | `200` | `-rate-limit-burst` | `-rate-limit-burst` (all) |
| `rate_limit.per_client` | `OBJSTORE_RATE_LIMIT_PER_CLIENT` | `false` | `-rate-limit-per-client` | `-rate-limit-per-client` (all) |
| `audit.enabled` | `OBJSTORE_AUDIT_ENABLED` | `true` | `-audit` | `-audit` (all) |
| `replication.allow_client_local_backends` | `OBJSTORE_REPLICATION_ALLOW_CLIENT_LOCAL_BACKENDS` | `false` | `-replication-allow-client-local-backends` | `-replication-allow-client-local-backends` (all) |

Rate limiting and audit logging apply to every transport a binary serves,
standalone or combined, with one audit logger per process. Audit records go to
standard output, or to standard error when `mcp.mode` is `stdio`, where standard
output carries the JSON-RPC stream.

A replication policy's source and destination backend type and settings come
from the client, so a backend whose settings take a filesystem path (`local`)
could read or write any directory the service user can reach. By default the
server refuses such a policy: adding one answers invalid argument (HTTP 400 on
REST and QUIC, `InvalidArgument` on gRPC, a tool error on MCP), and a
replication policy file that already holds one fails to load, with an error
naming the policy ID. Set `replication.allow_client_local_backends: true` only
when every client that may add a policy is trusted with the service user's
filesystem access.

`objstore-server` applies the QUIC body-size, timeout and stream-limit keys too;
they have no flag on that binary but are set through the file or environment.

The server prefix is `OBJSTORE_`. The `objstore` CLI keeps its own
`OBJECTSTORE_*` variables; see [CLI Configuration](cli.md).
