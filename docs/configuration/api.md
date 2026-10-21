# Configuration: API

> **Status: NORMATIVE** · Scope: go-objstore · Last verified: 2026-10-09
> Conflicts with this document are defects. Fix them here first.

Configuration has no endpoint, RPC or frame on any transport; it is read at startup and is not changeable at runtime. Its programmatic surface is Go only.

| Form | Reference |
|---|---|
| Server flags, `OBJSTORE_*` variables, YAML keys | [servers.md](servers.md) |
| `objstore` CLI file and `OBJECTSTORE_*` variables | [cli.md](cli.md) |
| REST server options (`restserver.ServerConfig`, `restserver.DefaultServerConfig()`) | [rest-server.md](rest-server.md) |
| gRPC server options (`grpcserver.DefaultServerOptions()`) | [grpc-server.md](grpc-server.md) |
| QUIC server options (`quicserver.DefaultOptions()`) | [quic-server.md](quic-server.md) |
| MCP server options (`mcpserver.DefaultServerConfig()`) | [mcp-server.md](mcp-server.md) |
| Unix socket options (`unixserver.DefaultConfig()`) | [servers.md](servers.md) |
| Backends: `objstore.FacadeConfig`, `objstore.BackendConfig` | [storage-backends.md](storage-backends.md) |
| Encryption | [encryption.md](encryption.md) |
| Lifecycle policies | [lifecycle.md](lifecycle.md) |

Lifecycle and replication policies are runtime data, not startup configuration. They are managed through each transport's API; the REST routes are listed in [rest-server.md](rest-server.md).

Versioning: server keys and flags are pre-1.0 (`-alpha` releases) and may change between releases; changes are recorded in `CHANGELOG.md`.
