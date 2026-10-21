# Configuration: Engineering

> **Status: NORMATIVE** · Scope: go-objstore · Last verified: 2026-10-09
> Conflicts with this document are defects. Fix them here first.

Implementation contract for `internal/serverconfig`. Design is in [architecture.md](architecture.md); the keys are in [servers.md](servers.md).

## Exported surface

| Symbol | File | Role |
|---|---|---|
| `Config` | `config.go` | Root struct: `GRPCConfig`, `RESTConfig`, `QUICConfig`, `MCPConfig`, `UnixConfig`, `RateLimitConfig`, `AuditConfig`, plus backend and path. |
| `Defaults`, `RESTServerDefaults`, `QUICServerDefaults` | `config.go` | Defaults layers for the combined binary and the standalone REST and QUIC binaries. |
| `Config.RESTAddr`, `Config.MCPMode`, `Config.Validate` | `config.go` | Joined REST address (IPv6 literals bracketed), MCP mode, cross-field validation. |
| `Load(name, args, defaults, bind)` | `loader.go` | Resolves flags, environment and file into a validated `*Config`. |
| `BindFunc` | `loader.go` | Registers a binary's flags on a `flag.FlagSet`. |
| `EnvPrefix`, `EnvVar` | `loader.go` | `OBJSTORE_` and key-to-variable mapping (upper case, `.` to `_`). |
| `BindServerFlags`, `BindGRPCServerFlags`, `BindRESTServerFlags`, `BindQUICServerFlags`, `BindMCPServerFlags` | `flags.go` | Per-binary flag sets. |

## Invariants

- Precedence is flags > environment > file > defaults, in every binary.
- A flag counts only when set on the command line.
- `-config` wins over `OBJSTORE_CONFIG`. A named file must exist; an absent file is an error, an unnamed one is not.
- Unknown keys and mistyped values are rejected before any transport starts.
- `Load` returns only a validated `Config`.

## Error semantics

| Error | Returned when |
|---|---|
| `*FlagError` | Command-line parse failure. |
| `*ConfigFileError` | The named file cannot be read or parsed. |
| `*DecodeError` | Unknown key or mistyped value. |
| `*InvalidSettingError` | A validation failure; several are joined so all are reported at once (port outside 0-65535, address not `host:port`, TLS certificate without key or key without certificate, MCP mode other than `stdio` or `http`). `Validate` does not require a QUIC key pair or `self_signed`: with neither, it passes, the combined server logs a warning and skips QUIC, and the standalone QUIC server exits in `main`. |
| `*TLSCredentialsError` | A configured TLS key pair cannot be loaded; the transport does not start. |
| `flag.ErrHelp` | `-h` or `-help`, returned unwrapped. |

All fail closed: the binary exits with a descriptive message and no transport runs on partial configuration.

## Configuration

Keys, environment variables, defaults and flags: [servers.md](servers.md). A commented template ships as `configs/objstore-server.yaml.example`; real `configs/*.yaml` files are git-ignored.

## Test strategy

Unit tests live beside the code (`config_test.go`, `transport_test.go`, `errors_test.go`) and cover precedence, unknown keys, validation errors and the typed errors. Per-transport behavior is covered by the integration targets in the `Makefile`.

## Observability

Startup logs report the selected backend and storage path, each enabled transport with its listen address, the replication policy file location and backend initialization errors. Audit records go to standard output, or standard error when `mcp.mode` is `stdio`.
