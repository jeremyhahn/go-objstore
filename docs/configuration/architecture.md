# Configuration: Architecture

> **Status: NORMATIVE** · Scope: go-objstore · Last verified: 2026-10-09
> Conflicts with this document are defects. Fix them here first.

How go-objstore components receive their settings. Key and flag tables are in [servers.md](servers.md), [cli.md](cli.md) and the per-component pages listed in [README.md](README.md).

## Shape

Three consumers, three entry points:

| Consumer | Entry point | Layers |
|---|---|---|
| Server binaries (`objstore-server`, `objstore-grpc-server`, `objstore-rest-server`, `objstore-quic-server`, `objstore-mcp-server`) | `internal/serverconfig` (`Load`) | flags > `OBJSTORE_*` environment > YAML file > defaults |
| `objstore` CLI | its own config loader | flags > `OBJECTSTORE_*` environment > `.objstore.yaml` > defaults |
| Embedding applications | Go structs and functional options in `pkg/server/*`, `pkg/objstore` | whatever the caller builds |

Server binaries share one layer so a setting means the same thing on every binary. Each binary binds only the flags for the transports it serves (`BindServerFlags`, `BindGRPCServerFlags`, `BindRESTServerFlags`, `BindQUICServerFlags`, `BindMCPServerFlags`).

## Data flow

1. The binary builds a defaults `Config` (the transport packages' own `Default*` values, so each default lives in one place).
2. `Load` registers the binary's flags, parses the arguments, reads the optional YAML file and the environment, and merges the layers.
3. The merged `Config` is validated; every problem is reported at once.
4. The binary starts each enabled transport from its section of the `Config`.

Servers read configuration once at startup. Changing a setting needs a restart. Lifecycle and replication policies are runtime data managed through the API, not startup configuration.

## Decisions

| Chosen | Rejected | Reason |
|---|---|---|
| One shared loader for all server binaries | A parser per binary | Divergent precedence and key names between binaries. |
| Flag overrides lower layers only when set on the command line | Flag defaults always win | A flag default would mask the environment and file. |
| Unknown file keys are an error | Ignore unknown keys | A misspelled key would silently run on defaults. |
| Programmatic options for TLS, auth adapters, timeouts | Expose every option as a flag | The flag surface stays small; embedders get the full set. |

## Trust boundaries

- Secrets (cloud credentials, key material) are supplied by environment or provider credential chains, not committed files. Credentials are never logged or stored in object metadata.
- A configured TLS key pair that cannot be loaded stops that transport. It never falls back to plaintext.

## Does not do

- No hot reload.
- No remote configuration source; the file is local.
- The server loader does not read `OBJECTSTORE_*` and the CLI does not read `OBJSTORE_*`.
