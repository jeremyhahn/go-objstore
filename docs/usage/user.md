# Usage: Getting Things Done

> **Status: NORMATIVE** · Scope: go-objstore · Last verified: 2026-10-09
> Conflicts with this document are defects. Fix them here first.

Task-oriented guide to storing and retrieving objects with go-objstore.

## Prerequisites

- Go, to use the library or build the CLI.
- A backend: the local filesystem for development, or S3, GCS, Azure or MinIO with credentials from the provider's usual sources.

## First run

From an application:

```go
import "github.com/jeremyhahn/go-objstore/pkg/objstore"

err := objstore.Initialize(&objstore.FacadeConfig{
    BackendConfigs: map[string]objstore.BackendConfig{
        "default": {Type: "local", Settings: map[string]string{"path": "/var/lib/objstore"}},
    },
    DefaultBackend: "default",
})
```

From a shell:

```bash
go install ./cmd/objstore
echo "hello" | objstore put - hello.txt
objstore get hello.txt
```

## Common tasks

| Task | Library | CLI |
|---|---|---|
| Store | `objstore.Put(key, reader)` | `objstore put <source-file> <destination-key>` |
| Fetch | `objstore.Get(key)`, then close the reader | `objstore get <key> [output-file]` (stdout when omitted) |
| List | `objstore.List(prefix)` | `objstore list <prefix>` |
| Delete | `objstore.Delete(key)` | `objstore delete <key>` |
| Use a specific backend | key reference `backend:key` | see [cli.md](cli.md) |
| Archive | `objstore.Archive(keyRef, archiver)` | `objstore archive <key> <type>` |

More: [getting-started.md](getting-started.md), [storage-backends.md](storage-backends.md), [cli.md](cli.md), [deployment.md](deployment.md). Settings for each backend and server: [../configuration/](../configuration/README.md). Working programs are in `examples/`.

## Run a server

```bash
objstore-server -backend local -path /var/lib/objstore
```

This serves gRPC, REST, QUIC and MCP; see [../configuration/servers.md](../configuration/servers.md) for ports and how to switch transports off.

## Troubleshooting

| Symptom | Fix |
|---|---|
| `not initialized` error | Call `objstore.Initialize` once at startup before any other call. |
| `backend not found` | The name before the colon in `backend:key` is not a configured backend. |
| `invalid key` or `invalid prefix` | The key was rejected by validation; see the message for the rule. |
| Reads leak file handles or connections | Close the reader returned by `Get`. |
| Cloud call fails with access denied | Check the credentials with the provider's own tool; objstore does not log them. |
