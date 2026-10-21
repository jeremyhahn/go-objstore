# Usage: Architecture

> **Status: NORMATIVE** · Scope: go-objstore · Last verified: 2026-10-09
> Conflicts with this document are defects. Fix them here first.

How applications consume go-objstore: as a Go library, through the `objstore` CLI, or through a server binary. Deeper design is in [../architecture/](../architecture/README.md).

## Shape

| Mode | Entry point | Use when |
|---|---|---|
| Library facade | `pkg/objstore` (`Initialize`, `Put`, `Get`, ...) | The application owns the process and wants one validated entry point over named backends. |
| Backend factory | `factory.NewStorage(type, settings)` | The application wants a `common.Storage` directly, or builds its own facade `Backends` map. |
| CLI | `cmd/objstore` | Scripts and operators. |
| Servers | `objstore-server` and the single-transport binaries | Other processes reach storage over gRPC, REST, QUIC, MCP or a Unix socket. |
| C API | [../c_client/](../c_client/README.md) | C/C++ callers. |

Every mode ends at the same `common.Storage` interface, so a backend behaves the same behind every entry point.

## Data flow

1. The caller supplies a key, or a `backend:key` reference naming a backend (a bare key uses the default backend).
2. The facade validates the key, prefix or backend name (`validation` package) before any backend call.
3. The facade resolves the backend and calls its `common.Storage` method.
4. The backend talks to its store (local filesystem, S3, GCS, Azure, MinIO, Glacier); see [../backends/](../backends/README.md).

The facade is a process-wide singleton guarded by a mutex; `Initialize` runs once, `Reset` clears it for tests.

## Decisions

| Chosen | Rejected | Reason |
|---|---|---|
| Facade over named backends with `backend:key` references | Callers hold backends and route themselves | One place validates input and routes; applications stay backend-agnostic. |
| Validation in the facade | Validation in each backend | A backend cannot be reached with an unchecked key through the facade. |
| Factory remains public | Facade only | Embedders such as servers and tests need a bare `common.Storage`. |

## Trust boundaries

Cloud credentials come from the caller's environment or settings and are never logged or stored in object metadata. Server transports add their own authentication; see [../configuration/](../configuration/README.md).

## Does not do

- No cross-backend transactions.
- No runtime configuration reload.
