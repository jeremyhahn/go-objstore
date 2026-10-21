# Usage: Engineering

> **Status: NORMATIVE** · Scope: go-objstore · Last verified: 2026-10-09
> Conflicts with this document are defects. Fix them here first.

Implementation contract of the `pkg/objstore` facade, the library entry point. Design is in [architecture.md](architecture.md).

## Exported surface

| Symbol | Role |
|---|---|
| `FacadeConfig` | `BackendConfigs` (name to `BackendConfig{Type, Settings}`), pre-built `Backends`, `DefaultBackend`. |
| `Initialize(*FacadeConfig) error` | One-time setup. Builds backends with `factory.NewStorage`, merges pre-built `Backends` (they win on a name clash). |
| `Reset()`, `IsInitialized()` | Clear (tests) and query the singleton. |
| `Backend(name)`, `DefaultBackend()`, `Backends()` | Look up backends. |
| `Put`, `Get`, `Delete`, `List` | Default-backend calls on a plain key or prefix. |
| `PutWithContext`, `PutWithMetadata`, `GetWithContext`, `GetMetadata`, `UpdateMetadata`, `DeleteWithContext`, `Exists`, `ListWithContext`, `Archive` | Take a key reference (`key` or `backend:key`). |
| `ListWithOptions(ctx, backendName, *common.ListOptions)` | Paginated listing with metadata. |
| `AddPolicy`, `RemovePolicy`, `GetPolicies` | Lifecycle policies on a named backend (empty name means default). |
| `GetReplicationManager`, `EnableReplication`, `ReplicationConfig` | Replication on a backend that supports it. |

## Invariants

- `Initialize` takes effect once per process (`sync.Once`); later calls return the first call's error (wrapping `ErrInitializationFailed`) and the facade stays uninitialized until `Reset`. With no `DefaultBackend` set, one of the configured backends is chosen, so set it explicitly when there is more than one.
- A key reference splits on the first colon: `backend:key`. A bare key uses the default backend.
- Keys, prefixes, key references and backend names are validated before any backend call; metadata `Custom` values go through `common.ValidateMetadata`.
- The facade guards its backend map with a read-write mutex and is safe for concurrent use.
- `Get` returns an `io.ReadCloser` the caller must close.

## Error semantics

| Error | Returned when |
|---|---|
| `ErrNotInitialized` | A call before `Initialize`. |
| `ErrInitializationFailed` | Every `Initialize` call after a failed first one (wraps the first error). |
| `ErrNoDefaultBackend` | No default backend is set. |
| `ErrBackendNotFound` | A named backend is not registered (wrapped, so test with `errors.Is`). |
| `common.ErrReplicationNotSupported` | The backend does not support replication. |
| Wrapped validation errors (`invalid key`, `invalid key reference`, `invalid prefix`, `invalid backend name`, `invalid metadata`) | Input rejected before the backend is called. |

Initialization fails with an error for a nil config, no backends, a backend that cannot be created, or a default backend that is not registered.

## Configuration

Backend `Settings` keys per type: [../configuration/storage-backends.md](../configuration/storage-backends.md). Replication defaults: policy file `.replication-policies.json`, interval 5 minutes, no-op logger and audit logger when unset; background sync runs only when `RunInBackground` is set.

## Test strategy

Unit tests sit beside the code (`pkg/objstore/facade_test.go`, `coverage_gaps_test.go`); `facade_integration_test.go` drives real backends. Server and CLI parity is covered by the `Makefile` integration targets. Call `Reset` between tests.

## Observability

The facade does not log. Server transports emit audit records; replication takes the caller's `adapters.Logger` and `auditlog.AuditLogger` (package `pkg/auditlog`).
