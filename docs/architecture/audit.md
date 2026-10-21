# Audit logging

Audit logging is split across two packages so that importing a storage backend
never links the server-side stacks.

| Package | Contents | Dependencies |
|---------|----------|--------------|
| `pkg/auditlog` | The contract: `AuditLogger`, `AuditEvent`, `EventType` and its constants, `Result` and its constants, and the no-op default (`NoOpAuditLogger`, `NewNoOpAuditLogger`). | Standard library and `pkg/adapters` only. |
| `pkg/audit` | The implementation: the slog-backed `DefaultAuditLogger` (`NewAuditLogger`, `NewDefaultAuditLogger`, `Config`, `OutputFormat`), the gin middleware, the gRPC interceptors, the `net/http` middleware and `LogRPC`. | gin, gRPC, uuid and `pkg/server/middleware`. |

## Backends

A backend that supports auditing (today `pkg/local`) imports `pkg/auditlog`
only. It holds an `auditlog.AuditLogger`, defaulting to
`auditlog.NewNoOpAuditLogger()`, and the consumer injects a real logger with
`SetAuditLogger`:

```go
import (
    "github.com/jeremyhahn/go-objstore/pkg/audit"
    "github.com/jeremyhahn/go-objstore/pkg/local"
)

store := local.New().(*local.Local)
store.SetAuditLogger(audit.NewDefaultAuditLogger())
```

A consumer that does not use audit never links `pkg/audit`, and with it gin,
the mongo bson codec or ugorji.

## Servers and replication

The REST, gRPC, QUIC, unix and MCP servers, and `pkg/replication`, name the
types through `pkg/auditlog` (`auditlog.AuditLogger`, `auditlog.EventObjectCreated`,
`auditlog.ResultSuccess`, ...) and obtain the implementation and middleware from
`pkg/audit`. Behaviour is unchanged.

## Pinned by a test

`internal/depguard` runs `go list -deps` for every backend package (`local`,
`memory`, `s3`, `gcs`, `azure`, `glacier`, `azurearchive`, `minio`) and fails if
the graph includes `github.com/gin-gonic/gin`, `go.mongodb.org/mongo-driver`,
`github.com/ugorji/go`, `pkg/audit` or `pkg/server`.
