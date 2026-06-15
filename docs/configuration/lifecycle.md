# Lifecycle Policy Configuration

Configuration reference for automatic data retention and archival policies.

Lifecycle policies are not configured through a config file. They are managed
at runtime through the CLI, the server APIs (REST, gRPC, QUIC, MCP), or the Go
API, and persist in a policy file alongside the storage path.

## Policy Parameters

### Required Fields
- `id` - Unique policy identifier
- `action` - Action to perform (`delete` or `archive`)
- Retention - How long to keep objects (days in the CLI, `retention_seconds` in the APIs)

### Optional Fields
- `prefix` - Object key prefix to match (empty matches all objects)
- `destination_type` / `destination_settings` - Archive backend for the `archive` action

## Policy Actions

### Delete Action
Objects older than the retention period are permanently deleted.

```bash
objstore policy add delete-temp-files temp/ 1 delete
```

### Archive Action
Objects older than the retention period are moved to an archive backend.

```bash
objstore policy add archive-old-data data/ 90 archive
```

## Managing Policies with the CLI

```bash
# Add a policy: objstore policy add <id> <prefix> <retention-days> <action>
objstore policy add cleanup-old-logs logs/ 30 delete     # Delete logs after 30 days
objstore policy add archive-backups backups/ 90 archive  # Archive backups after 90 days

# List all policies
objstore policy list

# Remove a policy
objstore policy remove cleanup-old-logs
```

Retention is given in days on the CLI.

## Managing Policies through the REST API

```bash
# Add a policy (retention_seconds: 2592000 = 30 days)
curl -X POST http://localhost:8080/api/v1/policies \
  -H 'Content-Type: application/json' \
  -d '{
    "id": "cleanup-logs",
    "prefix": "logs/",
    "retention_seconds": 2592000,
    "action": "delete"
  }'

# List policies
curl http://localhost:8080/api/v1/policies

# Remove a policy
curl -X DELETE http://localhost:8080/api/v1/policies/cleanup-logs

# Apply all policies now
curl -X POST http://localhost:8080/api/v1/policies/apply
```

Archive policies additionally take `destination_type` (e.g. `s3`, `glacier`,
`local`) and `destination_settings` (backend-specific settings map).

The MCP server exposes the same operations as the `objstore_add_policy`,
`objstore_remove_policy`, `objstore_get_policies`, and
`objstore_apply_policies` tools, also using `retention_seconds`.

## Policy Execution

Policies are evaluated when explicitly applied — there is no built-in
scheduler. Trigger evaluation manually or from cron:

```bash
# Apply all policies
objstore policy apply

# Against a remote server
objstore policy apply --server http://localhost:8080

# Cron job example (daily at 2 AM):
# 0 2 * * * /usr/local/bin/objstore policy apply
```

Objects matching a policy's prefix whose age exceeds the retention period are
deleted or archived when the policies are applied.

## Persistence

For the `local` backend the CLI uses a persistent lifecycle manager: policies
are stored in `.lifecycle-policies.json` within the storage path and survive
across CLI invocations and server restarts. The server binaries persist
replication policies similarly (`.replication-policies.json`).

## Programmatic Configuration

```go
import (
    "time"

    "github.com/jeremyhahn/go-objstore/pkg/common"
)

policy := common.LifecyclePolicy{
    ID:        "cleanup-logs",
    Prefix:    "logs/",
    Retention: 30 * 24 * time.Hour,
    Action:    "delete",
}
err := lifecycleManager.AddPolicy(policy)
```

## Backend-Specific Behavior

### Backends with Native Lifecycle Support
Some backends implement lifecycle natively:
- Amazon S3 lifecycle rules
- Google Cloud Storage lifecycle management
- Azure Blob Storage lifecycle policies

When available, native implementation is preferred.

### Backends without Native Support
Application-level policy evaluation for:
- Local filesystem
- MinIO (unless configured)
- Custom backends
