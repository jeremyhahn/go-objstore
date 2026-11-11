# Usage: API

> **Status: NORMATIVE** · Scope: go-objstore · Last verified: 2026-10-09
> Conflicts with this document are defects. Fix them here first.

The operations an application can perform and their form on each transport. REST routes are detailed in [../configuration/rest-server.md](../configuration/rest-server.md); the gRPC contract is `api/proto/objstore.proto`; the embedded Go calls are in [engineering.md](engineering.md).

## Capabilities by transport

| Capability | Embedded Go (`pkg/objstore`) | REST (`/api/v1`) | gRPC (`objstore.v1.ObjectStore`) | MCP tool | Unix socket method |
|---|---|---|---|---|---|
| Put | `Put`, `PutWithContext`, `PutWithMetadata` | `PUT /objects/{key}` | `Put`, `PutStream` (client stream) | `objstore_put` | `put` |
| Get | `Get`, `GetWithContext` | `GET /objects/{key}` | `Get` (server stream) | `objstore_get` | `get` |
| Delete | `Delete`, `DeleteWithContext` | `DELETE /objects/{key}` | `Delete` | `objstore_delete` | `delete` |
| List | `List`, `ListWithContext`, `ListWithOptions` | `GET /objects?prefix=` | `List` | `objstore_list` | `list` |
| Exists | `Exists` | `HEAD /objects/{key}`, `HEAD /exists/{key}` | `Exists` | `objstore_exists` | `exists` |
| Get metadata | `GetMetadata` | `GET /metadata/{key}` | `GetMetadata` | `objstore_get_metadata` | `get_metadata` |
| Update metadata | `UpdateMetadata` | `PUT /metadata/{key}` | `UpdateMetadata` | `objstore_update_metadata` | `update_metadata` |
| Health | none | `GET /health` | `Health` | `objstore_health` | `health` |
| Archive | `Archive` | `POST /archive` | `Archive` | `objstore_archive` | `archive` |
| Add lifecycle policy | `AddPolicy` | `POST /policies` | `AddPolicy` | `objstore_add_policy` | `add_policy` |
| Remove lifecycle policy | `RemovePolicy` | `DELETE /policies/{id}` | `RemovePolicy` | `objstore_remove_policy` | `remove_policy` |
| List lifecycle policies | `GetPolicies` | `GET /policies` | `GetPolicies` | `objstore_get_policies` | `get_policies` |
| Apply lifecycle policies | none | `POST /policies/apply` | `ApplyPolicies` | `objstore_apply_policies` | `apply_policies` |
| Add replication policy | `EnableReplication` and the replication manager | `POST /replication/policies` | `AddReplicationPolicy` | `objstore_add_replication_policy` | `add_replication_policy` |
| Remove replication policy | replication manager | `DELETE /replication/policies/{id}` | `RemoveReplicationPolicy` | `objstore_remove_replication_policy` | `remove_replication_policy` |
| List replication policies | replication manager | `GET /replication/policies` | `GetReplicationPolicies` | `objstore_list_replication_policies` | `get_replication_policies` |
| Get replication policy | replication manager | `GET /replication/policies/{id}` | `GetReplicationPolicy` | `objstore_get_replication_policy` | `get_replication_policy` |
| Trigger replication | replication manager | `POST /replication/trigger` | `TriggerReplication` | `objstore_trigger_replication` | `trigger_replication` |
| Replication status | replication manager | `GET /replication/status/{id}` | `GetReplicationStatus` | `objstore_get_replication_status` | `get_replication_status` |

REST object routes are served under `/api/v1` and, for backwards compatibility, at the root path. REST also serves `GET /metrics` (authorization required unless `--metrics-public`) and `GET /swagger/*`. The Unix socket also has a `ping` method.

## QUIC (HTTP/3)

The QUIC server serves object, metadata and policy routes over HTTP/3 at `/objects`, `/objects/{key}`, `/metadata/{key}`, `/policies`, `/policies/apply` and `/policies/{id}`, plus `/health`. Its configuration is in [../configuration/quic-server.md](../configuration/quic-server.md).

## Request and error conventions

- Request and response schemas for gRPC are the messages in `api/proto/objstore.proto`. A list is paginated with `max_results` and `continue_from`, and answers with `next_token` and `truncated`.
- A gRPC `Get` streams chunks; metadata arrives in the first message and `is_last` marks the final one.
- A gRPC `PutStream` is the upload counterpart: the first message is a `PutStreamHeader` (key, metadata), every later message a data `chunk`. The server sends its response headers once it accepts the header, stores the object when the client closes the stream, and stores nothing if the stream is cancelled. A missing header or empty key is `InvalidArgument`. Authorization is that of `Put`. Only the unary `Put` is bound by the server's receive limit; the `objstore` CLI and the Go SDK's `PutStream` send chunks of 1 MiB less 1 KiB (so a chunk fits the smallest allowed receive limit), falling back to `Put` against a server without `PutStream`.
- Keys, prefixes and metadata are validated on every transport; a rejected input is a client error, not a backend call.
- `DELETE` on REST answers `204 No Content`.

## Versioning

REST routes are versioned by the `/api/v1` prefix and gRPC by the `objstore.v1` package. The project is pre-1.0; changes are recorded in `CHANGELOG.md`.
