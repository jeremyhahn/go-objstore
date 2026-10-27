# go-objstore roadmap

> **Status: REFERENCE** · Scope: go-objstore · Last verified: 2026-10-27
> An as-built plan. Gitea issues are the backlog; this orders them.

## Mission

go-objstore is the portfolio's multi-backend object storage library: one interface over local disk, S3, GCS, Azure Blob and archive tiers.
go-xkms, go-trusted-ca and go-quicbaas consume it.
Its only first-party dependency is go-enclave.

## Milestones

- [Dev complete](../../milestone/5) — release candidate; tagged only after go-trusted-platform and every lower library are dev complete, `-alpha` until then — projected date set by the department plan; open release item: public release readiness, [#51](../../issues/51), needs info

## Complete

- **storage-interface** — one interface across every provider — [v0.4.0-alpha](../../releases/tag/v0.4.0-alpha) — done
- **local-provider** — listing tolerates concurrent writes and deletes — [#68](../../issues/68) — done
- **cloud-providers** — MinIO provider removed; S3-compatible endpoints use `s3` — [#65](../../issues/65) — done
- **archive-providers** — Glacier and Azure Archive as archive destinations — [v0.4.0-alpha](../../releases/tag/v0.4.0-alpha) — done
- **facade** — single validated entry point — [v0.4.0-alpha](../../releases/tag/v0.4.0-alpha) — done
- **servers** — one viper-based configuration layer for every server binary — [#34](../../issues/34) — done
- **storagefs** — filesystem abstraction over providers — [v0.4.0-alpha](../../releases/tag/v0.4.0-alpha) — done
- **encryption** — pluggable at-rest encryption — [v0.4.0-alpha](../../releases/tag/v0.4.0-alpha) — done
- **protected-memory** — credentials and keys in go-enclave locked memory — [#4](../../issues/4) — done
- **lifecycle** — retention and archival rules — [v0.4.0-alpha](../../releases/tag/v0.4.0-alpha) — done
- **replication** — provider-to-provider replication — [v0.4.0-alpha](../../releases/tag/v0.4.0-alpha) — done
- **cli** — `objstore` command — [v0.4.0-alpha](../../releases/tag/v0.4.0-alpha) — done
- **c-client** — `libobjstore` C library — [v0.4.0-alpha](../../releases/tag/v0.4.0-alpha) — done
- **sdks** — client SDKs in six languages — [v0.4.0-alpha](../../releases/tag/v0.4.0-alpha) — done
- **provider-buckets** — create, list and delete buckets on every provider, for go-quicbaas — [#89](../../issues/89) — done
- **provider-multipart** — initiate, upload part, complete and abort on every provider, for go-quicbaas — [#89](../../issues/89) — done
- **configure-credential-handle** — `Storage.Configure` takes the go-enclave protected credential handle, also for server archive destinations, the C library and replication targets — [#16](../../issues/16) — done
- **backup-agent** — `objstore-backup` agent in its own `backup/` module (backup, restore, verify, prune, scheduled service with source snapshots) — [#114](../../issues/114) — done

## Now

None.

## Next

None.

## Later

- **qrdb-storage-provider** — go-objstore backends serve as a go-qrdb backend storage provider, configured per backend by go-quicbaas; waits on a Software-seated lane (P2) — [docs/architecture](docs/architecture/storage-layer.md) — open

## Future

None.

## Dependencies

- Upstream: go-enclave v0.9.0-alpha adopted ([#88](../../issues/88)); Go 1.27.3 adopted ([#93](../../pulls/93)).
- Downstream: go-xkms, go-trusted-ca and go-quicbaas consume this library; go-quicbaas waits on buckets and multipart ([#90](../../issues/90)).
- Standards watch: none pinned.
