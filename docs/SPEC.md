# go-objstore specification

> **Status: NORMATIVE** · Scope: go-objstore · Last verified: 2026-10-27
> Conflicts with this document are defects. Fix them here first.

## Purpose

go-objstore is the multi-backend object storage library of the portfolio: one `Storage` interface over local disk, AWS S3, Google Cloud Storage, Azure Blob and the Glacier and Azure Archive tiers.
It is a foundational library. go-xkms, go-trusted-ca, go-quicbaas and go-qrdb consume it for persistent object storage.
It also ships servers (gRPC, REST, QUIC, Unix socket, MCP), a CLI, a C library and client SDKs over the same interface.
Its one first-party dependency is go-enclave, the locked-memory primitive.

## Boundaries

- Owns the storage abstraction, its provider implementations, the facade, the servers and the CLI.
- Owns at-rest encryption through a pluggable interface; it never owns key management.
- Holds credentials and keys in go-enclave locked, zeroized memory; never logs them or stores them in object metadata.
- Takes no portfolio dependency other than go-enclave.
- Never reaches a provider outside the common `Storage` interface; every provider satisfies it completely.
- Never supports MinIO; an S3-compatible endpoint is reached through the `s3` provider's `endpoint` option.
- Never edits another repository; work needed elsewhere is an issue on that repository.

## Features

| Feature | Status | Requirements | Detail |
|---|---|---|---|
| storage-interface | shipped | One `Storage` interface covers configuration, basic, context-aware, metadata, lifecycle and archival operations. Every provider implements all of it with the same behaviour and typed errors. | [docs/architecture](architecture/storage-layer.md) |
| local-provider | shipped | Stores objects on the local filesystem. Listing tolerates concurrent writes and deletes, and an absent directory lists as empty. | [docs/backends](backends/README.md) |
| cloud-providers | shipped | `s3`, `gcs` and `azure` providers use their SDK credential chains, or a credential passed through the `Storage.Configure` credential handle. `s3` reaches any S3-compatible endpoint through its `endpoint` option. | [docs/backends](backends/README.md) |
| archive-providers | shipped | `glacier` and `azure-archive` serve as archive destinations only, never as primary storage. | [docs/backends](backends/README.md) |
| facade | shipped | The `objstore` package is the single entry point. It validates input once, routes `backend:key` references and builds providers through the factory. | [docs/architecture](architecture/README.md) |
| servers | shipped | gRPC, REST, QUIC, Unix socket and MCP servers expose the interface with TLS/mTLS and pluggable authentication. Settings resolve flags over environment over file over defaults. | [docs/architecture](architecture/servers.md) |
| storagefs | shipped | Wraps any provider in file and directory operations like Go's `os` package. | [docs/storagefs](storagefs/README.md) |
| encryption | shipped | Optional at-rest encryption through the `Encrypter` and `EncrypterFactory` interfaces, with AES-GCM available. Key management is the caller's. | [docs/architecture](architecture/encryption.md) |
| protected-memory | shipped | Keys and credentials live in go-enclave locked, zeroized memory for their whole lifetime. Protected entry points are additive and the plain ones stay deprecated until a coordinated release. | [docs/architecture](architecture/protected-memory.md) |
| lifecycle | shipped | Retention and archival rules delete or archive objects by age and prefix. | [docs/lifecycle](lifecycle/README.md) |
| replication | shipped | Replicates objects between providers with change detection and a changelog. | [docs/replication](replication/README.md) |
| cli | shipped | The `objstore` command exposes the interface, lifecycle and replication from a shell. | [docs/usage](usage/cli.md) |
| c-client | shipped | `libobjstore` exposes the interface to C callers through opaque handles. | [docs/c_client](c_client/README.md) |
| sdks | shipped | Client SDKs for Go, Python, Ruby, Rust, TypeScript and C# speak the server APIs. | [api/sdks](../api/sdks/README.md) |
| provider-buckets | shipped | Providers create, list and delete buckets. The consumer is go-quicbaas. | [docs/buckets](buckets/README.md) |
| provider-multipart | shipped | Providers initiate a multipart upload, upload a part, complete it and abort it. The consumer is go-quicbaas. | [docs/buckets](buckets/README.md) |
| configure-credential-handle | shipped | `Storage.Configure` accepts the protected-memory credential handle (go-enclave `enclave.ByteAccess`, the type `pkg/cli` already holds for the backend key and secret) in place of the `map[string]string` credential values. A backend copies the credential out of the handle for the length of the call and never retains the handle. The handle is the credential path for the server archive `DestinationSettings`, the C library and replication policies as well: the servers and the C library move a supplied credential into a handle at the API boundary and destroy it when the call ends, and replication targets take the handle from a credential provider the embedding application supplies when it constructs the replication manager, never persisted, one handle per target (a persisted replication policy refuses a credential in its settings). This replaces the former `Configure(map[string]string)` signature of the `Storage` interface and of every provider and wrapper that implements it. The consumer is the go-qrdb object-storage backend. | [docs/architecture](architecture/protected-memory.md) |
| backup-agent | shipped | One backup agent binary, `objstore-backup`, in its own Go module `backup/` inside go-objstore on the xkey pattern (its own go.mod, released with this repository), consuming the Go client SDK (`api/sdks/go`) and the key seam go-xkms implements downward; it is not a separate repository. It talks to the objstore server through that SDK; the server gains only configurable gRPC message limits (`grpc.max_recv_msg_size`, `grpc.max_send_msg_size`). Keys come from go-xkms through the agent's key seam, or from a passphrase through the standalone PBKDF2 provider, so the root module keeps go-enclave as its only first-party dependency. Standards: FastCDC chunking, HMAC-SHA-256 chunk IDs (FIPS 198-1), a compression identifier in the sealed format with only `none` implemented (zstd or gzip is an open owner decision), AES-256-GCM (SP 800-38D) with HKDF (RFC 5869) subkeys, immutable packs plus index plus snapshot manifests as the snapshot format, restore with end-to-end verify, prune from a separate privileged identity, and a scheduled service with source snapshots. | [docs/backup-agent](backup-agent/README.md) |
| qrdb-storage-provider | planned | When object storage is configured as a go-qrdb backend storage provider, go-qrdb's storage API uses the go-objstore `Storage` interface and its backends: `local`, `s3`, `gcs` and `azure`, with `glacier` and `azure-archive` as archive destinations only. go-qrdb is the provider seam and consumes go-objstore; go-objstore takes no go-qrdb dependency and keeps go-enclave its only first-party dependency. go-quicbaas owns which go-objstore backend each configuration uses. go-objstore keeps the interface and the backend configuration it already documents ([docs/backends](backends/README.md), [docs/configuration](configuration/storage-backends.md)). | [docs/architecture](architecture/storage-layer.md) |

## Interfaces

- Go SDK: `pkg/objstore` facade, `pkg/common` interfaces, `pkg/factory` ([docs/usage](usage/getting-started.md)).
- CLI: `objstore` ([docs/usage/cli.md](usage/cli.md)); the portfolio surface is in handbook `software/docs/11-cli-surface.md`.
- Servers: gRPC, REST, QUIC, Unix socket, MCP ([docs/architecture/servers.md](architecture/servers.md), [api/](../api/)).
- C API: `libobjstore` ([docs/c_client](c_client/README.md)).
- Configuration: [docs/configuration](configuration/README.md).

## Release criteria

- Every provider passes the common interface tests and its integration target.
- The module builds on Go 1.27.2 with go-enclave as the only first-party dependency.
- Every server binary resolves settings through the shared configuration layer.
- No credential or key appears in a log line or in object metadata.
- Every row above is shipped, plus every seam row go-trusted-platform's SPEC names on this project, with the API stable.
- CHANGELOG.md carries a section for the release.
