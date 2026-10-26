# backup-agent

> **Status: APPROVED** · Scope: go-objstore backup-agent · Last verified: 2026-10-26
> The owner ruled the direction (CA m953, rulings 112/113) and the SPEC row is approved. The agent is being built in its own module, `backup/`; restore and verify (`backup/pkg/restore`) and prune (`backup/pkg/prune`) exist there as libraries, and the `objstore-backup` command drives them (see Usage). The service, source snapshots and the go-xkms key provider are still to come, and the row's status stays `planned` until it ships.

## Requirement

Owner order (Company Agent m910 (3), 2026-10-21): a backup agent as a go-objstore deliverable, deployable to any system, that backs the system up to a bucket, compressed, encrypted and incremental.

## Decision

Owner rulings (CA m953, items 112/113, 2026-10-23; ruling 175, 2026-10-25, which supersedes 112/113 on where the agent is built and where the seam lives):

- The backup agent is a go-objstore deliverable: one binary, `objstore-backup`, built from its own Go module inside this repository, `backup/` (module `github.com/jeremyhahn/go-objstore/backup`, its own go.mod), on the xkey pattern: a product-surface module in a subdirectory that consumes the project's SDK. It is not a separate repository.
- The `backup/` module consumes the Go client SDK module (`api/sdks/go`) and talks to an objstore server through it. Its only first-party dependencies are that SDK module and go-enclave; it never imports go-xkms.
- Keys come from go-xkms and reach the agent through the key seam below, whose interface is defined in `backup/`. go-xkms implements it downward, in its own repository. go-objstore's root module keeps go-enclave as its only first-party dependency.
- The server is unchanged. The agent is a client only: it adds no endpoint, no server option and no change to `common.Storage`.
- No code until the SPEC row is approved.

The agent stores a snapshot repository of immutable packs, an index and snapshot manifests as objects on the server it is pointed at. Because it uses only the SDK's object operations, it works on every backend the server fronts.

## Architecture

- **Chunking.** FastCDC splits each file into content-defined chunks, so an edit inside a large file re-uploads only the changed chunks.
- **Chunk identity.** Each chunk ID is HMAC-SHA-256 of the chunk under a repository secret. The agent uploads only chunks the index does not already hold.
- **Packing.** Chunks are compressed with the algorithm the sealed format records (`none` today; zstd or gzip waits on the open item below), then encrypted, then grouped into immutable pack objects written once.
- **Encryption.** Each pack, index, snapshot and config object is sealed with AES-256-GCM under a subkey derived with HKDF-SHA-256 from the repository data key.
- **Snapshot format.** Immutable packs; an index object per backup run that stores new chunks, mapping chunk IDs to packs, with all of them merged on load; and one snapshot manifest per backup run (tree, file metadata, chunk lists), written last. Objects are written once under `config`, `keys/`, `packs/<id[:2]>/`, `index/`, `snapshots/` and, for prune, `marks/`, each named by a keyed ID of its content; time and host are only inside the sealed manifest.
- **Key seam.** The data key is wrapped by a KEK that a key provider supplies. The interface is `keyseam.Provider` in the `backup/` module (`backup/pkg/keyseam`): `WrapKey(ctx, dataKey)` and `UnwrapKey(ctx, wrapped)`, with typed errors that carry no key material. The provider is a go-xkms client that implements the interface downward, in go-xkms; neither go-objstore module imports go-xkms. The unwrapped data key lives in go-enclave locked memory and is destroyed when the repository closes; only its wrapped form is persisted. Standalone mode, with no KMS, derives the KEK from a passphrase with PBKDF2-HMAC-SHA-256 (a random 16-byte salt stored beside the wrapped key, at least 600000 iterations) and holds it in go-enclave locked memory.
- **Restore.** Restore reads a snapshot manifest, fetches and decrypts the chunks it lists, rebuilds files and metadata, and verifies every chunk ID and file digest end to end. `verify` runs the same check on a repository without writing files.
- **Prune.** Retention prunes snapshots from a separate, privileged identity. The backup identity needs no delete right, so the repository works on a WORM bucket. In code, removal is a `store.PruneStore` that only `backup/pkg/prune` holds; a repository never hands it on. A policy keeps the last N snapshots, those within a duration, and the newest of each of the last N days, ISO weeks, months and years, by the time sealed in each manifest; it keeps the union, and one that keeps nothing is refused. Prune takes no lock, which the backup identity could not remove; it marks and sweeps. Each run writes a sealed mark under `marks/` naming the packs no kept snapshot uses, each with the time a run first marked it. A backup reads every mark before it loads the index, and neither dedups against a marked pack nor writes one under a marked name: it stores those chunks again under another name. A run removes a pack only if it is still unused and the newest earlier mark has named it for a grace period (24 hours by default) that must exceed the longest backup, so a backup that started before the mark has written its snapshot by then and its chunks keep their packs. One prune runs at a time. A run writes the new index first, then removes forgotten snapshots, superseded indexes, unused packs and old marks, so an interrupted run leaves every kept snapshot restorable and a rerun completes it. Partly used packs are kept and their unused bytes reported; repacking is not done. A dry run reports what would go and changes nothing, and `verify` reports packs that no index names.
- **Service.** The agent runs on Linux, macOS and Windows as a scheduled service, with optional source snapshots (VSS, LVM, btrfs, ZFS, APFS) for crash consistency.

## Usage

`objstore-backup` (`backup/cmd/objstore-backup`) is a standalone command over the libraries, using the passphrase key provider:

```
objstore-backup init      --address <host:port> --repo <prefix> --passphrase-file <file>
objstore-backup backup    [flags] [--host <name>] <path>...
objstore-backup snapshots [flags] [--json]
objstore-backup restore   [flags] [--overwrite] <snapshot> <target>
objstore-backup verify    [flags] [--json] [<snapshot>...]
objstore-backup prune     --prune-address <host:port> --repo <prefix> --passphrase-file <file> \
                          [--keep-last n] [--keep-within 30d] [--keep-daily n] [--keep-weekly n] \
                          [--keep-monthly n] [--keep-yearly n] [--grace 24h] [--dry-run] [--json]
```

- **Connection.** The server is reached through the Go client SDK: `--protocol` (rest, grpc, quic, mcp, unix), `--address`, `--tls`, `--ca-file`, `--cert-file`, `--key-file`, `--token-file`, `--tenant`, `--timeout`. `--repo` is the prefix the repository lives under.
- **gRPC message limit.** Over gRPC each pack is uploaded in one message, so the server's `grpc.max_recv_msg_size` must be at or above the repository's pack size plus a little framing; the server default of 32 MiB fits the default 16 MiB pack.
- **Prune identity.** `prune` takes none of those: it connects with its own `--prune-*` set, which must name a delete-capable identity. The backup identity needs no delete right and should have none.
- **Passphrase.** From `--passphrase-file`, refused unless it is a regular file with no group or other permission bits, or from standard input with `--passphrase-stdin`; never from an argument or the environment. The read buffer is cleared once the passphrase is in go-enclave locked memory. A bearer token file follows the same permission rule.
- **Snapshots** are named by ID or by a prefix that names one snapshot only.
- **Exit status.** 0 success, 1 failure, 2 usage error, 3 `verify` found problems.

## Standards it builds on

| Concern | Standard | Use |
|---|---|---|
| Encryption | NIST SP 800-38D (AES-GCM), AES-256 | Authenticated encryption of every chunk pack, index and snapshot object |
| Key derivation | RFC 5869 / NIST SP 800-56C (HKDF-SHA-256) | Per-purpose subkeys (chunk IDs, object IDs, packs, index, snapshots, config) from the repository data key; the pack subkey, the busiest, nears the SP 800-38D §8.3 limit of 2^32 random-nonce invocations only after 2^32 sealed chunks |
| Passphrase keys | NIST SP 800-132 (PBKDF2-HMAC-SHA-256) | Standalone mode only, where no KMS is available; Argon2id and scrypt are not FIPS-approved |
| Key wrapping | Envelope encryption through the agent's key seam | The data key is wrapped by a KEK from go-xkms (KMS or HSM), or from a local key held in go-enclave locked memory |
| FIPS | FIPS 140-3 through Go's `crypto/fips140` | The agent builds and passes its tests under `make test-fips`, like the rest of the repository |
| Chunk identity | HMAC-SHA-256 (FIPS 198-1, FIPS 180-4) keyed with a repository secret | Content-addressed deduplication without exposing plaintext hashes, which allow confirmation-of-file attacks |
| Chunking | FastCDC content-defined chunking (Xia et al., USENIX ATC 2016) | Incremental at sub-file granularity |
| Compression | Zstandard, RFC 8878 | Compress, then encrypt; compression after encryption gains nothing |
| Backup practice | 3-2-1 rule; NIST SP 800-34 Rev. 1 (contingency planning) §3.4.2; NIST SP 800-209 (storage infrastructure security) | Off-host copy, restore testing (`verify`), retention, immutable destination |
| Ransomware resistance | S3 Object Lock, Azure immutable blob storage, GCS bucket lock | Packs are immutable and written once; prune runs from a separate identity |
| Consistency | Windows VSS; LVM, btrfs and ZFS snapshots or `fsfreeze` on Linux; APFS snapshots on macOS | Optional snapshot of the source before the walk |

## Not chosen

Recommending restic or kopia and serving restic's REST backend protocol from `objstore-server` was rejected. restic encrypts with AES-256-CTR plus Poly1305-AES and derives keys with scrypt, so it is not FIPS 140-3 capable and its keys cannot come from go-xkms. Kopia carries its own S3, GCS and Azure backends, which the constitution forbids a portfolio deliverable to duplicate. In both cases the binary is not ours: we cannot fix it, sign it or hold it to the portfolio's security suite, so it is not a go-objstore deliverable.

## Open item

zstd in the backup module's go.mod (`backup/go.mod`): `github.com/klauspost/compress` (a new third-party dependency of the `backup/` module; the standard library has no zstd) or stdlib gzip (RFC 1952), with no new dependency but a worse ratio and speed. The root module and the SDK module are unaffected either way. The owner decides. Until then the sealed format records a compression identifier and implements only `none`.

## Boundaries

- go-qrdb's database backup (`Client.Backup()` in the constitution's go-quicbaas surface) is a separate feature and is unaffected.
- The agent is its own module, `backup/`, inside go-objstore; it consumes the Go client SDK module (`api/sdks/go`) and go-enclave and nothing else first-party.
- go-objstore's root module keeps its single first-party dependency (go-enclave), and the SDK module gains none. go-xkms keys reach the agent only through the key seam, whose interface is defined in `backup/` and which go-xkms implements downward; go-objstore never imports go-xkms.
- The server is unchanged; the agent is a client only.
- Cloud credentials are never logged and never written into object metadata or the repository.
