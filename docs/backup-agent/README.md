# backup-agent

> **Status: PROPOSAL** · Scope: go-objstore backup-agent · Last verified: 2026-10-23
> The owner ruled the direction (CA m953, rulings 112/113); the SPEC row is not yet approved. Build nothing from this document until it is, when it becomes NORMATIVE.

## Requirement

Owner order (Company Agent m910 (3), 2026-10-21): a backup agent as a go-objstore deliverable, deployable to any system, that backs the system up to a bucket, compressed, encrypted and incremental.

## Decision

Owner ruling (CA m953, items 112/113, 2026-10-23):

- The backup agent is a go-objstore deliverable: one binary, `objstore-backup`, built from the Go client SDK's module (`api/sdks/go`, its own go.mod). It talks to an objstore server through the Go client SDK.
- Keys come from go-xkms and reach the agent through the key seam below. go-objstore's root module keeps go-enclave as its only first-party dependency.
- The server is unchanged. The agent is a client only: it adds no endpoint, no server option and no change to `common.Storage`.
- No code until the SPEC row is approved.

The agent stores a snapshot repository of immutable packs, an index and snapshot manifests as objects on the server it is pointed at. Because it uses only the SDK's object operations, it works on every backend the server fronts.

## Architecture

- **Chunking.** FastCDC splits each file into content-defined chunks, so an edit inside a large file re-uploads only the changed chunks.
- **Chunk identity.** Each chunk ID is HMAC-SHA-256 of the chunk under a repository secret. The agent uploads only chunks the index does not already hold.
- **Packing.** Chunks are compressed with zstd, then encrypted, then grouped into immutable pack objects written once.
- **Encryption.** Each pack, index and snapshot object is sealed with AES-256-GCM under a subkey derived with HKDF-SHA-256 from the repository data key.
- **Snapshot format.** Immutable packs, one index that maps chunk IDs to packs, and one snapshot manifest per backup run (tree, file metadata, chunk lists).
- **Key seam.** The data key is wrapped by a KEK that a key provider supplies. The provider is a go-xkms client, which the agent receives through an interface defined in the SDK module. The agent never imports go-xkms into the root module. Standalone mode, with no KMS, derives the KEK from a passphrase with PBKDF2 and holds it in go-enclave locked memory.
- **Restore.** Restore reads a snapshot manifest, fetches and decrypts the chunks it lists, rebuilds files and metadata, and verifies every chunk ID and file digest end to end. `verify` runs the same check on a repository without writing files.
- **Prune.** Retention prunes snapshots from a separate, privileged identity. The backup identity needs no delete right, so the repository works on a WORM bucket.
- **Service.** The agent runs on Linux, macOS and Windows as a scheduled service, with optional source snapshots (VSS, LVM, btrfs, ZFS, APFS) for crash consistency.

## Standards it builds on

| Concern | Standard | Use |
|---|---|---|
| Encryption | NIST SP 800-38D (AES-GCM), AES-256 | Authenticated encryption of every chunk pack, index and snapshot object |
| Key derivation | RFC 5869 / NIST SP 800-56C (HKDF-SHA-256) | Per-pack subkeys from the repository data key, so no single key nears the SP 800-38D §8.3 limit of 2^32 random-nonce invocations |
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

zstd in the SDK module's go.mod: `github.com/klauspost/compress` (a new third-party dependency of `api/sdks/go`; the standard library has no zstd) or stdlib gzip (RFC 1952), with no new dependency but a worse ratio and speed. The root module is unaffected either way. The owner decides.

## Boundaries

- go-qrdb's database backup (`Client.Backup()` in the constitution's go-quicbaas surface) is a separate feature and is unaffected.
- go-objstore's root module keeps its single first-party dependency (go-enclave). go-xkms keys reach the agent only through the key seam, in the SDK module.
- The server is unchanged; the agent is a client only.
- Cloud credentials are never logged and never written into object metadata or the repository.
