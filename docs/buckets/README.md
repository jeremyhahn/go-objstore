# Buckets and multipart upload: API-shape proposal

> **Status: PROPOSAL** · Scope: go-objstore · Last verified: 2026-10-21
> Not approved. Build nothing from this document until the owner rules; it then becomes NORMATIVE or is deleted.

This document covers two SPEC.md rows: `provider-buckets` (create, list and delete buckets) and `provider-multipart` (initiate, upload a part, complete, abort). The consumer is go-quicbaas. It proposes the API shape, where bucket and upload state lives for each provider, and what the first tag leaves out. Identifiers marked *proposed* do not exist yet. Every other identifier and path was checked against this tree.

## 1. Recommendation

Add two optional capability interfaces to `pkg/common`, `BucketManager` and `MultipartUploader`. Callers detect them by type assertion. `Storage` is not widened. Every `Storage` provider in this tree (local, s3, gcs, azure, memory) implements both, natively or by emulation. The archive-only providers implement neither.

This follows the precedent already in the tree:

| Capability | Declared at | Detected at |
|---|---|---|
| `ReplicationCapable` | `pkg/common/storage.go:72` | `pkg/objstore/facade.go:641`, which returns `common.ErrReplicationNotSupported` when the assertion fails |
| `PathAccessor` | `pkg/common/path_accessor.go:22` | `pkg/pool/local_only.go:60` |
| `ArchiveOnlyStorage` | `pkg/common/archiveonly.go:17` | the factory's `archiveOnlyTypes` set, `pkg/factory/factory.go:29` |

The coordinator recommended optional `BucketManager` and `MultipartUploader` interfaces. This proposal adopts that and adds four changes, each needed by something in the current code:

1. **A typed "not supported" error and code.** `common.ErrorCode` has no unimplemented code today, and `common.Classify` (`pkg/common/errors.go:149`) falls through to `CodeInternal` for `ErrReplicationNotSupported`. The servers therefore answer an unsupported capability with 500 / `codes.Internal`. The two new interfaces need a code that maps to 501 / `codes.Unimplemented`.
2. **A factory constructor for account-scoped bucket management.** Every cloud provider's `Configure` requires a bucket or container: s3 and gcs return `common.ErrBucketNotSet`, and azure returns `common.ErrAccountNotSet` without `containerName`. A caller that wants to list or create buckets has no bucket to configure with.
3. **`ListMultipartUploads`.** Without it, abandoned uploads on emulated providers cannot be found and expired through one code path (§5.3).
4. **An explicit statement about the encryption wrapper.** `common.NewEncryptedStorage` (`pkg/common/encrypted_storage.go:50`) returns a plain `Storage`, so a type assertion on the wrapper hides both capabilities (§5.4).

## 2. Interfaces (proposed, `pkg/common`)

Context comes first in every method. Every method returns errors from §3 only, and providers wrap SDK errors in them.

```go
// BucketManager creates, lists and deletes buckets (containers on Azure,
// directories under a root on local). It is account-scoped: one value
// manages every bucket its credentials reach.
type BucketManager interface {
	CreateBucket(ctx context.Context, name string) error
	DeleteBucket(ctx context.Context, name string) error
	ListBuckets(ctx context.Context) ([]BucketInfo, error)

	// Bucket returns a Storage bound to the named bucket that reuses this
	// manager's client and credentials. See open question 4.
	Bucket(ctx context.Context, name string) (Storage, error)
}

type BucketInfo struct {
	Name    string
	Created time.Time // zero when the provider does not report it
}

// MultipartUploader uploads one object in independently sent parts.
// Keys are object keys in the provider's configured bucket, validated by
// ValidateKey exactly as Put validates them.
type MultipartUploader interface {
	InitiateMultipartUpload(ctx context.Context, key string, metadata *Metadata) (uploadID string, err error)
	UploadPart(ctx context.Context, key, uploadID string, partNumber int, data io.Reader, size int64) (PartInfo, error)
	CompleteMultipartUpload(ctx context.Context, key, uploadID string, parts []CompletedPart) error
	AbortMultipartUpload(ctx context.Context, key, uploadID string) error
	ListMultipartUploads(ctx context.Context, prefix string) ([]MultipartUploadInfo, error)
}

type PartInfo struct {
	PartNumber int
	ETag       string // opaque; the caller passes it back unchanged
	Size       int64
}

type CompletedPart struct {
	PartNumber int
	ETag       string
}

type MultipartUploadInfo struct {
	Key       string
	UploadID  string
	Initiated time.Time
}

const (
	MinPartSize = 5 << 20  // every part except the last
	MaxPartSize = 5 << 30
	MaxParts    = 10000    // part numbers 1..MaxParts
)
```

Design points:

- Neither interface embeds `Storage`. `PathAccessor` does not embed it either. A small interface is easier to fake in consumers' tests and lets a wrapper forward one capability without the other. `ReplicationCapable` embeds `Storage`, but nothing depends on that.
- Metadata is given at initiate, as S3 takes it, and applied to the completed object exactly as `PutWithMetadata` applies it. `ValidateMetadata` runs at initiate.
- `size` is the exact part length. s3 needs it for the request, and every provider uses it to reject a short non-final part before reading the body. A reader that yields a different length fails with `ErrInvalidArgument`.
- The limits are S3's, enforced by every provider including the emulated ones, so a caller sees the same behaviour on every provider (the `storage-interface` row requires that).
- An upload ID belongs to the provider instance that issued it. It cannot be completed on another backend.
- Complete is all-or-nothing. The object appears whole or not at all, and on success the staged parts are gone.

## 3. Errors (proposed additions to `pkg/common/errors.go`)

| Error | Returned when | `Classify` code |
|---|---|---|
| `ErrNotSupported` | The backend lacks the capability: failed type assertion in the facade, or an archive-only type in the factory. | `CodeUnimplemented` (new) |
| `ErrBucketNotFound` | Delete, `Bucket` or a multipart call names a bucket that does not exist. | `CodeNotFound` |
| `ErrBucketAlreadyExists` | Create names an existing bucket. Wraps `ErrAlreadyExists`. | `CodeAlreadyExists` |
| `ErrBucketNotEmpty` | Delete of a bucket that still holds objects or uploads. | `CodeFailedPrecondition` (new) |
| `ErrUploadNotFound` | Unknown, completed, aborted or expired upload ID. | `CodeNotFound` |
| `ErrInvalidBucketName`, `ErrInvalidPartNumber`, `ErrPartTooSmall`, `ErrPartTooLarge`, `ErrInvalidPartOrder`, `ErrPartMismatch` | Argument checks; `ErrPartMismatch` means an ETag or part number at complete does not match an uploaded part. Each wraps `ErrInvalidArgument`. | `CodeInvalidArgument` |

- New sentinels carry the package prefix (`"common: bucket not empty"`), as the errors convention requires. The existing sentinels in this file are unprefixed; that is debt and is not copied.
- Each new code gets a row in `HTTPStatus`, `GRPCStatus` and `JSONRPCError` (`pkg/server/errors/errors.go`): 501 / `Unimplemented` / the unimplemented JSON-RPC code, and 412 / `FailedPrecondition` / the precondition-failed code.
- `ErrReplicationNotSupported` should wrap `ErrNotSupported` in the same change, so that replication stops classifying as internal.

## 4. Facade and factory

### Facade (`pkg/objstore`, proposed functions)

The facade adds package functions in the style of `GetReplicationManager`. Each one resolves the backend, asserts the capability, returns `common.ErrNotSupported` when the assertion fails, and validates input once before calling the provider.

```go
func CreateBucket(ctx context.Context, backendName, bucket string) error
func DeleteBucket(ctx context.Context, backendName, bucket string) error
func ListBuckets(ctx context.Context, backendName string) ([]common.BucketInfo, error)

func InitiateMultipartUpload(ctx context.Context, keyRef string, metadata *common.Metadata) (string, error)
func UploadPart(ctx context.Context, keyRef, uploadID string, partNumber int, data io.Reader, size int64) (common.PartInfo, error)
func CompleteMultipartUpload(ctx context.Context, keyRef, uploadID string, parts []common.CompletedPart) error
func AbortMultipartUpload(ctx context.Context, keyRef, uploadID string) error
func ListMultipartUploads(ctx context.Context, backendName, prefix string) ([]common.MultipartUploadInfo, error)
```

- Multipart functions take a `backend:key` reference and route it like `Put` does (`getStorageForKey`). An empty backend name means the default backend, as in `GetReplicationManager`.
- Bucket functions act on the account behind a configured backend. A facade backend stays bound to the one bucket in its settings; adding a bucket does not add a facade backend.
- The facade validates the bucket name with a new `common.ValidateBucketName` (proposed): 3 to 63 characters, lowercase letters, digits and single hyphens, starting and ending with a letter or digit. This is the set that s3, gcs and azure all accept, so a name is portable across providers (open question 6).

### Factory (`pkg/factory`, proposed)

```go
type BucketManagerCreator func(settings map[string]string) (common.BucketManager, error)

func RegisterBucketManager(backendType string, creator BucketManagerCreator)
func NewBucketManager(backendType string, settings map[string]string) (common.BucketManager, error)
```

- This mirrors `RegisterArchiver` and `NewArchiver`. The creator configures the same provider struct without requiring the bucket setting, so there is one SDK client per provider and no second credential path.
- `NewBucketManager` returns `ErrArchiveOnlyBackend` for `glacier` and `azurearchive`, `ErrUnknownBackend` for an unknown type, and `common.ErrNotSupported` for a registered storage type that has no manager.
- `NewStorage` is unchanged. A caller reaches `MultipartUploader` by asserting on the `Storage` it returns.

## 5. Providers

### 5.1 Support matrix

| Provider | Buckets | Bucket state | Multipart | Upload state |
|---|---|---|---|---|
| `s3` | native: `CreateBucket`, `DeleteBucket`, `ListBuckets` on the existing `s3iface.S3API` client | the provider | native: `CreateMultipartUpload`, `UploadPart`, `CompleteMultipartUpload`, `AbortMultipartUpload`, `ListMultipartUploads` | the provider |
| `gcs` | native: `BucketHandle.Create`, `BucketHandle.Delete`, `Client.Buckets`; needs a new `project` setting (open question 5) | the provider | emulated over object compose | part objects and a manifest under the reserved prefix in the same bucket (§5.2) |
| `azure` | native containers through an account-level `ServiceURL` built from the same `accountName` / `endpoint` settings | the provider | native blocks (`StageBlock`, `CommitBlockList`); upload ID and metadata emulated | uncommitted blocks held by the service; a manifest blob under the reserved prefix |
| `local` | emulated: a bucket is a directory directly under a new `bucketRoot` setting | the filesystem | emulated | `<path>/.objstore-multipart/<uploadID>/`: one file per part and `upload.json` |
| `memory` | emulated: a per-instance map from bucket name to a child `*Memory` | process memory | emulated | a map on `*Memory` kept apart from `objects` |
| `glacier` | unsupported: `ErrArchiveOnlyBackend` from the factory | — | unsupported; the internal multipart in `Glacier.Put` stays private | — |
| `azurearchive` | unsupported: `ErrArchiveOnlyBackend` from the factory | — | unsupported | — |

### 5.2 Per-provider notes

- **s3.** `CreateBucket` sets the location constraint from the configured `region`. A delete of a non-empty bucket maps S3's `BucketNotEmpty` to `ErrBucketNotEmpty`. A part body is wrapped with `aws.ReadSeekCloser`, as `Put` already does (`pkg/s3/s3.go:99`). An S3-compatible endpoint is reached through the existing `endpoint` option; no MinIO-specific code.
- **gcs.** The Go client has no S3-style multipart call that this design relies on. Parts are written as objects `.objstore-multipart/<uploadID>/<partNumber>`. Complete composes them into the final key with `ObjectHandle.ComposerFrom`, which takes at most 32 sources per call, so more parts compose in a tree of intermediate objects. It then sets the metadata from the manifest and deletes the part objects. Creating and listing buckets needs a project ID, which `Configure` does not read today; it reads only `bucket` and `skip_client`.
- **azure.** The block ID is base64 of `<uploadID>/<partNumber>`, zero-padded so that every ID in a blob has the same length, as Azure requires. Each upload stages its blocks on a blob of its own under the reserved prefix (`.objstore-multipart/<uploadID>/staging`), never on the target key, because a block-list commit discards every uncommitted block it does not name and so would destroy a concurrent upload of the same key, or an ordinary overwrite. Complete reads the manifest, commits the listed blocks on the staging blob in part order, publishes the staging blob to the target key, and then deletes the upload's blobs. Publishing stages the target's own blocks on the service with Put Block From URL (`StageBlockFromURL`, 100 MiB ranges of the staging blob, block IDs that belong to the upload) and makes the object appear with one `CommitBlockList` on the target, so the target keeps its old content until that commit and a failed publication leaves nothing visible. A commit discards the uncommitted blocks it does not name, so when a concurrent upload of the key has discarded these blocks, the publication stages them again, up to ten times. The service reads the staging blob with a read-only service SAS on that one blob, signed in memory with the account key and valid for one hour; it is never logged or stored. If publication fails after the staging blob was committed, Complete with the same parts finds those blocks committed and publishes again without uploading the parts anew; the upload stays until publication succeeds. Concurrent uploads of one key both complete and the last to complete wins. Abort and expiry delete the staging blob and the manifest; a staging blob that holds only uncommitted blocks is first committed empty so that it can be deleted, and the service discards uncommitted blocks on its own after seven days. Deleting a container deletes its blobs, so `DeleteBucket` lists first and returns `ErrBucketNotEmpty`. That check races with a concurrent `Put`, and the race is documented, not hidden. `Configure` builds a container-scoped URL today (`azblob.NewContainerURL`), so the account-level `ServiceURL` is new.
- **local.** The bucket root is explicit because the alternative, the parent of `path`, would list unrelated directories as buckets. Without `bucketRoot`, `NewBucketManager("local", …)` fails with a typed configuration error. Parts are written with the same atomic write as `Put` (`writeFileAtomic`). Complete streams the parts in order into one atomic write of the final object and its metadata sidecar, then removes the upload directory.
- **memory.** `Memory.Configure` ignores its settings today (`pkg/memory/memory.go:54`), and each instance is one private namespace. The emulation keeps that: buckets are children of the instance that created them and are reached through `Bucket`. Nothing is process-global, so tests stay isolated.

### 5.3 Upload state, expiry and cleanup

| Item | Rule |
|---|---|
| Upload ID | Native on s3. Elsewhere 128 random bits, hex-encoded, generated at initiate. |
| Part numbers | 1..`MaxParts`. Re-uploading a part number replaces the part. |
| Part size | `MinPartSize` to `MaxPartSize`; only the final part may be smaller than the minimum, which is checked at complete. |
| Manifest | Emulated providers record key, metadata and initiation time at initiate (`upload.json` on local, a manifest object on gcs and azure, a struct on memory). |
| Expiry | A new `multipartExpiry` setting, default `168h`, which matches Azure's fixed seven-day discard of uncommitted blocks. An upload older than this is treated as aborted. |
| Cleanup | One facade helper, `AbortExpiredMultipartUploads(ctx, backendName string, olderThan time.Duration)` (proposed), runs `ListMultipartUploads` and then `AbortMultipartUpload`. local also runs it from its background lifecycle goroutine. On s3, an `AbortIncompleteMultipartUpload` lifecycle rule is documented as the provider-native alternative; objstore does not add the rule itself, because the bucket's lifecycle configuration belongs to its owner. |

### 5.4 Encryption at rest

- **local's at-rest encrypter** (`Local.SetAtRestEncrypterFactory`, `pkg/local/local.go:127`). Each part is encrypted when written, so no plaintext reaches the disk. Complete decrypts the parts in order and streams them through one `Encrypt` into the final object. The on-disk format is the same as `Put`'s, so `Get` does not change. The cost is one extra read and write of the object at complete.
- **The `encryptedStorage` wrapper.** `Encrypter.Encrypt` turns one plaintext stream into one ciphertext stream. Ciphertexts of separately encrypted parts, joined end to end, are not a stream that `Decrypt` can read. In the first tag the wrapper implements neither interface, so the facade returns `ErrNotSupported` for a wrapped backend. Later options are open question 3. The facade does not wrap backends today; the wrapper is applied by the replication syncer (`pkg/replication/syncer.go:103`) and by callers.
- **Provider-side encryption** (SSE on s3, the gcs and azure service defaults) applies to native multipart as it does to `Put`; objstore sets nothing.

### 5.5 Lifecycle and replication

- Staged parts and manifests are never objects. `Get`, `Exists`, `List`, `ListWithContext` and `ListWithOptions` never return them, and lifecycle policies never delete or archive them. On local, `walkObjects` skips only directories, `.metadata.json` sidecars and `.tmp-` files today (`pkg/local/local.go:809`), so it must skip the staging directory explicitly.
- On gcs and azure, staging lives under the reserved prefix `.objstore-multipart/` in the bucket. `ValidateKey` accepts keys with that prefix today, so a user key could collide with staging. The proposal makes `ValidateKey` reject that prefix (open question 2). A gcs native lifecycle rule with an empty prefix also matches staged parts. Rules set through `AddPolicy` exclude the reserved prefix where the provider allows it, and otherwise the expiry setting must stay below the rule's age.
- Replication sees only completed objects. local records one change event when an upload completes, through the same `ChangeLog` as `Put`, and none per part. Cloud providers replicate the completed object like any other. Uploads are never replicated, and an upload ID is never valid on a replica.
- Bucket operations take no part in lifecycle or replication. A policy is set on a `Storage` bound to one bucket, as it is today.

## 6. Credentials

Bucket management and multipart reuse the SDK client that `Configure` already built, through the same struct, so no new credential path is added. `Bucket(ctx, name)` exists so that a caller serving many buckets does not rebuild a `settings` map, secrets included, for each one. Upload IDs, manifests and error messages never carry credentials. Manifests hold only key, user metadata and timestamps, and user metadata passes through `ValidateMetadata` as on `Put`.

## 7. Out of scope for the first tag

The first tag ships the Go API only: `pkg/common`, the providers, `pkg/objstore` and `pkg/factory`. Each item below follows in its own change, with its own `api.md` and `cli.md` in this folder:

| Surface | Adds later |
|---|---|
| gRPC (`api/proto/objstore.proto`, `pkg/server/grpc`) | `CreateBucket`, `DeleteBucket`, `ListBuckets`; `CreateMultipartUpload`, client-streaming `UploadPart`, `CompleteMultipartUpload`, `AbortMultipartUpload`, `ListMultipartUploads`. The proto has no bucket or multipart RPC today. |
| REST (`pkg/server/rest`) | `PUT`, `DELETE` and `GET` on a bucket collection; multipart as `POST …?uploads`, `PUT …?partNumber=&uploadId=`, `POST …?uploadId=`, `DELETE …?uploadId=`, the S3 query shape go-quicbaas re-exports |
| QUIC (`pkg/server/quic`), Unix socket (`pkg/server/unix`) | the same operations as gRPC, one method each |
| MCP (`pkg/server/mcp`) | bucket tools only; multipart part bodies do not fit a tool call |
| C API (`cmd/objstorelib`) | `ObjstoreCreateBucket`, `ObjstoreDeleteBucket`, `ObjstoreListBuckets` and the five multipart calls over the existing opaque handles; exports today are `ObjstoreNewStorage`, `ObjstorePut`, `ObjstoreGet`, `ObjstoreDelete` and housekeeping |
| CLI and SDKs | verbs as the portfolio CLI surface defines them, then the client SDKs |

## 8. Alternatives considered

| Alternative | Rejected because |
|---|---|
| Widen `Storage` with the bucket and multipart methods | It breaks every implementer outside the providers: `encryptedStorage`, the storagefs and replication test doubles, and every consumer's fake. Each would grow methods that only return `ErrNotSupported`, and that is the optional interface again with a breaking change on top. |
| A separate package (`pkg/buckets`, `pkg/multipart`) with its own provider registry | The SDK clients are unexported fields of the provider structs (`S3.svc`, `GCS.client`, `Azure.container`). A separate package must either export them or build a second client from a second copy of the credentials. Both widen the credential surface, and the second option duplicates provider construction. |
| Bucket parameter on every multipart method (S3's wire shape) | Every `Storage` provider is bound to one bucket at `Configure`. A bucket parameter would let one instance write outside its configured bucket and bypass the policies and replication attached to it. `Bucket(ctx, name)` gives the same reach explicitly. |
| A whole-object `PutMultipart(ctx, key, io.Reader)` helper only | It does not meet the SPEC row. go-quicbaas re-exports part-by-part upload to clients that send parts in separate requests, possibly to different server instances. |

## 9. Open questions for the owner

1. Approve optional `BucketManager` and `MultipartUploader` interfaces over widening `Storage`, with the new `CodeUnimplemented` and `CodeFailedPrecondition` codes. Either way, SPEC.md's boundary "every provider satisfies [the common `Storage` interface] completely" should name the capability interfaces, or the rows should say they are optional per provider.
2. Reserve the key prefix `.objstore-multipart/` in `ValidateKey`. Rejecting keys that are accepted today is a behaviour change.
3. Multipart through `encryptedStorage`: unsupported in the first tag (recommended), or (a) the wrapper stages encrypted parts and re-encrypts at complete (double I/O, double egress on cloud), or (b) a chunked ciphertext format that `Get` reads part by part (changes the stored format).
4. Approve two additions beyond the SPEC rows' verbs: `BucketManager.Bucket`, for per-bucket `Storage` without re-passing secrets, and `ListMultipartUploads`, for one cleanup path. Recommended: both.
5. New settings: gcs `project`, local `bucketRoot`, and `multipartExpiry` (default `168h`) on every provider.
6. Bucket names: the portable set in §4 (recommended), or each provider's own rules.
7. Folder name: this folder covers both rows as `docs/buckets/`. Rename to match a SPEC feature name, or split per row, when the rows' Detail cells are filled.
