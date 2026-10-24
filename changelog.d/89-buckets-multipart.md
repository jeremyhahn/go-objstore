### Added

- Bucket management and multipart upload, as optional capabilities detected by
  type assertion; `Storage` is not widened. `common.BucketManager` offers
  `CreateBucket`, `DeleteBucket`, `ListBuckets` and `Bucket`;
  `common.MultipartUploader` offers `InitiateMultipartUpload`, `UploadPart`,
  `CompleteMultipartUpload`, `AbortMultipartUpload` and `ListMultipartUploads`.
  Both are implemented by the `memory`, `local`, `s3`, `gcs` and `azure`
  providers. The facade gains
  `CreateBucket`, `DeleteBucket`, `ListBuckets`, the five multipart functions
  and `AbortExpiredMultipartUploads`; each returns `common.ErrNotSupported`
  for a backend without the capability, including a backend wrapped by
  `common.NewEncryptedStorage`. `factory.RegisterBucketManager` and
  `factory.NewBucketManager` create a bucket manager by backend type. The
  design is in `docs/buckets/`.
- `common.ValidateBucketName` and the errors `ErrBucketNotFound`,
  `ErrBucketAlreadyExists`, `ErrBucketNotEmpty`, `ErrInvalidBucketName`,
  `ErrBucketRootNotSet`, `ErrUploadNotFound`, `ErrInvalidPartNumber`,
  `ErrPartTooSmall`, `ErrPartTooLarge`, `ErrInvalidPartOrder`,
  `ErrPartMismatch` and `ErrNotSupported`, with the error codes `CodeUnimplemented` and
  `CodeFailedPrecondition` and their HTTP (501, 412), gRPC and JSON-RPC
  mappings.
- Local settings `bucketRoot` (the directory whose subdirectories are the
  buckets) and `multipartExpiry` (how long an upload may stay open before it
  is treated as aborted; default `168h`). The lifecycle pass removes expired
  uploads.
- Cloud providers. `s3` uses native buckets and native multipart upload;
  `multipartExpiry` is enforced when an upload is completed, and
  `AbortExpiredMultipartUploads` aborts the older ones. `gcs` creates native
  buckets in the new `project` setting and emulates multipart upload with part
  objects and a manifest under the reserved prefix, completed through compose
  in a tree of at most 32 sources per call. `azure` manages containers through
  an account-level client built from `accountName` and `endpoint`, stages
  native blocks and commits the block list on complete; a manifest blob and
  one record per part under the reserved prefix hold the upload ID, the
  metadata and the part ETags. Every provider takes `multipartExpiry` (default
  `168h`), and `factory.NewBucketManager` creates the `s3`, `gcs` and `azure`
  managers without the bucket or container setting.
- `common.ErrProjectNotSet`, returned by the `gcs` bucket manager without a
  `project`.

### Changed

- `gcs` `GetMetadata` now also returns the custom metadata and the content
  encoding of an object.
- `azure` `DeleteBucket` lists the container first and returns
  `ErrBucketNotEmpty`, because the service itself would delete the blobs with
  it. A blob written between the check and the delete is lost with the
  container; stop writers before deleting a bucket.

- `common.ValidateKey` rejects keys under the prefix `.objstore-multipart/`,
  which the emulated multipart upload uses for staging. An object stored under
  that prefix by an earlier release can still be read but no longer be
  written.
- `common.ErrReplicationNotSupported` now wraps `common.ErrNotSupported`, so
  the server reports it as Unimplemented (HTTP 501, gRPC Unimplemented) where
  it reported an internal error before.
