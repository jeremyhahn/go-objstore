// Copyright (c) 2025 Jeremy Hahn
// Copyright (c) 2025 Automate The Things, LLC
//
// This file is part of go-objstore.
//
// go-objstore is dual-licensed:
//
// 1. GNU Affero General Public License v3.0 (AGPL-3.0)
//    See LICENSE file or visit https://www.gnu.org/licenses/agpl-3.0.html
//
// 2. Commercial License
//    Contact licensing@automatethethings.com for commercial licensing options.

package common

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
)

var (
	// Configuration errors

	// ErrNotConfigured is returned when a storage backend is not properly configured.
	ErrNotConfigured = errors.New("not configured")

	// ErrPathNotSet is returned when the required path is not set.
	ErrPathNotSet = errors.New("path not set")

	// ErrBucketNotSet is returned when the required bucket is not set.
	ErrBucketNotSet = errors.New("bucket not set")

	// ErrAccountNotSet is returned when required account credentials are not set.
	ErrAccountNotSet = errors.New("accountName, accountKey, or containerName not set")

	// ErrVaultNotSet is returned when the required vault name is not set.
	ErrVaultNotSet = errors.New("vaultName not set")

	// ErrRegionNotSet is returned when the required region is not set.
	ErrRegionNotSet = errors.New("region not set")

	// ErrContainerNotSet is returned when the required container name is not set.
	ErrContainerNotSet = errors.New("containerName not set")

	// ErrEndpointNotSet is returned when the required endpoint is not set.
	ErrEndpointNotSet = errors.New("endpoint not set")

	// ErrAccessKeyNotSet is returned when the required access key is not set.
	ErrAccessKeyNotSet = errors.New("accessKey not set")

	// ErrSecretKeyNotSet is returned when the required secret key is not set.
	ErrSecretKeyNotSet = errors.New("secretKey not set")

	// ErrInvalidLifecycleManagerType is returned when an invalid lifecycle manager type is specified.
	ErrInvalidLifecycleManagerType = errors.New("invalid lifecycleManagerType")

	// Storage operation errors

	// ErrStorageRequired is returned when a storage backend is required but not provided.
	ErrStorageRequired = errors.New("storage backend is required")

	// ErrArchiveDestinationNil is returned when the archive destination is nil.
	ErrArchiveDestinationNil = errors.New("archive destination cannot be nil")

	// ErrInvalidStorageHandle is returned when an invalid storage handle is provided.
	ErrInvalidStorageHandle = errors.New("invalid storage handle")

	// ErrBufferTooSmall is returned when a buffer is too small for the requested operation.
	ErrBufferTooSmall = errors.New("buffer too small")

	// ErrKeyNotFound is returned when a key is not found in storage.
	ErrKeyNotFound = errors.New("key not found")

	// ErrObjectChanged is returned by a conditional operation when the object
	// was replaced or updated after the revision the caller observed.
	ErrObjectChanged = errors.New("object changed since it was observed")

	// ErrMetadataNotFound is returned when metadata is not found for a key.
	ErrMetadataNotFound = errors.New("metadata not found for key")

	// ErrInternal is returned for internal errors during operations.
	ErrInternal = errors.New("internal error")

	// Lifecycle policy errors

	// ErrPolicyNil is returned when a policy is nil.
	ErrPolicyNil = errors.New("policy cannot be nil")

	// ErrDestinationTypeRequired is returned when destination_type is required for archive action.
	ErrDestinationTypeRequired = errors.New("destination_type required for archive action")

	// Replication policy errors

	// ErrPolicyNotFound is returned when a replication policy is not found.
	ErrPolicyNotFound = errors.New("replication policy not found")

	// Canonical cross-transport sentinels. Backends and services wrap these so
	// every transport (REST, gRPC, QUIC, MCP, unix) maps errors consistently
	// via Classify.

	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = errors.New("already exists")

	// ErrInvalidArgument is returned when a request argument is invalid.
	ErrInvalidArgument = errors.New("invalid argument")

	// ErrPermissionDenied is returned when the caller is not authorized.
	ErrPermissionDenied = errors.New("permission denied")

	// ErrUnauthenticated is returned when the caller is not authenticated.
	ErrUnauthenticated = errors.New("unauthenticated")

	// ErrResourceExhausted is returned when a rate or quota limit is exceeded.
	ErrResourceExhausted = errors.New("resource exhausted")

	// ErrUnavailable is returned when a backend or dependency is unavailable.
	ErrUnavailable = errors.New("unavailable")

	// ErrNotSupported is returned when the backend lacks a capability, for
	// example when a provider implements neither BucketManager nor
	// MultipartUploader, or when an archive-only type is asked for one.
	ErrNotSupported = errors.New("common: operation not supported")

	// ErrBucketNotFound is returned when a bucket does not exist.
	ErrBucketNotFound = errors.New("common: bucket not found")

	// ErrBucketAlreadyExists is returned when creating a bucket that exists.
	ErrBucketAlreadyExists = fmt.Errorf("common: bucket already exists: %w", ErrAlreadyExists)

	// ErrBucketNotEmpty is returned when deleting a bucket that still holds
	// objects or uploads.
	ErrBucketNotEmpty = errors.New("common: bucket not empty")

	// ErrUploadNotFound is returned for an unknown, completed, aborted or
	// expired multipart upload ID.
	ErrUploadNotFound = errors.New("common: multipart upload not found")

	// ErrInvalidBucketName is returned when a bucket name fails
	// ValidateBucketName.
	ErrInvalidBucketName = fmt.Errorf("common: invalid bucket name: %w", ErrInvalidArgument)

	// ErrBucketRootNotSet is returned when a provider that emulates buckets
	// over a directory is asked for a bucket manager without a bucketRoot
	// setting.
	ErrBucketRootNotSet = fmt.Errorf("common: bucketRoot not set: %w", ErrInvalidArgument)

	// ErrProjectNotSet is returned when a provider that creates and lists
	// buckets within a project is asked to do so without a project setting.
	ErrProjectNotSet = fmt.Errorf("common: project not set: %w", ErrInvalidArgument)

	// ErrInvalidPartNumber is returned when a part number is outside
	// 1..MaxParts.
	ErrInvalidPartNumber = fmt.Errorf("common: invalid part number: %w", ErrInvalidArgument)

	// ErrPartTooSmall is returned when a part other than the last is
	// smaller than MinPartSize.
	ErrPartTooSmall = fmt.Errorf("common: part too small: %w", ErrInvalidArgument)

	// ErrPartTooLarge is returned when a part is larger than MaxPartSize.
	ErrPartTooLarge = fmt.Errorf("common: part too large: %w", ErrInvalidArgument)

	// ErrInvalidPartOrder is returned when the parts given at completion are
	// not in strictly ascending part-number order, or are empty.
	ErrInvalidPartOrder = fmt.Errorf("common: invalid part order: %w", ErrInvalidArgument)

	// ErrPartMismatch is returned when an ETag or part number given at
	// completion does not match an uploaded part.
	ErrPartMismatch = fmt.Errorf("common: part mismatch: %w", ErrInvalidArgument)
)

// ErrorCode is the canonical classification of an error, independent of
// transport. Per-transport mappers translate it to HTTP statuses, gRPC codes,
// and JSON-RPC error codes.
type ErrorCode int

const (
	// CodeInternal classifies unrecognized errors. Keep as zero value so an
	// unset code maps to the safest default.
	CodeInternal ErrorCode = iota
	// CodeNotFound classifies missing objects, metadata, and policies.
	CodeNotFound
	// CodeAlreadyExists classifies conflicts with an existing resource.
	CodeAlreadyExists
	// CodeInvalidArgument classifies invalid request parameters.
	CodeInvalidArgument
	// CodePermissionDenied classifies authorization denials.
	CodePermissionDenied
	// CodeUnauthenticated classifies authentication failures.
	CodeUnauthenticated
	// CodeResourceExhausted classifies rate/quota limit errors.
	CodeResourceExhausted
	// CodeUnavailable classifies backend/dependency unavailability.
	CodeUnavailable
	// CodeCanceled classifies request cancellation.
	CodeCanceled
	// CodeDeadlineExceeded classifies timeouts.
	CodeDeadlineExceeded
	// CodeUnimplemented classifies a capability the backend does not have.
	CodeUnimplemented
	// CodeFailedPrecondition classifies a request that conflicts with the
	// current state of the resource, such as deleting a non-empty bucket.
	CodeFailedPrecondition
)

// Classify maps an error to its canonical ErrorCode. Matching uses errors.Is
// exclusively, so producers must wrap (or be) the canonical sentinels above,
// the std fs sentinels, or the context errors for classification to work.
func Classify(err error) ErrorCode {
	if err == nil {
		return CodeInternal
	}

	switch {
	case errors.Is(err, ErrKeyNotFound),
		errors.Is(err, ErrMetadataNotFound),
		errors.Is(err, ErrPolicyNotFound),
		errors.Is(err, ErrBucketNotFound),
		errors.Is(err, ErrUploadNotFound),
		// Raw filesystem not-found errors leaked by backends.
		errors.Is(err, fs.ErrNotExist):
		return CodeNotFound
	case errors.Is(err, ErrNotSupported):
		return CodeUnimplemented
	case errors.Is(err, ErrBucketNotEmpty):
		return CodeFailedPrecondition
	case errors.Is(err, ErrAlreadyExists):
		return CodeAlreadyExists
	case errors.Is(err, ErrInvalidArgument):
		return CodeInvalidArgument
	case errors.Is(err, ErrPermissionDenied),
		// Raw filesystem permission errors leaked by backends.
		errors.Is(err, fs.ErrPermission):
		return CodePermissionDenied
	case errors.Is(err, ErrUnauthenticated):
		return CodeUnauthenticated
	case errors.Is(err, ErrResourceExhausted):
		return CodeResourceExhausted
	case errors.Is(err, ErrUnavailable):
		return CodeUnavailable
	case errors.Is(err, context.Canceled):
		return CodeCanceled
	case errors.Is(err, context.DeadlineExceeded):
		return CodeDeadlineExceeded
	default:
		return CodeInternal
	}
}
