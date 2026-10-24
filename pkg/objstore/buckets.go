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

package objstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/validation"
)

// namedBackend returns the named backend, or the default backend when the
// name is empty.
func namedBackend(backendName string) (common.Storage, error) {
	if backendName == "" {
		return DefaultBackend()
	}
	return Backend(backendName)
}

// bucketManager returns the bucket capability of the named backend, or
// common.ErrNotSupported when the backend does not offer it.
func bucketManager(backendName string) (common.BucketManager, error) {
	storage, err := namedBackend(backendName)
	if err != nil {
		return nil, err
	}
	bm, ok := storage.(common.BucketManager)
	if !ok {
		return nil, common.ErrNotSupported
	}
	return bm, nil
}

// uploaderFor returns the multipart capability of the named backend, or
// common.ErrNotSupported when the backend does not offer it.
func uploaderFor(backendName string) (common.MultipartUploader, error) {
	storage, err := namedBackend(backendName)
	if err != nil {
		return nil, err
	}
	return asUploader(storage)
}

// asUploader asserts the multipart capability of a storage.
func asUploader(storage common.Storage) (common.MultipartUploader, error) {
	mu, ok := storage.(common.MultipartUploader)
	if !ok {
		return nil, common.ErrNotSupported
	}
	return mu, nil
}

// uploaderForKey validates a key reference and returns the multipart
// capability of the backend it selects, with the key inside that backend.
func uploaderForKey(keyRef string) (common.MultipartUploader, string, error) {
	if err := validation.ValidateKeyReference(keyRef); err != nil {
		return nil, "", fmt.Errorf("invalid key reference: %w", err)
	}
	storage, key, err := getStorageForKey(keyRef)
	if err != nil {
		return nil, "", err
	}
	mu, err := asUploader(storage)
	if err != nil {
		return nil, "", err
	}
	return mu, key, nil
}

// validateBackendArg validates an optional backend name.
func validateBackendArg(backendName string) error {
	if backendName == "" {
		return nil
	}
	if err := validation.ValidateBackendName(backendName); err != nil {
		return fmt.Errorf("invalid backend name: %w", err)
	}
	return nil
}

// CreateBucket creates a bucket on the named backend, or on the default
// backend when the name is empty. It returns common.ErrNotSupported when the
// backend does not manage buckets.
func CreateBucket(ctx context.Context, backendName, bucket string) error {
	if err := validateBackendArg(backendName); err != nil {
		return err
	}
	if err := common.ValidateBucketName(bucket); err != nil {
		return err
	}
	bm, err := bucketManager(backendName)
	if err != nil {
		return err
	}
	return bm.CreateBucket(ctx, bucket)
}

// DeleteBucket removes an empty bucket from the named backend, or from the
// default backend when the name is empty.
func DeleteBucket(ctx context.Context, backendName, bucket string) error {
	if err := validateBackendArg(backendName); err != nil {
		return err
	}
	if err := common.ValidateBucketName(bucket); err != nil {
		return err
	}
	bm, err := bucketManager(backendName)
	if err != nil {
		return err
	}
	return bm.DeleteBucket(ctx, bucket)
}

// ListBuckets returns the buckets of the named backend, or of the default
// backend when the name is empty.
func ListBuckets(ctx context.Context, backendName string) ([]common.BucketInfo, error) {
	if err := validateBackendArg(backendName); err != nil {
		return nil, err
	}
	bm, err := bucketManager(backendName)
	if err != nil {
		return nil, err
	}
	return bm.ListBuckets(ctx)
}

// InitiateMultipartUpload starts a multipart upload for the key reference and
// returns its upload ID. The reference selects the backend as it does for
// Put.
func InitiateMultipartUpload(ctx context.Context, keyRef string, metadata *common.Metadata) (string, error) {
	mu, key, err := uploaderForKey(keyRef)
	if err != nil {
		return "", err
	}
	return mu.InitiateMultipartUpload(ctx, key, metadata)
}

// UploadPart stores one part of a multipart upload; size is the exact part
// length.
func UploadPart(ctx context.Context, keyRef, uploadID string, partNumber int, data io.Reader, size int64) (common.PartInfo, error) {
	mu, key, err := uploaderForKey(keyRef)
	if err != nil {
		return common.PartInfo{}, err
	}
	return mu.UploadPart(ctx, key, uploadID, partNumber, data, size)
}

// CompleteMultipartUpload assembles the listed parts into the object.
func CompleteMultipartUpload(ctx context.Context, keyRef, uploadID string, parts []common.CompletedPart) error {
	mu, key, err := uploaderForKey(keyRef)
	if err != nil {
		return err
	}
	return mu.CompleteMultipartUpload(ctx, key, uploadID, parts)
}

// AbortMultipartUpload discards a multipart upload and its parts.
func AbortMultipartUpload(ctx context.Context, keyRef, uploadID string) error {
	mu, key, err := uploaderForKey(keyRef)
	if err != nil {
		return err
	}
	return mu.AbortMultipartUpload(ctx, key, uploadID)
}

// ListMultipartUploads returns the uploads in progress on the named backend,
// or on the default backend when the name is empty, whose key starts with
// prefix.
func ListMultipartUploads(ctx context.Context, backendName, prefix string) ([]common.MultipartUploadInfo, error) {
	if err := validateBackendArg(backendName); err != nil {
		return nil, err
	}
	if err := validation.ValidatePrefix(prefix); err != nil {
		return nil, fmt.Errorf("invalid prefix: %w", err)
	}
	mu, err := uploaderFor(backendName)
	if err != nil {
		return nil, err
	}
	return mu.ListMultipartUploads(ctx, prefix)
}

// AbortExpiredMultipartUploads aborts the uploads of the backend that were
// initiated more than olderThan ago and returns how many it aborted. An
// upload another caller removed meanwhile is not an error. A failure to abort
// one upload does not stop the others; the failures are returned joined.
func AbortExpiredMultipartUploads(ctx context.Context, backendName string, olderThan time.Duration) (int, error) {
	if olderThan <= 0 {
		return 0, fmt.Errorf("%w: olderThan must be positive", common.ErrInvalidArgument)
	}
	if err := validateBackendArg(backendName); err != nil {
		return 0, err
	}
	mu, err := uploaderFor(backendName)
	if err != nil {
		return 0, err
	}
	uploads, err := mu.ListMultipartUploads(ctx, "")
	if err != nil {
		return 0, err
	}

	cutoff := time.Now().Add(-olderThan)
	aborted := 0
	var errs []error
	for _, u := range uploads {
		if !u.Initiated.Before(cutoff) {
			continue
		}
		err := mu.AbortMultipartUpload(ctx, u.Key, u.UploadID)
		switch {
		case err == nil:
			aborted++
		case errors.Is(err, common.ErrUploadNotFound):
		default:
			errs = append(errs, err)
		}
	}
	return aborted, errors.Join(errs...)
}
