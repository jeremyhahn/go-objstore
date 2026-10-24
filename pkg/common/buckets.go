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
	"fmt"
	"io"
	"strings"
	"time"
)

// BucketManager creates, lists and deletes buckets (containers on Azure,
// directories under a root on local). It is account-scoped: one value
// manages every bucket its credentials reach.
//
// It is an optional capability. Callers detect it with a type assertion;
// Storage is not widened.
type BucketManager interface {
	// CreateBucket creates the named bucket. It returns
	// ErrBucketAlreadyExists when the bucket exists.
	CreateBucket(ctx context.Context, name string) error

	// DeleteBucket removes the named bucket. It returns ErrBucketNotFound
	// when the bucket does not exist and ErrBucketNotEmpty when it still
	// holds objects or uploads.
	DeleteBucket(ctx context.Context, name string) error

	// ListBuckets returns every bucket the manager reaches, sorted by name.
	ListBuckets(ctx context.Context) ([]BucketInfo, error)

	// Bucket returns a Storage bound to the named bucket that reuses this
	// manager's client and credentials. It returns ErrBucketNotFound when
	// the bucket does not exist.
	Bucket(ctx context.Context, name string) (Storage, error)
}

// BucketInfo describes one bucket.
type BucketInfo struct {
	Name    string
	Created time.Time // zero when the provider does not report it
}

// MultipartUploader uploads one object in independently sent parts.
// Keys are object keys in the provider's configured bucket, validated by
// ValidateKey exactly as Put validates them.
//
// It is an optional capability. Callers detect it with a type assertion
// on the Storage; Storage is not widened.
type MultipartUploader interface {
	// InitiateMultipartUpload starts an upload and returns its ID. The
	// metadata is applied to the completed object as PutWithMetadata
	// applies it. An upload ID belongs to the provider instance that
	// issued it.
	InitiateMultipartUpload(ctx context.Context, key string, metadata *Metadata) (uploadID string, err error)

	// UploadPart stores one part. size is the exact part length; a reader
	// that yields a different length fails with ErrInvalidArgument.
	// Uploading a part number again replaces the part.
	UploadPart(ctx context.Context, key, uploadID string, partNumber int, data io.Reader, size int64) (PartInfo, error)

	// CompleteMultipartUpload assembles the listed parts, in order, into
	// the object. It is all-or-nothing: the object appears whole or not at
	// all, and on success the staged parts are gone.
	CompleteMultipartUpload(ctx context.Context, key, uploadID string, parts []CompletedPart) error

	// AbortMultipartUpload discards an upload and its staged parts.
	AbortMultipartUpload(ctx context.Context, key, uploadID string) error

	// ListMultipartUploads returns the uploads in progress whose key starts
	// with prefix, sorted by key and then by initiation time.
	ListMultipartUploads(ctx context.Context, prefix string) ([]MultipartUploadInfo, error)
}

// PartInfo describes one uploaded part.
type PartInfo struct {
	PartNumber int
	ETag       string // opaque; the caller passes it back unchanged
	Size       int64
}

// CompletedPart names one uploaded part at completion.
type CompletedPart struct {
	PartNumber int
	ETag       string
}

// MultipartUploadInfo describes one upload in progress.
type MultipartUploadInfo struct {
	Key       string
	UploadID  string
	Initiated time.Time
}

const (
	// MinPartSize is the smallest size of every part except the last.
	MinPartSize = 5 << 20
	// MaxPartSize is the largest size of any part.
	MaxPartSize = 5 << 30
	// MaxParts is the largest part number; part numbers run 1..MaxParts.
	MaxParts = 10000

	// MultipartPrefix is the key prefix reserved for staged multipart
	// state. ValidateKey rejects keys under it.
	MultipartPrefix = ".objstore-multipart/"

	// MinBucketNameLength and MaxBucketNameLength bound a bucket name.
	MinBucketNameLength = 3
	MaxBucketNameLength = 63
)

// ValidateBucketName checks a bucket name against the set every provider
// accepts: 3 to 63 characters, lowercase letters, digits and single hyphens,
// starting and ending with a letter or digit. The returned error wraps
// ErrInvalidBucketName.
func ValidateBucketName(name string) error {
	if n := len(name); n < MinBucketNameLength || n > MaxBucketNameLength {
		return bucketNameError("bucket name must be 3 to 63 characters")
	}
	if isBucketEdgeInvalid(name[0]) || isBucketEdgeInvalid(name[len(name)-1]) {
		return bucketNameError("bucket name must start and end with a lowercase letter or digit")
	}
	if strings.Contains(name, "--") {
		return bucketNameError("bucket name cannot contain consecutive hyphens")
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '-' {
			return bucketNameError("bucket name may contain only lowercase letters, digits and hyphens")
		}
	}
	return nil
}

func isBucketEdgeInvalid(c byte) bool {
	return (c < 'a' || c > 'z') && (c < '0' || c > '9')
}

// bucketNameError wraps ErrInvalidBucketName with the reason.
func bucketNameError(msg string) error {
	return fmt.Errorf("%w: %s", ErrInvalidBucketName, msg)
}
