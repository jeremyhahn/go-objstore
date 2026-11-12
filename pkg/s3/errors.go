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

//go:build awss3

package s3

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/jeremyhahn/go-objstore/pkg/common"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// isObjectNotFound reports whether err is the SDK's answer for a missing
// object: NoSuchKey from GetObject or CopyObject, NotFound from HeadObject, or
// a 404 that carries no code at all (a HEAD answer has no body to carry one).
// A 404 with any other code, NoSuchBucket among them, is not a missing key.
func isObjectNotFound(err error) bool {
	var noSuchKey *types.NoSuchKey
	var notFound *types.NotFound
	if errors.As(err, &noSuchKey) || errors.As(err, &notFound) {
		return true
	}
	switch awsCode(err) {
	case codeNoSuchKey, codeNotFound:
		return true
	case "":
		var re *awshttp.ResponseError
		return errors.As(err, &re) && re.HTTPStatusCode() == http.StatusNotFound
	}
	return false
}

// objectError maps a missing-object error from the SDK to
// common.ErrKeyNotFound and a missing bucket (NoSuchBucket) to
// common.ErrBucketNotFound, keeping the SDK error in the chain so errors.As
// still reaches it. Any other error, and nil, is returned unchanged.
func objectError(err error, key string) error {
	if err == nil {
		return nil
	}
	if awsCode(err) == codeNoSuchBucket {
		return fmt.Errorf("%w: key %s: %w", common.ErrBucketNotFound, key, err)
	}
	if !isObjectNotFound(err) {
		return err
	}
	return fmt.Errorf("%w: %s: %w", common.ErrKeyNotFound, key, err)
}

// ErrUnknownStorageClass is the error for a storage class S3 does not know,
// or one a lifecycle transition cannot target. It wraps
// common.ErrInvalidArgument; errors.As reaches the *StorageClassError that
// names the setting and the class.
var ErrUnknownStorageClass = fmt.Errorf("%w: unknown S3 storage class", common.ErrInvalidArgument)

// ErrObjectTooLarge is the error for a Put larger than S3's largest object
// (common.MaxParts parts of common.MaxPartSize bytes, capped at 5 TiB).
var ErrObjectTooLarge = fmt.Errorf("%w: object larger than the S3 maximum", common.ErrInvalidArgument)

// StorageClassError names the setting that carried an unknown storage class.
type StorageClassError struct {
	Setting string // the settings key, e.g. "storage_class"
	Class   string // the value given
}

// Error implements error.
func (e *StorageClassError) Error() string {
	return fmt.Sprintf("%s: %s %q", ErrUnknownStorageClass, e.Setting, e.Class)
}

// Unwrap makes errors.Is(err, ErrUnknownStorageClass) and
// errors.Is(err, common.ErrInvalidArgument) true.
func (e *StorageClassError) Unwrap() error { return ErrUnknownStorageClass }
