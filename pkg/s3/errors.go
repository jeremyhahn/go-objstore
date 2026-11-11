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

	"github.com/aws/aws-sdk-go/aws/awserr" //nolint:staticcheck // Using v1 SDK, migration to v2 planned
	"github.com/aws/aws-sdk-go/service/s3" //nolint:staticcheck // Using v1 SDK, migration to v2 planned
)

// isObjectNotFound reports whether err is the SDK's answer for a missing
// object: NoSuchKey from GetObject or CopyObject, NotFound from HeadObject, or
// a 404 that carries no code at all (a HEAD answer has no body to carry one).
// A 404 with any other code, NoSuchBucket among them, is not a missing key.
func isObjectNotFound(err error) bool {
	switch awsCode(err) {
	case s3.ErrCodeNoSuchKey, codeNotFound:
		return true
	case "":
		var rf awserr.RequestFailure
		return errors.As(err, &rf) && rf.StatusCode() == http.StatusNotFound
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
	if awsCode(err) == s3.ErrCodeNoSuchBucket {
		return fmt.Errorf("%w: key %s: %w", common.ErrBucketNotFound, key, err)
	}
	if !isObjectNotFound(err) {
		return err
	}
	return fmt.Errorf("%w: %s: %w", common.ErrKeyNotFound, key, err)
}
