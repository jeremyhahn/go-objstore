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

package factory

import (
	"errors"
	"testing"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

func TestNewBucketManager_S3(t *testing.T) {
	// The bucket setting is not required: the manager serves every bucket.
	bm, err := NewBucketManager("s3", map[string]string{
		"region":   "eu-west-1",
		"endpoint": "http://127.0.0.1:1",
	})
	if err != nil || bm == nil {
		t.Fatalf("NewBucketManager = %v, %v", bm, err)
	}
	if _, ok := bm.(common.MultipartUploader); !ok {
		t.Error("the s3 manager does not offer multipart upload")
	}

	bm, err = NewBucketManager("s3", map[string]string{"multipartExpiry": "x"})
	if !errors.Is(err, common.ErrInvalidArgument) || bm != nil {
		t.Errorf("bad expiry = %v, %v", bm, err)
	}
}
