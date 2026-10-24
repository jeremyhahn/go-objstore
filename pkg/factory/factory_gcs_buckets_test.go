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

//go:build gcpstorage

package factory

import (
	"errors"
	"testing"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

func TestNewBucketManager_GCS(t *testing.T) {
	// The bucket setting is not required: the manager serves every bucket.
	bm, err := NewBucketManager("gcs", map[string]string{"project": "proj", "skip_client": "true"})
	if err != nil || bm == nil {
		t.Fatalf("NewBucketManager = %v, %v", bm, err)
	}
	if _, ok := bm.(common.MultipartUploader); !ok {
		t.Error("the gcs manager does not offer multipart upload")
	}

	bm, err = NewBucketManager("gcs", map[string]string{"skip_client": "true"})
	if !errors.Is(err, common.ErrProjectNotSet) || bm != nil {
		t.Errorf("no project = %v, %v", bm, err)
	}

	bm, err = NewBucketManager("gcs", map[string]string{"project": "p", "skip_client": "true", "multipartExpiry": "x"})
	if !errors.Is(err, common.ErrInvalidArgument) || bm != nil {
		t.Errorf("bad expiry = %v, %v", bm, err)
	}
}
