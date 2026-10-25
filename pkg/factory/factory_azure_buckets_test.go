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

//go:build azureblob

package factory

import (
	"encoding/base64"
	"errors"
	"testing"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

func TestNewBucketManager_Azure(t *testing.T) {
	key := base64.StdEncoding.EncodeToString([]byte("key"))

	// The container setting is not required: the manager serves every container.
	bm, err := NewBucketManager("azure", map[string]string{"accountName": "acct"}, keyCreds(t, key))
	if err != nil || bm == nil {
		t.Fatalf("NewBucketManager = %v, %v", bm, err)
	}
	if _, ok := bm.(common.MultipartUploader); !ok {
		t.Error("the azure manager does not offer multipart upload")
	}

	bm, err = NewBucketManager("azure", map[string]string{"accountName": "acct"}, noCreds)
	if !errors.Is(err, common.ErrAccountNotSet) || bm != nil {
		t.Errorf("no key = %v, %v", bm, err)
	}

	bm, err = NewBucketManager("azure", map[string]string{"accountName": "acct", "multipartExpiry": "x"}, keyCreds(t, key))
	if !errors.Is(err, common.ErrInvalidArgument) || bm != nil {
		t.Errorf("bad expiry = %v, %v", bm, err)
	}
}
