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

package azure

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/Azure/azure-storage-blob-go/azblob"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// missingBlobAzure returns an Azure whose every blob read answers BlobNotFound.
func missingBlobAzure() *Azure {
	return &Azure{container: &mockContainerEnhanced{
		newBlockBlobFn: func(string) BlobAPI {
			return &mockBlob{readFn: func(context.Context) (io.ReadCloser, error) {
				return nil, &fakeStorageError{code: azblob.ServiceCodeBlobNotFound}
			}}
		},
	}}
}

func TestAzure_Get_MissingKeyIsErrKeyNotFound(t *testing.T) {
	_, err := missingBlobAzure().Get("missing")
	if !errors.Is(err, common.ErrKeyNotFound) {
		t.Fatalf("expected common.ErrKeyNotFound, got %v", err)
	}
}

func TestAzure_GetWithContext_MissingKeyIsErrKeyNotFound(t *testing.T) {
	_, err := missingBlobAzure().GetWithContext(context.Background(), "missing")
	if !errors.Is(err, common.ErrKeyNotFound) {
		t.Fatalf("expected common.ErrKeyNotFound, got %v", err)
	}
}
