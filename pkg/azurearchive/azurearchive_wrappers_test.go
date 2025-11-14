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

//go:build azurearchive

package azurearchive

import (
	"bytes"
	"context"
	"io"
	"net/url"
	"testing"

	"github.com/Azure/azure-storage-blob-go/azblob"
)

func TestAzureArchive_Wrapper_Coverage(t *testing.T) {
	old := azureArchUploadFn
	azureArchUploadFn = func(_ context.Context, _ io.Reader, _ azblob.BlockBlobURL) error { return nil }
	defer func() { azureArchUploadFn = old }()

	u, _ := url.Parse("http://127.0.0.1:1/container")
	cw := containerWrapper{azblob.NewContainerURL(*u, azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{}))}
	bw := cw.NewBlockBlob("k").(blobWrapper)
	if err := bw.UploadFromReader(nil, bytes.NewBufferString("d")); err != nil {
		t.Fatalf("stubbed upload err: %v", err)
	}
}
