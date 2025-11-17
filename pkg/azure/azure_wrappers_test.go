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
	"bytes"
	"context"
	"io"
	"net/url"
	"testing"

	"github.com/Azure/azure-storage-blob-go/azblob"
)

func TestAzure_Wrappers_Coverage(t *testing.T) {
	// Stub wrapper functions to avoid network
	oldUp, oldDn, oldDel := azureUploadFn, azureDownloadFn, azureDeleteFn
	azureUploadFn = func(_ context.Context, _ io.Reader, _ azblob.BlockBlobURL) error { return nil }
	azureDownloadFn = func(_ context.Context, _ azblob.BlockBlobURL) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewBufferString("ok")), nil
	}
	azureDeleteFn = func(_ context.Context, _ azblob.BlockBlobURL) error { return nil }
	defer func() { azureUploadFn, azureDownloadFn, azureDeleteFn = oldUp, oldDn, oldDel }()

	// Build a containerWrapper with a dummy URL
	u, _ := url.Parse("http://127.0.0.1:1/container")
	cw := containerWrapper{azblob.NewContainerURL(*u, azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{}))}
	bw := cw.NewBlockBlob("k").(blobWrapper)

	if err := bw.UploadFromReader(nil, bytes.NewBufferString("d")); err != nil {
		t.Fatalf("upload stubbed err: %v", err)
	}
	rc, err := bw.NewReader(nil)
	if err != nil {
		t.Fatalf("download stubbed err: %v", err)
	}
	rc.Close()
	if err := bw.Delete(nil); err != nil {
		t.Fatalf("delete stubbed err: %v", err)
	}
}

func TestContainerWrapper_ListBlobsFlat(t *testing.T) {
	// Stub the list function
	oldList := azureListFn
	azureListFn = func(_ context.Context, _ azblob.ContainerURL, prefix string) ([]string, error) {
		return []string{"file1.txt", "file2.txt"}, nil
	}
	defer func() { azureListFn = oldList }()

	u, _ := url.Parse("http://127.0.0.1:1/container")
	cw := containerWrapper{azblob.NewContainerURL(*u, azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{}))}

	keys, err := cw.ListBlobsFlat(context.Background(), "")
	if err != nil {
		t.Fatalf("ListBlobsFlat error: %v", err)
	}
	if len(keys) != 2 {
		t.Fatalf("expected 2 keys, got %d", len(keys))
	}
}

func TestBlobWrapper_GetProperties(t *testing.T) {
	// Stub the GetProperties function
	oldGetProps := azureGetPropertiesFn
	azureGetPropertiesFn = func(_ context.Context, _ azblob.BlockBlobURL) error {
		return nil
	}
	defer func() { azureGetPropertiesFn = oldGetProps }()

	u, _ := url.Parse("http://127.0.0.1:1/container/blob")
	bw := blobWrapper{azblob.NewBlockBlobURL(*u, azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{}))}

	err := bw.GetProperties(context.Background())
	if err != nil {
		t.Fatalf("GetProperties error: %v", err)
	}
}
