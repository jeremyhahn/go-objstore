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
	azureGetPropertiesFn = func(_ context.Context, _ azblob.BlockBlobURL) (*BlobProperties, error) {
		return &BlobProperties{Size: 42, ContentType: "text/plain"}, nil
	}
	defer func() { azureGetPropertiesFn = oldGetProps }()

	u, _ := url.Parse("http://127.0.0.1:1/container/blob")
	bw := blobWrapper{azblob.NewBlockBlobURL(*u, azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{}))}

	props, err := bw.GetProperties(context.Background())
	if err != nil {
		t.Fatalf("GetProperties error: %v", err)
	}
	if props == nil || props.Size != 42 || props.ContentType != "text/plain" {
		t.Fatalf("GetProperties returned unexpected properties: %+v", props)
	}
}

func TestBlobWrapper_SetMetadataAndHeaders(t *testing.T) {
	// Stub the setter functions
	oldSetMeta, oldSetHeaders := azureSetMetadataFn, azureSetHTTPHeadersFn
	var gotMetadata map[string]string
	var gotHeaders azblob.BlobHTTPHeaders
	azureSetMetadataFn = func(_ context.Context, _ azblob.BlockBlobURL, metadata map[string]string) error {
		gotMetadata = metadata
		return nil
	}
	azureSetHTTPHeadersFn = func(_ context.Context, _ azblob.BlockBlobURL, headers azblob.BlobHTTPHeaders) error {
		gotHeaders = headers
		return nil
	}
	defer func() { azureSetMetadataFn, azureSetHTTPHeadersFn = oldSetMeta, oldSetHeaders }()

	u, _ := url.Parse("http://127.0.0.1:1/container/blob")
	bw := blobWrapper{azblob.NewBlockBlobURL(*u, azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{}))}

	if err := bw.SetMetadata(context.Background(), map[string]string{"k": "v"}); err != nil {
		t.Fatalf("SetMetadata error: %v", err)
	}
	if gotMetadata["k"] != "v" {
		t.Fatalf("SetMetadata passed unexpected metadata: %v", gotMetadata)
	}

	headers := azblob.BlobHTTPHeaders{ContentType: "text/plain"}
	if err := bw.SetHTTPHeaders(context.Background(), headers); err != nil {
		t.Fatalf("SetHTTPHeaders error: %v", err)
	}
	if gotHeaders.ContentType != "text/plain" {
		t.Fatalf("SetHTTPHeaders passed unexpected headers: %+v", gotHeaders)
	}
}
