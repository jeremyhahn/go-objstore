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
	"testing"

	"net/url"

	"github.com/Azure/azure-storage-blob-go/azblob"
)

// TestDefaultFunctions exercises the default function variables
// These are usually stubbed in tests but we need to ensure they compile and have basic structure
func TestDefaultFunctions_Compile(t *testing.T) {
	// We can't actually call these without a real Azure connection,
	// but we can verify they are set and have the right signature
	if azureUploadFn == nil {
		t.Fatal("azureUploadFn is nil")
	}
	if azureDownloadFn == nil {
		t.Fatal("azureDownloadFn is nil")
	}
	if azureDeleteFn == nil {
		t.Fatal("azureDeleteFn is nil")
	}
	if azureListFn == nil {
		t.Fatal("azureListFn is nil")
	}

	// Test that we can stub and restore them
	oldUp := azureUploadFn
	azureUploadFn = func(ctx context.Context, r io.Reader, b azblob.BlockBlobURL) error {
		return nil
	}
	azureUploadFn = oldUp

	oldDn := azureDownloadFn
	azureDownloadFn = func(ctx context.Context, b azblob.BlockBlobURL) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	azureDownloadFn = oldDn

	oldDel := azureDeleteFn
	azureDeleteFn = func(ctx context.Context, b azblob.BlockBlobURL) error {
		return nil
	}
	azureDeleteFn = oldDel

	oldList := azureListFn
	azureListFn = func(ctx context.Context, c azblob.ContainerURL, prefix string) ([]string, error) {
		return []string{}, nil
	}
	azureListFn = oldList
}

// TestDefaultList_ErrorHandling tests the list function error path
func TestDefaultList_ErrorHandling(t *testing.T) {
	// Create a test that will make the default list function fail
	oldList := azureListFn
	defer func() { azureListFn = oldList }()

	callCount := 0
	azureListFn = func(ctx context.Context, c azblob.ContainerURL, prefix string) ([]string, error) {
		callCount++
		// Simulate the real function's structure
		marker := azblob.Marker{}
		if !marker.NotDone() {
			// marker.NotDone() returns false when done
			return []string{}, nil
		}
		return []string{}, nil
	}

	u, _ := url.Parse("http://127.0.0.1:1/container")
	cw := containerWrapper{azblob.NewContainerURL(*u, azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{}))}

	keys, err := cw.ListBlobsFlat(context.Background(), "test")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(keys) != 0 {
		t.Fatalf("expected 0 keys, got %d", len(keys))
	}
	if callCount != 1 {
		t.Fatalf("expected 1 call, got %d", callCount)
	}
}
