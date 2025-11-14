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

//go:build cloud_integration && azureblob

package azure

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"testing"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/jeremyhahn/go-objstore/pkg/factory"
)

// TestAzure_RealCloud tests against real Azure Blob Storage
// Set OBJSTORE_TEST_REAL_AZURE=1 to enable
func TestAzure_RealCloud(t *testing.T) {
	if os.Getenv("OBJSTORE_TEST_REAL_AZURE") != "1" {
		t.Skip("Skipping real Azure test. Set OBJSTORE_TEST_REAL_AZURE=1 to enable")
	}

	container := os.Getenv("OBJSTORE_TEST_AZURE_CONTAINER")
	accountName := os.Getenv("OBJSTORE_TEST_AZURE_ACCOUNT")
	accountKey := os.Getenv("OBJSTORE_TEST_AZURE_KEY")

	if accountName == "" {
		t.Fatal("OBJSTORE_TEST_AZURE_ACCOUNT must be set")
	}
	if accountKey == "" {
		t.Fatal("OBJSTORE_TEST_AZURE_KEY must be set")
	}
	if container == "" {
		container = "go-objstore-integration-test"
	}

	t.Logf("Testing real Azure with container: %s", container)

	// Create Azure credentials
	cred, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		t.Fatalf("Failed to create Azure credentials: %v", err)
	}

	p := azblob.NewPipeline(cred, azblob.PipelineOptions{})
	URL, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net/%s", accountName, container))
	containerURL := azblob.NewContainerURL(*URL, p)

	ctx := context.Background()

	// Ensure container exists
	_, err = containerURL.Create(ctx, azblob.Metadata{}, azblob.PublicAccessNone)
	if err != nil {
		_, propErr := containerURL.GetProperties(ctx, azblob.LeaseAccessConditions{})
		if propErr != nil {
			t.Fatalf("Container '%s' does not exist and cannot be created: %v", container, err)
		}
	}

	// Create storage
	storage, err := factory.NewStorage("azure", map[string]string{
		"accountName":   accountName,
		"accountKey":    accountKey,
		"containerName": container,
	})
	if err != nil {
		t.Fatalf("Failed to create Azure storage: %v", err)
	}

	// Cleanup on exit
	defer func() {
		ctx := context.Background()

		// Delete all test blobs
		for marker := (azblob.Marker{}); marker.NotDone(); {
			listBlob, err := containerURL.ListBlobsFlatSegment(ctx, marker, azblob.ListBlobsSegmentOptions{
				Prefix: "test/",
			})
			if err != nil {
				break
			}
			marker = listBlob.NextMarker

			for _, blobInfo := range listBlob.Segment.BlobItems {
				blobURL := containerURL.NewBlockBlobURL(blobInfo.Name)
				blobURL.Delete(ctx, azblob.DeleteSnapshotsOptionInclude, azblob.BlobAccessConditions{})
			}
		}

		// Delete the container
		containerURL.Delete(ctx, azblob.ContainerAccessConditions{})
	}()

	// Run tests
	t.Run("BasicPutGet", func(t *testing.T) {
		key := "test/real-cloud.txt"
		data := []byte("hello real Azure")

		err := storage.PutWithContext(ctx, key, bytes.NewReader(data))
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		reader, err := storage.GetWithContext(ctx, key)
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		defer reader.Close()

		got, _ := io.ReadAll(reader)
		if string(got) != string(data) {
			t.Errorf("Data mismatch: got %q, want %q", got, data)
		}

		// Cleanup
		storage.DeleteWithContext(ctx, key)
	})

	t.Run("LargeFile", func(t *testing.T) {
		key := "test/large.bin"
		size := 10 * 1024 * 1024 // 10MB
		data := make([]byte, size)
		for i := range data {
			data[i] = byte(i % 256)
		}

		err := storage.PutWithContext(ctx, key, bytes.NewReader(data))
		if err != nil {
			t.Fatalf("Put large file failed: %v", err)
		}

		reader, err := storage.GetWithContext(ctx, key)
		if err != nil {
			t.Fatalf("Get large file failed: %v", err)
		}
		defer reader.Close()

		got, _ := io.ReadAll(reader)
		if len(got) != size {
			t.Errorf("Size mismatch: got %d, want %d", len(got), size)
		}

		// Cleanup
		storage.DeleteWithContext(ctx, key)
	})
}
