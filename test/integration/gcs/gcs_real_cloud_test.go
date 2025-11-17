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

//go:build cloud_integration && gcpstorage

package gcs

import (
	"bytes"
	"context"
	"io"
	"os"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/jeremyhahn/go-objstore/pkg/factory"
	"google.golang.org/api/iterator"
)

// TestGCS_RealCloud tests against real Google Cloud Storage
// Set OBJSTORE_TEST_REAL_GCS=1 to enable
func TestGCS_RealCloud(t *testing.T) {
	if os.Getenv("OBJSTORE_TEST_REAL_GCS") != "1" {
		t.Skip("Skipping real GCS test. Set OBJSTORE_TEST_REAL_GCS=1 to enable")
	}

	bucket := os.Getenv("OBJSTORE_TEST_GCS_BUCKET")
	if bucket == "" {
		bucket = "go-objstore-integration-test"
	}

	t.Logf("Testing real GCS with bucket: %s", bucket)

	// Create GCS client
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		t.Fatalf("Failed to create GCS client: %v", err)
	}
	defer client.Close()

	bkt := client.Bucket(bucket)

	// Ensure bucket exists
	projectID := os.Getenv("GOOGLE_CLOUD_PROJECT")
	if projectID != "" {
		err = bkt.Create(ctx, projectID, &storage.BucketAttrs{
			Location: "US",
		})
		if err != nil {
			// Check if exists
			_, attrErr := bkt.Attrs(ctx)
			if attrErr != nil {
				t.Fatalf("Bucket '%s' does not exist and cannot be created: %v", bucket, err)
			}
		}
	} else {
		// Just verify it exists
		_, err := bkt.Attrs(ctx)
		if err != nil {
			t.Fatalf("Bucket '%s' does not exist. Set GOOGLE_CLOUD_PROJECT to create it", bucket)
		}
	}

	// Create storage
	stor, err := factory.NewStorage("gcs", map[string]string{
		"bucket": bucket,
	})
	if err != nil {
		t.Fatalf("Failed to create GCS storage: %v", err)
	}

	// Cleanup on exit
	defer func() {
		ctx := context.Background()
		client, _ := storage.NewClient(ctx)
		if client != nil {
			defer client.Close()

			bkt := client.Bucket(bucket)
			// Delete all test objects
			it := bkt.Objects(ctx, &storage.Query{Prefix: "test/"})
			for {
				attrs, err := it.Next()
				if err == iterator.Done {
					break
				}
				if err != nil {
					break
				}
				bkt.Object(attrs.Name).Delete(ctx)
			}

			// Delete the bucket
			bkt.Delete(ctx)
		}
	}()

	// Run tests
	t.Run("BasicPutGet", func(t *testing.T) {
		key := "test/real-cloud.txt"
		data := []byte("hello real GCS")

		err := stor.PutWithContext(ctx, key, bytes.NewReader(data))
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}

		reader, err := stor.GetWithContext(ctx, key)
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		defer reader.Close()

		got, _ := io.ReadAll(reader)
		if string(got) != string(data) {
			t.Errorf("Data mismatch: got %q, want %q", got, data)
		}

		// Cleanup
		stor.DeleteWithContext(ctx, key)
	})

	t.Run("LargeFile", func(t *testing.T) {
		key := "test/large.bin"
		size := 10 * 1024 * 1024 // 10MB
		data := make([]byte, size)
		for i := range data {
			data[i] = byte(i % 256)
		}

		err := stor.PutWithContext(ctx, key, bytes.NewReader(data))
		if err != nil {
			t.Fatalf("Put large file failed: %v", err)
		}

		reader, err := stor.GetWithContext(ctx, key)
		if err != nil {
			t.Fatalf("Get large file failed: %v", err)
		}
		defer reader.Close()

		got, _ := io.ReadAll(reader)
		if len(got) != size {
			t.Errorf("Size mismatch: got %d, want %d", len(got), size)
		}

		// Cleanup
		stor.DeleteWithContext(ctx, key)
	})
}
