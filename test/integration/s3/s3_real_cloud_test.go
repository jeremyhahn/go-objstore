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

//go:build cloud_integration && awss3

package s3

import (
	"bytes"
	"context"
	"io"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	awss3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/jeremyhahn/go-objstore/pkg/factory"
)

// TestS3_RealCloud tests against real AWS S3
// Set OBJSTORE_TEST_REAL_S3=1 to enable
func TestS3_RealCloud(t *testing.T) {
	if os.Getenv("OBJSTORE_TEST_REAL_S3") != "1" {
		t.Skip("Skipping real AWS S3 test. Set OBJSTORE_TEST_REAL_S3=1 to enable")
	}

	bucket := os.Getenv("OBJSTORE_TEST_S3_BUCKET")
	region := os.Getenv("OBJSTORE_TEST_S3_REGION")
	if region == "" {
		region = "us-east-1"
	}
	if bucket == "" {
		bucket = "go-objstore-integration-test"
	}

	t.Logf("Testing real AWS S3 with bucket: %s", bucket)

	// Ensure bucket exists
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(region),
	})
	if err != nil {
		t.Fatalf("Failed to create AWS session: %v", err)
	}

	svc := awss3.New(sess)
	_, err = svc.CreateBucket(&awss3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		// Check if bucket exists
		_, headErr := svc.HeadBucket(&awss3.HeadBucketInput{
			Bucket: aws.String(bucket),
		})
		if headErr != nil {
			t.Fatalf("Bucket '%s' does not exist and cannot be created: %v", bucket, err)
		}
	}

	// Create storage
	storage, err := factory.NewStorage("s3", map[string]string{
		"bucket": bucket,
		"region": region,
	})
	if err != nil {
		t.Fatalf("Failed to create S3 storage: %v", err)
	}

	ctx := context.Background()

	// Cleanup on exit
	defer func() {
		// Delete all test objects
		listResp, _ := svc.ListObjectsV2(&awss3.ListObjectsV2Input{
			Bucket: aws.String(bucket),
			Prefix: aws.String("test/"),
		})
		for _, obj := range listResp.Contents {
			svc.DeleteObject(&awss3.DeleteObjectInput{
				Bucket: aws.String(bucket),
				Key:    obj.Key,
			})
		}

		// Delete the bucket
		svc.DeleteBucket(&awss3.DeleteBucketInput{
			Bucket: aws.String(bucket),
		})
	}()

	// Run tests
	t.Run("BasicPutGet", func(t *testing.T) {
		key := "test/real-cloud.txt"
		data := []byte("hello real AWS S3")

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
