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

//go:build integration

package common

import (
	"context"
	"crypto/rand"
	"net"
	"net/url"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	awss3 "github.com/aws/aws-sdk-go/service/s3"
)

// MustParse parses a URL string and panics on error
func MustParse(s string) *url.URL {
	u, _ := url.Parse(s)
	return u
}

// GenerateRandomData generates random data of specified size
func GenerateRandomData(size int) []byte {
	data := make([]byte, size)
	rand.Read(data)
	return data
}

// CreateMinIOBucket creates a bucket in MinIO for testing
func CreateMinIOBucket(t *testing.T, bucket string) *session.Session {
	t.Helper()
	endpoint := os.Getenv("S3_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://minio:9000"
	}

	sess, err := session.NewSession(&aws.Config{
		Region:           aws.String("us-east-1"),
		Endpoint:         aws.String(endpoint),
		S3ForcePathStyle: aws.Bool(true),
		Credentials:      credentials.NewStaticCredentials("minioadmin", "minioadmin", ""),
	})
	if err != nil {
		t.Fatal(err)
	}
	s3c := awss3.New(sess)
	_, _ = s3c.CreateBucket(&awss3.CreateBucketInput{Bucket: aws.String(bucket)})
	return sess
}

// CreateAzuriteContainer creates a container in Azurite for testing
func CreateAzuriteContainer(t *testing.T, container string) {
	t.Helper()
	account := "devstoreaccount1"
	key := "bXlrZXk="
	endpoint := os.Getenv("AZURE_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://azurite:10000/" + account
	} else {
		endpoint = endpoint + "/" + account
	}

	cred, err := azblob.NewSharedKeyCredential(account, key)
	if err != nil {
		t.Fatal(err)
	}
	p := azblob.NewPipeline(cred, azblob.PipelineOptions{})
	uParsed, _ := url.Parse(endpoint + "/" + container)
	cu := azblob.NewContainerURL(*uParsed, p)
	for i := 0; i < 10; i++ {
		_, err = cu.Create(context.Background(), azblob.Metadata{}, azblob.PublicAccessNone)
		if err == nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// CreateGCSBucket creates a bucket in GCS emulator for testing
func CreateGCSBucket(t *testing.T, bucket string) *storage.Client {
	t.Helper()
	host := os.Getenv("STORAGE_EMULATOR_HOST")
	if host == "" {
		t.Skip("STORAGE_EMULATOR_HOST not set")
	}
	u, _ := url.Parse(host)
	addr := u.Host
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Skipf("fake-gcs not reachable at %s: %v", addr, err)
	}
	_ = conn.Close()

	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		t.Skip("gcs client init failed in emulator env")
	}
	_ = client.Bucket(bucket).Create(ctx, "test-proj", nil)
	return client
}

// WaitForService waits for a service to be available
func WaitForService(t *testing.T, addr string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 1*time.Second)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("service at %s not available after %v", addr, timeout)
}
