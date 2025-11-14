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

//go:build minio

package minio

import (
	"bytes"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// benchS3Client is a mock S3 client for benchmarking MinIO
type benchS3Client struct {
	s3iface.S3API
	objects map[string][]byte
}

func (m *benchS3Client) PutObject(input *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
	data, err := io.ReadAll(input.Body)
	if err != nil {
		return nil, err
	}
	m.objects[*input.Key] = data
	return &s3.PutObjectOutput{
		ETag: aws.String("mock-etag"),
	}, nil
}

func (m *benchS3Client) GetObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	data, ok := m.objects[*input.Key]
	if !ok {
		return nil, fmt.Errorf("key not found")
	}
	return &s3.GetObjectOutput{
		Body: io.NopCloser(bytes.NewReader(data)),
	}, nil
}

func (m *benchS3Client) DeleteObject(input *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
	delete(m.objects, *input.Key)
	return &s3.DeleteObjectOutput{}, nil
}

func (m *benchS3Client) ListObjectsV2(input *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
	var contents []*s3.Object
	for key := range m.objects {
		contents = append(contents, &s3.Object{
			Key: aws.String(key),
		})
	}
	return &s3.ListObjectsV2Output{
		Contents:    contents,
		IsTruncated: aws.Bool(false),
	}, nil
}

func newMockMinIO() *MinIO {
	return &MinIO{
		svc:    &benchS3Client{objects: make(map[string][]byte)},
		bucket: "test-bucket",
	}
}

func BenchmarkMinIOPut(b *testing.B) {
	storage := newMockMinIO()
	data := bytes.NewReader(make([]byte, 1024)) // 1KB

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		data.Seek(0, 0)
		if err := storage.Put("test-key", data); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMinIOGet(b *testing.B) {
	storage := newMockMinIO()
	testData := make([]byte, 1024) // 1KB
	storage.Put("test-key", bytes.NewReader(testData))

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		rc, err := storage.Get("test-key")
		if err != nil {
			b.Fatal(err)
		}
		io.Copy(io.Discard, rc)
		rc.Close()
	}
}

func BenchmarkMinIODelete(b *testing.B) {
	storage := newMockMinIO()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		storage.Put("test-key", bytes.NewReader([]byte("test")))
		b.StartTimer()

		if err := storage.Delete("test-key"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMinIOList(b *testing.B) {
	storage := newMockMinIO()

	// Pre-populate with test objects
	for i := 0; i < 100; i++ {
		key := "test/object"
		storage.Put(key, bytes.NewReader([]byte("test")))
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := storage.List("test/")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMinIOAddPolicy(b *testing.B) {
	storage := newMockMinIO()
	policy := common.LifecyclePolicy{
		ID:        "bench-policy",
		Action:    "delete",
		Retention: 30 * 24 * time.Hour,
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := storage.AddPolicy(policy); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMinIOGetPolicies(b *testing.B) {
	storage := newMockMinIO()

	// Add some policies
	for i := 0; i < 10; i++ {
		storage.AddPolicy(common.LifecyclePolicy{
			ID:        "policy-0",
			Action:    "delete",
			Retention: 30 * 24 * time.Hour,
		})
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := storage.GetPolicies()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMinIOPutGetDelete(b *testing.B) {
	storage := newMockMinIO()
	data := bytes.NewReader(make([]byte, 1024)) // 1KB

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Put
		data.Seek(0, 0)
		if err := storage.Put("test-key", data); err != nil {
			b.Fatal(err)
		}

		// Get
		rc, err := storage.Get("test-key")
		if err != nil {
			b.Fatal(err)
		}
		io.Copy(io.Discard, rc)
		rc.Close()

		// Delete
		if err := storage.Delete("test-key"); err != nil {
			b.Fatal(err)
		}
	}
}
