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

//go:build gcpstorage

package gcs

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/jeremyhahn/go-objstore/pkg/common"
	"google.golang.org/api/iterator"
)

// Mock implementations for benchmarking
type mockGCSClient struct {
	bucket *mockGCSBucket
}

func (m *mockGCSClient) Bucket(name string) gcsBucket {
	return m.bucket
}

type mockGCSBucket struct {
	objects     map[string][]byte
	bucketAttrs *storage.BucketAttrs
}

func (m *mockGCSBucket) Object(name string) gcsObject {
	return &mockGCSObject{
		bucket: m,
		name:   name,
	}
}

func (m *mockGCSBucket) Objects(ctx context.Context, query *storage.Query) gcsIterator {
	var attrs []*storage.ObjectAttrs
	for key := range m.objects {
		attrs = append(attrs, &storage.ObjectAttrs{Name: key})
	}
	return &mockGCSIterator{attrs: attrs, index: 0}
}

func (m *mockGCSBucket) Attrs(ctx context.Context) (*storage.BucketAttrs, error) {
	if m.bucketAttrs == nil {
		m.bucketAttrs = &storage.BucketAttrs{
			Lifecycle: storage.Lifecycle{
				Rules: []storage.LifecycleRule{},
			},
		}
	}
	return m.bucketAttrs, nil
}

func (m *mockGCSBucket) Update(ctx context.Context, uattrs storage.BucketAttrsToUpdate) (*storage.BucketAttrs, error) {
	if m.bucketAttrs == nil {
		m.bucketAttrs = &storage.BucketAttrs{}
	}
	if uattrs.Lifecycle != nil {
		m.bucketAttrs.Lifecycle = *uattrs.Lifecycle
	}
	return m.bucketAttrs, nil
}

type mockGCSObject struct {
	bucket *mockGCSBucket
	name   string
}

func (m *mockGCSObject) NewWriter(ctx context.Context) io.WriteCloser {
	return &mockGCSWriter{
		bucket: m.bucket,
		name:   m.name,
		buf:    &bytes.Buffer{},
	}
}

func (m *mockGCSObject) NewReader(ctx context.Context) (io.ReadCloser, error) {
	data, ok := m.bucket.objects[m.name]
	if !ok {
		return nil, storage.ErrObjectNotExist
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (m *mockGCSObject) Delete(ctx context.Context) error {
	delete(m.bucket.objects, m.name)
	return nil
}

func (m *mockGCSObject) Attrs(ctx context.Context) (*storage.ObjectAttrs, error) {
	data, ok := m.bucket.objects[m.name]
	if !ok {
		return nil, storage.ErrObjectNotExist
	}
	return &storage.ObjectAttrs{
		Name: m.name,
		Size: int64(len(data)),
	}, nil
}

type mockGCSWriter struct {
	bucket *mockGCSBucket
	name   string
	buf    *bytes.Buffer
}

func (m *mockGCSWriter) Write(p []byte) (n int, err error) {
	return m.buf.Write(p)
}

func (m *mockGCSWriter) Close() error {
	m.bucket.objects[m.name] = m.buf.Bytes()
	return nil
}

type mockGCSIterator struct {
	attrs []*storage.ObjectAttrs
	index int
}

func (m *mockGCSIterator) Next() (*storage.ObjectAttrs, error) {
	if m.index >= len(m.attrs) {
		return nil, iterator.Done
	}
	attr := m.attrs[m.index]
	m.index++
	return attr, nil
}

func newMockGCS() *GCS {
	mockBucket := &mockGCSBucket{
		objects: make(map[string][]byte),
	}
	return &GCS{
		client: &mockGCSClient{
			bucket: mockBucket,
		},
		bucket: "test-bucket",
	}
}

func BenchmarkGCSPut(b *testing.B) {
	storage := newMockGCS()
	data := bytes.NewReader(make([]byte, 1024)) // 1KB

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = data.Seek(0, 0)
		if err := storage.Put("test-key", data); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGCSGet(b *testing.B) {
	storage := newMockGCS()
	testData := make([]byte, 1024) // 1KB
	storage.Put("test-key", bytes.NewReader(testData))

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		rc, err := storage.Get("test-key")
		if err != nil {
			b.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, rc)
		rc.Close()
	}
}

func BenchmarkGCSDelete(b *testing.B) {
	storage := newMockGCS()

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

func BenchmarkGCSList(b *testing.B) {
	storage := newMockGCS()

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

func BenchmarkGCSAddPolicy(b *testing.B) {
	storage := newMockGCS()
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

func BenchmarkGCSGetPolicies(b *testing.B) {
	storage := newMockGCS()

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

func BenchmarkGCSPutGetDelete(b *testing.B) {
	storage := newMockGCS()
	data := bytes.NewReader(make([]byte, 1024)) // 1KB

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Put
		_, _ = data.Seek(0, 0)
		if err := storage.Put("test-key", data); err != nil {
			b.Fatal(err)
		}

		// Get
		rc, err := storage.Get("test-key")
		if err != nil {
			b.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, rc)
		rc.Close()

		// Delete
		if err := storage.Delete("test-key"); err != nil {
			b.Fatal(err)
		}
	}
}
