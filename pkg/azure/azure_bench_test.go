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
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// Mock implementations for benchmarking
type mockBlobAPI struct {
	data []byte
}

func (m *mockBlobAPI) UploadFromReader(ctx context.Context, r io.Reader) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	m.data = data
	return nil
}

func (m *mockBlobAPI) NewReader(ctx context.Context) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(m.data)), nil
}

func (m *mockBlobAPI) Delete(ctx context.Context) error {
	m.data = nil
	return nil
}

func (m *mockBlobAPI) GetProperties(ctx context.Context) error {
	return nil
}

type mockContainerAPI struct {
	blobs map[string]*mockBlobAPI
}

func (m *mockContainerAPI) NewBlockBlob(name string) BlobAPI {
	if m.blobs == nil {
		m.blobs = make(map[string]*mockBlobAPI)
	}
	if _, ok := m.blobs[name]; !ok {
		m.blobs[name] = &mockBlobAPI{}
	}
	return m.blobs[name]
}

func (m *mockContainerAPI) ListBlobsFlat(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	for key := range m.blobs {
		keys = append(keys, key)
	}
	return keys, nil
}

func newMockAzure() *Azure {
	return &Azure{
		container: &mockContainerAPI{
			blobs: make(map[string]*mockBlobAPI),
		},
	}
}

func BenchmarkAzurePut(b *testing.B) {
	storage := newMockAzure()
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

func BenchmarkAzureGet(b *testing.B) {
	storage := newMockAzure()
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

func BenchmarkAzureDelete(b *testing.B) {
	storage := newMockAzure()

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

func BenchmarkAzureList(b *testing.B) {
	storage := newMockAzure()

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

func BenchmarkAzureAddPolicy(b *testing.B) {
	storage := newMockAzure()
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

func BenchmarkAzureGetPolicies(b *testing.B) {
	storage := newMockAzure()

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

func BenchmarkAzurePutGetDelete(b *testing.B) {
	storage := newMockAzure()
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
