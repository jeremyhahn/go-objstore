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


package local

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

func BenchmarkPut(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "objstore-bench-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	storage := &Local{path: tmpDir, lifecycleManager: NewLifecycleManager()}
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

func BenchmarkPutWithContext(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "objstore-bench-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	storage := &Local{path: tmpDir, lifecycleManager: NewLifecycleManager()}
	ctx := context.Background()
	data := bytes.NewReader(make([]byte, 1024))

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		data.Seek(0, 0)
		if err := storage.PutWithContext(ctx, "test-key", data); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGet(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "objstore-bench-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	storage := &Local{path: tmpDir, lifecycleManager: NewLifecycleManager()}
	data := bytes.NewReader(make([]byte, 1024))
	if err := storage.Put("test-key", data); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		r, err := storage.Get("test-key")
		if err != nil {
			b.Fatal(err)
		}
		r.Close()
	}
}

func BenchmarkGetWithContext(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "objstore-bench-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	storage := &Local{path: tmpDir, lifecycleManager: NewLifecycleManager()}
	ctx := context.Background()
	data := bytes.NewReader(make([]byte, 1024))
	if err := storage.Put("test-key", data); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		r, err := storage.GetWithContext(ctx, "test-key")
		if err != nil {
			b.Fatal(err)
		}
		r.Close()
	}
}

func BenchmarkDelete(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "objstore-bench-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	storage := &Local{path: tmpDir, lifecycleManager: NewLifecycleManager()}
	data := bytes.NewReader(make([]byte, 1024))

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		data.Seek(0, 0)
		if err := storage.Put("test-key", data); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()

		if err := storage.Delete("test-key"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkExists(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "objstore-bench-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	storage := &Local{path: tmpDir, lifecycleManager: NewLifecycleManager()}
	ctx := context.Background()
	data := bytes.NewReader(make([]byte, 1024))
	if err := storage.Put("test-key", data); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := storage.Exists(ctx, "test-key")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkList(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "objstore-bench-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	storage := &Local{path: tmpDir, lifecycleManager: NewLifecycleManager()}

	// Create 100 test files
	data := bytes.NewReader(make([]byte, 100))
	for i := 0; i < 100; i++ {
		key := filepath.Join("prefix", string(rune('a'+i%26)), "file")
		data.Seek(0, 0)
		if err := storage.Put(key, data); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := storage.List("prefix")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkListWithOptions(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "objstore-bench-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	storage := &Local{path: tmpDir, lifecycleManager: NewLifecycleManager()}
	ctx := context.Background()

	// Create 100 test files
	data := bytes.NewReader(make([]byte, 100))
	for i := 0; i < 100; i++ {
		key := filepath.Join("prefix", string(rune('a'+i%26)), "file")
		data.Seek(0, 0)
		if err := storage.Put(key, data); err != nil {
			b.Fatal(err)
		}
	}

	opts := &common.ListOptions{
		Prefix:     "prefix",
		MaxResults: 50,
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := storage.ListWithOptions(ctx, opts)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPutGetDelete(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "objstore-bench-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	storage := &Local{path: tmpDir, lifecycleManager: NewLifecycleManager()}
	data := bytes.NewReader(make([]byte, 1024))

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		data.Seek(0, 0)
		if err := storage.Put("test-key", data); err != nil {
			b.Fatal(err)
		}

		r, err := storage.Get("test-key")
		if err != nil {
			b.Fatal(err)
		}
		r.Close()

		if err := storage.Delete("test-key"); err != nil {
			b.Fatal(err)
		}
	}
}
