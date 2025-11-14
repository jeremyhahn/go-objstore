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

package storagefs

import (
	"io"
	"testing"
)

func BenchmarkStorageFSCreate(b *testing.B) {
	storage := newMockStorage()
	fs := New(storage)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		f, err := fs.Create("test-file.txt")
		if err != nil {
			b.Fatal(err)
		}
		f.Close()
		_ = fs.Remove("test-file.txt")
	}
}

func BenchmarkStorageFSOpen(b *testing.B) {
	storage := newMockStorage()
	fs := New(storage)

	// Pre-create a file
	f, _ := fs.Create("test-file.txt")
	f.Write([]byte("test data"))
	f.Close()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		f, err := fs.Open("test-file.txt")
		if err != nil {
			b.Fatal(err)
		}
		f.Close()
	}
}

func BenchmarkStorageFSMkdir(b *testing.B) {
	storage := newMockStorage()
	fs := New(storage)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dirName := "test-dir"
		b.StartTimer()

		if err := fs.Mkdir(dirName, 0755); err != nil {
			b.Fatal(err)
		}

		b.StopTimer()
		_ = fs.Remove(dirName)
		b.StartTimer()
	}
}

func BenchmarkStorageFSMkdirAll(b *testing.B) {
	storage := newMockStorage()
	fs := New(storage)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dirPath := "a/b/c/d"
		b.StartTimer()

		if err := fs.MkdirAll(dirPath, 0755); err != nil {
			b.Fatal(err)
		}

		b.StopTimer()
		_ = fs.RemoveAll("a")
		b.StartTimer()
	}
}

func BenchmarkStorageFSRemove(b *testing.B) {
	storage := newMockStorage()
	fs := New(storage)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		f, _ := fs.Create("test-file.txt")
		f.Write([]byte("test"))
		f.Close()
		b.StartTimer()

		if err := fs.Remove("test-file.txt"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStorageFSStat(b *testing.B) {
	storage := newMockStorage()
	fs := New(storage)

	// Pre-create a file
	f, _ := fs.Create("test-file.txt")
	f.Write([]byte("test data"))
	f.Close()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := fs.Stat("test-file.txt")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStorageFSRename(b *testing.B) {
	storage := newMockStorage()
	fs := New(storage)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		f, _ := fs.Create("old-name.txt")
		f.Write([]byte("test"))
		f.Close()
		b.StartTimer()

		if err := fs.Rename("old-name.txt", "new-name.txt"); err != nil {
			b.Fatal(err)
		}

		b.StopTimer()
		_ = fs.Remove("new-name.txt")
		b.StartTimer()
	}
}

func BenchmarkStorageFSReadDir(b *testing.B) {
	storage := newMockStorage()
	fs := New(storage)

	// Pre-create directory with files
	fs.MkdirAll("test-dir", 0755)
	for i := 0; i < 100; i++ {
		f, _ := fs.Create("test-dir/file.txt")
		f.Write([]byte("test"))
		f.Close()
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := fs.readDirEntries("test-dir")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStorageFSWriteRead(b *testing.B) {
	storage := newMockStorage()
	fs := New(storage)
	data := make([]byte, 1024) // 1KB

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Write
		f, err := fs.Create("test-file.txt")
		if err != nil {
			b.Fatal(err)
		}
		_, err = f.Write(data)
		if err != nil {
			b.Fatal(err)
		}
		f.Close()

		// Read
		f, err = fs.Open("test-file.txt")
		if err != nil {
			b.Fatal(err)
		}
		io.Copy(io.Discard, f)
		f.Close()

		// Cleanup
		_ = fs.Remove("test-file.txt")
	}
}
