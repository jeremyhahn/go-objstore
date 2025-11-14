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
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// TestRenameEdgeCases tests edge cases in Rename function to improve coverage
func TestRenameEdgeCases(t *testing.T) {
	t.Run("rename directory with Get error", func(t *testing.T) {
		mock := newMockStorage()
		fs := New(mock)

		// Create a directory
		err := fs.Mkdir("testdir", 0755)
		if err != nil {
			t.Fatalf("Failed to create directory: %v", err)
		}

		// Inject error for Get operation
		mock.getError = errors.New("get failed")

		// Try to rename - should fail when trying to get marker
		err = fs.Rename("testdir", "newdir")
		if err == nil {
			t.Fatal("Expected error when Get fails during directory rename")
		}

		// Clear error for cleanup
		mock.getError = nil
	})

	t.Run("rename directory with Put error", func(t *testing.T) {
		mock := newMockStorage()
		fs := New(mock)

		// Create a directory
		err := fs.Mkdir("testdir", 0755)
		if err != nil {
			t.Fatalf("Failed to create directory: %v", err)
		}

		// Inject error for Put operation (will fail when creating new marker)
		mock.putError = errors.New("put failed")

		// Try to rename
		err = fs.Rename("testdir", "newdir")
		if err == nil {
			t.Fatal("Expected error when Put fails during directory rename")
		}

		// Clear error
		mock.putError = nil
	})

	t.Run("rename file with Put error", func(t *testing.T) {
		mock := newMockStorage()
		fs := New(mock)

		// Create a file
		f, err := fs.Create("testfile.txt")
		if err != nil {
			t.Fatalf("Failed to create file: %v", err)
		}
		f.Write([]byte("test data"))
		f.Close()

		// Inject error for Put operation
		mock.putError = errors.New("put failed")

		// Try to rename
		err = fs.Rename("testfile.txt", "newfile.txt")
		if err == nil {
			t.Fatal("Expected error when Put fails during file rename")
		}

		// Clear error
		mock.putError = nil
	})

	t.Run("rename file with Delete error", func(t *testing.T) {
		mock := newMockStorage()
		fs := New(mock)

		// Create a file
		f, err := fs.Create("testfile.txt")
		if err != nil {
			t.Fatalf("Failed to create file: %v", err)
		}
		f.Write([]byte("test data"))
		f.Close()

		// Inject error for Delete operation
		mock.deleteError = errors.New("delete failed")

		// Try to rename
		err = fs.Rename("testfile.txt", "newfile.txt")
		if err == nil {
			t.Fatal("Expected error when Delete fails during file rename")
		}

		// Clear error
		mock.deleteError = nil
	})
}

// TestMetadataInternalErrors tests error paths in metadata operations
func TestMetadataInternalErrors(t *testing.T) {
	t.Run("putMetadataInternal with Put error", func(t *testing.T) {
		mock := newMockStorage()
		fs := New(mock)

		// Inject error for Put
		mock.putError = errors.New("put failed")

		meta := fileMetadata{
			Name:    "test",
			Size:    100,
			Mode:    0644,
			ModTime: time.Now(),
			IsDir:   false,
		}

		err := fs.putMetadataInternal("test", meta)
		if err == nil {
			t.Fatal("Expected error when Put fails")
		}

		mock.putError = nil
	})

	t.Run("getMetadataInternal with Get error", func(t *testing.T) {
		mock := newMockStorage()
		fs := New(mock)

		// Inject error for Get
		mock.getError = errors.New("get failed")

		_, err := fs.getMetadataInternal("test")
		if err == nil {
			t.Fatal("Expected error when Get fails")
		}

		mock.getError = nil
	})

	t.Run("getMetadataInternal with invalid JSON", func(t *testing.T) {
		mock := newMockStorage()
		fs := New(mock)

		// Put invalid JSON
		err := mock.Put(".meta/test", bytes.NewReader([]byte("invalid json")))
		if err != nil {
			t.Fatalf("Failed to put invalid JSON: %v", err)
		}

		_, err = fs.getMetadataInternal("test")
		if err == nil {
			t.Fatal("Expected error when JSON is invalid")
		}
	})

	t.Run("deleteMetadata with Delete error", func(t *testing.T) {
		mock := newMockStorage()
		fs := New(mock)

		// Inject error for Delete
		mock.deleteError = errors.New("delete failed")

		err := fs.deleteMetadata("test")
		if err == nil {
			t.Fatal("Expected error when Delete fails")
		}

		mock.deleteError = nil
	})
}

// TestListKeysWithoutLister tests listKeys when storage doesn't implement lister
func TestListKeysWithoutLister(t *testing.T) {
	// Create a mock that doesn't implement the lister interface
	type nonListerMock struct {
		*mockStorage
	}

	baseMock := newMockStorage()
	mock := &nonListerMock{mockStorage: baseMock}
	fs := New(mock)

	// Call listKeys - should return empty list
	keys := fs.listKeys("test")
	if len(keys) != 0 {
		t.Fatalf("Expected empty list, got %d keys", len(keys))
	}
}

// TestNormalizePathEdgeCases tests edge cases in normalizePath
func TestNormalizePathEdgeCases(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"", "."},
		{"/", "."},
		{".", "."},
		{"path/with\\backslash", "path/with/backslash"},
		{"/leading/slash", "leading/slash"},
		{"./relative/./path", "relative/path"},
		{"path/../parent", "parent"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := normalizePath(tt.input)
			if result != tt.expected {
				t.Errorf("normalizePath(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

// TestMkdirEdgeCases tests edge cases in Mkdir
func TestMkdirEdgeCases(t *testing.T) {
	t.Run("mkdir with Put error for marker", func(t *testing.T) {
		mock := newMockStorage()
		fs := New(mock)

		// Inject error for Put
		mock.putError = errors.New("put failed")

		err := fs.Mkdir("testdir", 0755)
		if err == nil {
			t.Fatal("Expected error when Put fails")
		}

		mock.putError = nil
	})
}

// TestRemoveEdgeCases tests edge cases in Remove
func TestRemoveEdgeCases(t *testing.T) {
	t.Run("remove directory with Delete marker error", func(t *testing.T) {
		mock := newMockStorage()
		fs := New(mock)

		// Create a directory
		err := fs.Mkdir("testdir", 0755)
		if err != nil {
			t.Fatalf("Failed to create directory: %v", err)
		}

		// Inject error for Delete
		mock.deleteError = errors.New("delete failed")

		err = fs.Remove("testdir")
		if err == nil {
			t.Fatal("Expected error when Delete fails")
		}

		mock.deleteError = nil
	})

	t.Run("remove file with Delete error", func(t *testing.T) {
		mock := newMockStorage()
		fs := New(mock)

		// Create a file
		f, err := fs.Create("testfile.txt")
		if err != nil {
			t.Fatalf("Failed to create file: %v", err)
		}
		f.Close()

		// Inject error for Delete
		mock.deleteError = errors.New("delete failed")

		err = fs.Remove("testfile.txt")
		if err == nil {
			t.Fatal("Expected error when Delete fails")
		}

		mock.deleteError = nil
	})
}

// TestFileInfoUnmarshalJSONError tests error handling in UnmarshalJSON
func TestFileInfoUnmarshalJSONError(t *testing.T) {
	fi := &FileInfo{}
	err := fi.UnmarshalJSON([]byte("invalid json"))
	if err == nil {
		t.Fatal("Expected error when unmarshaling invalid JSON")
	}
}

// TestStorageFileSyncError tests error handling in Sync
func TestStorageFileSyncError(t *testing.T) {
	t.Run("sync with Put error", func(t *testing.T) {
		mock := newMockStorage()
		fs := New(mock)

		f, err := fs.Create("test.txt")
		if err != nil {
			t.Fatalf("Failed to create file: %v", err)
		}

		f.Write([]byte("test data"))

		// Inject error for Put
		mock.putError = errors.New("put failed")

		err = f.(*StorageFile).Sync()
		if err == nil {
			t.Fatal("Expected error when Put fails during Sync")
		}

		mock.putError = nil
		f.Close()
	})
}

// TestStorageFileCloseError tests error handling in Close
func TestStorageFileCloseError(t *testing.T) {
	t.Run("close with Put error", func(t *testing.T) {
		mock := newMockStorage()
		fs := New(mock)

		f, err := fs.Create("test.txt")
		if err != nil {
			t.Fatalf("Failed to create file: %v", err)
		}

		f.Write([]byte("test data"))

		// Inject error for Put
		mock.putError = errors.New("put failed")

		err = f.Close()
		if err == nil {
			t.Fatal("Expected error when Put fails during Close")
		}

		mock.putError = nil
	})
}

// errorReader is a reader that always returns an error
type errorReader struct{}

func (e *errorReader) Read(p []byte) (n int, err error) {
	return 0, errors.New("read error")
}

func (e *errorReader) Close() error {
	return nil
}

// errorInjectMock wraps mockStorage to inject error readers for specific keys
type errorInjectMock struct {
	*mockStorage
	errorKeys map[string]bool
}

func (m *errorInjectMock) Get(key string) (io.ReadCloser, error) {
	if m.errorKeys[key] {
		return &errorReader{}, nil
	}
	return m.mockStorage.Get(key)
}

// TestStorageFileReadError tests read error with io.Copy failure
func TestStorageFileReadError(t *testing.T) {
	t.Run("newStorageFile read mode with io.Copy error", func(t *testing.T) {
		mock := &errorInjectMock{
			mockStorage: newMockStorage(),
			errorKeys:   make(map[string]bool),
		}
		fs := New(mock)

		// Create a file first
		f, err := fs.Create("test.txt")
		if err != nil {
			t.Fatalf("Failed to create file: %v", err)
		}
		f.Write([]byte("test data"))
		f.Close()

		// Now make Get return error reader for this specific key
		mock.errorKeys["test.txt"] = true

		// Try to open for reading
		_, err = fs.Open("test.txt")
		if err == nil {
			t.Fatal("Expected error when io.Copy fails during read")
		}
	})

	t.Run("newStorageFile write-only append mode with io.Copy error", func(t *testing.T) {
		mock := &errorInjectMock{
			mockStorage: newMockStorage(),
			errorKeys:   make(map[string]bool),
		}
		fs := New(mock)

		// Create a file first
		f, err := fs.Create("test.txt")
		if err != nil {
			t.Fatalf("Failed to create file: %v", err)
		}
		f.Write([]byte("test data"))
		f.Close()

		// Now make Get return error reader for this specific key
		mock.errorKeys["test.txt"] = true

		// Try to open in write-only append mode
		_, err = fs.OpenFile("test.txt", os.O_WRONLY|os.O_APPEND, 0644)
		if err == nil {
			t.Fatal("Expected error when io.Copy fails during append")
		}
	})
}

// TestMockStorageContextMethods tests context-aware methods for coverage
func TestMockStorageContextMethods(t *testing.T) {
	mock := newMockStorage()
	ctx := context.Background()

	t.Run("PutWithContext", func(t *testing.T) {
		err := mock.PutWithContext(ctx, "key1", bytes.NewReader([]byte("data")))
		if err != nil {
			t.Fatalf("PutWithContext failed: %v", err)
		}
	})

	t.Run("PutWithMetadata", func(t *testing.T) {
		err := mock.PutWithMetadata(ctx, "key2", bytes.NewReader([]byte("data")), nil)
		if err != nil {
			t.Fatalf("PutWithMetadata failed: %v", err)
		}
	})

	t.Run("GetWithContext", func(t *testing.T) {
		mock.Put("key3", bytes.NewReader([]byte("data")))
		_, err := mock.GetWithContext(ctx, "key3")
		if err != nil {
			t.Fatalf("GetWithContext failed: %v", err)
		}
	})

	t.Run("GetMetadata", func(t *testing.T) {
		_, err := mock.GetMetadata(ctx, "key1")
		if err != nil {
			t.Fatalf("GetMetadata failed: %v", err)
		}
	})

	t.Run("UpdateMetadata", func(t *testing.T) {
		err := mock.UpdateMetadata(ctx, "key1", nil)
		if err != nil {
			t.Fatalf("UpdateMetadata failed: %v", err)
		}
	})

	t.Run("DeleteWithContext", func(t *testing.T) {
		err := mock.DeleteWithContext(ctx, "key1")
		if err != nil {
			t.Fatalf("DeleteWithContext failed: %v", err)
		}
	})

	t.Run("Exists", func(t *testing.T) {
		mock.Put("key4", bytes.NewReader([]byte("data")))
		exists, err := mock.Exists(ctx, "key4")
		if err != nil {
			t.Fatalf("Exists failed: %v", err)
		}
		if !exists {
			t.Fatal("Expected key to exist")
		}
	})

	t.Run("Exists non-existent", func(t *testing.T) {
		exists, err := mock.Exists(ctx, "nonexistent")
		if err != nil {
			t.Fatalf("Exists failed: %v", err)
		}
		if exists {
			t.Fatal("Expected key to not exist")
		}
	})

	t.Run("List", func(t *testing.T) {
		mock.Put("prefix/key1", bytes.NewReader([]byte("data")))
		mock.Put("prefix/key2", bytes.NewReader([]byte("data")))
		keys, err := mock.List("prefix/")
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}
		if len(keys) < 2 {
			t.Fatalf("Expected at least 2 keys, got %d", len(keys))
		}
	})

	t.Run("ListWithContext", func(t *testing.T) {
		keys, err := mock.ListWithContext(ctx, "prefix/")
		if err != nil {
			t.Fatalf("ListWithContext failed: %v", err)
		}
		if len(keys) < 2 {
			t.Fatalf("Expected at least 2 keys, got %d", len(keys))
		}
	})

	t.Run("ListWithOptions", func(t *testing.T) {
		opts := &common.ListOptions{
			Prefix: "prefix/",
		}
		result, err := mock.ListWithOptions(ctx, opts)
		if err != nil {
			t.Fatalf("ListWithOptions failed: %v", err)
		}
		if len(result.Objects) < 2 {
			t.Fatalf("Expected at least 2 objects, got %d", len(result.Objects))
		}
	})

	t.Run("Archive", func(t *testing.T) {
		archiver := newMockStorage()
		mock.Put("archive-key", bytes.NewReader([]byte("archive data")))
		err := mock.Archive("archive-key", archiver)
		if err != nil {
			t.Fatalf("Archive failed: %v", err)
		}
	})

	t.Run("Archive non-existent", func(t *testing.T) {
		archiver := newMockStorage()
		err := mock.Archive("nonexistent-archive-key", archiver)
		if err == nil {
			t.Fatal("Expected error when archiving non-existent key")
		}
	})

	t.Run("AddPolicy", func(t *testing.T) {
		err := mock.AddPolicy(common.LifecyclePolicy{})
		if err != nil {
			t.Fatalf("AddPolicy failed: %v", err)
		}
	})

	t.Run("RemovePolicy", func(t *testing.T) {
		err := mock.RemovePolicy("policy-id")
		if err != nil {
			t.Fatalf("RemovePolicy failed: %v", err)
		}
	})

	t.Run("GetPolicies", func(t *testing.T) {
		policies, err := mock.GetPolicies()
		if err != nil {
			t.Fatalf("GetPolicies failed: %v", err)
		}
		if len(policies) != 0 {
			t.Fatalf("Expected 0 policies, got %d", len(policies))
		}
	})

	t.Run("RunPolicies", func(t *testing.T) {
		err := mock.RunPolicies(ctx)
		if err != nil {
			t.Fatalf("RunPolicies failed: %v", err)
		}
	})

	t.Run("Configure", func(t *testing.T) {
		err := mock.Configure(map[string]string{"key": "value"})
		if err != nil {
			t.Fatalf("Configure failed: %v", err)
		}
	})

	t.Run("clear", func(t *testing.T) {
		mock.clear()
		keys, _ := mock.List("")
		if len(keys) != 0 {
			t.Fatalf("Expected 0 keys after clear, got %d", len(keys))
		}
	})
}

// TestMockStoragePutError tests Put error handling with io.ReadAll failure
func TestMockStoragePutError(t *testing.T) {
	mock := newMockStorage()

	// Test with error reader for Put
	err := mock.Put("key", &errorReader{})
	if err == nil {
		t.Fatal("Expected error from Put with error reader")
	}

	// Test with putError set
	mock.putError = errors.New("put error")
	err = mock.Put("key", bytes.NewReader([]byte("data")))
	if err == nil {
		t.Fatal("Expected error from Put")
	}
}

// TestMockStorageDeleteNonExistent tests Delete on non-existent key
func TestMockStorageDeleteNonExistent(t *testing.T) {
	mock := newMockStorage()

	err := mock.Delete("nonexistent")
	if err == nil {
		t.Fatal("Expected error when deleting non-existent key")
	}
}

// TestStorageFileReadAtEOF tests ReadAt returning EOF
func TestStorageFileReadAtEOF(t *testing.T) {
	mock := newMockStorage()
	fs := New(mock)

	// Create a file with content
	f, err := fs.Create("test.txt")
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	f.Write([]byte("hello"))
	f.Close()

	// Open for reading
	f, err = fs.Open("test.txt")
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer f.Close()

	// Read at offset that requires EOF
	buf := make([]byte, 10)
	n, err := f.(*StorageFile).ReadAt(buf, 3)
	if err != io.EOF {
		t.Fatalf("Expected EOF, got %v", err)
	}
	if n != 2 {
		t.Fatalf("Expected 2 bytes read, got %d", n)
	}
}

// TestStorageFileWriteAtExpand tests WriteAt expanding buffer
func TestStorageFileWriteAtExpand(t *testing.T) {
	mock := newMockStorage()
	fs := New(mock)

	// Create a file with small content
	f, err := fs.Create("test.txt")
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	f.Write([]byte("hi"))

	// Write at offset beyond current size
	sf := f.(*StorageFile)
	n, err := sf.WriteAt([]byte("world"), 10)
	if err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}
	if n != 5 {
		t.Fatalf("Expected 5 bytes written, got %d", n)
	}

	f.Close()
}

// TestStorageFileTruncateExpand tests Truncate expanding file
func TestStorageFileTruncateExpand(t *testing.T) {
	mock := newMockStorage()
	fs := New(mock)

	// Create a file with content
	f, err := fs.Create("test.txt")
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	f.Write([]byte("hello"))

	// Truncate to larger size
	sf := f.(*StorageFile)
	err = sf.Truncate(20)
	if err != nil {
		t.Fatalf("Truncate failed: %v", err)
	}

	// Verify new size
	info, _ := sf.Stat()
	if info.Size() != 20 {
		t.Fatalf("Expected size 20, got %d", info.Size())
	}

	f.Close()
}

// TestStorageFileWritePadding tests Write with offset beyond buffer size
func TestStorageFileWritePadding(t *testing.T) {
	mock := newMockStorage()
	fs := New(mock)

	f, err := fs.Create("test.txt")
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	// Seek beyond end and write
	sf := f.(*StorageFile)
	sf.Seek(10, io.SeekStart)
	n, err := sf.Write([]byte("data"))
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if n != 4 {
		t.Fatalf("Expected 4 bytes written, got %d", n)
	}

	// Verify file was padded
	info, _ := sf.Stat()
	if info.Size() < 14 {
		t.Fatalf("Expected size >= 14, got %d", info.Size())
	}

	f.Close()
}

// TestStorageFileWriteAtWithinBuffer tests WriteAt writing within existing buffer
func TestStorageFileWriteAtWithinBuffer(t *testing.T) {
	mock := newMockStorage()
	fs := New(mock)

	f, err := fs.Create("test.txt")
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	// Write initial content
	f.Write([]byte("hello world"))

	// Write at offset within buffer
	sf := f.(*StorageFile)
	n, err := sf.WriteAt([]byte("WORLD"), 6)
	if err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}
	if n != 5 {
		t.Fatalf("Expected 5 bytes written, got %d", n)
	}

	f.Close()
}

// TestStorageFileWriteOverwrite tests Write overwriting existing content
func TestStorageFileWriteOverwrite(t *testing.T) {
	mock := newMockStorage()
	fs := New(mock)

	f, err := fs.Create("test.txt")
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	// Write initial content
	f.Write([]byte("hello world"))

	// Seek to middle and write (overwrite)
	sf := f.(*StorageFile)
	sf.Seek(6, io.SeekStart)
	n, err := sf.Write([]byte("Go"))
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if n != 2 {
		t.Fatalf("Expected 2 bytes written, got %d", n)
	}

	f.Close()
}

// TestStorageFileAppendMode tests write in append mode
func TestStorageFileAppendMode(t *testing.T) {
	mock := newMockStorage()
	fs := New(mock)

	// Create file with initial content
	f, err := fs.Create("test.txt")
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	f.Write([]byte("hello"))
	f.Close()

	// Open in append mode
	f, err = fs.OpenFile("test.txt", os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}

	// Write should append even after seek
	sf := f.(*StorageFile)
	sf.Seek(0, io.SeekStart)
	n, err := sf.Write([]byte(" world"))
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if n != 6 {
		t.Fatalf("Expected 6 bytes written, got %d", n)
	}

	f.Close()
}

// TestGetMetadataInternalReadAllError tests error in io.ReadAll
func TestGetMetadataInternalReadAllError(t *testing.T) {
	mock := &errorInjectMock{
		mockStorage: newMockStorage(),
		errorKeys:   make(map[string]bool),
	}
	fs := New(mock)

	// Create metadata
	mock.Put(".meta/test", bytes.NewReader([]byte(`{"name":"test"}`)))

	// Make Get return error reader for this metadata key
	mock.errorKeys[".meta/test"] = true

	_, err := fs.getMetadataInternal("test")
	if err == nil {
		t.Fatal("Expected error when ReadAll fails")
	}
}
