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
	"errors"
	"io"
	"os"
	"testing"
	"time"
)

// TestIsNotFoundError_Comprehensive tests the isNotFoundError helper comprehensively
func TestIsNotFoundError_Comprehensive(t *testing.T) {
	if isNotFoundError(nil) {
		t.Error("nil should not be not found error")
	}
	if !isNotFoundError(os.ErrNotExist) {
		t.Error("ErrNotExist should be not found error")
	}
	if !isNotFoundError(errors.New("key not found")) {
		t.Error("'key not found' should be not found error")
	}
	if isNotFoundError(errors.New("other error")) {
		t.Error("other errors should not be not found error")
	}
}

// TestFile_DirectoryOperations_Comprehensive tests all directory file operations
func TestFile_DirectoryOperations_Comprehensive(t *testing.T) {
	storage := newMockStorage()
	fs := New(storage)

	fs.Mkdir("testdir", 0755)
	file, err := newStorageFile(fs, "testdir", os.O_RDWR, 0755)
	if err != nil {
		t.Fatalf("failed to open directory: %v", err)
	}

	buf := make([]byte, 10)

	tests := []struct {
		name     string
		fn       func() error
		expected error
	}{
		{"Read", func() error { _, err := file.Read(buf); return err }, ErrIsDirectory},
		{"ReadAt", func() error { _, err := file.ReadAt(buf, 0); return err }, ErrIsDirectory},
		{"Seek", func() error { _, err := file.Seek(0, io.SeekStart); return err }, ErrIsDirectory},
		{"Write", func() error { _, err := file.Write([]byte("test")); return err }, ErrIsDirectory},
		{"WriteAt", func() error { _, err := file.WriteAt([]byte("test"), 0); return err }, ErrIsDirectory},
		{"Truncate", func() error { return file.Truncate(10) }, ErrIsDirectory},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.fn()
			if err != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, err)
			}
		})
	}

	// Test Readdir and Readdirnames
	_, err = file.Readdir(-1)
	if err != nil {
		t.Errorf("Readdir on directory should work, got %v", err)
	}

	_, err = file.Readdirnames(-1)
	if err != nil {
		t.Errorf("Readdirnames on directory should work, got %v", err)
	}

	// Test Sync on directory
	err = file.Sync()
	if err != nil {
		t.Errorf("Sync on directory should work, got %v", err)
	}

	// Test Stat on directory
	info, err := file.Stat()
	if err != nil {
		t.Errorf("Stat on directory should work, got %v", err)
	}
	if !info.IsDir() {
		t.Error("expected IsDir to be true")
	}
}

// TestFile_PermissionErrors_Comprehensive tests permission errors
func TestFile_PermissionErrors_Comprehensive(t *testing.T) {
	storage := newMockStorage()
	storage.Put("test.txt", bytes.NewReader([]byte("hello world")))
	fs := New(storage)

	t.Run("read-only file write operations", func(t *testing.T) {
		file, _ := newStorageFile(fs, "test.txt", os.O_RDONLY, 0644)

		if _, err := file.Write([]byte("test")); err != os.ErrPermission {
			t.Errorf("Write: expected ErrPermission, got %v", err)
		}
		if _, err := file.WriteAt([]byte("test"), 0); err != os.ErrPermission {
			t.Errorf("WriteAt: expected ErrPermission, got %v", err)
		}
		if err := file.Truncate(10); err != os.ErrPermission {
			t.Errorf("Truncate: expected ErrPermission, got %v", err)
		}
	})

	t.Run("write-only file read operations", func(t *testing.T) {
		file, _ := newStorageFile(fs, "test.txt", os.O_WRONLY, 0644)

		buf := make([]byte, 10)
		if _, err := file.ReadAt(buf, 0); err != os.ErrPermission {
			t.Errorf("ReadAt: expected ErrPermission, got %v", err)
		}
	})
}

// TestFile_WriteOperations_Comprehensive tests various write scenarios
func TestFile_WriteOperations_Comprehensive(t *testing.T) {
	t.Run("Write with nil buffer initially", func(t *testing.T) {
		storage := newMockStorage()
		fs := New(storage)
		file, _ := newStorageFile(fs, "test.txt", os.O_WRONLY|os.O_CREATE, 0644)

		n, err := file.Write([]byte("test"))
		if err != nil || n != 4 {
			t.Errorf("expected 4 bytes written, got %d, err: %v", n, err)
		}
	})

	t.Run("Write beyond buffer end", func(t *testing.T) {
		storage := newMockStorage()
		fs := New(storage)
		file, _ := newStorageFile(fs, "test.txt", os.O_RDWR|os.O_CREATE, 0644)

		file.Write([]byte("hello"))
		file.Seek(10, io.SeekStart)
		n, err := file.Write([]byte("world"))
		if err != nil || n != 5 {
			t.Errorf("expected 5 bytes written, got %d, err: %v", n, err)
		}
	})

	t.Run("Write in middle of buffer", func(t *testing.T) {
		storage := newMockStorage()
		fs := New(storage)
		file, _ := newStorageFile(fs, "test.txt", os.O_RDWR|os.O_CREATE, 0644)

		file.Write([]byte("hello world"))
		file.Seek(6, io.SeekStart)
		n, err := file.Write([]byte("Go"))
		if err != nil || n != 2 {
			t.Errorf("expected 2 bytes written, got %d, err: %v", n, err)
		}
	})

	t.Run("WriteAt expanding buffer", func(t *testing.T) {
		storage := newMockStorage()
		fs := New(storage)
		file, _ := newStorageFile(fs, "test.txt", os.O_RDWR|os.O_CREATE, 0644)

		n, err := file.WriteAt([]byte("test"), 100)
		if err != nil || n != 4 {
			t.Errorf("expected 4 bytes written, got %d, err: %v", n, err)
		}
	})

	t.Run("WriteAt within existing buffer", func(t *testing.T) {
		storage := newMockStorage()
		fs := New(storage)
		file, _ := newStorageFile(fs, "test.txt", os.O_RDWR|os.O_CREATE, 0644)

		file.Write([]byte("hello world"))
		n, err := file.WriteAt([]byte("Go"), 6)
		if err != nil || n != 2 {
			t.Errorf("expected 2 bytes written, got %d, err: %v", n, err)
		}
	})

	t.Run("WriteAt with nil buffer", func(t *testing.T) {
		storage := newMockStorage()
		fs := New(storage)
		file, _ := newStorageFile(fs, "test.txt", os.O_WRONLY|os.O_CREATE, 0644)

		n, err := file.WriteAt([]byte("test"), 0)
		if err != nil || n != 4 {
			t.Errorf("expected 4 bytes written, got %d, err: %v", n, err)
		}
	})
}

// TestFile_ReadOperations_Comprehensive tests various read scenarios
func TestFile_ReadOperations_Comprehensive(t *testing.T) {
	t.Run("ReadAt at EOF", func(t *testing.T) {
		storage := newMockStorage()
		storage.Put("test.txt", bytes.NewReader([]byte("hello")))
		fs := New(storage)
		file, _ := newStorageFile(fs, "test.txt", os.O_RDONLY, 0644)

		buf := make([]byte, 10)
		n, err := file.ReadAt(buf, 5)
		if err != io.EOF || n != 0 {
			t.Errorf("expected 0 bytes and EOF, got %d, %v", n, err)
		}
	})

	t.Run("ReadAt partial read", func(t *testing.T) {
		storage := newMockStorage()
		storage.Put("test.txt", bytes.NewReader([]byte("hello")))
		fs := New(storage)
		file, _ := newStorageFile(fs, "test.txt", os.O_RDONLY, 0644)

		buf := make([]byte, 10)
		n, err := file.ReadAt(buf, 2)
		if err != io.EOF || n != 3 {
			t.Errorf("expected 3 bytes and EOF, got %d, %v", n, err)
		}
	})

	t.Run("Read with nil buffer", func(t *testing.T) {
		storage := newMockStorage()
		fs := New(storage)
		file, _ := newStorageFile(fs, "test.txt", os.O_RDWR|os.O_CREATE, 0644)

		buf := make([]byte, 10)
		n, err := file.Read(buf)
		if n != 0 || err != io.EOF {
			t.Errorf("expected 0 bytes and EOF for nil buffer, got %d, %v", n, err)
		}
	})
}

// TestFile_SeekOperations_Comprehensive tests seek scenarios
func TestFile_SeekOperations_Comprehensive(t *testing.T) {
	t.Run("Seek with invalid whence", func(t *testing.T) {
		storage := newMockStorage()
		fs := New(storage)
		file, _ := newStorageFile(fs, "test.txt", os.O_RDWR|os.O_CREATE, 0644)

		_, err := file.Seek(0, 999)
		if err == nil {
			t.Error("expected error for invalid whence")
		}
	})

	t.Run("Seek to negative offset", func(t *testing.T) {
		storage := newMockStorage()
		fs := New(storage)
		file, _ := newStorageFile(fs, "test.txt", os.O_RDWR|os.O_CREATE, 0644)

		_, err := file.Seek(-10, io.SeekStart)
		if err == nil {
			t.Error("expected error for negative offset")
		}
	})
}

// TestFile_TruncateOperations_Comprehensive tests truncate scenarios
func TestFile_TruncateOperations_Comprehensive(t *testing.T) {
	t.Run("Truncate to larger size", func(t *testing.T) {
		storage := newMockStorage()
		fs := New(storage)
		file, _ := newStorageFile(fs, "test.txt", os.O_RDWR|os.O_CREATE, 0644)

		file.Write([]byte("hello"))
		err := file.Truncate(10)
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		info, _ := file.Stat()
		if info.Size() != 10 {
			t.Errorf("expected size 10, got %d", info.Size())
		}
	})

	t.Run("Truncate to same size", func(t *testing.T) {
		storage := newMockStorage()
		fs := New(storage)
		file, _ := newStorageFile(fs, "test.txt", os.O_RDWR|os.O_CREATE, 0644)

		file.Write([]byte("hello"))
		err := file.Truncate(5)
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
	})

	t.Run("Truncate with nil buffer", func(t *testing.T) {
		storage := newMockStorage()
		fs := New(storage)
		file, _ := newStorageFile(fs, "test.txt", os.O_RDWR|os.O_CREATE, 0644)

		err := file.Truncate(10)
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
	})
}

// TestStorageFS_Operations_Comprehensive tests FS operations comprehensively
func TestStorageFS_Operations_Comprehensive(t *testing.T) {
	t.Run("Rename directory", func(t *testing.T) {
		storage := newMockStorage()
		fs := New(storage)

		fs.Mkdir("olddir", 0755)
		err := fs.Rename("olddir", "newdir")
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		exists, _ := fs.dirExists("newdir")
		if !exists {
			t.Error("expected new directory to exist")
		}
	})

	t.Run("Rename file with metadata", func(t *testing.T) {
		storage := newMockStorage()
		fs := New(storage)

		file, _ := fs.Create("old.txt")
		file.Write([]byte("test"))
		file.Close()

		err := fs.Rename("old.txt", "new.txt")
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
	})

	t.Run("Stat file without metadata", func(t *testing.T) {
		storage := newMockStorage()
		fs := New(storage)

		storage.Put("test.txt", bytes.NewReader([]byte("hello")))

		info, err := fs.Stat("test.txt")
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
		if info == nil {
			t.Error("expected info")
		}
	})

	t.Run("Chmod non-existent file", func(t *testing.T) {
		storage := newMockStorage()
		fs := New(storage)

		err := fs.Chmod("nonexistent.txt", 0644)
		if err == nil {
			t.Error("expected error for non-existent file")
		}
	})

	t.Run("Chtimes non-existent file", func(t *testing.T) {
		storage := newMockStorage()
		fs := New(storage)

		now := time.Now()
		err := fs.Chtimes("nonexistent.txt", now, now)
		if err == nil {
			t.Error("expected error for non-existent file")
		}
	})

	t.Run("Chmod directory", func(t *testing.T) {
		storage := newMockStorage()
		fs := New(storage)

		fs.Mkdir("testdir", 0755)
		err := fs.Chmod("testdir", 0700)
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
	})

	t.Run("Chtimes file", func(t *testing.T) {
		storage := newMockStorage()
		fs := New(storage)

		file, _ := fs.Create("test.txt")
		file.Close()

		now := time.Now()
		err := fs.Chtimes("test.txt", now, now)
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
	})
}

// TestNewStorageFile_EdgeCases_Comprehensive tests edge cases in newStorageFile
func TestNewStorageFile_EdgeCases_Comprehensive(t *testing.T) {
	t.Run("Create with O_TRUNC and O_CREATE", func(t *testing.T) {
		storage := newMockStorage()
		storage.Put("test.txt", bytes.NewReader([]byte("existing")))
		fs := New(storage)

		file, err := newStorageFile(fs, "test.txt", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		buf := make([]byte, 10)
		n, _ := file.Read(buf)
		if n != 0 {
			t.Errorf("expected empty buffer after truncate, got %d bytes", n)
		}
	})

	t.Run("Open with O_APPEND", func(t *testing.T) {
		storage := newMockStorage()
		storage.Put("test.txt", bytes.NewReader([]byte("hello")))
		fs := New(storage)

		file, err := newStorageFile(fs, "test.txt", os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		n, err := file.Write([]byte(" world"))
		if err != nil || n != 6 {
			t.Errorf("expected 6 bytes written, got %d, err: %v", n, err)
		}
	})

	t.Run("Create with O_CREATE but not O_TRUNC", func(t *testing.T) {
		storage := newMockStorage()
		storage.Put("test.txt", bytes.NewReader([]byte("existing")))
		fs := New(storage)

		file, err := newStorageFile(fs, "test.txt", os.O_WRONLY|os.O_CREATE, 0644)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if file.buf == nil {
			t.Error("expected buffer to be initialized")
		}
	})

	t.Run("Directory without metadata", func(t *testing.T) {
		storage := newMockStorage()
		fs := New(storage)

		storage.Put("testdir/.dir", bytes.NewReader([]byte{}))

		file, err := newStorageFile(fs, "testdir", os.O_RDONLY, 0755)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if !file.isDir {
			t.Error("expected file to be marked as directory")
		}
	})

	t.Run("Read mode with non-NotExist error", func(t *testing.T) {
		storage := newMockStorage()
		storage.Put("test.txt", bytes.NewReader([]byte("content")))
		storage.getError = errors.New("read error")
		fs := New(storage)

		_, err := newStorageFile(fs, "test.txt", os.O_RDONLY, 0644)
		if err == nil {
			t.Error("expected error")
		}
	})
}

// TestReadDirEntries_Comprehensive tests readDirEntries and dirEntry methods
func TestReadDirEntries_Comprehensive(t *testing.T) {
	storage := newMockStorage()
	fs := New(storage)

	// Create a test directory with files and subdirectories
	if err := fs.MkdirAll("testdir", 0755); err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}

	// Create a file
	f, err := fs.Create("testdir/file.txt")
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	f.Write([]byte("test content"))
	f.Close()

	// Create a subdirectory
	if err := fs.MkdirAll("testdir/subdir", 0755); err != nil {
		t.Fatalf("Failed to create subdirectory: %v", err)
	}

	// Read directory entries
	entries, err := fs.readDirEntries("testdir")
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if entries == nil {
		t.Error("expected non-nil entries slice")
	}

	// Test all dirEntry methods on each entry
	foundFile := false
	foundDir := false

	for _, entry := range entries {
		// Test Name() method
		name := entry.Name()

		if name == "file.txt" {
			foundFile = true
			// Test file entry methods
			if entry.IsDir() {
				t.Error("File entry should not be a directory")
			}
			mode := entry.Type()
			if mode.IsDir() {
				t.Error("File entry type should not be directory")
			}
			info, err := entry.Info()
			if err != nil {
				t.Errorf("File entry Info() failed: %v", err)
			}
			if info == nil {
				t.Error("File entry Info() returned nil")
			}
		} else if name == "subdir" {
			foundDir = true
			// Test directory entry methods
			if !entry.IsDir() {
				t.Error("Directory entry should be a directory")
			}
			mode := entry.Type()
			if !mode.IsDir() {
				t.Error("Directory entry type should be directory")
			}
			info, err := entry.Info()
			if err != nil {
				t.Errorf("Directory entry Info() failed: %v", err)
			}
			if info == nil {
				t.Error("Directory entry Info() returned nil")
			}
		}
	}

	if !foundFile {
		t.Error("Expected to find file.txt in directory entries")
	}
	if !foundDir {
		t.Error("Expected to find subdir in directory entries")
	}
}

// TestStorageErrors_Comprehensive tests storage error handling
func TestStorageErrors_Comprehensive(t *testing.T) {
	t.Run("Close with put error", func(t *testing.T) {
		storage := newMockStorage()
		fs := New(storage)

		file, _ := newStorageFile(fs, "test.txt", os.O_RDWR|os.O_CREATE, 0644)
		file.Write([]byte("test"))

		storage.putError = errors.New("put failed")
		err := file.Close()
		if err == nil {
			t.Error("expected error")
		}
	})

	t.Run("Sync with put error", func(t *testing.T) {
		storage := newMockStorage()
		fs := New(storage)

		file, _ := newStorageFile(fs, "test.txt", os.O_RDWR|os.O_CREATE, 0644)
		file.Write([]byte("test"))

		storage.putError = errors.New("put failed")
		err := file.Sync()
		if err == nil {
			t.Error("expected error")
		}
	})

	t.Run("Mkdir with put error", func(t *testing.T) {
		storage := newMockStorage()
		storage.putError = errors.New("put failed")
		fs := New(storage)

		err := fs.Mkdir("testdir", 0755)
		if err == nil {
			t.Error("expected error")
		}
	})

	t.Run("Remove file with delete error", func(t *testing.T) {
		storage := newMockStorage()
		fs := New(storage)

		file, _ := fs.Create("test.txt")
		file.Close()

		storage.deleteError = errors.New("delete failed")
		err := fs.Remove("test.txt")
		if err == nil {
			t.Error("expected error")
		}
	})

	t.Run("Remove directory with delete error", func(t *testing.T) {
		storage := newMockStorage()
		fs := New(storage)

		fs.Mkdir("testdir", 0755)

		storage.deleteError = errors.New("delete failed")
		err := fs.Remove("testdir")
		if err == nil {
			t.Error("expected error")
		}
	})

	t.Run("Rename with get error", func(t *testing.T) {
		storage := newMockStorage()
		fs := New(storage)

		file, _ := fs.Create("old.txt")
		file.Close()

		storage.getError = errors.New("get failed")
		err := fs.Rename("old.txt", "new.txt")
		if err == nil {
			t.Error("expected error")
		}
	})

	t.Run("Rename with put error", func(t *testing.T) {
		storage := newMockStorage()
		fs := New(storage)

		file, _ := fs.Create("old.txt")
		file.Close()

		storage.putError = errors.New("put failed")
		err := fs.Rename("old.txt", "new.txt")
		if err == nil {
			t.Error("expected error")
		}
	})

	t.Run("Readdir on non-directory", func(t *testing.T) {
		storage := newMockStorage()
		storage.Put("test.txt", bytes.NewReader([]byte("test")))
		fs := New(storage)

		file, _ := newStorageFile(fs, "test.txt", os.O_RDONLY, 0644)
		_, err := file.Readdir(-1)
		if err != ErrNotDirectory {
			t.Errorf("expected ErrNotDirectory, got %v", err)
		}
	})

	t.Run("Readdirnames on non-directory", func(t *testing.T) {
		storage := newMockStorage()
		storage.Put("test.txt", bytes.NewReader([]byte("test")))
		fs := New(storage)

		file, _ := newStorageFile(fs, "test.txt", os.O_RDONLY, 0644)
		_, err := file.Readdirnames(-1)
		if err != ErrNotDirectory {
			t.Errorf("expected ErrNotDirectory, got %v", err)
		}
	})
}

// TestFile_ClosedFileOperations_All tests all operations on closed files
func TestFile_ClosedFileOperations_All(t *testing.T) {
	storage := newMockStorage()
	fs := New(storage)

	file, _ := newStorageFile(fs, "test.txt", os.O_RDWR|os.O_CREATE, 0644)
	file.Write([]byte("test"))
	file.Close()

	buf := make([]byte, 10)

	tests := []struct {
		name string
		fn   func() error
	}{
		{"Read", func() error { _, err := file.Read(buf); return err }},
		{"ReadAt", func() error { _, err := file.ReadAt(buf, 0); return err }},
		{"Write", func() error { _, err := file.Write([]byte("test")); return err }},
		{"WriteAt", func() error { _, err := file.WriteAt([]byte("test"), 0); return err }},
		{"Seek", func() error { _, err := file.Seek(0, io.SeekStart); return err }},
		{"Truncate", func() error { return file.Truncate(0) }},
		{"Sync", func() error { return file.Sync() }},
		{"Stat", func() error { _, err := file.Stat(); return err }},
		{"Readdir", func() error { _, err := file.Readdir(-1); return err }},
		{"Readdirnames", func() error { _, err := file.Readdirnames(-1); return err }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.fn()
			if !errors.Is(err, os.ErrClosed) {
				t.Errorf("expected ErrClosed, got %v", err)
			}
		})
	}
}

// TestNewStorageFile_AllFlagCombinations tests different flag combinations
func TestNewStorageFile_AllFlagCombinations(t *testing.T) {
	tests := []struct {
		name string
		flag int
		prep func(*mockStorage)
	}{
		{"O_RDONLY existing", os.O_RDONLY, func(s *mockStorage) {
			s.Put("test.txt", bytes.NewReader([]byte("content")))
		}},
		{"O_WRONLY|O_CREATE new", os.O_WRONLY | os.O_CREATE, func(s *mockStorage) {}},
		{"O_RDWR|O_CREATE new", os.O_RDWR | os.O_CREATE, func(s *mockStorage) {}},
		{"O_WRONLY|O_APPEND existing", os.O_WRONLY | os.O_APPEND, func(s *mockStorage) {
			s.Put("test.txt", bytes.NewReader([]byte("hello")))
		}},
		{"O_WRONLY|O_CREATE|O_TRUNC existing", os.O_WRONLY | os.O_CREATE | os.O_TRUNC, func(s *mockStorage) {
			s.Put("test.txt", bytes.NewReader([]byte("old")))
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage := newMockStorage()
			tt.prep(storage)
			fs := New(storage)

			file, err := newStorageFile(fs, "test.txt", tt.flag, 0644)
			if err != nil {
				t.Errorf("expected no error, got %v", err)
			}
			if file == nil {
				t.Error("expected file, got nil")
			}
			file.Close()
		})
	}
}

// TestStorageFS_RemoveAll_EdgeCases tests RemoveAll edge cases
func TestStorageFS_RemoveAll_EdgeCases(t *testing.T) {
	t.Run("RemoveAll non-existent path", func(t *testing.T) {
		storage := newMockStorage()
		fs := New(storage)

		err := fs.RemoveAll("nonexistent")
		if err != nil {
			t.Errorf("RemoveAll should not error on non-existent, got %v", err)
		}
	})

	t.Run("RemoveAll existing file", func(t *testing.T) {
		storage := newMockStorage()
		fs := New(storage)

		file, _ := fs.Create("test.txt")
		file.Close()

		err := fs.RemoveAll("test.txt")
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
	})

	t.Run("RemoveAll existing directory", func(t *testing.T) {
		storage := newMockStorage()
		fs := New(storage)

		fs.Mkdir("testdir", 0755)

		err := fs.RemoveAll("testdir")
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
	})
}

// TestFile_Read_WriteOnlyFile tests read on write-only file
func TestFile_Read_WriteOnlyFile(t *testing.T) {
	storage := newMockStorage()
	fs := New(storage)

	file, _ := newStorageFile(fs, "test.txt", os.O_WRONLY|os.O_CREATE, 0644)

	buf := make([]byte, 10)
	_, err := file.Read(buf)
	if err != os.ErrPermission {
		t.Errorf("expected ErrPermission, got %v", err)
	}
}

// TestNormalizePath_MoreEdgeCases tests additional path normalization
func TestNormalizePath_MoreEdgeCases(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"//double//slash", "double/slash"},
		{"./current/./dir", "current/dir"},
		{"parent/../child", "child"},
	}

	for _, tt := range tests {
		result := normalizePath(tt.input)
		if result != tt.expected {
			t.Errorf("normalizePath(%q) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}

// TestSplitPath_MoreEdgeCases tests additional path splitting
func TestSplitPath_MoreEdgeCases(t *testing.T) {
	tests := []struct {
		input          string
		expectedLength int
	}{
		{"", 0},
		{"single", 1},
		{"a/b", 2},
		{"a/b/c/d", 4},
	}

	for _, tt := range tests {
		result := splitPath(tt.input)
		if len(result) != tt.expectedLength {
			t.Errorf("splitPath(%q) length = %d, want %d", tt.input, len(result), tt.expectedLength)
		}
	}
}
