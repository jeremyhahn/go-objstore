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
)

// TestFinalCoverage_AllDirectoryOperations tests directory operations comprehensively
func TestFinalCoverage_AllDirectoryOperations(t *testing.T) {
	storage := newMockStorage()
	fs := New(storage)

	// Test all directory file operations
	fs.Mkdir("testdir", 0755)
	file, _ := newStorageFile(fs, "testdir", os.O_RDONLY, 0755)

	buf := make([]byte, 10)

	// Read on directory
	_, err := file.Read(buf)
	if err != ErrIsDirectory {
		t.Errorf("Read: expected ErrIsDirectory, got %v", err)
	}

	// ReadAt on directory
	_, err = file.ReadAt(buf, 0)
	if err != ErrIsDirectory {
		t.Errorf("ReadAt: expected ErrIsDirectory, got %v", err)
	}

	// Seek on directory
	_, err = file.Seek(0, io.SeekStart)
	if err != ErrIsDirectory {
		t.Errorf("Seek: expected ErrIsDirectory, got %v", err)
	}

	// Write on directory
	file2, _ := newStorageFile(fs, "testdir", os.O_WRONLY, 0755)
	_, err = file2.Write([]byte("test"))
	if err != ErrIsDirectory {
		t.Errorf("Write: expected ErrIsDirectory, got %v", err)
	}

	// WriteAt on directory
	_, err = file2.WriteAt([]byte("test"), 0)
	if err != ErrIsDirectory {
		t.Errorf("WriteAt: expected ErrIsDirectory, got %v", err)
	}

	// Truncate on directory
	err = file2.Truncate(10)
	if err != ErrIsDirectory {
		t.Errorf("Truncate: expected ErrIsDirectory, got %v", err)
	}
}

// TestFinalCoverage_ReadOnlyFileOperations tests read-only file write attempts
func TestFinalCoverage_ReadOnlyFileOperations(t *testing.T) {
	storage := newMockStorage()
	storage.Put("test.txt", bytes.NewReader([]byte("hello")))
	fs := New(storage)

	file, _ := newStorageFile(fs, "test.txt", os.O_RDONLY, 0644)

	// Write on read-only
	_, err := file.Write([]byte("test"))
	if err != os.ErrPermission {
		t.Errorf("Write: expected ErrPermission, got %v", err)
	}

	// WriteAt on read-only
	_, err = file.WriteAt([]byte("test"), 0)
	if err != os.ErrPermission {
		t.Errorf("WriteAt: expected ErrPermission, got %v", err)
	}

	// Truncate on read-only
	err = file.Truncate(10)
	if err != os.ErrPermission {
		t.Errorf("Truncate: expected ErrPermission, got %v", err)
	}
}

// TestFinalCoverage_WriteOnlyFileOperations tests write-only file read attempts
func TestFinalCoverage_WriteOnlyFileOperations(t *testing.T) {
	storage := newMockStorage()
	storage.Put("test.txt", bytes.NewReader([]byte("hello")))
	fs := New(storage)

	file, _ := newStorageFile(fs, "test.txt", os.O_WRONLY, 0644)

	buf := make([]byte, 10)

	// ReadAt on write-only
	_, err := file.ReadAt(buf, 0)
	if err != os.ErrPermission {
		t.Errorf("ReadAt: expected ErrPermission, got %v", err)
	}
}

// TestFinalCoverage_StorageErrors tests error handling in storage operations
func TestFinalCoverage_StorageErrors(t *testing.T) {
	t.Run("Close with put error", func(t *testing.T) {
		storage := newMockStorage()
		fs := New(storage)

		file, _ := newStorageFile(fs, "test.txt", os.O_RDWR|os.O_CREATE, 0644)
		file.Write([]byte("test"))

		storage.putError = errors.New("put failed")
		err := file.Close()
		if err == nil {
			t.Error("expected error on close with put failure")
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
			t.Error("expected error on sync with put failure")
		}
	})

	t.Run("Mkdir with put error", func(t *testing.T) {
		storage := newMockStorage()
		storage.putError = errors.New("put failed")
		fs := New(storage)

		err := fs.Mkdir("testdir", 0755)
		if err == nil {
			t.Error("expected error on mkdir with put failure")
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
			t.Error("expected error on remove with delete failure")
		}
	})

	t.Run("Remove directory with delete error", func(t *testing.T) {
		storage := newMockStorage()
		fs := New(storage)

		fs.Mkdir("testdir", 0755)

		storage.deleteError = errors.New("delete failed")
		err := fs.Remove("testdir")
		if err == nil {
			t.Error("expected error on remove with delete failure")
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
			t.Error("expected error on rename with get failure")
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
			t.Error("expected error on rename with put failure")
		}
	})

	t.Run("NewStorageFile with read error", func(t *testing.T) {
		storage := newMockStorage()
		storage.Put("test.txt", bytes.NewReader([]byte("content")))
		storage.getError = errors.New("read failed")
		fs := New(storage)

		_, err := newStorageFile(fs, "test.txt", os.O_RDONLY, 0644)
		if err == nil {
			t.Error("expected error opening file with read failure")
		}
	})
}
