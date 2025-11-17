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
	"errors"
	"os"
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	storage := newMockStorage()
	fs := New(storage)

	if fs == nil {
		t.Fatal("New() returned nil")
	}

	// Verify it implements Fs interface
	var _ Fs = fs
}

func TestStorageFS_Name(t *testing.T) {
	storage := newMockStorage()
	fs := New(storage)

	name := fs.Name()
	if name != "StorageFS" {
		t.Errorf("Name() = %q, want %q", name, "StorageFS")
	}
}

func TestNormalizePath(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"empty path", "", "."},
		{"current dir", ".", "."},
		{"root", "/", "."},
		{"simple path", "file.txt", "file.txt"},
		{"with leading slash", "/file.txt", "file.txt"},
		{"with double slash", "//file.txt", "file.txt"},
		{"directory path", "/dir/file.txt", "dir/file.txt"},
		{"with trailing slash", "/dir/", "dir"},
		{"complex path", "//dir1//dir2///file.txt", "dir1/dir2/file.txt"},
		{"parent refs", "../file.txt", "../file.txt"},
		{"current refs", "./dir/./file.txt", "dir/file.txt"},
		{"windows-style", "dir\\file.txt", "dir/file.txt"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := normalizePath(tt.input)
			if got != tt.want {
				t.Errorf("normalizePath(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestIsDir(t *testing.T) {
	tests := []struct {
		name string
		path string
		want bool
	}{
		{"empty path", "", true},
		{"current dir", ".", true},
		{"root", "/", true},
		{"file with extension", "file.txt", false},
		{"file without extension", "file", false},
		{"directory-like", "dir/", true},
		{"nested file", "dir/file.txt", false},
		{"nested dir", "dir/subdir/", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isDir(tt.path)
			if got != tt.want {
				t.Errorf("isDir(%q) = %v, want %v", tt.path, got, tt.want)
			}
		})
	}
}

func TestStorageFS_Mkdir(t *testing.T) {
	storage := newMockStorage()
	fs := New(storage)

	err := fs.Mkdir("testdir", 0755)
	if err != nil {
		t.Fatalf("Mkdir() error = %v", err)
	}

	// Verify directory marker was created
	if !storage.exists("testdir/.dir") {
		t.Error("directory marker file not created")
	}

	// Verify metadata was created
	if !storage.exists(".meta/testdir") {
		t.Error("directory metadata not created")
	}
}

func TestStorageFS_Mkdir_AlreadyExists(t *testing.T) {
	storage := newMockStorage()
	fs := New(storage)

	// Create directory
	err := fs.Mkdir("testdir", 0755)
	if err != nil {
		t.Fatalf("Mkdir() error = %v", err)
	}

	// Try to create again
	err = fs.Mkdir("testdir", 0755)
	if !errors.Is(err, os.ErrExist) {
		t.Errorf("Mkdir() error = %v, want os.ErrExist", err)
	}
}

func TestStorageFS_MkdirAll(t *testing.T) {
	storage := newMockStorage()
	fs := New(storage)

	err := fs.MkdirAll("dir1/dir2/dir3", 0755)
	if err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}

	// Verify all directory markers were created
	dirs := []string{"dir1", "dir1/dir2", "dir1/dir2/dir3"}
	for _, dir := range dirs {
		if !storage.exists(dir + "/.dir") {
			t.Errorf("directory marker for %q not created", dir)
		}
		if !storage.exists(".meta/" + dir) {
			t.Errorf("metadata for %q not created", dir)
		}
	}
}

func TestStorageFS_MkdirAll_Idempotent(t *testing.T) {
	storage := newMockStorage()
	fs := New(storage)

	// Create directory structure
	err := fs.MkdirAll("dir1/dir2", 0755)
	if err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}

	// Call again - should succeed
	err = fs.MkdirAll("dir1/dir2", 0755)
	if err != nil {
		t.Errorf("MkdirAll() second call error = %v, want nil", err)
	}
}

func TestStorageFS_Remove(t *testing.T) {
	storage := newMockStorage()
	fs := New(storage)

	// Create a directory
	if err := fs.Mkdir("testdir", 0755); err != nil {
		t.Fatalf("Mkdir() error = %v", err)
	}

	// Remove it
	err := fs.Remove("testdir")
	if err != nil {
		t.Fatalf("Remove() error = %v", err)
	}

	// Verify it's gone
	if storage.exists("testdir/.dir") {
		t.Error("directory marker still exists after Remove")
	}
	if storage.exists(".meta/testdir") {
		t.Error("metadata still exists after Remove")
	}
}

func TestStorageFS_Remove_NotExist(t *testing.T) {
	storage := newMockStorage()
	fs := New(storage)

	err := fs.Remove("nonexistent")
	if !errors.Is(err, os.ErrNotExist) {
		t.Errorf("Remove() error = %v, want os.ErrNotExist", err)
	}
}

func TestStorageFS_RemoveAll(t *testing.T) {
	storage := newMockStorage()
	fs := New(storage)

	// Create nested directory structure
	if err := fs.MkdirAll("dir1/dir2/dir3", 0755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}

	// Remove top level
	err := fs.RemoveAll("dir1")
	if err != nil {
		t.Fatalf("RemoveAll() error = %v", err)
	}

	// Verify all are gone
	keys := storage.listKeys("dir1")
	if len(keys) > 0 {
		t.Errorf("RemoveAll() left %d keys, want 0", len(keys))
	}

	// Metadata should also be gone
	metaKeys := storage.listKeys(".meta/dir1")
	if len(metaKeys) > 0 {
		t.Errorf("RemoveAll() left %d metadata keys, want 0", len(metaKeys))
	}
}

func TestStorageFS_Stat_Directory(t *testing.T) {
	storage := newMockStorage()
	fs := New(storage)

	if err := fs.Mkdir("testdir", 0755); err != nil {
		t.Fatalf("Mkdir() error = %v", err)
	}

	info, err := fs.Stat("testdir")
	if err != nil {
		t.Fatalf("Stat() error = %v", err)
	}

	if !info.IsDir() {
		t.Error("Stat() IsDir() = false, want true")
	}
	if info.Name() != "testdir" {
		t.Errorf("Stat() Name() = %q, want %q", info.Name(), "testdir")
	}
	if info.Mode()&os.ModeDir == 0 {
		t.Error("Stat() Mode() doesn't have ModeDir bit set")
	}
}

func TestStorageFS_Stat_NotExist(t *testing.T) {
	storage := newMockStorage()
	fs := New(storage)

	_, err := fs.Stat("nonexistent")
	if !errors.Is(err, os.ErrNotExist) {
		t.Errorf("Stat() error = %v, want os.ErrNotExist", err)
	}
}

func TestStorageFS_Rename(t *testing.T) {
	storage := newMockStorage()
	fs := New(storage)

	// Create a directory
	if err := fs.Mkdir("oldname", 0755); err != nil {
		t.Fatalf("Mkdir() error = %v", err)
	}

	// Rename it
	err := fs.Rename("oldname", "newname")
	if err != nil {
		t.Fatalf("Rename() error = %v", err)
	}

	// Verify old name doesn't exist
	if storage.exists("oldname/.dir") {
		t.Error("old directory marker still exists")
	}

	// Verify new name exists
	if !storage.exists("newname/.dir") {
		t.Error("new directory marker not created")
	}
	if !storage.exists(".meta/newname") {
		t.Error("new metadata not created")
	}
}

func TestStorageFS_Rename_NotExist(t *testing.T) {
	storage := newMockStorage()
	fs := New(storage)

	err := fs.Rename("nonexistent", "newname")
	if !errors.Is(err, os.ErrNotExist) {
		t.Errorf("Rename() error = %v, want os.ErrNotExist", err)
	}
}

func TestStorageFS_Chmod(t *testing.T) {
	storage := newMockStorage()
	fs := New(storage)

	// Create a directory
	if err := fs.Mkdir("testdir", 0755); err != nil {
		t.Fatalf("Mkdir() error = %v", err)
	}

	// Change mode
	err := fs.Chmod("testdir", 0700)
	if err != nil {
		t.Fatalf("Chmod() error = %v", err)
	}

	// Verify mode changed in metadata
	info, err := fs.Stat("testdir")
	if err != nil {
		t.Fatalf("Stat() error = %v", err)
	}

	mode := info.Mode() & os.ModePerm
	if mode != 0700 {
		t.Errorf("Chmod() mode = %o, want %o", mode, 0700)
	}
}

func TestStorageFS_Chtimes(t *testing.T) {
	storage := newMockStorage()
	fs := New(storage)

	// Create a directory
	if err := fs.Mkdir("testdir", 0755); err != nil {
		t.Fatalf("Mkdir() error = %v", err)
	}

	// Change times
	newTime := time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC)
	err := fs.Chtimes("testdir", newTime, newTime)
	if err != nil {
		t.Fatalf("Chtimes() error = %v", err)
	}

	// Verify time changed in metadata
	info, err := fs.Stat("testdir")
	if err != nil {
		t.Fatalf("Stat() error = %v", err)
	}

	if !info.ModTime().Equal(newTime) {
		t.Errorf("Chtimes() modTime = %v, want %v", info.ModTime(), newTime)
	}
}

func TestStorageFS_Chown(t *testing.T) {
	storage := newMockStorage()
	fs := New(storage)

	// Create a directory
	if err := fs.Mkdir("testdir", 0755); err != nil {
		t.Fatalf("Mkdir() error = %v", err)
	}

	// Chown should return ErrInvalid (not supported)
	err := fs.Chown("testdir", 1000, 1000)
	if !errors.Is(err, os.ErrInvalid) {
		t.Errorf("Chown() error = %v, want os.ErrInvalid", err)
	}
}

func TestMetadataPrefix(t *testing.T) {
	storage := newMockStorage()
	fs := New(storage)

	// Create directory
	if err := fs.Mkdir("test", 0755); err != nil {
		t.Fatalf("Mkdir() error = %v", err)
	}

	// Verify metadata is stored with .meta/ prefix
	keys := storage.listKeys(".meta/")
	found := false
	for _, k := range keys {
		if k == ".meta/test" {
			found = true
			break
		}
	}
	if !found {
		t.Error("metadata not stored with .meta/ prefix")
	}
}
