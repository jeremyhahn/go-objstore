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
	"fmt"
	"io"
	"os"
	"path"
	"testing"
)

// TestReaddir_BasicFunctionality tests basic directory listing
func TestReaddir_BasicFunctionality(t *testing.T) {
	storage := newMockStorage()
	fs := New(storage)

	// Create a directory with some files and subdirectories
	if err := fs.MkdirAll("testdir", 0755); err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}

	// Create some files in the directory
	files := []string{"file1.txt", "file2.txt", "file3.txt"}
	for _, filename := range files {
		filepath := path.Join("testdir", filename)
		f, err := fs.Create(filepath)
		if err != nil {
			t.Fatalf("Failed to create file %s: %v", filename, err)
		}
		_, _ = f.WriteString("test content")
		f.Close()
	}

	// Create some subdirectories
	subdirs := []string{"subdir1", "subdir2"}
	for _, dirname := range subdirs {
		dirpath := path.Join("testdir", dirname)
		if err := fs.MkdirAll(dirpath, 0755); err != nil {
			t.Fatalf("Failed to create subdirectory %s: %v", dirname, err)
		}
	}

	// Open the directory and read all entries
	dir, err := fs.Open("testdir")
	if err != nil {
		t.Fatalf("Failed to open directory: %v", err)
	}
	defer dir.Close()

	entries, err := dir.Readdir(-1)
	if err != nil && err != io.EOF {
		t.Fatalf("Failed to read directory: %v", err)
	}

	// Should have 3 files + 2 subdirectories = 5 entries
	expectedCount := len(files) + len(subdirs)
	if len(entries) != expectedCount {
		t.Errorf("Expected %d entries, got %d", expectedCount, len(entries))
	}

	// Verify each entry exists
	foundFiles := make(map[string]bool)
	foundDirs := make(map[string]bool)

	for _, entry := range entries {
		if entry.IsDir() {
			foundDirs[entry.Name()] = true
		} else {
			foundFiles[entry.Name()] = true
		}
	}

	for _, filename := range files {
		if !foundFiles[filename] {
			t.Errorf("Expected file %s not found in directory listing", filename)
		}
	}

	for _, dirname := range subdirs {
		if !foundDirs[dirname] {
			t.Errorf("Expected subdirectory %s not found in directory listing", dirname)
		}
	}
}

// TestReaddir_PaginatedReading tests reading directory in chunks
func TestReaddir_PaginatedReading(t *testing.T) {
	storage := newMockStorage()
	fs := New(storage)

	// Create a directory with multiple files
	if err := fs.MkdirAll("testdir", 0755); err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}

	// Create 10 files
	for i := 0; i < 10; i++ {
		filename := path.Join("testdir", fmt.Sprintf("file%02d.txt", i))
		f, err := fs.Create(filename)
		if err != nil {
			t.Fatalf("Failed to create file: %v", err)
		}
		f.Close()
	}

	// Open directory
	dir, err := fs.Open("testdir")
	if err != nil {
		t.Fatalf("Failed to open directory: %v", err)
	}
	defer dir.Close()

	// Read in chunks of 3
	var allEntries []os.FileInfo
	for {
		entries, err := dir.Readdir(3)
		allEntries = append(allEntries, entries...)

		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Failed to read directory: %v", err)
		}

		// Should never get more than 3 at a time (unless last chunk)
		if len(entries) > 3 {
			t.Errorf("Expected at most 3 entries per read, got %d", len(entries))
		}
	}

	// Should have read all 10 files
	if len(allEntries) != 10 {
		t.Errorf("Expected 10 total entries, got %d", len(allEntries))
	}
}

// TestReaddirnames_BasicFunctionality tests basic directory name listing
func TestReaddirnames_BasicFunctionality(t *testing.T) {
	storage := newMockStorage()
	fs := New(storage)

	// Create a directory with files
	if err := fs.MkdirAll("testdir", 0755); err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}

	expectedNames := []string{"file1.txt", "file2.txt", "subdir"}
	for _, name := range expectedNames {
		if name == "subdir" {
			if err := fs.MkdirAll(path.Join("testdir", name), 0755); err != nil {
				t.Fatalf("Failed to create subdirectory: %v", err)
			}
		} else {
			f, err := fs.Create(path.Join("testdir", name))
			if err != nil {
				t.Fatalf("Failed to create file: %v", err)
			}
			f.Close()
		}
	}

	// Open directory and read names
	dir, err := fs.Open("testdir")
	if err != nil {
		t.Fatalf("Failed to open directory: %v", err)
	}
	defer dir.Close()

	names, err := dir.Readdirnames(-1)
	if err != nil && err != io.EOF {
		t.Fatalf("Failed to read directory names: %v", err)
	}

	if len(names) != len(expectedNames) {
		t.Errorf("Expected %d names, got %d", len(expectedNames), len(names))
	}

	// Verify all expected names are present
	foundNames := make(map[string]bool)
	for _, name := range names {
		foundNames[name] = true
	}

	for _, expected := range expectedNames {
		if !foundNames[expected] {
			t.Errorf("Expected name %s not found in listing", expected)
		}
	}
}

// TestReaddir_EmptyDirectory tests reading an empty directory
func TestReaddir_EmptyDirectory(t *testing.T) {
	storage := newMockStorage()
	fs := New(storage)

	// Create an empty directory
	if err := fs.MkdirAll("emptydir", 0755); err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}

	// Open and read directory
	dir, err := fs.Open("emptydir")
	if err != nil {
		t.Fatalf("Failed to open directory: %v", err)
	}
	defer dir.Close()

	entries, err := dir.Readdir(-1)
	if err != nil && err != io.EOF {
		t.Fatalf("Failed to read directory: %v", err)
	}

	if len(entries) != 0 {
		t.Errorf("Expected 0 entries in empty directory, got %d", len(entries))
	}
}

// TestReaddir_OnNonDirectory tests that Readdir fails on files
func TestReaddir_OnNonDirectory(t *testing.T) {
	storage := newMockStorage()
	fs := New(storage)

	// Create a file
	f, err := fs.Create("testfile.txt")
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	f.Close()

	// Try to open file and read as directory
	file, err := fs.Open("testfile.txt")
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()

	_, err = file.Readdir(-1)
	if err != ErrNotDirectory {
		t.Errorf("Expected ErrNotDirectory, got %v", err)
	}
}

// TestReaddir_MetadataHandling tests that file metadata is properly returned
func TestReaddir_MetadataHandling(t *testing.T) {
	storage := newMockStorage()
	fs := New(storage)

	// Create directory
	if err := fs.MkdirAll("testdir", 0755); err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}

	// Create a file with specific content
	filepath := path.Join("testdir", "test.txt")
	f, err := fs.Create(filepath)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	testContent := []byte("Hello, World!")
	if _, err := f.Write(testContent); err != nil {
		t.Fatalf("Failed to write to file: %v", err)
	}
	f.Close()

	// Read directory
	dir, err := fs.Open("testdir")
	if err != nil {
		t.Fatalf("Failed to open directory: %v", err)
	}
	defer dir.Close()

	entries, err := dir.Readdir(-1)
	if err != nil && err != io.EOF {
		t.Fatalf("Failed to read directory: %v", err)
	}

	if len(entries) != 1 {
		t.Fatalf("Expected 1 entry, got %d", len(entries))
	}

	entry := entries[0]
	if entry.Name() != "test.txt" {
		t.Errorf("Expected name 'test.txt', got %s", entry.Name())
	}

	if entry.IsDir() {
		t.Error("Expected entry to be a file, not a directory")
	}

	// Size should match the content we wrote
	if entry.Size() != int64(len(testContent)) {
		t.Errorf("Expected size %d, got %d", len(testContent), entry.Size())
	}
}

// TestReaddir_NestedDirectories tests listing with nested directory structure
func TestReaddir_NestedDirectories(t *testing.T) {
	storage := newMockStorage()
	fs := New(storage)

	// Create nested directory structure
	if err := fs.MkdirAll("a/b/c", 0755); err != nil {
		t.Fatalf("Failed to create nested directories: %v", err)
	}

	// Add files at different levels
	for _, filepath := range []string{"a/file1.txt", "a/b/file2.txt", "a/b/c/file3.txt"} {
		f, err := fs.Create(filepath)
		if err != nil {
			t.Fatalf("Failed to create file %s: %v", filepath, err)
		}
		f.Close()
	}

	// List directory "a" - should only show direct children
	dir, err := fs.Open("a")
	if err != nil {
		t.Fatalf("Failed to open directory: %v", err)
	}
	defer dir.Close()

	entries, err := dir.Readdir(-1)
	if err != nil && err != io.EOF {
		t.Fatalf("Failed to read directory: %v", err)
	}

	// Should have: file1.txt and subdirectory b
	if len(entries) != 2 {
		t.Errorf("Expected 2 entries in directory 'a', got %d", len(entries))
	}

	// Verify we have one file and one directory
	fileCount := 0
	dirCount := 0
	for _, entry := range entries {
		if entry.IsDir() {
			dirCount++
			if entry.Name() != "b" {
				t.Errorf("Expected directory 'b', got %s", entry.Name())
			}
		} else {
			fileCount++
			if entry.Name() != "file1.txt" {
				t.Errorf("Expected file 'file1.txt', got %s", entry.Name())
			}
		}
	}

	if fileCount != 1 || dirCount != 1 {
		t.Errorf("Expected 1 file and 1 directory, got %d files and %d directories", fileCount, dirCount)
	}
}

// TestReaddir_RootDirectory tests listing the root directory
func TestReaddir_RootDirectory(t *testing.T) {
	storage := newMockStorage()
	fs := New(storage)

	// Create a testdir to work with (mock storage doesn't support true root operations)
	if err := fs.MkdirAll("testroot", 0755); err != nil {
		t.Fatalf("Failed to create test directory: %v", err)
	}

	// Create some items in the test root directory
	if err := fs.MkdirAll("testroot/dir1", 0755); err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}

	f, err := fs.Create("testroot/file1.txt")
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	f.Close()

	// List the test root directory
	dir, err := fs.Open("testroot")
	if err != nil {
		t.Fatalf("Failed to open directory: %v", err)
	}
	defer dir.Close()

	entries, err := dir.Readdir(-1)
	if err != nil && err != io.EOF {
		t.Fatalf("Failed to read directory: %v", err)
	}

	// Should have dir1 and file1.txt
	foundDir := false
	foundFile := false
	for _, entry := range entries {
		if entry.Name() == "dir1" && entry.IsDir() {
			foundDir = true
		}
		if entry.Name() == "file1.txt" && !entry.IsDir() {
			foundFile = true
		}
	}

	if !foundDir {
		t.Error("Expected to find 'dir1' directory in listing")
	}
	if !foundFile {
		t.Error("Expected to find 'file1.txt' file in listing")
	}
}

// TestReaddirnames_Pagination tests paginated name reading
func TestReaddirnames_Pagination(t *testing.T) {
	storage := newMockStorage()
	fs := New(storage)

	// Create directory with files
	if err := fs.MkdirAll("testdir", 0755); err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}

	// Create 5 files
	for i := 0; i < 5; i++ {
		filename := path.Join("testdir", fmt.Sprintf("file%02d.txt", i))
		f, err := fs.Create(filename)
		if err != nil {
			t.Fatalf("Failed to create file: %v", err)
		}
		f.Close()
	}

	// Open directory
	dir, err := fs.Open("testdir")
	if err != nil {
		t.Fatalf("Failed to open directory: %v", err)
	}
	defer dir.Close()

	// Read 2 names at a time
	allNames := []string{}
	for {
		names, err := dir.Readdirnames(2)
		allNames = append(allNames, names...)

		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Failed to read directory names: %v", err)
		}
	}

	if len(allNames) != 5 {
		t.Errorf("Expected 5 total names, got %d", len(allNames))
	}
}

// TestReaddir_ClosedDirectory tests that reading closed directory fails
func TestReaddir_ClosedDirectory(t *testing.T) {
	storage := newMockStorage()
	fs := New(storage)

	if err := fs.MkdirAll("testdir", 0755); err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}

	dir, err := fs.Open("testdir")
	if err != nil {
		t.Fatalf("Failed to open directory: %v", err)
	}

	// Close the directory
	dir.Close()

	// Try to read from closed directory
	_, err = dir.Readdir(-1)
	if err == nil {
		t.Error("Expected error when reading closed directory")
	}
}

// TestDirEntry_Methods tests the dirEntry interface methods
func TestDirEntry_Methods(t *testing.T) {
	storage := newMockStorage()
	fs := New(storage)

	// Create a directory with a file and subdirectory
	if err := fs.MkdirAll("testdir", 0755); err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}

	// Create a file
	f, err := fs.Create("testdir/file.txt")
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	f.WriteString("test content")
	f.Close()

	// Create a subdirectory
	if err := fs.MkdirAll("testdir/subdir", 0755); err != nil {
		t.Fatalf("Failed to create subdirectory: %v", err)
	}

	// Read directory entries
	dir, err := fs.Open("testdir")
	if err != nil {
		t.Fatalf("Failed to open directory: %v", err)
	}
	defer dir.Close()

	entries, err := dir.Readdir(-1)
	if err != nil && err != io.EOF {
		t.Fatalf("Failed to read directory: %v", err)
	}

	// Find the file and directory entries
	var fileEntry, dirEntry os.FileInfo
	for _, entry := range entries {
		if entry.Name() == "file.txt" {
			fileEntry = entry
		} else if entry.Name() == "subdir" {
			dirEntry = entry
		}
	}

	if fileEntry == nil {
		t.Fatal("File entry not found")
	}

	if dirEntry == nil {
		t.Fatal("Directory entry not found")
	}

	// Test file entry methods
	if fileEntry.Name() != "file.txt" {
		t.Errorf("Expected name 'file.txt', got %s", fileEntry.Name())
	}

	if fileEntry.IsDir() {
		t.Error("File entry should not be a directory")
	}

	if fileEntry.Mode().IsDir() {
		t.Error("File entry mode should not indicate directory")
	}

	// Test directory entry methods
	if dirEntry.Name() != "subdir" {
		t.Errorf("Expected name 'subdir', got %s", dirEntry.Name())
	}

	if !dirEntry.IsDir() {
		t.Error("Directory entry should be a directory")
	}

	if !dirEntry.Mode().IsDir() {
		t.Error("Directory entry mode should indicate directory")
	}
}
