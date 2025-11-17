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
	"io"
	"os"
	"testing"
	"time"
)

func TestStorageFS_Create(t *testing.T) {
	storage := newMockStorage()
	fs := New(storage)

	file, err := fs.Create("test.txt")
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	defer file.Close()

	if file == nil {
		t.Fatal("Create() returned nil file")
	}

	data := []byte("hello world")
	n, err := file.Write(data)
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if n != len(data) {
		t.Errorf("Write() wrote %d bytes, want %d", n, len(data))
	}

	if err := file.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if !storage.exists("test.txt") {
		t.Error("file not found in storage after Create")
	}

	if !storage.exists(".meta/test.txt") {
		t.Error("metadata not found after Create")
	}
}

func TestStorageFS_Open(t *testing.T) {
	storage := newMockStorage()
	fs := New(storage)

	file, err := fs.Create("test.txt")
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	data := []byte("hello world")
	file.Write(data)
	file.Close()

	file, err = fs.Open("test.txt")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer file.Close()

	readData := make([]byte, len(data))
	n, err := file.Read(readData)
	if err != nil && err != io.EOF {
		t.Fatalf("Read() error = %v", err)
	}
	if n != len(data) {
		t.Errorf("Read() read %d bytes, want %d", n, len(data))
	}
	if string(readData) != string(data) {
		t.Errorf("Read() data = %q, want %q", readData, data)
	}
}

func TestStorageFS_Open_NotExist(t *testing.T) {
	storage := newMockStorage()
	fs := New(storage)

	_, err := fs.Open("nonexistent.txt")
	if !errors.Is(err, os.ErrNotExist) {
		t.Errorf("Open() error = %v, want os.ErrNotExist", err)
	}
}

func TestStorageFS_OpenFile_ReadOnly(t *testing.T) {
	storage := newMockStorage()
	fs := New(storage)

	file, err := fs.Create("test.txt")
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	data := []byte("hello world")
	file.Write(data)
	file.Close()

	file, err = fs.OpenFile("test.txt", os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("OpenFile() error = %v", err)
	}
	defer file.Close()

	_, err = file.Write([]byte("new data"))
	if err == nil {
		t.Error("Write() on RDONLY file succeeded, want error")
	}
}

func TestStorageFS_OpenFile_WriteOnly(t *testing.T) {
	storage := newMockStorage()
	fs := New(storage)

	file, err := fs.OpenFile("test.txt", os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("OpenFile() error = %v", err)
	}
	defer file.Close()

	data := []byte("hello world")
	n, err := file.Write(data)
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if n != len(data) {
		t.Errorf("Write() wrote %d bytes, want %d", n, len(data))
	}

	file.Close()

	if !storage.exists("test.txt") {
		t.Error("file not found after OpenFile with CREATE")
	}
}

func TestFileInfo_Methods(t *testing.T) {
	info := &fileInfo{
		name:    "test.txt",
		size:    1024,
		mode:    0644,
		modTime: time.Time{},
		isDir:   false,
	}

	if info.Name() != "test.txt" {
		t.Errorf("Name() = %q, want %q", info.Name(), "test.txt")
	}

	if info.Size() != 1024 {
		t.Errorf("Size() = %d, want %d", info.Size(), 1024)
	}

	if info.Mode() != 0644 {
		t.Errorf("Mode() = %o, want %o", info.Mode(), 0644)
	}

	if !info.ModTime().IsZero() {
		t.Errorf("ModTime() = %v, want zero time", info.ModTime())
	}

	if info.IsDir() != false {
		t.Errorf("IsDir() = %v, want %v", info.IsDir(), false)
	}

	if info.Sys() != nil {
		t.Errorf("Sys() = %v, want nil", info.Sys())
	}
}

func TestFileInfo_Directory(t *testing.T) {
	info := &fileInfo{
		name:  "testdir",
		size:  0,
		mode:  os.ModeDir | 0755,
		isDir: true,
	}

	if !info.IsDir() {
		t.Error("IsDir() = false for directory, want true")
	}

	if info.Mode()&os.ModeDir == 0 {
		t.Error("Mode() doesn't have ModeDir bit set")
	}
}
