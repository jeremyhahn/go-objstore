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
	"strings"
	"sync"
	"testing"
	"time"
)

// mockStorageForFile extends the base mockStorage for file tests with metadata tracking
type mockStorageForFile struct {
	*mockStorage
	metadata map[string]mockMetadata
	mu       sync.RWMutex
	putErr   error
	getErr   error
}

type mockMetadata struct {
	size    int64
	modTime time.Time
}

func newMockStorageForFile() *mockStorageForFile {
	return &mockStorageForFile{
		mockStorage: newMockStorage(),
		metadata:    make(map[string]mockMetadata),
	}
}

func (m *mockStorageForFile) Put(key string, data io.Reader) error {
	if m.putErr != nil {
		return m.putErr
	}

	buf := new(bytes.Buffer)
	n, err := io.Copy(buf, data)
	if err != nil {
		return err
	}

	// Call parent Put
	if err := m.mockStorage.Put(key, bytes.NewReader(buf.Bytes())); err != nil {
		return err
	}

	// Store metadata
	m.mu.Lock()
	defer m.mu.Unlock()
	m.metadata[key] = mockMetadata{
		size:    n,
		modTime: time.Now(),
	}
	return nil
}

func (m *mockStorageForFile) Get(key string) (io.ReadCloser, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	return m.mockStorage.Get(key)
}

func (m *mockStorageForFile) getSize(key string) int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if md, ok := m.metadata[key]; ok {
		return md.size
	}
	return 0
}

func (m *mockStorageForFile) getModTime(key string) time.Time {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if md, ok := m.metadata[key]; ok {
		return md.modTime
	}
	return time.Time{}
}

// TestNewStorageFile_WriteMode tests creating a file in write mode
func TestNewStorageFile_WriteMode(t *testing.T) {
	storage := newMockStorageForFile()
	fs := New(storage)

	file, err := newStorageFile(fs, "test.txt", os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if file == nil {
		t.Fatal("expected file, got nil")
	}
	if file.Name() != "test.txt" {
		t.Errorf("expected name 'test.txt', got '%s'", file.Name())
	}
	if file.buf == nil {
		t.Error("expected buffer to be initialized for write mode")
	}
}

// TestNewStorageFile_ReadMode tests creating a file in read mode
func TestNewStorageFile_ReadMode(t *testing.T) {
	storage := newMockStorageForFile()
	testData := []byte("test content")
	_ = storage.Put("test.txt", bytes.NewReader(testData))

	fs := New(storage)

	file, err := newStorageFile(fs, "test.txt", os.O_RDONLY, 0644)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if file == nil {
		t.Fatal("expected file, got nil")
	}
	if file.buf == nil {
		t.Error("expected buffer to be initialized for read mode")
	}
}

// TestNewStorageFile_ReadNonExistent tests opening non-existent file in read mode
func TestNewStorageFile_ReadNonExistent(t *testing.T) {
	storage := newMockStorageForFile()
	fs := New(storage)

	_, err := newStorageFile(fs, "nonexistent.txt", os.O_RDONLY, 0644)
	if err == nil {
		t.Error("expected error for non-existent file, got nil")
	}
}

// TestStorageFile_Write tests writing data to a file
func TestStorageFile_Write(t *testing.T) {
	storage := newMockStorageForFile()
	fs := New(storage)

	file, err := newStorageFile(fs, "test.txt", os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	data := []byte("hello world")
	n, err := file.Write(data)
	if err != nil {
		t.Errorf("Write() failed: %v", err)
	}
	if n != len(data) {
		t.Errorf("expected %d bytes written, got %d", len(data), n)
	}

	// Data should be in buffer, not in storage yet
	if storage.exists("test.txt") {
		t.Error("data should not be in storage before Close()")
	}

	// Close to flush
	if err := file.Close(); err != nil {
		t.Errorf("Close() failed: %v", err)
	}

	// Now data should be in storage
	if !storage.exists("test.txt") {
		t.Error("data should be in storage after Close()")
	}

	rc, err := storage.Get("test.txt")
	if err != nil {
		t.Fatalf("Get() failed: %v", err)
	}
	stored, _ := io.ReadAll(rc)
	rc.Close()

	if !bytes.Equal(stored, data) {
		t.Errorf("expected stored data %q, got %q", data, stored)
	}
}

// TestStorageFile_WriteString tests WriteString method
func TestStorageFile_WriteString(t *testing.T) {
	storage := newMockStorageForFile()
	fs := New(storage)

	file, err := newStorageFile(fs, "test.txt", os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	data := "hello world"
	n, err := file.WriteString(data)
	if err != nil {
		t.Errorf("WriteString() failed: %v", err)
	}
	if n != len(data) {
		t.Errorf("expected %d bytes written, got %d", len(data), n)
	}

	if err := file.Close(); err != nil {
		t.Errorf("Close() failed: %v", err)
	}

	rc, err := storage.Get("test.txt")
	if err != nil {
		t.Fatalf("Get() failed: %v", err)
	}
	stored, _ := io.ReadAll(rc)
	rc.Close()

	if string(stored) != data {
		t.Errorf("expected stored data %q, got %q", data, stored)
	}
}

// TestStorageFile_Read tests reading data from a file
func TestStorageFile_Read(t *testing.T) {
	storage := newMockStorageForFile()
	testData := []byte("hello world")
	_ = storage.Put("test.txt", bytes.NewReader(testData))

	fs := New(storage)

	file, err := newStorageFile(fs, "test.txt", os.O_RDONLY, 0644)
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}

	buf := make([]byte, len(testData))
	n, err := file.Read(buf)
	if err != nil {
		t.Errorf("Read() failed: %v", err)
	}
	if n != len(testData) {
		t.Errorf("expected %d bytes read, got %d", len(testData), n)
	}
	if !bytes.Equal(buf, testData) {
		t.Errorf("expected data %q, got %q", testData, buf)
	}

	// Second read should return EOF
	n, err = file.Read(buf)
	if err != io.EOF {
		t.Errorf("expected io.EOF, got %v", err)
	}
	if n != 0 {
		t.Errorf("expected 0 bytes, got %d", n)
	}

	if err := file.Close(); err != nil {
		t.Errorf("Close() failed: %v", err)
	}
}

// TestStorageFile_ReadAt tests ReadAt method
func TestStorageFile_ReadAt(t *testing.T) {
	storage := newMockStorageForFile()
	testData := []byte("hello world")
	_ = storage.Put("test.txt", bytes.NewReader(testData))

	fs := New(storage)

	file, err := newStorageFile(fs, "test.txt", os.O_RDONLY, 0644)
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	defer file.Close()

	buf := make([]byte, 5)
	n, err := file.ReadAt(buf, 6)
	if err != nil && err != io.EOF {
		t.Errorf("ReadAt() failed: %v", err)
	}
	if n != 5 {
		t.Errorf("expected 5 bytes read, got %d", n)
	}
	if string(buf) != "world" {
		t.Errorf("expected 'world', got %q", buf)
	}
}

// TestStorageFile_Seek tests Seek method
func TestStorageFile_Seek(t *testing.T) {
	storage := newMockStorageForFile()
	testData := []byte("hello world")
	_ = storage.Put("test.txt", bytes.NewReader(testData))

	fs := New(storage)

	file, err := newStorageFile(fs, "test.txt", os.O_RDONLY, 0644)
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	defer file.Close()

	// Seek to position 6
	pos, err := file.Seek(6, io.SeekStart)
	if err != nil {
		t.Errorf("Seek() failed: %v", err)
	}
	if pos != 6 {
		t.Errorf("expected position 6, got %d", pos)
	}

	// Read from position
	buf := make([]byte, 5)
	n, err := file.Read(buf)
	if err != nil {
		t.Errorf("Read() failed: %v", err)
	}
	if n != 5 {
		t.Errorf("expected 5 bytes, got %d", n)
	}
	if string(buf) != "world" {
		t.Errorf("expected 'world', got %q", buf)
	}

	// Seek from current position
	pos, err = file.Seek(-5, io.SeekCurrent)
	if err != nil {
		t.Errorf("Seek() failed: %v", err)
	}
	if pos != 6 {
		t.Errorf("expected position 6, got %d", pos)
	}

	// Seek from end
	pos, err = file.Seek(-5, io.SeekEnd)
	if err != nil {
		t.Errorf("Seek() failed: %v", err)
	}
	if pos != 6 {
		t.Errorf("expected position 6, got %d", pos)
	}
}

// TestStorageFile_WriteAt tests WriteAt method
func TestStorageFile_WriteAt(t *testing.T) {
	storage := newMockStorageForFile()
	fs := New(storage)

	file, err := newStorageFile(fs, "test.txt", os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	// Write initial data
	_, err = file.Write([]byte("hello world"))
	if err != nil {
		t.Errorf("Write() failed: %v", err)
	}

	// WriteAt should replace data at offset
	n, err := file.WriteAt([]byte("WORLD"), 6)
	if err != nil {
		t.Errorf("WriteAt() failed: %v", err)
	}
	if n != 5 {
		t.Errorf("expected 5 bytes written, got %d", n)
	}

	if err := file.Close(); err != nil {
		t.Errorf("Close() failed: %v", err)
	}

	rc, err := storage.Get("test.txt")
	if err != nil {
		t.Fatalf("Get() failed: %v", err)
	}
	stored, _ := io.ReadAll(rc)
	rc.Close()

	expected := "hello WORLD"
	if string(stored) != expected {
		t.Errorf("expected %q, got %q", expected, stored)
	}
}

// TestStorageFile_Append tests append mode
func TestStorageFile_Append(t *testing.T) {
	storage := newMockStorageForFile()
	testData := []byte("hello")
	_ = storage.Put("test.txt", bytes.NewReader(testData))

	fs := New(storage)

	file, err := newStorageFile(fs, "test.txt", os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}

	_, err = file.Write([]byte(" world"))
	if err != nil {
		t.Errorf("Write() failed: %v", err)
	}

	if err := file.Close(); err != nil {
		t.Errorf("Close() failed: %v", err)
	}

	rc, err := storage.Get("test.txt")
	if err != nil {
		t.Fatalf("Get() failed: %v", err)
	}
	stored, _ := io.ReadAll(rc)
	rc.Close()

	expected := "hello world"
	if string(stored) != expected {
		t.Errorf("expected %q, got %q", expected, stored)
	}
}

// TestStorageFile_Truncate tests Truncate method
func TestStorageFile_Truncate(t *testing.T) {
	storage := newMockStorageForFile()
	fs := New(storage)

	file, err := newStorageFile(fs, "test.txt", os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	_, err = file.Write([]byte("hello world"))
	if err != nil {
		t.Errorf("Write() failed: %v", err)
	}

	// Truncate to 5 bytes
	err = file.Truncate(5)
	if err != nil {
		t.Errorf("Truncate() failed: %v", err)
	}

	if err := file.Close(); err != nil {
		t.Errorf("Close() failed: %v", err)
	}

	rc, err := storage.Get("test.txt")
	if err != nil {
		t.Fatalf("Get() failed: %v", err)
	}
	stored, _ := io.ReadAll(rc)
	rc.Close()

	expected := "hello"
	if string(stored) != expected {
		t.Errorf("expected %q, got %q", expected, stored)
	}
}

// TestStorageFile_Sync tests Sync method
func TestStorageFile_Sync(t *testing.T) {
	storage := newMockStorageForFile()
	fs := New(storage)

	file, err := newStorageFile(fs, "test.txt", os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	data := []byte("hello world")
	_, err = file.Write(data)
	if err != nil {
		t.Errorf("Write() failed: %v", err)
	}

	// Sync should flush to storage
	err = file.Sync()
	if err != nil {
		t.Errorf("Sync() failed: %v", err)
	}

	// Data should be in storage after Sync
	if !storage.exists("test.txt") {
		t.Error("data should be in storage after Sync()")
	}

	rc, err := storage.Get("test.txt")
	if err != nil {
		t.Fatalf("Get() failed: %v", err)
	}
	stored, _ := io.ReadAll(rc)
	rc.Close()

	if !bytes.Equal(stored, data) {
		t.Errorf("expected stored data %q, got %q", data, stored)
	}

	if err := file.Close(); err != nil {
		t.Errorf("Close() failed: %v", err)
	}
}

// TestStorageFile_Stat tests Stat method
func TestStorageFile_Stat(t *testing.T) {
	storage := newMockStorageForFile()
	testData := []byte("hello world")
	_ = storage.Put("test.txt", bytes.NewReader(testData))

	fs := New(storage)

	file, err := newStorageFile(fs, "test.txt", os.O_RDONLY, 0644)
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		t.Errorf("Stat() failed: %v", err)
	}
	if info.Name() != "test.txt" {
		t.Errorf("expected name 'test.txt', got '%s'", info.Name())
	}
	if info.Size() != int64(len(testData)) {
		t.Errorf("expected size %d, got %d", len(testData), info.Size())
	}
	if info.IsDir() {
		t.Error("expected file, not directory")
	}
}

// TestStorageFile_ClosedOperations tests operations on closed file
func TestStorageFile_ClosedOperations(t *testing.T) {
	storage := newMockStorageForFile()
	fs := New(storage)

	file, err := newStorageFile(fs, "test.txt", os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	if err := file.Close(); err != nil {
		t.Errorf("Close() failed: %v", err)
	}

	// Operations on closed file should return error
	_, err = file.Write([]byte("test"))
	if !errors.Is(err, os.ErrClosed) {
		t.Errorf("expected os.ErrClosed, got %v", err)
	}

	buf := make([]byte, 10)
	_, err = file.Read(buf)
	if !errors.Is(err, os.ErrClosed) {
		t.Errorf("expected os.ErrClosed, got %v", err)
	}

	_, err = file.Seek(0, io.SeekStart)
	if !errors.Is(err, os.ErrClosed) {
		t.Errorf("expected os.ErrClosed, got %v", err)
	}
}

// TestStorageFile_ReadOnlyWrite tests writing to read-only file
func TestStorageFile_ReadOnlyWrite(t *testing.T) {
	storage := newMockStorageForFile()
	testData := []byte("hello world")
	_ = storage.Put("test.txt", bytes.NewReader(testData))

	fs := New(storage)

	file, err := newStorageFile(fs, "test.txt", os.O_RDONLY, 0644)
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	defer file.Close()

	_, err = file.Write([]byte("test"))
	if !errors.Is(err, os.ErrPermission) {
		t.Errorf("expected os.ErrPermission, got %v", err)
	}
}

// TestStorageFile_WriteOnlyRead tests reading from write-only file
func TestStorageFile_WriteOnlyRead(t *testing.T) {
	storage := newMockStorageForFile()
	fs := New(storage)

	file, err := newStorageFile(fs, "test.txt", os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}
	defer file.Close()

	buf := make([]byte, 10)
	_, err = file.Read(buf)
	if !errors.Is(err, os.ErrPermission) {
		t.Errorf("expected os.ErrPermission, got %v", err)
	}
}

// TestStorageFile_ConcurrentWrites tests concurrent write operations
func TestStorageFile_ConcurrentWrites(t *testing.T) {
	storage := newMockStorageForFile()
	fs := New(storage)

	file, err := newStorageFile(fs, "test.txt", os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	var wg sync.WaitGroup
	numGoroutines := 10
	bytesPerWrite := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			data := []byte(strings.Repeat("a", bytesPerWrite))
			_, err := file.Write(data)
			if err != nil {
				t.Errorf("Write() failed: %v", err)
			}
		}(i)
	}

	wg.Wait()

	if err := file.Close(); err != nil {
		t.Errorf("Close() failed: %v", err)
	}

	rc, err := storage.Get("test.txt")
	if err != nil {
		t.Fatalf("Get() failed: %v", err)
	}
	stored, _ := io.ReadAll(rc)
	rc.Close()

	expectedLen := numGoroutines * bytesPerWrite
	if len(stored) != expectedLen {
		t.Errorf("expected %d bytes, got %d", expectedLen, len(stored))
	}
}

// TestStorageFile_StorageError tests handling storage errors
func TestStorageFile_StorageError(t *testing.T) {
	storage := newMockStorageForFile()
	storage.putErr = errors.New("storage error")

	fs := New(storage)

	file, err := newStorageFile(fs, "test.txt", os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	_, err = file.Write([]byte("test"))
	if err != nil {
		t.Errorf("Write() failed: %v", err)
	}

	// Close should fail due to storage error
	err = file.Close()
	if err == nil {
		t.Error("expected error from Close(), got nil")
	}
}

// TestStorageFile_Readdir tests Readdir method
func TestStorageFile_Readdir(t *testing.T) {
	storage := newMockStorageForFile()
	fs := New(storage)

	// Readdir not supported for files, should return error
	file, err := newStorageFile(fs, "test.txt", os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}
	defer file.Close()

	_, err = file.Readdir(0)
	if err == nil {
		t.Error("expected error from Readdir() on file, got nil")
	}
}

// TestStorageFile_Readdirnames tests Readdirnames method
func TestStorageFile_Readdirnames(t *testing.T) {
	storage := newMockStorageForFile()
	fs := New(storage)

	file, err := newStorageFile(fs, "test.txt", os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}
	defer file.Close()

	_, err = file.Readdirnames(0)
	if err == nil {
		t.Error("expected error from Readdirnames() on file, got nil")
	}
}

// TestStorageFile_ReadWrite tests read-write mode
func TestStorageFile_ReadWrite(t *testing.T) {
	storage := newMockStorageForFile()
	testData := []byte("hello")
	_ = storage.Put("test.txt", bytes.NewReader(testData))

	fs := New(storage)

	file, err := newStorageFile(fs, "test.txt", os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	defer file.Close()

	// Read existing data
	buf := make([]byte, 5)
	n, err := file.Read(buf)
	if err != nil {
		t.Errorf("Read() failed: %v", err)
	}
	if n != 5 || string(buf) != "hello" {
		t.Errorf("expected 'hello', got %q", buf)
	}

	// Write new data
	_, err = file.Write([]byte(" world"))
	if err != nil {
		t.Errorf("Write() failed: %v", err)
	}

	if err := file.Close(); err != nil {
		t.Errorf("Close() failed: %v", err)
	}

	rc, err := storage.Get("test.txt")
	if err != nil {
		t.Fatalf("Get() failed: %v", err)
	}
	stored, _ := io.ReadAll(rc)
	rc.Close()

	expected := "hello world"
	if string(stored) != expected {
		t.Errorf("expected %q, got %q", expected, stored)
	}
}

// TestStorageFile_DoubleClose tests closing a file twice
func TestStorageFile_DoubleClose(t *testing.T) {
	storage := newMockStorageForFile()
	fs := New(storage)

	file, err := newStorageFile(fs, "test.txt", os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("failed to create file: %v", err)
	}

	if err := file.Close(); err != nil {
		t.Errorf("first Close() failed: %v", err)
	}

	// Second close should not panic, may return error
	err = file.Close()
	if err == nil {
		t.Log("second Close() returned nil (acceptable)")
	} else if !errors.Is(err, os.ErrClosed) {
		t.Errorf("expected os.ErrClosed or nil, got %v", err)
	}
}
