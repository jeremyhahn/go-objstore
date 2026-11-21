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

package common

import (
	"errors"
	"io"
	"os"
	"sync"
	"testing"
	"time"
)

// mockFile is a mock implementation of LifecycleFile for testing
type mockFile struct {
	fs     *mockFileSystem
	name   string
	offset int
	closed bool
	mutex  sync.Mutex
}

func (f *mockFile) Read(p []byte) (n int, err error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	if f.closed {
		return 0, os.ErrClosed
	}

	data := f.fs.getData(f.name)
	if f.offset >= len(data) {
		return 0, io.EOF
	}

	n = copy(p, data[f.offset:])
	f.offset += n
	return n, nil
}

func (f *mockFile) Write(p []byte) (n int, err error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	if f.closed {
		return 0, os.ErrClosed
	}

	data := f.fs.getData(f.name)

	// Expand data if needed
	newLen := f.offset + len(p)
	if newLen > len(data) {
		newData := make([]byte, newLen)
		copy(newData, data)
		data = newData
	}

	n = copy(data[f.offset:], p)
	f.offset += n

	f.fs.setData(f.name, data)
	return n, nil
}

func (f *mockFile) Close() error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	f.closed = true
	return nil
}

func (f *mockFile) Seek(offset int64, whence int) (int64, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	if f.closed {
		return 0, os.ErrClosed
	}

	data := f.fs.getData(f.name)

	switch whence {
	case io.SeekStart:
		f.offset = int(offset)
	case io.SeekCurrent:
		f.offset += int(offset)
	case io.SeekEnd:
		f.offset = len(data) + int(offset)
	}

	return int64(f.offset), nil
}

func (f *mockFile) Truncate(size int64) error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	if f.closed {
		return os.ErrClosed
	}

	data := f.fs.getData(f.name)

	if int(size) < len(data) {
		data = data[:size]
	} else {
		newData := make([]byte, size)
		copy(newData, data)
		data = newData
	}

	f.fs.setData(f.name, data)
	return nil
}

func (f *mockFile) Sync() error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	if f.closed {
		return os.ErrClosed
	}

	return nil
}

// mockFileSystem is a mock implementation of FileSystem for testing
type mockFileSystem struct {
	files map[string][]byte
	mutex sync.RWMutex
}

func newMockFileSystem() *mockFileSystem {
	return &mockFileSystem{
		files: make(map[string][]byte),
	}
}

func (m *mockFileSystem) getData(name string) []byte {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.files[name]
}

func (m *mockFileSystem) setData(name string, data []byte) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.files[name] = data
}

func (m *mockFileSystem) OpenFile(name string, flag int, perm os.FileMode) (LifecycleFile, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// Check if file exists
	_, exists := m.files[name]

	// Handle O_CREATE flag
	if flag&os.O_CREATE != 0 {
		if !exists {
			m.files[name] = make([]byte, 0)
		}
	} else if !exists {
		return nil, os.ErrNotExist
	}

	// Handle O_TRUNC flag
	if flag&os.O_TRUNC != 0 {
		m.files[name] = make([]byte, 0)
	}

	// Create a new file handle
	file := &mockFile{
		fs:     m,
		name:   name,
		offset: 0,
		closed: false,
	}

	return file, nil
}

func (m *mockFileSystem) Remove(name string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if _, exists := m.files[name]; !exists {
		return os.ErrNotExist
	}

	delete(m.files, name)
	return nil
}

func (m *mockFileSystem) Rename(src, dst string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	data, exists := m.files[src]
	if !exists {
		return os.ErrNotExist
	}

	m.files[dst] = data
	delete(m.files, src)
	return nil
}

// TestNewPersistentLifecycleManager tests creating a new persistent lifecycle manager
func TestNewPersistentLifecycleManager(t *testing.T) {
	fs := newMockFileSystem()

	// Test with default policy file
	lm, err := NewPersistentLifecycleManager(fs, "")
	if err != nil {
		t.Fatalf("Failed to create persistent lifecycle manager: %v", err)
	}
	if lm == nil {
		t.Fatal("Expected non-nil lifecycle manager")
	}
	if lm.policyFile != ".lifecycle-policies.json" {
		t.Errorf("Expected default policy file '.lifecycle-policies.json', got '%s'", lm.policyFile)
	}

	// Test with custom policy file
	lm, err = NewPersistentLifecycleManager(fs, "custom-policies.json")
	if err != nil {
		t.Fatalf("Failed to create persistent lifecycle manager: %v", err)
	}
	if lm.policyFile != "custom-policies.json" {
		t.Errorf("Expected policy file 'custom-policies.json', got '%s'", lm.policyFile)
	}

	// Test with nil filesystem
	_, err = NewPersistentLifecycleManager(nil, "")
	if err == nil {
		t.Error("Expected error when creating manager with nil filesystem")
	}
}

// TestPersistentLifecycleManager_AddPolicy tests adding a policy
func TestPersistentLifecycleManager_AddPolicy(t *testing.T) {
	fs := newMockFileSystem()
	lm, err := NewPersistentLifecycleManager(fs, "test-policies.json")
	if err != nil {
		t.Fatalf("Failed to create persistent lifecycle manager: %v", err)
	}

	policy := LifecyclePolicy{
		ID:        "test-policy",
		Prefix:    "logs/",
		Retention: 24 * time.Hour,
		Action:    "delete",
	}

	err = lm.AddPolicy(policy)
	if err != nil {
		t.Fatalf("Failed to add policy: %v", err)
	}

	// Verify policy was added
	policies, err := lm.GetPolicies()
	if err != nil {
		t.Fatalf("Failed to get policies: %v", err)
	}
	if len(policies) != 1 {
		t.Errorf("Expected 1 policy, got %d", len(policies))
	}
	if policies[0].ID != "test-policy" {
		t.Errorf("Expected policy ID 'test-policy', got '%s'", policies[0].ID)
	}

	// Test adding policy with empty ID
	invalidPolicy := LifecyclePolicy{
		Prefix:    "data/",
		Retention: 48 * time.Hour,
		Action:    "delete",
	}
	err = lm.AddPolicy(invalidPolicy)
	if err == nil {
		t.Error("Expected error when adding policy with empty ID")
	}
	if !errors.Is(err, ErrInvalidPolicy) {
		t.Errorf("Expected ErrInvalidPolicy, got %v", err)
	}
}

// TestPersistentLifecycleManager_RemovePolicy tests removing a policy
func TestPersistentLifecycleManager_RemovePolicy(t *testing.T) {
	fs := newMockFileSystem()
	lm, err := NewPersistentLifecycleManager(fs, "test-policies.json")
	if err != nil {
		t.Fatalf("Failed to create persistent lifecycle manager: %v", err)
	}

	policy := LifecyclePolicy{
		ID:        "test-policy",
		Prefix:    "logs/",
		Retention: 24 * time.Hour,
		Action:    "delete",
	}

	err = lm.AddPolicy(policy)
	if err != nil {
		t.Fatalf("Failed to add policy: %v", err)
	}

	err = lm.RemovePolicy("test-policy")
	if err != nil {
		t.Fatalf("Failed to remove policy: %v", err)
	}

	// Verify policy was removed
	policies, err := lm.GetPolicies()
	if err != nil {
		t.Fatalf("Failed to get policies: %v", err)
	}
	if len(policies) != 0 {
		t.Errorf("Expected 0 policies, got %d", len(policies))
	}

	// Test removing non-existent policy (should not error)
	err = lm.RemovePolicy("non-existent")
	if err != nil {
		t.Errorf("Unexpected error when removing non-existent policy: %v", err)
	}
}

// TestPersistentLifecycleManager_Persistence tests that policies survive restart
func TestPersistentLifecycleManager_Persistence(t *testing.T) {
	fs := newMockFileSystem()

	// Create first manager and add policies
	lm1, err := NewPersistentLifecycleManager(fs, "test-policies.json")
	if err != nil {
		t.Fatalf("Failed to create first lifecycle manager: %v", err)
	}

	policy1 := LifecyclePolicy{
		ID:        "policy-1",
		Prefix:    "logs/",
		Retention: 24 * time.Hour,
		Action:    "delete",
	}
	policy2 := LifecyclePolicy{
		ID:        "policy-2",
		Prefix:    "temp/",
		Retention: 48 * time.Hour,
		Action:    "delete",
	}

	if err := lm1.AddPolicy(policy1); err != nil {
		t.Fatalf("Failed to add policy 1: %v", err)
	}
	if err := lm1.AddPolicy(policy2); err != nil {
		t.Fatalf("Failed to add policy 2: %v", err)
	}

	// Create second manager (simulating process restart)
	lm2, err := NewPersistentLifecycleManager(fs, "test-policies.json")
	if err != nil {
		t.Fatalf("Failed to create second lifecycle manager: %v", err)
	}

	// Verify policies were loaded
	policies, err := lm2.GetPolicies()
	if err != nil {
		t.Fatalf("Failed to get policies from second manager: %v", err)
	}
	if len(policies) != 2 {
		t.Errorf("Expected 2 policies, got %d", len(policies))
	}

	// Verify policy contents
	foundPolicy1 := false
	foundPolicy2 := false
	for _, p := range policies {
		if p.ID == "policy-1" {
			foundPolicy1 = true
			if p.Prefix != "logs/" || p.Action != "delete" {
				t.Error("Policy 1 contents mismatch")
			}
		}
		if p.ID == "policy-2" {
			foundPolicy2 = true
			if p.Prefix != "temp/" || p.Action != "delete" {
				t.Error("Policy 2 contents mismatch")
			}
		}
	}

	if !foundPolicy1 {
		t.Error("Policy 1 not found after restart")
	}
	if !foundPolicy2 {
		t.Error("Policy 2 not found after restart")
	}
}

// TestPersistentLifecycleManager_ConcurrentAccess tests concurrent policy operations
func TestPersistentLifecycleManager_ConcurrentAccess(t *testing.T) {
	fs := newMockFileSystem()
	lm, err := NewPersistentLifecycleManager(fs, "test-policies.json")
	if err != nil {
		t.Fatalf("Failed to create persistent lifecycle manager: %v", err)
	}

	// Add policies concurrently
	var wg sync.WaitGroup
	numGoroutines := 10

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			policy := LifecyclePolicy{
				ID:        string(rune('a' + id)),
				Prefix:    "test/",
				Retention: 24 * time.Hour,
				Action:    "delete",
			}
			_ = lm.AddPolicy(policy)
		}(i)
	}

	wg.Wait()

	// Verify policies
	policies, err := lm.GetPolicies()
	if err != nil {
		t.Fatalf("Failed to get policies: %v", err)
	}
	if len(policies) != numGoroutines {
		t.Errorf("Expected %d policies, got %d", numGoroutines, len(policies))
	}
}

// TestPersistentLifecycleManager_ArchivePolicy tests that archive policies are handled correctly
func TestPersistentLifecycleManager_ArchivePolicy(t *testing.T) {
	fs := newMockFileSystem()
	lm, err := NewPersistentLifecycleManager(fs, "test-policies.json")
	if err != nil {
		t.Fatalf("Failed to create persistent lifecycle manager: %v", err)
	}

	// Note: Archiver interface cannot be serialized, so we don't set Destination
	// The comment in the code notes this limitation
	policy := LifecyclePolicy{
		ID:        "archive-policy",
		Prefix:    "old/",
		Retention: 90 * 24 * time.Hour,
		Action:    "archive",
		// Destination would need to be set after loading from storage
	}

	err = lm.AddPolicy(policy)
	if err != nil {
		t.Fatalf("Failed to add archive policy: %v", err)
	}

	// Create new manager to test persistence
	lm2, err := NewPersistentLifecycleManager(fs, "test-policies.json")
	if err != nil {
		t.Fatalf("Failed to create second lifecycle manager: %v", err)
	}

	policies, err := lm2.GetPolicies()
	if err != nil {
		t.Fatalf("Failed to get policies: %v", err)
	}

	if len(policies) != 1 {
		t.Fatalf("Expected 1 policy, got %d", len(policies))
	}

	if policies[0].Action != "archive" {
		t.Errorf("Expected action 'archive', got '%s'", policies[0].Action)
	}

	// Destination should be nil since it's not serializable
	if policies[0].Destination != nil {
		t.Error("Expected Destination to be nil after deserialization")
	}
}

// mockAdaptableFS is a mock filesystem that returns any instead of LifecycleFile
type mockAdaptableFS struct {
	*mockFileSystem
}

func (m *mockAdaptableFS) OpenFile(name string, flag int, perm os.FileMode) (any, error) {
	return m.mockFileSystem.OpenFile(name, flag, perm)
}

func (m *mockAdaptableFS) Remove(name string) error {
	return m.mockFileSystem.Remove(name)
}

// TestNewFileSystemAdapter tests creating a filesystem adapter
func TestNewFileSystemAdapter(t *testing.T) {
	mockFS := &mockAdaptableFS{mockFileSystem: newMockFileSystem()}
	adapter := NewFileSystemAdapter(mockFS)

	if adapter == nil {
		t.Fatal("Expected non-nil adapter")
	}

	// Test OpenFile through adapter
	file, err := adapter.OpenFile("test.txt", os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("Failed to open file through adapter: %v", err)
	}
	if file == nil {
		t.Fatal("Expected non-nil file")
	}
	defer func() { _ = file.Close() }()

	// Test Write through adapter
	data := []byte("test data")
	n, err := file.Write(data)
	if err != nil {
		t.Fatalf("Failed to write through adapter: %v", err)
	}
	if n != len(data) {
		t.Errorf("Expected to write %d bytes, wrote %d", len(data), n)
	}

	// Test Remove through adapter
	err = adapter.Remove("test.txt")
	if err != nil {
		t.Fatalf("Failed to remove file through adapter: %v", err)
	}
}

// TestFileAdapter tests the fileAdapter wrapper
func TestFileAdapter(t *testing.T) {
	mockFS := newMockFileSystem()

	// Open a file to get an any to wrap
	file, err := mockFS.OpenFile("test.txt", os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Create adapter with the file
	adapter := &fileAdapter{file: file}

	// Test Write
	testData := []byte("Hello, World!")
	n, err := adapter.Write(testData)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if n != len(testData) {
		t.Errorf("Expected to write %d bytes, wrote %d", len(testData), n)
	}

	// Test Seek
	offset, err := adapter.Seek(0, io.SeekStart)
	if err != nil {
		t.Fatalf("Seek failed: %v", err)
	}
	if offset != 0 {
		t.Errorf("Expected offset 0, got %d", offset)
	}

	// Test Read
	readBuf := make([]byte, len(testData))
	n, err = adapter.Read(readBuf)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if n != len(testData) {
		t.Errorf("Expected to read %d bytes, read %d", len(testData), n)
	}
	if string(readBuf) != string(testData) {
		t.Errorf("Expected to read %q, got %q", testData, readBuf)
	}

	// Test Truncate
	err = adapter.Truncate(5)
	if err != nil {
		t.Fatalf("Truncate failed: %v", err)
	}

	// Test Sync
	err = adapter.Sync()
	if err != nil {
		t.Fatalf("Sync failed: %v", err)
	}

	// Test Close
	err = adapter.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

// TestFileAdapter_Errors tests error paths in fileAdapter
func TestFileAdapter_Errors(t *testing.T) {
	// Test with an object that doesn't support any operations
	unsupportedFile := struct{}{}
	adapter := &fileAdapter{file: unsupportedFile}

	// Test Read error
	buf := make([]byte, 10)
	_, err := adapter.Read(buf)
	if err == nil {
		t.Error("Expected error for unsupported Read")
	}

	// Test Write error
	_, err = adapter.Write([]byte("test"))
	if err == nil {
		t.Error("Expected error for unsupported Write")
	}

	// Test Close error
	err = adapter.Close()
	if err == nil {
		t.Error("Expected error for unsupported Close")
	}

	// Test Seek error
	_, err = adapter.Seek(0, io.SeekStart)
	if err == nil {
		t.Error("Expected error for unsupported Seek")
	}

	// Test Truncate error
	err = adapter.Truncate(0)
	if err == nil {
		t.Error("Expected error for unsupported Truncate")
	}

	// Test Sync error
	err = adapter.Sync()
	if err == nil {
		t.Error("Expected error for unsupported Sync")
	}
}

// storeFS is an in-memory FileSystem whose files buffer their writes and
// store them on Sync and on Close, as a StorageFS does. failWrite and
// failClose make the next that many writes, or closes, of a file opened for
// writing fail: a failing Write keeps half its bytes, a failing Close stores
// the buffer first (as a StorageFS whose metadata write fails after the Put).
type storeFS struct {
	mu        sync.Mutex
	files     map[string][]byte
	stores    map[string]int
	inPlace   bool
	failWrite int
	failClose int
}

func newStoreFS(inPlace bool) *storeFS {
	return &storeFS{files: map[string][]byte{}, stores: map[string]int{}, inPlace: inPlace}
}

func (s *storeFS) ReplacesInPlace() bool { return s.inPlace }

func (s *storeFS) OpenFile(name string, flag int, _ os.FileMode) (LifecycleFile, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if flag&(os.O_WRONLY|os.O_RDWR) == 0 {
		data, ok := s.files[name]
		if !ok {
			return nil, os.ErrNotExist
		}
		return &storeFile{fs: s, name: name, data: append([]byte(nil), data...)}, nil
	}
	f := &storeFile{fs: s, name: name, writable: true}
	if flag&os.O_TRUNC == 0 {
		f.data = append([]byte(nil), s.files[name]...)
	}
	return f, nil
}

func (s *storeFS) Remove(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.files[name]; !ok {
		return os.ErrNotExist
	}
	delete(s.files, name)
	return nil
}

func (s *storeFS) Rename(src, dst string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, ok := s.files[src]
	if !ok {
		return os.ErrNotExist
	}
	s.files[dst] = data
	delete(s.files, src)
	return nil
}

func (s *storeFS) stored(name string) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, ok := s.files[name]
	return data, ok
}

type storeFile struct {
	fs       *storeFS
	name     string
	data     []byte
	off      int
	writable bool
}

func (f *storeFile) Read(p []byte) (int, error) {
	if f.off >= len(f.data) {
		return 0, io.EOF
	}
	n := copy(p, f.data[f.off:])
	f.off += n
	return n, nil
}

func (f *storeFile) Write(p []byte) (int, error) {
	f.fs.mu.Lock()
	fail := f.fs.failWrite > 0
	if fail {
		f.fs.failWrite--
	}
	f.fs.mu.Unlock()
	if fail {
		f.data = append(f.data, p[:len(p)/2]...)
		return len(p) / 2, errFS
	}
	f.data = append(f.data, p...)
	return len(p), nil
}

func (f *storeFile) store() {
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()
	f.fs.files[f.name] = append([]byte(nil), f.data...)
	f.fs.stores[f.name]++
}

func (f *storeFile) Sync() error {
	if f.writable {
		f.store()
	}
	return nil
}

func (f *storeFile) Close() error {
	if !f.writable {
		return nil
	}
	f.store()
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()
	if f.fs.failClose > 0 {
		f.fs.failClose--
		return errFS
	}
	return nil
}

func (f *storeFile) Seek(int64, int) (int64, error) { return 0, ErrNoSeek }
func (f *storeFile) Truncate(int64) error           { return ErrNoTruncate }

// TestPersistentLifecycleManager_FailedSaveKeepsPreviousPolicies: a save
// whose Write or Close fails returns the error and leaves the stored policy
// document as it was, whether the policy file is staged and renamed or (on an
// InPlaceFileSystem) written in place, and a successful in-place save stores
// the policy file once.
func TestPersistentLifecycleManager_FailedSaveKeepsPreviousPolicies(t *testing.T) {
	const policyFile = "policies.json"
	for _, inPlace := range []bool{false, true} {
		for _, failure := range []string{"write", "close"} {
			name := failure
			if inPlace {
				name = "in-place/" + name
			} else {
				name = "staged/" + name
			}
			t.Run(name, func(t *testing.T) {
				fsys := newStoreFS(inPlace)
				arm := func() {
					fsys.mu.Lock()
					defer fsys.mu.Unlock()
					if failure == "write" {
						fsys.failWrite = 1
					} else {
						fsys.failClose = 1
					}
				}
				lm, err := NewPersistentLifecycleManager(fsys, policyFile)
				if err != nil {
					t.Fatal(err)
				}

				// A first save that fails leaves no policy file.
				arm()
				if err := lm.AddPolicy(LifecyclePolicy{ID: "first", Action: "delete", Retention: time.Hour}); !errors.Is(err, errFS) {
					t.Fatalf("failing first AddPolicy() error = %v, want errFS", err)
				}
				if data, ok := fsys.stored(policyFile); ok {
					t.Fatalf("policy file after a failed first save = %q, want none", data)
				}

				fsys.stores = map[string]int{}
				if err := lm.AddPolicy(LifecyclePolicy{ID: "keep", Action: "delete", Retention: time.Hour}); err != nil {
					t.Fatal(err)
				}
				if inPlace && fsys.stores[policyFile] != 1 {
					t.Errorf("in-place save stored the policy file %d times, want 1", fsys.stores[policyFile])
				}
				before, ok := fsys.stored(policyFile)
				if !ok {
					t.Fatal("no policy file after a successful save")
				}

				arm()
				if err := lm.AddPolicy(LifecyclePolicy{ID: "rejected", Action: "delete", Retention: time.Hour}); !errors.Is(err, errFS) {
					t.Fatalf("failing AddPolicy() error = %v, want errFS", err)
				}
				if after, _ := fsys.stored(policyFile); string(after) != string(before) {
					t.Errorf("policy file after a failed save = %q, want the previous %q", after, before)
				}
				for name := range fsys.files {
					if name != policyFile {
						t.Errorf("failed save left %q behind", name)
					}
				}

				reloaded, err := NewPersistentLifecycleManager(fsys, policyFile)
				if err != nil {
					t.Fatal(err)
				}
				policies, err := reloaded.GetPolicies()
				if err != nil {
					t.Fatal(err)
				}
				if len(policies) != 1 || policies[0].ID != "keep" {
					t.Errorf("policies after reload = %+v, want only keep", policies)
				}
			})
		}
	}
}

// TestNewFileSystemAdapter_ForwardsReplacesInPlace: the adapter keeps the
// wrapped filesystem's in-place declaration, and makes none of its own.
func TestNewFileSystemAdapter_ForwardsReplacesInPlace(t *testing.T) {
	inPlace := NewFileSystemAdapter(inPlaceAdaptableFS{&mockAdaptableFS{mockFileSystem: newMockFileSystem()}})
	if ifs, ok := inPlace.(InPlaceFileSystem); !ok || !ifs.ReplacesInPlace() {
		t.Error("adapter over an in-place filesystem does not declare ReplacesInPlace")
	}
	plain := NewFileSystemAdapter(&mockAdaptableFS{mockFileSystem: newMockFileSystem()})
	if ifs, ok := plain.(InPlaceFileSystem); ok && ifs.ReplacesInPlace() {
		t.Error("adapter over a plain filesystem declares ReplacesInPlace")
	}
}

type inPlaceAdaptableFS struct{ *mockAdaptableFS }

func (inPlaceAdaptableFS) ReplacesInPlace() bool { return true }
