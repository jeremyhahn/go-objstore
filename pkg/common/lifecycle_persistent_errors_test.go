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
	"testing"
	"time"
)

// errFS is a sentinel for injection errors in the filesystem mocks below.
var errFS = errors.New("filesystem error")

// openErrFS is a FileSystem whose OpenFile always returns an error.
type openErrFS struct{}

func (o *openErrFS) OpenFile(_ string, _ int, _ os.FileMode) (LifecycleFile, error) {
	return nil, errFS
}
func (o *openErrFS) Remove(_ string) error    { return nil }
func (o *openErrFS) Rename(_, _ string) error { return nil }

// writeErrFile is a LifecycleFile whose Write returns an error.
type writeErrFile struct {
	closed bool
}

func (f *writeErrFile) Read(_ []byte) (int, error)         { return 0, io.EOF }
func (f *writeErrFile) Write(_ []byte) (int, error)        { return 0, errFS }
func (f *writeErrFile) Close() error                       { f.closed = true; return nil }
func (f *writeErrFile) Seek(_ int64, _ int) (int64, error) { return 0, nil }
func (f *writeErrFile) Truncate(_ int64) error             { return nil }
func (f *writeErrFile) Sync() error                        { return nil }

// syncErrFile is a LifecycleFile whose Sync returns an error.
type syncErrFile struct{}

func (f *syncErrFile) Read(_ []byte) (int, error)         { return 0, io.EOF }
func (f *syncErrFile) Write(p []byte) (int, error)        { return len(p), nil }
func (f *syncErrFile) Close() error                       { return nil }
func (f *syncErrFile) Seek(_ int64, _ int) (int64, error) { return 0, nil }
func (f *syncErrFile) Truncate(_ int64) error             { return nil }
func (f *syncErrFile) Sync() error                        { return errFS }

// closeErrFile is a LifecycleFile whose Close returns an error.
type closeErrFile struct{}

func (f *closeErrFile) Read(_ []byte) (int, error)         { return 0, io.EOF }
func (f *closeErrFile) Write(p []byte) (int, error)        { return len(p), nil }
func (f *closeErrFile) Close() error                       { return errFS }
func (f *closeErrFile) Seek(_ int64, _ int) (int64, error) { return 0, nil }
func (f *closeErrFile) Truncate(_ int64) error             { return nil }
func (f *closeErrFile) Sync() error                        { return nil }

// readErrFile is a LifecycleFile whose Read returns an error (for load path).
type readErrFile struct{}

func (f *readErrFile) Read(_ []byte) (int, error)         { return 0, errFS }
func (f *readErrFile) Write(p []byte) (int, error)        { return len(p), nil }
func (f *readErrFile) Close() error                       { return nil }
func (f *readErrFile) Seek(_ int64, _ int) (int64, error) { return 0, nil }
func (f *readErrFile) Truncate(_ int64) error             { return nil }
func (f *readErrFile) Sync() error                        { return nil }

// onceOpenFS opens a real file on the first call and one of the error
// helper files on subsequent calls. It is used to test the save error
// paths (which open a temp file) while keeping load (first open) happy.
type onceOpenFS struct {
	callCount int
	// openErrAfter controls at which call count OpenFile starts erroring.
	openErrAfter int
	// openFile is used for the first openErrAfter calls (happy path).
	openFile func(n int) (LifecycleFile, error)
}

func (o *onceOpenFS) OpenFile(name string, flag int, perm os.FileMode) (LifecycleFile, error) {
	o.callCount++
	return o.openFile(o.callCount)
}
func (o *onceOpenFS) Remove(_ string) error    { return nil }
func (o *onceOpenFS) Rename(_, _ string) error { return nil }

// renameErrFS is a FileSystem that fails only on Rename.
type renameErrFS struct {
	inner FileSystem
}

func (r *renameErrFS) OpenFile(name string, flag int, perm os.FileMode) (LifecycleFile, error) {
	return r.inner.OpenFile(name, flag, perm)
}
func (r *renameErrFS) Remove(name string) error { return r.inner.Remove(name) }
func (r *renameErrFS) Rename(_, _ string) error { return errFS }

// loadBadJSONFS wraps a mockFileSystem so that the load call opens a file
// filled with invalid JSON.
type loadBadJSONFS struct {
	*mockFileSystem
}

func (l *loadBadJSONFS) OpenFile(name string, flag int, perm os.FileMode) (LifecycleFile, error) {
	// For RDONLY (load), return a file with bad JSON content.
	if flag == os.O_RDONLY {
		l.mockFileSystem.setData(name, []byte("{bad json"))
	}
	return l.mockFileSystem.OpenFile(name, flag, perm)
}

// TestNewPersistentLifecycleManager_LoadError verifies that a non-ErrNotExist
// error returned during initial load propagates out of the constructor.
func TestNewPersistentLifecycleManager_LoadError(t *testing.T) {
	fs := &openErrFS{}
	// openErrFS.OpenFile always returns errFS, which is not os.ErrNotExist,
	// so the constructor must return an error.
	_, err := NewPersistentLifecycleManager(fs, "policies.json")
	if !errors.Is(err, errFS) {
		t.Errorf("NewPersistentLifecycleManager() error = %v, want errFS", err)
	}
}

// TestPersistentLifecycleManager_Save_OpenError verifies that save
// propagates an error when OpenFile fails for the temp file.
func TestPersistentLifecycleManager_Save_OpenError(t *testing.T) {
	// First call (load during construction) must succeed (ErrNotExist is ok).
	// Subsequent calls (save) must fail.
	callCount := 0
	fs := &onceOpenFS{
		openFile: func(n int) (LifecycleFile, error) {
			callCount++
			if callCount == 1 {
				// load: file not found is acceptable
				return nil, os.ErrNotExist
			}
			return nil, errFS
		},
	}
	lm, err := NewPersistentLifecycleManager(fs, "policies.json")
	if err != nil {
		t.Fatalf("NewPersistentLifecycleManager() unexpected error: %v", err)
	}

	err = lm.AddPolicy(LifecyclePolicy{
		ID:        "p1",
		Prefix:    "logs/",
		Retention: 24 * time.Hour,
		Action:    "delete",
	})
	if !errors.Is(err, errFS) {
		t.Errorf("AddPolicy() error = %v, want errFS (from save/OpenFile)", err)
	}
}

// TestPersistentLifecycleManager_Save_WriteError verifies that save
// propagates a Write error on the temp file.
func TestPersistentLifecycleManager_Save_WriteError(t *testing.T) {
	callCount := 0
	fs := &onceOpenFS{
		openFile: func(n int) (LifecycleFile, error) {
			callCount++
			if callCount == 1 {
				return nil, os.ErrNotExist
			}
			return &writeErrFile{}, nil
		},
	}
	lm, err := NewPersistentLifecycleManager(fs, "policies.json")
	if err != nil {
		t.Fatalf("NewPersistentLifecycleManager() unexpected error: %v", err)
	}

	err = lm.AddPolicy(LifecyclePolicy{ID: "p1", Action: "delete", Retention: time.Hour})
	if !errors.Is(err, errFS) {
		t.Errorf("AddPolicy() error = %v, want errFS (from save/Write)", err)
	}
}

// TestPersistentLifecycleManager_Save_SyncError verifies that save
// propagates a Sync error on the temp file.
func TestPersistentLifecycleManager_Save_SyncError(t *testing.T) {
	callCount := 0
	fs := &onceOpenFS{
		openFile: func(n int) (LifecycleFile, error) {
			callCount++
			if callCount == 1 {
				return nil, os.ErrNotExist
			}
			return &syncErrFile{}, nil
		},
	}
	lm, err := NewPersistentLifecycleManager(fs, "policies.json")
	if err != nil {
		t.Fatalf("NewPersistentLifecycleManager() unexpected error: %v", err)
	}

	err = lm.AddPolicy(LifecyclePolicy{ID: "p1", Action: "delete", Retention: time.Hour})
	if !errors.Is(err, errFS) {
		t.Errorf("AddPolicy() error = %v, want errFS (from save/Sync)", err)
	}
}

// TestPersistentLifecycleManager_Save_CloseError verifies that save
// propagates a Close error on the temp file.
func TestPersistentLifecycleManager_Save_CloseError(t *testing.T) {
	callCount := 0
	fs := &onceOpenFS{
		openFile: func(n int) (LifecycleFile, error) {
			callCount++
			if callCount == 1 {
				return nil, os.ErrNotExist
			}
			return &closeErrFile{}, nil
		},
	}
	lm, err := NewPersistentLifecycleManager(fs, "policies.json")
	if err != nil {
		t.Fatalf("NewPersistentLifecycleManager() unexpected error: %v", err)
	}

	err = lm.AddPolicy(LifecyclePolicy{ID: "p1", Action: "delete", Retention: time.Hour})
	if !errors.Is(err, errFS) {
		t.Errorf("AddPolicy() error = %v, want errFS (from save/Close)", err)
	}
}

// TestPersistentLifecycleManager_Save_RenameError verifies that save
// propagates a Rename error after the temp file was written successfully.
func TestPersistentLifecycleManager_Save_RenameError(t *testing.T) {
	inner := newMockFileSystem()
	fs := &renameErrFS{inner: inner}

	lm, err := NewPersistentLifecycleManager(fs, "policies.json")
	if err != nil {
		t.Fatalf("NewPersistentLifecycleManager() unexpected error: %v", err)
	}

	err = lm.AddPolicy(LifecyclePolicy{ID: "p1", Action: "delete", Retention: time.Hour})
	if !errors.Is(err, errFS) {
		t.Errorf("AddPolicy() error = %v, want errFS (from save/Rename)", err)
	}
}

// TestPersistentLifecycleManager_Load_ReadError verifies that load
// propagates a Read error so NewPersistentLifecycleManager returns it.
func TestPersistentLifecycleManager_Load_ReadError(t *testing.T) {
	callCount := 0
	fs := &onceOpenFS{
		openFile: func(n int) (LifecycleFile, error) {
			callCount++
			// First call is load; return a file that errors on Read.
			return &readErrFile{}, nil
		},
	}
	_, err := NewPersistentLifecycleManager(fs, "policies.json")
	if !errors.Is(err, errFS) {
		t.Errorf("NewPersistentLifecycleManager() error = %v, want errFS", err)
	}
}

// TestPersistentLifecycleManager_Load_BadJSON verifies that a corrupt policy
// file (invalid JSON) causes NewPersistentLifecycleManager to return a
// non-nil error.
func TestPersistentLifecycleManager_Load_BadJSON(t *testing.T) {
	inner := newMockFileSystem()
	fs := &loadBadJSONFS{mockFileSystem: inner}

	// Prime the file so OpenFile(RDONLY) finds it.
	inner.setData("policies.json", []byte("{not valid json"))

	_, err := NewPersistentLifecycleManager(fs, "policies.json")
	if err == nil {
		t.Error("NewPersistentLifecycleManager() expected error for bad JSON, got nil")
	}
}

// TestFileSystemAdapter_Rename verifies that Rename is correctly delegated
// through a NewFileSystemAdapter.
func TestFileSystemAdapter_Rename(t *testing.T) {
	inner := newMockFileSystem()

	adaptable := &mockAdaptableFS{mockFileSystem: inner}
	adapter := NewFileSystemAdapter(adaptable)

	// Create src so Rename has something to move.
	f, err := inner.OpenFile("src.txt", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		t.Fatalf("OpenFile(src) failed: %v", err)
	}
	if _, err := f.Write([]byte("content")); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	_ = f.Close()

	if err := adapter.Rename("src.txt", "dst.txt"); err != nil {
		t.Fatalf("Rename() failed: %v", err)
	}

	// dst must now exist.
	if _, ok := inner.files["dst.txt"]; !ok {
		t.Error("dst.txt not found after Rename")
	}
	if _, ok := inner.files["src.txt"]; ok {
		t.Error("src.txt still exists after Rename")
	}
}

// mockAdaptableFSBadFile is a mockAdaptableFS whose OpenFile returns an
// any that does NOT implement LifecycleFile. NewFileSystemAdapter must
// fall back to the fileAdapter wrapper.
type mockAdaptableFSBadFile struct {
	inner *mockFileSystem
}

// stubNonLifecycleFile implements only io.ReadWriteCloser — not the full
// LifecycleFile interface (missing Seek/Truncate/Sync).
type stubNonLifecycleFile struct{}

func (s *stubNonLifecycleFile) Read(_ []byte) (int, error)  { return 0, io.EOF }
func (s *stubNonLifecycleFile) Write(p []byte) (int, error) { return len(p), nil }
func (s *stubNonLifecycleFile) Close() error                { return nil }

func (m *mockAdaptableFSBadFile) OpenFile(_ string, _ int, _ os.FileMode) (any, error) {
	return &stubNonLifecycleFile{}, nil
}
func (m *mockAdaptableFSBadFile) Remove(name string) error     { return m.inner.Remove(name) }
func (m *mockAdaptableFSBadFile) Rename(src, dst string) error { return m.inner.Rename(src, dst) }

// TestNewFileSystemAdapter_NonLifecycleFileFallback verifies that when the
// adaptable filesystem returns an object that does NOT satisfy LifecycleFile,
// NewFileSystemAdapter wraps it in a fileAdapter instead of panicking or
// returning nil.
func TestNewFileSystemAdapter_NonLifecycleFileFallback(t *testing.T) {
	inner := newMockFileSystem()
	adaptable := &mockAdaptableFSBadFile{inner: inner}
	adapter := NewFileSystemAdapter(adaptable)

	file, err := adapter.OpenFile("any.txt", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		t.Fatalf("OpenFile() failed: %v", err)
	}
	if file == nil {
		t.Fatal("OpenFile() returned nil file")
	}
	// The wrapped file should support Write and Read via fileAdapter methods.
	n, err := file.Write([]byte("hello"))
	if err != nil {
		t.Errorf("Write() failed: %v", err)
	}
	if n != 5 {
		t.Errorf("Write() n = %d, want 5", n)
	}
}

// mockAdaptableFSOpenErr is an adaptable FS whose OpenFile always errors.
// This exercises the error-return branch inside NewFileSystemAdapter's
// openFile closure.
type mockAdaptableFSOpenErr struct {
	inner *mockFileSystem
}

func (m *mockAdaptableFSOpenErr) OpenFile(_ string, _ int, _ os.FileMode) (any, error) {
	return nil, errFS
}
func (m *mockAdaptableFSOpenErr) Remove(name string) error     { return m.inner.Remove(name) }
func (m *mockAdaptableFSOpenErr) Rename(src, dst string) error { return m.inner.Rename(src, dst) }

// TestNewFileSystemAdapter_OpenFileError verifies that an error returned by
// the underlying adaptable filesystem's OpenFile is propagated through the
// adapter's openFile closure.
func TestNewFileSystemAdapter_OpenFileError(t *testing.T) {
	inner := newMockFileSystem()
	adaptable := &mockAdaptableFSOpenErr{inner: inner}
	adapter := NewFileSystemAdapter(adaptable)

	_, err := adapter.OpenFile("any.txt", os.O_RDONLY, 0)
	if !errors.Is(err, errFS) {
		t.Errorf("OpenFile() error = %v, want errFS", err)
	}
}
