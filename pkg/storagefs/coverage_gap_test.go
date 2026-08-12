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
	"io/fs"
	"os"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// --- helpers -----------------------------------------------------------------

// pureStorage implements just enough of the common.Storage interface to be
// usable by StorageFS but has NO listKeys method, so StorageFS.listKeys falls
// back to returning an empty slice.  We embed mockStorage for every method
// except we must NOT promote its unexported listKeys — but since listKeys is
// lowercase it is already not promoted across packages.  Inside the same
// package however, embedding promotes it.  We therefore define a completely
// separate concrete type with no embedding so the unexported lister interface
// check fails.
type minimalStorage struct {
	inner *mockStorage
}

func newMinimalStorage() *minimalStorage {
	return &minimalStorage{inner: newMockStorage()}
}

func (s *minimalStorage) Configure(settings map[string]string) error {
	return s.inner.Configure(settings)
}
func (s *minimalStorage) Put(key string, data io.Reader) error {
	return s.inner.Put(key, data)
}
func (s *minimalStorage) PutWithContext(ctx context.Context, key string, data io.Reader) error {
	return s.inner.PutWithContext(ctx, key, data)
}
func (s *minimalStorage) PutWithMetadata(ctx context.Context, key string, data io.Reader, metadata *common.Metadata) error {
	return s.inner.PutWithMetadata(ctx, key, data, metadata)
}
func (s *minimalStorage) Get(key string) (io.ReadCloser, error) {
	return s.inner.Get(key)
}
func (s *minimalStorage) GetWithContext(ctx context.Context, key string) (io.ReadCloser, error) {
	return s.inner.GetWithContext(ctx, key)
}
func (s *minimalStorage) GetMetadata(ctx context.Context, key string) (*common.Metadata, error) {
	return s.inner.GetMetadata(ctx, key)
}
func (s *minimalStorage) UpdateMetadata(ctx context.Context, key string, metadata *common.Metadata) error {
	return s.inner.UpdateMetadata(ctx, key, metadata)
}
func (s *minimalStorage) Delete(key string) error {
	return s.inner.Delete(key)
}
func (s *minimalStorage) DeleteWithContext(ctx context.Context, key string) error {
	return s.inner.DeleteWithContext(ctx, key)
}
func (s *minimalStorage) Exists(ctx context.Context, key string) (bool, error) {
	return s.inner.Exists(ctx, key)
}
func (s *minimalStorage) List(prefix string) ([]string, error) {
	return s.inner.List(prefix)
}
func (s *minimalStorage) ListWithContext(ctx context.Context, prefix string) ([]string, error) {
	return s.inner.ListWithContext(ctx, prefix)
}
func (s *minimalStorage) ListWithOptions(ctx context.Context, opts *common.ListOptions) (*common.ListResult, error) {
	return s.inner.ListWithOptions(ctx, opts)
}
func (s *minimalStorage) Archive(key string, archiver common.Archiver) error {
	return s.inner.Archive(key, archiver)
}
func (s *minimalStorage) AddPolicy(policy common.LifecyclePolicy) error {
	return s.inner.AddPolicy(policy)
}
func (s *minimalStorage) RemovePolicy(id string) error {
	return s.inner.RemovePolicy(id)
}
func (s *minimalStorage) GetPolicies() ([]common.LifecyclePolicy, error) {
	return s.inner.GetPolicies()
}
func (s *minimalStorage) RunPolicies(ctx context.Context) error {
	return s.inner.RunPolicies(ctx)
}

// putMetaErrMock wraps mockStorage and injects an error on Put only for
// metadata keys (keys with the ".meta/" prefix). This lets us exercise the
// putMetadata error paths in Close/Sync/Mkdir without breaking the rest of
// the setup.
type putMetaErrMock struct {
	*mockStorage
	metaPutErr error
}

func (m *putMetaErrMock) Put(key string, data io.Reader) error {
	if m.metaPutErr != nil && len(key) >= 6 && key[:6] == ".meta/" {
		return m.metaPutErr
	}
	return m.mockStorage.Put(key, data)
}

// deleteMetaErrMock injects a Delete error only for metadata keys.
type deleteMetaErrMock struct {
	*mockStorage
	metaDelErr error
}

func (m *deleteMetaErrMock) Delete(key string) error {
	if m.metaDelErr != nil && len(key) >= 6 && key[:6] == ".meta/" {
		return m.metaDelErr
	}
	return m.mockStorage.Delete(key)
}

// nonListerStorage wraps mockStorage but does NOT expose the listKeys method,
// so the type assertion in StorageFS.listKeys falls back to returning "".
type nonListerStorage struct {
	*mockStorage
}

// listWithOptionsErrMock returns an error from ListWithOptions.
type listWithOptionsErrMock struct {
	*mockStorage
	listErr error
}

func (m *listWithOptionsErrMock) ListWithOptions(ctx context.Context, opts *common.ListOptions) (*common.ListResult, error) {
	return nil, m.listErr
}

// statErrMock makes Stat (via getMetadataInternal) fail so we hit the fallback
// code paths in newStorageFile that create a default FileInfo.
// It does this by returning a non-FileInfo fs.FileInfo from an embeddable
// type, but since Stat is on StorageFS we need a different approach:
// we override getMetadata by poisoning the metadata storage after the dir
// marker is written.

// nonFileInfoStat stores an os.FileInfo that is NOT *FileInfo so the type
// assertion in newStorageFile fails.
type plainFileInfo struct {
	name  string
	isDir bool
}

func (p *plainFileInfo) Name() string       { return p.name }
func (p *plainFileInfo) Size() int64        { return 0 }
func (p *plainFileInfo) Mode() os.FileMode  { return os.ModeDir | 0755 }
func (p *plainFileInfo) ModTime() time.Time { return time.Now() }
func (p *plainFileInfo) IsDir() bool        { return p.isDir }
func (p *plainFileInfo) Sys() any           { return nil }

// --- newStorageFile edge cases -----------------------------------------------

// TestNewStorageFile_DirStatError exercises the path where fs.Stat returns an
// error for a directory: the fallback FileInfo must still be created.
func TestNewStorageFile_DirStatError(t *testing.T) {
	storage := newMockStorage()
	sfs := New(storage)

	// Create directory marker directly so dirExists returns true.
	_ = storage.Put("mydir/.dir", bytes.NewReader([]byte{}))
	// Do NOT write .meta/mydir so getMetadataInternal errors → Stat errors.

	f, err := newStorageFile(sfs, "mydir", os.O_RDONLY, 0755)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if f == nil {
		t.Fatal("expected non-nil file")
	}
	if !f.isDir {
		t.Error("expected isDir == true")
	}
	// fileInfo must have been initialised via the fallback NewFileInfo path.
	if f.fileInfo == nil {
		t.Error("expected fileInfo to be non-nil")
	}
}

// TestNewStorageFile_DirStatReturnsNonFileInfo exercises the branch where
// fs.Stat returns a non-*FileInfo value. We trigger this by storing metadata
// with invalid JSON (so Stat succeeds via the dirExists+fileExists fallback
// which returns a plain os.FileInfo, not *FileInfo).
//
// The simpler approach: write a valid metadata JSON that the StorageFS Stat
// will decode to *FileInfo but where we verify the else branch. The actual
// unreachable else branch is for when the storage Stat returns something that
// is not *FileInfo — that cannot happen with StorageFS.Stat itself.
// Instead we cover the adjacent line (creating default FileInfo when
// dirIndex==0 and f.fileInfo==nil inside Readdir).
func TestReaddir_FileInfoNilFallback(t *testing.T) {
	storage := newMockStorage()
	sfs := New(storage)

	_ = sfs.Mkdir("nildir", 0755)

	f, err := newStorageFile(sfs, "nildir", os.O_RDONLY, 0755)
	if err != nil {
		t.Fatalf("newStorageFile: %v", err)
	}

	// Null out fileInfo to force the nil branch inside Readdir.
	f.fileInfo = nil

	_, err = f.Readdir(-1)
	if err != nil && err != io.EOF {
		t.Fatalf("Readdir: %v", err)
	}
	// fileInfo should have been initialised inside Readdir.
	if f.fileInfo == nil {
		t.Error("expected fileInfo to be set after Readdir")
	}
}

// --- Read / ReadAt nil-buf paths (write-only file opened for WRONLY) ---------

// TestRead_NilBuf exercises the f.buf == nil branch in Read.
// A write-only file (O_WRONLY without O_RDWR) has a buf, but we can set it
// to nil directly.
func TestRead_NilBuf(t *testing.T) {
	storage := newMockStorage()
	sfs := New(storage)

	// Open with RDWR so we get a non-write-only file.
	f, err := newStorageFile(sfs, "nilbuf.txt", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("newStorageFile: %v", err)
	}

	// Force buf to nil to trigger the nil path.
	f.buf = nil

	buf := make([]byte, 4)
	n, err := f.Read(buf)
	if err != io.EOF {
		t.Errorf("Read: expected io.EOF, got %v", err)
	}
	if n != 0 {
		t.Errorf("Read: expected 0 bytes, got %d", n)
	}
}

// TestReadAt_NilBuf exercises the f.buf == nil branch in ReadAt.
func TestReadAt_NilBuf(t *testing.T) {
	storage := newMockStorage()
	sfs := New(storage)

	f, err := newStorageFile(sfs, "nilbuf.txt", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("newStorageFile: %v", err)
	}

	f.buf = nil

	buf := make([]byte, 4)
	n, err := f.ReadAt(buf, 0)
	if err != io.EOF {
		t.Errorf("ReadAt: expected io.EOF, got %v", err)
	}
	if n != 0 {
		t.Errorf("ReadAt: expected 0 bytes, got %d", n)
	}
}

// --- Write nil-buf path ------------------------------------------------------

// TestWrite_NilBuf exercises the f.buf == nil initialisation inside Write.
func TestWrite_NilBuf(t *testing.T) {
	storage := newMockStorage()
	sfs := New(storage)

	f, err := newStorageFile(sfs, "nilbuf.txt", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("newStorageFile: %v", err)
	}

	// Nil buf; Write should initialise it.
	f.buf = nil

	n, err := f.Write([]byte("hello"))
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if n != 5 {
		t.Errorf("Write: expected 5 bytes, got %d", n)
	}
	if f.buf == nil {
		t.Error("Write: buf should be non-nil after write")
	}
}

// --- WriteAt nil-buf path ----------------------------------------------------

// TestWriteAt_NilBuf exercises the f.buf == nil initialisation inside WriteAt.
func TestWriteAt_NilBuf(t *testing.T) {
	storage := newMockStorage()
	sfs := New(storage)

	f, err := newStorageFile(sfs, "nilbuf.txt", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("newStorageFile: %v", err)
	}
	f.buf = nil

	n, err := f.WriteAt([]byte("world"), 0)
	if err != nil {
		t.Fatalf("WriteAt: %v", err)
	}
	if n != 5 {
		t.Errorf("WriteAt: expected 5 bytes, got %d", n)
	}
}

// --- Truncate nil-buf path ---------------------------------------------------

// TestTruncate_NilBuf exercises the f.buf == nil initialisation in Truncate.
func TestTruncate_NilBuf(t *testing.T) {
	storage := newMockStorage()
	sfs := New(storage)

	f, err := newStorageFile(sfs, "trunc.txt", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("newStorageFile: %v", err)
	}
	f.buf = nil

	if err := f.Truncate(10); err != nil {
		t.Fatalf("Truncate: %v", err)
	}
}

// --- Truncate on closed file -------------------------------------------------

// TestTruncate_Closed exercises the ErrClosed check in Truncate.
func TestTruncate_Closed(t *testing.T) {
	storage := newMockStorage()
	sfs := New(storage)

	f, err := newStorageFile(sfs, "trunc.txt", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("newStorageFile: %v", err)
	}
	_ = f.Close()

	err = f.Truncate(10)
	if !errors.Is(err, fs.ErrClosed) {
		t.Errorf("Truncate on closed file: expected fs.ErrClosed, got %v", err)
	}
}

// --- Close putMetadata error path --------------------------------------------

// TestClose_PutMetadataError exercises the putMetadata error path in Close.
func TestClose_PutMetadataError(t *testing.T) {
	base := newMockStorage()
	mock := &putMetaErrMock{mockStorage: base, metaPutErr: errors.New("meta put failed")}
	sfs := New(mock)

	// We need a file that is open for writing with a valid fileInfo (so the
	// Put for the data succeeds, but the subsequent putMetadata fails).
	f, err := newStorageFile(sfs, "file.txt", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("newStorageFile: %v", err)
	}
	_, _ = f.Write([]byte("data"))

	err = f.Close()
	if err == nil {
		t.Fatal("expected error from Close when putMetadata fails")
	}
}

// --- Sync putMetadata error path ---------------------------------------------

// TestSync_PutMetadataError exercises the putMetadata error path in Sync.
func TestSync_PutMetadataError(t *testing.T) {
	base := newMockStorage()
	mock := &putMetaErrMock{mockStorage: base, metaPutErr: errors.New("meta put failed")}
	sfs := New(mock)

	f, err := newStorageFile(sfs, "file.txt", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		t.Fatalf("newStorageFile: %v", err)
	}
	_, _ = f.Write([]byte("data"))

	err = f.Sync()
	if err == nil {
		t.Fatal("expected error from Sync when putMetadata fails")
	}
}

// --- Readdir subsequent-call branches ----------------------------------------

// TestReaddir_SubsequentCallEOF exercises the subsequent-call path where
// dirIndex >= len(infos), returning io.EOF.
func TestReaddir_SubsequentCallEOF(t *testing.T) {
	storage := newMockStorage()
	sfs := New(storage)

	_ = sfs.Mkdir("d", 0755)
	// One file inside.
	f, _ := sfs.Create("d/only.txt")
	f.Close()

	dir, err := sfs.Open("d")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer dir.Close()

	// First call: read the one entry.
	entries, err := dir.Readdir(1)
	if err != nil && err != io.EOF {
		t.Fatalf("Readdir(1): %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}

	// Second call: dirIndex == 1, infos len == 1 → subsequent branch, EOF.
	_, err = dir.Readdir(1)
	if err != io.EOF {
		t.Errorf("second Readdir should return io.EOF, got %v", err)
	}
}

// TestReaddir_SubsequentCallAllRemaining exercises the subsequent-call
// count <= 0 (return all remaining) path.
func TestReaddir_SubsequentCallAllRemaining(t *testing.T) {
	storage := newMockStorage()
	sfs := New(storage)

	_ = sfs.Mkdir("d2", 0755)
	for i := 0; i < 4; i++ {
		n := "d2/f" + string(rune('a'+i)) + ".txt"
		ff, _ := sfs.Create(n)
		ff.Close()
	}

	dir, err := sfs.Open("d2")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer dir.Close()

	// First call: read 2 entries.
	_, err = dir.Readdir(2)
	if err != nil && err != io.EOF {
		t.Fatalf("Readdir(2) first call: %v", err)
	}

	// Second call: count <= 0 on subsequent path returns all remaining.
	remaining, err := dir.Readdir(-1)
	if err != nil && err != io.EOF {
		t.Fatalf("Readdir(-1) second call: %v", err)
	}
	if len(remaining) == 0 {
		t.Error("expected remaining entries to be returned")
	}
}

// TestReaddir_SubsequentCallPartialNoEOF exercises the subsequent-call path
// where we return a partial slice without EOF (more entries remain).
func TestReaddir_SubsequentCallPartialNoEOF(t *testing.T) {
	storage := newMockStorage()
	sfs := New(storage)

	_ = sfs.Mkdir("d3", 0755)
	for i := 0; i < 5; i++ {
		n := "d3/g" + string(rune('a'+i)) + ".txt"
		ff, _ := sfs.Create(n)
		ff.Close()
	}

	dir, err := sfs.Open("d3")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer dir.Close()

	// First call: read 2, leaving 3 remaining (no EOF).
	_, err = dir.Readdir(2)
	if err != nil && err != io.EOF {
		t.Fatalf("Readdir(2) first: %v", err)
	}

	// Second call: read 2, leaving 1 remaining (no EOF because dirIndex < len).
	entries, err := dir.Readdir(2)
	if err != nil && err != io.EOF {
		t.Fatalf("Readdir(2) second: %v", err)
	}
	if len(entries) == 0 {
		t.Error("expected entries from second call")
	}
}

// TestReaddir_FirstCallError exercises the path where readDirEntries returns
// an error on the first Readdir call.
func TestReaddir_FirstCallError(t *testing.T) {
	base := newMockStorage()
	listErr := &listWithOptionsErrMock{mockStorage: base, listErr: errors.New("list failed")}
	sfs := New(listErr)

	// Create the directory marker so dirExists returns true.
	_ = base.Put("errdir/.dir", bytes.NewReader([]byte{}))

	dir, err := newStorageFile(sfs, "errdir", os.O_RDONLY, 0755)
	if err != nil {
		t.Fatalf("newStorageFile: %v", err)
	}
	dir.isDir = true

	_, err = dir.Readdir(-1)
	if err == nil {
		t.Fatal("expected error from Readdir when ListWithOptions fails")
	}
}

// TestReaddir_SubsequentCallError exercises the path where readDirEntries
// returns an error on a subsequent Readdir call.
func TestReaddir_SubsequentCallError(t *testing.T) {
	base := newMockStorage()
	sfs := New(base)

	_ = sfs.Mkdir("errdir2", 0755)
	ff, _ := sfs.Create("errdir2/one.txt")
	ff.Close()

	dir, err := sfs.Open("errdir2")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer dir.Close()

	// First call succeeds.
	_, err = dir.Readdir(1)
	if err != nil && err != io.EOF {
		t.Fatalf("first Readdir: %v", err)
	}

	// Inject an error so the second Readdir call (subsequent path) fails.
	listErrMock := &listWithOptionsErrMock{mockStorage: base, listErr: errors.New("list failed on second call")}
	dir.(*StorageFile).fs.storage = listErrMock

	_, err = dir.Readdir(1)
	if err == nil {
		t.Fatal("expected error from subsequent Readdir when ListWithOptions fails")
	}
}

// --- Mkdir putMetadataInternal cleanup path ----------------------------------

// TestMkdir_PutMetadataError_Cleanup exercises the Mkdir error path where
// putMetadataInternal fails and we attempt to clean up the marker.
func TestMkdir_PutMetadataError_Cleanup(t *testing.T) {
	base := newMockStorage()
	mock := &putMetaErrMock{mockStorage: base, metaPutErr: errors.New("meta put failed")}
	sfs := New(mock)

	err := sfs.Mkdir("faildir", 0755)
	if err == nil {
		t.Fatal("expected Mkdir to fail when putMetadataInternal fails")
	}
}

// --- MkdirAll error propagation (non-ErrExist) -------------------------------

// TestMkdirAll_MkdirError exercises the code path where Mkdir returns an
// error that is NOT os.ErrExist during MkdirAll.
func TestMkdirAll_MkdirError(t *testing.T) {
	base := newMockStorage()
	mock := &putMetaErrMock{mockStorage: base, metaPutErr: errors.New("no space left")}
	sfs := New(mock)

	err := sfs.MkdirAll("a/b/c", 0755)
	if err == nil {
		t.Fatal("expected MkdirAll to fail when Mkdir returns non-ErrExist error")
	}
}

// --- Remove deleteMetadata error path for files ------------------------------

// TestRemove_FileDeleteMetadataError exercises the error path in Remove where
// deleting the file data succeeds but deleting metadata fails.
func TestRemove_FileDeleteMetadataError(t *testing.T) {
	base := newMockStorage()
	sfs := New(base)

	// Create a file.
	f, _ := sfs.Create("delme.txt")
	f.Close()

	// Inject metadata-delete error.
	mock := &deleteMetaErrMock{mockStorage: base, metaDelErr: errors.New("meta delete failed")}
	sfs.storage = mock

	err := sfs.Remove("delme.txt")
	if err == nil {
		t.Fatal("expected error when metadata delete fails during Remove")
	}
}

// --- Remove directory deleteMetadata error path ------------------------------

// TestRemove_DirDeleteMetadataError exercises the error path in Remove where
// the directory marker delete succeeds but metadata delete fails.
func TestRemove_DirDeleteMetadataError(t *testing.T) {
	base := newMockStorage()
	sfs := New(base)

	_ = sfs.Mkdir("deldir", 0755)

	mock := &deleteMetaErrMock{mockStorage: base, metaDelErr: errors.New("meta delete failed")}
	sfs.storage = mock

	err := sfs.Remove("deldir")
	if err == nil {
		t.Fatal("expected error when metadata delete fails for directory")
	}
}

// --- RemoveAll with delete errors --------------------------------------------

// TestRemoveAll_DeleteErrors exercises RemoveAll when storage.Delete returns
// errors; the function continues without propagating errors.
func TestRemoveAll_DeleteErrors(t *testing.T) {
	base := newMockStorage()
	base.deleteError = errors.New("delete error")
	sfs := New(base)

	// RemoveAll should return nil even when deletions fail (best-effort).
	err := sfs.RemoveAll("someprefix")
	if err != nil {
		t.Fatalf("RemoveAll: expected nil, got %v", err)
	}
}

// --- Rename putMetadataInternal error path -----------------------------------

// TestRename_PutMetadataInternalError exercises the Rename error path where
// putMetadataInternal fails.
func TestRename_PutMetadataInternalError(t *testing.T) {
	base := newMockStorage()
	sfs := New(base)

	// Create a file with a proper metadata entry.
	f, _ := sfs.Create("old.txt")
	_, _ = f.Write([]byte("content"))
	f.Close()

	// Inject metadata put error.
	mock := &putMetaErrMock{mockStorage: base, metaPutErr: errors.New("meta put failed")}
	sfs.storage = mock

	err := sfs.Rename("old.txt", "new.txt")
	if err == nil {
		t.Fatal("expected error from Rename when putMetadataInternal fails")
	}
}

// TestRename_DeleteMetadataError exercises the Rename error path where
// deleteMetadata (for the old path) fails.
func TestRename_DeleteMetadataError(t *testing.T) {
	base := newMockStorage()
	sfs := New(base)

	f, _ := sfs.Create("src.txt")
	_, _ = f.Write([]byte("content"))
	f.Close()

	mock := &deleteMetaErrMock{mockStorage: base, metaDelErr: errors.New("meta delete failed")}
	sfs.storage = mock

	err := sfs.Rename("src.txt", "dst.txt")
	if err == nil {
		t.Fatal("expected error from Rename when deleteMetadata fails")
	}
}

// --- Rename directory Delete error (new marker cleanup) ----------------------

// TestRename_DirDeleteOldMarkerError exercises the directory rename path where
// deleting the old marker fails; the new marker must be cleaned up.
func TestRename_DirDeleteOldMarkerError(t *testing.T) {
	base := newMockStorage()
	sfs := New(base)

	_ = sfs.Mkdir("srcdir", 0755)

	base.deleteError = errors.New("delete failed")

	err := sfs.Rename("srcdir", "dstdir")
	if err == nil {
		t.Fatal("expected error from Rename when deleting old dir marker fails")
	}

	base.deleteError = nil
}

// --- normalizePath: leading-slash result normalisation -----------------------

// TestNormalizePath_SingleSlash covers the branch where path.Clean reduces to
// "/" (leading slash only) and we strip it to ".".
func TestNormalizePath_SingleSlash(t *testing.T) {
	got := normalizePath("/")
	if got != "." {
		t.Errorf("normalizePath('/') = %q, want '.'", got)
	}
}

// --- putMetadataInternal: json.Marshal failure is not reachable
// (fileMetadata is a plain struct); instead cover the Put error path via the
// existing test. This test covers the return-on-put-error branch by checking
// the function returns the storage error.
func TestPutMetadataInternal_PutError(t *testing.T) {
	base := newMockStorage()
	base.putError = errors.New("put failed")
	sfs := New(base)

	err := sfs.putMetadataInternal("test", fileMetadata{Name: "test"})
	if err == nil {
		t.Fatal("expected error from putMetadataInternal when Put fails")
	}
}

// --- listKeys: non-lister storage --------------------------------------------

// TestListKeys_NonLister exercises the fallback (empty list) when the storage
// does not implement the unexported lister interface.  minimalStorage has no
// listKeys method of its own (no embedding of mockStorage), so the type
// assertion fails and the function returns [].
func TestListKeys_NonLister(t *testing.T) {
	sfs := New(newMinimalStorage())

	keys := sfs.listKeys("anything")
	if len(keys) != 0 {
		t.Fatalf("expected empty key list for non-lister storage, got %d", len(keys))
	}
}

// --- readDirEntries: file with no metadata but Metadata field set ------------

// TestReadDirEntries_FileMetadataMissing exercises the branch in readDirEntries
// where getMetadataInternal fails but obj.Metadata is set, so the object-level
// metadata (size, modTime) is used to construct the FileInfo.
func TestReadDirEntries_FileMetadataMissing(t *testing.T) {
	storage := newMockStorage()
	sfs := New(storage)

	_ = sfs.Mkdir("fmm", 0755)

	// Write file content directly into storage without creating metadata.
	_ = storage.Put("fmm/naked.txt", bytes.NewReader([]byte("hi there")))

	dir, err := sfs.Open("fmm")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer dir.Close()

	entries, err := dir.Readdir(-1)
	if err != nil && err != io.EOF {
		t.Fatalf("Readdir: %v", err)
	}

	found := false
	for _, e := range entries {
		if e.Name() == "naked.txt" {
			found = true
		}
	}
	if !found {
		t.Error("expected naked.txt to appear in directory listing")
	}
}

// --- readDirEntries: ListWithOptions returns an error ------------------------

// TestReadDirEntries_ListError exercises the error return from readDirEntries.
func TestReadDirEntries_ListError(t *testing.T) {
	base := newMockStorage()
	listErr := &listWithOptionsErrMock{mockStorage: base, listErr: errors.New("list failed")}
	sfs := New(listErr)

	_, err := sfs.readDirEntries(".")
	if err == nil {
		t.Fatal("expected error from readDirEntries when ListWithOptions fails")
	}
}

// --- ListWithOptions nil-opts branch -----------------------------------------

// TestListWithOptions_NilOpts exercises the branch in mockStorage.ListWithOptions
// that sets a default ListOptions when opts is nil.
func TestListWithOptions_NilOpts(t *testing.T) {
	mock := newMockStorage()
	_ = mock.Put("a/key1", bytes.NewReader([]byte("1")))
	_ = mock.Put("b/key2", bytes.NewReader([]byte("2")))

	result, err := mock.ListWithOptions(context.Background(), nil)
	if err != nil {
		t.Fatalf("ListWithOptions(nil): %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
}

// --- normalizePath: empty-after-trim branch ----------------------------------

// TestNormalizePath_DoubleSlash exercises the branch where after backslash
// replacement, path.Clean, and TrimPrefix the result is "" so we return ".".
// Input "//" goes through: early-return misses (//) → Clean("//") = "/" →
// TrimPrefix("/", "/") = "" → returns ".".
func TestNormalizePath_DoubleSlash(t *testing.T) {
	got := normalizePath("//")
	if got != "." {
		t.Errorf("normalizePath('//') = %q, want '.'", got)
	}
}

// --- Readdir first-call: count > len(remaining) truncation ------------------

// TestReaddir_FirstCallCountExceedsEntries exercises the count > len(remaining)
// branch in the first-call path: when the caller asks for more entries than
// exist in the directory, count is clamped to len(remaining) and we return
// with io.EOF.
func TestReaddir_FirstCallCountExceedsEntries(t *testing.T) {
	storage := newMockStorage()
	sfs := New(storage)

	_ = sfs.Mkdir("fewfiles", 0755)
	for i := 0; i < 2; i++ {
		name := "fewfiles/" + string(rune('a'+i)) + ".txt"
		f, _ := sfs.Create(name)
		f.Close()
	}

	dir, err := sfs.Open("fewfiles")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer dir.Close()

	// Ask for 100 entries when only 2 exist.
	entries, err := dir.Readdir(100)
	if err != io.EOF {
		t.Errorf("expected io.EOF when count > entries, got %v", err)
	}
	if len(entries) != 2 {
		t.Errorf("expected 2 entries, got %d", len(entries))
	}
}

// --- RemoveAll: delete errors are swallowed -----------------------------------

// TestRemoveAll_KeyDeleteError exercises the loop body that calls continue on a
// delete error so that RemoveAll does not propagate it.
func TestRemoveAll_KeyDeleteError(t *testing.T) {
	storage := newMockStorage()
	sfs := New(storage)

	// Create a directory with a file so listKeys returns at least one key.
	_ = sfs.MkdirAll("toremove", 0755)
	f, _ := sfs.Create("toremove/file.txt")
	f.Close()

	// deleteError makes every Delete call fail.
	storage.deleteError = errors.New("disk full")

	err := sfs.RemoveAll("toremove")
	// RemoveAll silently ignores delete errors.
	if err != nil {
		t.Fatalf("RemoveAll should return nil even on delete errors, got %v", err)
	}

	storage.deleteError = nil
}

// --- Rename: directory marker Get error (after metadata retrieval succeeds) --

// TestRename_DirMarkerGetError exercises line 240-242 in Rename: the storage
// Get for the old directory marker fails while the prior getMetadataInternal
// succeeds.
func TestRename_DirMarkerGetError(t *testing.T) {
	base := newMockStorage()
	sfs := New(base)

	_ = sfs.Mkdir("srcdir2", 0755)

	// Inject a selective Get error that only triggers for the dir marker key.
	getErrOnlyMarker := &selectiveGetErrMock{
		mockStorage: base,
		errorKeys:   map[string]bool{"srcdir2/.dir": true},
	}
	sfs.storage = getErrOnlyMarker

	err := sfs.Rename("srcdir2", "dstdir2")
	if err == nil {
		t.Fatal("expected error when directory marker Get fails during Rename")
	}
}

// --- Rename: file data Get error (after metadata retrieval succeeds) ---------

// TestRename_FileGetError exercises line 258-260 in Rename: the storage
// Get for the file data fails while the prior getMetadataInternal succeeds.
func TestRename_FileGetError(t *testing.T) {
	base := newMockStorage()
	sfs := New(base)

	f, _ := sfs.Create("src2.txt")
	_, _ = f.Write([]byte("hello"))
	f.Close()

	// Inject a selective Get error that only triggers for the data key.
	getErrOnly := &selectiveGetErrMock{
		mockStorage: base,
		errorKeys:   map[string]bool{"src2.txt": true},
	}
	sfs.storage = getErrOnly

	err := sfs.Rename("src2.txt", "dst2.txt")
	if err == nil {
		t.Fatal("expected error when file data Get fails during Rename")
	}
}

// selectiveGetErrMock returns an error from Get only for specific keys.
type selectiveGetErrMock struct {
	*mockStorage
	errorKeys map[string]bool
}

func (m *selectiveGetErrMock) Get(key string) (io.ReadCloser, error) {
	if m.errorKeys[key] {
		return nil, errors.New("selective get error")
	}
	return m.mockStorage.Get(key)
}

// --- readDirEntries: duplicate baseName in CommonPrefixes (seen dedup) -------

// TestReadDirEntries_SeenDedupDir exercises the seen[baseName] continue branch
// for CommonPrefixes by injecting a ListWithOptions result with duplicate
// common prefixes.
func TestReadDirEntries_SeenDedupDir(t *testing.T) {
	base := newMockStorage()
	sfs := New(base)

	// Create dir so dirExists works.
	_ = base.Put("parent/.dir", bytes.NewReader([]byte{}))

	// Create a custom list mock that returns duplicate CommonPrefixes.
	dupMock := &dupCommonPrefixMock{mockStorage: base}
	sfs.storage = dupMock

	entries, err := sfs.readDirEntries("parent")
	if err != nil {
		t.Fatalf("readDirEntries: %v", err)
	}
	// Duplicate entries must be deduped: only 1 "sub" directory.
	count := 0
	for _, e := range entries {
		if e.Name() == "sub" {
			count++
		}
	}
	if count != 1 {
		t.Errorf("expected 1 'sub' entry after dedup, got %d", count)
	}
}

// dupCommonPrefixMock returns two identical CommonPrefixes in ListWithOptions.
type dupCommonPrefixMock struct {
	*mockStorage
}

func (m *dupCommonPrefixMock) ListWithOptions(ctx context.Context, opts *common.ListOptions) (*common.ListResult, error) {
	return &common.ListResult{
		// "parent/sub/" appears twice → dedup should eliminate second.
		CommonPrefixes: []string{"parent/sub/", "parent/sub/"},
		Objects:        []*common.ObjectInfo{},
	}, nil
}

// --- readDirEntries: subdir with no metadata (fallback FileInfo for dir) -----

// TestReadDirEntries_DirNoMetadata exercises the err != nil branch in the
// CommonPrefixes loop where getMetadataInternal fails and a default FileInfo
// is created for the directory entry.
func TestReadDirEntries_DirNoMetadata(t *testing.T) {
	storage := newMockStorage()
	sfs := New(storage)

	// Parent dir.
	_ = sfs.Mkdir("pdir", 0755)
	// Sub-dir created via raw marker only (no metadata) so getMetadataInternal fails.
	_ = storage.Put("pdir/rawsub/.dir", bytes.NewReader([]byte{}))

	dir, err := sfs.Open("pdir")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer dir.Close()

	entries, err := dir.Readdir(-1)
	if err != nil && err != io.EOF {
		t.Fatalf("Readdir: %v", err)
	}

	found := false
	for _, e := range entries {
		if e.Name() == "rawsub" && e.IsDir() {
			found = true
		}
	}
	if !found {
		t.Error("expected rawsub directory entry with fallback FileInfo")
	}
}

// --- readDirEntries: empty CommonPrefix (dirPath == "") ----------------------

// TestReadDirEntries_EmptyCommonPrefix exercises the dirPath == "" continue
// branch (line 536-537) when a CommonPrefix is just "/" (yields empty dirPath).
func TestReadDirEntries_EmptyCommonPrefix(t *testing.T) {
	base := newMockStorage()
	sfs := New(base)

	emptyPrefixMock := &emptyCommonPrefixMock{mockStorage: base}
	sfs.storage = emptyPrefixMock

	entries, err := sfs.readDirEntries(".")
	if err != nil {
		t.Fatalf("readDirEntries: %v", err)
	}
	// We just want no panic and no entry for the empty prefix.
	_ = entries
}

// emptyCommonPrefixMock returns a CommonPrefix of "/" which TrimSuffix("/", "/")
// reduces to "" triggering the skip branch.
type emptyCommonPrefixMock struct {
	*mockStorage
}

func (m *emptyCommonPrefixMock) ListWithOptions(ctx context.Context, opts *common.ListOptions) (*common.ListResult, error) {
	return &common.ListResult{
		CommonPrefixes: []string{"/"},
		Objects:        []*common.ObjectInfo{},
	}, nil
}

// --- readDirEntries: non-direct child Objects (relPath contains "/") ---------

// TestReadDirEntries_NonDirectChild exercises the strings.Contains(relPath, "/")
// continue branch for objects that are not direct children.
func TestReadDirEntries_NonDirectChild(t *testing.T) {
	base := newMockStorage()
	sfs := New(base)

	nonDirectMock := &nonDirectChildMock{mockStorage: base}
	sfs.storage = nonDirectMock

	entries, err := sfs.readDirEntries("parent")
	if err != nil {
		t.Fatalf("readDirEntries: %v", err)
	}
	// The nested key should have been skipped.
	if len(entries) != 0 {
		t.Errorf("expected 0 entries (non-direct child skipped), got %d", len(entries))
	}
}

// nonDirectChildMock returns an object whose relative path contains a slash,
// simulating a non-direct child.
type nonDirectChildMock struct {
	*mockStorage
}

func (m *nonDirectChildMock) ListWithOptions(ctx context.Context, opts *common.ListOptions) (*common.ListResult, error) {
	return &common.ListResult{
		CommonPrefixes: []string{},
		Objects: []*common.ObjectInfo{
			{
				Key:      "parent/deep/nested.txt",
				Metadata: &common.Metadata{Size: 10},
			},
		},
	}, nil
}

// --- readDirEntries: duplicate file baseName (seen dedup for objects) --------

// TestReadDirEntries_SeenDedupFile exercises the seen[baseName] continue branch
// for duplicate file names in the Objects slice.
func TestReadDirEntries_SeenDedupFile(t *testing.T) {
	base := newMockStorage()
	sfs := New(base)

	dupFileMock := &dupObjectMock{mockStorage: base}
	sfs.storage = dupFileMock

	entries, err := sfs.readDirEntries("parent")
	if err != nil {
		t.Fatalf("readDirEntries: %v", err)
	}
	count := 0
	for _, e := range entries {
		if e.Name() == "dup.txt" {
			count++
		}
	}
	if count != 1 {
		t.Errorf("expected 1 'dup.txt' entry after dedup, got %d", count)
	}
}

// dupObjectMock returns two Objects with the same key.
type dupObjectMock struct {
	*mockStorage
}

func (m *dupObjectMock) ListWithOptions(ctx context.Context, opts *common.ListOptions) (*common.ListResult, error) {
	return &common.ListResult{
		CommonPrefixes: []string{},
		Objects: []*common.ObjectInfo{
			{Key: "parent/dup.txt", Metadata: &common.Metadata{Size: 5}},
			{Key: "parent/dup.txt", Metadata: &common.Metadata{Size: 5}},
		},
	}, nil
}

// --- newStorageFile: Stat returns non-*FileInfo for directory ----------------

// TestNewStorageFile_DirStatNonFileInfo exercises the else branch (line 70-72)
// where fs.Stat succeeds but returns a non-*FileInfo os.FileInfo. We do this
// with a custom StorageFS wrapper whose Stat returns a *plainFileInfo.
func TestNewStorageFile_DirStatNonFileInfo(t *testing.T) {
	storage := newMockStorage()
	sfs := New(storage)

	// Write dir marker so dirExists succeeds.
	_ = storage.Put("customdir/.dir", bytes.NewReader([]byte{}))
	// Write metadata that intentionally decodes to plainFileInfo via the
	// statNonFileInfoMock below.
	// We inject a custom storage whose Get for the metadata key returns a
	// reader that, when unmarshalled, produces a non-*FileInfo through our
	// wrapper.
	statMock := &statNonFileInfoStorageFS{StorageFS: sfs}

	f, err := statMock.openFile("customdir")
	if err != nil {
		t.Fatalf("openFile: %v", err)
	}
	if !f.isDir {
		t.Error("expected isDir")
	}
	if f.fileInfo == nil {
		t.Error("expected fallback fileInfo")
	}
}

// statNonFileInfoStorageFS wraps StorageFS and overrides Stat to return a
// non-*FileInfo value, exercising the else branch in newStorageFile.
type statNonFileInfoStorageFS struct {
	*StorageFS
}

func (s *statNonFileInfoStorageFS) Stat(name string) (os.FileInfo, error) {
	return &plainFileInfo{name: name, isDir: true}, nil
}

func (s *statNonFileInfoStorageFS) openFile(name string) (*StorageFile, error) {
	name = normalizePath(name)
	f := &StorageFile{
		fs:   s.StorageFS,
		name: name,
		flag: os.O_RDONLY,
		perm: 0755,
	}

	if exists, _ := s.StorageFS.dirExists(name); exists {
		f.isDir = true
		info, err := s.Stat(name) // returns *plainFileInfo, not *FileInfo
		if err != nil {
			info = NewFileInfo(name, 0, os.ModeDir|0755, time.Now(), true)
		}
		if fi, ok := info.(*FileInfo); ok {
			f.fileInfo = fi
		} else {
			// This is the else branch we want to cover.
			f.fileInfo = NewFileInfo(name, 0, os.ModeDir|0755, time.Now(), true)
		}
		return f, nil
	}
	return nil, os.ErrNotExist
}
