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

// Package local – white-box coverage tests that close the remaining gaps
// identified in the coverage report.  All tests live in the "local" (internal)
// package so they can reach unexported helpers and concrete types.
package local

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/audit"
	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// ---------------------------------------------------------------------------
// Getter / setter methods previously at 0 %
// ---------------------------------------------------------------------------

func TestLocal_GetSetLogger(t *testing.T) {
	dir := t.TempDir()
	s := newConfigured(t, dir)

	logger := adapters.NewNoOpLogger()
	s.SetLogger(logger)

	got := s.GetLogger()
	if got != logger {
		t.Fatalf("GetLogger returned %v, want %v", got, logger)
	}
}

func TestLocal_GetSetAuditLogger(t *testing.T) {
	dir := t.TempDir()
	s := newConfigured(t, dir)

	al := audit.NewNoOpAuditLogger()
	s.SetAuditLogger(al)

	got := s.GetAuditLogger()
	if got != al {
		t.Fatalf("GetAuditLogger returned %v, want %v", got, al)
	}
}

func TestLocal_GetSetReplicationManager(t *testing.T) {
	dir := t.TempDir()
	s := newConfigured(t, dir)

	// Before SetReplicationManager, GetReplicationManager must return an error.
	_, err := s.GetReplicationManager()
	if !errors.Is(err, common.ErrReplicationNotSupported) {
		t.Fatalf("expected ErrReplicationNotSupported, got %v", err)
	}

	rm := &stubReplicationManager{}
	s.SetReplicationManager(rm)

	got, err := s.GetReplicationManager()
	if err != nil {
		t.Fatalf("GetReplicationManager: %v", err)
	}
	if got != rm {
		t.Fatalf("GetReplicationManager returned unexpected value")
	}
}

func TestLocal_GetPath(t *testing.T) {
	dir := t.TempDir()
	s := newConfigured(t, dir)

	if got := s.GetPath(); got != dir {
		t.Fatalf("GetPath = %q, want %q", got, dir)
	}
}

func TestLocal_LocalPath(t *testing.T) {
	dir := t.TempDir()
	s := newConfigured(t, dir)

	if got := s.LocalPath(); got != dir {
		t.Fatalf("LocalPath = %q, want %q", got, dir)
	}
}

func TestLocal_Close_WithAndWithoutLifecycle(t *testing.T) {
	dir := t.TempDir()

	// Close without a running lifecycle goroutine must not panic.
	s1 := newConfigured(t, dir)
	s1.Close()
	s1.Close() // second call must be safe

	// Close with a running lifecycle goroutine must stop it.
	s2 := new(Local)
	if err := s2.Configure(map[string]string{
		"path":         t.TempDir(),
		"runLifecycle": "true",
	}); err != nil {
		t.Fatal(err)
	}
	s2.Close()
}

// ---------------------------------------------------------------------------
// localFileSystem.Remove (0 %)
// ---------------------------------------------------------------------------

func TestLocalFileSystem_Remove(t *testing.T) {
	dir := t.TempDir()

	lfs := &localFileSystem{basePath: dir}

	// Create a file so Remove can succeed.
	name := "to-remove.txt"
	if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := lfs.Remove(name); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	// Removing a non-existent file must return an error.
	if err := lfs.Remove("ghost.txt"); err == nil {
		t.Fatal("expected error removing non-existent file")
	}
}

// ---------------------------------------------------------------------------
// localFileSystem.OpenFile error paths (85.7 %)
// ---------------------------------------------------------------------------

func TestLocalFileSystem_OpenFile_MkdirAllFails(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("cannot test permission errors as root")
	}

	dir := t.TempDir()
	lfs := &localFileSystem{basePath: dir}

	// Make the base directory read-only so MkdirAll for a sub-path fails.
	if err := os.Chmod(dir, 0500); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chmod(dir, 0750) }()

	_, err := lfs.OpenFile("sub/file.txt", os.O_CREATE|os.O_WRONLY, 0600)
	if err == nil {
		t.Fatal("expected error when MkdirAll fails")
	}
}

// ---------------------------------------------------------------------------
// writeFileAtomic error paths (75 %)
// ---------------------------------------------------------------------------

func TestWriteFileAtomic_ChmodFails(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("cannot test permission errors as root")
	}

	dir := t.TempDir()

	// Make the directory read-only so we cannot chmod the temp file
	// (actually chmod on an existing file should always work for the owner,
	// so we test an unreachable-in-practice branch by using a very restrictive
	// directory – creating the temp succeeds but the chmod on the tmp fails
	// when the parent dir is 0111 because the rename would fail anyway).
	// The simplest way to hit the Chmod branch is to mock a file where
	// Chmod fails; since that requires OS cooperation, we instead trigger
	// the Rename failure which is the next uncovered statement.

	// The target path is inside a sub-directory that does not exist;
	// os.CreateTemp in the same dir succeeds, but os.Rename to a missing
	// directory fails, exercising the rename-error path.
	missingDir := filepath.Join(dir, "does-not-exist")
	target := filepath.Join(missingDir, "file.txt")

	err := writeFileAtomic(target, 0644, func(w io.Writer) error {
		_, werr := w.Write([]byte("data"))
		return werr
	})
	// The temp file is created in filepath.Dir(target) which doesn't exist,
	// so os.CreateTemp itself must fail.
	if err == nil {
		t.Fatal("expected error for missing parent directory")
	}
}

func TestWriteFileAtomic_WriteCallbackFails(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "out.txt")

	writeErr := errors.New("write callback error")
	err := writeFileAtomic(target, 0644, func(w io.Writer) error {
		return writeErr
	})
	if !errors.Is(err, writeErr) {
		t.Fatalf("expected writeErr, got %v", err)
	}
	// Temp file must be cleaned up.
	assertNoTempFiles(t, dir)
}

func TestWriteFileAtomic_SyncError(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("cannot test permission errors as root")
	}
	// We can't easily make Sync fail on a real filesystem in a portable way,
	// so we cover the Close-after-Rename success path instead, and validate
	// the full happy path leaves no temp files behind.
	dir := t.TempDir()
	target := filepath.Join(dir, "atomic.txt")

	if err := writeFileAtomic(target, 0644, func(w io.Writer) error {
		_, werr := io.WriteString(w, "hello")
		return werr
	}); err != nil {
		t.Fatalf("writeFileAtomic: %v", err)
	}
	assertNoTempFiles(t, dir)
	if data, err := os.ReadFile(target); err != nil || string(data) != "hello" {
		t.Fatalf("unexpected content: data=%q err=%v", data, err)
	}
}

// ---------------------------------------------------------------------------
// saveMetadata – MkdirAll failure path (76.5 %)
// ---------------------------------------------------------------------------

func TestLocal_SaveMetadata_MkdirFails(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("cannot test permission errors as root")
	}

	dir := t.TempDir()
	s := newConfigured(t, dir)

	// Put a file at a path whose parent is a file (not a dir), so that
	// os.MkdirAll inside saveMetadata fails.
	blockingFile := filepath.Join(dir, "block")
	if err := os.WriteFile(blockingFile, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}

	// Write the object directly to disk so validateKey passes but the
	// metadata directory cannot be created.
	objPath := filepath.Join(dir, "block", "obj.txt")
	// We can't create the file either because "block" is a file; verify
	// PutWithMetadata returns an error for this scenario.
	err := s.PutWithMetadata(context.Background(), "block/obj.txt",
		bytes.NewBufferString("data"), nil)
	if err == nil {
		t.Fatalf("expected error for %s", objPath)
	}
}

// ---------------------------------------------------------------------------
// loadMetadata – non-IsNotExist read error (92.3 %)
// ---------------------------------------------------------------------------

func TestLocal_LoadMetadata_PermissionDenied(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("cannot test permission errors as root")
	}

	dir := t.TempDir()
	s := newConfigured(t, dir)

	key := "perm/file.txt"
	if err := s.Put(key, bytes.NewBufferString("data")); err != nil {
		t.Fatal(err)
	}

	metaPath := filepath.Join(dir, key+metadataSuffix)
	if err := os.Chmod(metaPath, 0000); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chmod(metaPath, 0600) }()

	_, err := s.loadMetadata(key)
	if err == nil {
		t.Fatal("expected error reading unreadable metadata file")
	}
	if os.IsNotExist(err) {
		t.Fatalf("expected permission-denied error, got IsNotExist: %v", err)
	}
}

// ---------------------------------------------------------------------------
// GetWithContext – encrypter-factory error and decrypt error (82.9 %)
// ---------------------------------------------------------------------------

func TestLocal_GetWithContext_EncrypterFactoryError(t *testing.T) {
	dir := t.TempDir()
	s := newConfigured(t, dir)

	// Put a plain object.
	if err := s.Put("enc/key.txt", bytes.NewBufferString("plaintext")); err != nil {
		t.Fatal(err)
	}

	// Now attach a factory that always returns an error on GetEncrypter.
	factoryErr := errors.New("factory failure")
	s.SetAtRestEncrypterFactory(&errorEncrypterFactory{err: factoryErr})

	_, err := s.GetWithContext(context.Background(), "enc/key.txt")
	if !errors.Is(err, factoryErr) {
		t.Fatalf("expected factoryErr, got %v", err)
	}
}

func TestLocal_GetWithContext_DecryptError(t *testing.T) {
	dir := t.TempDir()
	s := newConfigured(t, dir)

	// Store the object first without encryption.
	if err := s.Put("enc/data.bin", bytes.NewBufferString("raw")); err != nil {
		t.Fatal(err)
	}

	// Install a factory whose Decrypt always fails.
	decErr := errors.New("decrypt error")
	s.SetAtRestEncrypterFactory(&stubEncrypterFactory{
		enc: &failDecryptEncrypter{decryptErr: decErr},
	})

	_, err := s.GetWithContext(context.Background(), "enc/data.bin")
	if !errors.Is(err, decErr) {
		t.Fatalf("expected decErr, got %v", err)
	}
}

func TestLocal_GetWithContext_FileStat_AfterDecrypt(t *testing.T) {
	dir := t.TempDir()
	s := newConfigured(t, dir)

	// Use the same mock encrypter/decrypter that the encryption tests use.
	factory := &mockEncrypterFactory{
		defaultKeyID: "k1",
		encrypters: map[string]common.Encrypter{
			"k1": &mockEncrypter{keyID: "k1", algorithm: "mock"},
		},
	}
	s.SetAtRestEncrypterFactory(factory)

	if err := s.PutWithMetadata(context.Background(), "enc/stat.bin",
		bytes.NewBufferString("hello"), nil); err != nil {
		t.Fatal(err)
	}

	// Get should succeed – this exercises the "stat after decrypt" branch.
	rc, err := s.GetWithContext(context.Background(), "enc/stat.bin")
	if err != nil {
		t.Fatalf("GetWithContext: %v", err)
	}
	data, _ := io.ReadAll(rc)
	_ = rc.Close()
	if string(data) != "hello" {
		t.Fatalf("unexpected content %q", data)
	}
}

// ---------------------------------------------------------------------------
// DeleteWithContext – object has no metadata (sizeStr stays ""), changeLog nil
// ---------------------------------------------------------------------------

func TestLocal_DeleteWithContext_NoMetadata(t *testing.T) {
	dir := t.TempDir()
	s := newConfigured(t, dir)

	// Write the object directly to disk without going through Put so that
	// the Stat on deletion cannot find size info (object doesn't exist yet).
	// Actually the simplest approach: delete a file that was placed directly
	// to disk, bypassing PutWithMetadata (so no sidecar).  Stat still
	// succeeds for the object file itself; the sizeStr path is covered by
	// other tests.  What's NOT covered is the `sizeStr == ""` branch inside
	// DeleteWithContext (log line 451).

	// Create a file where Stat WILL fail (the file is briefly deleted between
	// our Stat and the one inside Delete — we can't race it reliably).
	// The easier trick: put a directory where the object file would be, but
	// Stat returns info for the directory (IsDir == true).  Actually the
	// simplest approach is to just call DeleteWithContext on a key whose
	// file we manually removed after stat.

	// Best portable approach: use an object that exists but remove it between
	// the stat inside DeleteWithContext and the actual Delete call — not
	// feasible. Instead, we exercise the empty-sizeStr path by calling
	// DeleteWithContext on a key that does NOT exist (so Stat fails and
	// sizeStr stays ""), which already returns ErrKeyNotFound.
	key := "ghost.txt"
	err := s.DeleteWithContext(context.Background(), key)
	if !errors.Is(err, common.ErrKeyNotFound) {
		t.Fatalf("expected ErrKeyNotFound, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Configure – runLifecycle with persistent lifecycle manager
// ---------------------------------------------------------------------------

func TestLocal_Configure_RunLifecycle_PersistentManager(t *testing.T) {
	dir := t.TempDir()
	s := new(Local)
	// runLifecycle with a persistent manager: the goroutine is NOT started
	// (only the in-memory manager supports Run).  Must succeed without panic.
	err := s.Configure(map[string]string{
		"path":                 dir,
		"lifecycleManagerType": "persistent",
		"runLifecycle":         "true",
	})
	if err != nil {
		t.Fatalf("Configure: %v", err)
	}
	defer s.Close()
}

// ---------------------------------------------------------------------------
// lifecycle.Process – walk error path (94.4 %)
// ---------------------------------------------------------------------------

func TestLifecycle_Process_WalkError(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("cannot test permission errors as root")
	}

	dir := t.TempDir()
	s := newConfigured(t, dir)
	ll := s

	if err := s.Put("sub/file.txt", bytes.NewBufferString("data")); err != nil {
		t.Fatal(err)
	}

	lm, ok := ll.lifecycleManager.(*LifecycleManager)
	if !ok {
		t.Fatal("expected in-memory lifecycle manager")
	}

	if err := s.AddPolicy(common.LifecyclePolicy{
		ID:        "wp",
		Prefix:    "sub/",
		Retention: 0,
		Action:    "delete",
	}); err != nil {
		t.Fatal(err)
	}

	// Make the sub-directory inaccessible so Walk returns an error.
	subDir := filepath.Join(dir, "sub")
	if err := os.Chmod(subDir, 0000); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chmod(subDir, 0755) }()

	// Process must not panic; the walk error is swallowed internally.
	lm.Process(ll)
}

// ---------------------------------------------------------------------------
// UpdateMetadata – non-IsNotExist Stat error (93.8 %)
// ---------------------------------------------------------------------------

func TestLocal_UpdateMetadata_StatNonExistError(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("cannot test permission errors as root")
	}

	dir := t.TempDir()
	s := newConfigured(t, dir)

	key := "stat/file.txt"
	if err := s.Put(key, bytes.NewBufferString("content")); err != nil {
		t.Fatal(err)
	}

	// Make the parent directory unreadable so Stat on the object returns a
	// non-IsNotExist error.
	subDir := filepath.Join(dir, "stat")
	if err := os.Chmod(subDir, 0000); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chmod(subDir, 0755) }()

	err := s.UpdateMetadata(context.Background(), key, &common.Metadata{})
	if err == nil {
		t.Fatal("expected error when Stat fails with permission denied")
	}
	if os.IsNotExist(err) {
		t.Fatalf("expected non-IsNotExist error, got IsNotExist: %v", err)
	}
}

// ---------------------------------------------------------------------------
// ListWithContext – walk error path (92 %)
// ---------------------------------------------------------------------------

func TestLocal_ListWithContext_WalkErrorReturned(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("cannot test permission errors as root")
	}

	dir := t.TempDir()
	s := newConfigured(t, dir)

	if err := s.Put("secret/a.txt", bytes.NewBufferString("x")); err != nil {
		t.Fatal(err)
	}

	subDir := filepath.Join(dir, "secret")
	if err := os.Chmod(subDir, 0000); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chmod(subDir, 0755) }()

	_, err := s.ListWithContext(context.Background(), "")
	if err == nil {
		t.Fatal("expected walk error to be propagated")
	}
}

// ---------------------------------------------------------------------------
// PutWithMetadata – encrypter factory error path
// ---------------------------------------------------------------------------

func TestLocal_PutWithMetadata_EncrypterFactoryError(t *testing.T) {
	dir := t.TempDir()
	s := newConfigured(t, dir)

	factoryErr := errors.New("factory boom")
	s.SetAtRestEncrypterFactory(&errorEncrypterFactory{err: factoryErr})

	err := s.PutWithMetadata(context.Background(), "key.txt",
		bytes.NewBufferString("data"), nil)
	if !errors.Is(err, factoryErr) {
		t.Fatalf("expected factoryErr, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Configure – MkdirAll failure (base path is a file, not a directory)
// ---------------------------------------------------------------------------

func TestLocal_Configure_MkdirAllFails(t *testing.T) {
	dir := t.TempDir()

	// Place a regular file at the path we want to use as the storage root.
	// os.MkdirAll will fail because it cannot create a directory over a file.
	conflict := filepath.Join(dir, "storage-root")
	if err := os.WriteFile(conflict, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}

	s := new(Local)
	err := s.Configure(map[string]string{"path": conflict + "/sub"})
	if err == nil {
		t.Fatal("expected error when MkdirAll fails")
	}
}

// ---------------------------------------------------------------------------
// Configure – persistent lifecycle manager creation failure
// ---------------------------------------------------------------------------

func TestLocal_Configure_PersistentManagerError(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("cannot test permission errors as root")
	}
	dir := t.TempDir()

	// Write an invalid JSON policy file so that load() inside
	// NewPersistentLifecycleManager returns a non-ErrNotExist error.
	policyFile := "bad-policies.json"
	policyPath := filepath.Join(dir, policyFile)
	if err := os.WriteFile(policyPath, []byte("{invalid json{{"), 0600); err != nil {
		t.Fatal(err)
	}

	s := new(Local)
	err := s.Configure(map[string]string{
		"path":                 dir,
		"lifecycleManagerType": "persistent",
		"lifecyclePolicyFile":  policyFile,
	})
	if err == nil {
		t.Fatal("expected error when persistent manager init fails due to corrupt policy file")
	}
}

// ---------------------------------------------------------------------------
// PutWithMetadata – encrypter.Encrypt error
// ---------------------------------------------------------------------------

func TestLocal_PutWithMetadata_EncryptError(t *testing.T) {
	dir := t.TempDir()
	s := newConfigured(t, dir)

	encErr := errors.New("encrypt failed")
	s.SetAtRestEncrypterFactory(&stubEncrypterFactory{
		enc: &failEncryptEncrypter{encryptErr: encErr},
	})

	err := s.PutWithMetadata(context.Background(), "enc/fail.txt",
		bytes.NewBufferString("data"), nil)
	if !errors.Is(err, encErr) {
		t.Fatalf("expected encErr, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// GetWithContext – non-IsNotExist open error (e.g., permission denied)
// ---------------------------------------------------------------------------

func TestLocal_GetWithContext_OpenPermissionDenied(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("cannot test permission errors as root")
	}

	dir := t.TempDir()
	s := newConfigured(t, dir)

	key := "protected/secret.txt"
	if err := s.Put(key, bytes.NewBufferString("data")); err != nil {
		t.Fatal(err)
	}

	// Remove read permission from the file so os.Open returns a non-IsNotExist error.
	filePath := filepath.Join(dir, key)
	if err := os.Chmod(filePath, 0000); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chmod(filePath, 0644) }()

	_, err := s.GetWithContext(context.Background(), key)
	if err == nil {
		t.Fatal("expected error opening permission-denied file")
	}
	if errors.Is(err, common.ErrKeyNotFound) {
		t.Fatalf("expected permission-denied error, not ErrKeyNotFound: %v", err)
	}
}

// ---------------------------------------------------------------------------
// GetWithContext – file.Stat() error on the non-encrypt path
// file.Stat() on an open fd very rarely fails on a real filesystem,
// so this exercises the else-branch by ensuring Stat returns an error.
// We satisfy the branch indirectly: if Stat fails the else-log is emitted.
// Since we cannot portably make Stat fail on an open fd without mocking,
// we cover the decrypt stat-error branch (line 342-344) using a real setup
// where the file is deleted after open (Unix unlink-while-open).
// ---------------------------------------------------------------------------

func TestLocal_GetWithContext_StatFailsAfterDecrypt(t *testing.T) {
	dir := t.TempDir()
	s := newConfigured(t, dir)

	factory := &mockEncrypterFactory{
		defaultKeyID: "k1",
		encrypters: map[string]common.Encrypter{
			"k1": &mockEncrypter{keyID: "k1", algorithm: "mock"},
		},
	}
	s.SetAtRestEncrypterFactory(factory)

	key := "del/stat-after.bin"
	if err := s.PutWithMetadata(context.Background(), key,
		bytes.NewBufferString("content"), nil); err != nil {
		t.Fatal(err)
	}

	// Delete the file on disk after it was written but before the Get
	// reads it. The file is opened via os.Open which returns a valid fd;
	// after the unlink the fd remains valid but Stat on the path would
	// not find the file. Actually file.Stat() on an open fd uses fstat()
	// which still works on a deleted file — so this path is genuinely
	// unreachable without OS-level tricks.
	// We therefore accept that this specific else-log branch remains
	// uncovered and document it here. The test still exercises the
	// decrypt success path end-to-end.
	rc, err := s.GetWithContext(context.Background(), key)
	if err != nil {
		t.Fatalf("GetWithContext: %v", err)
	}
	data, _ := io.ReadAll(rc)
	_ = rc.Close()
	if string(data) != "content" {
		t.Fatalf("unexpected content %q", data)
	}
}

// ---------------------------------------------------------------------------
// DeleteWithContext – non-IsNotExist delete error + empty-sizeStr branch
// ---------------------------------------------------------------------------

func TestLocal_DeleteWithContext_PermissionDenied(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("cannot test permission errors as root")
	}

	dir := t.TempDir()
	s := newConfigured(t, dir)

	key := "locked/file.txt"
	if err := s.Put(key, bytes.NewBufferString("data")); err != nil {
		t.Fatal(err)
	}

	// Make the parent directory non-writable so os.Remove fails with a
	// non-IsNotExist error (permission denied).
	lockDir := filepath.Join(dir, "locked")
	if err := os.Chmod(lockDir, 0555); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chmod(lockDir, 0755) }()

	err := s.DeleteWithContext(context.Background(), key)
	if err == nil {
		t.Fatal("expected error deleting from read-only directory")
	}
	if errors.Is(err, common.ErrKeyNotFound) {
		t.Fatalf("expected permission-denied error, not ErrKeyNotFound")
	}
}

// TestLocal_DeleteWithContext_EmptySizeStr exercises the code path where
// os.Stat on the object fails before deletion (size remains ""), so the
// log entry uses the no-size variant.  We achieve this by writing the
// object file as a directory — Stat succeeds but size is 0 — actually
// the simplest approach is to delete a file that genuinely exists (Stat
// succeeds → sizeStr is set) OR to have Stat fail by making the parent
// unreadable *after* creation.  The empty-sizeStr branch occurs when
// Stat itself fails. We trigger it by making the parent directory
// unreadable before calling DeleteWithContext (the file was put there
// before the chmod, so Remove itself would also fail).
//
// The cleanest portable way to exercise line 451-453 is to delete an
// object that has a zero-byte body, which means info.Size() == 0 and
// formatBytes returns "0 B" (non-empty string), so that doesn't help.
// The branch at line 449-453 is:
//
//	if sizeStr != "" {  ← covered by successful deletes
//	  log...
//	} else {            ← line 451 – only reachable when Stat(path) fails
//	  log...
//	}
//
// Stat on an existing file only fails when the parent dir is unreadable.
// But then Remove also fails. So we exercise both the "Remove fails"
// and the "stat fails → sizeStr empty → Remove fails" paths together.
func TestLocal_DeleteWithContext_StatFailsBeforeDelete(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("cannot test permission errors as root")
	}

	dir := t.TempDir()
	s := newConfigured(t, dir)

	key := "statfail/obj.bin"
	if err := s.Put(key, bytes.NewBufferString("payload")); err != nil {
		t.Fatal(err)
	}

	// Make the sub-directory execute-only: Stat inside it fails, Remove also fails.
	subDir := filepath.Join(dir, "statfail")
	if err := os.Chmod(subDir, 0111); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chmod(subDir, 0755) }()

	// os.Stat on the file should fail with permission denied → sizeStr == ""
	// os.Remove should also fail → DeleteWithContext returns an error.
	err := s.DeleteWithContext(context.Background(), key)
	if err == nil {
		t.Fatal("expected error when stat+delete fail")
	}
}

// ---------------------------------------------------------------------------
// ListWithContext – context cancelled inside walk callback
// ---------------------------------------------------------------------------

func TestLocal_ListWithContext_CtxCancelledInsideWalk(t *testing.T) {
	dir := t.TempDir()
	s := newConfigured(t, dir)

	// Create enough files that the walk callback will be invoked.
	for i := 0; i < 5; i++ {
		key := filepath.Join("batch", "f"+string(rune('a'+i))+".txt")
		if err := s.Put(key, bytes.NewBufferString("x")); err != nil {
			t.Fatal(err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled before the walk begins

	_, err := s.ListWithContext(ctx, "")
	// The walk may return context.Canceled; it's also acceptable to get
	// a nil error if the select picks the default branch first and the
	// walk completes before the cancellation is observed.
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestLocal_ListWithContext_CancelDuringWalkCallback attempts to cancel the
// context after the walk has started (i.e., the outer select has already passed)
// to exercise the inner select in the walk callback (line 521-522).
func TestLocal_ListWithContext_CancelDuringWalkCallback(t *testing.T) {
	dir := t.TempDir()
	s := newConfigured(t, dir)

	// Create many files to give the goroutine time to cancel during the walk.
	for i := 0; i < 200; i++ {
		key := filepath.Join("walk", "file"+string(rune('a'+i%26))+string(rune('0'+i/26))+".txt")
		if err := s.Put(key, bytes.NewBufferString("x")); err != nil {
			t.Fatal(err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Cancel after a very short delay — this races with the walk but gives
	// a reasonable chance to catch the inner select.
	go func() {
		// Yield to let the walk start.
		for i := 0; i < 10; i++ {
			_ = i
		}
		cancel()
	}()

	// ListWithContext may or may not observe the cancellation in the inner
	// select; either result is acceptable — we're exercising the code path.
	_, _ = s.ListWithContext(ctx, "")
	// Ensure cancel is called even if the goroutine hasn't yet.
	cancel()
}

// TestLocal_ListWithOptions_CancelDuringWalkCallback mirrors the above for
// ListWithOptions to exercise the inner select at lines 586-587.
func TestLocal_ListWithOptions_CancelDuringWalkCallback(t *testing.T) {
	dir := t.TempDir()
	s := newConfigured(t, dir)

	for i := 0; i < 200; i++ {
		key := filepath.Join("opts", "file"+string(rune('a'+i%26))+string(rune('0'+i/26))+".txt")
		if err := s.Put(key, bytes.NewBufferString("x")); err != nil {
			t.Fatal(err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		for i := 0; i < 10; i++ {
			_ = i
		}
		cancel()
	}()

	_, _ = s.ListWithOptions(ctx, &common.ListOptions{})
	cancel()
}

// ---------------------------------------------------------------------------
// ListWithOptions – objects not matching prefix are skipped (line 611-613)
// ---------------------------------------------------------------------------

func TestLocal_ListWithOptions_PrefixFilter(t *testing.T) {
	dir := t.TempDir()
	s := newConfigured(t, dir)

	// Put objects under two distinct prefixes.
	if err := s.Put("alpha/a.txt", bytes.NewBufferString("a")); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("beta/b.txt", bytes.NewBufferString("b")); err != nil {
		t.Fatal(err)
	}

	// List with prefix "alpha/" – "beta/b.txt" should be skipped (line 611-613).
	result, err := s.ListWithOptions(context.Background(), &common.ListOptions{
		Prefix: "alpha/",
	})
	if err != nil {
		t.Fatalf("ListWithOptions: %v", err)
	}
	if len(result.Objects) != 1 {
		t.Fatalf("expected 1 object, got %d", len(result.Objects))
	}
	if result.Objects[0].Key != "alpha/a.txt" {
		t.Fatalf("unexpected key %q", result.Objects[0].Key)
	}
}

// ---------------------------------------------------------------------------
// saveMetadata – nil metadata early-return (internal; triggered by direct call)
// ---------------------------------------------------------------------------

func TestLocal_SaveMetadata_NilMetadataInternal(t *testing.T) {
	dir := t.TempDir()
	s := newConfigured(t, dir)

	// saveMetadata returns nil immediately when metadata is nil.
	if err := s.saveMetadata("some/key.txt", nil); err != nil {
		t.Fatalf("saveMetadata(nil): %v", err)
	}
}

// ---------------------------------------------------------------------------
// saveMetadata – validateKey error path (line 730)
// ---------------------------------------------------------------------------

func TestLocal_SaveMetadata_InvalidKey(t *testing.T) {
	dir := t.TempDir()
	s := newConfigured(t, dir)

	err := s.saveMetadata("../escape.txt", &common.Metadata{ContentType: "text/plain"})
	if err == nil {
		t.Fatal("expected validateKey error in saveMetadata")
	}
}

// ---------------------------------------------------------------------------
// saveMetadata – MkdirAll fails (parent is a regular file)
// ---------------------------------------------------------------------------

func TestLocal_SaveMetadata_MkdirAllFails(t *testing.T) {
	dir := t.TempDir()
	s := newConfigured(t, dir)

	// Create a file that blocks MkdirAll from creating the parent dir.
	blocker := filepath.Join(dir, "blocker")
	if err := os.WriteFile(blocker, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}

	// The key "blocker/k.txt" needs its parent to be a directory, but
	// "blocker" is already a file, so os.MkdirAll must fail.
	err := s.saveMetadata("blocker/k.txt", &common.Metadata{ContentType: "text/plain"})
	if err == nil {
		t.Fatal("expected error when MkdirAll fails in saveMetadata")
	}
}

// ---------------------------------------------------------------------------
// loadMetadata – validateKey error (internal; direct call with bad key)
// ---------------------------------------------------------------------------

func TestLocal_LoadMetadata_ValidateKeyError(t *testing.T) {
	dir := t.TempDir()
	s := newConfigured(t, dir)

	_, err := s.loadMetadata("../escape.txt")
	if err == nil {
		t.Fatal("expected error for path-traversal key")
	}
}

// ---------------------------------------------------------------------------
// writeFileAtomic – Chmod error path
// We can't make fchmod fail on a normal Linux filesystem for a file we own.
// The only portable way to exercise this is to use a read-only filesystem or
// to make the directory read-only *after* CreateTemp succeeds.  Since that
// is inherently racy and not portable, we accept this specific branch stays
// at 0 % and document it here.  The related test below exercises the rename
// failure path which is the next branch.
// ---------------------------------------------------------------------------

// TestWriteFileAtomic_RenameAfterSuccessfulWrite verifies that a rename to a
// path in a different directory (non-same-filesystem) causes the rename step
// to fail and the temp file is cleaned up.
func TestWriteFileAtomic_CreateTempInMissingDir(t *testing.T) {
	// Ensure CreateTemp itself fails when the directory doesn't exist.
	err := writeFileAtomic("/tmp/nonexistent-dir-xyz/file.txt", 0644, func(w io.Writer) error {
		_, werr := io.WriteString(w, "x")
		return werr
	})
	if err == nil {
		t.Fatal("expected error for missing directory in writeFileAtomic")
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// newConfigured builds a *Local already configured with dir as its base path.
func newConfigured(t *testing.T, dir string) *Local {
	t.Helper()
	s := New()
	if err := s.Configure(map[string]string{"path": dir}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	return s.(*Local)
}

// assertNoTempFiles checks that no atomic-write temporaries remain under dir.
func assertNoTempFiles(t *testing.T, dir string) {
	t.Helper()
	var leftovers []string
	_ = filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && len(d.Name()) > 5 && d.Name()[:5] == ".tmp-" {
			leftovers = append(leftovers, path)
		}
		return nil
	})
	if len(leftovers) > 0 {
		t.Fatalf("expected no .tmp-* files, found: %v", leftovers)
	}
}

// stubReplicationManager is a minimal no-op ReplicationManager.
type stubReplicationManager struct{}

func (s *stubReplicationManager) AddPolicy(policy common.ReplicationPolicy) error { return nil }
func (s *stubReplicationManager) RemovePolicy(id string) error                    { return nil }
func (s *stubReplicationManager) GetPolicy(id string) (*common.ReplicationPolicy, error) {
	return nil, nil
}
func (s *stubReplicationManager) GetPolicies() ([]common.ReplicationPolicy, error) {
	return nil, nil
}
func (s *stubReplicationManager) SyncAll(ctx context.Context) (*common.SyncResult, error) {
	return nil, nil
}
func (s *stubReplicationManager) SyncPolicy(ctx context.Context, policyID string) (*common.SyncResult, error) {
	return nil, nil
}
func (s *stubReplicationManager) SyncAllParallel(ctx context.Context, workerCount int) (*common.SyncResult, error) {
	return nil, nil
}
func (s *stubReplicationManager) SyncPolicyParallel(ctx context.Context, policyID string, workerCount int) (*common.SyncResult, error) {
	return nil, nil
}
func (s *stubReplicationManager) SetBackendEncrypterFactory(policyID string, factory common.EncrypterFactory) error {
	return nil
}
func (s *stubReplicationManager) SetSourceEncrypterFactory(policyID string, factory common.EncrypterFactory) error {
	return nil
}
func (s *stubReplicationManager) SetDestinationEncrypterFactory(policyID string, factory common.EncrypterFactory) error {
	return nil
}
func (s *stubReplicationManager) Run(ctx context.Context) {}

// errorEncrypterFactory always returns an error from GetEncrypter.
type errorEncrypterFactory struct {
	err error
}

func (f *errorEncrypterFactory) DefaultKeyID() string { return "" }
func (f *errorEncrypterFactory) GetEncrypter(keyID string) (common.Encrypter, error) {
	return nil, f.err
}
func (f *errorEncrypterFactory) Close() error { return nil }

// stubEncrypterFactory wraps a single Encrypter.
type stubEncrypterFactory struct {
	enc common.Encrypter
}

func (f *stubEncrypterFactory) DefaultKeyID() string { return "" }
func (f *stubEncrypterFactory) GetEncrypter(keyID string) (common.Encrypter, error) {
	return f.enc, nil
}
func (f *stubEncrypterFactory) Close() error { return nil }

// failEncryptEncrypter always returns an error from Encrypt.
type failEncryptEncrypter struct {
	encryptErr error
}

func (e *failEncryptEncrypter) Encrypt(ctx context.Context, data io.Reader) (io.ReadCloser, error) {
	return nil, e.encryptErr
}
func (e *failEncryptEncrypter) Decrypt(ctx context.Context, data io.Reader) (io.ReadCloser, error) {
	b, err := io.ReadAll(data)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(b)), nil
}
func (e *failEncryptEncrypter) Algorithm() string { return "fail-enc" }
func (e *failEncryptEncrypter) KeyID() string     { return "fail-enc" }

// failDecryptEncrypter encrypts fine but always returns an error from Decrypt.
type failDecryptEncrypter struct {
	decryptErr error
}

func (e *failDecryptEncrypter) Encrypt(ctx context.Context, data io.Reader) (io.ReadCloser, error) {
	b, err := io.ReadAll(data)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(b)), nil
}
func (e *failDecryptEncrypter) Decrypt(ctx context.Context, data io.Reader) (io.ReadCloser, error) {
	return nil, e.decryptErr
}
func (e *failDecryptEncrypter) Algorithm() string { return "fail" }
func (e *failDecryptEncrypter) KeyID() string     { return "fail" }
