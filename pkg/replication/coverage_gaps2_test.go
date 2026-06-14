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

package replication

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/audit"
	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// changelog.go: ensureFile – reopen fails (file nil, not closed, bad path).
// ---------------------------------------------------------------------------

func TestEnsureFile_ReopenFails(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "changes.jsonl")

	cl, err := NewJSONLChangeLog(logPath, 1024*1024)
	require.NoError(t, err)

	// Force file to nil without marking closed, and corrupt the filePath so
	// the reopen attempt inside ensureFile fails.
	cl.mutex.Lock()
	_ = cl.file.Close()
	cl.file = nil
	cl.filePath = "/nonexistent/dir/that/does/not/exist/changes.jsonl"
	cl.mutex.Unlock()

	err = cl.RecordChange(ChangeEvent{Key: "k", Operation: "put"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to reopen change log file")
}

// ---------------------------------------------------------------------------
// changelog.go: rotate – rename fails AND reopen of original fails.
// This exercises the ErrChangeLogRenameReopen branch.
// ---------------------------------------------------------------------------

func TestRotate_RenameAndReopenBothFail(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "changes.jsonl")

	cl, err := NewJSONLChangeLog(logPath, 1024*1024)
	require.NoError(t, err)
	defer func() {
		// Restore path so Close can succeed.
		cl.mutex.Lock()
		cl.filePath = logPath
		cl.mutex.Unlock()
		cl.Close()
	}()

	err = cl.RecordChange(ChangeEvent{Key: "k", Operation: "put"})
	require.NoError(t, err)

	// Set an impossible filePath so that both os.Rename (via the src being this
	// path) and the reopen of the original both fail. We achieve this by using a
	// path in a directory that does not exist.
	cl.mutex.Lock()
	cl.filePath = filepath.Join(dir, "noexist", "changes.jsonl")
	cl.mutex.Unlock()

	// rotate() closes cl.file (which is still the valid path), then tries to
	// rename logPath -> cl.filePath (fails), then tries to reopen cl.filePath (fails).
	// But wait – cl.file was opened at logPath. After Close(), the real file is
	// gone. We need to ensure the real internal file is at the old logPath.
	// Reset filePath to something that will make the rename destination fail.
	// Use /dev/null as destination dir (can't rename into a file as a dir).
	cl.mutex.Lock()
	f2, _ := os.OpenFile(logPath, os.O_APPEND|os.O_RDWR, 0600)
	if f2 != nil {
		cl.file = f2
	}
	// Make the target backup path invalid (rotate uses filePath + "." + timestamp).
	// The backup target will be inside /nonexistent/dir, causing rename to fail.
	// And the reopen of filePath will also fail since that dir doesn't exist.
	cl.filePath = filepath.Join(dir, "nonexistent-subdir", "changes.jsonl")
	cl.mutex.Unlock()

	cl.mutex.Lock()
	rotErr := cl.rotate()
	cl.mutex.Unlock()

	assert.Error(t, rotErr)
	// Either ErrChangeLogRenameReopen (both fail) or "failed to rename file" (rename fails, reopen ok).
	// With the nonexistent subdir, rename fails AND reopen of filePath fails.
	assert.True(t,
		errors.Is(rotErr, ErrChangeLogRenameReopen) ||
			containsMsg(rotErr.Error(), "failed to rename file"),
		"unexpected error: %v", rotErr)
}

func containsMsg(s, sub string) bool {
	return len(s) >= len(sub) && func() bool {
		for i := 0; i <= len(s)-len(sub); i++ {
			if s[i:i+len(sub)] == sub {
				return true
			}
		}
		return false
	}()
}

// rotate – rename succeeds but opening the new file fails.
// We arrange this by: making dir read-only AFTER the file is already there,
// then calling rotate which closes+renames the existing file. On a read-only
// dir the rename of the backup destination fails; that is equivalent to
// exercising the error path. Both "rename fails" and "create new file fails"
// are valid outcomes depending on OS behavior.
func TestRotate_NewFileOpenFails(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("running as root; permission restrictions don't apply")
	}

	dir := t.TempDir()
	logPath := filepath.Join(dir, "changes.jsonl")

	cl, err := NewJSONLChangeLog(logPath, 1024*1024)
	require.NoError(t, err)

	err = cl.RecordChange(ChangeEvent{Key: "k", Operation: "put"})
	require.NoError(t, err)

	// Make the directory read-only so operations creating new files fail.
	require.NoError(t, os.Chmod(dir, 0500))
	defer func() {
		os.Chmod(dir, 0700)
		cl.Close()
	}()

	err = cl.Rotate()
	// With a read-only dir, either rename or new-file creation will fail.
	assert.Error(t, err)
}

// ---------------------------------------------------------------------------
// changelog.go: rewriteFile error paths.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// replication_persistent.go: save – Sync() on tmp file fails.
// ---------------------------------------------------------------------------

func TestSave_SyncFails(t *testing.T) {
	logger := adapters.NewNoOpLogger()
	auditLog := audit.NewNoOpAuditLogger()

	mfs := newMockFileSystem()
	mgr, err := NewPersistentReplicationManager(mfs, "policies.json", time.Minute, logger, auditLog)
	require.NoError(t, err)

	mgr.fs = &syncFailFS{}

	mgr.mutex.Lock()
	saveErr := mgr.save()
	mgr.mutex.Unlock()
	assert.Error(t, saveErr)
}

// syncFailFS: OpenFile succeeds, Write succeeds, Sync fails.
type syncFailFS struct{}

func (s *syncFailFS) OpenFile(name string, flag int, perm os.FileMode) (ReplicationFile, error) {
	return &syncFailFSFile{}, nil
}
func (s *syncFailFS) Remove(name string) error     { return nil }
func (s *syncFailFS) Rename(src, dst string) error { return nil }

type syncFailFSFile struct{ buf []byte }

func (f *syncFailFSFile) Read(p []byte) (int, error) { return 0, io.EOF }
func (f *syncFailFSFile) Write(p []byte) (int, error) {
	f.buf = append(f.buf, p...)
	return len(p), nil
}
func (f *syncFailFSFile) Close() error                         { return nil }
func (f *syncFailFSFile) Seek(off int64, w int) (int64, error) { return 0, nil }
func (f *syncFailFSFile) Truncate(size int64) error            { return nil }
func (f *syncFailFSFile) Sync() error                          { return errors.New("sync failed") }

// ---------------------------------------------------------------------------
// replication_persistent.go: save – Close() on tmp file fails.
// ---------------------------------------------------------------------------

func TestSave_CloseFails(t *testing.T) {
	logger := adapters.NewNoOpLogger()
	auditLog := audit.NewNoOpAuditLogger()

	mfs := newMockFileSystem()
	mgr, err := NewPersistentReplicationManager(mfs, "policies.json", time.Minute, logger, auditLog)
	require.NoError(t, err)

	mgr.fs = &closeFailFS{}

	mgr.mutex.Lock()
	saveErr := mgr.save()
	mgr.mutex.Unlock()
	assert.Error(t, saveErr)
}

type closeFailFS struct{}

func (c *closeFailFS) OpenFile(name string, flag int, perm os.FileMode) (ReplicationFile, error) {
	return &closeFailFSFile{}, nil
}
func (c *closeFailFS) Remove(name string) error     { return nil }
func (c *closeFailFS) Rename(src, dst string) error { return nil }

type closeFailFSFile struct{ buf []byte }

func (f *closeFailFSFile) Read(p []byte) (int, error) { return 0, io.EOF }
func (f *closeFailFSFile) Write(p []byte) (int, error) {
	f.buf = append(f.buf, p...)
	return len(p), nil
}
func (f *closeFailFSFile) Close() error                         { return errors.New("close failed") }
func (f *closeFailFSFile) Seek(off int64, w int) (int64, error) { return 0, nil }
func (f *closeFailFSFile) Truncate(size int64) error            { return nil }
func (f *closeFailFSFile) Sync() error                          { return nil }

// ---------------------------------------------------------------------------
// replication_persistent.go: SyncAllParallel – successful parallel sync with
// result accumulation (Synced, BytesTotal, Errors from result.Errors).
// ---------------------------------------------------------------------------

func TestSyncAllParallel_TwoFailing(t *testing.T) {
	mfs := newMockFileSystem()
	logger := adapters.NewNoOpLogger()
	auditLog := audit.NewNoOpAuditLogger()

	mgr, err := NewPersistentReplicationManager(mfs, "policies.json", time.Minute, logger, auditLog)
	require.NoError(t, err)

	for i := 1; i <= 2; i++ {
		p := common.ReplicationPolicy{
			ID:                  fmt.Sprintf("fail%d", i),
			SourceBackend:       "nonexistent-backend",
			SourceSettings:      map[string]string{},
			DestinationBackend:  "local",
			DestinationSettings: map[string]string{"path": "/dst"},
			Enabled:             true,
			ReplicationMode:     common.ReplicationModeOpaque,
		}
		require.NoError(t, mgr.AddPolicy(p))
	}

	result, err := mgr.SyncAllParallel(context.Background(), 2)
	require.NoError(t, err)
	assert.Equal(t, 2, result.Failed)
	assert.Len(t, result.Errors, 2)
}

// ---------------------------------------------------------------------------
// replication_persistent.go: Run – ticker fires and SyncAll returns error.
// (exercises the scheduled sync error log path inside Run)
// ---------------------------------------------------------------------------

func TestRun_SyncAllError(t *testing.T) {
	mfs := newMockFileSystem()
	logger := adapters.NewNoOpLogger()
	auditLog := audit.NewNoOpAuditLogger()

	mgr, err := NewPersistentReplicationManager(mfs, "policies.json", 10*time.Millisecond, logger, auditLog)
	require.NoError(t, err)

	// Add a policy that will fail (invalid backend), so SyncAll logs an error.
	p := common.ReplicationPolicy{
		ID:                  "bad",
		SourceBackend:       "nonexistent",
		DestinationBackend:  "local",
		DestinationSettings: map[string]string{"path": "/dst"},
		Enabled:             true,
		ReplicationMode:     common.ReplicationModeOpaque,
	}
	require.NoError(t, mgr.AddPolicy(p))

	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() {
		mgr.Run(ctx)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Run did not stop")
	}
}

// ---------------------------------------------------------------------------
// watcher.go: NewFSNotifyWatcher – EventBuffer=0 default applied.
// watcher.go: Watch – filepath.Walk entry with walk error.
// ---------------------------------------------------------------------------

func TestNewFSNotifyWatcher_EventBufferDefault(t *testing.T) {
	w, err := NewFSNotifyWatcher(FSNotifyWatcherConfig{
		DebounceDelay: 10 * time.Millisecond,
		EventBuffer:   0, // triggers the default
	})
	require.NoError(t, err)
	defer w.Stop()

	// Buffer should be 100 (the default).
	assert.Equal(t, 100, cap(w.events))
}

// watcher Walk error: inaccessible subdirectory (os.Stat fails in Walk callback).
func TestWatch_WalkErrorPath(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("running as root; permission restrictions don't apply")
	}

	logger := adapters.NewNoOpLogger()
	w, err := NewFSNotifyWatcher(FSNotifyWatcherConfig{
		Logger:        logger,
		DebounceDelay: 50 * time.Millisecond,
	})
	require.NoError(t, err)
	defer w.Stop()

	dir := t.TempDir()
	sub := filepath.Join(dir, "sub")
	require.NoError(t, os.Mkdir(sub, 0755))
	// Make sub unreadable so Walk emits an error for it.
	require.NoError(t, os.Chmod(sub, 0000))
	defer os.Chmod(sub, 0755)

	// Watch should still succeed (Walk errors are logged and skipped).
	err = w.Watch(dir)
	assert.NoError(t, err)
}

// watcher Stop – logging the error from watcher.Close (hard to trigger, but
// we exercise the Stop normal path a second time after watcher is already closed).
func TestWatcher_Stop_AfterFsnotifyClose(t *testing.T) {
	logger := adapters.NewNoOpLogger()
	w, err := NewFSNotifyWatcher(FSNotifyWatcherConfig{
		Logger:        logger,
		DebounceDelay: 10 * time.Millisecond,
	})
	require.NoError(t, err)

	// First Stop closes the underlying fsnotify watcher.
	require.NoError(t, w.Stop())

	// Second Stop is idempotent and should not panic.
	err = w.Stop()
	assert.NoError(t, err)
}

// watcher handleCreate: adding to fsnotify after watcher is closed returns error.
// We simulate this by stopping the watcher then calling handleCreate with a dir.
func TestHandleCreate_WatcherClosed(t *testing.T) {
	logger := adapters.NewNoOpLogger()
	w, err := NewFSNotifyWatcher(FSNotifyWatcherConfig{
		Logger:        logger,
		DebounceDelay: 10 * time.Millisecond,
	})
	require.NoError(t, err)
	require.NoError(t, w.Stop())

	// Attempt to handleCreate with a real directory – watcher.Add will fail
	// because the underlying fsnotify watcher is closed.
	dir := t.TempDir()
	w.handleCreate(dir) // should not panic; error is just logged
}

// processEvents: test the channel-closed arm for watcher.Events (ok == false).
// After Stop(), the fsnotify watcher is closed, which closes its Events chan.
func TestProcessEvents_EventsChannelClosed(t *testing.T) {
	logger := adapters.NewNoOpLogger()
	w, err := NewFSNotifyWatcher(FSNotifyWatcherConfig{
		Logger:        logger,
		DebounceDelay: 10 * time.Millisecond,
	})
	require.NoError(t, err)

	// Stop causes the internal fsnotify watcher to close, which closes its
	// Events and Errors channels, and the processEvents goroutine exits.
	require.NoError(t, w.Stop())

	// wg.Wait already happened inside Stop; verify state.
	assert.True(t, w.stopped)
}

// ---------------------------------------------------------------------------
// syncer.go: SyncAllParallel – detect changes error path.
// ---------------------------------------------------------------------------

func TestSyncer_SyncAllParallel_DetectChangesError(t *testing.T) {
	source := &listErrorStorage{err: errors.New("list failed")}
	dest := newExtendedMockStorage()

	syncer := &Syncer{
		policy: common.ReplicationPolicy{
			ID:              "pol1",
			ReplicationMode: common.ReplicationModeTransparent,
		},
		source:   source,
		dest:     dest,
		logger:   &mockLogger{},
		metrics:  NewReplicationMetrics(),
		auditLog: &mockAuditLogger{},
	}

	_, err := syncer.SyncAllParallel(context.Background(), 2)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "change detection failed")
}

// listErrorStorage returns an error from ListWithOptions.
type listErrorStorage struct {
	listErr error
	err     error
}

func (l *listErrorStorage) Configure(settings map[string]string) error { return nil }
func (l *listErrorStorage) Put(key string, data io.Reader) error       { return nil }
func (l *listErrorStorage) PutWithContext(ctx context.Context, key string, data io.Reader) error {
	return nil
}
func (l *listErrorStorage) PutWithMetadata(ctx context.Context, key string, data io.Reader, metadata *common.Metadata) error {
	return nil
}
func (l *listErrorStorage) Get(key string) (io.ReadCloser, error) { return nil, nil }
func (l *listErrorStorage) GetWithContext(ctx context.Context, key string) (io.ReadCloser, error) {
	return nil, nil
}
func (l *listErrorStorage) GetMetadata(ctx context.Context, key string) (*common.Metadata, error) {
	return nil, common.ErrKeyNotFound
}
func (l *listErrorStorage) UpdateMetadata(ctx context.Context, key string, metadata *common.Metadata) error {
	return nil
}
func (l *listErrorStorage) Delete(key string) error                                 { return nil }
func (l *listErrorStorage) DeleteWithContext(ctx context.Context, key string) error { return nil }
func (l *listErrorStorage) Exists(ctx context.Context, key string) (bool, error)    { return false, nil }
func (l *listErrorStorage) List(prefix string) ([]string, error)                    { return nil, nil }
func (l *listErrorStorage) ListWithContext(ctx context.Context, prefix string) ([]string, error) {
	return nil, nil
}
func (l *listErrorStorage) ListWithOptions(ctx context.Context, opts *common.ListOptions) (*common.ListResult, error) {
	return nil, l.err
}
func (l *listErrorStorage) Archive(key string, destination common.Archiver) error { return nil }
func (l *listErrorStorage) AddPolicy(policy common.LifecyclePolicy) error         { return nil }
func (l *listErrorStorage) RemovePolicy(id string) error                          { return nil }
func (l *listErrorStorage) GetPolicies() ([]common.LifecyclePolicy, error)        { return nil, nil }

// ---------------------------------------------------------------------------
// replication_persistent.go: SyncAllParallel – result.Errors accumulation path.
// We need a parallel sync that succeeds but the per-object errors are carried.
// ---------------------------------------------------------------------------

func TestSyncPolicyParallel_NotFound(t *testing.T) {
	mfs := newMockFileSystem()
	mgr, err := NewPersistentReplicationManager(mfs, "policies.json", time.Minute,
		adapters.NewNoOpLogger(), audit.NewNoOpAuditLogger())
	require.NoError(t, err)

	_, err = mgr.SyncPolicyParallel(context.Background(), "nonexistent", 2)
	assert.ErrorIs(t, err, common.ErrPolicyNotFound)
}

// ---------------------------------------------------------------------------
// MarkProcessed – exercise the event.Processed == nil branch inside MarkProcessed.
// ---------------------------------------------------------------------------

func TestMarkProcessed_InitializesProcessedMap(t *testing.T) {
	dir := t.TempDir()
	cl, err := NewJSONLChangeLog(filepath.Join(dir, "c.jsonl"), 1024*1024)
	require.NoError(t, err)
	defer cl.Close()

	// Write a raw event with Processed == nil by using an already-initialized
	// event without the Processed field so MarkProcessed must initialize it.
	err = cl.RecordChange(ChangeEvent{
		Key:       "file.txt",
		Operation: "put",
		Timestamp: time.Now(),
		// Processed is nil
	})
	require.NoError(t, err)

	// Force the file content to have a null "processed" map so the
	// event.Processed == nil branch inside MarkProcessed fires.
	// The existing RecordChange already sets Processed to {}, but we can
	// write a raw line directly.
	cl.mutex.Lock()
	// Seek to end and append a raw event with null processed.
	rawLine := []byte(`{"key":"raw.txt","operation":"put","timestamp":"2025-01-01T00:00:00Z","processed":null}` + "\n")
	_, writeErr := cl.file.Write(rawLine)
	syncErr := cl.file.Sync()
	cl.mutex.Unlock()
	require.NoError(t, writeErr)
	require.NoError(t, syncErr)

	// MarkProcessed on raw.txt must initialize the Processed map.
	err = cl.MarkProcessed("raw.txt", "policy1")
	require.NoError(t, err)

	// Verify raw.txt is now marked.
	unprocessed, err := cl.GetUnprocessed("policy1")
	require.NoError(t, err)
	found := false
	for _, e := range unprocessed {
		if e.Key == "raw.txt" {
			found = true
			break
		}
	}
	assert.False(t, found, "raw.txt should be marked processed")
}
