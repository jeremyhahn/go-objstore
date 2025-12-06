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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFSNotifyWatcher_Watch(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(t *testing.T) string
		wantErr bool
		errType error
	}{
		{
			name: "watch valid directory",
			setup: func(t *testing.T) string {
				dir := t.TempDir()
				return dir
			},
			wantErr: false,
		},
		{
			name: "watch non-existent directory",
			setup: func(t *testing.T) string {
				return "/non/existent/path"
			},
			wantErr: true,
		},
		{
			name: "watch stopped watcher",
			setup: func(t *testing.T) string {
				return t.TempDir()
			},
			wantErr: true,
			errType: ErrWatcherStopped,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := adapters.NewNoOpLogger()
			watcher, err := NewFSNotifyWatcher(FSNotifyWatcherConfig{
				Logger: logger,
			})
			require.NoError(t, err)
			defer watcher.Stop()

			path := tt.setup(t)

			if tt.errType == ErrWatcherStopped {
				// Stop the watcher first
				require.NoError(t, watcher.Stop())
			}

			err = watcher.Watch(path)
			if tt.wantErr {
				assert.Error(t, err)
				if tt.errType != nil {
					assert.ErrorIs(t, err, tt.errType)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestFSNotifyWatcher_CreateEvent(t *testing.T) {
	logger := adapters.NewNoOpLogger()
	watcher, err := NewFSNotifyWatcher(FSNotifyWatcherConfig{
		Logger:        logger,
		DebounceDelay: 50 * time.Millisecond,
	})
	require.NoError(t, err)
	defer watcher.Stop()

	dir := t.TempDir()
	err = watcher.Watch(dir)
	require.NoError(t, err)

	// Create a file
	testFile := filepath.Join(dir, "test.txt")
	err = os.WriteFile(testFile, []byte("test content"), 0644)
	require.NoError(t, err)

	// Wait for event
	select {
	case event := <-watcher.Events():
		assert.Equal(t, testFile, event.Path)
		assert.Equal(t, "put", event.Operation)
		assert.False(t, event.Timestamp.IsZero())
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for create event")
	}
}

func TestFSNotifyWatcher_WriteEvent(t *testing.T) {
	logger := adapters.NewNoOpLogger()
	watcher, err := NewFSNotifyWatcher(FSNotifyWatcherConfig{
		Logger:        logger,
		DebounceDelay: 50 * time.Millisecond,
	})
	require.NoError(t, err)
	defer watcher.Stop()

	dir := t.TempDir()
	testFile := filepath.Join(dir, "test.txt")

	// Create initial file
	err = os.WriteFile(testFile, []byte("initial"), 0644)
	require.NoError(t, err)

	// Now start watching
	err = watcher.Watch(dir)
	require.NoError(t, err)

	// Modify the file
	err = os.WriteFile(testFile, []byte("modified content"), 0644)
	require.NoError(t, err)

	// Wait for event
	select {
	case event := <-watcher.Events():
		assert.Equal(t, testFile, event.Path)
		assert.Equal(t, "put", event.Operation)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for write event")
	}
}

func TestFSNotifyWatcher_DeleteEvent(t *testing.T) {
	logger := adapters.NewNoOpLogger()
	watcher, err := NewFSNotifyWatcher(FSNotifyWatcherConfig{
		Logger:        logger,
		DebounceDelay: 50 * time.Millisecond,
	})
	require.NoError(t, err)
	defer watcher.Stop()

	dir := t.TempDir()
	testFile := filepath.Join(dir, "test.txt")

	// Create file
	err = os.WriteFile(testFile, []byte("test"), 0644)
	require.NoError(t, err)

	// Start watching
	err = watcher.Watch(dir)
	require.NoError(t, err)

	// Delete the file
	err = os.Remove(testFile)
	require.NoError(t, err)

	// Wait for event
	select {
	case event := <-watcher.Events():
		assert.Equal(t, testFile, event.Path)
		assert.Equal(t, "delete", event.Operation)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for delete event")
	}
}

func TestFSNotifyWatcher_RenameEvent(t *testing.T) {
	logger := adapters.NewNoOpLogger()
	watcher, err := NewFSNotifyWatcher(FSNotifyWatcherConfig{
		Logger:        logger,
		DebounceDelay: 50 * time.Millisecond,
	})
	require.NoError(t, err)
	defer watcher.Stop()

	dir := t.TempDir()
	testFile := filepath.Join(dir, "test.txt")
	renamedFile := filepath.Join(dir, "renamed.txt")

	// Create file
	err = os.WriteFile(testFile, []byte("test"), 0644)
	require.NoError(t, err)

	// Start watching
	err = watcher.Watch(dir)
	require.NoError(t, err)

	// Rename the file
	err = os.Rename(testFile, renamedFile)
	require.NoError(t, err)

	// We expect two events: delete for old name, create for new name
	eventsReceived := 0
	timeout := time.After(2 * time.Second)

	for eventsReceived < 2 {
		select {
		case event := <-watcher.Events():
			eventsReceived++
			// Either delete or put operation is expected
			assert.Contains(t, []string{"delete", "put"}, event.Operation)
		case <-timeout:
			t.Fatalf("timeout waiting for rename events, received %d events", eventsReceived)
		}
	}
}

func TestFSNotifyWatcher_RecursiveWatch(t *testing.T) {
	logger := adapters.NewNoOpLogger()
	watcher, err := NewFSNotifyWatcher(FSNotifyWatcherConfig{
		Logger:        logger,
		DebounceDelay: 50 * time.Millisecond,
	})
	require.NoError(t, err)
	defer watcher.Stop()

	dir := t.TempDir()

	// Create nested directory structure
	subDir := filepath.Join(dir, "subdir")
	err = os.MkdirAll(subDir, 0755)
	require.NoError(t, err)

	nestedDir := filepath.Join(subDir, "nested")
	err = os.MkdirAll(nestedDir, 0755)
	require.NoError(t, err)

	// Start watching
	err = watcher.Watch(dir)
	require.NoError(t, err)

	// Create file in nested directory
	testFile := filepath.Join(nestedDir, "test.txt")
	err = os.WriteFile(testFile, []byte("test"), 0644)
	require.NoError(t, err)

	// Wait for event
	select {
	case event := <-watcher.Events():
		assert.Equal(t, testFile, event.Path)
		assert.Equal(t, "put", event.Operation)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for event in nested directory")
	}
}

func TestFSNotifyWatcher_NewDirectoryWatch(t *testing.T) {
	logger := adapters.NewNoOpLogger()
	watcher, err := NewFSNotifyWatcher(FSNotifyWatcherConfig{
		Logger:        logger,
		DebounceDelay: 50 * time.Millisecond,
	})
	require.NoError(t, err)
	defer watcher.Stop()

	dir := t.TempDir()
	err = watcher.Watch(dir)
	require.NoError(t, err)

	// Create a new subdirectory while watching
	newDir := filepath.Join(dir, "newdir")
	err = os.MkdirAll(newDir, 0755)
	require.NoError(t, err)

	// Give the watcher time to add the new directory
	time.Sleep(200 * time.Millisecond)

	// Create file in new directory
	testFile := filepath.Join(newDir, "test.txt")
	err = os.WriteFile(testFile, []byte("test"), 0644)
	require.NoError(t, err)

	// Wait for event
	foundFileEvent := false
	timeout := time.After(2 * time.Second)

	for !foundFileEvent {
		select {
		case event := <-watcher.Events():
			if event.Path == testFile {
				foundFileEvent = true
				assert.Equal(t, "put", event.Operation)
			}
		case <-timeout:
			t.Fatal("timeout waiting for event in new directory")
		}
	}
}

func TestFSNotifyWatcher_Debouncing(t *testing.T) {
	logger := adapters.NewNoOpLogger()
	debounceDelay := 200 * time.Millisecond

	watcher, err := NewFSNotifyWatcher(FSNotifyWatcherConfig{
		Logger:        logger,
		DebounceDelay: debounceDelay,
	})
	require.NoError(t, err)
	defer watcher.Stop()

	dir := t.TempDir()
	testFile := filepath.Join(dir, "test.txt")

	err = watcher.Watch(dir)
	require.NoError(t, err)

	// Write to file multiple times rapidly
	numWrites := 5
	for i := 0; i < numWrites; i++ {
		err = os.WriteFile(testFile, []byte("content"), 0644)
		require.NoError(t, err)
		time.Sleep(20 * time.Millisecond) // Less than debounce delay
	}

	// Count events received
	eventsReceived := 0
	timeout := time.After(1 * time.Second)

	for {
		select {
		case event := <-watcher.Events():
			if event.Path == testFile {
				eventsReceived++
			}
		case <-timeout:
			// Due to debouncing, we should receive fewer events than writes
			assert.Less(t, eventsReceived, numWrites,
				"Expected debouncing to reduce events from %d to fewer", numWrites)
			return
		}
	}
}

func TestFSNotifyWatcher_Stop(t *testing.T) {
	logger := adapters.NewNoOpLogger()
	watcher, err := NewFSNotifyWatcher(FSNotifyWatcherConfig{
		Logger: logger,
	})
	require.NoError(t, err)

	dir := t.TempDir()
	err = watcher.Watch(dir)
	require.NoError(t, err)

	// Stop the watcher
	err = watcher.Stop()
	assert.NoError(t, err)

	// Events channel should be closed
	_, ok := <-watcher.Events()
	assert.False(t, ok, "Events channel should be closed after Stop()")

	// Calling Stop() again should be safe
	err = watcher.Stop()
	assert.NoError(t, err)

	// Watching after stop should fail
	err = watcher.Watch(t.TempDir())
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrWatcherStopped)
}

func TestFSNotifyWatcher_IgnoreHiddenFiles(t *testing.T) {
	logger := adapters.NewNoOpLogger()
	watcher, err := NewFSNotifyWatcher(FSNotifyWatcherConfig{
		Logger:        logger,
		DebounceDelay: 50 * time.Millisecond,
	})
	require.NoError(t, err)
	defer watcher.Stop()

	dir := t.TempDir()
	err = watcher.Watch(dir)
	require.NoError(t, err)

	// Create hidden file
	hiddenFile := filepath.Join(dir, ".hidden")
	err = os.WriteFile(hiddenFile, []byte("hidden"), 0644)
	require.NoError(t, err)

	// Create normal file
	normalFile := filepath.Join(dir, "normal.txt")
	err = os.WriteFile(normalFile, []byte("normal"), 0644)
	require.NoError(t, err)

	// Should only receive event for normal file
	select {
	case event := <-watcher.Events():
		assert.Equal(t, normalFile, event.Path)
		assert.NotEqual(t, hiddenFile, event.Path)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for event")
	}

	// Ensure no event for hidden file
	select {
	case event := <-watcher.Events():
		t.Fatalf("unexpected event for hidden file: %+v", event)
	case <-time.After(200 * time.Millisecond):
		// Expected - no event for hidden file
	}
}

func TestFSNotifyWatcher_IgnoreMetadataFiles(t *testing.T) {
	logger := adapters.NewNoOpLogger()
	watcher, err := NewFSNotifyWatcher(FSNotifyWatcherConfig{
		Logger:        logger,
		DebounceDelay: 50 * time.Millisecond,
	})
	require.NoError(t, err)
	defer watcher.Stop()

	dir := t.TempDir()
	err = watcher.Watch(dir)
	require.NoError(t, err)

	// Create metadata file
	metadataFile := filepath.Join(dir, "file.metadata.json")
	err = os.WriteFile(metadataFile, []byte("{}"), 0644)
	require.NoError(t, err)

	// Create normal file
	normalFile := filepath.Join(dir, "file.txt")
	err = os.WriteFile(normalFile, []byte("content"), 0644)
	require.NoError(t, err)

	// Should only receive event for normal file
	select {
	case event := <-watcher.Events():
		assert.Equal(t, normalFile, event.Path)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for event")
	}

	// Ensure no event for metadata file
	select {
	case event := <-watcher.Events():
		t.Fatalf("unexpected event for metadata file: %+v", event)
	case <-time.After(200 * time.Millisecond):
		// Expected - no event for metadata file
	}
}

func TestFSNotifyWatcher_IgnoreTempFiles(t *testing.T) {
	logger := adapters.NewNoOpLogger()
	watcher, err := NewFSNotifyWatcher(FSNotifyWatcherConfig{
		Logger:        logger,
		DebounceDelay: 50 * time.Millisecond,
	})
	require.NoError(t, err)
	defer watcher.Stop()

	dir := t.TempDir()
	err = watcher.Watch(dir)
	require.NoError(t, err)

	// Create temp files
	tempFiles := []string{
		filepath.Join(dir, "file~"),
		filepath.Join(dir, "file.tmp"),
	}

	for _, tempFile := range tempFiles {
		err = os.WriteFile(tempFile, []byte("temp"), 0644)
		require.NoError(t, err)
	}

	// Create normal file
	normalFile := filepath.Join(dir, "normal.txt")
	err = os.WriteFile(normalFile, []byte("content"), 0644)
	require.NoError(t, err)

	// Should only receive event for normal file
	select {
	case event := <-watcher.Events():
		assert.Equal(t, normalFile, event.Path)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for event")
	}

	// Ensure no events for temp files
	select {
	case event := <-watcher.Events():
		t.Fatalf("unexpected event for temp file: %+v", event)
	case <-time.After(200 * time.Millisecond):
		// Expected - no events for temp files
	}
}

func TestFSNotifyWatcher_MultipleWatchers(t *testing.T) {
	logger := adapters.NewNoOpLogger()

	dir1 := t.TempDir()
	dir2 := t.TempDir()

	watcher1, err := NewFSNotifyWatcher(FSNotifyWatcherConfig{
		Logger:        logger,
		DebounceDelay: 50 * time.Millisecond,
	})
	require.NoError(t, err)
	defer watcher1.Stop()

	watcher2, err := NewFSNotifyWatcher(FSNotifyWatcherConfig{
		Logger:        logger,
		DebounceDelay: 50 * time.Millisecond,
	})
	require.NoError(t, err)
	defer watcher2.Stop()

	err = watcher1.Watch(dir1)
	require.NoError(t, err)

	err = watcher2.Watch(dir2)
	require.NoError(t, err)

	// Create files in both directories
	file1 := filepath.Join(dir1, "file1.txt")
	file2 := filepath.Join(dir2, "file2.txt")

	err = os.WriteFile(file1, []byte("content1"), 0644)
	require.NoError(t, err)

	err = os.WriteFile(file2, []byte("content2"), 0644)
	require.NoError(t, err)

	// Each watcher should receive only its own event
	select {
	case event := <-watcher1.Events():
		assert.Equal(t, file1, event.Path)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for watcher1 event")
	}

	select {
	case event := <-watcher2.Events():
		assert.Equal(t, file2, event.Path)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for watcher2 event")
	}
}

func TestFSNotifyWatcher_WatchSamePath(t *testing.T) {
	logger := adapters.NewNoOpLogger()
	watcher, err := NewFSNotifyWatcher(FSNotifyWatcherConfig{
		Logger: logger,
	})
	require.NoError(t, err)
	defer watcher.Stop()

	dir := t.TempDir()

	// Watch the same path twice
	err = watcher.Watch(dir)
	require.NoError(t, err)

	err = watcher.Watch(dir)
	require.NoError(t, err) // Should not error
}

func TestFSNotifyWatcher_EventBuffer(t *testing.T) {
	logger := adapters.NewNoOpLogger()

	// Small buffer to test overflow
	watcher, err := NewFSNotifyWatcher(FSNotifyWatcherConfig{
		Logger:        logger,
		DebounceDelay: 500 * time.Millisecond, // Long debounce
		EventBuffer:   2,                      // Small buffer
	})
	require.NoError(t, err)
	defer watcher.Stop()

	dir := t.TempDir()
	err = watcher.Watch(dir)
	require.NoError(t, err)

	// Create many files quickly
	for i := 0; i < 10; i++ {
		file := filepath.Join(dir, fmt.Sprintf("file%d.txt", i))
		err = os.WriteFile(file, []byte("content"), 0644)
		require.NoError(t, err)
		time.Sleep(10 * time.Millisecond)
	}

	// Due to debouncing and small buffer, some events might be dropped
	// Just verify we can still receive events
	timeout := time.After(2 * time.Second)
	eventsReceived := 0

	for eventsReceived < 2 {
		select {
		case <-watcher.Events():
			eventsReceived++
		case <-timeout:
			t.Fatal("timeout waiting for events")
		}
	}

	assert.GreaterOrEqual(t, eventsReceived, 2)
}

func TestWatcherError_Error(t *testing.T) {
	baseErr := errors.New("base error")
	watcherErr := &WatcherError{
		Op:   "watch",
		Path: "/test/path",
		Err:  baseErr,
	}

	errMsg := watcherErr.Error()
	assert.Contains(t, errMsg, "watch")
	assert.Contains(t, errMsg, "/test/path")
	assert.Contains(t, errMsg, "base error")
}

func TestWatcherError_Unwrap(t *testing.T) {
	baseErr := errors.New("base error")
	watcherErr := &WatcherError{
		Op:   "watch",
		Path: "/test/path",
		Err:  baseErr,
	}

	assert.Equal(t, baseErr, watcherErr.Unwrap())
}

// Benchmark tests
func BenchmarkFSNotifyWatcher_SingleFile(b *testing.B) {
	logger := adapters.NewNoOpLogger()
	watcher, err := NewFSNotifyWatcher(FSNotifyWatcherConfig{
		Logger:        logger,
		DebounceDelay: 10 * time.Millisecond,
	})
	require.NoError(b, err)
	defer watcher.Stop()

	dir := b.TempDir()
	err = watcher.Watch(dir)
	require.NoError(b, err)

	testFile := filepath.Join(dir, "bench.txt")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err = os.WriteFile(testFile, []byte("content"), 0644)
		require.NoError(b, err)

		// Drain event
		select {
		case <-watcher.Events():
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func BenchmarkFSNotifyWatcher_ManyFiles(b *testing.B) {
	logger := adapters.NewNoOpLogger()
	watcher, err := NewFSNotifyWatcher(FSNotifyWatcherConfig{
		Logger:        logger,
		DebounceDelay: 10 * time.Millisecond,
	})
	require.NoError(b, err)
	defer watcher.Stop()

	dir := b.TempDir()
	err = watcher.Watch(dir)
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		file := filepath.Join(dir, fmt.Sprintf("file%d.txt", i))
		err = os.WriteFile(file, []byte("content"), 0644)
		require.NoError(b, err)
	}

	// Drain events
	timeout := time.After(1 * time.Second)
	for {
		select {
		case <-watcher.Events():
		case <-timeout:
			return
		}
	}
}
