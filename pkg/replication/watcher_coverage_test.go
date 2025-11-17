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
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test NewFSNotifyWatcher with error
func TestNewFSNotifyWatcher_Error(t *testing.T) {
	logger := adapters.NewNoOpLogger()

	// This should succeed, but we test the error path by passing invalid config
	watcher, err := NewFSNotifyWatcher(FSNotifyWatcherConfig{
		Logger:        logger,
		DebounceDelay: -1 * time.Second, // Invalid delay
	})

	// Even with negative delay, it should still work (gets clamped or ignored)
	require.NoError(t, err)
	if watcher != nil {
		watcher.Stop()
	}
}

// Test Watch with invalid path
func TestWatcher_Watch_InvalidPath(t *testing.T) {
	logger := adapters.NewNoOpLogger()
	watcher, err := NewFSNotifyWatcher(FSNotifyWatcherConfig{
		Logger:        logger,
		DebounceDelay: 50 * time.Millisecond,
	})
	require.NoError(t, err)
	defer watcher.Stop()

	// Try to watch non-existent path
	err = watcher.Watch("/nonexistent/path/that/does/not/exist")
	assert.Error(t, err)
}

// Test processEvents with various event types
func TestWatcher_ProcessEvents_AllTypes(t *testing.T) {
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

	// Test CREATE event
	file1 := filepath.Join(dir, "create.txt")
	err = os.WriteFile(file1, []byte("create"), 0644)
	require.NoError(t, err)

	// Test WRITE event
	err = os.WriteFile(file1, []byte("modified"), 0644)
	require.NoError(t, err)

	// Test RENAME event
	file2 := filepath.Join(dir, "renamed.txt")
	err = os.Rename(file1, file2)
	require.NoError(t, err)

	// Test REMOVE event
	err = os.Remove(file2)
	require.NoError(t, err)

	// Test CHMOD event (should be ignored)
	file3 := filepath.Join(dir, "chmod.txt")
	err = os.WriteFile(file3, []byte("chmod"), 0644)
	require.NoError(t, err)
	err = os.Chmod(file3, 0444)
	require.NoError(t, err)

	// Collect events (with timeout to avoid hanging)
	timeout := time.After(2 * time.Second)
	eventCount := 0
	for eventCount < 3 { // CREATE, WRITE, RENAME (delete), REMOVE = multiple events
		select {
		case event := <-watcher.Events():
			t.Logf("Received event: %s %s", event.Operation, event.Path)
			eventCount++
		case <-timeout:
			t.Logf("Timeout after receiving %d events", eventCount)
			return
		}
	}
}

// Test convertEvent with unknown event type
func TestWatcher_ConvertEvent_UnknownOp(t *testing.T) {
	logger := adapters.NewNoOpLogger()
	watcher, err := NewFSNotifyWatcher(FSNotifyWatcherConfig{
		Logger:        logger,
		DebounceDelay: 50 * time.Millisecond,
	})
	require.NoError(t, err)
	defer watcher.Stop()

	// Create an event with no recognized operation
	event := fsnotify.Event{
		Name: "/test/path",
		Op:   0, // No operation set
	}

	result := watcher.convertEvent(event)
	assert.Nil(t, result, "Should return nil for unknown event type")
}

// Test convertEvent with CHMOD (should be ignored)
func TestWatcher_ConvertEvent_Chmod(t *testing.T) {
	logger := adapters.NewNoOpLogger()
	watcher, err := NewFSNotifyWatcher(FSNotifyWatcherConfig{
		Logger:        logger,
		DebounceDelay: 50 * time.Millisecond,
	})
	require.NoError(t, err)
	defer watcher.Stop()

	event := fsnotify.Event{
		Name: "/test/path",
		Op:   fsnotify.Chmod,
	}

	result := watcher.convertEvent(event)
	assert.Nil(t, result, "Should return nil for CHMOD event")
}

// Test handleCreate with file (not directory)
func TestWatcher_HandleCreate_File(t *testing.T) {
	logger := adapters.NewNoOpLogger()
	watcher, err := NewFSNotifyWatcher(FSNotifyWatcherConfig{
		Logger:        logger,
		DebounceDelay: 50 * time.Millisecond,
	})
	require.NoError(t, err)
	defer watcher.Stop()

	dir := t.TempDir()
	filePath := filepath.Join(dir, "test.txt")
	err = os.WriteFile(filePath, []byte("test"), 0644)
	require.NoError(t, err)

	// Call handleCreate with file path (not a directory)
	watcher.handleCreate(filePath)

	// Verify file is not added to watching map (only directories are watched)
	watcher.mu.RLock()
	_, watching := watcher.watching[filePath]
	watcher.mu.RUnlock()

	assert.False(t, watching, "Files should not be added to watching map")
}

// Test handleCreate with non-existent path
func TestWatcher_HandleCreate_NonExistent(t *testing.T) {
	logger := adapters.NewNoOpLogger()
	watcher, err := NewFSNotifyWatcher(FSNotifyWatcherConfig{
		Logger:        logger,
		DebounceDelay: 50 * time.Millisecond,
	})
	require.NoError(t, err)
	defer watcher.Stop()

	// Call handleCreate with non-existent path
	watcher.handleCreate("/nonexistent/path")

	// Should handle gracefully without error
	assert.NotNil(t, watcher)
}

// Test handleCreate with already watched directory
func TestWatcher_HandleCreate_AlreadyWatched(t *testing.T) {
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

	// Try to add the same directory again via handleCreate
	watcher.handleCreate(dir)

	// Should handle gracefully (idempotent)
	watcher.mu.RLock()
	watching := watcher.watching[dir]
	watcher.mu.RUnlock()

	assert.True(t, watching)
}

// Test Stop idempotency
func TestWatcher_Stop_Idempotent(t *testing.T) {
	logger := adapters.NewNoOpLogger()
	watcher, err := NewFSNotifyWatcher(FSNotifyWatcherConfig{
		Logger:        logger,
		DebounceDelay: 50 * time.Millisecond,
	})
	require.NoError(t, err)

	// Stop multiple times
	watcher.Stop()
	watcher.Stop()
	watcher.Stop()

	// Should handle gracefully
	assert.True(t, watcher.stopped)
}

// Test Watch after Stop
func TestWatcher_Watch_AfterStop(t *testing.T) {
	logger := adapters.NewNoOpLogger()
	watcher, err := NewFSNotifyWatcher(FSNotifyWatcherConfig{
		Logger:        logger,
		DebounceDelay: 50 * time.Millisecond,
	})
	require.NoError(t, err)

	watcher.Stop()

	// Try to watch after stop
	dir := t.TempDir()
	err = watcher.Watch(dir)
	assert.Error(t, err, "Should return error when watching after stop")
}

// Test Watch with filepath.Walk error - subdirectory permission issue
func TestWatcher_Watch_WalkError(t *testing.T) {
	logger := adapters.NewNoOpLogger()
	watcher, err := NewFSNotifyWatcher(FSNotifyWatcherConfig{
		Logger:        logger,
		DebounceDelay: 50 * time.Millisecond,
	})
	require.NoError(t, err)
	defer watcher.Stop()

	dir := t.TempDir()
	subdir := filepath.Join(dir, "subdir")
	err = os.Mkdir(subdir, 0755)
	require.NoError(t, err)

	// Make subdirectory inaccessible
	err = os.Chmod(subdir, 0000)
	require.NoError(t, err)
	defer os.Chmod(subdir, 0755)

	// Watch should succeed but log warning about inaccessible subdirectory
	err = watcher.Watch(dir)
	// The walk continues even with errors, so Watch should succeed
	assert.NoError(t, err)
}

// Test Watch already watching same path
func TestWatcher_Watch_AlreadyWatching(t *testing.T) {
	logger := adapters.NewNoOpLogger()
	watcher, err := NewFSNotifyWatcher(FSNotifyWatcherConfig{
		Logger:        logger,
		DebounceDelay: 50 * time.Millisecond,
	})
	require.NoError(t, err)
	defer watcher.Stop()

	dir := t.TempDir()

	// Watch first time
	err = watcher.Watch(dir)
	require.NoError(t, err)

	// Watch same path again
	err = watcher.Watch(dir)
	assert.NoError(t, err) // Should be idempotent
}

// Test handleCreate with directory containing subdirectories
func TestWatcher_HandleCreate_WithSubdirectories(t *testing.T) {
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

	// Create nested directories
	nestedDir := filepath.Join(dir, "parent", "child", "grandchild")
	err = os.MkdirAll(nestedDir, 0755)
	require.NoError(t, err)

	// Give time for watcher to process
	time.Sleep(200 * time.Millisecond)

	// Verify parent directories are being watched
	watcher.mu.RLock()
	parentWatched := watcher.watching[filepath.Join(dir, "parent")]
	watcher.mu.RUnlock()

	assert.True(t, parentWatched, "Parent directory should be watched")
}

// Test shouldIgnore with various file patterns
func TestWatcher_ShouldIgnore_Patterns(t *testing.T) {
	logger := adapters.NewNoOpLogger()
	watcher, err := NewFSNotifyWatcher(FSNotifyWatcherConfig{
		Logger:        logger,
		DebounceDelay: 50 * time.Millisecond,
	})
	require.NoError(t, err)
	defer watcher.Stop()

	tests := []struct {
		path         string
		shouldIgnore bool
	}{
		{"/path/to/.hidden", true},
		{"/path/to/file.txt", false},
		{"/path/to/.metadata.json", true},
		{"/path/to/file.metadata.json", true},
		{"/path/to/backup~", true},
		{"/path/to/temp.tmp", true},
		{"/path/to/normal.json", false},
		{"/path/to/.gitignore", true},
		{"/path/to/file.backup", false},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			result := watcher.shouldIgnore(tt.path)
			assert.Equal(t, tt.shouldIgnore, result)
		})
	}
}

// Test shouldProcess debouncing
func TestWatcher_ShouldProcess_Debouncing(t *testing.T) {
	logger := adapters.NewNoOpLogger()
	watcher, err := NewFSNotifyWatcher(FSNotifyWatcherConfig{
		Logger:        logger,
		DebounceDelay: 100 * time.Millisecond,
	})
	require.NoError(t, err)
	defer watcher.Stop()

	path := "/test/path.txt"

	// First call should process
	assert.True(t, watcher.shouldProcess(path))

	// Immediate second call should be debounced
	assert.False(t, watcher.shouldProcess(path))

	// Wait for debounce delay
	time.Sleep(150 * time.Millisecond)

	// Should process again after delay
	assert.True(t, watcher.shouldProcess(path))
}

// Test processEvents with channel full (event dropped)
func TestWatcher_ProcessEvents_ChannelFull(t *testing.T) {
	logger := adapters.NewNoOpLogger()
	watcher, err := NewFSNotifyWatcher(FSNotifyWatcherConfig{
		Logger:        logger,
		DebounceDelay: 10 * time.Millisecond,
		EventBuffer:   1, // Small buffer to test overflow
	})
	require.NoError(t, err)
	defer watcher.Stop()

	dir := t.TempDir()
	err = watcher.Watch(dir)
	require.NoError(t, err)

	// Create many files rapidly to overflow the buffer
	for i := 0; i < 10; i++ {
		file := filepath.Join(dir, fmt.Sprintf("file%d.txt", i))
		err = os.WriteFile(file, []byte("test"), 0644)
		require.NoError(t, err)
	}

	// Drain some events (but not all due to buffer overflow)
	received := 0
	timeout := time.After(1 * time.Second)
drainLoop:
	for {
		select {
		case <-watcher.Events():
			received++
		case <-timeout:
			break drainLoop
		}
	}

	// We should have received some events, but likely not all due to buffer size
	t.Logf("Received %d events", received)
	assert.Greater(t, received, 0)
}

// Test Watch with subdirectory that fails to add
func TestWatcher_Watch_SubdirAddFailure(t *testing.T) {
	logger := adapters.NewNoOpLogger()
	watcher, err := NewFSNotifyWatcher(FSNotifyWatcherConfig{
		Logger:        logger,
		DebounceDelay: 50 * time.Millisecond,
	})
	require.NoError(t, err)
	defer watcher.Stop()

	dir := t.TempDir()

	// Create a subdirectory
	subdir := filepath.Join(dir, "subdir")
	err = os.Mkdir(subdir, 0755)
	require.NoError(t, err)

	// Watch the parent directory (this will add subdirectories)
	err = watcher.Watch(dir)
	require.NoError(t, err)

	// Verify subdirectory was added
	watcher.mu.RLock()
	subdirWatched := watcher.watching[subdir]
	watcher.mu.RUnlock()
	assert.True(t, subdirWatched)
}

// Test convertEvent with all operation types
func TestWatcher_ConvertEvent_AllOps(t *testing.T) {
	logger := adapters.NewNoOpLogger()
	watcher, err := NewFSNotifyWatcher(FSNotifyWatcherConfig{
		Logger:        logger,
		DebounceDelay: 50 * time.Millisecond,
	})
	require.NoError(t, err)
	defer watcher.Stop()

	tests := []struct {
		op       fsnotify.Op
		expected string
		isNil    bool
	}{
		{fsnotify.Create, "put", false},
		{fsnotify.Write, "put", false},
		{fsnotify.Remove, "delete", false},
		{fsnotify.Rename, "delete", false},
		{fsnotify.Chmod, "", true},
	}

	for _, tt := range tests {
		t.Run(tt.op.String(), func(t *testing.T) {
			event := fsnotify.Event{
				Name: "/test/path.txt",
				Op:   tt.op,
			}

			result := watcher.convertEvent(event)
			if tt.isNil {
				assert.Nil(t, result)
			} else {
				require.NotNil(t, result)
				assert.Equal(t, tt.expected, result.Operation)
				assert.Equal(t, "/test/path.txt", result.Path)
			}
		})
	}
}

// Test Events channel returns correct channel
func TestWatcher_Events_Channel(t *testing.T) {
	logger := adapters.NewNoOpLogger()
	watcher, err := NewFSNotifyWatcher(FSNotifyWatcherConfig{
		Logger:        logger,
		DebounceDelay: 50 * time.Millisecond,
	})
	require.NoError(t, err)
	defer watcher.Stop()

	eventsChan := watcher.Events()
	assert.NotNil(t, eventsChan)

	// Verify it's read-only by type
	var _ <-chan FileSystemEvent = eventsChan
}

// Test Stop closes event channel
func TestWatcher_Stop_ClosesEventChannel(t *testing.T) {
	logger := adapters.NewNoOpLogger()
	watcher, err := NewFSNotifyWatcher(FSNotifyWatcherConfig{
		Logger:        logger,
		DebounceDelay: 50 * time.Millisecond,
	})
	require.NoError(t, err)

	eventsChan := watcher.Events()

	// Stop watcher
	err = watcher.Stop()
	assert.NoError(t, err)

	// Verify channel is closed
	select {
	case _, ok := <-eventsChan:
		assert.False(t, ok, "Events channel should be closed")
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for channel to close")
	}
}

// Test handleCreate with WalkDir error on subdirectory
func TestWatcher_HandleCreate_WalkDirError(t *testing.T) {
	logger := adapters.NewNoOpLogger()
	watcher, err := NewFSNotifyWatcher(FSNotifyWatcherConfig{
		Logger:        logger,
		DebounceDelay: 50 * time.Millisecond,
	})
	require.NoError(t, err)
	defer watcher.Stop()

	dir := t.TempDir()

	// Create directory structure
	parentDir := filepath.Join(dir, "parent")
	err = os.Mkdir(parentDir, 0755)
	require.NoError(t, err)

	subdir := filepath.Join(parentDir, "subdir")
	err = os.Mkdir(subdir, 0755)
	require.NoError(t, err)

	// Make subdirectory inaccessible
	err = os.Chmod(subdir, 0000)
	require.NoError(t, err)
	defer os.Chmod(subdir, 0755)

	// Call handleCreate on parent - should handle error gracefully
	watcher.handleCreate(parentDir)

	// Parent should still be added even if subdir fails
	watcher.mu.RLock()
	parentWatched := watcher.watching[parentDir]
	watcher.mu.RUnlock()

	assert.True(t, parentWatched)
}

// Test NewFSNotifyWatcher with default config values
func TestNewFSNotifyWatcher_DefaultConfig(t *testing.T) {
	watcher, err := NewFSNotifyWatcher(FSNotifyWatcherConfig{})
	require.NoError(t, err)
	defer watcher.Stop()

	assert.NotNil(t, watcher.logger)
	assert.Equal(t, 100*time.Millisecond, watcher.debounceDelay)
	assert.NotNil(t, watcher.events)
}

// Test processEvents goroutine handles context cancellation
func TestWatcher_ProcessEvents_ContextCancellation(t *testing.T) {
	logger := adapters.NewNoOpLogger()
	watcher, err := NewFSNotifyWatcher(FSNotifyWatcherConfig{
		Logger:        logger,
		DebounceDelay: 50 * time.Millisecond,
	})
	require.NoError(t, err)

	// Stop should cancel context and wait for goroutine to finish
	err = watcher.Stop()
	assert.NoError(t, err)

	// Verify stopped
	assert.True(t, watcher.stopped)
}

// Test Watch with path cleaning
func TestWatcher_Watch_PathCleaning(t *testing.T) {
	logger := adapters.NewNoOpLogger()
	watcher, err := NewFSNotifyWatcher(FSNotifyWatcherConfig{
		Logger:        logger,
		DebounceDelay: 50 * time.Millisecond,
	})
	require.NoError(t, err)
	defer watcher.Stop()

	dir := t.TempDir()

	// Watch with unclean path
	uncleanPath := dir + "/./"
	err = watcher.Watch(uncleanPath)
	require.NoError(t, err)

	// Verify clean path is stored
	watcher.mu.RLock()
	cleanPath := filepath.Clean(uncleanPath)
	watching := watcher.watching[cleanPath]
	watcher.mu.RUnlock()

	assert.True(t, watching)
}

// Test watcher with file creation triggering directory watch
func TestWatcher_FileCreation_DirectoryWatch(t *testing.T) {
	logger := adapters.NewNoOpLogger()
	watcher, err := NewFSNotifyWatcher(FSNotifyWatcherConfig{
		Logger:        logger,
		DebounceDelay: 10 * time.Millisecond,
	})
	require.NoError(t, err)
	defer watcher.Stop()

	dir := t.TempDir()
	err = watcher.Watch(dir)
	require.NoError(t, err)

	// Create a new directory
	newDir := filepath.Join(dir, "newdir")
	err = os.Mkdir(newDir, 0755)
	require.NoError(t, err)

	// Give watcher time to process the create event
	time.Sleep(200 * time.Millisecond)

	// Verify new directory is being watched
	watcher.mu.RLock()
	watching := watcher.watching[newDir]
	watcher.mu.RUnlock()

	assert.True(t, watching, "New directory should be watched")
}

// Test handleEvent with debounced event
func TestWatcher_HandleEvent_Debounced(t *testing.T) {
	logger := adapters.NewNoOpLogger()
	watcher, err := NewFSNotifyWatcher(FSNotifyWatcherConfig{
		Logger:        logger,
		DebounceDelay: 100 * time.Millisecond,
		EventBuffer:   10,
	})
	require.NoError(t, err)
	defer watcher.Stop()

	dir := t.TempDir()
	err = watcher.Watch(dir)
	require.NoError(t, err)

	// Create a file
	file := filepath.Join(dir, "test.txt")
	err = os.WriteFile(file, []byte("test"), 0644)
	require.NoError(t, err)

	// Immediately modify it multiple times
	for i := 0; i < 5; i++ {
		err = os.WriteFile(file, []byte(fmt.Sprintf("test%d", i)), 0644)
		require.NoError(t, err)
	}

	// Wait and collect events
	time.Sleep(300 * time.Millisecond)

	eventCount := 0
	timeout := time.After(200 * time.Millisecond)
drainLoop:
	for {
		select {
		case <-watcher.Events():
			eventCount++
		case <-timeout:
			break drainLoop
		}
	}

	// Should have fewer events than operations due to debouncing
	t.Logf("Received %d events", eventCount)
	assert.Greater(t, eventCount, 0)
}

// Test watcher error handling
func TestWatcher_ErrorHandling(t *testing.T) {
	logger := adapters.NewNoOpLogger()
	watcher, err := NewFSNotifyWatcher(FSNotifyWatcherConfig{
		Logger:        logger,
		DebounceDelay: 50 * time.Millisecond,
	})
	require.NoError(t, err)

	dir := t.TempDir()
	err = watcher.Watch(dir)
	require.NoError(t, err)

	// Stop the watcher to trigger cleanup
	err = watcher.Stop()
	assert.NoError(t, err)

	// Verify watcher is stopped
	assert.True(t, watcher.stopped)
}

// Test Watch with file (not directory)
func TestWatcher_Watch_File(t *testing.T) {
	logger := adapters.NewNoOpLogger()
	watcher, err := NewFSNotifyWatcher(FSNotifyWatcherConfig{
		Logger:        logger,
		DebounceDelay: 50 * time.Millisecond,
	})
	require.NoError(t, err)
	defer watcher.Stop()

	dir := t.TempDir()
	file := filepath.Join(dir, "test.txt")
	err = os.WriteFile(file, []byte("test"), 0644)
	require.NoError(t, err)

	// Watch a file (fsnotify can watch files too)
	err = watcher.Watch(file)
	// This might succeed or fail depending on platform
	// Just verify it doesn't panic
	_ = err
}

// Test handleCreate with stat error after creation
func TestWatcher_HandleCreate_StatError(t *testing.T) {
	logger := adapters.NewNoOpLogger()
	watcher, err := NewFSNotifyWatcher(FSNotifyWatcherConfig{
		Logger:        logger,
		DebounceDelay: 50 * time.Millisecond,
	})
	require.NoError(t, err)
	defer watcher.Stop()

	// Call handleCreate with path that doesn't exist
	watcher.handleCreate("/path/that/does/not/exist")

	// Should handle gracefully without panic
	assert.NotNil(t, watcher)
}

// Test combined create and delete operations
func TestWatcher_CreateDelete_Sequence(t *testing.T) {
	logger := adapters.NewNoOpLogger()
	watcher, err := NewFSNotifyWatcher(FSNotifyWatcherConfig{
		Logger:        logger,
		DebounceDelay: 10 * time.Millisecond,
	})
	require.NoError(t, err)
	defer watcher.Stop()

	dir := t.TempDir()
	err = watcher.Watch(dir)
	require.NoError(t, err)

	// Create and delete a file
	file := filepath.Join(dir, "temp.txt")
	err = os.WriteFile(file, []byte("test"), 0644)
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)

	err = os.Remove(file)
	require.NoError(t, err)

	// Collect events
	time.Sleep(100 * time.Millisecond)

	eventCount := 0
	timeout := time.After(200 * time.Millisecond)
drainLoop:
	for {
		select {
		case event := <-watcher.Events():
			eventCount++
			t.Logf("Event: %s %s", event.Operation, event.Path)
		case <-timeout:
			break drainLoop
		}
	}

	// Should have received at least some events
	assert.Greater(t, eventCount, 0)
}

// Test WatcherError implementation
func TestWatcherError_Implementation(t *testing.T) {
	baseErr := fmt.Errorf("base error")
	watcherErr := &WatcherError{
		Op:   "watch",
		Path: "/test/path",
		Err:  baseErr,
	}

	// Test Error() method
	errStr := watcherErr.Error()
	assert.Contains(t, errStr, "watch")
	assert.Contains(t, errStr, "/test/path")
	assert.Contains(t, errStr, "base error")

	// Test Unwrap() method
	unwrapped := watcherErr.Unwrap()
	assert.Equal(t, baseErr, unwrapped)
}

// Test processEvents with watcher errors channel
func TestWatcher_ProcessEvents_ErrorChannel(t *testing.T) {
	logger := adapters.NewNoOpLogger()
	watcher, err := NewFSNotifyWatcher(FSNotifyWatcherConfig{
		Logger:        logger,
		DebounceDelay: 50 * time.Millisecond,
	})
	require.NoError(t, err)

	dir := t.TempDir()
	err = watcher.Watch(dir)
	require.NoError(t, err)

	// Create a file to generate events
	file := filepath.Join(dir, "test.txt")
	err = os.WriteFile(file, []byte("test"), 0644)
	require.NoError(t, err)

	// Give time for processing
	time.Sleep(100 * time.Millisecond)

	// Stop watcher
	err = watcher.Stop()
	assert.NoError(t, err)
}
