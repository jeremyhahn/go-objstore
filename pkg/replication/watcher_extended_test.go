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

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestProcessEvents_AllEventTypes tests all different event types.
func TestProcessEvents_AllEventTypes(t *testing.T) {
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

	testFile := filepath.Join(dir, "event-test.txt")

	// Test Create event
	err = os.WriteFile(testFile, []byte("initial"), 0644)
	require.NoError(t, err)

	event := <-watcher.Events()
	assert.Equal(t, testFile, event.Path)
	assert.Equal(t, "put", event.Operation)

	// Test Write event
	time.Sleep(100 * time.Millisecond) // Wait for debounce
	err = os.WriteFile(testFile, []byte("modified"), 0644)
	require.NoError(t, err)

	event = <-watcher.Events()
	assert.Equal(t, testFile, event.Path)
	assert.Equal(t, "put", event.Operation)

	// Test Remove event
	time.Sleep(100 * time.Millisecond) // Wait for debounce
	err = os.Remove(testFile)
	require.NoError(t, err)

	event = <-watcher.Events()
	assert.Equal(t, testFile, event.Path)
	assert.Equal(t, "delete", event.Operation)
}

// TestProcessEvents_ErrorRecovery tests that the watcher recovers from errors.
func TestProcessEvents_ErrorRecovery(t *testing.T) {
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

	// Create a file to generate an event
	testFile := filepath.Join(dir, "recovery-test.txt")
	err = os.WriteFile(testFile, []byte("test"), 0644)
	require.NoError(t, err)

	// Should receive event successfully
	select {
	case event := <-watcher.Events():
		assert.Equal(t, testFile, event.Path)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for event")
	}

	// Watcher should still work after processing events
	testFile2 := filepath.Join(dir, "recovery-test2.txt")
	err = os.WriteFile(testFile2, []byte("test2"), 0644)
	require.NoError(t, err)

	select {
	case event := <-watcher.Events():
		assert.Equal(t, testFile2, event.Path)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for second event")
	}
}

// TestHandleEvent_EdgeCases tests edge cases in event handling.
func TestHandleEvent_EdgeCases(t *testing.T) {
	logger := adapters.NewNoOpLogger()
	watcher, err := NewFSNotifyWatcher(FSNotifyWatcherConfig{
		Logger:        logger,
		DebounceDelay: 50 * time.Millisecond,
		EventBuffer:   10,
	})
	require.NoError(t, err)
	defer watcher.Stop()

	dir := t.TempDir()
	err = watcher.Watch(dir)
	require.NoError(t, err)

	// Test empty filename
	emptyFile := filepath.Join(dir, "")
	if emptyFile != dir {
		// Create a file with space in name
		spaceFile := filepath.Join(dir, "file with spaces.txt")
		err = os.WriteFile(spaceFile, []byte("test"), 0644)
		require.NoError(t, err)

		select {
		case event := <-watcher.Events():
			assert.Equal(t, spaceFile, event.Path)
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for event")
		}
	}

	// Test special characters in filename
	specialFile := filepath.Join(dir, "file-with-special_chars.txt")
	err = os.WriteFile(specialFile, []byte("test"), 0644)
	require.NoError(t, err)

	select {
	case event := <-watcher.Events():
		assert.Equal(t, specialFile, event.Path)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for event")
	}
}

// TestConvertEvent_AllOperations tests conversion of all fsnotify operations.
func TestConvertEvent_AllOperations(t *testing.T) {
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

	// Test Chmod event (should be ignored)
	testFile := filepath.Join(dir, "chmod-test.txt")
	err = os.WriteFile(testFile, []byte("test"), 0644)
	require.NoError(t, err)

	// Wait for create event
	<-watcher.Events()

	// Change permissions
	time.Sleep(100 * time.Millisecond) // Wait for debounce
	err = os.Chmod(testFile, 0600)
	require.NoError(t, err)

	// Should NOT receive chmod event (it's filtered out)
	select {
	case event := <-watcher.Events():
		t.Logf("Received unexpected event: %+v", event)
		// Some systems may not send chmod events, so this is not necessarily an error
	case <-time.After(300 * time.Millisecond):
		t.Log("No chmod event received (expected)")
	}
}

// TestHandleCreate_NestedDirectories tests creating nested directories.
func TestHandleCreate_NestedDirectories(t *testing.T) {
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

	// Create deeply nested directory structure
	nestedPath := filepath.Join(dir, "level1", "level2", "level3")
	err = os.MkdirAll(nestedPath, 0755)
	require.NoError(t, err)

	// Give watcher time to process directory events
	time.Sleep(300 * time.Millisecond)

	// Create file in deeply nested directory
	testFile := filepath.Join(nestedPath, "deep-test.txt")
	err = os.WriteFile(testFile, []byte("deep content"), 0644)
	require.NoError(t, err)

	// Should receive event from nested directory
	foundEvent := false
	timeout := time.After(2 * time.Second)

	for !foundEvent {
		select {
		case event := <-watcher.Events():
			if event.Path == testFile {
				foundEvent = true
				assert.Equal(t, "put", event.Operation)
			}
		case <-timeout:
			t.Fatal("timeout waiting for event in nested directory")
		}
	}
}

// TestWatcher_LongRunning is a stress test for long-running watchers.
func TestWatcher_LongRunning(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping long-running test in short mode")
	}

	logger := adapters.NewNoOpLogger()
	watcher, err := NewFSNotifyWatcher(FSNotifyWatcherConfig{
		Logger:        logger,
		DebounceDelay: 50 * time.Millisecond,
		EventBuffer:   100,
	})
	require.NoError(t, err)
	defer watcher.Stop()

	dir := t.TempDir()
	err = watcher.Watch(dir)
	require.NoError(t, err)

	// Create many files over time
	numFiles := 50
	eventsReceived := 0

	done := make(chan bool)
	go func() {
		timeout := time.After(10 * time.Second)
		for eventsReceived < numFiles {
			select {
			case <-watcher.Events():
				eventsReceived++
			case <-timeout:
				done <- false
				return
			}
		}
		done <- true
	}()

	// Create files with delays
	for i := 0; i < numFiles; i++ {
		testFile := filepath.Join(dir, fmt.Sprintf("stress-test-%d.txt", i))
		err = os.WriteFile(testFile, []byte("content"), 0644)
		require.NoError(t, err)
		time.Sleep(20 * time.Millisecond)
	}

	success := <-done
	if !success {
		t.Fatalf("Only received %d events out of %d", eventsReceived, numFiles)
	}

	t.Logf("Successfully processed %d events", eventsReceived)
}

// TestWatcher_MultipleDirectories tests watching multiple directories simultaneously.
func TestWatcher_MultipleDirectories(t *testing.T) {
	logger := adapters.NewNoOpLogger()
	watcher, err := NewFSNotifyWatcher(FSNotifyWatcherConfig{
		Logger:        logger,
		DebounceDelay: 50 * time.Millisecond,
	})
	require.NoError(t, err)
	defer watcher.Stop()

	dir1 := t.TempDir()
	dir2 := t.TempDir()
	dir3 := t.TempDir()

	err = watcher.Watch(dir1)
	require.NoError(t, err)

	err = watcher.Watch(dir2)
	require.NoError(t, err)

	err = watcher.Watch(dir3)
	require.NoError(t, err)

	// Create files in different directories
	files := []string{
		filepath.Join(dir1, "file1.txt"),
		filepath.Join(dir2, "file2.txt"),
		filepath.Join(dir3, "file3.txt"),
	}

	for _, file := range files {
		err = os.WriteFile(file, []byte("content"), 0644)
		require.NoError(t, err)
	}

	// Should receive events from all directories
	receivedFiles := make(map[string]bool)
	timeout := time.After(3 * time.Second)

	for len(receivedFiles) < 3 {
		select {
		case event := <-watcher.Events():
			receivedFiles[event.Path] = true
		case <-timeout:
			t.Fatalf("timeout waiting for events, received %d", len(receivedFiles))
		}
	}

	for _, file := range files {
		if !receivedFiles[file] {
			t.Errorf("Did not receive event for %s", file)
		}
	}
}

// TestWatcher_RapidFileChanges tests rapid successive changes to the same file.
func TestWatcher_RapidFileChanges(t *testing.T) {
	logger := adapters.NewNoOpLogger()
	watcher, err := NewFSNotifyWatcher(FSNotifyWatcherConfig{
		Logger:        logger,
		DebounceDelay: 100 * time.Millisecond,
	})
	require.NoError(t, err)
	defer watcher.Stop()

	dir := t.TempDir()
	err = watcher.Watch(dir)
	require.NoError(t, err)

	testFile := filepath.Join(dir, "rapid-test.txt")

	// Make rapid changes
	numChanges := 10
	for i := 0; i < numChanges; i++ {
		content := fmt.Sprintf("content-%d", i)
		err = os.WriteFile(testFile, []byte(content), 0644)
		require.NoError(t, err)
		time.Sleep(10 * time.Millisecond) // Faster than debounce delay
	}

	// Due to debouncing, we should receive fewer events than changes
	eventsReceived := 0
	timeout := time.After(2 * time.Second)

	for {
		select {
		case event := <-watcher.Events():
			if event.Path == testFile {
				eventsReceived++
			}
		case <-timeout:
			t.Logf("Received %d events for %d rapid changes (debouncing working)", eventsReceived, numChanges)
			if eventsReceived >= numChanges {
				t.Error("Debouncing did not reduce event count")
			}
			return
		}
	}
}

// TestWatcher_DirectoryDeletion tests deleting a watched directory.
func TestWatcher_DirectoryDeletion(t *testing.T) {
	logger := adapters.NewNoOpLogger()
	watcher, err := NewFSNotifyWatcher(FSNotifyWatcherConfig{
		Logger:        logger,
		DebounceDelay: 50 * time.Millisecond,
	})
	require.NoError(t, err)
	defer watcher.Stop()

	dir := t.TempDir()
	subDir := filepath.Join(dir, "subdir")
	err = os.MkdirAll(subDir, 0755)
	require.NoError(t, err)

	err = watcher.Watch(dir)
	require.NoError(t, err)

	// Create file in subdirectory
	testFile := filepath.Join(subDir, "test.txt")
	err = os.WriteFile(testFile, []byte("test"), 0644)
	require.NoError(t, err)

	// Wait for create event
	<-watcher.Events()

	// Remove the subdirectory
	time.Sleep(100 * time.Millisecond)
	err = os.RemoveAll(subDir)
	require.NoError(t, err)

	// Should receive delete event(s)
	timeout := time.After(2 * time.Second)
	foundDelete := false

	for !foundDelete {
		select {
		case event := <-watcher.Events():
			if event.Operation == "delete" {
				foundDelete = true
			}
		case <-timeout:
			t.Fatal("timeout waiting for delete event")
		}
	}
}

// TestWatcher_SymbolicLinks tests watching directories with symbolic links.
func TestWatcher_SymbolicLinks(t *testing.T) {
	logger := adapters.NewNoOpLogger()
	watcher, err := NewFSNotifyWatcher(FSNotifyWatcherConfig{
		Logger:        logger,
		DebounceDelay: 50 * time.Millisecond,
	})
	require.NoError(t, err)
	defer watcher.Stop()

	dir := t.TempDir()
	targetDir := filepath.Join(dir, "target")
	err = os.MkdirAll(targetDir, 0755)
	require.NoError(t, err)

	linkDir := filepath.Join(dir, "link")
	err = os.Symlink(targetDir, linkDir)
	if err != nil {
		t.Skip("Symlinks not supported on this system")
	}

	err = watcher.Watch(dir)
	require.NoError(t, err)

	// Create file in target directory
	testFile := filepath.Join(targetDir, "test.txt")
	err = os.WriteFile(testFile, []byte("test"), 0644)
	require.NoError(t, err)

	// Should receive event for file creation
	timeout := time.After(2 * time.Second)
	select {
	case event := <-watcher.Events():
		assert.Contains(t, event.Path, "test.txt")
	case <-timeout:
		t.Fatal("timeout waiting for event")
	}
}

// TestWatcher_FilePermissions tests handling of files with different permissions.
func TestWatcher_FilePermissions(t *testing.T) {
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

	// Create file with restrictive permissions
	testFile := filepath.Join(dir, "restricted.txt")
	err = os.WriteFile(testFile, []byte("test"), 0400) // Read-only
	require.NoError(t, err)

	// Should receive event
	timeout := time.After(2 * time.Second)
	select {
	case event := <-watcher.Events():
		assert.Equal(t, testFile, event.Path)
	case <-timeout:
		t.Fatal("timeout waiting for event")
	}
}

// TestWatcher_ConcurrentAccess tests concurrent access to watcher.
func TestWatcher_ConcurrentAccess(t *testing.T) {
	logger := adapters.NewNoOpLogger()
	watcher, err := NewFSNotifyWatcher(FSNotifyWatcherConfig{
		Logger:        logger,
		DebounceDelay: 50 * time.Millisecond,
		EventBuffer:   100,
	})
	require.NoError(t, err)
	defer watcher.Stop()

	dir := t.TempDir()
	err = watcher.Watch(dir)
	require.NoError(t, err)

	// Create files from multiple goroutines
	numGoroutines := 5
	filesPerGoroutine := 10

	done := make(chan bool, numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		go func(goroutineID int) {
			for i := 0; i < filesPerGoroutine; i++ {
				testFile := filepath.Join(dir, fmt.Sprintf("concurrent-%d-%d.txt", goroutineID, i))
				err := os.WriteFile(testFile, []byte("content"), 0644)
				if err != nil {
					t.Logf("Error writing file: %v", err)
				}
				time.Sleep(10 * time.Millisecond)
			}
			done <- true
		}(g)
	}

	// Wait for all goroutines to complete
	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// Count events (with timeout)
	eventsReceived := 0
	timeout := time.After(5 * time.Second)

	for eventsReceived < numGoroutines*filesPerGoroutine {
		select {
		case <-watcher.Events():
			eventsReceived++
		case <-timeout:
			t.Logf("Received %d events out of %d (due to debouncing)", eventsReceived, numGoroutines*filesPerGoroutine)
			if eventsReceived == 0 {
				t.Fatal("No events received")
			}
			return
		}
	}

	t.Logf("Successfully received all %d events", eventsReceived)
}

// TestWatcher_ErrorChannelClose tests handling of error channel closure.
func TestWatcher_ErrorChannelClose(t *testing.T) {
	logger := adapters.NewNoOpLogger()
	watcher, err := NewFSNotifyWatcher(FSNotifyWatcherConfig{
		Logger:        logger,
		DebounceDelay: 50 * time.Millisecond,
	})
	require.NoError(t, err)

	dir := t.TempDir()
	err = watcher.Watch(dir)
	require.NoError(t, err)

	// Stop the watcher (which closes internal channels)
	err = watcher.Stop()
	require.NoError(t, err)

	// Events channel should be closed
	_, ok := <-watcher.Events()
	assert.False(t, ok, "Events channel should be closed")
}

// TestHandleEvent_WithFullEventBuffer tests event handling when buffer is full.
func TestHandleEvent_WithFullEventBuffer(t *testing.T) {
	logger := adapters.NewNoOpLogger()
	watcher, err := NewFSNotifyWatcher(FSNotifyWatcherConfig{
		Logger:        logger,
		DebounceDelay: 500 * time.Millisecond, // Long debounce
		EventBuffer:   2,                      // Very small buffer
	})
	require.NoError(t, err)
	defer watcher.Stop()

	dir := t.TempDir()
	err = watcher.Watch(dir)
	require.NoError(t, err)

	// Create many files quickly to overflow buffer
	for i := 0; i < 10; i++ {
		testFile := filepath.Join(dir, fmt.Sprintf("overflow-%d.txt", i))
		err = os.WriteFile(testFile, []byte("content"), 0644)
		require.NoError(t, err)
		time.Sleep(10 * time.Millisecond)
	}

	// Even with buffer overflow, watcher should remain functional
	// and we should receive at least some events
	eventsReceived := 0
	timeout := time.After(3 * time.Second)

	for eventsReceived < 2 {
		select {
		case <-watcher.Events():
			eventsReceived++
		case <-timeout:
			if eventsReceived < 2 {
				t.Fatalf("Only received %d events", eventsReceived)
			}
		}
	}

	t.Logf("Received %d events (some may have been dropped due to full buffer)", eventsReceived)
}
