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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewJSONLChangeLog(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "changes.jsonl")

	cl, err := NewJSONLChangeLog(logPath, 1024*1024)
	require.NoError(t, err)
	require.NotNil(t, cl)
	defer cl.Close()

	assert.Equal(t, logPath, cl.filePath)
	assert.Equal(t, int64(1024*1024), cl.maxSize)
	assert.NotNil(t, cl.file)
}

func TestNewJSONLChangeLog_InvalidPath(t *testing.T) {
	// Try to create log in non-existent directory without permissions
	_, err := NewJSONLChangeLog("/nonexistent/path/changes.jsonl", 1024)
	assert.Error(t, err)
}

func TestRecordChange(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "changes.jsonl")

	cl, err := NewJSONLChangeLog(logPath, 1024*1024)
	require.NoError(t, err)
	defer cl.Close()

	event := ChangeEvent{
		Key:       "test/file.txt",
		Operation: "put",
		ETag:      "etag123",
		Size:      1024,
	}

	err = cl.RecordChange(event)
	require.NoError(t, err)

	// Verify file was written
	info, err := os.Stat(logPath)
	require.NoError(t, err)
	assert.Greater(t, info.Size(), int64(0))
}

func TestRecordChange_SetsTimestamp(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "changes.jsonl")

	cl, err := NewJSONLChangeLog(logPath, 1024*1024)
	require.NoError(t, err)
	defer cl.Close()

	event := ChangeEvent{
		Key:       "test/file.txt",
		Operation: "put",
	}

	before := time.Now()
	err = cl.RecordChange(event)
	require.NoError(t, err)
	after := time.Now()

	// Retrieve and verify timestamp was set
	events, err := cl.GetUnprocessed("policy1")
	require.NoError(t, err)
	require.Len(t, events, 1)

	assert.True(t, events[0].Timestamp.After(before) || events[0].Timestamp.Equal(before))
	assert.True(t, events[0].Timestamp.Before(after) || events[0].Timestamp.Equal(after))
}

func TestRecordChange_InitializesProcessedMap(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "changes.jsonl")

	cl, err := NewJSONLChangeLog(logPath, 1024*1024)
	require.NoError(t, err)
	defer cl.Close()

	event := ChangeEvent{
		Key:       "test/file.txt",
		Operation: "put",
	}

	err = cl.RecordChange(event)
	require.NoError(t, err)

	// Verify Processed map was initialized
	events, err := cl.GetUnprocessed("policy1")
	require.NoError(t, err)
	require.Len(t, events, 1)
	assert.NotNil(t, events[0].Processed)
}

func TestGetUnprocessed(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "changes.jsonl")

	cl, err := NewJSONLChangeLog(logPath, 1024*1024)
	require.NoError(t, err)
	defer cl.Close()

	// Record multiple events
	events := []ChangeEvent{
		{Key: "file1.txt", Operation: "put", ETag: "etag1", Size: 100},
		{Key: "file2.txt", Operation: "put", ETag: "etag2", Size: 200},
		{Key: "file3.txt", Operation: "delete"},
	}

	for _, event := range events {
		err := cl.RecordChange(event)
		require.NoError(t, err)
	}

	// Get unprocessed for policy1
	unprocessed, err := cl.GetUnprocessed("policy1")
	require.NoError(t, err)
	assert.Len(t, unprocessed, 3)

	// Verify all events are returned
	keys := make(map[string]bool)
	for _, e := range unprocessed {
		keys[e.Key] = true
	}
	assert.True(t, keys["file1.txt"])
	assert.True(t, keys["file2.txt"])
	assert.True(t, keys["file3.txt"])
}

func TestGetUnprocessed_FiltersByPolicy(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "changes.jsonl")

	cl, err := NewJSONLChangeLog(logPath, 1024*1024)
	require.NoError(t, err)
	defer cl.Close()

	// Record events and mark some as processed
	event1 := ChangeEvent{
		Key:       "file1.txt",
		Operation: "put",
	}
	err = cl.RecordChange(event1)
	require.NoError(t, err)

	// Mark as processed for policy1
	err = cl.MarkProcessed("file1.txt", "policy1")
	require.NoError(t, err)

	event2 := ChangeEvent{
		Key:       "file2.txt",
		Operation: "put",
	}
	err = cl.RecordChange(event2)
	require.NoError(t, err)

	// Get unprocessed for policy1 - should only return file2.txt
	unprocessed, err := cl.GetUnprocessed("policy1")
	require.NoError(t, err)
	assert.Len(t, unprocessed, 1)
	assert.Equal(t, "file2.txt", unprocessed[0].Key)

	// Get unprocessed for policy2 - should return both
	unprocessed, err = cl.GetUnprocessed("policy2")
	require.NoError(t, err)
	assert.Len(t, unprocessed, 2)
}

func TestMarkProcessed(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "changes.jsonl")

	cl, err := NewJSONLChangeLog(logPath, 1024*1024)
	require.NoError(t, err)
	defer cl.Close()

	// Record event
	event := ChangeEvent{
		Key:       "test.txt",
		Operation: "put",
	}
	err = cl.RecordChange(event)
	require.NoError(t, err)

	// Verify unprocessed
	unprocessed, err := cl.GetUnprocessed("policy1")
	require.NoError(t, err)
	assert.Len(t, unprocessed, 1)

	// Mark as processed
	err = cl.MarkProcessed("test.txt", "policy1")
	require.NoError(t, err)

	// Verify now processed
	unprocessed, err = cl.GetUnprocessed("policy1")
	require.NoError(t, err)
	assert.Len(t, unprocessed, 0)
}

func TestMarkProcessed_MultipleKeys(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "changes.jsonl")

	cl, err := NewJSONLChangeLog(logPath, 1024*1024)
	require.NoError(t, err)
	defer cl.Close()

	// Record multiple events
	for i := 1; i <= 3; i++ {
		event := ChangeEvent{
			Key:       filepath.Join("test", fmt.Sprintf("file%d.txt", i)),
			Operation: "put",
		}
		err := cl.RecordChange(event)
		require.NoError(t, err)
	}

	// Mark one as processed
	err = cl.MarkProcessed("test/file2.txt", "policy1")
	require.NoError(t, err)

	// Verify correct filtering
	unprocessed, err := cl.GetUnprocessed("policy1")
	require.NoError(t, err)
	assert.Len(t, unprocessed, 2)

	keys := make(map[string]bool)
	for _, e := range unprocessed {
		keys[e.Key] = true
	}
	assert.True(t, keys["test/file1.txt"])
	assert.True(t, keys["test/file3.txt"])
	assert.False(t, keys["test/file2.txt"])
}

func TestRotate(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "changes.jsonl")

	cl, err := NewJSONLChangeLog(logPath, 1024*1024)
	require.NoError(t, err)
	defer cl.Close()

	// Record event
	event := ChangeEvent{
		Key:       "test.txt",
		Operation: "put",
	}
	err = cl.RecordChange(event)
	require.NoError(t, err)

	// Rotate
	err = cl.Rotate()
	require.NoError(t, err)

	// Verify backup file exists
	files, err := os.ReadDir(tmpDir)
	require.NoError(t, err)

	foundBackup := false
	for _, file := range files {
		if file.Name() != "changes.jsonl" && !file.IsDir() {
			foundBackup = true
			break
		}
	}
	assert.True(t, foundBackup, "backup file should exist")

	// Verify new file is empty or small
	info, err := os.Stat(logPath)
	require.NoError(t, err)
	assert.Equal(t, int64(0), info.Size())

	// Verify can still write to new file
	event2 := ChangeEvent{
		Key:       "test2.txt",
		Operation: "put",
	}
	err = cl.RecordChange(event2)
	require.NoError(t, err)
}

func TestRotate_AutomaticOnMaxSize(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "changes.jsonl")

	// Set very small max size to trigger rotation
	cl, err := NewJSONLChangeLog(logPath, 100)
	require.NoError(t, err)
	defer cl.Close()

	// Record events until rotation
	for i := 0; i < 10; i++ {
		event := ChangeEvent{
			Key:       filepath.Join("test", fmt.Sprintf("file%d.txt", i)),
			Operation: "put",
			ETag:      "etag-very-long-string-to-increase-size",
			Size:      1024,
		}
		err := cl.RecordChange(event)
		require.NoError(t, err)
	}

	// Check for backup files
	files, err := os.ReadDir(tmpDir)
	require.NoError(t, err)

	backupCount := 0
	for _, file := range files {
		if file.Name() != "changes.jsonl" && !file.IsDir() {
			backupCount++
		}
	}

	// Should have at least one backup due to rotation
	assert.Greater(t, backupCount, 0, "should have created backup files")
}

func TestConcurrency(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "changes.jsonl")

	cl, err := NewJSONLChangeLog(logPath, 1024*1024)
	require.NoError(t, err)
	defer cl.Close()

	const numGoroutines = 100
	const eventsPerGoroutine = 10

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Concurrent writes
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < eventsPerGoroutine; j++ {
				event := ChangeEvent{
					Key:       filepath.Join(fmt.Sprintf("goroutine%d", id), fmt.Sprintf("file%d.txt", j)),
					Operation: "put",
					Size:      int64(id * j),
				}
				err := cl.RecordChange(event)
				assert.NoError(t, err)
			}
		}(i)
	}

	wg.Wait()

	// Verify all events were recorded
	unprocessed, err := cl.GetUnprocessed("policy1")
	require.NoError(t, err)
	assert.Equal(t, numGoroutines*eventsPerGoroutine, len(unprocessed))
}

func TestConcurrency_ReadWrite(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "changes.jsonl")

	cl, err := NewJSONLChangeLog(logPath, 1024*1024)
	require.NoError(t, err)
	defer cl.Close()

	const numWriters = 50
	const numReaders = 50
	const eventsPerWriter = 5

	var wg sync.WaitGroup
	wg.Add(numWriters + numReaders)

	// Concurrent writers
	for i := 0; i < numWriters; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < eventsPerWriter; j++ {
				event := ChangeEvent{
					Key:       filepath.Join(fmt.Sprintf("writer%d", id), fmt.Sprintf("file%d.txt", j)),
					Operation: "put",
				}
				_ = cl.RecordChange(event)
			}
		}(i)
	}

	// Concurrent readers
	for i := 0; i < numReaders; i++ {
		go func(id int) {
			defer wg.Done()
			_, _ = cl.GetUnprocessed(fmt.Sprintf("policy%d", id))
		}(i)
	}

	wg.Wait()
}

func TestLargeChangelog(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large changelog test in short mode")
	}

	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "changes.jsonl")

	cl, err := NewJSONLChangeLog(logPath, 10*1024*1024) // 10MB
	require.NoError(t, err)
	defer cl.Close()

	const numEvents = 1000

	// Record many events
	for i := 0; i < numEvents; i++ {
		event := ChangeEvent{
			Key:       filepath.Join("large", fmt.Sprintf("file%05d.txt", i)),
			Operation: "put",
			ETag:      fmt.Sprintf("etag-%05d", i),
			Size:      int64(i * 100),
		}
		err := cl.RecordChange(event)
		require.NoError(t, err)
	}

	// Verify all can be retrieved
	unprocessed, err := cl.GetUnprocessed("policy1")
	require.NoError(t, err)
	assert.Equal(t, numEvents, len(unprocessed))

	// Mark half as processed
	for i := 0; i < numEvents/2; i++ {
		key := filepath.Join("large", fmt.Sprintf("file%05d.txt", i))
		err := cl.MarkProcessed(key, "policy1")
		require.NoError(t, err)
	}

	// Verify correct count
	unprocessed, err = cl.GetUnprocessed("policy1")
	require.NoError(t, err)
	assert.Equal(t, numEvents/2, len(unprocessed))
}

func TestMultiplePolicies(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "changes.jsonl")

	cl, err := NewJSONLChangeLog(logPath, 1024*1024)
	require.NoError(t, err)
	defer cl.Close()

	// Record events
	events := []ChangeEvent{
		{Key: "file1.txt", Operation: "put"},
		{Key: "file2.txt", Operation: "put"},
		{Key: "file3.txt", Operation: "delete"},
	}

	for _, event := range events {
		err := cl.RecordChange(event)
		require.NoError(t, err)
	}

	// Mark different events as processed for different policies
	err = cl.MarkProcessed("file1.txt", "policy1")
	require.NoError(t, err)

	err = cl.MarkProcessed("file2.txt", "policy2")
	require.NoError(t, err)

	err = cl.MarkProcessed("file3.txt", "policy1")
	require.NoError(t, err)

	// Verify policy1 sees only file2.txt as unprocessed
	unprocessed1, err := cl.GetUnprocessed("policy1")
	require.NoError(t, err)
	assert.Len(t, unprocessed1, 1)
	assert.Equal(t, "file2.txt", unprocessed1[0].Key)

	// Verify policy2 sees file1.txt and file3.txt as unprocessed
	unprocessed2, err := cl.GetUnprocessed("policy2")
	require.NoError(t, err)
	assert.Len(t, unprocessed2, 2)

	keys := make(map[string]bool)
	for _, e := range unprocessed2 {
		keys[e.Key] = true
	}
	assert.True(t, keys["file1.txt"])
	assert.True(t, keys["file3.txt"])
}

func TestClose(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "changes.jsonl")

	cl, err := NewJSONLChangeLog(logPath, 1024*1024)
	require.NoError(t, err)

	err = cl.Close()
	require.NoError(t, err)

	// Verify file is closed (operations should fail)
	event := ChangeEvent{
		Key:       "test.txt",
		Operation: "put",
	}
	err = cl.RecordChange(event)
	assert.Error(t, err)
}

func TestClose_MultipleCloses(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "changes.jsonl")

	cl, err := NewJSONLChangeLog(logPath, 1024*1024)
	require.NoError(t, err)

	// First close should succeed
	err = cl.Close()
	require.NoError(t, err)

	// Second close should not panic
	err = cl.Close()
	// Error is acceptable since file is already closed
	_ = err
}

func TestChangeEvent_JSONSerialization(t *testing.T) {
	event := ChangeEvent{
		Key:       "test/file.txt",
		Operation: "put",
		Timestamp: time.Now(),
		ETag:      "etag123",
		Size:      1024,
		Processed: map[string]bool{
			"policy1": true,
			"policy2": false,
		},
	}

	// Marshal and unmarshal
	data, err := json.Marshal(event)
	require.NoError(t, err)

	var decoded ChangeEvent
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, event.Key, decoded.Key)
	assert.Equal(t, event.Operation, decoded.Operation)
	assert.Equal(t, event.ETag, decoded.ETag)
	assert.Equal(t, event.Size, decoded.Size)
	assert.Equal(t, event.Processed["policy1"], decoded.Processed["policy1"])
	assert.Equal(t, event.Processed["policy2"], decoded.Processed["policy2"])
}

func TestGetUnprocessed_EmptyLog(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "changes.jsonl")

	cl, err := NewJSONLChangeLog(logPath, 1024*1024)
	require.NoError(t, err)
	defer cl.Close()

	unprocessed, err := cl.GetUnprocessed("policy1")
	require.NoError(t, err)
	assert.Empty(t, unprocessed)
}

func TestMarkProcessed_NonexistentKey(t *testing.T) {
	tmpDir := t.TempDir()
	logPath := filepath.Join(tmpDir, "changes.jsonl")

	cl, err := NewJSONLChangeLog(logPath, 1024*1024)
	require.NoError(t, err)
	defer cl.Close()

	// Record one event
	event := ChangeEvent{
		Key:       "exists.txt",
		Operation: "put",
	}
	err = cl.RecordChange(event)
	require.NoError(t, err)

	// Mark non-existent key - should not error
	err = cl.MarkProcessed("nonexistent.txt", "policy1")
	require.NoError(t, err)

	// Verify original event is still unprocessed
	unprocessed, err := cl.GetUnprocessed("policy1")
	require.NoError(t, err)
	assert.Len(t, unprocessed, 1)
	assert.Equal(t, "exists.txt", unprocessed[0].Key)
}
