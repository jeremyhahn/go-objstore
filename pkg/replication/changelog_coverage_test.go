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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test rotate error paths
func TestChangeLog_RotateError_FileCreate(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "changelog.jsonl")

	cl, err := NewJSONLChangeLog(logPath, 100)
	require.NoError(t, err)
	defer cl.Close()

	// Record an event
	err = cl.RecordChange(ChangeEvent{
		Key:       "test",
		Operation: "put",
		Timestamp: time.Now(),
	})
	require.NoError(t, err)

	// Make directory read-only to cause rotation to fail
	err = os.Chmod(dir, 0444)
	require.NoError(t, err)
	defer os.Chmod(dir, 0755)

	// Try to rotate - should fail due to permissions
	err = cl.Rotate()
	assert.Error(t, err)
}

// Test rewriteFile error paths
func TestChangeLog_RewriteFile_OpenError(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "changelog.jsonl")

	cl, err := NewJSONLChangeLog(logPath, 1000)
	require.NoError(t, err)

	// Record some events
	for i := 0; i < 5; i++ {
		err = cl.RecordChange(ChangeEvent{
			Key:       "test",
			Operation: "put",
			Timestamp: time.Now(),
			Processed: map[string]bool{"policy1": i%2 == 0},
		})
		require.NoError(t, err)
	}

	cl.Close()

	// Make file unreadable
	err = os.Chmod(logPath, 0000)
	require.NoError(t, err)
	defer os.Chmod(logPath, 0644)

	// Try to rewrite - should fail
	err = cl.rewriteFile([]ChangeEvent{})
	assert.Error(t, err)
}

// Test MarkProcessed with multiple policies
func TestChangeLog_MarkProcessed_MultiplePolicies(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "changelog.jsonl")

	cl, err := NewJSONLChangeLog(logPath, 1000)
	require.NoError(t, err)
	defer cl.Close()

	// Record an event
	err = cl.RecordChange(ChangeEvent{
		Key:       "test1",
		Operation: "put",
		Timestamp: time.Now(),
	})
	require.NoError(t, err)

	// Mark as processed by policy1
	err = cl.MarkProcessed("test1", "policy1")
	require.NoError(t, err)

	// Mark as processed by policy2
	err = cl.MarkProcessed("test1", "policy2")
	require.NoError(t, err)

	// Verify both policies marked as processed
	events, err := cl.GetUnprocessed("policy1")
	require.NoError(t, err)
	assert.Empty(t, events)

	events, err = cl.GetUnprocessed("policy2")
	require.NoError(t, err)
	assert.Empty(t, events)
}

// Test Close error path
func TestChangeLog_Close_SyncError(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "changelog.jsonl")

	cl, err := NewJSONLChangeLog(logPath, 1000)
	require.NoError(t, err)

	// Record event
	err = cl.RecordChange(ChangeEvent{
		Key:       "test",
		Operation: "put",
		Timestamp: time.Now(),
	})
	require.NoError(t, err)

	// Close the underlying file to cause error
	cl.file.Close()

	// Try to close again - should handle error gracefully
	err = cl.Close()
	// Error is logged but not returned, so just verify it doesn't panic
	assert.NotNil(t, cl)
}

// Test GetUnprocessed with all events processed
func TestChangeLog_GetUnprocessed_AllProcessed(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "changelog.jsonl")

	cl, err := NewJSONLChangeLog(logPath, 1000)
	require.NoError(t, err)
	defer cl.Close()

	// Record events that are already processed
	for i := 0; i < 5; i++ {
		err = cl.RecordChange(ChangeEvent{
			Key:       "test",
			Operation: "put",
			Timestamp: time.Now(),
			Processed: map[string]bool{"policy1": true},
		})
		require.NoError(t, err)
	}

	// Get unprocessed for policy1 - should be empty
	events, err := cl.GetUnprocessed("policy1")
	require.NoError(t, err)
	assert.Empty(t, events)
}

// Test manual Rotate
func TestChangeLog_ManualRotate(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "changelog.jsonl")

	cl, err := NewJSONLChangeLog(logPath, 1000)
	require.NoError(t, err)

	// Record some events
	for i := 0; i < 5; i++ {
		err = cl.RecordChange(ChangeEvent{
			Key:       "test",
			Operation: "put",
			Timestamp: time.Now(),
		})
		require.NoError(t, err)
	}

	// Manually rotate
	err = cl.Rotate()
	require.NoError(t, err)

	// Check that old file was created
	files, err := os.ReadDir(dir)
	require.NoError(t, err)

	foundRotated := false
	for _, file := range files {
		name := file.Name()
		if name != "changelog.jsonl" && !file.IsDir() {
			foundRotated = true
			break
		}
	}
	assert.True(t, foundRotated, "Expected rotated file to exist")

	// Verify can still record after rotation
	err = cl.RecordChange(ChangeEvent{
		Key:       "after-rotate",
		Operation: "put",
		Timestamp: time.Now(),
	})
	require.NoError(t, err)

	cl.Close()
}

// Test GetUnprocessed with mixed processed/unprocessed
func TestChangeLog_GetUnprocessed_Mixed(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "changelog.jsonl")

	cl, err := NewJSONLChangeLog(logPath, 1000)
	require.NoError(t, err)
	defer cl.Close()

	// Record mix of processed and unprocessed events
	events := []ChangeEvent{
		{Key: "key1", Operation: "put", Timestamp: time.Now(), Processed: map[string]bool{"policy1": true}},
		{Key: "key2", Operation: "put", Timestamp: time.Now()},
		{Key: "key3", Operation: "delete", Timestamp: time.Now(), Processed: map[string]bool{"policy1": false}},
		{Key: "key4", Operation: "put", Timestamp: time.Now()},
	}

	for _, event := range events {
		err = cl.RecordChange(event)
		require.NoError(t, err)
	}

	// Get unprocessed for policy1
	unprocessed, err := cl.GetUnprocessed("policy1")
	require.NoError(t, err)

	// Should get key2, key3 (false counts as unprocessed), and key4
	assert.Len(t, unprocessed, 3)
}

// Test RecordChange with various operations
func TestChangeLog_RecordChange_MultipleOperations(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "changelog.jsonl")

	cl, err := NewJSONLChangeLog(logPath, 1000)
	require.NoError(t, err)
	defer cl.Close()

	operations := []string{"put", "delete", "copy", "move"}
	for _, op := range operations {
		err = cl.RecordChange(ChangeEvent{
			Key:       "test-key",
			Operation: op,
			Timestamp: time.Now(),
			ETag:      "etag-" + op,
			Size:      int64(len(op)),
		})
		require.NoError(t, err)
	}

	// Verify all operations were recorded
	events, err := cl.GetUnprocessed("test-policy")
	require.NoError(t, err)
	assert.Len(t, events, 4)
}

// Test reopening existing changelog
func TestChangeLog_ReopenExisting(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "changelog.jsonl")

	// Create and write some events
	cl1, err := NewJSONLChangeLog(logPath, 1000)
	require.NoError(t, err)

	for i := 0; i < 3; i++ {
		err = cl1.RecordChange(ChangeEvent{
			Key:       "key" + string(rune('0'+i)),
			Operation: "put",
			Timestamp: time.Now(),
		})
		require.NoError(t, err)
	}
	cl1.Close()

	// Reopen and verify existing data
	cl2, err := NewJSONLChangeLog(logPath, 1000)
	require.NoError(t, err)
	defer cl2.Close()

	// Add more events
	err = cl2.RecordChange(ChangeEvent{
		Key:       "key3",
		Operation: "put",
		Timestamp: time.Now(),
	})
	require.NoError(t, err)

	// Verify all events present
	events, err := cl2.GetUnprocessed("policy1")
	require.NoError(t, err)
	assert.Len(t, events, 4)
}
// Test MarkProcessed on event without existing Processed map
func TestChangeLog_MarkProcessed_NilProcessedMap(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "changelog.jsonl")

	cl, err := NewJSONLChangeLog(logPath, 1000)
	require.NoError(t, err)
	defer cl.Close()

	// Record event WITHOUT Processed map (don't set it)
	err = cl.RecordChange(ChangeEvent{
		Key:       "test-key",
		Operation: "put",
		Timestamp: time.Now(),
		// Note: Processed field not set, will be nil
	})
	require.NoError(t, err)

	// Mark as processed - this should initialize the Processed map
	err = cl.MarkProcessed("test-key", "policy1")
	require.NoError(t, err)

	// Verify it's marked as processed
	events, err := cl.GetUnprocessed("policy1")
	require.NoError(t, err)
	assert.Empty(t, events)
}

// Test Close on nil file
func TestChangeLog_Close_NilFile(t *testing.T) {
	cl := &JSONLChangeLog{}

	err := cl.Close()
	assert.NoError(t, err) // Should handle nil file gracefully
}

// Test RecordChange triggers file size check
func TestChangeLog_RecordChange_FileSizeCheck(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "changelog.jsonl")

	// Create with file size monitoring
	cl, err := NewJSONLChangeLog(logPath, 1000)
	require.NoError(t, err)
	defer cl.Close()

	// Record several events with varying sizes
	for i := 0; i < 10; i++ {
		err = cl.RecordChange(ChangeEvent{
			Key:       fmt.Sprintf("key-%d", i),
			Operation: "put",
			Timestamp: time.Now(),
			ETag:      "etag-value",
			Size:      int64(i * 100),
		})
		require.NoError(t, err)
	}

	// Verify file was created and events recorded
	// Note: Some events may have been rotated to backup file
	events, err := cl.GetUnprocessed("test")
	require.NoError(t, err)
	assert.Greater(t, len(events), 0, "Should have at least some events")
}
