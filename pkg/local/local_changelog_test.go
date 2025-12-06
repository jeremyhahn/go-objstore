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

package local

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockChangeLog implements ChangeLog for testing
type mockChangeLog struct {
	events []ChangeEvent
}

func newMockChangeLog() *mockChangeLog {
	return &mockChangeLog{
		events: make([]ChangeEvent, 0),
	}
}

func (m *mockChangeLog) RecordChange(event ChangeEvent) error {
	m.events = append(m.events, event)
	return nil
}

func (m *mockChangeLog) getEvents() []ChangeEvent {
	return m.events
}

func (m *mockChangeLog) clear() {
	m.events = make([]ChangeEvent, 0)
}

func TestSetChangeLog(t *testing.T) {
	tmpDir := t.TempDir()

	local := New()
	err := local.Configure(map[string]string{"path": tmpDir})
	require.NoError(t, err)

	localBackend := local.(*Local)

	// Initially no change log
	assert.Nil(t, localBackend.changeLog)

	// Set change log
	changeLog := newMockChangeLog()
	localBackend.SetChangeLog(changeLog)

	// Verify it was set
	assert.NotNil(t, localBackend.changeLog)
	assert.Equal(t, changeLog, localBackend.changeLog)
}

func TestPutRecordsChange(t *testing.T) {
	tmpDir := t.TempDir()

	local := New()
	err := local.Configure(map[string]string{"path": tmpDir})
	require.NoError(t, err)

	localBackend := local.(*Local)
	changeLog := newMockChangeLog()
	localBackend.SetChangeLog(changeLog)

	// Put an object
	testData := []byte("test data for changelog")
	err = local.Put("test/file.txt", bytes.NewReader(testData))
	require.NoError(t, err)

	// Verify change was recorded
	events := changeLog.getEvents()
	require.Len(t, events, 1)

	event := events[0]
	assert.Equal(t, "test/file.txt", event.Key)
	assert.Equal(t, "put", event.Operation)
	assert.NotEmpty(t, event.ETag)
	assert.Equal(t, int64(len(testData)), event.Size)
	assert.False(t, event.Timestamp.IsZero())
}

func TestPutMultipleRecordsChanges(t *testing.T) {
	tmpDir := t.TempDir()

	local := New()
	err := local.Configure(map[string]string{"path": tmpDir})
	require.NoError(t, err)

	localBackend := local.(*Local)
	changeLog := newMockChangeLog()
	localBackend.SetChangeLog(changeLog)

	// Put multiple objects
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("test/file%d.txt", i)
		data := []byte(fmt.Sprintf("test data %d", i))
		err := local.Put(key, bytes.NewReader(data))
		require.NoError(t, err)
	}

	// Verify all changes were recorded
	events := changeLog.getEvents()
	require.Len(t, events, 5)

	for i, event := range events {
		expectedKey := fmt.Sprintf("test/file%d.txt", i)
		assert.Equal(t, expectedKey, event.Key)
		assert.Equal(t, "put", event.Operation)
	}
}

func TestDeleteRecordsChange(t *testing.T) {
	tmpDir := t.TempDir()

	local := New()
	err := local.Configure(map[string]string{"path": tmpDir})
	require.NoError(t, err)

	localBackend := local.(*Local)

	// Put an object first (without changelog)
	testData := []byte("test data")
	err = local.Put("test/file.txt", bytes.NewReader(testData))
	require.NoError(t, err)

	// Now set changelog and delete
	changeLog := newMockChangeLog()
	localBackend.SetChangeLog(changeLog)

	err = local.Delete("test/file.txt")
	require.NoError(t, err)

	// Verify delete was recorded
	events := changeLog.getEvents()
	require.Len(t, events, 1)

	event := events[0]
	assert.Equal(t, "test/file.txt", event.Key)
	assert.Equal(t, "delete", event.Operation)
	assert.False(t, event.Timestamp.IsZero())
}

func TestDeleteMultipleRecordsChanges(t *testing.T) {
	tmpDir := t.TempDir()

	local := New()
	err := local.Configure(map[string]string{"path": tmpDir})
	require.NoError(t, err)

	localBackend := local.(*Local)

	// Put multiple objects first
	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("test/file%d.txt", i)
		data := []byte(fmt.Sprintf("test data %d", i))
		err := local.Put(key, bytes.NewReader(data))
		require.NoError(t, err)
	}

	// Set changelog and delete all
	changeLog := newMockChangeLog()
	localBackend.SetChangeLog(changeLog)

	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("test/file%d.txt", i)
		err := local.Delete(key)
		require.NoError(t, err)
	}

	// Verify all deletes were recorded
	events := changeLog.getEvents()
	require.Len(t, events, 3)

	for i, event := range events {
		expectedKey := fmt.Sprintf("test/file%d.txt", i)
		assert.Equal(t, expectedKey, event.Key)
		assert.Equal(t, "delete", event.Operation)
	}
}

func TestWithoutChangeLog(t *testing.T) {
	tmpDir := t.TempDir()

	local := New()
	err := local.Configure(map[string]string{"path": tmpDir})
	require.NoError(t, err)

	// No change log set - operations should still work

	// Put
	testData := []byte("test data")
	err = local.Put("test/file.txt", bytes.NewReader(testData))
	require.NoError(t, err)

	// Get
	reader, err := local.Get("test/file.txt")
	require.NoError(t, err)
	reader.Close()

	// Delete
	err = local.Delete("test/file.txt")
	require.NoError(t, err)
}

func TestPutDeleteSequence(t *testing.T) {
	tmpDir := t.TempDir()

	local := New()
	err := local.Configure(map[string]string{"path": tmpDir})
	require.NoError(t, err)

	localBackend := local.(*Local)
	changeLog := newMockChangeLog()
	localBackend.SetChangeLog(changeLog)

	// Put, delete, put again sequence
	testData := []byte("test data")

	err = local.Put("test/file.txt", bytes.NewReader(testData))
	require.NoError(t, err)

	err = local.Delete("test/file.txt")
	require.NoError(t, err)

	err = local.Put("test/file.txt", bytes.NewReader(testData))
	require.NoError(t, err)

	// Verify sequence was recorded
	events := changeLog.getEvents()
	require.Len(t, events, 3)

	assert.Equal(t, "put", events[0].Operation)
	assert.Equal(t, "delete", events[1].Operation)
	assert.Equal(t, "put", events[2].Operation)

	// All should have same key
	for _, event := range events {
		assert.Equal(t, "test/file.txt", event.Key)
	}
}

func TestPutWithContext(t *testing.T) {
	tmpDir := t.TempDir()

	local := New()
	err := local.Configure(map[string]string{"path": tmpDir})
	require.NoError(t, err)

	localBackend := local.(*Local)
	changeLog := newMockChangeLog()
	localBackend.SetChangeLog(changeLog)

	// PutWithContext should also record
	ctx := context.Background()
	testData := []byte("test data")
	err = local.PutWithContext(ctx, "test/file.txt", bytes.NewReader(testData))
	require.NoError(t, err)

	events := changeLog.getEvents()
	require.Len(t, events, 1)
	assert.Equal(t, "put", events[0].Operation)
}

func TestDeleteWithContext(t *testing.T) {
	tmpDir := t.TempDir()

	local := New()
	err := local.Configure(map[string]string{"path": tmpDir})
	require.NoError(t, err)

	localBackend := local.(*Local)

	// Put first
	testData := []byte("test data")
	err = local.Put("test/file.txt", bytes.NewReader(testData))
	require.NoError(t, err)

	// Set changelog and delete with context
	changeLog := newMockChangeLog()
	localBackend.SetChangeLog(changeLog)

	ctx := context.Background()
	err = local.DeleteWithContext(ctx, "test/file.txt")
	require.NoError(t, err)

	events := changeLog.getEvents()
	require.Len(t, events, 1)
	assert.Equal(t, "delete", events[0].Operation)
}

func TestChangeLogMetadata(t *testing.T) {
	tmpDir := t.TempDir()

	local := New()
	err := local.Configure(map[string]string{"path": tmpDir})
	require.NoError(t, err)

	localBackend := local.(*Local)
	changeLog := newMockChangeLog()
	localBackend.SetChangeLog(changeLog)

	// Put with specific data to check size and ETag
	testData := []byte("hello world test data")
	err = local.Put("test/file.txt", bytes.NewReader(testData))
	require.NoError(t, err)

	events := changeLog.getEvents()
	require.Len(t, events, 1)

	event := events[0]
	// Verify metadata fields
	assert.Equal(t, int64(len(testData)), event.Size, "size should match data length")
	assert.NotEmpty(t, event.ETag, "ETag should be set")
	assert.Contains(t, event.ETag, "-", "ETag should contain timestamp and size")
}

func TestChangeLogDisableReEnable(t *testing.T) {
	tmpDir := t.TempDir()

	local := New()
	err := local.Configure(map[string]string{"path": tmpDir})
	require.NoError(t, err)

	localBackend := local.(*Local)

	// Enable changelog
	changeLog := newMockChangeLog()
	localBackend.SetChangeLog(changeLog)

	testData := []byte("test data 1")
	err = local.Put("test/file1.txt", bytes.NewReader(testData))
	require.NoError(t, err)

	assert.Len(t, changeLog.getEvents(), 1)

	// Disable changelog (set to nil)
	localBackend.SetChangeLog(nil)

	// This put should not be recorded
	err = local.Put("test/file2.txt", bytes.NewReader(testData))
	require.NoError(t, err)

	assert.Len(t, changeLog.getEvents(), 1) // Still 1, not 2

	// Re-enable
	localBackend.SetChangeLog(changeLog)

	err = local.Put("test/file3.txt", bytes.NewReader(testData))
	require.NoError(t, err)

	assert.Len(t, changeLog.getEvents(), 2) // Now 2
}
