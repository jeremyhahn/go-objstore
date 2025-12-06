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

package memory

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

func TestNew(t *testing.T) {
	storage := New()
	if storage == nil {
		t.Fatal("New() returned nil")
	}
}

func TestConfigure(t *testing.T) {
	storage := New()
	err := storage.Configure(nil)
	if err != nil {
		t.Fatalf("Configure() returned error: %v", err)
	}

	err = storage.Configure(map[string]string{"any": "setting"})
	if err != nil {
		t.Fatalf("Configure() with settings returned error: %v", err)
	}
}

func TestPutAndGet(t *testing.T) {
	storage := New()
	_ = storage.Configure(nil)

	testData := []byte("hello world")
	err := storage.Put("test-key", bytes.NewReader(testData))
	if err != nil {
		t.Fatalf("Put() returned error: %v", err)
	}

	reader, err := storage.Get("test-key")
	if err != nil {
		t.Fatalf("Get() returned error: %v", err)
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("io.ReadAll() returned error: %v", err)
	}

	if !bytes.Equal(data, testData) {
		t.Fatalf("Get() returned wrong data: got %q, want %q", data, testData)
	}
}

func TestPutWithContext(t *testing.T) {
	storage := New()
	_ = storage.Configure(nil)

	ctx := context.Background()
	testData := []byte("context data")
	err := storage.PutWithContext(ctx, "context-key", bytes.NewReader(testData))
	if err != nil {
		t.Fatalf("PutWithContext() returned error: %v", err)
	}

	reader, err := storage.GetWithContext(ctx, "context-key")
	if err != nil {
		t.Fatalf("GetWithContext() returned error: %v", err)
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("io.ReadAll() returned error: %v", err)
	}

	if !bytes.Equal(data, testData) {
		t.Fatalf("GetWithContext() returned wrong data: got %q, want %q", data, testData)
	}
}

func TestPutWithMetadata(t *testing.T) {
	storage := New()
	_ = storage.Configure(nil)

	ctx := context.Background()
	testData := []byte("metadata data")
	metadata := &common.Metadata{
		ContentType: "text/plain",
		Custom:      map[string]string{"key": "value"},
	}

	err := storage.PutWithMetadata(ctx, "metadata-key", bytes.NewReader(testData), metadata)
	if err != nil {
		t.Fatalf("PutWithMetadata() returned error: %v", err)
	}

	gotMetadata, err := storage.GetMetadata(ctx, "metadata-key")
	if err != nil {
		t.Fatalf("GetMetadata() returned error: %v", err)
	}

	if gotMetadata.ContentType != "text/plain" {
		t.Fatalf("GetMetadata() returned wrong ContentType: got %q, want %q", gotMetadata.ContentType, "text/plain")
	}

	if gotMetadata.Custom["key"] != "value" {
		t.Fatalf("GetMetadata() returned wrong custom metadata: got %q, want %q", gotMetadata.Custom["key"], "value")
	}

	if gotMetadata.Size != int64(len(testData)) {
		t.Fatalf("GetMetadata() returned wrong size: got %d, want %d", gotMetadata.Size, len(testData))
	}
}

func TestGetNotFound(t *testing.T) {
	storage := New()
	_ = storage.Configure(nil)

	_, err := storage.Get("nonexistent-key")
	if err == nil {
		t.Fatal("Get() should return error for nonexistent key")
	}

	if !errors.Is(err, common.ErrKeyNotFound) {
		t.Fatalf("Get() should return ErrKeyNotFound, got: %v", err)
	}
}

func TestGetMetadataNotFound(t *testing.T) {
	storage := New()
	_ = storage.Configure(nil)

	ctx := context.Background()
	_, err := storage.GetMetadata(ctx, "nonexistent-key")
	if err == nil {
		t.Fatal("GetMetadata() should return error for nonexistent key")
	}

	if !errors.Is(err, common.ErrMetadataNotFound) {
		t.Fatalf("GetMetadata() should return ErrMetadataNotFound, got: %v", err)
	}
}

func TestUpdateMetadata(t *testing.T) {
	storage := New()
	_ = storage.Configure(nil)

	ctx := context.Background()
	testData := []byte("update metadata data")
	err := storage.Put("update-key", bytes.NewReader(testData))
	if err != nil {
		t.Fatalf("Put() returned error: %v", err)
	}

	newMetadata := &common.Metadata{
		ContentType: "application/json",
		Custom:      map[string]string{"updated": "true"},
	}

	err = storage.UpdateMetadata(ctx, "update-key", newMetadata)
	if err != nil {
		t.Fatalf("UpdateMetadata() returned error: %v", err)
	}

	gotMetadata, err := storage.GetMetadata(ctx, "update-key")
	if err != nil {
		t.Fatalf("GetMetadata() returned error: %v", err)
	}

	if gotMetadata.ContentType != "application/json" {
		t.Fatalf("UpdateMetadata() failed: ContentType is %q, want %q", gotMetadata.ContentType, "application/json")
	}

	if gotMetadata.Custom["updated"] != "true" {
		t.Fatalf("UpdateMetadata() failed: custom metadata not updated")
	}
}

func TestUpdateMetadataNotFound(t *testing.T) {
	storage := New()
	_ = storage.Configure(nil)

	ctx := context.Background()
	err := storage.UpdateMetadata(ctx, "nonexistent-key", &common.Metadata{})
	if err == nil {
		t.Fatal("UpdateMetadata() should return error for nonexistent key")
	}

	if !errors.Is(err, common.ErrKeyNotFound) {
		t.Fatalf("UpdateMetadata() should return ErrKeyNotFound, got: %v", err)
	}
}

func TestDelete(t *testing.T) {
	storage := New()
	_ = storage.Configure(nil)

	testData := []byte("delete me")
	err := storage.Put("delete-key", bytes.NewReader(testData))
	if err != nil {
		t.Fatalf("Put() returned error: %v", err)
	}

	err = storage.Delete("delete-key")
	if err != nil {
		t.Fatalf("Delete() returned error: %v", err)
	}

	_, err = storage.Get("delete-key")
	if err == nil {
		t.Fatal("Get() should return error after Delete()")
	}
}

func TestDeleteNotFound(t *testing.T) {
	storage := New()
	_ = storage.Configure(nil)

	err := storage.Delete("nonexistent-key")
	if err == nil {
		t.Fatal("Delete() should return error for nonexistent key")
	}

	if !errors.Is(err, common.ErrKeyNotFound) {
		t.Fatalf("Delete() should return ErrKeyNotFound, got: %v", err)
	}
}

func TestDeleteWithContext(t *testing.T) {
	storage := New()
	_ = storage.Configure(nil)

	ctx := context.Background()
	testData := []byte("delete me with context")
	err := storage.PutWithContext(ctx, "delete-ctx-key", bytes.NewReader(testData))
	if err != nil {
		t.Fatalf("PutWithContext() returned error: %v", err)
	}

	err = storage.DeleteWithContext(ctx, "delete-ctx-key")
	if err != nil {
		t.Fatalf("DeleteWithContext() returned error: %v", err)
	}

	_, err = storage.GetWithContext(ctx, "delete-ctx-key")
	if err == nil {
		t.Fatal("GetWithContext() should return error after DeleteWithContext()")
	}
}

func TestExists(t *testing.T) {
	storage := New()
	_ = storage.Configure(nil)

	ctx := context.Background()

	exists, err := storage.Exists(ctx, "nonexistent-key")
	if err != nil {
		t.Fatalf("Exists() returned error: %v", err)
	}
	if exists {
		t.Fatal("Exists() should return false for nonexistent key")
	}

	err = storage.Put("exists-key", bytes.NewReader([]byte("data")))
	if err != nil {
		t.Fatalf("Put() returned error: %v", err)
	}

	exists, err = storage.Exists(ctx, "exists-key")
	if err != nil {
		t.Fatalf("Exists() returned error: %v", err)
	}
	if !exists {
		t.Fatal("Exists() should return true for existing key")
	}
}

func TestList(t *testing.T) {
	storage := New()
	_ = storage.Configure(nil)

	// Put some test objects
	keys := []string{"prefix/a", "prefix/b", "prefix/c", "other/d"}
	for _, key := range keys {
		err := storage.Put(key, bytes.NewReader([]byte("data")))
		if err != nil {
			t.Fatalf("Put(%q) returned error: %v", key, err)
		}
	}

	// List with prefix
	result, err := storage.List("prefix/")
	if err != nil {
		t.Fatalf("List() returned error: %v", err)
	}

	if len(result) != 3 {
		t.Fatalf("List() returned %d keys, want 3", len(result))
	}

	for _, key := range result {
		if !strings.HasPrefix(key, "prefix/") {
			t.Fatalf("List() returned key without prefix: %q", key)
		}
	}
}

func TestListAll(t *testing.T) {
	storage := New()
	_ = storage.Configure(nil)

	keys := []string{"a", "b", "c"}
	for _, key := range keys {
		err := storage.Put(key, bytes.NewReader([]byte("data")))
		if err != nil {
			t.Fatalf("Put(%q) returned error: %v", key, err)
		}
	}

	result, err := storage.List("")
	if err != nil {
		t.Fatalf("List() returned error: %v", err)
	}

	if len(result) != 3 {
		t.Fatalf("List() returned %d keys, want 3", len(result))
	}
}

func TestListWithContext(t *testing.T) {
	storage := New()
	_ = storage.Configure(nil)

	ctx := context.Background()
	err := storage.PutWithContext(ctx, "ctx-key", bytes.NewReader([]byte("data")))
	if err != nil {
		t.Fatalf("PutWithContext() returned error: %v", err)
	}

	result, err := storage.ListWithContext(ctx, "")
	if err != nil {
		t.Fatalf("ListWithContext() returned error: %v", err)
	}

	if len(result) != 1 {
		t.Fatalf("ListWithContext() returned %d keys, want 1", len(result))
	}
}

func TestListWithOptions(t *testing.T) {
	storage := New()
	_ = storage.Configure(nil)

	ctx := context.Background()

	// Create hierarchical objects
	keys := []string{"dir/a.txt", "dir/b.txt", "dir/subdir/c.txt", "other/d.txt"}
	for _, key := range keys {
		err := storage.PutWithContext(ctx, key, bytes.NewReader([]byte("data")))
		if err != nil {
			t.Fatalf("PutWithContext(%q) returned error: %v", key, err)
		}
	}

	// Test with delimiter
	result, err := storage.ListWithOptions(ctx, &common.ListOptions{
		Prefix:    "dir/",
		Delimiter: "/",
	})
	if err != nil {
		t.Fatalf("ListWithOptions() returned error: %v", err)
	}

	if len(result.Objects) != 2 {
		t.Fatalf("ListWithOptions() returned %d objects, want 2", len(result.Objects))
	}

	if len(result.CommonPrefixes) != 1 {
		t.Fatalf("ListWithOptions() returned %d common prefixes, want 1", len(result.CommonPrefixes))
	}

	if result.CommonPrefixes[0] != "dir/subdir/" {
		t.Fatalf("ListWithOptions() returned wrong common prefix: got %q, want %q", result.CommonPrefixes[0], "dir/subdir/")
	}
}

func TestListWithOptionsPagination(t *testing.T) {
	storage := New()
	_ = storage.Configure(nil)

	ctx := context.Background()

	// Create multiple objects
	for i := 0; i < 5; i++ {
		key := string(rune('a'+i)) + ".txt"
		err := storage.PutWithContext(ctx, key, bytes.NewReader([]byte("data")))
		if err != nil {
			t.Fatalf("PutWithContext(%q) returned error: %v", key, err)
		}
	}

	// Get first page
	result, err := storage.ListWithOptions(ctx, &common.ListOptions{
		MaxResults: 2,
	})
	if err != nil {
		t.Fatalf("ListWithOptions() returned error: %v", err)
	}

	if len(result.Objects) != 2 {
		t.Fatalf("ListWithOptions() returned %d objects, want 2", len(result.Objects))
	}

	if !result.Truncated {
		t.Fatal("ListWithOptions() should indicate truncation")
	}

	// Get second page
	result2, err := storage.ListWithOptions(ctx, &common.ListOptions{
		MaxResults:   2,
		ContinueFrom: result.NextToken,
	})
	if err != nil {
		t.Fatalf("ListWithOptions() returned error: %v", err)
	}

	if len(result2.Objects) != 2 {
		t.Fatalf("ListWithOptions() second page returned %d objects, want 2", len(result2.Objects))
	}
}

func TestContextCancellation(t *testing.T) {
	storage := New()
	_ = storage.Configure(nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	err := storage.PutWithContext(ctx, "test", bytes.NewReader([]byte("data")))
	if err == nil {
		t.Fatal("PutWithContext() should return error for cancelled context")
	}

	_, err = storage.GetWithContext(ctx, "test")
	if err == nil {
		t.Fatal("GetWithContext() should return error for cancelled context")
	}

	err = storage.DeleteWithContext(ctx, "test")
	if err == nil {
		t.Fatal("DeleteWithContext() should return error for cancelled context")
	}

	_, err = storage.Exists(ctx, "test")
	if err == nil {
		t.Fatal("Exists() should return error for cancelled context")
	}

	_, err = storage.ListWithContext(ctx, "")
	if err == nil {
		t.Fatal("ListWithContext() should return error for cancelled context")
	}

	_, err = storage.ListWithOptions(ctx, nil)
	if err == nil {
		t.Fatal("ListWithOptions() should return error for cancelled context")
	}

	_, err = storage.GetMetadata(ctx, "test")
	if err == nil {
		t.Fatal("GetMetadata() should return error for cancelled context")
	}

	err = storage.UpdateMetadata(ctx, "test", nil)
	if err == nil {
		t.Fatal("UpdateMetadata() should return error for cancelled context")
	}
}

func TestInvalidKey(t *testing.T) {
	storage := New()
	_ = storage.Configure(nil)

	ctx := context.Background()

	// Test path traversal attack
	err := storage.PutWithContext(ctx, "../etc/passwd", bytes.NewReader([]byte("data")))
	if err == nil {
		t.Fatal("PutWithContext() should reject path traversal")
	}

	_, err = storage.GetWithContext(ctx, "../etc/passwd")
	if err == nil {
		t.Fatal("GetWithContext() should reject path traversal")
	}

	err = storage.DeleteWithContext(ctx, "../etc/passwd")
	if err == nil {
		t.Fatal("DeleteWithContext() should reject path traversal")
	}
}

func TestLifecyclePolicies(t *testing.T) {
	storage := New()
	_ = storage.Configure(nil)

	// Add a policy
	policy := common.LifecyclePolicy{
		ID:        "test-policy",
		Prefix:    "temp/",
		Retention: time.Hour,
		Action:    "delete",
	}

	err := storage.AddPolicy(policy)
	if err != nil {
		t.Fatalf("AddPolicy() returned error: %v", err)
	}

	// Get policies
	policies, err := storage.GetPolicies()
	if err != nil {
		t.Fatalf("GetPolicies() returned error: %v", err)
	}

	if len(policies) != 1 {
		t.Fatalf("GetPolicies() returned %d policies, want 1", len(policies))
	}

	if policies[0].ID != "test-policy" {
		t.Fatalf("GetPolicies() returned wrong policy ID: got %q, want %q", policies[0].ID, "test-policy")
	}

	// Remove policy
	err = storage.RemovePolicy("test-policy")
	if err != nil {
		t.Fatalf("RemovePolicy() returned error: %v", err)
	}

	policies, err = storage.GetPolicies()
	if err != nil {
		t.Fatalf("GetPolicies() returned error: %v", err)
	}

	if len(policies) != 0 {
		t.Fatalf("GetPolicies() returned %d policies after removal, want 0", len(policies))
	}
}

func TestClearAndCount(t *testing.T) {
	storage := New().(*Memory)
	_ = storage.Configure(nil)

	// Add some objects
	for i := 0; i < 5; i++ {
		key := string(rune('a' + i))
		err := storage.Put(key, bytes.NewReader([]byte("data")))
		if err != nil {
			t.Fatalf("Put() returned error: %v", err)
		}
	}

	if storage.Count() != 5 {
		t.Fatalf("Count() returned %d, want 5", storage.Count())
	}

	storage.Clear()

	if storage.Count() != 0 {
		t.Fatalf("Count() after Clear() returned %d, want 0", storage.Count())
	}
}

func TestDataIsolation(t *testing.T) {
	storage := New()
	_ = storage.Configure(nil)

	// Put data
	originalData := []byte("original data")
	err := storage.Put("test-key", bytes.NewReader(originalData))
	if err != nil {
		t.Fatalf("Put() returned error: %v", err)
	}

	// Get data and modify it
	reader, err := storage.Get("test-key")
	if err != nil {
		t.Fatalf("Get() returned error: %v", err)
	}
	data, _ := io.ReadAll(reader)
	reader.Close()

	// Modify the returned data
	data[0] = 'X'

	// Get data again and verify it's unchanged
	reader2, err := storage.Get("test-key")
	if err != nil {
		t.Fatalf("Get() returned error: %v", err)
	}
	data2, _ := io.ReadAll(reader2)
	reader2.Close()

	if !bytes.Equal(data2, originalData) {
		t.Fatalf("Data was modified: got %q, want %q", data2, originalData)
	}
}

func TestArchive(t *testing.T) {
	// Create source storage
	source := New()
	_ = source.Configure(nil)

	// Create destination storage (mock archiver)
	dest := New()
	_ = dest.Configure(nil)

	// Put data in source
	testData := []byte("archive me")
	err := source.Put("archive-key", bytes.NewReader(testData))
	if err != nil {
		t.Fatalf("Put() returned error: %v", err)
	}

	// Archive to destination
	err = source.Archive("archive-key", dest)
	if err != nil {
		t.Fatalf("Archive() returned error: %v", err)
	}

	// Verify data was copied to destination
	reader, err := dest.Get("archive-key")
	if err != nil {
		t.Fatalf("Get() from destination returned error: %v", err)
	}
	defer reader.Close()

	data, _ := io.ReadAll(reader)
	if !bytes.Equal(data, testData) {
		t.Fatalf("Archive() copied wrong data: got %q, want %q", data, testData)
	}
}

func TestArchiveNotFound(t *testing.T) {
	storage := New()
	_ = storage.Configure(nil)

	dest := New()
	_ = dest.Configure(nil)

	err := storage.Archive("nonexistent-key", dest)
	if err == nil {
		t.Fatal("Archive() should return error for nonexistent key")
	}
}

func TestArchiveNilDestination(t *testing.T) {
	storage := New()
	_ = storage.Configure(nil)

	err := storage.Put("test-key", bytes.NewReader([]byte("data")))
	if err != nil {
		t.Fatalf("Put() returned error: %v", err)
	}

	err = storage.Archive("test-key", nil)
	if err == nil {
		t.Fatal("Archive() should return error for nil destination")
	}

	if !errors.Is(err, common.ErrArchiveDestinationNil) {
		t.Fatalf("Archive() should return ErrArchiveDestinationNil, got: %v", err)
	}
}

func TestLifecycleManagerProcess(t *testing.T) {
	mem := &Memory{
		objects:          make(map[string]*object),
		lifecycleManager: NewLifecycleManager(),
	}

	lm := NewLifecycleManager()

	// Add an object with old timestamp
	err := mem.PutWithContext(context.Background(), "logs/old.txt", bytes.NewReader([]byte("old data")))
	if err != nil {
		t.Fatalf("PutWithContext() returned error: %v", err)
	}

	// Manually set the last modified time to be older than retention
	mem.mu.Lock()
	if obj, ok := mem.objects["logs/old.txt"]; ok {
		obj.metadata.LastModified = time.Now().Add(-48 * time.Hour)
	}
	mem.mu.Unlock()

	// Add a policy to delete objects older than 1 hour
	policy := common.LifecyclePolicy{
		ID:        "delete-old-logs",
		Prefix:    "logs/",
		Action:    "delete",
		Retention: time.Hour,
	}
	err = lm.AddPolicy(policy)
	if err != nil {
		t.Fatalf("AddPolicy() returned error: %v", err)
	}

	// Process the lifecycle
	lm.Process(mem)

	// Verify the old object was deleted
	exists, _ := mem.Exists(context.Background(), "logs/old.txt")
	if exists {
		t.Error("Process() should have deleted the old object")
	}
}

func TestLifecycleManagerProcessArchive(t *testing.T) {
	mem := &Memory{
		objects:          make(map[string]*object),
		lifecycleManager: NewLifecycleManager(),
	}

	dest := &Memory{
		objects:          make(map[string]*object),
		lifecycleManager: NewLifecycleManager(),
	}

	lm := NewLifecycleManager()

	// Add an object with old timestamp
	err := mem.PutWithContext(context.Background(), "archive/old.txt", bytes.NewReader([]byte("old data")))
	if err != nil {
		t.Fatalf("PutWithContext() returned error: %v", err)
	}

	// Manually set the last modified time to be older than retention
	mem.mu.Lock()
	if obj, ok := mem.objects["archive/old.txt"]; ok {
		obj.metadata.LastModified = time.Now().Add(-48 * time.Hour)
	}
	mem.mu.Unlock()

	// Add a policy to archive objects older than 1 hour
	policy := common.LifecyclePolicy{
		ID:          "archive-old",
		Prefix:      "archive/",
		Action:      "archive",
		Retention:   time.Hour,
		Destination: dest,
	}
	err = lm.AddPolicy(policy)
	if err != nil {
		t.Fatalf("AddPolicy() returned error: %v", err)
	}

	// Process the lifecycle
	lm.Process(mem)

	// Verify the object was archived to destination
	exists, _ := dest.Exists(context.Background(), "archive/old.txt")
	if !exists {
		t.Error("Process() should have archived the object to destination")
	}
}

func TestLifecycleManagerProcessNoPolicies(t *testing.T) {
	mem := &Memory{
		objects:          make(map[string]*object),
		lifecycleManager: NewLifecycleManager(),
	}

	lm := NewLifecycleManager()

	// Add an object
	err := mem.PutWithContext(context.Background(), "test.txt", bytes.NewReader([]byte("data")))
	if err != nil {
		t.Fatalf("PutWithContext() returned error: %v", err)
	}

	// Process with no policies - should do nothing
	lm.Process(mem)

	// Verify the object still exists
	exists, _ := mem.Exists(context.Background(), "test.txt")
	if !exists {
		t.Error("Process() with no policies should not delete objects")
	}
}

func TestLifecycleManagerProcessObjectNotOldEnough(t *testing.T) {
	mem := &Memory{
		objects:          make(map[string]*object),
		lifecycleManager: NewLifecycleManager(),
	}

	lm := NewLifecycleManager()

	// Add a fresh object
	err := mem.PutWithContext(context.Background(), "logs/new.txt", bytes.NewReader([]byte("new data")))
	if err != nil {
		t.Fatalf("PutWithContext() returned error: %v", err)
	}

	// Add a policy to delete objects older than 1 hour
	policy := common.LifecyclePolicy{
		ID:        "delete-old-logs",
		Prefix:    "logs/",
		Action:    "delete",
		Retention: time.Hour,
	}
	err = lm.AddPolicy(policy)
	if err != nil {
		t.Fatalf("AddPolicy() returned error: %v", err)
	}

	// Process the lifecycle
	lm.Process(mem)

	// Verify the new object was NOT deleted
	exists, _ := mem.Exists(context.Background(), "logs/new.txt")
	if !exists {
		t.Error("Process() should NOT delete objects that are not old enough")
	}
}
