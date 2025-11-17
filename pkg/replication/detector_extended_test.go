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
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// TestDetectChanges_LargeDataset tests change detection with many objects.
func TestDetectChanges_LargeDataset(t *testing.T) {
	source := newMockStorage()
	dest := newMockStorage()

	now := time.Now()
	numObjects := 100

	// Add many objects to source
	for i := 0; i < numObjects; i++ {
		key := "large-dataset-" + string(rune('0'+i%10)) + "-" + string(rune('0'+i/10)) + ".txt"
		source.objects[key] = &common.Metadata{
			Size:         int64(i * 100),
			ETag:         "etag-" + string(rune('0'+i%10)),
			LastModified: now,
		}
	}

	// Add half of them to destination (with same metadata)
	for i := 0; i < numObjects/2; i++ {
		key := "large-dataset-" + string(rune('0'+i%10)) + "-" + string(rune('0'+i/10)) + ".txt"
		dest.objects[key] = source.objects[key]
	}

	detector := NewChangeDetector(source, dest)
	changed, err := detector.DetectChanges(context.Background(), "")

	if err != nil {
		t.Fatalf("DetectChanges failed: %v", err)
	}

	// Should detect the half that are missing from destination
	if len(changed) != numObjects/2 {
		t.Errorf("Expected %d changed objects, got %d", numObjects/2, len(changed))
	}
}

// TestDetectChanges_AllObjectsChanged tests when all objects have changed.
func TestDetectChanges_AllObjectsChanged(t *testing.T) {
	source := newMockStorage()
	dest := newMockStorage()

	now := time.Now()
	oldTime := now.Add(-1 * time.Hour)

	// Add objects to both source and dest with different metadata
	for i := 1; i <= 10; i++ {
		key := "changed-" + string(rune('0'+i)) + ".txt"

		source.objects[key] = &common.Metadata{
			Size:         int64(i * 100),
			ETag:         "etag-new-" + string(rune('0'+i)),
			LastModified: now,
		}

		dest.objects[key] = &common.Metadata{
			Size:         int64(i * 50),
			ETag:         "etag-old-" + string(rune('0'+i)),
			LastModified: oldTime,
		}
	}

	detector := NewChangeDetector(source, dest)
	changed, err := detector.DetectChanges(context.Background(), "")

	if err != nil {
		t.Fatalf("DetectChanges failed: %v", err)
	}

	if len(changed) != 10 {
		t.Errorf("Expected 10 changed objects, got %d", len(changed))
	}
}

// TestDetectChanges_CustomMetadata tests change detection with custom metadata.
func TestDetectChanges_CustomMetadata(t *testing.T) {
	source := newMockStorage()
	dest := newMockStorage()

	now := time.Now()

	// Add object to source with custom metadata
	source.objects["custom-meta.txt"] = &common.Metadata{
		Size:         1000,
		ETag:         "etag123",
		LastModified: now,
		ContentType:  "text/plain",
		Custom: map[string]string{
			"author":  "test-user",
			"version": "2.0",
		},
	}

	// Add same object to dest with different custom metadata
	dest.objects["custom-meta.txt"] = &common.Metadata{
		Size:         1000,
		ETag:         "etag123", // Same ETag
		LastModified: now,       // Same timestamp
		ContentType:  "text/plain",
		Custom: map[string]string{
			"author":  "test-user",
			"version": "1.0", // Different version
		},
	}

	detector := NewChangeDetector(source, dest)
	changed, err := detector.DetectChanges(context.Background(), "")

	if err != nil {
		t.Fatalf("DetectChanges failed: %v", err)
	}

	// With same ETag and timestamp, should NOT be detected as changed
	// (custom metadata is not compared in hasChanged logic)
	if len(changed) != 0 {
		t.Errorf("Expected 0 changed objects, got %d", len(changed))
	}
}

// TestDetectChanges_MixedScenarios tests various mixed scenarios.
func TestDetectChanges_MixedScenarios(t *testing.T) {
	source := newMockStorage()
	dest := newMockStorage()

	now := time.Now()
	oldTime := now.Add(-1 * time.Hour)

	// Scenario 1: Object only in source (new)
	source.objects["new-object.txt"] = &common.Metadata{
		Size:         100,
		ETag:         "etag-new",
		LastModified: now,
	}

	// Scenario 2: Object unchanged (same metadata)
	source.objects["unchanged.txt"] = &common.Metadata{
		Size:         200,
		ETag:         "etag-same",
		LastModified: now,
	}
	dest.objects["unchanged.txt"] = &common.Metadata{
		Size:         200,
		ETag:         "etag-same",
		LastModified: now,
	}

	// Scenario 3: Object with different ETag
	source.objects["different-etag.txt"] = &common.Metadata{
		Size:         300,
		ETag:         "etag-new",
		LastModified: now,
	}
	dest.objects["different-etag.txt"] = &common.Metadata{
		Size:         300,
		ETag:         "etag-old",
		LastModified: now,
	}

	// Scenario 4: Object with different size
	source.objects["different-size.txt"] = &common.Metadata{
		Size:         500,
		ETag:         "",
		LastModified: now,
	}
	dest.objects["different-size.txt"] = &common.Metadata{
		Size:         400,
		ETag:         "",
		LastModified: now,
	}

	// Scenario 5: Object with newer timestamp
	source.objects["newer-time.txt"] = &common.Metadata{
		Size:         600,
		ETag:         "",
		LastModified: now,
	}
	dest.objects["newer-time.txt"] = &common.Metadata{
		Size:         600,
		ETag:         "",
		LastModified: oldTime,
	}

	// Scenario 6: Object with older timestamp (should not sync)
	source.objects["older-time.txt"] = &common.Metadata{
		Size:         700,
		ETag:         "",
		LastModified: oldTime,
	}
	dest.objects["older-time.txt"] = &common.Metadata{
		Size:         700,
		ETag:         "",
		LastModified: now,
	}

	detector := NewChangeDetector(source, dest)
	changed, err := detector.DetectChanges(context.Background(), "")

	if err != nil {
		t.Fatalf("DetectChanges failed: %v", err)
	}

	// Should detect: new-object, different-etag, different-size, newer-time
	// Should NOT detect: unchanged, older-time
	expectedChanges := 4
	if len(changed) != expectedChanges {
		t.Errorf("Expected %d changed objects, got %d", expectedChanges, len(changed))
		t.Logf("Changed objects: %v", changed)
	}

	// Verify specific objects are in the changed list
	changedMap := make(map[string]bool)
	for _, key := range changed {
		changedMap[key] = true
	}

	expectedChanged := []string{"new-object.txt", "different-etag.txt", "different-size.txt", "newer-time.txt"}
	for _, key := range expectedChanged {
		if !changedMap[key] {
			t.Errorf("Expected %s to be in changed list", key)
		}
	}

	// Verify unchanged objects are NOT in the list
	if changedMap["unchanged.txt"] {
		t.Error("unchanged.txt should not be in changed list")
	}
	if changedMap["older-time.txt"] {
		t.Error("older-time.txt should not be in changed list")
	}
}

// TestDetectChanges_EmptySource tests detection with empty source.
func TestDetectChanges_EmptySource(t *testing.T) {
	source := newMockStorage()
	dest := newMockStorage()

	// Add objects to dest but not source
	dest.objects["orphan.txt"] = &common.Metadata{
		Size:         100,
		ETag:         "etag",
		LastModified: time.Now(),
	}

	detector := NewChangeDetector(source, dest)
	changed, err := detector.DetectChanges(context.Background(), "")

	if err != nil {
		t.Fatalf("DetectChanges failed: %v", err)
	}

	// Empty source means no objects to sync
	if len(changed) != 0 {
		t.Errorf("Expected 0 changed objects from empty source, got %d", len(changed))
	}
}

// TestDetectChanges_EmptyDestination tests detection with empty destination.
func TestDetectChanges_EmptyDestination(t *testing.T) {
	source := newMockStorage()
	dest := newMockStorage()

	now := time.Now()

	// Add objects to source but not dest
	for i := 1; i <= 5; i++ {
		key := "source-only-" + string(rune('0'+i)) + ".txt"
		source.objects[key] = &common.Metadata{
			Size:         int64(i * 100),
			ETag:         "etag" + string(rune('0'+i)),
			LastModified: now,
		}
	}

	detector := NewChangeDetector(source, dest)
	changed, err := detector.DetectChanges(context.Background(), "")

	if err != nil {
		t.Fatalf("DetectChanges failed: %v", err)
	}

	// All source objects should be detected as changed
	if len(changed) != 5 {
		t.Errorf("Expected 5 changed objects, got %d", len(changed))
	}
}

// TestDetectChanges_NilMetadata tests handling of nil metadata.
func TestDetectChanges_NilMetadata(t *testing.T) {
	source := newMockStorage()
	dest := newMockStorage()

	now := time.Now()

	// Add object to source
	source.objects["test.txt"] = &common.Metadata{
		Size:         100,
		ETag:         "etag",
		LastModified: now,
	}

	// Simulate destination returning error (nil metadata)
	dest.getMetaError = common.ErrKeyNotFound

	detector := NewChangeDetector(source, dest)
	changed, err := detector.DetectChanges(context.Background(), "")

	if err != nil {
		t.Fatalf("DetectChanges failed: %v", err)
	}

	// Should treat as changed when metadata fetch fails
	if len(changed) != 1 {
		t.Errorf("Expected 1 changed object, got %d", len(changed))
	}

	if changed[0] != "test.txt" {
		t.Errorf("Expected 'test.txt', got '%s'", changed[0])
	}
}

// TestDetectChanges_PrefixFiltering tests prefix-based filtering.
func TestDetectChanges_PrefixFiltering(t *testing.T) {
	source := newMockStorage()
	dest := newMockStorage()

	now := time.Now()

	// Add objects with various prefixes
	prefixes := []string{"photos/", "documents/", "videos/", "music/"}
	for _, prefix := range prefixes {
		for i := 1; i <= 3; i++ {
			key := prefix + "file" + string(rune('0'+i)) + ".txt"
			source.objects[key] = &common.Metadata{
				Size:         int64(i * 100),
				ETag:         "etag" + string(rune('0'+i)),
				LastModified: now,
			}
		}
	}

	detector := NewChangeDetector(source, dest)

	// Test each prefix separately
	for _, prefix := range prefixes {
		changed, err := detector.DetectChanges(context.Background(), prefix)

		if err != nil {
			t.Fatalf("DetectChanges failed for prefix %s: %v", prefix, err)
		}

		if len(changed) != 3 {
			t.Errorf("Expected 3 changed objects for prefix %s, got %d", prefix, len(changed))
		}

		// Verify all returned keys have the correct prefix
		for _, key := range changed {
			if len(key) < len(prefix) || key[:len(prefix)] != prefix {
				t.Errorf("Key %s does not have prefix %s", key, prefix)
			}
		}
	}
}

// TestDetectChanges_ConcurrentAccess tests concurrent change detection.
func TestDetectChanges_ConcurrentAccess(t *testing.T) {
	source := newMockStorage()
	dest := newMockStorage()

	now := time.Now()

	// Add test objects
	for i := 1; i <= 20; i++ {
		key := "concurrent-" + string(rune('0'+i%10)) + ".txt"
		source.objects[key] = &common.Metadata{
			Size:         int64(i * 100),
			ETag:         "etag" + string(rune('0'+i%10)),
			LastModified: now,
		}
	}

	detector := NewChangeDetector(source, dest)

	// Run multiple detections concurrently
	numGoroutines := 5
	done := make(chan bool, numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		go func() {
			_, err := detector.DetectChanges(context.Background(), "")
			if err != nil {
				t.Errorf("Concurrent DetectChanges failed: %v", err)
			}
			done <- true
		}()
	}

	// Wait for all goroutines to complete
	for i := 0; i < numGoroutines; i++ {
		<-done
	}
}

// TestDetectChanges_SpecialCharactersInKeys tests keys with special characters.
func TestDetectChanges_SpecialCharactersInKeys(t *testing.T) {
	source := newMockStorage()
	dest := newMockStorage()

	now := time.Now()

	// Add objects with special characters
	specialKeys := []string{
		"file with spaces.txt",
		"file-with-dashes.txt",
		"file_with_underscores.txt",
		"file.with.dots.txt",
		"file(with)parentheses.txt",
		"file[with]brackets.txt",
	}

	for _, key := range specialKeys {
		source.objects[key] = &common.Metadata{
			Size:         100,
			ETag:         "etag",
			LastModified: now,
		}
	}

	detector := NewChangeDetector(source, dest)
	changed, err := detector.DetectChanges(context.Background(), "")

	if err != nil {
		t.Fatalf("DetectChanges failed: %v", err)
	}

	if len(changed) != len(specialKeys) {
		t.Errorf("Expected %d changed objects, got %d", len(specialKeys), len(changed))
	}
}

// TestDetectChanges_VeryLargeFiles tests detection with very large file sizes.
func TestDetectChanges_VeryLargeFiles(t *testing.T) {
	source := newMockStorage()
	dest := newMockStorage()

	now := time.Now()

	// Add very large file to source
	source.objects["large-file.bin"] = &common.Metadata{
		Size:         10 * 1024 * 1024 * 1024, // 10GB
		ETag:         "large-etag",
		LastModified: now,
	}

	// Add same file to dest with slightly different size
	dest.objects["large-file.bin"] = &common.Metadata{
		Size:         10*1024*1024*1024 + 1, // 10GB + 1 byte
		ETag:         "large-etag",
		LastModified: now,
	}

	detector := NewChangeDetector(source, dest)
	changed, err := detector.DetectChanges(context.Background(), "")

	if err != nil {
		t.Fatalf("DetectChanges failed: %v", err)
	}

	// Should detect change due to size difference
	if len(changed) != 1 {
		t.Errorf("Expected 1 changed object, got %d", len(changed))
	}
}

// TestDetectChanges_TimestampEdgeCases tests edge cases with timestamps.
func TestDetectChanges_TimestampEdgeCases(t *testing.T) {
	source := newMockStorage()
	dest := newMockStorage()

	baseTime := time.Now()

	// Test 1: Exactly same timestamp
	source.objects["same-time.txt"] = &common.Metadata{
		Size:         100,
		ETag:         "",
		LastModified: baseTime,
	}
	dest.objects["same-time.txt"] = &common.Metadata{
		Size:         100,
		ETag:         "",
		LastModified: baseTime,
	}

	// Test 2: 1 nanosecond difference
	source.objects["nano-diff.txt"] = &common.Metadata{
		Size:         100,
		ETag:         "",
		LastModified: baseTime.Add(1 * time.Nanosecond),
	}
	dest.objects["nano-diff.txt"] = &common.Metadata{
		Size:         100,
		ETag:         "",
		LastModified: baseTime,
	}

	// Test 3: Zero timestamp
	source.objects["zero-time.txt"] = &common.Metadata{
		Size:         100,
		ETag:         "",
		LastModified: time.Time{},
	}
	dest.objects["zero-time.txt"] = &common.Metadata{
		Size:         100,
		ETag:         "",
		LastModified: time.Time{},
	}

	detector := NewChangeDetector(source, dest)
	changed, err := detector.DetectChanges(context.Background(), "")

	if err != nil {
		t.Fatalf("DetectChanges failed: %v", err)
	}

	// Should detect nano-diff as changed
	if len(changed) != 1 {
		t.Errorf("Expected 1 changed object, got %d", len(changed))
	}

	if len(changed) > 0 && changed[0] != "nano-diff.txt" {
		t.Errorf("Expected 'nano-diff.txt' to be changed, got '%s'", changed[0])
	}
}

// TestHasChanged_BothETagsEmpty tests hasChanged when both ETags are empty.
func TestHasChanged_BothETagsEmpty(t *testing.T) {
	now := time.Now()

	src := &common.Metadata{
		Size:         100,
		ETag:         "",
		LastModified: now,
	}
	dest := &common.Metadata{
		Size:         100,
		ETag:         "",
		LastModified: now,
	}

	if hasChanged(src, dest) {
		t.Error("Expected no change when ETags are empty and size/time match")
	}
}

// TestHasChanged_OneETagEmpty tests hasChanged when one ETag is empty.
func TestHasChanged_OneETagEmpty(t *testing.T) {
	now := time.Now()

	// Source has ETag, dest doesn't
	src := &common.Metadata{
		Size:         100,
		ETag:         "etag123",
		LastModified: now,
	}
	dest := &common.Metadata{
		Size:         100,
		ETag:         "",
		LastModified: now,
	}

	// Should not use ETag comparison when one is empty
	if hasChanged(src, dest) {
		t.Error("Expected no change when one ETag is empty and size/time match")
	}
}
