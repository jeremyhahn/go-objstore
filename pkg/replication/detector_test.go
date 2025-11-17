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
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

func TestNewChangeDetector(t *testing.T) {
	source := newMockStorage()
	dest := newMockStorage()

	detector := NewChangeDetector(source, dest)

	if detector == nil {
		t.Fatal("NewChangeDetector returned nil")
	}
	if detector.source != source {
		t.Error("source storage not set correctly")
	}
	if detector.dest != dest {
		t.Error("destination storage not set correctly")
	}
}

func TestDetectChanges_NewObject(t *testing.T) {
	source := newMockStorage()
	dest := newMockStorage()

	// Add object to source but not destination
	source.objects["test.txt"] = &common.Metadata{
		Size:         100,
		ETag:         "etag123",
		LastModified: time.Now(),
	}

	detector := NewChangeDetector(source, dest)
	changed, err := detector.DetectChanges(context.Background(), "")

	if err != nil {
		t.Fatalf("DetectChanges failed: %v", err)
	}
	if len(changed) != 1 {
		t.Fatalf("expected 1 changed object, got %d", len(changed))
	}
	if changed[0] != "test.txt" {
		t.Errorf("expected 'test.txt', got '%s'", changed[0])
	}
}

func TestDetectChanges_ModifiedObject_DifferentETag(t *testing.T) {
	source := newMockStorage()
	dest := newMockStorage()

	now := time.Now()

	// Same object, different ETags
	source.objects["test.txt"] = &common.Metadata{
		Size:         100,
		ETag:         "etag-new",
		LastModified: now,
	}
	dest.objects["test.txt"] = &common.Metadata{
		Size:         100,
		ETag:         "etag-old",
		LastModified: now,
	}

	detector := NewChangeDetector(source, dest)
	changed, err := detector.DetectChanges(context.Background(), "")

	if err != nil {
		t.Fatalf("DetectChanges failed: %v", err)
	}
	if len(changed) != 1 {
		t.Fatalf("expected 1 changed object, got %d", len(changed))
	}
	if changed[0] != "test.txt" {
		t.Errorf("expected 'test.txt', got '%s'", changed[0])
	}
}

func TestDetectChanges_ModifiedObject_DifferentSize(t *testing.T) {
	source := newMockStorage()
	dest := newMockStorage()

	now := time.Now()

	// Same object, different sizes
	source.objects["test.txt"] = &common.Metadata{
		Size:         200,
		ETag:         "",
		LastModified: now,
	}
	dest.objects["test.txt"] = &common.Metadata{
		Size:         100,
		ETag:         "",
		LastModified: now,
	}

	detector := NewChangeDetector(source, dest)
	changed, err := detector.DetectChanges(context.Background(), "")

	if err != nil {
		t.Fatalf("DetectChanges failed: %v", err)
	}
	if len(changed) != 1 {
		t.Fatalf("expected 1 changed object, got %d", len(changed))
	}
}

func TestDetectChanges_ModifiedObject_NewerTimestamp(t *testing.T) {
	source := newMockStorage()
	dest := newMockStorage()

	oldTime := time.Now().Add(-1 * time.Hour)
	newTime := time.Now()

	// Same object, source is newer
	source.objects["test.txt"] = &common.Metadata{
		Size:         100,
		ETag:         "",
		LastModified: newTime,
	}
	dest.objects["test.txt"] = &common.Metadata{
		Size:         100,
		ETag:         "",
		LastModified: oldTime,
	}

	detector := NewChangeDetector(source, dest)
	changed, err := detector.DetectChanges(context.Background(), "")

	if err != nil {
		t.Fatalf("DetectChanges failed: %v", err)
	}
	if len(changed) != 1 {
		t.Fatalf("expected 1 changed object, got %d", len(changed))
	}
}

func TestDetectChanges_NoChanges(t *testing.T) {
	source := newMockStorage()
	dest := newMockStorage()

	now := time.Now()

	// Identical objects
	source.objects["test.txt"] = &common.Metadata{
		Size:         100,
		ETag:         "etag123",
		LastModified: now,
	}
	dest.objects["test.txt"] = &common.Metadata{
		Size:         100,
		ETag:         "etag123",
		LastModified: now,
	}

	detector := NewChangeDetector(source, dest)
	changed, err := detector.DetectChanges(context.Background(), "")

	if err != nil {
		t.Fatalf("DetectChanges failed: %v", err)
	}
	if len(changed) != 0 {
		t.Fatalf("expected 0 changed objects, got %d", len(changed))
	}
}

func TestDetectChanges_WithPrefix(t *testing.T) {
	source := newMockStorage()
	dest := newMockStorage()

	now := time.Now()

	// Add objects with different prefixes
	source.objects["photos/cat.jpg"] = &common.Metadata{
		Size:         100,
		ETag:         "etag1",
		LastModified: now,
	}
	source.objects["documents/report.pdf"] = &common.Metadata{
		Size:         200,
		ETag:         "etag2",
		LastModified: now,
	}

	detector := NewChangeDetector(source, dest)
	changed, err := detector.DetectChanges(context.Background(), "photos/")

	if err != nil {
		t.Fatalf("DetectChanges failed: %v", err)
	}
	if len(changed) != 1 {
		t.Fatalf("expected 1 changed object, got %d", len(changed))
	}
	if changed[0] != "photos/cat.jpg" {
		t.Errorf("expected 'photos/cat.jpg', got '%s'", changed[0])
	}
}

func TestDetectChanges_Pagination(t *testing.T) {
	source := newMockStorage()
	dest := newMockStorage()

	now := time.Now()

	// Add multiple objects
	source.objects["file1.txt"] = &common.Metadata{Size: 100, ETag: "etag1", LastModified: now}
	source.objects["file2.txt"] = &common.Metadata{Size: 100, ETag: "etag2", LastModified: now}
	source.shouldTruncate = true

	detector := NewChangeDetector(source, dest)
	changed, err := detector.DetectChanges(context.Background(), "")

	if err != nil {
		t.Fatalf("DetectChanges failed: %v", err)
	}

	// Should make multiple calls due to pagination
	if source.listCallCount < 2 {
		t.Errorf("expected multiple list calls for pagination, got %d", source.listCallCount)
	}

	// Should eventually find all objects
	if len(changed) < 1 {
		t.Errorf("expected at least 1 changed object, got %d", len(changed))
	}
}

func TestDetectChanges_SourceListError(t *testing.T) {
	source := newMockStorage()
	dest := newMockStorage()

	source.listError = errors.New("list failed")

	detector := NewChangeDetector(source, dest)
	_, err := detector.DetectChanges(context.Background(), "")

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err.Error() != "list failed" {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestHasChanged_DestinationNil(t *testing.T) {
	src := &common.Metadata{
		Size: 100,
		ETag: "etag123",
	}

	if !hasChanged(src, nil) {
		t.Error("expected hasChanged to return true when dest is nil")
	}
}

func TestHasChanged_ETagMismatch(t *testing.T) {
	src := &common.Metadata{
		Size: 100,
		ETag: "etag-new",
	}
	dest := &common.Metadata{
		Size: 100,
		ETag: "etag-old",
	}

	if !hasChanged(src, dest) {
		t.Error("expected hasChanged to return true for different ETags")
	}
}

func TestHasChanged_SizeMismatch(t *testing.T) {
	src := &common.Metadata{
		Size: 200,
		ETag: "",
	}
	dest := &common.Metadata{
		Size: 100,
		ETag: "",
	}

	if !hasChanged(src, dest) {
		t.Error("expected hasChanged to return true for different sizes")
	}
}

func TestHasChanged_SourceNewer(t *testing.T) {
	oldTime := time.Now().Add(-1 * time.Hour)
	newTime := time.Now()

	src := &common.Metadata{
		Size:         100,
		ETag:         "",
		LastModified: newTime,
	}
	dest := &common.Metadata{
		Size:         100,
		ETag:         "",
		LastModified: oldTime,
	}

	if !hasChanged(src, dest) {
		t.Error("expected hasChanged to return true when source is newer")
	}
}

func TestHasChanged_NoChange(t *testing.T) {
	now := time.Now()

	src := &common.Metadata{
		Size:         100,
		ETag:         "etag123",
		LastModified: now,
	}
	dest := &common.Metadata{
		Size:         100,
		ETag:         "etag123",
		LastModified: now,
	}

	if hasChanged(src, dest) {
		t.Error("expected hasChanged to return false for identical metadata")
	}
}

func TestHasChanged_EmptyETags(t *testing.T) {
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
		t.Error("expected hasChanged to return false when ETags are empty and size/time match")
	}
}

func TestHasChanged_DestinationOlder(t *testing.T) {
	oldTime := time.Now().Add(-1 * time.Hour)
	newTime := time.Now()

	src := &common.Metadata{
		Size:         100,
		ETag:         "",
		LastModified: oldTime,
	}
	dest := &common.Metadata{
		Size:         100,
		ETag:         "",
		LastModified: newTime,
	}

	if hasChanged(src, dest) {
		t.Error("expected hasChanged to return false when destination is newer")
	}
}
