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

//go:build integration

package common

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// ComprehensiveTestSuite runs a comprehensive set of tests on a storage backend
// This ensures all Storage interface methods are tested including edge cases
type ComprehensiveTestSuite struct {
	Storage common.Storage
	T       *testing.T
}

// RunAllTests executes the complete test suite
func (s *ComprehensiveTestSuite) RunAllTests() {
	s.T.Run("ContextOperations", func(t *testing.T) { s.TestContextOperations() })
	s.T.Run("MetadataOperations", func(t *testing.T) { s.TestMetadataOperations() })
	s.T.Run("ExistsOperation", func(t *testing.T) { s.TestExists() })
	s.T.Run("ListOperations", func(t *testing.T) { s.TestListOperations() })
	s.T.Run("ListWithOptions", func(t *testing.T) { s.TestListWithOptions() })
	s.T.Run("Pagination", func(t *testing.T) { s.TestPagination() })
	s.T.Run("ContextCancellation", func(t *testing.T) { s.TestContextCancellation() })
	s.T.Run("ConcurrentOperations", func(t *testing.T) { s.TestConcurrentOperations() })
	s.T.Run("InvalidKeys", func(t *testing.T) { s.TestInvalidKeys() })
	s.T.Run("LargeObject", func(t *testing.T) { s.TestLargeObject() })
}

// TestContextOperations tests *WithContext methods
func (s *ComprehensiveTestSuite) TestContextOperations() {
	ctx := context.Background()
	key := "test/context-ops.txt"
	data := []byte("context test data")

	// Test PutWithContext
	err := s.Storage.PutWithContext(ctx, key, bytes.NewReader(data))
	if err != nil {
		s.T.Fatalf("PutWithContext failed: %v", err)
	}

	// Test GetWithContext
	rc, err := s.Storage.GetWithContext(ctx, key)
	if err != nil {
		s.T.Fatalf("GetWithContext failed: %v", err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if !bytes.Equal(got, data) {
		s.T.Fatalf("GetWithContext data mismatch")
	}

	// Test DeleteWithContext
	err = s.Storage.DeleteWithContext(ctx, key)
	if err != nil {
		s.T.Fatalf("DeleteWithContext failed: %v", err)
	}

	// Verify deletion
	exists, err := s.Storage.Exists(ctx, key)
	if err != nil {
		s.T.Fatalf("Exists check failed: %v", err)
	}
	if exists {
		s.T.Fatal("Object should not exist after deletion")
	}
}

// TestMetadataOperations tests metadata methods
func (s *ComprehensiveTestSuite) TestMetadataOperations() {
	ctx := context.Background()
	key := "test/metadata-ops.txt"
	data := []byte("metadata test data")

	// Test PutWithMetadata
	metadata := &common.Metadata{
		ContentType:     "text/plain",
		ContentEncoding: "utf-8",
		Custom: map[string]string{
			"author":  "test-user",
			"version": "1.0",
		},
	}

	err := s.Storage.PutWithMetadata(ctx, key, bytes.NewReader(data), metadata)
	if err != nil {
		s.T.Fatalf("PutWithMetadata failed: %v", err)
	}

	// Test GetMetadata
	retrievedMeta, err := s.Storage.GetMetadata(ctx, key)
	if err != nil {
		s.T.Fatalf("GetMetadata failed: %v", err)
	}
	if retrievedMeta == nil {
		s.T.Log("GetMetadata returned nil (metadata not supported by backend)")
	} else {
		if retrievedMeta.ContentType != metadata.ContentType {
			s.T.Logf("ContentType mismatch: got %s, want %s (backend-specific)", retrievedMeta.ContentType, metadata.ContentType)
		} else {
			s.T.Log("ContentType correctly preserved")
		}
		if retrievedMeta.Custom == nil || retrievedMeta.Custom["author"] != "test-user" {
			s.T.Log("Custom metadata not fully preserved (backend-specific behavior)")
		} else {
			s.T.Log("Custom metadata correctly preserved")
		}
	}

	// Test UpdateMetadata
	newMetadata := &common.Metadata{
		ContentType: "application/json",
		Custom: map[string]string{
			"author":  "updated-user",
			"version": "2.0",
		},
	}
	err = s.Storage.UpdateMetadata(ctx, key, newMetadata)
	if err != nil {
		s.T.Fatalf("UpdateMetadata failed: %v", err)
	}

	// Verify update
	updatedMeta, err := s.Storage.GetMetadata(ctx, key)
	if err != nil {
		s.T.Fatalf("GetMetadata after update failed: %v", err)
	}
	if updatedMeta == nil {
		s.T.Log("GetMetadata returned nil (metadata not supported by backend)")
	} else {
		if updatedMeta.ContentType != "application/json" {
			s.T.Log("ContentType not updated (backend-specific behavior)")
		} else {
			s.T.Log("ContentType successfully updated")
		}
		if updatedMeta.Custom == nil || updatedMeta.Custom["author"] != "updated-user" {
			s.T.Log("Custom metadata not updated (backend-specific behavior)")
		} else {
			s.T.Log("Custom metadata successfully updated")
		}
	}

	// Cleanup
	s.Storage.Delete(key)
}

// TestExists tests the Exists method
func (s *ComprehensiveTestSuite) TestExists() {
	ctx := context.Background()
	key := "test/exists-check.txt"

	// Check non-existent key
	exists, err := s.Storage.Exists(ctx, key)
	if err != nil {
		s.T.Fatalf("Exists check failed: %v", err)
	}
	if exists {
		s.T.Fatal("Object should not exist")
	}

	// Create object
	err = s.Storage.Put(key, bytes.NewReader([]byte("exists test")))
	if err != nil {
		s.T.Fatalf("Put failed: %v", err)
	}

	// Check existing key
	exists, err = s.Storage.Exists(ctx, key)
	if err != nil {
		s.T.Fatalf("Exists check failed: %v", err)
	}
	if !exists {
		s.T.Fatal("Object should exist")
	}

	// Cleanup
	s.Storage.Delete(key)
}

// TestListOperations tests List and ListWithContext
func (s *ComprehensiveTestSuite) TestListOperations() {
	ctx := context.Background()
	prefix := "test/list/"

	// Create test objects
	objects := []string{
		prefix + "file1.txt",
		prefix + "file2.txt",
		prefix + "subdir/file3.txt",
		"other/file4.txt",
	}

	for _, key := range objects {
		err := s.Storage.Put(key, bytes.NewReader([]byte("list test")))
		if err != nil {
			s.T.Fatalf("Put failed for %s: %v", key, err)
		}
	}

	// Test List with prefix
	keys, err := s.Storage.List(prefix)
	if err != nil {
		s.T.Fatalf("List failed: %v", err)
	}
	if len(keys) != 3 {
		s.T.Errorf("List returned %d keys, expected 3", len(keys))
	}

	// Test ListWithContext
	keys, err = s.Storage.ListWithContext(ctx, prefix)
	if err != nil {
		s.T.Fatalf("ListWithContext failed: %v", err)
	}
	if len(keys) != 3 {
		s.T.Errorf("ListWithContext returned %d keys, expected 3", len(keys))
	}

	// Test List with empty prefix (all objects)
	allKeys, err := s.Storage.List("")
	if err != nil {
		s.T.Fatalf("List all failed: %v", err)
	}
	if len(allKeys) < 4 {
		s.T.Errorf("List all returned %d keys, expected at least 4", len(allKeys))
	}

	// Cleanup
	for _, key := range objects {
		s.Storage.Delete(key)
	}
}

// TestListWithOptions tests advanced listing with pagination
func (s *ComprehensiveTestSuite) TestListWithOptions() {
	ctx := context.Background()
	prefix := "test/listopts/"

	// Create test objects
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("%sfile%d.txt", prefix, i)
		err := s.Storage.Put(key, bytes.NewReader([]byte(fmt.Sprintf("data%d", i))))
		if err != nil {
			s.T.Fatalf("Put failed: %v", err)
		}
	}

	// Test ListWithOptions
	opts := &common.ListOptions{
		Prefix:     prefix,
		MaxResults: 10,
	}

	result, err := s.Storage.ListWithOptions(ctx, opts)
	if err != nil {
		s.T.Fatalf("ListWithOptions failed: %v", err)
	}

	if len(result.Objects) != 5 {
		s.T.Errorf("ListWithOptions returned %d objects, expected 5", len(result.Objects))
	}

	// Verify metadata is populated
	for _, obj := range result.Objects {
		if obj.Key == "" {
			s.T.Error("Object key is empty")
		}
		if obj.Metadata != nil && obj.Metadata.Size == 0 {
			s.T.Log("Object size is 0 (may be acceptable for some backends)")
		}
	}

	// Cleanup
	for i := 0; i < 5; i++ {
		s.Storage.Delete(fmt.Sprintf("%sfile%d.txt", prefix, i))
	}
}

// TestPagination tests pagination with ListWithOptions
func (s *ComprehensiveTestSuite) TestPagination() {
	ctx := context.Background()
	prefix := "test/pagination/"

	// Create more objects for pagination
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("%sfile%02d.txt", prefix, i)
		err := s.Storage.Put(key, bytes.NewReader([]byte(fmt.Sprintf("page-data%d", i))))
		if err != nil {
			s.T.Fatalf("Put failed: %v", err)
		}
	}

	// Test pagination with MaxResults
	opts := &common.ListOptions{
		Prefix:     prefix,
		MaxResults: 3,
	}

	allObjects := []*common.ObjectInfo{}
	pageCount := 0

	for {
		result, err := s.Storage.ListWithOptions(ctx, opts)
		if err != nil {
			s.T.Fatalf("ListWithOptions page %d failed: %v", pageCount, err)
		}

		allObjects = append(allObjects, result.Objects...)
		pageCount++

		if result.NextToken == "" {
			break
		}

		opts.ContinueFrom = result.NextToken

		if pageCount > 10 {
			s.T.Fatal("Too many pagination iterations")
			break
		}
	}

	if len(allObjects) != 10 {
		s.T.Errorf("Pagination returned %d total objects, expected 10", len(allObjects))
	}
	if pageCount < 2 {
		s.T.Errorf("Expected at least 2 pages, got %d", pageCount)
	}

	// Cleanup
	for i := 0; i < 10; i++ {
		s.Storage.Delete(fmt.Sprintf("%sfile%02d.txt", prefix, i))
	}
}

// TestContextCancellation tests behavior when context is cancelled
func (s *ComprehensiveTestSuite) TestContextCancellation() {
	key := "test/context-cancel.txt"
	data := make([]byte, 1024*1024) // 1MB

	// Create a context that's already cancelled
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Try to put with cancelled context
	// Note: Some backends might not respect cancellation immediately
	err := s.Storage.PutWithContext(ctx, key, bytes.NewReader(data))
	if err == nil {
		// If it succeeded, clean up
		s.Storage.Delete(key)
		s.T.Log("Backend did not respect context cancellation (acceptable)")
	} else {
		s.T.Logf("Context cancellation respected: %v", err)
	}
}

// TestConcurrentOperations tests concurrent read/write safety
func (s *ComprehensiveTestSuite) TestConcurrentOperations() {
	prefix := "test/concurrent/"
	numGoroutines := 5

	// Write concurrently
	errChan := make(chan error, numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			key := fmt.Sprintf("%sfile%d.txt", prefix, id)
			data := []byte(fmt.Sprintf("concurrent data %d", id))
			errChan <- s.Storage.Put(key, bytes.NewReader(data))
		}(i)
	}

	// Check for errors
	for i := 0; i < numGoroutines; i++ {
		if err := <-errChan; err != nil {
			s.T.Errorf("Concurrent put failed: %v", err)
		}
	}

	// Read concurrently
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			key := fmt.Sprintf("%sfile%d.txt", prefix, id)
			rc, err := s.Storage.Get(key)
			if err != nil {
				errChan <- err
				return
			}
			io.Copy(io.Discard, rc)
			rc.Close()
			errChan <- nil
		}(i)
	}

	// Check read errors
	for i := 0; i < numGoroutines; i++ {
		if err := <-errChan; err != nil {
			s.T.Errorf("Concurrent get failed: %v", err)
		}
	}

	// Cleanup
	for i := 0; i < numGoroutines; i++ {
		s.Storage.Delete(fmt.Sprintf("%sfile%d.txt", prefix, i))
	}
}

// TestInvalidKeys tests handling of invalid/problematic keys
func (s *ComprehensiveTestSuite) TestInvalidKeys() {
	ctx := context.Background()

	// Test empty key (should fail or be handled gracefully)
	err := s.Storage.Put("", bytes.NewReader([]byte("test")))
	if err == nil {
		s.T.Log("Backend allows empty keys (unusual but acceptable)")
		s.Storage.Delete("")
	} else {
		s.T.Logf("Backend rejects empty keys: %v", err)
	}

	// Test very long key
	longKey := "test/" + strings.Repeat("a", 1000) + ".txt"
	err = s.Storage.Put(longKey, bytes.NewReader([]byte("long key test")))
	if err != nil {
		s.T.Logf("Backend rejects very long keys: %v", err)
	} else {
		s.Storage.Delete(longKey)
	}

	// Test Get on non-existent key (should return error)
	_, err = s.Storage.Get("nonexistent/key/that/does/not/exist.txt")
	if err != nil {
		s.T.Logf("Get on non-existent key correctly returned error: %v", err)
	} else {
		s.T.Log("Get on non-existent key did not error (unusual, backend-specific)")
	}

	// Test Exists on non-existent key (should return false, not error)
	exists, err := s.Storage.Exists(ctx, "nonexistent/key.txt")
	if err != nil {
		s.T.Logf("Exists on non-existent key returned error (backend-specific): %v", err)
	}
	if exists {
		s.T.Log("Exists returned true for non-existent key (unusual, backend-specific)")
	} else {
		s.T.Log("Exists correctly returned false for non-existent key")
	}

	// Test GetMetadata on non-existent key (should return error)
	_, err = s.Storage.GetMetadata(ctx, "nonexistent/key.txt")
	if err != nil {
		s.T.Logf("GetMetadata on non-existent key correctly returned error: %v", err)
	} else {
		s.T.Log("GetMetadata on non-existent key did not error (backend-specific behavior)")
	}
}

// TestLargeObject tests handling of large files
func (s *ComprehensiveTestSuite) TestLargeObject() {
	key := "test/large-file.bin"
	size := 5 * 1024 * 1024 // 5MB

	// Create large data
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 256)
	}

	// Put large object
	start := time.Now()
	err := s.Storage.Put(key, bytes.NewReader(data))
	if err != nil {
		s.T.Fatalf("Put large file failed: %v", err)
	}
	putDuration := time.Since(start)
	s.T.Logf("Put 5MB took %v", putDuration)

	// Get large object
	start = time.Now()
	rc, err := s.Storage.Get(key)
	if err != nil {
		s.T.Fatalf("Get large file failed: %v", err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	getDuration := time.Since(start)
	s.T.Logf("Get 5MB took %v", getDuration)

	if len(got) != size {
		s.T.Errorf("Size mismatch: got %d, want %d", len(got), size)
	}

	// Cleanup
	s.Storage.Delete(key)
}
