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

//go:build local

package local

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

func TestLocal_ListWithOptions_Pagination(t *testing.T) {
	t.Run("pagination with small page size", func(t *testing.T) {
		tmpDir := t.TempDir()
		storage := New()
		err := storage.Configure(map[string]string{"path": tmpDir})
		if err != nil {
			t.Fatalf("failed to configure storage: %v", err)
		}

		ctx := context.Background()

		// Put 10 files
		for i := 0; i < 10; i++ {
			data := bytes.NewReader([]byte(fmt.Sprintf("data%d", i)))
			key := fmt.Sprintf("test/%02d", i)
			err = storage.PutWithContext(ctx, key, data)
			if err != nil {
				t.Fatalf("failed to put: %v", err)
			}
		}

		// List first page
		opts := &common.ListOptions{
			Prefix:     "test/",
			MaxResults: 3,
		}

		result, err := storage.ListWithOptions(ctx, opts)
		if err != nil {
			t.Fatalf("failed to list: %v", err)
		}

		if len(result.Objects) != 3 {
			t.Errorf("expected 3 objects, got %d", len(result.Objects))
		}

		if !result.Truncated {
			t.Error("expected Truncated to be true")
		}

		if result.NextToken == "" {
			t.Error("expected NextToken to be set")
		}

		// List second page
		opts.ContinueFrom = result.NextToken
		result2, err := storage.ListWithOptions(ctx, opts)
		if err != nil {
			t.Fatalf("failed to list page 2: %v", err)
		}

		if len(result2.Objects) != 3 {
			t.Errorf("expected 3 objects on page 2, got %d", len(result2.Objects))
		}

		if !result2.Truncated {
			t.Error("expected Truncated to be true on page 2")
		}
	})

	t.Run("pagination with exact multiple", func(t *testing.T) {
		tmpDir := t.TempDir()
		storage := New()
		err := storage.Configure(map[string]string{"path": tmpDir})
		if err != nil {
			t.Fatalf("failed to configure storage: %v", err)
		}

		ctx := context.Background()

		// Put 6 files
		for i := 0; i < 6; i++ {
			data := bytes.NewReader([]byte(fmt.Sprintf("data%d", i)))
			key := fmt.Sprintf("test/%d", i)
			err = storage.PutWithContext(ctx, key, data)
			if err != nil {
				t.Fatalf("failed to put: %v", err)
			}
		}

		// List with page size 3 (exactly 2 pages)
		opts := &common.ListOptions{
			Prefix:     "test/",
			MaxResults: 3,
		}

		result, err := storage.ListWithOptions(ctx, opts)
		if err != nil {
			t.Fatalf("failed to list: %v", err)
		}

		if len(result.Objects) != 3 {
			t.Errorf("expected 3 objects, got %d", len(result.Objects))
		}

		// Get last page
		opts.ContinueFrom = result.NextToken
		result2, err := storage.ListWithOptions(ctx, opts)
		if err != nil {
			t.Fatalf("failed to list page 2: %v", err)
		}

		if len(result2.Objects) != 3 {
			t.Errorf("expected 3 objects on page 2, got %d", len(result2.Objects))
		}

		if result2.Truncated {
			t.Error("expected Truncated to be false on last page")
		}

		if result2.NextToken != "" {
			t.Error("expected empty NextToken on last page")
		}
	})

	t.Run("pagination with default max results", func(t *testing.T) {
		tmpDir := t.TempDir()
		storage := New()
		err := storage.Configure(map[string]string{"path": tmpDir})
		if err != nil {
			t.Fatalf("failed to configure storage: %v", err)
		}

		ctx := context.Background()

		// Put 5 files
		for i := 0; i < 5; i++ {
			data := bytes.NewReader([]byte(fmt.Sprintf("data%d", i)))
			key := fmt.Sprintf("test/%d", i)
			err = storage.PutWithContext(ctx, key, data)
			if err != nil {
				t.Fatalf("failed to put: %v", err)
			}
		}

		// List with no max results (should use default)
		opts := &common.ListOptions{
			Prefix: "test/",
		}

		result, err := storage.ListWithOptions(ctx, opts)
		if err != nil {
			t.Fatalf("failed to list: %v", err)
		}

		if len(result.Objects) != 5 {
			t.Errorf("expected 5 objects, got %d", len(result.Objects))
		}

		if result.Truncated {
			t.Error("expected Truncated to be false")
		}
	})

	t.Run("empty result with pagination", func(t *testing.T) {
		tmpDir := t.TempDir()
		storage := New()
		err := storage.Configure(map[string]string{"path": tmpDir})
		if err != nil {
			t.Fatalf("failed to configure storage: %v", err)
		}

		ctx := context.Background()

		opts := &common.ListOptions{
			Prefix:     "nonexistent/",
			MaxResults: 10,
		}

		result, err := storage.ListWithOptions(ctx, opts)
		if err != nil {
			t.Fatalf("failed to list: %v", err)
		}

		if len(result.Objects) != 0 {
			t.Errorf("expected 0 objects, got %d", len(result.Objects))
		}

		if result.Truncated {
			t.Error("expected Truncated to be false")
		}
	})
}

func TestLocal_ListWithOptions_Delimiter(t *testing.T) {
	t.Run("hierarchical listing with delimiter", func(t *testing.T) {
		tmpDir := t.TempDir()
		storage := New()
		err := storage.Configure(map[string]string{"path": tmpDir})
		if err != nil {
			t.Fatalf("failed to configure storage: %v", err)
		}

		ctx := context.Background()

		// Create hierarchical structure
		files := []string{
			"a/1.txt",
			"a/2.txt",
			"a/b/3.txt",
			"a/c/4.txt",
			"a/c/5.txt",
		}

		for _, file := range files {
			data := bytes.NewReader([]byte("data"))
			err = storage.PutWithContext(ctx, file, data)
			if err != nil {
				t.Fatalf("failed to put %s: %v", file, err)
			}
		}

		// List with delimiter
		opts := &common.ListOptions{
			Prefix:    "a/",
			Delimiter: "/",
		}

		result, err := storage.ListWithOptions(ctx, opts)
		if err != nil {
			t.Fatalf("failed to list: %v", err)
		}

		// Should get 2 files (1.txt, 2.txt) and 2 common prefixes (b/, c/)
		if len(result.Objects) != 2 {
			t.Errorf("expected 2 objects, got %d", len(result.Objects))
		}

		if len(result.CommonPrefixes) != 2 {
			t.Errorf("expected 2 common prefixes, got %d", len(result.CommonPrefixes))
		}

		// Verify common prefixes
		expectedPrefixes := map[string]bool{
			"a/b/": true,
			"a/c/": true,
		}

		for _, prefix := range result.CommonPrefixes {
			if !expectedPrefixes[prefix] {
				t.Errorf("unexpected common prefix: %s", prefix)
			}
		}
	})

	t.Run("delimiter listing at subdirectory level", func(t *testing.T) {
		tmpDir := t.TempDir()
		storage := New()
		err := storage.Configure(map[string]string{"path": tmpDir})
		if err != nil {
			t.Fatalf("failed to configure storage: %v", err)
		}

		ctx := context.Background()

		// Create hierarchical structure
		files := []string{
			"a/b/1.txt",
			"a/b/2.txt",
			"a/b/c/3.txt",
		}

		for _, file := range files {
			data := bytes.NewReader([]byte("data"))
			err = storage.PutWithContext(ctx, file, data)
			if err != nil {
				t.Fatalf("failed to put %s: %v", file, err)
			}
		}

		// List a/b/ with delimiter
		opts := &common.ListOptions{
			Prefix:    "a/b/",
			Delimiter: "/",
		}

		result, err := storage.ListWithOptions(ctx, opts)
		if err != nil {
			t.Fatalf("failed to list: %v", err)
		}

		// Should get 2 files and 1 common prefix (c/)
		if len(result.Objects) != 2 {
			t.Errorf("expected 2 objects, got %d", len(result.Objects))
		}

		if len(result.CommonPrefixes) != 1 {
			t.Errorf("expected 1 common prefix, got %d", len(result.CommonPrefixes))
		}

		if result.CommonPrefixes[0] != "a/b/c/" {
			t.Errorf("expected common prefix 'a/b/c/', got '%s'", result.CommonPrefixes[0])
		}
	})

	t.Run("delimiter with no hierarchical structure", func(t *testing.T) {
		tmpDir := t.TempDir()
		storage := New()
		err := storage.Configure(map[string]string{"path": tmpDir})
		if err != nil {
			t.Fatalf("failed to configure storage: %v", err)
		}

		ctx := context.Background()

		// Create flat structure
		files := []string{
			"test/1.txt",
			"test/2.txt",
			"test/3.txt",
		}

		for _, file := range files {
			data := bytes.NewReader([]byte("data"))
			err = storage.PutWithContext(ctx, file, data)
			if err != nil {
				t.Fatalf("failed to put %s: %v", file, err)
			}
		}

		// List with delimiter
		opts := &common.ListOptions{
			Prefix:    "test/",
			Delimiter: "/",
		}

		result, err := storage.ListWithOptions(ctx, opts)
		if err != nil {
			t.Fatalf("failed to list: %v", err)
		}

		// Should get all 3 files, no common prefixes
		if len(result.Objects) != 3 {
			t.Errorf("expected 3 objects, got %d", len(result.Objects))
		}

		if len(result.CommonPrefixes) != 0 {
			t.Errorf("expected 0 common prefixes, got %d", len(result.CommonPrefixes))
		}
	})
}

func TestLocal_ListWithOptions_Combined(t *testing.T) {
	t.Run("pagination with delimiter", func(t *testing.T) {
		tmpDir := t.TempDir()
		storage := New()
		err := storage.Configure(map[string]string{"path": tmpDir})
		if err != nil {
			t.Fatalf("failed to configure storage: %v", err)
		}

		ctx := context.Background()

		// Create mixed structure
		files := []string{
			"root/1.txt",
			"root/2.txt",
			"root/3.txt",
			"root/a/file.txt",
			"root/b/file.txt",
		}

		for _, file := range files {
			data := bytes.NewReader([]byte("data"))
			err = storage.PutWithContext(ctx, file, data)
			if err != nil {
				t.Fatalf("failed to put %s: %v", file, err)
			}
		}

		// List with delimiter - should get 3 files and 2 common prefixes
		opts := &common.ListOptions{
			Prefix:    "root/",
			Delimiter: "/",
		}

		result, err := storage.ListWithOptions(ctx, opts)
		if err != nil {
			t.Fatalf("failed to list: %v", err)
		}

		// Should get 3 objects (1.txt, 2.txt, 3.txt) and 2 common prefixes (a/, b/)
		if len(result.Objects) != 3 {
			t.Errorf("expected 3 objects, got %d", len(result.Objects))
		}

		if len(result.CommonPrefixes) != 2 {
			t.Errorf("expected 2 common prefixes, got %d", len(result.CommonPrefixes))
		}
	})

	t.Run("nil options uses defaults", func(t *testing.T) {
		tmpDir := t.TempDir()
		storage := New()
		err := storage.Configure(map[string]string{"path": tmpDir})
		if err != nil {
			t.Fatalf("failed to configure storage: %v", err)
		}

		ctx := context.Background()

		// Put a file
		data := bytes.NewReader([]byte("data"))
		err = storage.PutWithContext(ctx, "test/file", data)
		if err != nil {
			t.Fatalf("failed to put: %v", err)
		}

		// List with nil options
		result, err := storage.ListWithOptions(ctx, nil)
		if err != nil {
			t.Fatalf("failed to list: %v", err)
		}

		if len(result.Objects) != 1 {
			t.Errorf("expected 1 object, got %d", len(result.Objects))
		}
	})

	t.Run("list with cancelled context", func(t *testing.T) {
		tmpDir := t.TempDir()
		storage := New()
		err := storage.Configure(map[string]string{"path": tmpDir})
		if err != nil {
			t.Fatalf("failed to configure storage: %v", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		opts := &common.ListOptions{
			Prefix: "test/",
		}

		_, err = storage.ListWithOptions(ctx, opts)
		if err != context.Canceled {
			t.Errorf("expected context.Canceled, got %v", err)
		}
	})
}
