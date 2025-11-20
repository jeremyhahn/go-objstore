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
	"os"
	"path/filepath"
	"testing"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

func TestLocal_PutWithMetadata(t *testing.T) {
	t.Run("put with custom metadata", func(t *testing.T) {
		tmpDir := t.TempDir()
		storage := New()
		err := storage.Configure(map[string]string{"path": tmpDir})
		if err != nil {
			t.Fatalf("failed to configure storage: %v", err)
		}

		ctx := context.Background()
		metadata := &common.Metadata{
			ContentType:     "application/json",
			ContentEncoding: "gzip",
			Custom: map[string]string{
				"author":  "test",
				"version": "1.0",
			},
		}

		data := bytes.NewReader([]byte("test data"))
		err = storage.PutWithMetadata(ctx, "test/key", data, metadata)
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		// Verify metadata was saved
		retrieved, err := storage.GetMetadata(ctx, "test/key")
		if err != nil {
			t.Fatalf("failed to get metadata: %v", err)
		}

		if retrieved.ContentType != "application/json" {
			t.Errorf("expected ContentType 'application/json', got '%s'", retrieved.ContentType)
		}
		if retrieved.ContentEncoding != "gzip" {
			t.Errorf("expected ContentEncoding 'gzip', got '%s'", retrieved.ContentEncoding)
		}
		if retrieved.Custom["author"] != "test" {
			t.Errorf("expected author 'test', got '%s'", retrieved.Custom["author"])
		}
		if retrieved.Size != 9 {
			t.Errorf("expected size 9, got %d", retrieved.Size)
		}
	})

	t.Run("put without metadata creates basic metadata", func(t *testing.T) {
		tmpDir := t.TempDir()
		storage := New()
		err := storage.Configure(map[string]string{"path": tmpDir})
		if err != nil {
			t.Fatalf("failed to configure storage: %v", err)
		}

		ctx := context.Background()
		data := bytes.NewReader([]byte("test data"))
		err = storage.PutWithMetadata(ctx, "test/key", data, nil)
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		// Verify basic metadata was created
		retrieved, err := storage.GetMetadata(ctx, "test/key")
		if err != nil {
			t.Fatalf("failed to get metadata: %v", err)
		}

		if retrieved.Size != 9 {
			t.Errorf("expected size 9, got %d", retrieved.Size)
		}
		if retrieved.ETag == "" {
			t.Error("expected ETag to be set")
		}
	})

	t.Run("metadata persists across operations", func(t *testing.T) {
		tmpDir := t.TempDir()
		storage := New()
		err := storage.Configure(map[string]string{"path": tmpDir})
		if err != nil {
			t.Fatalf("failed to configure storage: %v", err)
		}

		ctx := context.Background()
		metadata := &common.Metadata{
			ContentType: "text/plain",
			Custom: map[string]string{
				"key": "value",
			},
		}

		data := bytes.NewReader([]byte("test data"))
		err = storage.PutWithMetadata(ctx, "test/key", data, metadata)
		if err != nil {
			t.Fatalf("failed to put: %v", err)
		}

		// Retrieve metadata
		retrieved, err := storage.GetMetadata(ctx, "test/key")
		if err != nil {
			t.Fatalf("failed to get metadata: %v", err)
		}

		if retrieved.ContentType != "text/plain" {
			t.Errorf("expected ContentType 'text/plain', got '%s'", retrieved.ContentType)
		}
		if retrieved.Custom["key"] != "value" {
			t.Errorf("expected custom key 'value', got '%s'", retrieved.Custom["key"])
		}
	})
}

func TestLocal_GetMetadata(t *testing.T) {
	t.Run("get metadata for existing file with metadata", func(t *testing.T) {
		tmpDir := t.TempDir()
		storage := New()
		err := storage.Configure(map[string]string{"path": tmpDir})
		if err != nil {
			t.Fatalf("failed to configure storage: %v", err)
		}

		ctx := context.Background()
		metadata := &common.Metadata{
			ContentType: "application/xml",
		}

		data := bytes.NewReader([]byte("test data"))
		err = storage.PutWithMetadata(ctx, "test/key", data, metadata)
		if err != nil {
			t.Fatalf("failed to put: %v", err)
		}

		retrieved, err := storage.GetMetadata(ctx, "test/key")
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		if retrieved.ContentType != "application/xml" {
			t.Errorf("expected ContentType 'application/xml', got '%s'", retrieved.ContentType)
		}
	})

	t.Run("get metadata for file without metadata returns nil", func(t *testing.T) {
		tmpDir := t.TempDir()
		storage := New()
		err := storage.Configure(map[string]string{"path": tmpDir})
		if err != nil {
			t.Fatalf("failed to configure storage: %v", err)
		}

		// Create file directly without using PutWithMetadata
		path := filepath.Join(tmpDir, "test/key")
		err = os.MkdirAll(filepath.Dir(path), 0755)
		if err != nil {
			t.Fatalf("failed to create dir: %v", err)
		}
		err = os.WriteFile(path, []byte("data"), 0644)
		if err != nil {
			t.Fatalf("failed to write file: %v", err)
		}

		ctx := context.Background()
		metadata, err := storage.GetMetadata(ctx, "test/key")
		if err == nil {
			t.Error("expected error for missing metadata file")
		}

		if metadata != nil {
			t.Errorf("expected nil metadata when error occurs, got %v", metadata)
		}
	})

	t.Run("get metadata with cancelled context", func(t *testing.T) {
		tmpDir := t.TempDir()
		storage := New()
		err := storage.Configure(map[string]string{"path": tmpDir})
		if err != nil {
			t.Fatalf("failed to configure storage: %v", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err = storage.GetMetadata(ctx, "test/key")
		if err != context.Canceled {
			t.Errorf("expected context.Canceled, got %v", err)
		}
	})
}

func TestLocal_UpdateMetadata(t *testing.T) {
	t.Run("update metadata for existing file", func(t *testing.T) {
		tmpDir := t.TempDir()
		storage := New()
		err := storage.Configure(map[string]string{"path": tmpDir})
		if err != nil {
			t.Fatalf("failed to configure storage: %v", err)
		}

		ctx := context.Background()
		metadata := &common.Metadata{
			ContentType: "text/plain",
		}

		data := bytes.NewReader([]byte("test data"))
		err = storage.PutWithMetadata(ctx, "test/key", data, metadata)
		if err != nil {
			t.Fatalf("failed to put: %v", err)
		}

		// Update metadata
		newMetadata := &common.Metadata{
			ContentType:     "application/json",
			ContentEncoding: "gzip",
			Custom: map[string]string{
				"updated": "true",
			},
		}

		err = storage.UpdateMetadata(ctx, "test/key", newMetadata)
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		// Verify metadata was updated
		retrieved, err := storage.GetMetadata(ctx, "test/key")
		if err != nil {
			t.Fatalf("failed to get metadata: %v", err)
		}

		if retrieved.ContentType != "application/json" {
			t.Errorf("expected ContentType 'application/json', got '%s'", retrieved.ContentType)
		}
		if retrieved.ContentEncoding != "gzip" {
			t.Errorf("expected ContentEncoding 'gzip', got '%s'", retrieved.ContentEncoding)
		}
		if retrieved.Custom["updated"] != "true" {
			t.Errorf("expected custom updated 'true', got '%s'", retrieved.Custom["updated"])
		}
	})

	t.Run("update metadata for non-existing file returns error", func(t *testing.T) {
		tmpDir := t.TempDir()
		storage := New()
		err := storage.Configure(map[string]string{"path": tmpDir})
		if err != nil {
			t.Fatalf("failed to configure storage: %v", err)
		}

		ctx := context.Background()
		metadata := &common.Metadata{
			ContentType: "text/plain",
		}

		err = storage.UpdateMetadata(ctx, "nonexistent", metadata)
		if err == nil {
			t.Error("expected error for non-existing file")
		}
	})

	t.Run("update metadata with cancelled context", func(t *testing.T) {
		tmpDir := t.TempDir()
		storage := New()
		err := storage.Configure(map[string]string{"path": tmpDir})
		if err != nil {
			t.Fatalf("failed to configure storage: %v", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		metadata := &common.Metadata{
			ContentType: "text/plain",
		}

		err = storage.UpdateMetadata(ctx, "test/key", metadata)
		if err != context.Canceled {
			t.Errorf("expected context.Canceled, got %v", err)
		}
	})
}

func TestLocal_DeleteRemovesMetadata(t *testing.T) {
	t.Run("delete removes both file and metadata", func(t *testing.T) {
		tmpDir := t.TempDir()
		storage := New()
		err := storage.Configure(map[string]string{"path": tmpDir})
		if err != nil {
			t.Fatalf("failed to configure storage: %v", err)
		}

		ctx := context.Background()
		metadata := &common.Metadata{
			ContentType: "text/plain",
		}

		data := bytes.NewReader([]byte("test data"))
		err = storage.PutWithMetadata(ctx, "test/key", data, metadata)
		if err != nil {
			t.Fatalf("failed to put: %v", err)
		}

		// Verify metadata file exists
		metadataPath := filepath.Join(tmpDir, "test/key") + metadataSuffix
		if _, err := os.Stat(metadataPath); os.IsNotExist(err) {
			t.Fatal("metadata file was not created")
		}

		// Delete
		err = storage.DeleteWithContext(ctx, "test/key")
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		// Verify metadata file is removed
		if _, err := os.Stat(metadataPath); !os.IsNotExist(err) {
			t.Error("metadata file still exists after delete")
		}
	})
}

func TestLocal_MetadataInListWithOptions(t *testing.T) {
	t.Run("list with options includes metadata", func(t *testing.T) {
		tmpDir := t.TempDir()
		storage := New()
		err := storage.Configure(map[string]string{"path": tmpDir})
		if err != nil {
			t.Fatalf("failed to configure storage: %v", err)
		}

		ctx := context.Background()

		// Put files with metadata
		for i := 1; i <= 3; i++ {
			metadata := &common.Metadata{
				ContentType: "text/plain",
				Custom: map[string]string{
					"index": string(rune('0' + i)),
				},
			}

			data := bytes.NewReader([]byte("data"))
			key := filepath.Join("test", string(rune('0'+i)))
			err = storage.PutWithMetadata(ctx, key, data, metadata)
			if err != nil {
				t.Fatalf("failed to put: %v", err)
			}
		}

		// List with options
		opts := &common.ListOptions{
			Prefix:     "test/",
			MaxResults: 10,
		}

		result, err := storage.ListWithOptions(ctx, opts)
		if err != nil {
			t.Fatalf("failed to list: %v", err)
		}

		if len(result.Objects) != 3 {
			t.Errorf("expected 3 objects, got %d", len(result.Objects))
		}

		for _, obj := range result.Objects {
			if obj.Metadata == nil {
				t.Error("expected metadata to be present")
			}
			if obj.Metadata.ContentType != "text/plain" {
				t.Errorf("expected ContentType 'text/plain', got '%s'", obj.Metadata.ContentType)
			}
		}
	})
}
