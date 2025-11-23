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

package objstore

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/factory"
)

// TestFacadeIntegrationMultipleBackends tests the facade with real storage backends
func TestFacadeIntegrationMultipleBackends(t *testing.T) {
	// Create temporary directories for local backends
	tmpDir := t.TempDir()
	localPath := filepath.Join(tmpDir, "local")
	localPath2 := filepath.Join(tmpDir, "local2")

	// Create local storage backends
	local1, err := factory.NewStorage("local", map[string]string{
		"path": localPath,
	})
	if err != nil {
		t.Fatalf("Failed to create local storage 1: %v", err)
	}

	local2, err := factory.NewStorage("local", map[string]string{
		"path": localPath2,
	})
	if err != nil {
		t.Fatalf("Failed to create local storage 2: %v", err)
	}

	// Initialize facade with multiple backends
	Reset()
	err = Initialize(&FacadeConfig{
		Backends: map[string]common.Storage{
			"local":  local1,
			"local2": local2,
		},
		DefaultBackend: "local",
	})
	if err != nil {
		t.Fatalf("Failed to initialize facade: %v", err)
	}
	defer Reset()

	// Test 1: Put and Get from default backend
	t.Run("PutAndGetDefaultBackend", func(t *testing.T) {
		data := []byte("hello world")
		err := Put("test1.txt", bytes.NewReader(data))
		if err != nil {
			t.Fatalf("Put() error = %v", err)
		}

		// Verify file exists in local storage
		filePath := filepath.Join(localPath, "test1.txt")
		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			t.Error("Expected file to exist in local storage")
		}

		// Get the data back
		reader, err := Get("test1.txt")
		if err != nil {
			t.Fatalf("Get() error = %v", err)
		}
		defer reader.Close()

		content, _ := io.ReadAll(reader)
		if !bytes.Equal(content, data) {
			t.Errorf("Expected %q, got %q", data, content)
		}
	})

	// Test 2: Put and Get with backend prefix
	t.Run("PutAndGetWithBackendPrefix", func(t *testing.T) {
		ctx := context.Background()
		data := []byte("data in backend 2")

		err := PutWithContext(ctx, "local2:test2.txt", bytes.NewReader(data))
		if err != nil {
			t.Fatalf("PutWithContext() error = %v", err)
		}

		// Verify file exists in local2 storage
		filePath := filepath.Join(localPath2, "test2.txt")
		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			t.Error("Expected file to exist in local2 storage")
		}

		// Get the data back
		reader, err := GetWithContext(ctx, "local2:test2.txt")
		if err != nil {
			t.Fatalf("GetWithContext() error = %v", err)
		}
		defer reader.Close()

		content, _ := io.ReadAll(reader)
		if !bytes.Equal(content, data) {
			t.Errorf("Expected %q, got %q", data, content)
		}
	})

	// Test 3: Put with metadata
	t.Run("PutWithMetadata", func(t *testing.T) {
		ctx := context.Background()
		data := []byte("data with metadata")
		metadata := &common.Metadata{
			ContentType: "text/plain",
			Custom: map[string]string{
				"author": "test",
				"version": "1.0",
			},
		}

		err := PutWithMetadata(ctx, "test3.txt", bytes.NewReader(data), metadata)
		if err != nil {
			t.Fatalf("PutWithMetadata() error = %v", err)
		}

		// Get metadata back
		retrievedMetadata, err := GetMetadata(ctx, "test3.txt")
		if err != nil {
			t.Fatalf("GetMetadata() error = %v", err)
		}

		if retrievedMetadata.ContentType != metadata.ContentType {
			t.Errorf("Expected ContentType %q, got %q", metadata.ContentType, retrievedMetadata.ContentType)
		}
	})

	// Test 4: List objects
	t.Run("List", func(t *testing.T) {
		ctx := context.Background()

		// Create multiple objects
		objects := map[string]string{
			"logs/app.log":   "log1",
			"logs/error.log": "log2",
			"data/file.txt":  "data",
		}

		for key, content := range objects {
			err := PutWithContext(ctx, key, bytes.NewReader([]byte(content)))
			if err != nil {
				t.Fatalf("Failed to put %s: %v", key, err)
			}
		}

		// List with prefix
		keys, err := ListWithContext(ctx, "logs/")
		if err != nil {
			t.Fatalf("ListWithContext() error = %v", err)
		}

		if len(keys) != 2 {
			t.Errorf("Expected 2 keys with 'logs/' prefix, got %d", len(keys))
		}

		// List all
		allKeys, err := ListWithContext(ctx, "")
		if err != nil {
			t.Fatalf("ListWithContext() error = %v", err)
		}

		// Should have at least the 3 objects we just created (plus any from previous tests)
		if len(allKeys) < 3 {
			t.Errorf("Expected at least 3 keys, got %d", len(allKeys))
		}
	})

	// Test 5: Exists
	t.Run("Exists", func(t *testing.T) {
		ctx := context.Background()

		// Create an object
		err := PutWithContext(ctx, "exists-test.txt", bytes.NewReader([]byte("test")))
		if err != nil {
			t.Fatalf("Put() error = %v", err)
		}

		// Check exists
		exists, err := Exists(ctx, "exists-test.txt")
		if err != nil {
			t.Fatalf("Exists() error = %v", err)
		}
		if !exists {
			t.Error("Expected object to exist")
		}

		// Check non-existent
		exists, err = Exists(ctx, "non-existent.txt")
		if err != nil {
			t.Fatalf("Exists() error = %v", err)
		}
		if exists {
			t.Error("Expected object not to exist")
		}
	})

	// Test 6: Delete
	t.Run("Delete", func(t *testing.T) {
		ctx := context.Background()

		// Create an object
		err := PutWithContext(ctx, "delete-test.txt", bytes.NewReader([]byte("test")))
		if err != nil {
			t.Fatalf("Put() error = %v", err)
		}

		// Verify it exists
		exists, _ := Exists(ctx, "delete-test.txt")
		if !exists {
			t.Fatal("Expected object to exist before delete")
		}

		// Delete it
		err = DeleteWithContext(ctx, "delete-test.txt")
		if err != nil {
			t.Fatalf("DeleteWithContext() error = %v", err)
		}

		// Verify it's gone
		exists, _ = Exists(ctx, "delete-test.txt")
		if exists {
			t.Error("Expected object to be deleted")
		}
	})

	// Test 7: Multiple backends isolation
	t.Run("MultipleBackendsIsolation", func(t *testing.T) {
		ctx := context.Background()

		// Put same key in different backends
		err := PutWithContext(ctx, "local:shared.txt", bytes.NewReader([]byte("in local")))
		if err != nil {
			t.Fatalf("Put to local error = %v", err)
		}

		err = PutWithContext(ctx, "local2:shared.txt", bytes.NewReader([]byte("in local2")))
		if err != nil {
			t.Fatalf("Put to local2 error = %v", err)
		}

		// Retrieve from local
		reader1, err := GetWithContext(ctx, "local:shared.txt")
		if err != nil {
			t.Fatalf("Get from local error = %v", err)
		}
		content1, _ := io.ReadAll(reader1)
		reader1.Close()

		// Retrieve from local2
		reader2, err := GetWithContext(ctx, "local2:shared.txt")
		if err != nil {
			t.Fatalf("Get from local2 error = %v", err)
		}
		content2, _ := io.ReadAll(reader2)
		reader2.Close()

		// Verify they're different
		if bytes.Equal(content1, content2) {
			t.Error("Expected different content from different backends")
		}

		if string(content1) != "in local" {
			t.Errorf("Expected 'in local', got %q", content1)
		}

		if string(content2) != "in local2" {
			t.Errorf("Expected 'in local2', got %q", content2)
		}
	})

	// Test 8: ListWithOptions
	t.Run("ListWithOptions", func(t *testing.T) {
		ctx := context.Background()

		// Create some objects for testing
		for i := 0; i < 5; i++ {
			key := filepath.Join("test-list", string(rune('a'+i))+".txt")
			err := PutWithContext(ctx, key, bytes.NewReader([]byte("data")))
			if err != nil {
				t.Fatalf("Put error = %v", err)
			}
		}

		opts := &common.ListOptions{
			Prefix:     "test-list/",
			MaxResults: 3,
		}

		result, err := ListWithOptions(ctx, "", opts)
		if err != nil {
			t.Fatalf("ListWithOptions() error = %v", err)
		}

		if len(result.Objects) == 0 {
			t.Error("Expected some objects in result")
		}

		// Verify objects have metadata
		for _, obj := range result.Objects {
			if obj.Metadata == nil {
				t.Error("Expected object to have metadata")
			}
		}
	})
}

// TestFacadeIntegrationSecurity tests security features of the facade
func TestFacadeIntegrationSecurity(t *testing.T) {
	tmpDir := t.TempDir()
	localPath := filepath.Join(tmpDir, "local")

	local, err := factory.NewStorage("local", map[string]string{
		"path": localPath,
	})
	if err != nil {
		t.Fatalf("Failed to create local storage: %v", err)
	}

	Reset()
	err = Initialize(&FacadeConfig{
		Backends: map[string]common.Storage{
			"local": local,
		},
		DefaultBackend: "local",
	})
	if err != nil {
		t.Fatalf("Failed to initialize facade: %v", err)
	}
	defer Reset()

	// Test path traversal prevention
	t.Run("PathTraversalPrevention", func(t *testing.T) {
		ctx := context.Background()

		attacks := []string{
			"../etc/passwd",
			"path/../../../etc/passwd",
			"foo/../../etc/passwd",
		}

		for _, attack := range attacks {
			err := PutWithContext(ctx, attack, bytes.NewReader([]byte("malicious")))
			if err == nil {
				t.Errorf("Expected error for path traversal attack: %s", attack)
			}

			_, err = GetWithContext(ctx, attack)
			if err == nil {
				t.Errorf("Expected error for path traversal attack: %s", attack)
			}

			err = DeleteWithContext(ctx, attack)
			if err == nil {
				t.Errorf("Expected error for path traversal attack: %s", attack)
			}
		}
	})

	// Test invalid backend names
	t.Run("InvalidBackendNames", func(t *testing.T) {
		ctx := context.Background()

		invalidBackends := []string{
			"UPPERCASE:key",
			"bad_chars:key",
			"has space:key",
		}

		for _, ref := range invalidBackends {
			err := PutWithContext(ctx, ref, bytes.NewReader([]byte("data")))
			if err == nil {
				t.Errorf("Expected error for invalid backend reference: %s", ref)
			}
		}
	})

	// Test null byte prevention
	t.Run("NullBytePrevention", func(t *testing.T) {
		ctx := context.Background()

		attacks := []string{
			"file\x00.txt",
			"data\x00/file.txt",
		}

		for _, attack := range attacks {
			err := PutWithContext(ctx, attack, bytes.NewReader([]byte("malicious")))
			if err == nil {
				t.Errorf("Expected error for null byte attack: %q", attack)
			}
		}
	})

	// Test control character prevention
	t.Run("ControlCharacterPrevention", func(t *testing.T) {
		ctx := context.Background()

		attacks := []string{
			"file\ndata.txt",
			"file\rdata.txt",
			"file\tdata.txt",
		}

		for _, attack := range attacks {
			err := PutWithContext(ctx, attack, bytes.NewReader([]byte("malicious")))
			if err == nil {
				t.Errorf("Expected error for control character attack: %q", attack)
			}
		}
	})
}
