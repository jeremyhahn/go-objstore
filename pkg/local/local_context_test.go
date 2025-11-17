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
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLocal_PutWithContext(t *testing.T) {
	t.Run("successful put with valid context", func(t *testing.T) {
		tmpDir := t.TempDir()
		storage := New()
		err := storage.Configure(map[string]string{"path": tmpDir})
		if err != nil {
			t.Fatalf("failed to configure storage: %v", err)
		}

		ctx := context.Background()
		data := bytes.NewReader([]byte("test data"))
		err = storage.PutWithContext(ctx, "test/key", data)
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		// Verify file exists
		path := filepath.Join(tmpDir, "test/key")
		if _, err := os.Stat(path); os.IsNotExist(err) {
			t.Error("file was not created")
		}
	})

	t.Run("put with cancelled context", func(t *testing.T) {
		tmpDir := t.TempDir()
		storage := New()
		err := storage.Configure(map[string]string{"path": tmpDir})
		if err != nil {
			t.Fatalf("failed to configure storage: %v", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		data := bytes.NewReader([]byte("test data"))
		err = storage.PutWithContext(ctx, "test/key", data)
		if err != context.Canceled {
			t.Errorf("expected context.Canceled, got %v", err)
		}
	})

	t.Run("put with timeout context", func(t *testing.T) {
		tmpDir := t.TempDir()
		storage := New()
		err := storage.Configure(map[string]string{"path": tmpDir})
		if err != nil {
			t.Fatalf("failed to configure storage: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
		defer cancel()

		time.Sleep(2 * time.Millisecond) // Wait for timeout

		data := bytes.NewReader([]byte("test data"))
		err = storage.PutWithContext(ctx, "test/key", data)
		if err != context.DeadlineExceeded {
			t.Errorf("expected context.DeadlineExceeded, got %v", err)
		}
	})
}

func TestLocal_GetWithContext(t *testing.T) {
	t.Run("successful get with valid context", func(t *testing.T) {
		tmpDir := t.TempDir()
		storage := New()
		err := storage.Configure(map[string]string{"path": tmpDir})
		if err != nil {
			t.Fatalf("failed to configure storage: %v", err)
		}

		// Put a file first
		ctx := context.Background()
		data := bytes.NewReader([]byte("test data"))
		err = storage.PutWithContext(ctx, "test/key", data)
		if err != nil {
			t.Fatalf("failed to put: %v", err)
		}

		// Get the file
		reader, err := storage.GetWithContext(ctx, "test/key")
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
		defer reader.Close()

		buf := new(bytes.Buffer)
		_, err = buf.ReadFrom(reader)
		if err != nil {
			t.Fatalf("failed to read: %v", err)
		}

		if buf.String() != "test data" {
			t.Errorf("expected 'test data', got '%s'", buf.String())
		}
	})

	t.Run("get with cancelled context", func(t *testing.T) {
		tmpDir := t.TempDir()
		storage := New()
		err := storage.Configure(map[string]string{"path": tmpDir})
		if err != nil {
			t.Fatalf("failed to configure storage: %v", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err = storage.GetWithContext(ctx, "test/key")
		if err != context.Canceled {
			t.Errorf("expected context.Canceled, got %v", err)
		}
	})
}

func TestLocal_DeleteWithContext(t *testing.T) {
	t.Run("successful delete with valid context", func(t *testing.T) {
		tmpDir := t.TempDir()
		storage := New()
		err := storage.Configure(map[string]string{"path": tmpDir})
		if err != nil {
			t.Fatalf("failed to configure storage: %v", err)
		}

		// Put a file first
		ctx := context.Background()
		data := bytes.NewReader([]byte("test data"))
		err = storage.PutWithContext(ctx, "test/key", data)
		if err != nil {
			t.Fatalf("failed to put: %v", err)
		}

		// Delete the file
		err = storage.DeleteWithContext(ctx, "test/key")
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		// Verify file is gone
		path := filepath.Join(tmpDir, "test/key")
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Error("file still exists after delete")
		}
	})

	t.Run("delete with cancelled context", func(t *testing.T) {
		tmpDir := t.TempDir()
		storage := New()
		err := storage.Configure(map[string]string{"path": tmpDir})
		if err != nil {
			t.Fatalf("failed to configure storage: %v", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err = storage.DeleteWithContext(ctx, "test/key")
		if err != context.Canceled {
			t.Errorf("expected context.Canceled, got %v", err)
		}
	})
}

func TestLocal_Exists(t *testing.T) {
	t.Run("exists returns true for existing file", func(t *testing.T) {
		tmpDir := t.TempDir()
		storage := New()
		err := storage.Configure(map[string]string{"path": tmpDir})
		if err != nil {
			t.Fatalf("failed to configure storage: %v", err)
		}

		ctx := context.Background()
		data := bytes.NewReader([]byte("test data"))
		err = storage.PutWithContext(ctx, "test/key", data)
		if err != nil {
			t.Fatalf("failed to put: %v", err)
		}

		exists, err := storage.Exists(ctx, "test/key")
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
		if !exists {
			t.Error("expected exists to be true")
		}
	})

	t.Run("exists returns false for non-existing file", func(t *testing.T) {
		tmpDir := t.TempDir()
		storage := New()
		err := storage.Configure(map[string]string{"path": tmpDir})
		if err != nil {
			t.Fatalf("failed to configure storage: %v", err)
		}

		ctx := context.Background()
		exists, err := storage.Exists(ctx, "nonexistent")
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
		if exists {
			t.Error("expected exists to be false")
		}
	})

	t.Run("exists with cancelled context", func(t *testing.T) {
		tmpDir := t.TempDir()
		storage := New()
		err := storage.Configure(map[string]string{"path": tmpDir})
		if err != nil {
			t.Fatalf("failed to configure storage: %v", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err = storage.Exists(ctx, "test/key")
		if err != context.Canceled {
			t.Errorf("expected context.Canceled, got %v", err)
		}
	})
}

func TestLocal_ListWithContext(t *testing.T) {
	t.Run("list with valid context", func(t *testing.T) {
		tmpDir := t.TempDir()
		storage := New()
		err := storage.Configure(map[string]string{"path": tmpDir})
		if err != nil {
			t.Fatalf("failed to configure storage: %v", err)
		}

		ctx := context.Background()

		// Put multiple files
		files := []string{"a/1", "a/2", "b/1"}
		for _, file := range files {
			data := bytes.NewReader([]byte("data"))
			err = storage.PutWithContext(ctx, file, data)
			if err != nil {
				t.Fatalf("failed to put %s: %v", file, err)
			}
		}

		// List with prefix
		keys, err := storage.ListWithContext(ctx, "a/")
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		if len(keys) != 2 {
			t.Errorf("expected 2 keys, got %d", len(keys))
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

		_, err = storage.ListWithContext(ctx, "")
		if err != context.Canceled {
			t.Errorf("expected context.Canceled, got %v", err)
		}
	})
}

func TestLocal_BackwardCompatibility(t *testing.T) {
	t.Run("old Put method still works", func(t *testing.T) {
		tmpDir := t.TempDir()
		storage := New()
		err := storage.Configure(map[string]string{"path": tmpDir})
		if err != nil {
			t.Fatalf("failed to configure storage: %v", err)
		}

		data := bytes.NewReader([]byte("test data"))
		err = storage.Put("test/key", data)
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		// Verify file exists
		path := filepath.Join(tmpDir, "test/key")
		if _, err := os.Stat(path); os.IsNotExist(err) {
			t.Error("file was not created")
		}
	})

	t.Run("old Get method still works", func(t *testing.T) {
		tmpDir := t.TempDir()
		storage := New()
		err := storage.Configure(map[string]string{"path": tmpDir})
		if err != nil {
			t.Fatalf("failed to configure storage: %v", err)
		}

		data := bytes.NewReader([]byte("test data"))
		err = storage.Put("test/key", data)
		if err != nil {
			t.Fatalf("failed to put: %v", err)
		}

		reader, err := storage.Get("test/key")
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
		defer reader.Close()
	})

	t.Run("old Delete method still works", func(t *testing.T) {
		tmpDir := t.TempDir()
		storage := New()
		err := storage.Configure(map[string]string{"path": tmpDir})
		if err != nil {
			t.Fatalf("failed to configure storage: %v", err)
		}

		data := bytes.NewReader([]byte("test data"))
		err = storage.Put("test/key", data)
		if err != nil {
			t.Fatalf("failed to put: %v", err)
		}

		err = storage.Delete("test/key")
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
	})

	t.Run("old List method still works", func(t *testing.T) {
		tmpDir := t.TempDir()
		storage := New()
		err := storage.Configure(map[string]string{"path": tmpDir})
		if err != nil {
			t.Fatalf("failed to configure storage: %v", err)
		}

		data := bytes.NewReader([]byte("test data"))
		err = storage.Put("test/key", data)
		if err != nil {
			t.Fatalf("failed to put: %v", err)
		}

		keys, err := storage.List("test/")
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		if len(keys) != 1 {
			t.Errorf("expected 1 key, got %d", len(keys))
		}
	})
}
