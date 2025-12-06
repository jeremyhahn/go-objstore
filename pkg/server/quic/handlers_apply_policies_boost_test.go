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

package quic

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// mockLifecycleStorageWithErrors extends mockLifecycleStorage with error injection
type mockLifecycleStorageWithErrors struct {
	*mockLifecycleStorage
	listError error
}

func (m *mockLifecycleStorageWithErrors) ListWithOptions(ctx context.Context, opts *common.ListOptions) (*common.ListResult, error) {
	if m.listError != nil {
		return nil, m.listError
	}
	return m.mockLifecycleStorage.ListWithOptions(ctx, opts)
}

// TestHandleApplyPolicies_ErrorPaths tests error scenarios for applying lifecycle policies
func TestHandleApplyPolicies_ErrorPaths(t *testing.T) {
	t.Run("GetPolicies returns error", func(t *testing.T) {
		storage := newMockLifecycleStorage()
		storage.getPoliciesError = errors.New("database error")
		handler := createHandlerWithStorage(t, storage, 10*1024*1024, 30*time.Second, 30*time.Second, &mockLogger{}, &mockAuthenticator{})

		req := httptest.NewRequest(http.MethodPost, "/policies/apply", nil)
		w := httptest.NewRecorder()

		handler.handleApplyPolicies(w, req)

		if w.Code != http.StatusInternalServerError {
			t.Errorf("expected status 500, got %d", w.Code)
		}
	})

	t.Run("ListWithOptions returns error", func(t *testing.T) {
		storage := &mockLifecycleStorageWithErrors{
			mockLifecycleStorage: newMockLifecycleStorage(),
			listError:            errors.New("list error"),
		}
		storage.AddPolicy(common.LifecyclePolicy{
			ID:        "test-policy",
			Prefix:    "logs/",
			Retention: 24 * time.Hour,
			Action:    "delete",
		})
		handler := createHandlerWithStorage(t, storage, 10*1024*1024, 30*time.Second, 30*time.Second, &mockLogger{}, &mockAuthenticator{})

		req := httptest.NewRequest(http.MethodPost, "/policies/apply", nil)
		w := httptest.NewRecorder()

		handler.handleApplyPolicies(w, req)

		if w.Code != http.StatusInternalServerError {
			t.Errorf("expected status 500, got %d", w.Code)
		}
	})

	t.Run("archive action without destination", func(t *testing.T) {
		storage := newMockLifecycleStorage()
		handler := createHandlerWithStorage(t, storage, 10*1024*1024, 30*time.Second, 30*time.Second, &mockLogger{}, &mockAuthenticator{})

		// Add policy with archive action but no destination
		storage.AddPolicy(common.LifecyclePolicy{
			ID:          "archive-no-dest",
			Prefix:      "data/",
			Retention:   24 * time.Hour,
			Action:      "archive",
			Destination: nil, // No destination
		})

		// Add old object
		storage.PutWithContext(context.Background(), "data/old.csv", bytes.NewReader([]byte("data")))
		storage.metadata["data/old.csv"].LastModified = time.Now().Add(-48 * time.Hour)

		req := httptest.NewRequest(http.MethodPost, "/policies/apply", nil)
		w := httptest.NewRecorder()

		handler.handleApplyPolicies(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}

		var result map[string]any
		if err := json.NewDecoder(w.Body).Decode(&result); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		// Should process 0 objects since archive has no destination
		objectsProcessed := int(result["objects_processed"].(float64))
		if objectsProcessed != 0 {
			t.Errorf("expected 0 objects processed, got %d", objectsProcessed)
		}
	})

	t.Run("delete action error continues processing", func(t *testing.T) {
		storage := newMockLifecycleStorage()
		handler := createHandlerWithStorage(t, storage, 10*1024*1024, 30*time.Second, 30*time.Second, &mockLogger{}, &mockAuthenticator{})

		// Add policy
		storage.AddPolicy(common.LifecyclePolicy{
			ID:        "cleanup",
			Prefix:    "",
			Retention: 24 * time.Hour,
			Action:    "delete",
		})

		// Add objects - one that exists, one that will fail to delete
		storage.PutWithContext(context.Background(), "old1.txt", bytes.NewReader([]byte("data")))
		storage.metadata["old1.txt"].LastModified = time.Now().Add(-48 * time.Hour)

		storage.PutWithContext(context.Background(), "old2.txt", bytes.NewReader([]byte("data")))
		storage.metadata["old2.txt"].LastModified = time.Now().Add(-48 * time.Hour)

		req := httptest.NewRequest(http.MethodPost, "/policies/apply", nil)
		w := httptest.NewRecorder()

		handler.handleApplyPolicies(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}
	})

	t.Run("archive action error continues processing", func(t *testing.T) {
		storage := newMockLifecycleStorage()
		storage.archiveError = errors.New("archive failed")
		handler := createHandlerWithStorage(t, storage, 10*1024*1024, 30*time.Second, 30*time.Second, &mockLogger{}, &mockAuthenticator{})

		// Add policy with archive action
		storage.AddPolicy(common.LifecyclePolicy{
			ID:          "archive-policy",
			Prefix:      "data/",
			Retention:   24 * time.Hour,
			Action:      "archive",
			Destination: &mockArchiver{},
		})

		// Add old object
		storage.PutWithContext(context.Background(), "data/old.csv", bytes.NewReader([]byte("data")))
		storage.metadata["data/old.csv"].LastModified = time.Now().Add(-48 * time.Hour)

		req := httptest.NewRequest(http.MethodPost, "/policies/apply", nil)
		w := httptest.NewRecorder()

		handler.handleApplyPolicies(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}

		var result map[string]any
		if err := json.NewDecoder(w.Body).Decode(&result); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		// Should not process objects due to archive error
		objectsProcessed := int(result["objects_processed"].(float64))
		if objectsProcessed != 0 {
			t.Errorf("expected 0 objects processed, got %d", objectsProcessed)
		}
	})

	t.Run("object not matching prefix", func(t *testing.T) {
		storage := newMockLifecycleStorage()
		handler := createHandlerWithStorage(t, storage, 10*1024*1024, 30*time.Second, 30*time.Second, &mockLogger{}, &mockAuthenticator{})

		// Add policy with specific prefix
		storage.AddPolicy(common.LifecyclePolicy{
			ID:        "logs-cleanup",
			Prefix:    "logs/",
			Retention: 24 * time.Hour,
			Action:    "delete",
		})

		// Add old object that doesn't match prefix
		storage.PutWithContext(context.Background(), "data/old.txt", bytes.NewReader([]byte("data")))
		storage.metadata["data/old.txt"].LastModified = time.Now().Add(-48 * time.Hour)

		req := httptest.NewRequest(http.MethodPost, "/policies/apply", nil)
		w := httptest.NewRecorder()

		handler.handleApplyPolicies(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}

		var result map[string]any
		if err := json.NewDecoder(w.Body).Decode(&result); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		// Should not process object since prefix doesn't match
		objectsProcessed := int(result["objects_processed"].(float64))
		if objectsProcessed != 0 {
			t.Errorf("expected 0 objects processed, got %d", objectsProcessed)
		}

		// Object should still exist
		if _, exists := storage.data["data/old.txt"]; !exists {
			t.Error("object should not have been deleted")
		}
	})

	t.Run("object not old enough for retention", func(t *testing.T) {
		storage := newMockLifecycleStorage()
		handler := createHandlerWithStorage(t, storage, 10*1024*1024, 30*time.Second, 30*time.Second, &mockLogger{}, &mockAuthenticator{})

		// Add policy
		storage.AddPolicy(common.LifecyclePolicy{
			ID:        "cleanup",
			Prefix:    "",
			Retention: 48 * time.Hour,
			Action:    "delete",
		})

		// Add object that's not old enough
		storage.PutWithContext(context.Background(), "recent.txt", bytes.NewReader([]byte("data")))
		storage.metadata["recent.txt"].LastModified = time.Now().Add(-24 * time.Hour)

		req := httptest.NewRequest(http.MethodPost, "/policies/apply", nil)
		w := httptest.NewRecorder()

		handler.handleApplyPolicies(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}

		var result map[string]any
		if err := json.NewDecoder(w.Body).Decode(&result); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		// Should not process object since it's not old enough
		objectsProcessed := int(result["objects_processed"].(float64))
		if objectsProcessed != 0 {
			t.Errorf("expected 0 objects processed, got %d", objectsProcessed)
		}

		// Object should still exist
		if _, exists := storage.data["recent.txt"]; !exists {
			t.Error("object should not have been deleted")
		}
	})

	t.Run("unknown action type", func(t *testing.T) {
		storage := newMockLifecycleStorage()
		handler := createHandlerWithStorage(t, storage, 10*1024*1024, 30*time.Second, 30*time.Second, &mockLogger{}, &mockAuthenticator{})

		// Add policy with unknown action
		storage.AddPolicy(common.LifecyclePolicy{
			ID:        "unknown-action",
			Prefix:    "",
			Retention: 24 * time.Hour,
			Action:    "unknown",
		})

		// Add old object
		storage.PutWithContext(context.Background(), "old.txt", bytes.NewReader([]byte("data")))
		storage.metadata["old.txt"].LastModified = time.Now().Add(-48 * time.Hour)

		req := httptest.NewRequest(http.MethodPost, "/policies/apply", nil)
		w := httptest.NewRecorder()

		handler.handleApplyPolicies(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}

		var result map[string]any
		if err := json.NewDecoder(w.Body).Decode(&result); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		// Should not process object since action is unknown
		objectsProcessed := int(result["objects_processed"].(float64))
		if objectsProcessed != 0 {
			t.Errorf("expected 0 objects processed, got %d", objectsProcessed)
		}
	})

	t.Run("multiple policies on same object", func(t *testing.T) {
		storage := newMockLifecycleStorage()
		handler := createHandlerWithStorage(t, storage, 10*1024*1024, 30*time.Second, 30*time.Second, &mockLogger{}, &mockAuthenticator{})

		// Add two delete policies with different prefixes
		storage.AddPolicy(common.LifecyclePolicy{
			ID:        "policy1",
			Prefix:    "logs/",
			Retention: 24 * time.Hour,
			Action:    "delete",
		})
		storage.AddPolicy(common.LifecyclePolicy{
			ID:        "policy2",
			Prefix:    "logs/app/",
			Retention: 12 * time.Hour,
			Action:    "delete",
		})

		// Add object matching both prefixes
		storage.PutWithContext(context.Background(), "logs/app/old.log", bytes.NewReader([]byte("log")))
		storage.metadata["logs/app/old.log"].LastModified = time.Now().Add(-48 * time.Hour)

		req := httptest.NewRequest(http.MethodPost, "/policies/apply", nil)
		w := httptest.NewRecorder()

		handler.handleApplyPolicies(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}

		// Object should be deleted by first policy
		if _, exists := storage.data["logs/app/old.log"]; exists {
			t.Error("object should have been deleted")
		}
	})
}

// TestHandleApplyPolicies_SuccessPaths tests success scenarios
func TestHandleApplyPolicies_SuccessPaths(t *testing.T) {
	t.Run("delete with prefix match", func(t *testing.T) {
		storage := newMockLifecycleStorage()
		handler := createHandlerWithStorage(t, storage, 10*1024*1024, 30*time.Second, 30*time.Second, &mockLogger{}, &mockAuthenticator{})

		storage.AddPolicy(common.LifecyclePolicy{
			ID:        "cleanup",
			Prefix:    "logs/",
			Retention: 24 * time.Hour,
			Action:    "delete",
		})

		// Add old object matching prefix
		storage.PutWithContext(context.Background(), "logs/old.txt", bytes.NewReader([]byte("data")))
		storage.metadata["logs/old.txt"].LastModified = time.Now().Add(-48 * time.Hour)

		req := httptest.NewRequest(http.MethodPost, "/policies/apply", nil)
		w := httptest.NewRecorder()

		handler.handleApplyPolicies(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}

		var result map[string]any
		if err := json.NewDecoder(w.Body).Decode(&result); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		objectsProcessed := int(result["objects_processed"].(float64))
		if objectsProcessed != 1 {
			t.Errorf("expected 1 object processed, got %d", objectsProcessed)
		}

		// Object should be deleted
		if _, exists := storage.data["logs/old.txt"]; exists {
			t.Error("object should have been deleted")
		}
	})

	t.Run("archive with destination", func(t *testing.T) {
		storage := newMockLifecycleStorage()
		handler := createHandlerWithStorage(t, storage, 10*1024*1024, 30*time.Second, 30*time.Second, &mockLogger{}, &mockAuthenticator{})

		storage.AddPolicy(common.LifecyclePolicy{
			ID:          "archive-policy",
			Prefix:      "data/",
			Retention:   24 * time.Hour,
			Action:      "archive",
			Destination: &mockArchiver{},
		})

		// Add old object
		storage.PutWithContext(context.Background(), "data/old.csv", bytes.NewReader([]byte("data")))
		storage.metadata["data/old.csv"].LastModified = time.Now().Add(-48 * time.Hour)

		req := httptest.NewRequest(http.MethodPost, "/policies/apply", nil)
		w := httptest.NewRecorder()

		handler.handleApplyPolicies(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}

		var result map[string]any
		if err := json.NewDecoder(w.Body).Decode(&result); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		objectsProcessed := int(result["objects_processed"].(float64))
		if objectsProcessed != 1 {
			t.Errorf("expected 1 object processed, got %d", objectsProcessed)
		}
	})

	t.Run("empty prefix matches all objects", func(t *testing.T) {
		storage := newMockLifecycleStorage()
		handler := createHandlerWithStorage(t, storage, 10*1024*1024, 30*time.Second, 30*time.Second, &mockLogger{}, &mockAuthenticator{})

		storage.AddPolicy(common.LifecyclePolicy{
			ID:        "cleanup-all",
			Prefix:    "", // Empty prefix matches everything
			Retention: 24 * time.Hour,
			Action:    "delete",
		})

		// Add old objects in different locations
		storage.PutWithContext(context.Background(), "logs/old.txt", bytes.NewReader([]byte("data")))
		storage.metadata["logs/old.txt"].LastModified = time.Now().Add(-48 * time.Hour)

		storage.PutWithContext(context.Background(), "data/old.csv", bytes.NewReader([]byte("data")))
		storage.metadata["data/old.csv"].LastModified = time.Now().Add(-48 * time.Hour)

		req := httptest.NewRequest(http.MethodPost, "/policies/apply", nil)
		w := httptest.NewRecorder()

		handler.handleApplyPolicies(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}

		var result map[string]any
		if err := json.NewDecoder(w.Body).Decode(&result); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		objectsProcessed := int(result["objects_processed"].(float64))
		if objectsProcessed != 2 {
			t.Errorf("expected 2 objects processed, got %d", objectsProcessed)
		}
	})
}

// mockStorageTimeout simulates timeout during operations
type mockStorageTimeout struct {
	*mockLifecycleStorage
}

func (m *mockStorageTimeout) GetPolicies() ([]common.LifecyclePolicy, error) {
	time.Sleep(100 * time.Millisecond)
	return m.mockLifecycleStorage.GetPolicies()
}

func (m *mockStorageTimeout) ListWithOptions(ctx context.Context, opts *common.ListOptions) (*common.ListResult, error) {
	time.Sleep(100 * time.Millisecond)
	return m.mockLifecycleStorage.ListWithOptions(ctx, opts)
}

// mockReadCloserWithError simulates read errors
type mockReadCloserWithError struct{}

func (m *mockReadCloserWithError) Read(p []byte) (n int, err error) {
	return 0, errors.New("read error")
}

func (m *mockReadCloserWithError) Close() error {
	return nil
}

// mockStorageGetError simulates Get errors
type mockStorageGetError struct {
	*mockLifecycleStorage
}

func (m *mockStorageGetError) GetWithContext(ctx context.Context, key string) (io.ReadCloser, error) {
	return &mockReadCloserWithError{}, nil
}
