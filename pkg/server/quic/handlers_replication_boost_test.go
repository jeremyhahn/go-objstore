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
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// TestHandleGetReplicationPolicies_ErrorPaths tests error scenarios for getting replication policies
func TestHandleGetReplicationPolicies_ErrorPaths(t *testing.T) {
	t.Run("GetReplicationManager returns ErrReplicationNotSupported", func(t *testing.T) {
		storage := NewMockStorageWithReplication()
		storage.repErr = common.ErrReplicationNotSupported
		handler := setupReplicationTestHandler(t, storage)

		req := httptest.NewRequest(http.MethodGet, "/replication/policies", nil)
		w := httptest.NewRecorder()

		handler.handleGetReplicationPolicies(w, req)

		if w.Code != http.StatusInternalServerError {
			t.Errorf("expected status 500, got %d", w.Code)
		}

		if !bytes.Contains(w.Body.Bytes(), []byte("replication not supported")) {
			t.Errorf("expected 'replication not supported' error, got: %s", w.Body.String())
		}
	})

	t.Run("GetReplicationManager returns other error", func(t *testing.T) {
		storage := NewMockStorageWithReplication()
		storage.repErr = errors.New("some other error")
		handler := setupReplicationTestHandler(t, storage)

		req := httptest.NewRequest(http.MethodGet, "/replication/policies", nil)
		w := httptest.NewRecorder()

		handler.handleGetReplicationPolicies(w, req)

		if w.Code != http.StatusInternalServerError {
			t.Errorf("expected status 500, got %d", w.Code)
		}
	})

	t.Run("GetPolicies returns error", func(t *testing.T) {
		storage := NewMockStorageWithReplication()
		storage.repMgr.getError = errors.New("failed to get policies")
		handler := setupReplicationTestHandler(t, storage)

		req := httptest.NewRequest(http.MethodGet, "/replication/policies", nil)
		w := httptest.NewRecorder()

		handler.handleGetReplicationPolicies(w, req)

		if w.Code != http.StatusInternalServerError {
			t.Errorf("expected status 500, got %d", w.Code)
		}
	})

	t.Run("empty policies list", func(t *testing.T) {
		storage := NewMockStorageWithReplication()
		handler := setupReplicationTestHandler(t, storage)

		req := httptest.NewRequest(http.MethodGet, "/replication/policies", nil)
		w := httptest.NewRecorder()

		handler.handleGetReplicationPolicies(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}

		var response map[string]any
		if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		count := int(response["count"].(float64))
		if count != 0 {
			t.Errorf("expected 0 policies, got %d", count)
		}
	})
}

// TestHandleAddReplicationPolicy_ErrorPaths tests error scenarios for adding replication policies
func TestHandleAddReplicationPolicy_ErrorPaths(t *testing.T) {
	t.Run("invalid JSON body", func(t *testing.T) {
		storage := NewMockStorageWithReplication()
		handler := setupReplicationTestHandler(t, storage)

		req := httptest.NewRequest(http.MethodPost, "/replication/policies", bytes.NewReader([]byte("invalid json")))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		handler.handleAddReplicationPolicy(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("expected status 400, got %d", w.Code)
		}
	})

	t.Run("default replication mode", func(t *testing.T) {
		storage := NewMockStorageWithReplication()
		handler := setupReplicationTestHandler(t, storage)

		body, _ := json.Marshal(map[string]any{
			"id":                  "test-default-mode",
			"source_backend":      "local",
			"destination_backend": "s3",
			"check_interval":      300,
			"enabled":             true,
			// No replication_mode specified - should default to transparent
		})
		req := httptest.NewRequest(http.MethodPost, "/replication/policies", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		handler.handleAddReplicationPolicy(w, req)

		if w.Code != http.StatusCreated {
			t.Errorf("expected status 201, got %d", w.Code)
		}

		policy, err := storage.repMgr.GetPolicy("test-default-mode")
		if err != nil {
			t.Fatalf("failed to get policy: %v", err)
		}

		if policy.ReplicationMode != common.ReplicationModeTransparent {
			t.Errorf("expected default replication mode to be transparent, got %s", policy.ReplicationMode)
		}
	})

	t.Run("GetReplicationManager returns ErrReplicationNotSupported", func(t *testing.T) {
		storage := NewMockStorageWithReplication()
		storage.repErr = common.ErrReplicationNotSupported
		handler := setupReplicationTestHandler(t, storage)

		body, _ := json.Marshal(map[string]any{
			"id":                  "test-policy",
			"source_backend":      "local",
			"destination_backend": "s3",
			"check_interval":      300,
		})
		req := httptest.NewRequest(http.MethodPost, "/replication/policies", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		handler.handleAddReplicationPolicy(w, req)

		if w.Code != http.StatusInternalServerError {
			t.Errorf("expected status 500, got %d", w.Code)
		}
	})

	t.Run("GetReplicationManager returns other error", func(t *testing.T) {
		storage := NewMockStorageWithReplication()
		storage.repErr = errors.New("some other error")
		handler := setupReplicationTestHandler(t, storage)

		body, _ := json.Marshal(map[string]any{
			"id":                  "test-policy",
			"source_backend":      "local",
			"destination_backend": "s3",
			"check_interval":      300,
		})
		req := httptest.NewRequest(http.MethodPost, "/replication/policies", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		handler.handleAddReplicationPolicy(w, req)

		if w.Code != http.StatusInternalServerError {
			t.Errorf("expected status 500, got %d", w.Code)
		}
	})

	t.Run("AddPolicy returns general error", func(t *testing.T) {
		storage := NewMockStorageWithReplication()
		storage.repMgr.addError = errors.New("database error")
		handler := setupReplicationTestHandler(t, storage)

		body, _ := json.Marshal(map[string]any{
			"id":                  "test-policy",
			"source_backend":      "local",
			"destination_backend": "s3",
			"check_interval":      300,
		})
		req := httptest.NewRequest(http.MethodPost, "/replication/policies", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		handler.handleAddReplicationPolicy(w, req)

		if w.Code != http.StatusInternalServerError {
			t.Errorf("expected status 500, got %d", w.Code)
		}
	})

	t.Run("with encryption policy", func(t *testing.T) {
		storage := NewMockStorageWithReplication()
		handler := setupReplicationTestHandler(t, storage)

		body, _ := json.Marshal(map[string]any{
			"id":                  "test-with-encryption",
			"source_backend":      "local",
			"destination_backend": "s3",
			"check_interval":      300,
			"enabled":             true,
			"encryption": map[string]any{
				"backend": map[string]any{
					"enabled":  true,
					"provider": "custom",
				},
			},
		})
		req := httptest.NewRequest(http.MethodPost, "/replication/policies", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		handler.handleAddReplicationPolicy(w, req)

		if w.Code != http.StatusCreated {
			t.Errorf("expected status 201, got %d", w.Code)
		}
	})
}

// TestHandleGetReplicationPolicy_ErrorPaths tests error scenarios for getting a single replication policy
func TestHandleGetReplicationPolicy_ErrorPaths(t *testing.T) {
	t.Run("empty policy ID", func(t *testing.T) {
		storage := NewMockStorageWithReplication()
		handler := setupReplicationTestHandler(t, storage)

		req := httptest.NewRequest(http.MethodGet, "/replication/policies/", nil)
		w := httptest.NewRecorder()

		handler.handleGetReplicationPolicy(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("expected status 400, got %d", w.Code)
		}
	})

	t.Run("GetReplicationManager returns ErrReplicationNotSupported", func(t *testing.T) {
		storage := NewMockStorageWithReplication()
		storage.repErr = common.ErrReplicationNotSupported
		handler := setupReplicationTestHandler(t, storage)

		req := httptest.NewRequest(http.MethodGet, "/replication/policies/test-id", nil)
		w := httptest.NewRecorder()

		handler.handleGetReplicationPolicy(w, req)

		if w.Code != http.StatusInternalServerError {
			t.Errorf("expected status 500, got %d", w.Code)
		}
	})

	t.Run("GetReplicationManager returns other error", func(t *testing.T) {
		storage := NewMockStorageWithReplication()
		storage.repErr = errors.New("some other error")
		handler := setupReplicationTestHandler(t, storage)

		req := httptest.NewRequest(http.MethodGet, "/replication/policies/test-id", nil)
		w := httptest.NewRecorder()

		handler.handleGetReplicationPolicy(w, req)

		if w.Code != http.StatusInternalServerError {
			t.Errorf("expected status 500, got %d", w.Code)
		}
	})

	t.Run("GetPolicy returns general error", func(t *testing.T) {
		storage := NewMockStorageWithReplication()
		storage.repMgr.getError = errors.New("database error")
		handler := setupReplicationTestHandler(t, storage)

		req := httptest.NewRequest(http.MethodGet, "/replication/policies/test-id", nil)
		w := httptest.NewRecorder()

		handler.handleGetReplicationPolicy(w, req)

		if w.Code != http.StatusInternalServerError {
			t.Errorf("expected status 500, got %d", w.Code)
		}
	})

	t.Run("policy with encryption", func(t *testing.T) {
		storage := NewMockStorageWithReplication()
		handler := setupReplicationTestHandler(t, storage)

		storage.repMgr.AddPolicy(common.ReplicationPolicy{
			ID:                 "test-encryption",
			SourceBackend:      "local",
			DestinationBackend: "s3",
			CheckInterval:      5 * time.Minute,
			Enabled:            true,
			ReplicationMode:    common.ReplicationModeTransparent,
			Encryption: &common.EncryptionPolicy{
				Backend: &common.EncryptionConfig{
					Enabled:  true,
					Provider: "custom",
				},
			},
		})

		req := httptest.NewRequest(http.MethodGet, "/replication/policies/test-encryption", nil)
		w := httptest.NewRecorder()

		handler.handleGetReplicationPolicy(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}

		var response map[string]any
		if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		if response["encryption"] == nil {
			t.Error("expected encryption to be present in response")
		}
	})
}

// TestHandleDeleteReplicationPolicy_ErrorPaths tests error scenarios for deleting replication policies
func TestHandleDeleteReplicationPolicy_ErrorPaths(t *testing.T) {
	t.Run("empty policy ID", func(t *testing.T) {
		storage := NewMockStorageWithReplication()
		handler := setupReplicationTestHandler(t, storage)

		req := httptest.NewRequest(http.MethodDelete, "/replication/policies/", nil)
		w := httptest.NewRecorder()

		handler.handleDeleteReplicationPolicy(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("expected status 400, got %d", w.Code)
		}
	})

	t.Run("GetReplicationManager returns ErrReplicationNotSupported", func(t *testing.T) {
		storage := NewMockStorageWithReplication()
		storage.repErr = common.ErrReplicationNotSupported
		handler := setupReplicationTestHandler(t, storage)

		req := httptest.NewRequest(http.MethodDelete, "/replication/policies/test-id", nil)
		w := httptest.NewRecorder()

		handler.handleDeleteReplicationPolicy(w, req)

		if w.Code != http.StatusInternalServerError {
			t.Errorf("expected status 500, got %d", w.Code)
		}
	})

	t.Run("GetReplicationManager returns other error", func(t *testing.T) {
		storage := NewMockStorageWithReplication()
		storage.repErr = errors.New("some other error")
		handler := setupReplicationTestHandler(t, storage)

		req := httptest.NewRequest(http.MethodDelete, "/replication/policies/test-id", nil)
		w := httptest.NewRecorder()

		handler.handleDeleteReplicationPolicy(w, req)

		if w.Code != http.StatusInternalServerError {
			t.Errorf("expected status 500, got %d", w.Code)
		}
	})

	t.Run("RemovePolicy returns general error", func(t *testing.T) {
		storage := NewMockStorageWithReplication()
		storage.repMgr.removeError = errors.New("database error")
		handler := setupReplicationTestHandler(t, storage)

		// Add policy first
		storage.repMgr.AddPolicy(common.ReplicationPolicy{
			ID:                 "test-id",
			SourceBackend:      "local",
			DestinationBackend: "s3",
			CheckInterval:      5 * time.Minute,
		})

		req := httptest.NewRequest(http.MethodDelete, "/replication/policies/test-id", nil)
		w := httptest.NewRecorder()

		handler.handleDeleteReplicationPolicy(w, req)

		if w.Code != http.StatusInternalServerError {
			t.Errorf("expected status 500, got %d", w.Code)
		}
	})
}

// TestHandleTriggerReplication_ErrorPaths tests error scenarios for triggering replication
func TestHandleTriggerReplication_ErrorPaths(t *testing.T) {
	t.Run("GetReplicationManager returns ErrReplicationNotSupported", func(t *testing.T) {
		storage := NewMockStorageWithReplication()
		storage.repErr = common.ErrReplicationNotSupported
		handler := setupReplicationTestHandler(t, storage)

		req := httptest.NewRequest(http.MethodPost, "/replication/trigger", nil)
		w := httptest.NewRecorder()

		handler.handleTriggerReplication(w, req)

		if w.Code != http.StatusInternalServerError {
			t.Errorf("expected status 500, got %d", w.Code)
		}
	})

	t.Run("GetReplicationManager returns other error", func(t *testing.T) {
		storage := NewMockStorageWithReplication()
		storage.repErr = errors.New("some other error")
		handler := setupReplicationTestHandler(t, storage)

		req := httptest.NewRequest(http.MethodPost, "/replication/trigger", nil)
		w := httptest.NewRecorder()

		handler.handleTriggerReplication(w, req)

		if w.Code != http.StatusInternalServerError {
			t.Errorf("expected status 500, got %d", w.Code)
		}
	})

	t.Run("sync all with error", func(t *testing.T) {
		storage := NewMockStorageWithReplication()
		storage.repMgr.syncError = errors.New("sync failed")
		handler := setupReplicationTestHandler(t, storage)

		req := httptest.NewRequest(http.MethodPost, "/replication/trigger", nil)
		w := httptest.NewRecorder()

		handler.handleTriggerReplication(w, req)

		if w.Code != http.StatusInternalServerError {
			t.Errorf("expected status 500, got %d", w.Code)
		}
	})

	t.Run("sync with result including errors", func(t *testing.T) {
		// Create a custom mock replication manager for this test
		storage := NewMockStorageWithReplication()
		handler := setupReplicationTestHandler(t, storage)

		// Create a wrapper to override SyncAll
		type customMockReplicationManager struct {
			*MockReplicationManager
		}

		customMgr := &customMockReplicationManager{
			MockReplicationManager: storage.repMgr,
		}

		customMgr.MockReplicationManager.policies = map[string]common.ReplicationPolicy{
			"test": {
				ID:                 "test",
				SourceBackend:      "local",
				DestinationBackend: "s3",
				CheckInterval:      5 * time.Minute,
			},
		}

		req := httptest.NewRequest(http.MethodPost, "/replication/trigger", nil)
		w := httptest.NewRecorder()

		handler.handleTriggerReplication(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}

		var response map[string]any
		if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		// Just verify we got a successful response with result
		if !response["success"].(bool) {
			t.Error("expected success to be true")
		}
	})
}

// TestHandleReplicationPolicies_MethodRouting tests method routing
func TestHandleReplicationPolicies_MethodRouting(t *testing.T) {
	storage := NewMockStorageWithReplication()
	handler := setupReplicationTestHandler(t, storage)

	t.Run("PUT method not allowed", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPut, "/replication/policies", nil)
		w := httptest.NewRecorder()

		handler.handleReplicationPolicies(w, req)

		if w.Code != http.StatusMethodNotAllowed {
			t.Errorf("expected status 405, got %d", w.Code)
		}
	})

	t.Run("PATCH method not allowed", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPatch, "/replication/policies", nil)
		w := httptest.NewRecorder()

		handler.handleReplicationPolicies(w, req)

		if w.Code != http.StatusMethodNotAllowed {
			t.Errorf("expected status 405, got %d", w.Code)
		}
	})
}
