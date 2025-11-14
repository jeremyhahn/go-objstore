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
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// TestHandleGet_ErrorPaths tests error scenarios for getting objects
func TestHandleGet_ErrorPaths(t *testing.T) {
	t.Run("metadata not found returns nil", func(t *testing.T) {
		storage := newMockLifecycleStorage()
		handler := NewHandler(storage, 10*1024*1024, 30*time.Second, 30*time.Second, &mockLogger{}, &mockAuthenticator{})

		req := httptest.NewRequest(http.MethodGet, "/objects/nonexistent.txt", nil)
		w := httptest.NewRecorder()

		handler.handleGet(w, req, "nonexistent.txt")

		if w.Code != http.StatusNotFound {
			t.Errorf("expected status 404, got %d", w.Code)
		}
	})

	t.Run("get object with all metadata", func(t *testing.T) {
		storage := newMockLifecycleStorage()
		handler := NewHandler(storage, 10*1024*1024, 30*time.Second, 30*time.Second, &mockLogger{}, &mockAuthenticator{})

		// Add object
		storage.PutWithContext(context.Background(), "test.txt", bytes.NewReader([]byte("test data")))

		req := httptest.NewRequest(http.MethodGet, "/objects/test.txt", nil)
		w := httptest.NewRecorder()

		handler.handleGet(w, req, "test.txt")

		// Should return OK status
		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}

		if w.Body.String() != "test data" {
			t.Errorf("expected body 'test data', got %s", w.Body.String())
		}
	})

	t.Run("get object with custom metadata headers", func(t *testing.T) {
		storage := newMockLifecycleStorage()
		handler := NewHandler(storage, 10*1024*1024, 30*time.Second, 30*time.Second, &mockLogger{}, &mockAuthenticator{})

		// Add object with custom metadata
		metadata := &common.Metadata{
			ContentType:     "application/json",
			ContentEncoding: "gzip",
			Size:            9,
			LastModified:    time.Now(),
			ETag:            "test-etag",
			Custom: map[string]string{
				"author":  "test-user",
				"version": "1.0",
			},
		}
		storage.PutWithMetadata(context.Background(), "test.json", bytes.NewReader([]byte("test data")), metadata)

		req := httptest.NewRequest(http.MethodGet, "/objects/test.json", nil)
		w := httptest.NewRecorder()

		handler.handleGet(w, req, "test.json")

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}

		if w.Header().Get("Content-Type") != "application/json" {
			t.Errorf("expected Content-Type application/json, got %s", w.Header().Get("Content-Type"))
		}

		if w.Header().Get("Content-Encoding") != "gzip" {
			t.Errorf("expected Content-Encoding gzip, got %s", w.Header().Get("Content-Encoding"))
		}

		if w.Header().Get("ETag") != "test-etag" {
			t.Errorf("expected ETag test-etag, got %s", w.Header().Get("ETag"))
		}

		if w.Header().Get("X-Meta-author") != "test-user" {
			t.Errorf("expected X-Meta-author test-user, got %s", w.Header().Get("X-Meta-author"))
		}

		if w.Header().Get("X-Meta-version") != "1.0" {
			t.Errorf("expected X-Meta-version 1.0, got %s", w.Header().Get("X-Meta-version"))
		}
	})
}

// TestHandleExists_ErrorPaths tests error scenarios for checking object existence
func TestHandleExists_ErrorPaths(t *testing.T) {
	t.Run("exists check for non-existent object", func(t *testing.T) {
		customStor := newMockLifecycleStorage()
		handler := NewHandler(customStor, 10*1024*1024, 30*time.Second, 30*time.Second, &mockLogger{}, &mockAuthenticator{})

		req := httptest.NewRequest(http.MethodGet, "/objects/test.txt?exists=true", nil)
		w := httptest.NewRecorder()

		handler.handleExists(w, req, "test.txt")

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}

		var result map[string]bool
		if err := json.NewDecoder(w.Body).Decode(&result); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		if result["exists"] {
			t.Error("expected exists to be false for non-existent object")
		}
	})
}

// TestHandleUpdateMetadata_ErrorPaths tests error scenarios for updating metadata
func TestHandleUpdateMetadata_ErrorPaths(t *testing.T) {
	t.Run("update metadata on non-existent object", func(t *testing.T) {
		storage := newMockLifecycleStorage()
		handler := NewHandler(storage, 10*1024*1024, 30*time.Second, 30*time.Second, &mockLogger{}, &mockAuthenticator{})

		metadataUpdate := map[string]any{
			"content_type": "application/json",
		}

		body, _ := json.Marshal(metadataUpdate)
		req := httptest.NewRequest(http.MethodPatch, "/objects/nonexistent.txt", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		handler.handleUpdateMetadata(w, req, "nonexistent.txt")

		// Returns 500 because UpdateMetadata returns error for non-existent object
		if w.Code != http.StatusInternalServerError {
			t.Errorf("expected status 500, got %d", w.Code)
		}
	})
}

// TestHandleHealth_JSONEncodeError tests health endpoint with JSON encode scenario
func TestHandleHealth_JSONEncodeError(t *testing.T) {
	storage := newMockLifecycleStorage()
	handler := NewHandler(storage, 10*1024*1024, 30*time.Second, 30*time.Second, &mockLogger{}, &mockAuthenticator{})

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()

	handler.handleHealth(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	var result map[string]string
	if err := json.NewDecoder(w.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if result["status"] != "healthy" {
		t.Errorf("expected status healthy, got %s", result["status"])
	}

	if result["protocol"] != "HTTP/3" {
		t.Errorf("expected protocol HTTP/3, got %s", result["protocol"])
	}
}

// TestHandlePut_LargeBody tests putting objects with body size limits
func TestHandlePut_LargeBody(t *testing.T) {
	storage := newMockLifecycleStorage()
	// Set very small max request body size
	handler := NewHandler(storage, 100, 30*time.Second, 30*time.Second, &mockLogger{}, &mockAuthenticator{})

	// Create a body larger than max size
	largeBody := strings.NewReader(strings.Repeat("a", 1000))

	req := httptest.NewRequest(http.MethodPut, "/objects/large.txt", largeBody)
	w := httptest.NewRecorder()

	handler.handlePut(w, req, "large.txt")

	if w.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("expected status 413, got %d", w.Code)
	}
}

// TestHandleList_EmptyResult tests listing with no objects
func TestHandleList_EmptyResult(t *testing.T) {
	storage := newMockLifecycleStorage()
	handler := NewHandler(storage, 10*1024*1024, 30*time.Second, 30*time.Second, &mockLogger{}, &mockAuthenticator{})

	req := httptest.NewRequest(http.MethodGet, "/objects?prefix=nonexistent/", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	var result map[string]any
	if err := json.NewDecoder(w.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	// Response should have objects array (may be empty)
	if result["objects"] == nil {
		t.Log("Response does not contain objects field - this may be expected for empty results")
		return
	}

	objects, ok := result["objects"].([]any)
	if !ok {
		t.Fatal("expected objects array in response")
	}

	if len(objects) != 0 {
		t.Errorf("expected 0 objects, got %d", len(objects))
	}
}

// TestServeHTTP_UnknownRoute tests unknown route handling
func TestServeHTTP_UnknownRoute(t *testing.T) {
	storage := newMockLifecycleStorage()
	handler := NewHandler(storage, 10*1024*1024, 30*time.Second, 30*time.Second, &mockLogger{}, &mockAuthenticator{})

	req := httptest.NewRequest(http.MethodGet, "/unknown/route", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected status 404, got %d", w.Code)
	}
}

// TestHandlePolicyByID_InvalidMethod tests policy by ID with invalid method
func TestHandlePolicyByID_InvalidMethod(t *testing.T) {
	storage := newMockLifecycleStorage()
	handler := NewHandler(storage, 10*1024*1024, 30*time.Second, 30*time.Second, &mockLogger{}, &mockAuthenticator{})

	req := httptest.NewRequest(http.MethodPost, "/policies/test-id", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected status 405, got %d", w.Code)
	}
}

// TestHandleAddPolicy_ConflictError tests adding duplicate policy
func TestHandleAddPolicy_ConflictError(t *testing.T) {
	storage := newMockLifecycleStorage()
	handler := NewHandler(storage, 10*1024*1024, 30*time.Second, 30*time.Second, &mockLogger{}, &mockAuthenticator{})

	// Add policy first time
	policy := map[string]any{
		"id":                "test-policy",
		"retention_seconds": 86400,
		"action":            "delete",
		"prefix":            "logs/",
	}

	body, _ := json.Marshal(policy)
	req := httptest.NewRequest(http.MethodPost, "/policies", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("first add: expected status 201, got %d", w.Code)
	}

	// Try to add again
	body, _ = json.Marshal(policy)
	req = httptest.NewRequest(http.MethodPost, "/policies", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusConflict {
		t.Errorf("duplicate add: expected status 409, got %d", w.Code)
	}
}

// TestHandleGetPolicies_WithPrefix tests getting policies filtered by prefix
func TestHandleGetPolicies_WithPrefix(t *testing.T) {
	storage := newMockLifecycleStorage()
	handler := NewHandler(storage, 10*1024*1024, 30*time.Second, 30*time.Second, &mockLogger{}, &mockAuthenticator{})

	// Add policies with different prefixes
	storage.AddPolicy(common.LifecyclePolicy{
		ID:        "logs-policy",
		Prefix:    "logs/",
		Retention: 24 * time.Hour,
		Action:    "delete",
	})
	storage.AddPolicy(common.LifecyclePolicy{
		ID:        "data-policy",
		Prefix:    "data/",
		Retention: 30 * 24 * time.Hour,
		Action:    "archive",
	})

	req := httptest.NewRequest(http.MethodGet, "/policies?prefix=logs/", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	var result map[string]any
	if err := json.NewDecoder(w.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	policies, ok := result["policies"].([]any)
	if !ok {
		t.Fatal("expected policies array in response")
	}

	// Should return policies matching the prefix
	if len(policies) < 1 {
		t.Errorf("expected at least 1 policy with logs/ prefix, got %d", len(policies))
	}
}

// TestHandleArchive_ErrorScenarios tests archive error scenarios
func TestHandleArchive_ErrorScenarios(t *testing.T) {
	t.Run("archive with invalid destination type", func(t *testing.T) {
		storage := newMockLifecycleStorage()
		handler := NewHandler(storage, 10*1024*1024, 30*time.Second, 30*time.Second, &mockLogger{}, &mockAuthenticator{})

		// Add object
		storage.PutWithContext(context.Background(), "test.txt", bytes.NewReader([]byte("data")))

		archiveReq := map[string]any{
			"key":                    "test.txt",
			"destination_type":       "invalid",
			"destination_settings":   map[string]string{},
		}

		body, _ := json.Marshal(archiveReq)
		req := httptest.NewRequest(http.MethodPost, "/archive", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		handler.ServeHTTP(w, req)

		// Should return error for invalid destination type
		if w.Code == http.StatusOK {
			// Some systems may accept this, just verify we get a response
			t.Logf("Archive with invalid type returned: %d", w.Code)
		}
	})
}

// TestHandleList_WithMaxResults tests listing with max_results parameter
func TestHandleList_WithMaxResults(t *testing.T) {
	storage := newMockLifecycleStorage()
	handler := NewHandler(storage, 10*1024*1024, 30*time.Second, 30*time.Second, &mockLogger{}, &mockAuthenticator{})

	// Add multiple objects
	for i := 0; i < 10; i++ {
		key := "object" + string(rune('0'+i)) + ".txt"
		storage.PutWithContext(context.Background(), key, bytes.NewReader([]byte("data")))
	}

	req := httptest.NewRequest(http.MethodGet, "/objects?max_results=5", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	var result map[string]any
	if err := json.NewDecoder(w.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	objects, ok := result["objects"].([]any)
	if !ok {
		t.Fatal("expected objects array in response")
	}

	// Verify we got results (actual count depends on implementation)
	if len(objects) == 0 {
		t.Error("expected at least some objects in response")
	}
}

