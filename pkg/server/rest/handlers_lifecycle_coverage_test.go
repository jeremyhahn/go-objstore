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

package rest

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

// TestAddPolicy_MissingAction tests missing action error path
func TestAddPolicy_MissingAction(t *testing.T) {
	storage := newMockLifecycleStorage()
	handler := NewHandler(storage)

	router := gin.New()
	router.POST("/policies", handler.AddPolicy)

	requestBody := map[string]any{
		"id":                "policy1",
		"prefix":            "logs/",
		"retention_seconds": 86400,
		// Missing "action" field
	}

	body, _ := json.Marshal(requestBody)
	req := httptest.NewRequest("POST", "/policies", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status %d, got %d: %s", http.StatusBadRequest, w.Code, w.Body.String())
	}
}

// TestAddPolicy_InvalidRetention tests invalid retention_seconds error path
func TestAddPolicy_InvalidRetention(t *testing.T) {
	storage := newMockLifecycleStorage()
	handler := NewHandler(storage)

	router := gin.New()
	router.POST("/policies", handler.AddPolicy)

	requestBody := map[string]any{
		"id":                "policy1",
		"prefix":            "logs/",
		"retention_seconds": -100, // Invalid negative retention
		"action":            "delete",
	}

	body, _ := json.Marshal(requestBody)
	req := httptest.NewRequest("POST", "/policies", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status %d, got %d: %s", http.StatusBadRequest, w.Code, w.Body.String())
	}
}

// TestAddPolicy_ArchiveMissingDestinationType tests archive without destination_type
func TestAddPolicy_ArchiveMissingDestinationType(t *testing.T) {
	storage := newMockLifecycleStorage()
	handler := NewHandler(storage)

	router := gin.New()
	router.POST("/policies", handler.AddPolicy)

	requestBody := AddPolicyRequest{
		ID:               "policy1",
		Prefix:           "logs/",
		Retention: 24 * time.Hour,
		Action:           "archive",
		// Missing DestinationType
	}

	body, _ := json.Marshal(requestBody)
	req := httptest.NewRequest("POST", "/policies", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status %d, got %d: %s", http.StatusBadRequest, w.Code, w.Body.String())
	}
}

// TestAddPolicy_ArchiveInvalidDestinationType tests archive with invalid destination type
func TestAddPolicy_ArchiveInvalidDestinationType(t *testing.T) {
	storage := newMockLifecycleStorage()
	handler := NewHandler(storage)

	router := gin.New()
	router.POST("/policies", handler.AddPolicy)

	requestBody := AddPolicyRequest{
		ID:                  "policy1",
		Prefix:              "logs/",
		Retention:    24 * time.Hour,
		Action:              "archive",
		DestinationType:     "invalid-type",
		DestinationSettings: map[string]string{"path": "/tmp/test"},
	}

	body, _ := json.Marshal(requestBody)
	req := httptest.NewRequest("POST", "/policies", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status %d, got %d: %s", http.StatusBadRequest, w.Code, w.Body.String())
	}
}

// TestArchive_MissingDestinationType tests missing destination_type error path
func TestArchive_MissingDestinationType(t *testing.T) {
	storage := newMockLifecycleStorage()
	handler := NewHandler(storage)

	router := gin.New()
	router.POST("/archive", handler.Archive)

	requestBody := map[string]any{
		"key": "test-key",
		// Missing destination_type
	}

	body, _ := json.Marshal(requestBody)
	req := httptest.NewRequest("POST", "/archive", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status %d, got %d: %s", http.StatusBadRequest, w.Code, w.Body.String())
	}
}

// TestArchive_InvalidJSON tests invalid JSON error path
func TestArchive_InvalidJSON(t *testing.T) {
	storage := newMockLifecycleStorage()
	handler := NewHandler(storage)

	router := gin.New()
	router.POST("/archive", handler.Archive)

	req := httptest.NewRequest("POST", "/archive", bytes.NewReader([]byte("invalid json")))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status %d, got %d: %s", http.StatusBadRequest, w.Code, w.Body.String())
	}
}

// TestArchive_InvalidKey tests invalid key error path
func TestArchive_InvalidKey(t *testing.T) {
	storage := newMockLifecycleStorage()
	handler := NewHandler(storage)

	router := gin.New()
	router.POST("/archive", handler.Archive)

	requestBody := ArchiveRequest{
		Key:                 "../../../etc/passwd", // Invalid key with path traversal
		DestinationType:     "local",
		DestinationSettings: map[string]string{"path": "/tmp/test"},
	}

	body, _ := json.Marshal(requestBody)
	req := httptest.NewRequest("POST", "/archive", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status %d, got %d: %s", http.StatusBadRequest, w.Code, w.Body.String())
	}
}

// TestArchive_InvalidDestinationType tests archive with invalid archiver type
func TestArchive_InvalidDestinationType(t *testing.T) {
	storage := newMockLifecycleStorage()
	handler := NewHandler(storage)

	router := gin.New()
	router.POST("/archive", handler.Archive)

	requestBody := ArchiveRequest{
		Key:                 "test-key",
		DestinationType:     "invalid-archiver-type",
		DestinationSettings: map[string]string{"path": "/tmp/test"},
	}

	body, _ := json.Marshal(requestBody)
	req := httptest.NewRequest("POST", "/archive", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status %d, got %d: %s", http.StatusBadRequest, w.Code, w.Body.String())
	}
}
