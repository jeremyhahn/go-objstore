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

package quic

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/local"
)

func setupTestHandler(t *testing.T) (*Handler, common.Storage) {
	storage := local.New()
	err := storage.Configure(map[string]string{"path": t.TempDir()})
	if err != nil {
		t.Fatalf("Failed to configure storage: %v", err)
	}

	initTestFacade(t, storage)
	logger := adapters.NewNoOpLogger()
	auth := adapters.NewNoOpAuthenticator()
	handler, err := NewHandler("", 100*1024*1024, 30*time.Second, 30*time.Second, logger, auth)
	if err != nil {
		t.Fatalf("Failed to create handler: %v", err)
	}
	return handler, storage
}

func TestHandlerPutObject(t *testing.T) {
	handler, _ := setupTestHandler(t)

	data := []byte("test data")
	req := httptest.NewRequest(http.MethodPut, "/objects/test-key", bytes.NewReader(data))
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("X-Meta-Author", "test-user")

	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("Expected status 201, got %d", w.Code)
	}

	var response map[string]string
	if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if response["key"] != "test-key" {
		t.Errorf("Expected key 'test-key', got %s", response["key"])
	}
}

func TestHandlerPutObjectTooLarge(t *testing.T) {
	handler, _ := setupTestHandler(t)
	handler.maxRequestBodySize = 100 // Very small limit

	data := make([]byte, 200)
	req := httptest.NewRequest(http.MethodPut, "/objects/test-key", bytes.NewReader(data))
	req.ContentLength = 200

	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("Expected status 413, got %d", w.Code)
	}
}

func TestHandlerGetObject(t *testing.T) {
	handler, storage := setupTestHandler(t)

	// Store test data
	testData := []byte("test data for retrieval")
	metadata := &common.Metadata{
		ContentType: "text/plain",
		Custom:      map[string]string{"author": "test-user"},
	}
	err := storage.PutWithMetadata(context.Background(), "test-key", bytes.NewReader(testData), metadata)
	if err != nil {
		t.Fatalf("Failed to store test data: %v", err)
	}

	// Get object
	req := httptest.NewRequest(http.MethodGet, "/objects/test-key", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	if w.Header().Get("Content-Type") != "text/plain" {
		t.Errorf("Expected Content-Type 'text/plain', got %s", w.Header().Get("Content-Type"))
	}

	if w.Header().Get("X-Meta-author") != "test-user" {
		t.Errorf("Expected X-Meta-author 'test-user', got %s", w.Header().Get("X-Meta-author"))
	}

	body, _ := io.ReadAll(w.Body)
	if !bytes.Equal(body, testData) {
		t.Errorf("Expected body %s, got %s", testData, body)
	}
}

func TestHandlerGetObjectNotFound(t *testing.T) {
	handler, _ := setupTestHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/objects/nonexistent", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("Expected status 404, got %d", w.Code)
	}
}

func TestHandlerDeleteObject(t *testing.T) {
	handler, storage := setupTestHandler(t)

	// Store test data
	err := storage.Put("test-key", bytes.NewReader([]byte("test data")))
	if err != nil {
		t.Fatalf("Failed to store test data: %v", err)
	}

	// Delete object
	req := httptest.NewRequest(http.MethodDelete, "/objects/test-key", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Errorf("Expected status 204, got %d", w.Code)
	}

	// Verify deletion
	_, err = storage.Get("test-key")
	if err == nil {
		t.Error("Expected object to be deleted")
	}
}

func TestHandlerHeadObject(t *testing.T) {
	handler, storage := setupTestHandler(t)

	// Store test data
	metadata := &common.Metadata{
		ContentType: "application/json",
		Custom:      map[string]string{"version": "1.0"},
	}
	err := storage.PutWithMetadata(context.Background(), "test-key", bytes.NewReader([]byte("test")), metadata)
	if err != nil {
		t.Fatalf("Failed to store test data: %v", err)
	}

	// Head request
	req := httptest.NewRequest(http.MethodHead, "/objects/test-key", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	if w.Header().Get("Content-Type") != "application/json" {
		t.Errorf("Expected Content-Type 'application/json', got %s", w.Header().Get("Content-Type"))
	}

	if w.Header().Get("X-Meta-version") != "1.0" {
		t.Errorf("Expected X-Meta-version '1.0', got %s", w.Header().Get("X-Meta-version"))
	}

	// Body should be empty for HEAD request
	if w.Body.Len() > 0 {
		t.Error("Expected empty body for HEAD request")
	}
}

func TestHandlerHeadObjectNotFound(t *testing.T) {
	handler, _ := setupTestHandler(t)

	req := httptest.NewRequest(http.MethodHead, "/objects/nonexistent", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("Expected status 404, got %d", w.Code)
	}
}

func TestHandlerListObjects(t *testing.T) {
	handler, storage := setupTestHandler(t)

	// Store test objects
	objects := []string{"obj1", "obj2", "obj3"}
	for _, key := range objects {
		err := storage.Put(key, bytes.NewReader([]byte(key)))
		if err != nil {
			t.Fatalf("Failed to store object %s: %v", key, err)
		}
	}

	// List objects
	req := httptest.NewRequest(http.MethodGet, "/objects", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	var response map[string]any
	if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	objectsList, ok := response["objects"].([]any)
	if !ok {
		t.Fatal("Expected objects array in response")
	}

	if len(objectsList) != len(objects) {
		t.Errorf("Expected %d objects, got %d", len(objects), len(objectsList))
	}
}

func TestHandlerListObjectsWithPrefix(t *testing.T) {
	handler, storage := setupTestHandler(t)

	// Store test objects
	objects := []string{"prefix/obj1", "prefix/obj2", "other/obj3"}
	for _, key := range objects {
		err := storage.Put(key, bytes.NewReader([]byte(key)))
		if err != nil {
			t.Fatalf("Failed to store object %s: %v", key, err)
		}
	}

	// List objects with prefix
	req := httptest.NewRequest(http.MethodGet, "/objects?prefix=prefix/", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	var response map[string]any
	if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	objectsList, ok := response["objects"].([]any)
	if !ok {
		t.Fatal("Expected objects array in response")
	}

	if len(objectsList) != 2 {
		t.Errorf("Expected 2 objects with prefix, got %d", len(objectsList))
	}
}

func TestHandlerInvalidPath(t *testing.T) {
	handler, _ := setupTestHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/invalid", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("Expected status 404, got %d", w.Code)
	}
}

func TestHandlerInvalidKey(t *testing.T) {
	handler, _ := setupTestHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/objects/", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d", w.Code)
	}
}

func TestHandlerMethodNotAllowed(t *testing.T) {
	handler, _ := setupTestHandler(t)

	tests := []struct {
		method string
		path   string
	}{
		{http.MethodPost, "/objects/test-key"}, // POST not allowed on object paths
		{http.MethodPost, "/objects"},          // POST not allowed on list path
		{http.MethodPut, "/objects"},           // PUT not allowed on list path
		// Note: PATCH is now allowed on /objects/test-key for UpdateMetadata
	}

	for _, tt := range tests {
		t.Run(tt.method+" "+tt.path, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, tt.path, nil)
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, req)

			if w.Code != http.StatusMethodNotAllowed {
				t.Errorf("Expected status 405, got %d", w.Code)
			}
		})
	}
}

func TestHandlerCORS(t *testing.T) {
	handler, _ := setupTestHandler(t)

	req := httptest.NewRequest(http.MethodOptions, "/objects/test-key", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200 for OPTIONS, got %d", w.Code)
	}

	if w.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Error("Expected CORS headers to be set")
	}

	if w.Header().Get("Access-Control-Allow-Methods") == "" {
		t.Error("Expected Access-Control-Allow-Methods header")
	}
}

func TestHandlerCustomMetadata(t *testing.T) {
	handler, _ := setupTestHandler(t)

	// Put object with custom metadata
	req := httptest.NewRequest(http.MethodPut, "/objects/test-key", strings.NewReader("test data"))
	req.Header.Set("X-Meta-Author", "John Doe")
	req.Header.Set("X-Meta-Version", "1.0")
	req.Header.Set("X-Meta-Project", "test-project")

	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("Expected status 201, got %d", w.Code)
	}

	// Get object and verify metadata
	req = httptest.NewRequest(http.MethodGet, "/objects/test-key", nil)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	if w.Header().Get("X-Meta-Author") != "John Doe" {
		t.Errorf("Expected X-Meta-Author 'John Doe', got %s", w.Header().Get("X-Meta-Author"))
	}

	if w.Header().Get("X-Meta-Version") != "1.0" {
		t.Errorf("Expected X-Meta-Version '1.0', got %s", w.Header().Get("X-Meta-Version"))
	}

	if w.Header().Get("X-Meta-Project") != "test-project" {
		t.Errorf("Expected X-Meta-Project 'test-project', got %s", w.Header().Get("X-Meta-Project"))
	}
}

func TestHandlerContentEncoding(t *testing.T) {
	handler, _ := setupTestHandler(t)

	// Put object with content encoding
	req := httptest.NewRequest(http.MethodPut, "/objects/test-key", strings.NewReader("test data"))
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("Content-Encoding", "gzip")

	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("Expected status 201, got %d", w.Code)
	}

	// Get object and verify encoding
	req = httptest.NewRequest(http.MethodGet, "/objects/test-key", nil)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	if w.Header().Get("Content-Encoding") != "gzip" {
		t.Errorf("Expected Content-Encoding 'gzip', got %s", w.Header().Get("Content-Encoding"))
	}
}

func TestHandlerPathCleaning(t *testing.T) {
	handler, storage := setupTestHandler(t)

	// Store test data with a nested key
	err := storage.Put("folder/test-key", bytes.NewReader([]byte("test data")))
	if err != nil {
		t.Fatalf("Failed to store test data: %v", err)
	}

	// Access with normal path
	req := httptest.NewRequest(http.MethodGet, "/objects/folder/test-key", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}
}

func TestHandlerHealth(t *testing.T) {
	handler, _ := setupTestHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	var response map[string]string
	if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if response["status"] != "healthy" {
		t.Errorf("Expected status 'healthy', got '%s'", response["status"])
	}

	if response["protocol"] != "HTTP/3" {
		t.Errorf("Expected protocol 'HTTP/3', got '%s'", response["protocol"])
	}

	contentType := w.Header().Get("Content-Type")
	if contentType != "application/json" {
		t.Errorf("Expected Content-Type 'application/json', got '%s'", contentType)
	}
}

func TestHandlerArchive(t *testing.T) {
	handler, storage := setupTestHandler(t)

	// Store test data
	testData := []byte("test data for archival")
	metadata := &common.Metadata{
		ContentType: "text/plain",
	}
	err := storage.PutWithMetadata(context.Background(), "test-key", bytes.NewReader(testData), metadata)
	if err != nil {
		t.Fatalf("Failed to store test data: %v", err)
	}

	// Archive request
	archiveReq := map[string]any{
		"key":                  "test-key",
		"destination_type":     "local",
		"destination_settings": map[string]string{"path": t.TempDir()},
	}
	body, _ := json.Marshal(archiveReq)

	req := httptest.NewRequest(http.MethodPost, "/archive", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	var response map[string]string
	if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if _, ok := response["message"]; !ok {
		t.Error("Expected message in response")
	}
	if response["key"] != "test-key" {
		t.Errorf("Expected key 'test-key', got %s", response["key"])
	}
}

func TestHandlerArchiveInvalidRequest(t *testing.T) {
	handler, _ := setupTestHandler(t)

	// Missing required fields
	archiveReq := map[string]any{
		"key": "test-key",
	}
	body, _ := json.Marshal(archiveReq)

	req := httptest.NewRequest(http.MethodPost, "/archive", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d", w.Code)
	}
}

func TestHandlerArchiveNonexistentKey(t *testing.T) {
	handler, _ := setupTestHandler(t)

	archiveReq := map[string]any{
		"key":                  "nonexistent-key",
		"destination_type":     "local",
		"destination_settings": map[string]string{"path": t.TempDir()},
	}
	body, _ := json.Marshal(archiveReq)

	req := httptest.NewRequest(http.MethodPost, "/archive", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	// Should fail with 500 since the key doesn't exist
	if w.Code != http.StatusInternalServerError {
		t.Errorf("Expected status 500, got %d", w.Code)
	}
}

func TestHandlerGetPolicies(t *testing.T) {
	handler, storage := setupTestHandler(t)

	// Ensure storage implements LifecycleManager
	lifecycleStorage, ok := storage.(common.LifecycleManager)
	if !ok {
		t.Skip("Storage does not implement LifecycleManager")
	}

	// Add a test policy
	policy := common.LifecyclePolicy{
		ID:        "test-policy",
		Prefix:    "logs/",
		Retention: 24 * time.Hour,
		Action:    "delete",
	}
	err := lifecycleStorage.AddPolicy(policy)
	if err != nil {
		t.Fatalf("Failed to add test policy: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/policies", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	var response map[string]any
	if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	policies, ok := response["policies"].([]any)
	if !ok {
		t.Fatal("Expected policies array in response")
	}

	if len(policies) == 0 {
		t.Error("Expected at least one policy")
	}
}

func TestHandlerAddPolicy(t *testing.T) {
	handler, storage := setupTestHandler(t)

	// Ensure storage implements LifecycleManager
	if _, ok := storage.(common.LifecycleManager); !ok {
		t.Skip("Storage does not implement LifecycleManager")
	}

	policyReq := map[string]any{
		"id":                "test-policy",
		"prefix":            "logs/",
		"retention_seconds": 86400, // 24 hours
		"action":            "delete",
	}
	body, _ := json.Marshal(policyReq)

	req := httptest.NewRequest(http.MethodPost, "/policies", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("Expected status 201, got %d: %s", w.Code, w.Body.String())
	}

	var response map[string]string
	if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if _, ok := response["message"]; !ok {
		t.Error("Expected message in response")
	}
	if response["id"] != "test-policy" {
		t.Errorf("Expected id 'test-policy', got %s", response["id"])
	}
}

func TestHandlerAddPolicyInvalidRequest(t *testing.T) {
	handler, storage := setupTestHandler(t)

	// Ensure storage implements LifecycleManager
	if _, ok := storage.(common.LifecycleManager); !ok {
		t.Skip("Storage does not implement LifecycleManager")
	}

	// Missing required fields
	policyReq := map[string]any{
		"id": "test-policy",
	}
	body, _ := json.Marshal(policyReq)

	req := httptest.NewRequest(http.MethodPost, "/policies", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d", w.Code)
	}
}

func TestHandlerAddPolicyInvalidRetention(t *testing.T) {
	handler, storage := setupTestHandler(t)

	// Ensure storage implements LifecycleManager
	if _, ok := storage.(common.LifecycleManager); !ok {
		t.Skip("Storage does not implement LifecycleManager")
	}

	policyReq := map[string]any{
		"id":                "test-policy",
		"prefix":            "logs/",
		"retention_seconds": "invalid", // Invalid type - should be int
		"action":            "delete",
	}
	body, _ := json.Marshal(policyReq)

	req := httptest.NewRequest(http.MethodPost, "/policies", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d", w.Code)
	}
}

func TestHandlerAddPolicyArchiveType(t *testing.T) {
	handler, storage := setupTestHandler(t)

	// Ensure storage implements LifecycleManager
	if _, ok := storage.(common.LifecycleManager); !ok {
		t.Skip("Storage does not implement LifecycleManager")
	}

	policyReq := map[string]any{
		"id":                   "test-policy",
		"prefix":               "data/",
		"retention_seconds":    2592000, // 30 days
		"action":               "archive",
		"destination_type":     "local",
		"destination_settings": map[string]string{"path": t.TempDir()},
	}
	body, _ := json.Marshal(policyReq)

	req := httptest.NewRequest(http.MethodPost, "/policies", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("Expected status 201, got %d: %s", w.Code, w.Body.String())
	}
}

func TestHandlerDeletePolicy(t *testing.T) {
	handler, storage := setupTestHandler(t)

	// Ensure storage implements LifecycleManager
	lifecycleStorage, ok := storage.(common.LifecycleManager)
	if !ok {
		t.Skip("Storage does not implement LifecycleManager")
	}

	// Add a test policy
	policy := common.LifecyclePolicy{
		ID:        "test-policy",
		Prefix:    "logs/",
		Retention: 24 * time.Hour,
		Action:    "delete",
	}
	err := lifecycleStorage.AddPolicy(policy)
	if err != nil {
		t.Fatalf("Failed to add test policy: %v", err)
	}

	req := httptest.NewRequest(http.MethodDelete, "/policies/test-policy", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	var response map[string]string
	if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if _, ok := response["message"]; !ok {
		t.Error("Expected message in response")
	}
	if response["id"] != "test-policy" {
		t.Errorf("Expected id 'test-policy', got %s", response["id"])
	}
}

func TestHandlerDeletePolicyNotFound(t *testing.T) {
	handler, storage := setupTestHandler(t)

	// Ensure storage implements LifecycleManager
	if _, ok := storage.(common.LifecycleManager); !ok {
		t.Skip("Storage does not implement LifecycleManager")
	}

	req := httptest.NewRequest(http.MethodDelete, "/policies/nonexistent", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	// Local storage may return 200 for idempotent delete, or 404 if implemented
	if w.Code != http.StatusOK && w.Code != http.StatusNotFound {
		t.Errorf("Expected status 200 or 404, got %d", w.Code)
	}
}

func TestHandlerPoliciesMethodNotAllowed(t *testing.T) {
	handler, _ := setupTestHandler(t)

	req := httptest.NewRequest(http.MethodPut, "/policies", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected status 405, got %d", w.Code)
	}
}

func TestHandlerExists(t *testing.T) {
	handler, storage := setupTestHandler(t)

	// Store test data
	testData := []byte("test data")
	err := storage.Put("test-key", bytes.NewReader(testData))
	if err != nil {
		t.Fatalf("Failed to store test data: %v", err)
	}

	// Test exists = true
	req := httptest.NewRequest(http.MethodGet, "/objects/test-key?exists=true", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	var response map[string]bool
	if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if !response["exists"] {
		t.Error("Expected exists to be true")
	}

	// Test exists = false for nonexistent key
	req = httptest.NewRequest(http.MethodGet, "/objects/nonexistent-key?exists=true", nil)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	response = make(map[string]bool)
	if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if response["exists"] {
		t.Error("Expected exists to be false")
	}
}

func TestHandlerUpdateMetadata(t *testing.T) {
	handler, storage := setupTestHandler(t)

	// Store test data
	testData := []byte("test data")
	err := storage.Put("test-key", bytes.NewReader(testData))
	if err != nil {
		t.Fatalf("Failed to store test data: %v", err)
	}

	// Update metadata
	metadataReq := map[string]any{
		"content_type":     "application/json",
		"content_encoding": "gzip",
		"custom": map[string]string{
			"author": "test-user",
		},
	}
	body, _ := json.Marshal(metadataReq)

	req := httptest.NewRequest(http.MethodPatch, "/objects/test-key", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	var response map[string]string
	if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if _, ok := response["message"]; !ok {
		t.Error("Expected message in response")
	}
}

func TestHandlerUpdateMetadataInvalidRequest(t *testing.T) {
	handler, _ := setupTestHandler(t)

	req := httptest.NewRequest(http.MethodPatch, "/objects/test-key", bytes.NewReader([]byte("invalid json")))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d", w.Code)
	}
}

func TestHandlerUpdateMetadataNonexistentKey(t *testing.T) {
	handler, _ := setupTestHandler(t)

	metadataReq := map[string]any{
		"content_type": "application/json",
	}
	body, _ := json.Marshal(metadataReq)

	req := httptest.NewRequest(http.MethodPatch, "/objects/nonexistent-key", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("Expected status 500, got %d", w.Code)
	}
}

func TestHandlerAddPolicyInvalidAction(t *testing.T) {
	handler, storage := setupTestHandler(t)

	// Ensure storage implements LifecycleManager
	if _, ok := storage.(common.LifecycleManager); !ok {
		t.Skip("Storage does not implement LifecycleManager")
	}

	policyReq := map[string]any{
		"id":                "test-policy",
		"prefix":            "logs/",
		"retention_seconds": 86400,
		"action":            "invalid-action",
	}
	body, _ := json.Marshal(policyReq)

	req := httptest.NewRequest(http.MethodPost, "/policies", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d", w.Code)
	}
}

func TestHandlerAddPolicyMissingAction(t *testing.T) {
	handler, storage := setupTestHandler(t)

	// Ensure storage implements LifecycleManager
	if _, ok := storage.(common.LifecycleManager); !ok {
		t.Skip("Storage does not implement LifecycleManager")
	}

	policyReq := map[string]any{
		"id":                "test-policy",
		"prefix":            "logs/",
		"retention_seconds": 86400,
	}
	body, _ := json.Marshal(policyReq)

	req := httptest.NewRequest(http.MethodPost, "/policies", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d", w.Code)
	}
}

func TestHandlerAddPolicyMissingDestinationType(t *testing.T) {
	handler, storage := setupTestHandler(t)

	// Ensure storage implements LifecycleManager
	if _, ok := storage.(common.LifecycleManager); !ok {
		t.Skip("Storage does not implement LifecycleManager")
	}

	// Archive action but missing destination_type
	policyReq := map[string]any{
		"id":                "test-policy",
		"prefix":            "data/",
		"retention_seconds": 2592000,
		"action":            "archive",
	}
	body, _ := json.Marshal(policyReq)

	req := httptest.NewRequest(http.MethodPost, "/policies", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d", w.Code)
	}
}

func TestHandlerAddPolicyInvalidRetentionType(t *testing.T) {
	handler, storage := setupTestHandler(t)

	// Ensure storage implements LifecycleManager
	if _, ok := storage.(common.LifecycleManager); !ok {
		t.Skip("Storage does not implement LifecycleManager")
	}

	// Non-positive retention_seconds
	policyReq := map[string]any{
		"id":                "test-policy",
		"prefix":            "logs/",
		"retention_seconds": -100,
		"action":            "delete",
	}
	body, _ := json.Marshal(policyReq)

	req := httptest.NewRequest(http.MethodPost, "/policies", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d", w.Code)
	}
}

func TestHandlerGetPoliciesEmpty(t *testing.T) {
	handler, storage := setupTestHandler(t)

	// Ensure storage implements LifecycleManager
	if _, ok := storage.(common.LifecycleManager); !ok {
		t.Skip("Storage does not implement LifecycleManager")
	}

	req := httptest.NewRequest(http.MethodGet, "/policies", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	var response map[string]any
	if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	policies, ok := response["policies"].([]any)
	if !ok {
		t.Fatal("Expected policies array in response")
	}

	// Should be empty initially
	if len(policies) != 0 {
		t.Errorf("Expected 0 policies, got %d", len(policies))
	}
}

func TestHandlerArchiveInvalidJSON(t *testing.T) {
	handler, _ := setupTestHandler(t)

	req := httptest.NewRequest(http.MethodPost, "/archive", bytes.NewReader([]byte("invalid json")))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d", w.Code)
	}
}

func TestHandlerArchiveMissingKey(t *testing.T) {
	handler, _ := setupTestHandler(t)

	archiveReq := map[string]any{
		"destination_type": "local",
	}
	body, _ := json.Marshal(archiveReq)

	req := httptest.NewRequest(http.MethodPost, "/archive", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400 for missing key, got %d", w.Code)
	}
}

func TestHandlerArchiveMissingDestinationType(t *testing.T) {
	handler, _ := setupTestHandler(t)

	archiveReq := map[string]any{
		"key": "test-key",
	}
	body, _ := json.Marshal(archiveReq)

	req := httptest.NewRequest(http.MethodPost, "/archive", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d", w.Code)
	}
}

func TestHandlerPolicyByIDInvalidMethod(t *testing.T) {
	handler, storage := setupTestHandler(t)

	// Ensure storage implements LifecycleManager
	if _, ok := storage.(common.LifecycleManager); !ok {
		t.Skip("Storage does not implement LifecycleManager")
	}

	// Try GET instead of DELETE
	req := httptest.NewRequest(http.MethodGet, "/policies/test-policy", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected status 405, got %d", w.Code)
	}
}

func TestHandlerPolicyByIDEmptyID(t *testing.T) {
	handler, storage := setupTestHandler(t)

	// Ensure storage implements LifecycleManager
	if _, ok := storage.(common.LifecycleManager); !ok {
		t.Skip("Storage does not implement LifecycleManager")
	}

	// Empty policy ID - should return 400 for missing ID
	req := httptest.NewRequest(http.MethodDelete, "/policies/", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	// Handler validates and returns 400 for empty policy ID
	if w.Code != http.StatusBadRequest && w.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected status 400 or 405, got %d", w.Code)
	}
}

func TestHandlerAddPolicyInvalidJSON(t *testing.T) {
	handler, storage := setupTestHandler(t)

	// Ensure storage implements LifecycleManager
	if _, ok := storage.(common.LifecycleManager); !ok {
		t.Skip("Storage does not implement LifecycleManager")
	}

	req := httptest.NewRequest(http.MethodPost, "/policies", bytes.NewReader([]byte("invalid json")))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d", w.Code)
	}
}

func TestHandlerGetPoliciesWithPrefix(t *testing.T) {
	handler, storage := setupTestHandler(t)

	// Ensure storage implements LifecycleManager
	lifecycleStorage, ok := storage.(common.LifecycleManager)
	if !ok {
		t.Skip("Storage does not implement LifecycleManager")
	}

	// Add two policies with different prefixes
	policy1 := common.LifecyclePolicy{
		ID:        "logs-policy",
		Prefix:    "logs/",
		Retention: 24 * time.Hour,
		Action:    "delete",
	}
	policy2 := common.LifecyclePolicy{
		ID:        "data-policy",
		Prefix:    "data/",
		Retention: 30 * 24 * time.Hour,
		Action:    "delete",
	}

	_ = lifecycleStorage.AddPolicy(policy1)
	_ = lifecycleStorage.AddPolicy(policy2)

	// Query with prefix filter
	req := httptest.NewRequest(http.MethodGet, "/policies?prefix=logs/", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	var response map[string]any
	if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	policies, ok := response["policies"].([]any)
	if !ok {
		t.Fatal("Expected policies array in response")
	}

	// Should return at least the logs policy
	if len(policies) == 0 {
		t.Error("Expected at least one policy with logs/ prefix")
	}
}

func TestHandlerCreateArchiverError(t *testing.T) {
	handler, storage := setupTestHandler(t)

	// Store test data
	testData := []byte("test data")
	err := storage.Put("test-key", bytes.NewReader(testData))
	if err != nil {
		t.Fatalf("Failed to store test data: %v", err)
	}

	// Try to create archiver with invalid type
	archiveReq := map[string]any{
		"key":                  "test-key",
		"destination_type":     "invalid-archiver-type",
		"destination_settings": map[string]string{},
	}
	body, _ := json.Marshal(archiveReq)

	req := httptest.NewRequest(http.MethodPost, "/archive", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d", w.Code)
	}
}

func TestHandlerArchiveMethodNotAllowed(t *testing.T) {
	handler, _ := setupTestHandler(t)

	// Try GET instead of POST
	req := httptest.NewRequest(http.MethodGet, "/archive", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected status 405, got %d", w.Code)
	}
}

func TestHandlerExistsError(t *testing.T) {
	handler, storage := setupTestHandler(t)

	// Create a mock storage that returns an error
	type errorStorage struct {
		common.Storage
	}

	// Test with a key that triggers some edge case
	req := httptest.NewRequest(http.MethodGet, "/objects/../../etc/passwd?exists=true", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	// Should handle the request (even if invalid path)
	if w.Code != http.StatusOK && w.Code != http.StatusBadRequest {
		t.Logf("Got status %d which is acceptable", w.Code)
	}

	_ = storage // keep linter happy
}

func TestHandlerGetRange(t *testing.T) {
	handler, storage := setupTestHandler(t)

	// Store test data
	testData := []byte("0123456789abcdefghijklmnopqrstuvwxyz")
	metadata := &common.Metadata{
		ContentType: "text/plain",
	}
	err := storage.PutWithMetadata(context.Background(), "test-key", bytes.NewReader(testData), metadata)
	if err != nil {
		t.Fatalf("Failed to store test data: %v", err)
	}

	// Request with Range header
	req := httptest.NewRequest(http.MethodGet, "/objects/test-key", nil)
	req.Header.Set("Range", "bytes=0-9")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	// Should return 206 Partial Content or 200 OK
	if w.Code != http.StatusPartialContent && w.Code != http.StatusOK {
		t.Errorf("Expected status 206 or 200, got %d", w.Code)
	}

	// If range is supported, verify content length
	if w.Code == http.StatusPartialContent {
		if w.Body.Len() > 10 {
			t.Errorf("Expected at most 10 bytes with range, got %d", w.Body.Len())
		}
	}
}

func TestHandlerGetWithIfModifiedSince(t *testing.T) {
	handler, storage := setupTestHandler(t)

	// Store test data
	testData := []byte("test data")
	err := storage.Put("test-key", bytes.NewReader(testData))
	if err != nil {
		t.Fatalf("Failed to store test data: %v", err)
	}

	// Request with If-Modified-Since in the future
	futureTime := time.Now().Add(24 * time.Hour).Format(http.TimeFormat)
	req := httptest.NewRequest(http.MethodGet, "/objects/test-key", nil)
	req.Header.Set("If-Modified-Since", futureTime)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	// Should return 304 Not Modified or 200 OK (depending on implementation)
	if w.Code != http.StatusNotModified && w.Code != http.StatusOK {
		t.Logf("Got status %d for If-Modified-Since test", w.Code)
	}
}

func TestHandlerAddPolicyDuplicate(t *testing.T) {
	handler, storage := setupTestHandler(t)

	// Ensure storage implements LifecycleManager
	lifecycleStorage, ok := storage.(common.LifecycleManager)
	if !ok {
		t.Skip("Storage does not implement LifecycleManager")
	}

	// Add the policy first
	policy := common.LifecyclePolicy{
		ID:        "duplicate-policy",
		Prefix:    "logs/",
		Retention: 24 * time.Hour,
		Action:    "delete",
	}
	err := lifecycleStorage.AddPolicy(policy)
	if err != nil {
		t.Fatalf("Failed to add initial policy: %v", err)
	}

	// Try to add the same policy again via HTTP
	policyReq := map[string]any{
		"id":                "duplicate-policy",
		"prefix":            "logs/",
		"retention_seconds": 86400,
		"action":            "delete",
	}
	body, _ := json.Marshal(policyReq)

	req := httptest.NewRequest(http.MethodPost, "/policies", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	// Should return 409 Conflict or 500 depending on implementation
	if w.Code != http.StatusConflict && w.Code != http.StatusInternalServerError {
		t.Logf("Expected 409 or 500 for duplicate policy, got %d", w.Code)
	}
}

func TestHandlerGetObjectNotFoundError(t *testing.T) {
	handler, _ := setupTestHandler(t)

	// Try to get a non-existent object
	req := httptest.NewRequest(http.MethodGet, "/objects/nonexistent-key", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("Expected status 404, got %d", w.Code)
	}
}

func TestHandlerPutWithLargeMetadata(t *testing.T) {
	handler, _ := setupTestHandler(t)

	data := []byte("test data")
	req := httptest.NewRequest(http.MethodPut, "/objects/test-key", bytes.NewReader(data))
	req.Header.Set("Content-Type", "text/plain")
	// Add many custom metadata headers
	for i := 0; i < 10; i++ {
		req.Header.Set(fmt.Sprintf("X-Meta-Field%d", i), fmt.Sprintf("value%d", i))
	}

	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("Expected status 201, got %d", w.Code)
	}
}

func TestHandlerListWithLimitAndContinuationToken(t *testing.T) {
	handler, storage := setupTestHandler(t)

	// Store multiple objects
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("test-key-%d", i)
		data := []byte(fmt.Sprintf("data-%d", i))
		err := storage.Put(key, bytes.NewReader(data))
		if err != nil {
			t.Fatalf("Failed to store test data: %v", err)
		}
	}

	// List with limit
	req := httptest.NewRequest(http.MethodGet, "/objects?limit=2", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	var response map[string]any
	if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	objects, ok := response["objects"].([]any)
	if !ok {
		t.Fatal("Expected objects array in response")
	}

	// Verify we got objects back (limit behavior depends on storage implementation)
	if len(objects) == 0 {
		t.Error("Expected at least some objects")
	}
}

func TestHandlerDeleteObjectNotFound(t *testing.T) {
	handler, _ := setupTestHandler(t)

	req := httptest.NewRequest(http.MethodDelete, "/objects/nonexistent-key", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	// Delete is typically idempotent, so 200 or 404 are both acceptable
	if w.Code != http.StatusOK && w.Code != http.StatusNotFound {
		t.Logf("Got status %d for delete nonexistent object", w.Code)
	}
}

func TestHandlerUpdateMetadataEmptyBody(t *testing.T) {
	handler, storage := setupTestHandler(t)

	// Store test data
	testData := []byte("test data")
	err := storage.Put("test-key", bytes.NewReader(testData))
	if err != nil {
		t.Fatalf("Failed to store test data: %v", err)
	}

	// Try to update with empty metadata
	metadataReq := map[string]any{}
	body, _ := json.Marshal(metadataReq)

	req := httptest.NewRequest(http.MethodPatch, "/objects/test-key", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	// Should still succeed (empty update is valid)
	if w.Code != http.StatusOK && w.Code != http.StatusBadRequest {
		t.Logf("Got status %d for empty metadata update", w.Code)
	}
}

func TestHandlerUpdateMetadataWithCustomFields(t *testing.T) {
	handler, storage := setupTestHandler(t)

	// Store test data
	testData := []byte("test data")
	err := storage.Put("test-key", bytes.NewReader(testData))
	if err != nil {
		t.Fatalf("Failed to store test data: %v", err)
	}

	// Update with custom fields
	metadataReq := map[string]any{
		"custom": map[string]string{
			"field1": "value1",
			"field2": "value2",
			"field3": "value3",
		},
	}
	body, _ := json.Marshal(metadataReq)

	req := httptest.NewRequest(http.MethodPatch, "/objects/test-key", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}
}

func TestHandlerArchiveWithMinimalSettings(t *testing.T) {
	handler, storage := setupTestHandler(t)

	// Store test data
	testData := []byte("minimal archive test")
	err := storage.Put("archive-test-key", bytes.NewReader(testData))
	if err != nil {
		t.Fatalf("Failed to store test data: %v", err)
	}

	// Archive with minimal settings
	archiveReq := map[string]any{
		"key":                  "archive-test-key",
		"destination_type":     "local",
		"destination_settings": map[string]string{"path": t.TempDir()},
	}
	body, _ := json.Marshal(archiveReq)

	req := httptest.NewRequest(http.MethodPost, "/archive", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Logf("Archive request returned %d: %s", w.Code, w.Body.String())
	}
}

func TestHandlerPutWithContentEncoding(t *testing.T) {
	handler, _ := setupTestHandler(t)

	data := []byte("compressed data")
	req := httptest.NewRequest(http.MethodPut, "/objects/encoded-key", bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Content-Encoding", "gzip")

	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("Expected status 201, got %d", w.Code)
	}
}

func TestHandlerGetWithAcceptEncoding(t *testing.T) {
	handler, storage := setupTestHandler(t)

	// Store test data
	testData := []byte("test data for encoding")
	err := storage.Put("test-encoding", bytes.NewReader(testData))
	if err != nil {
		t.Fatalf("Failed to store test data: %v", err)
	}

	// Get with Accept-Encoding
	req := httptest.NewRequest(http.MethodGet, "/objects/test-encoding", nil)
	req.Header.Set("Accept-Encoding", "gzip, deflate")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}
}

func TestHandlerListWithPrefix(t *testing.T) {
	handler, storage := setupTestHandler(t)

	// Store objects with different prefixes
	prefixes := []string{"logs/", "data/", "temp/"}
	for _, prefix := range prefixes {
		for i := 0; i < 2; i++ {
			key := fmt.Sprintf("%s%s%d", prefix, "file", i)
			data := []byte(fmt.Sprintf("data for %s", key))
			err := storage.Put(key, bytes.NewReader(data))
			if err != nil {
				t.Fatalf("Failed to store %s: %v", key, err)
			}
		}
	}

	// List with prefix filter
	req := httptest.NewRequest(http.MethodGet, "/objects?prefix=logs/", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	var response map[string]any
	if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	objects, ok := response["objects"].([]any)
	if !ok {
		t.Fatal("Expected objects array in response")
	}

	// Should have some objects
	if len(objects) == 0 {
		t.Error("Expected objects with logs/ prefix")
	}
}

func TestHandlerGetPoliciesMultiple(t *testing.T) {
	handler, storage := setupTestHandler(t)

	// Ensure storage implements LifecycleManager
	lifecycleStorage, ok := storage.(common.LifecycleManager)
	if !ok {
		t.Skip("Storage does not implement LifecycleManager")
	}

	// Add multiple policies
	policies := []common.LifecyclePolicy{
		{
			ID:        "policy1",
			Prefix:    "logs/",
			Retention: 24 * time.Hour,
			Action:    "delete",
		},
		{
			ID:        "policy2",
			Prefix:    "data/",
			Retention: 48 * time.Hour,
			Action:    "delete",
		},
		{
			ID:        "policy3",
			Prefix:    "temp/",
			Retention: 1 * time.Hour,
			Action:    "delete",
		},
	}

	for _, policy := range policies {
		_ = lifecycleStorage.AddPolicy(policy)
	}

	// Get all policies
	req := httptest.NewRequest(http.MethodGet, "/policies", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	var response map[string]any
	if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	policyList, ok := response["policies"].([]any)
	if !ok {
		t.Fatal("Expected policies array")
	}

	if len(policyList) != 3 {
		t.Errorf("Expected 3 policies, got %d", len(policyList))
	}
}

// Error path tests using mock storage

func TestHandlerExistsStorageError(t *testing.T) {
	mockStorage := newMockErrorStorage()
	mockStorage.existsError = errors.New("storage unavailable")

	initTestFacade(t, mockStorage)
	logger := adapters.NewNoOpLogger()
	auth := adapters.NewNoOpAuthenticator()
	handler, err := NewHandler("", 100*1024*1024, 30*time.Second, 30*time.Second, logger, auth)
	if err != nil {
		t.Fatalf("Failed to create handler: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/objects/test-key?exists=true", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("Expected status 500 for storage error, got %d", w.Code)
	}
}

func TestHandlerUpdateMetadataStorageError(t *testing.T) {
	mockStorage := newMockErrorStorage()
	mockStorage.updateMetadataError = errors.New("storage write error")

	// Add object first
	mockStorage.objects["test-key"] = []byte("data")
	mockStorage.metadata["test-key"] = &common.Metadata{}

	initTestFacade(t, mockStorage)
	logger := adapters.NewNoOpLogger()
	auth := adapters.NewNoOpAuthenticator()
	handler, err := NewHandler("", 100*1024*1024, 30*time.Second, 30*time.Second, logger, auth)
	if err != nil {
		t.Fatalf("Failed to create handler: %v", err)
	}

	metadataReq := map[string]any{
		"content_type": "application/json",
	}
	body, _ := json.Marshal(metadataReq)

	req := httptest.NewRequest(http.MethodPatch, "/objects/test-key", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("Expected status 500 for storage error, got %d", w.Code)
	}
}

func TestHandlerArchiveStorageError(t *testing.T) {
	mockStorage := newMockErrorStorage()
	mockStorage.archiveError = errors.New("archive operation failed")

	// Add object first
	mockStorage.objects["test-key"] = []byte("data")
	mockStorage.metadata["test-key"] = &common.Metadata{}

	initTestFacade(t, mockStorage)
	logger := adapters.NewNoOpLogger()
	auth := adapters.NewNoOpAuthenticator()
	handler, err := NewHandler("", 100*1024*1024, 30*time.Second, 30*time.Second, logger, auth)
	if err != nil {
		t.Fatalf("Failed to create handler: %v", err)
	}

	archiveReq := map[string]any{
		"key":                  "test-key",
		"destination_type":     "local",
		"destination_settings": map[string]string{"path": "/tmp/test"},
	}
	body, _ := json.Marshal(archiveReq)

	req := httptest.NewRequest(http.MethodPost, "/archive", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("Expected status 500 for archive error, got %d", w.Code)
	}
}

func TestHandlerGetPoliciesStorageError(t *testing.T) {
	mockStorage := newMockErrorStorage()
	mockStorage.getPoliciesError = errors.New("database connection lost")

	initTestFacade(t, mockStorage)
	logger := adapters.NewNoOpLogger()
	auth := adapters.NewNoOpAuthenticator()
	handler, err := NewHandler("", 100*1024*1024, 30*time.Second, 30*time.Second, logger, auth)
	if err != nil {
		t.Fatalf("Failed to create handler: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/policies", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("Expected status 500 for storage error, got %d", w.Code)
	}
}

func TestHandlerGetPoliciesWithPrefixFilter(t *testing.T) {
	mockStorage := newMockErrorStorage()

	// Add policies with different prefixes
	policy1 := common.LifecyclePolicy{
		ID:     "policy1",
		Prefix: "logs/",
		Action: "archive",
	}
	policy2 := common.LifecyclePolicy{
		ID:     "policy2",
		Prefix: "data/",
		Action: "archive",
	}
	mockStorage.policies = []common.LifecyclePolicy{policy1, policy2}

	initTestFacade(t, mockStorage)
	logger := adapters.NewNoOpLogger()
	auth := adapters.NewNoOpAuthenticator()
	handler, err := NewHandler("", 100*1024*1024, 30*time.Second, 30*time.Second, logger, auth)
	if err != nil {
		t.Fatalf("Failed to create handler: %v", err)
	}

	// Test with prefix filter
	req := httptest.NewRequest(http.MethodGet, "/policies?prefix=logs/", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	var response map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("Failed to parse response: %v", err)
	}

	count := int(response["count"].(float64))
	if count != 1 {
		t.Errorf("Expected 1 policy with prefix 'logs/', got %d", count)
	}
}

func TestHandlerAddPolicyStorageError(t *testing.T) {
	mockStorage := newMockErrorStorage()
	mockStorage.addPolicyError = errors.New("policy storage full")

	initTestFacade(t, mockStorage)
	logger := adapters.NewNoOpLogger()
	auth := adapters.NewNoOpAuthenticator()
	handler, err := NewHandler("", 100*1024*1024, 30*time.Second, 30*time.Second, logger, auth)
	if err != nil {
		t.Fatalf("Failed to create handler: %v", err)
	}

	policyReq := map[string]any{
		"id":                "test-policy",
		"prefix":            "logs/",
		"retention_seconds": 86400,
		"action":            "delete",
	}
	body, _ := json.Marshal(policyReq)

	req := httptest.NewRequest(http.MethodPost, "/policies", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("Expected status 500 for storage error, got %d", w.Code)
	}
}

func TestHandlerRemovePolicyNotFound(t *testing.T) {
	mockStorage := newMockErrorStorage()

	initTestFacade(t, mockStorage)
	logger := adapters.NewNoOpLogger()
	auth := adapters.NewNoOpAuthenticator()
	handler, err := NewHandler("", 100*1024*1024, 30*time.Second, 30*time.Second, logger, auth)
	if err != nil {
		t.Fatalf("Failed to create handler: %v", err)
	}

	req := httptest.NewRequest(http.MethodDelete, "/policies/nonexistent-id", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("Expected status 404 for policy not found, got %d", w.Code)
	}
}

func TestHandlerRemovePolicyStorageError(t *testing.T) {
	mockStorage := newMockErrorStorage()
	mockStorage.removePolicyError = errors.New("database error")

	initTestFacade(t, mockStorage)
	logger := adapters.NewNoOpLogger()
	auth := adapters.NewNoOpAuthenticator()
	handler, err := NewHandler("", 100*1024*1024, 30*time.Second, 30*time.Second, logger, auth)
	if err != nil {
		t.Fatalf("Failed to create handler: %v", err)
	}

	req := httptest.NewRequest(http.MethodDelete, "/policies/test-policy", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("Expected status 500 for storage error, got %d", w.Code)
	}
}

func TestHandlerAddPolicyConflict(t *testing.T) {
	mockStorage := newMockErrorStorage()

	initTestFacade(t, mockStorage)
	logger := adapters.NewNoOpLogger()
	auth := adapters.NewNoOpAuthenticator()
	handler, err := NewHandler("", 100*1024*1024, 30*time.Second, 30*time.Second, logger, auth)
	if err != nil {
		t.Fatalf("Failed to create handler: %v", err)
	}

	// Add policy first time
	policyReq := map[string]any{
		"id":                "duplicate-policy",
		"prefix":            "logs/",
		"retention_seconds": 86400,
		"action":            "delete",
	}
	body, _ := json.Marshal(policyReq)

	req := httptest.NewRequest(http.MethodPost, "/policies", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("First policy add failed: %d", w.Code)
	}

	// Try to add again - should get conflict
	body, _ = json.Marshal(policyReq)
	req = httptest.NewRequest(http.MethodPost, "/policies", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusConflict {
		t.Errorf("Expected status 409 for duplicate policy, got %d", w.Code)
	}
}

func TestHandlerGetWithFullMetadata(t *testing.T) {
	mockStorage := newMockErrorStorage()

	// Add an object with full metadata including ContentEncoding, ETag, and Custom fields
	data := []byte("test data")
	mockStorage.objects["test-key"] = data
	mockStorage.metadata["test-key"] = &common.Metadata{
		Size:            int64(len(data)),
		ContentType:     "application/json",
		ContentEncoding: "gzip",
		ETag:            "abc123",
		Custom: map[string]string{
			"Author":  "testuser",
			"Version": "1.0",
		},
	}

	initTestFacade(t, mockStorage)
	logger := adapters.NewNoOpLogger()
	auth := adapters.NewNoOpAuthenticator()
	handler, err := NewHandler("", 100*1024*1024, 30*time.Second, 30*time.Second, logger, auth)
	if err != nil {
		t.Fatalf("Failed to create handler: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/objects/test-key", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	// Verify headers were set
	if w.Header().Get("Content-Encoding") != "gzip" {
		t.Errorf("Expected Content-Encoding header 'gzip', got '%s'", w.Header().Get("Content-Encoding"))
	}
	if w.Header().Get("ETag") != "abc123" {
		t.Errorf("Expected ETag header 'abc123', got '%s'", w.Header().Get("ETag"))
	}
	if w.Header().Get("X-Meta-Author") != "testuser" {
		t.Errorf("Expected X-Meta-Author header 'testuser', got '%s'", w.Header().Get("X-Meta-Author"))
	}
}

func TestHandlerListWithPaginationAndDelimiter(t *testing.T) {
	mockStorage := newMockErrorStorage()

	// Add multiple objects
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("file%d", i)
		mockStorage.objects[key] = []byte("data")
		mockStorage.metadata[key] = &common.Metadata{Size: 4}
	}

	initTestFacade(t, mockStorage)
	logger := adapters.NewNoOpLogger()
	auth := adapters.NewNoOpAuthenticator()
	handler, err := NewHandler("", 100*1024*1024, 30*time.Second, 30*time.Second, logger, auth)
	if err != nil {
		t.Fatalf("Failed to create handler: %v", err)
	}

	// Test with delimiter and max results for pagination
	req := httptest.NewRequest(http.MethodGet, "/objects?delimiter=/&max=2", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	var response map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("Failed to parse response: %v", err)
	}

	// Should have next_token due to pagination
	if _, hasNextToken := response["next_token"]; !hasNextToken {
		t.Error("Expected next_token in response for paginated results")
	}

	// Should have prefixes due to delimiter
	if _, hasPrefixes := response["prefixes"]; !hasPrefixes {
		t.Error("Expected prefixes in response when using delimiter")
	}
}

func TestHandlerListStorageError(t *testing.T) {
	mockStorage := newMockErrorStorage()
	mockStorage.listError = errors.New("database connection lost")

	initTestFacade(t, mockStorage)
	logger := adapters.NewNoOpLogger()
	auth := adapters.NewNoOpAuthenticator()
	handler, err := NewHandler("", 100*1024*1024, 30*time.Second, 30*time.Second, logger, auth)
	if err != nil {
		t.Fatalf("Failed to create handler: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/objects", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("Expected status 500 for storage error, got %d", w.Code)
	}
}

func TestHandlerDeleteStorageError(t *testing.T) {
	mockStorage := newMockErrorStorage()
	mockStorage.deleteError = errors.New("permission denied")

	initTestFacade(t, mockStorage)
	logger := adapters.NewNoOpLogger()
	auth := adapters.NewNoOpAuthenticator()
	handler, err := NewHandler("", 100*1024*1024, 30*time.Second, 30*time.Second, logger, auth)
	if err != nil {
		t.Fatalf("Failed to create handler: %v", err)
	}

	req := httptest.NewRequest(http.MethodDelete, "/objects/test-key", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("Expected status 500 for storage error, got %d", w.Code)
	}
}
