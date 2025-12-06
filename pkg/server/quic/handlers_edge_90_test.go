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
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// mockStorageWithExistsError returns error on Exists call
type mockStorageWithExistsError struct {
	*mockLifecycleStorage
}

func (m *mockStorageWithExistsError) Exists(ctx context.Context, key string) (bool, error) {
	return false, errors.New("storage error")
}

// TestHandleExists_StorageError tests exists with storage error
func TestHandleExists_StorageError(t *testing.T) {
	storage := &mockStorageWithExistsError{
		mockLifecycleStorage: newMockLifecycleStorage(),
	}
	handler := createHandlerWithStorage(t, storage, 10*1024*1024, 30*time.Second, 30*time.Second, &mockLogger{}, &mockAuthenticator{})

	req := httptest.NewRequest(http.MethodGet, "/objects/test.txt?exists=true", nil)
	w := httptest.NewRecorder()

	handler.handleExists(w, req, "test.txt")

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected status 500, got %d", w.Code)
	}
}

// mockStorageWithGetMetadataError returns error on GetMetadata
type mockStorageWithGetMetadataError struct {
	*mockLifecycleStorage
}

func (m *mockStorageWithGetMetadataError) GetMetadata(ctx context.Context, key string) (*common.Metadata, error) {
	return nil, errors.New("metadata error")
}

// TestHandleGet_MetadataError tests get with metadata error
func TestHandleGet_MetadataError(t *testing.T) {
	storage := &mockStorageWithGetMetadataError{
		mockLifecycleStorage: newMockLifecycleStorage(),
	}
	handler := createHandlerWithStorage(t, storage, 10*1024*1024, 30*time.Second, 30*time.Second, &mockLogger{}, &mockAuthenticator{})

	req := httptest.NewRequest(http.MethodGet, "/objects/test.txt", nil)
	w := httptest.NewRecorder()

	handler.handleGet(w, req, "test.txt")

	if w.Code != http.StatusNotFound {
		t.Errorf("expected status 404, got %d", w.Code)
	}
}

// TestHandleGetPolicies_StorageError tests get policies with storage error
func TestHandleGetPolicies_StorageError(t *testing.T) {
	storage := newMockLifecycleStorage()
	storage.getPoliciesError = errors.New("database error")
	handler := createHandlerWithStorage(t, storage, 10*1024*1024, 30*time.Second, 30*time.Second, &mockLogger{}, &mockAuthenticator{})

	req := httptest.NewRequest(http.MethodGet, "/policies", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected status 500, got %d", w.Code)
	}
}

// TestHandleAddPolicy_StorageError tests add policy with storage error
func TestHandleAddPolicy_StorageError(t *testing.T) {
	storage := newMockLifecycleStorage()
	storage.addPolicyError = errors.New("database error")
	handler := createHandlerWithStorage(t, storage, 10*1024*1024, 30*time.Second, 30*time.Second, &mockLogger{}, &mockAuthenticator{})

	policy := map[string]any{
		"id":                "test-policy",
		"retention_seconds": 86400,
		"action":            "delete",
	}

	body, _ := json.Marshal(policy)
	req := httptest.NewRequest(http.MethodPost, "/policies", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected status 500, got %d", w.Code)
	}
}

// TestHandleUpdateMetadata_WithCustomMetadata tests update with custom metadata
func TestHandleUpdateMetadata_WithCustomMetadata(t *testing.T) {
	storage := newMockLifecycleStorage()
	handler := createHandlerWithStorage(t, storage, 10*1024*1024, 30*time.Second, 30*time.Second, &mockLogger{}, &mockAuthenticator{})

	// Add object
	storage.PutWithContext(context.Background(), "test.txt", bytes.NewReader([]byte("data")))

	metadataUpdate := map[string]any{
		"content_type": "application/json",
		"custom": map[string]string{
			"author": "test",
		},
	}

	body, _ := json.Marshal(metadataUpdate)
	req := httptest.NewRequest(http.MethodPatch, "/objects/test.txt", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handler.handleUpdateMetadata(w, req, "test.txt")

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}
}

// TestServeHTTP_PoliciesRoute tests policies routing
func TestServeHTTP_PoliciesRoute(t *testing.T) {
	storage := newMockLifecycleStorage()
	handler := createHandlerWithStorage(t, storage, 10*1024*1024, 30*time.Second, 30*time.Second, &mockLogger{}, &mockAuthenticator{})

	// Test GET /policies
	req := httptest.NewRequest(http.MethodGet, "/policies", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("GET /policies: expected status 200, got %d", w.Code)
	}

	// Test POST /policies
	policy := map[string]any{
		"id":                "test",
		"retention_seconds": 86400,
		"action":            "delete",
	}
	body, _ := json.Marshal(policy)
	req = httptest.NewRequest(http.MethodPost, "/policies", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("POST /policies: expected status 201, got %d", w.Code)
	}

	// Skip GET /policies/test as it may not be supported

	// Test DELETE /policies/test
	req = httptest.NewRequest(http.MethodDelete, "/policies/test", nil)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("DELETE /policies/test: expected status 200, got %d", w.Code)
	}
}

// TestHandleGet_NilMetadata tests get with nil metadata
func TestHandleGet_NilMetadata(t *testing.T) {
	storage := newMockLifecycleStorage()
	handler := createHandlerWithStorage(t, storage, 10*1024*1024, 30*time.Second, 30*time.Second, &mockLogger{}, &mockAuthenticator{})

	// Add object
	storage.PutWithContext(context.Background(), "test.txt", bytes.NewReader([]byte("data")))
	// Set metadata to nil
	storage.metadata["test.txt"] = nil

	req := httptest.NewRequest(http.MethodGet, "/objects/test.txt", nil)
	w := httptest.NewRecorder()

	handler.handleGet(w, req, "test.txt")

	if w.Code != http.StatusNotFound {
		t.Errorf("expected status 404, got %d", w.Code)
	}
}

// TestHandleGet_AllHeadersSet tests get with all metadata fields set
func TestHandleGet_AllHeadersSet(t *testing.T) {
	storage := newMockLifecycleStorage()
	handler := createHandlerWithStorage(t, storage, 10*1024*1024, 30*time.Second, 30*time.Second, &mockLogger{}, &mockAuthenticator{})

	metadata := &common.Metadata{
		ContentType:     "application/octet-stream",
		ContentEncoding: "br",
		Size:            4,
		LastModified:    time.Now(),
		ETag:            "abc123",
		Custom:          nil, // No custom metadata
	}
	storage.PutWithMetadata(context.Background(), "binary.dat", bytes.NewReader([]byte("data")), metadata)

	req := httptest.NewRequest(http.MethodGet, "/objects/binary.dat", nil)
	w := httptest.NewRecorder()

	handler.handleGet(w, req, "binary.dat")

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	if w.Header().Get("Content-Type") != "application/octet-stream" {
		t.Errorf("expected Content-Type application/octet-stream, got %s", w.Header().Get("Content-Type"))
	}

	if w.Header().Get("Content-Encoding") != "br" {
		t.Errorf("expected Content-Encoding br, got %s", w.Header().Get("Content-Encoding"))
	}

	if w.Header().Get("ETag") != "abc123" {
		t.Errorf("expected ETag abc123, got %s", w.Header().Get("ETag"))
	}
}
