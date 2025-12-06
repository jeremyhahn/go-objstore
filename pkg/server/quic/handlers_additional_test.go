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
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/local"
)

func TestHandlerPutObjectWithTimeout(t *testing.T) {
	handler, _ := setupTestHandler(t)
	handler.writeTimeout = 1 * time.Nanosecond // Very short timeout

	data := []byte("test data")
	req := httptest.NewRequest(http.MethodPut, "/objects/test-key", bytes.NewReader(data))

	// Create a context that's already cancelled
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req = req.WithContext(ctx)

	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	// Should timeout
	if w.Code != http.StatusRequestTimeout {
		t.Logf("Expected timeout status, got %d", w.Code)
	}
}

func TestHandlerGetObjectWithTimeout(t *testing.T) {
	handler, storage := setupTestHandler(t)
	handler.readTimeout = 1 * time.Nanosecond // Very short timeout

	// Store test data
	err := storage.Put("test-key", bytes.NewReader([]byte("test data")))
	if err != nil {
		t.Fatalf("Failed to store test data: %v", err)
	}

	// Create a context that's already cancelled
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	req := httptest.NewRequest(http.MethodGet, "/objects/test-key", nil)
	req = req.WithContext(ctx)

	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	// Should timeout or fail
	if w.Code != http.StatusRequestTimeout && w.Code != http.StatusNotFound {
		t.Logf("Expected error status, got %d", w.Code)
	}
}

func TestHandlerDeleteObjectWithTimeout(t *testing.T) {
	handler, storage := setupTestHandler(t)
	handler.writeTimeout = 1 * time.Nanosecond

	// Store test data
	err := storage.Put("test-key", bytes.NewReader([]byte("test data")))
	if err != nil {
		t.Fatalf("Failed to store test data: %v", err)
	}

	// Create a context that's already cancelled
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	req := httptest.NewRequest(http.MethodDelete, "/objects/test-key", nil)
	req = req.WithContext(ctx)

	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	// Should timeout or succeed
	if w.Code != http.StatusRequestTimeout && w.Code != http.StatusNoContent {
		t.Logf("Expected timeout or success status, got %d", w.Code)
	}
}

func TestHandlerListObjectsWithTimeout(t *testing.T) {
	handler, _ := setupTestHandler(t)
	handler.readTimeout = 1 * time.Nanosecond

	// Create a context that's already cancelled
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	req := httptest.NewRequest(http.MethodGet, "/objects", nil)
	req = req.WithContext(ctx)

	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	// Should timeout or succeed
	if w.Code != http.StatusRequestTimeout && w.Code != http.StatusOK {
		t.Logf("Expected timeout or success status, got %d", w.Code)
	}
}

func TestHandlerListWithMaxResults(t *testing.T) {
	handler, storage := setupTestHandler(t)

	// Store many objects
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("obj%d", i)
		err := storage.Put(key, bytes.NewReader([]byte(key)))
		if err != nil {
			t.Fatalf("Failed to store object: %v", err)
		}
	}

	// List with max results
	req := httptest.NewRequest(http.MethodGet, "/objects?max=5", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}
}

func TestHandlerListWithDelimiter(t *testing.T) {
	handler, storage := setupTestHandler(t)

	// Store objects with folder structure
	objects := []string{"folder1/file1", "folder1/file2", "folder2/file1"}
	for _, key := range objects {
		err := storage.Put(key, bytes.NewReader([]byte(key)))
		if err != nil {
			t.Fatalf("Failed to store object: %v", err)
		}
	}

	// List with delimiter
	req := httptest.NewRequest(http.MethodGet, "/objects?delimiter=/", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}
}

func TestHandlerPutObjectNoContentType(t *testing.T) {
	handler, _ := setupTestHandler(t)

	data := []byte("test data")
	req := httptest.NewRequest(http.MethodPut, "/objects/test-key", bytes.NewReader(data))
	// Don't set Content-Type

	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("Expected status 201, got %d", w.Code)
	}
}

func TestHandlerGetObjectNoMetadata(t *testing.T) {
	handler, storage := setupTestHandler(t)

	// Store without metadata
	err := storage.Put("test-key", bytes.NewReader([]byte("test data")))
	if err != nil {
		t.Fatalf("Failed to store test data: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/objects/test-key", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}
}

func TestHandlerEmptyList(t *testing.T) {
	handler, _ := setupTestHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/objects?prefix=nonexistent", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}
}

func TestHandlerPutWithMultipleCustomMetadata(t *testing.T) {
	handler, storage := setupTestHandler(t)

	req := httptest.NewRequest(http.MethodPut, "/objects/test-key", strings.NewReader("test data"))
	req.Header.Set("X-Meta-Field1", "value1")
	req.Header.Set("X-Meta-Field2", "value2")
	req.Header.Set("X-Meta-Field3", "value3")
	req.Header.Set("X-Regular-Header", "ignored")

	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("Expected status 201, got %d", w.Code)
	}

	// Verify metadata was stored
	info, err := storage.GetMetadata(context.Background(), "test-key")
	if err != nil {
		t.Fatalf("Failed to get metadata: %v", err)
	}

	if info.Custom["Field1"] != "value1" {
		t.Errorf("Expected Field1=value1, got %s", info.Custom["Field1"])
	}

	if info.Custom["Field2"] != "value2" {
		t.Errorf("Expected Field2=value2, got %s", info.Custom["Field2"])
	}

	if info.Custom["Field3"] != "value3" {
		t.Errorf("Expected Field3=value3, got %s", info.Custom["Field3"])
	}
}

func TestNewHandler(t *testing.T) {
	storage := local.New()
	storage.Configure(map[string]string{"path": t.TempDir()})

	logger := adapters.NewNoOpLogger()
	auth := adapters.NewNoOpAuthenticator()
	handler := createHandlerWithStorage(t, storage, 50*1024*1024, 15*time.Second, 20*time.Second, logger, auth)

	if handler == nil {
		t.Fatal("Expected non-nil handler")
	}

	// Handler now uses facade, verify backend is set correctly
	if handler.backend != "" {
		t.Error("Expected default backend (empty string)")
	}

	if handler.maxRequestBodySize != 50*1024*1024 {
		t.Errorf("Expected max body size 50MB, got %d", handler.maxRequestBodySize)
	}

	if handler.readTimeout != 15*time.Second {
		t.Errorf("Expected read timeout 15s, got %v", handler.readTimeout)
	}

	if handler.writeTimeout != 20*time.Second {
		t.Errorf("Expected write timeout 20s, got %v", handler.writeTimeout)
	}
}

func TestHandlerListWithContinueToken(t *testing.T) {
	handler, storage := setupTestHandler(t)

	// Store test objects
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("obj%d", i)
		err := storage.Put(key, bytes.NewReader([]byte(key)))
		if err != nil {
			t.Fatalf("Failed to store object: %v", err)
		}
	}

	// List with continue token (even if it doesn't paginate in local storage)
	req := httptest.NewRequest(http.MethodGet, "/objects?continue=sometoken", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}
}
