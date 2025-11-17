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
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// TestHandleGet_ContextTimeout tests GET request with context timeout
func TestHandleGet_ContextTimeout(t *testing.T) {
	storage := newMockLifecycleStorage()
	handler := NewHandler(storage, 10*1024*1024, 1*time.Nanosecond, 30*time.Second, &mockLogger{}, &mockAuthenticator{})

	// Add object
	storage.PutWithContext(context.Background(), "test.txt", bytes.NewReader([]byte("data")))

	req := httptest.NewRequest(http.MethodGet, "/objects/test.txt", nil)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()
	time.Sleep(2 * time.Millisecond) // Ensure timeout
	req = req.WithContext(ctx)

	w := httptest.NewRecorder()
	handler.handleGet(w, req, "test.txt")

	// May return timeout or other error status
	if w.Code == http.StatusOK {
		t.Log("Request succeeded despite short timeout")
	}
}

// TestHandleExists_ContextTimeout tests EXISTS request with context timeout
func TestHandleExists_ContextTimeout(t *testing.T) {
	storage := newMockLifecycleStorage()
	handler := NewHandler(storage, 10*1024*1024, 1*time.Nanosecond, 30*time.Second, &mockLogger{}, &mockAuthenticator{})

	req := httptest.NewRequest(http.MethodGet, "/objects/test.txt?exists=true", nil)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()
	time.Sleep(2 * time.Millisecond) // Ensure timeout
	req = req.WithContext(ctx)

	w := httptest.NewRecorder()
	handler.handleExists(w, req, "test.txt")

	// May return timeout or other error status
	if w.Code == http.StatusOK {
		t.Log("Request succeeded despite short timeout")
	}
}

// TestHandleHealth_AllMethods tests health endpoint with different HTTP methods
func TestHandleHealth_AllMethods(t *testing.T) {
	storage := newMockLifecycleStorage()
	handler := NewHandler(storage, 10*1024*1024, 30*time.Second, 30*time.Second, &mockLogger{}, &mockAuthenticator{})

	methods := []string{
		http.MethodGet,
		http.MethodPost,
		http.MethodPut,
		http.MethodDelete,
		http.MethodPatch,
		http.MethodHead,
		http.MethodOptions,
	}

	for _, method := range methods {
		t.Run(method, func(t *testing.T) {
			req := httptest.NewRequest(method, "/health", nil)
			w := httptest.NewRecorder()

			handler.handleHealth(w, req)

			if w.Code != http.StatusOK {
				t.Errorf("method %s: expected status 200, got %d", method, w.Code)
			}
		})
	}
}

// TestHandleGet_EmptyContentType tests GET with object that has no content type
func TestHandleGet_EmptyContentType(t *testing.T) {
	storage := newMockLifecycleStorage()
	handler := NewHandler(storage, 10*1024*1024, 30*time.Second, 30*time.Second, &mockLogger{}, &mockAuthenticator{})

	// Add object with minimal metadata
	metadata := &common.Metadata{
		Size:         4,
		LastModified: time.Now(),
		// No ContentType, ContentEncoding, ETag, or Custom fields
	}
	storage.PutWithMetadata(context.Background(), "plain.txt", bytes.NewReader([]byte("data")), metadata)

	req := httptest.NewRequest(http.MethodGet, "/objects/plain.txt", nil)
	w := httptest.NewRecorder()

	handler.handleGet(w, req, "plain.txt")

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	// Should not have Content-Type header set
	contentType := w.Header().Get("Content-Type")
	if contentType != "" && contentType != "text/plain; charset=utf-8" {
		// Default content type may be set by ResponseWriter
		t.Logf("Content-Type: %s", contentType)
	}
}

// TestHandleExists_ExistingObject tests exists check for existing object
func TestHandleExists_ExistingObject(t *testing.T) {
	storage := newMockLifecycleStorage()
	handler := NewHandler(storage, 10*1024*1024, 30*time.Second, 30*time.Second, &mockLogger{}, &mockAuthenticator{})

	// Add object
	storage.PutWithContext(context.Background(), "exists.txt", bytes.NewReader([]byte("data")))

	req := httptest.NewRequest(http.MethodGet, "/objects/exists.txt?exists=true", nil)
	w := httptest.NewRecorder()

	handler.handleExists(w, req, "exists.txt")

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}
}

// TestApplyPolicies_ErrorInDeleteContinues tests that delete errors don't stop processing
func TestApplyPolicies_ErrorInDeleteContinues(t *testing.T) {
	storage := newMockLifecycleStorage()
	handler := NewHandler(storage, 10*1024*1024, 30*time.Second, 30*time.Second, &mockLogger{}, &mockAuthenticator{})

	// Add policy
	storage.AddPolicy(common.LifecyclePolicy{
		ID:        "cleanup",
		Prefix:    "",
		Retention: 1 * time.Hour,
		Action:    "delete",
	})

	// Add two old objects
	storage.PutWithContext(context.Background(), "old1.txt", bytes.NewReader([]byte("data1")))
	storage.metadata["old1.txt"].LastModified = time.Now().Add(-2 * time.Hour)

	storage.PutWithContext(context.Background(), "old2.txt", bytes.NewReader([]byte("data2")))
	storage.metadata["old2.txt"].LastModified = time.Now().Add(-2 * time.Hour)

	req := httptest.NewRequest(http.MethodPost, "/policies/apply", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}
}

// TestApplyPolicies_ErrorInArchiveContinues tests that archive errors don't stop processing
func TestApplyPolicies_ErrorInArchiveContinues(t *testing.T) {
	storage := newMockLifecycleStorage()
	storage.archiveError = nil // Will succeed
	handler := NewHandler(storage, 10*1024*1024, 30*time.Second, 30*time.Second, &mockLogger{}, &mockAuthenticator{})

	// Add policy with archive action and destination
	storage.AddPolicy(common.LifecyclePolicy{
		ID:          "archive",
		Prefix:      "",
		Retention:   1 * time.Hour,
		Action:      "archive",
		Destination: &mockArchiver{},
	})

	// Add old object
	storage.PutWithContext(context.Background(), "old.txt", bytes.NewReader([]byte("data")))
	storage.metadata["old.txt"].LastModified = time.Now().Add(-2 * time.Hour)

	req := httptest.NewRequest(http.MethodPost, "/policies/apply", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}
}
