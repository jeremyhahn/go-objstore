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
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/local"
)

// TestHandlePutNoContentLengthIsBounded: without Content-Length the LimitReader stores only maxRequestBodySize bytes (201).
func TestHandlePutNoContentLengthIsBounded(t *testing.T) {
	const maxBody int64 = 64

	storage := local.New()
	if err := storage.Configure(map[string]string{"path": t.TempDir()}, noCreds); err != nil {
		t.Fatalf("Failed to configure storage: %v", err)
	}
	initTestFacade(t, storage)

	logger := adapters.NewNoOpLogger()
	auth := adapters.NewNoOpAuthenticator()
	handler, err := NewHandler("", maxBody, 30*time.Second, 30*time.Second, logger, auth, nil, nil)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}

	// Body is far larger than the limit.
	body := bytes.Repeat([]byte("A"), int(maxBody)*10)

	req := httptest.NewRequest(http.MethodPut, "/objects/chunked-key", bytes.NewReader(body))
	// Simulate an unknown-length (chunked) request: no usable Content-Length.
	req.ContentLength = -1

	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	// The Content-Length pre-check is bypassed for unknown length, so the
	// request succeeds; the LimitReader is what protects us.
	if w.Code != http.StatusCreated {
		t.Fatalf("expected status %d, got %d", http.StatusCreated, w.Code)
	}

	// Read the stored object back and assert it was capped at maxBody bytes,
	// proving the LimitReader bounded the read.
	getReq := httptest.NewRequest(http.MethodGet, "/objects/chunked-key", nil)
	getW := httptest.NewRecorder()
	handler.ServeHTTP(getW, getReq)

	if getW.Code != http.StatusOK {
		t.Fatalf("GET expected status %d, got %d", http.StatusOK, getW.Code)
	}

	stored, err := io.ReadAll(getW.Body)
	if err != nil {
		t.Fatalf("reading stored object failed: %v", err)
	}

	if int64(len(stored)) != maxBody {
		t.Fatalf("stored object not bounded by limit: got %d bytes, want exactly %d", len(stored), maxBody)
	}
}
