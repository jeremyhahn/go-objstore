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

package middleware

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
)

func TestRequestIDHTTPMiddleware(t *testing.T) {
	t.Run("generates and echoes an ID", func(t *testing.T) {
		var ctxID string
		h := RequestIDHTTPMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctxID = GetRequestIDFromContext(r.Context())
		}))

		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/objects/k", nil))

		header := w.Header().Get(RequestIDHeader)
		if header == "" {
			t.Fatal("X-Request-ID response header not set")
		}
		if ctxID != header {
			t.Errorf("context ID %q != header ID %q", ctxID, header)
		}
	})

	t.Run("echoes a valid inbound ID", func(t *testing.T) {
		h := RequestIDHTTPMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))

		req := httptest.NewRequest(http.MethodGet, "/objects/k", nil)
		req.Header.Set(RequestIDHeader, "client-id-123")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)

		if got := w.Header().Get(RequestIDHeader); got != "client-id-123" {
			t.Errorf("inbound ID not echoed, got %q", got)
		}
	})

	t.Run("replaces a malformed inbound ID", func(t *testing.T) {
		h := RequestIDHTTPMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))

		req := httptest.NewRequest(http.MethodGet, "/objects/k", nil)
		req.Header.Set(RequestIDHeader, "bad id with spaces\n")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)

		got := w.Header().Get(RequestIDHeader)
		if got == "" || got == "bad id with spaces\n" {
			t.Errorf("malformed inbound ID must be replaced, got %q", got)
		}
	})
}

func TestEnsureRequestID(t *testing.T) {
	ctx, id := EnsureRequestID(context.Background())
	if id == "" {
		t.Fatal("expected generated ID")
	}
	if got := GetRequestIDFromContext(ctx); got != id {
		t.Errorf("context ID %q != returned ID %q", got, id)
	}

	// Idempotent: an existing ID is preserved.
	ctx2, id2 := EnsureRequestID(ctx)
	if id2 != id {
		t.Errorf("existing ID replaced: %q != %q", id2, id)
	}
	if ctx2 != ctx {
		t.Error("context should be unchanged when an ID exists")
	}
}

func TestRateLimiterHTTPMiddleware(t *testing.T) {
	limiter := NewRateLimiter(&RateLimitConfig{
		RequestsPerSecond: 1,
		Burst:             1,
		PerIP:             true,
	}, adapters.NewDefaultLogger())
	defer limiter.Stop()

	h := limiter.HTTPMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	do := func(remote string) int {
		req := httptest.NewRequest(http.MethodGet, "/objects/k", nil)
		req.RemoteAddr = remote
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		return w.Code
	}

	if code := do("10.0.0.1:1111"); code != http.StatusOK {
		t.Fatalf("first request = %d, want 200", code)
	}
	if code := do("10.0.0.1:1111"); code != http.StatusTooManyRequests {
		t.Errorf("second request = %d, want 429", code)
	}
	// A different client has its own bucket.
	if code := do("10.0.0.2:2222"); code != http.StatusOK {
		t.Errorf("other client = %d, want 200", code)
	}
}
