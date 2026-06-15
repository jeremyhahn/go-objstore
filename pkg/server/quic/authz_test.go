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
	"context"
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"google.golang.org/grpc/metadata"
)

// readerAuthenticator authenticates every request as a principal holding only
// the "reader" role.
type readerAuthenticator struct{}

func (readerAuthenticator) AuthenticateHTTP(_ context.Context, _ *http.Request) (*adapters.Principal, error) {
	return &adapters.Principal{ID: "reader-1", Roles: []string{"reader"}}, nil
}

func (readerAuthenticator) AuthenticateGRPC(_ context.Context, _ metadata.MD) (*adapters.Principal, error) {
	return &adapters.Principal{ID: "reader-1", Roles: []string{"reader"}}, nil
}

func (readerAuthenticator) AuthenticateMTLS(_ context.Context, _ *tls.ConnectionState) (*adapters.Principal, error) {
	return &adapters.Principal{ID: "reader-1", Roles: []string{"reader"}}, nil
}

func newAuthzHandler(t *testing.T, auth adapters.Authenticator, authz adapters.Authorizer) *Handler {
	t.Helper()
	storage := newMockLifecycleStorage()
	initTestFacade(t, storage)
	handler, err := NewHandler("", 100*1024*1024, 30*time.Second, 30*time.Second, adapters.NewNoOpLogger(), auth, authz, nil)
	if err != nil {
		t.Fatalf("NewHandler failed: %v", err)
	}
	return handler
}

// TestQUICAuthorizationBackwardCompat verifies the default (NoOp) authorizer
// allows all operations.
func TestQUICAuthorizationBackwardCompat(t *testing.T) {
	handler := newAuthzHandler(t, adapters.NewNoOpAuthenticator(), adapters.NewNoOpAuthorizer())

	req := httptest.NewRequest(http.MethodPut, "/objects/k1", strings.NewReader("data"))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Errorf("default authorizer PUT = %d, want %d (body=%s)", w.Code, http.StatusCreated, w.Body.String())
	}
}

// TestQUICAuthorizationEnforced verifies a restrictive RBAC authorizer allows
// read but forbids write/delete/admin with HTTP 403.
func TestQUICAuthorizationEnforced(t *testing.T) {
	authz := adapters.NewRBACAuthorizer(map[string][]string{
		"reader": {adapters.ActionRead, adapters.ActionList},
	})
	handler := newAuthzHandler(t, readerAuthenticator{}, authz)

	t.Run("denied write", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPut, "/objects/k1", strings.NewReader("data"))
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		if w.Code != http.StatusForbidden {
			t.Errorf("PUT = %d, want 403", w.Code)
		}
	})

	t.Run("denied delete", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodDelete, "/objects/k1", nil)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		if w.Code != http.StatusForbidden {
			t.Errorf("DELETE = %d, want 403", w.Code)
		}
	})

	t.Run("denied admin policies", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/policies", nil)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		if w.Code != http.StatusForbidden {
			t.Errorf("GET /policies = %d, want 403", w.Code)
		}
	})

	t.Run("allowed read not forbidden", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/objects/k1", nil)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		if w.Code == http.StatusForbidden {
			t.Errorf("GET should be allowed for reader, got 403")
		}
	})

	t.Run("health public", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/health", nil)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		if w.Code == http.StatusForbidden {
			t.Errorf("health must remain public, got 403")
		}
	})
}
