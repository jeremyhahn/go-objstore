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
	"context"
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"google.golang.org/grpc/metadata"
)

// readerOnlyAuthenticator authenticates every request as a principal holding
// only the "reader" role, for exercising authorization enforcement.
type readerOnlyAuthenticator struct{}

func (readerOnlyAuthenticator) AuthenticateHTTP(_ context.Context, _ *http.Request) (*adapters.Principal, error) {
	return &adapters.Principal{ID: "reader-1", Name: "Reader", Type: "user", Roles: []string{"reader"}}, nil
}

func (readerOnlyAuthenticator) AuthenticateGRPC(_ context.Context, _ metadata.MD) (*adapters.Principal, error) {
	return &adapters.Principal{ID: "reader-1", Roles: []string{"reader"}}, nil
}

func (readerOnlyAuthenticator) AuthenticateMTLS(_ context.Context, _ *tls.ConnectionState) (*adapters.Principal, error) {
	return &adapters.Principal{ID: "reader-1", Roles: []string{"reader"}}, nil
}

func newRESTServer(t *testing.T, config *ServerConfig) *Server {
	t.Helper()
	storage := NewMockStorage()
	initTestFacade(t, storage)
	server, err := NewServer(storage, config)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}
	return server
}

func newRESTServerWithRBAC(t *testing.T) *Server {
	t.Helper()
	config := DefaultServerConfig()
	config.Mode = gin.TestMode
	config.Authenticator = readerOnlyAuthenticator{}
	config.Authorizer = adapters.NewRBACAuthorizer(map[string][]string{
		"reader": {adapters.ActionRead, adapters.ActionList},
	})
	return newRESTServer(t, config)
}

// TestRESTAuthorizationBackwardCompat verifies that the default (NoOp) authorizer
// allows all operations, preserving prior behavior.
func TestRESTAuthorizationBackwardCompat(t *testing.T) {
	config := DefaultServerConfig()
	config.Mode = gin.TestMode
	server := newRESTServer(t, config)
	router := server.Router()

	// PUT (write) must succeed under the allow-all default.
	req := httptest.NewRequest(http.MethodPut, "/api/v1/objects/backcompat-key", strings.NewReader("data"))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Errorf("default authorizer PUT = %d, want %d (body=%s)", w.Code, http.StatusCreated, w.Body.String())
	}
}

// TestRESTAuthorizationEnforced verifies a restrictive RBAC authorizer allows
// permitted actions and forbids others with HTTP 403.
func TestRESTAuthorizationEnforced(t *testing.T) {
	server := newRESTServerWithRBAC(t)
	router := server.Router()

	t.Run("allowed read", func(t *testing.T) {
		// GET on a missing key returns 404, but importantly NOT 403 — the
		// authorizer permitted the read action.
		req := httptest.NewRequest(http.MethodGet, "/api/v1/objects/some-key", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code == http.StatusForbidden {
			t.Errorf("read should be allowed for reader role, got 403")
		}
	})

	t.Run("denied write", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPut, "/api/v1/objects/some-key", strings.NewReader("data"))
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != http.StatusForbidden {
			t.Errorf("write should be forbidden for reader role, got %d", w.Code)
		}
	})

	t.Run("denied delete", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodDelete, "/api/v1/objects/some-key", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != http.StatusForbidden {
			t.Errorf("delete should be forbidden for reader role, got %d", w.Code)
		}
	})

	t.Run("denied admin (policies)", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/policies", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != http.StatusForbidden {
			t.Errorf("policies (admin) should be forbidden for reader role, got %d", w.Code)
		}
	})

	t.Run("health remains public", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/health", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code == http.StatusForbidden {
			t.Errorf("health must remain public, got 403")
		}
	})
}
