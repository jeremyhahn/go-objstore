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
	return newRESTServerWithRolePermissions(t, map[string][]string{
		"reader": {adapters.ActionRead, adapters.ActionList},
	})
}

// newRESTServerWithRolePermissions builds a server that authenticates every
// request as the "reader" role and authorizes it against the given RBAC map.
func newRESTServerWithRolePermissions(t *testing.T, rolePermissions map[string][]string) *Server {
	t.Helper()
	config := DefaultServerConfig()
	config.Mode = gin.TestMode
	config.Authenticator = readerOnlyAuthenticator{}
	config.Authorizer = adapters.NewRBACAuthorizer(rolePermissions)
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

// newRESTServerDenyAll builds a server whose authorizer denies every action,
// for exercising the /metrics authorization default.
func newRESTServerDenyAll(t *testing.T, metricsPublic bool) *Server {
	t.Helper()
	config := DefaultServerConfig()
	config.Mode = gin.TestMode
	config.Authenticator = readerOnlyAuthenticator{}
	// RBAC map with no permissions for the authenticated role denies everything.
	config.Authorizer = adapters.NewRBACAuthorizer(map[string][]string{})
	config.MetricsPublic = metricsPublic
	return newRESTServer(t, config)
}

// TestMetricsRequiresAuthorizationByDefault verifies that /metrics is subject
// to authorization unless MetricsPublic is set.
func TestMetricsRequiresAuthorizationByDefault(t *testing.T) {
	router := newRESTServerDenyAll(t, false).Router()

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusForbidden {
		t.Errorf("GET /metrics with deny-all authorizer = %d, want %d", w.Code, http.StatusForbidden)
	}
}

// TestMetricsPublicFlagExemptsAuthorization verifies that MetricsPublic restores
// the unauthenticated scrape behavior.
func TestMetricsPublicFlagExemptsAuthorization(t *testing.T) {
	router := newRESTServerDenyAll(t, true).Router()

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("GET /metrics with MetricsPublic = %d, want %d", w.Code, http.StatusOK)
	}
}

// denyAllAuthenticator rejects every request, simulating a strict
// token-validating authenticator with no credentials supplied.
type denyAllAuthenticator struct{}

func (denyAllAuthenticator) AuthenticateHTTP(_ context.Context, _ *http.Request) (*adapters.Principal, error) {
	return nil, context.DeadlineExceeded // any error denies
}

func (denyAllAuthenticator) AuthenticateGRPC(_ context.Context, _ metadata.MD) (*adapters.Principal, error) {
	return nil, context.DeadlineExceeded
}

func (denyAllAuthenticator) AuthenticateMTLS(_ context.Context, _ *tls.ConnectionState) (*adapters.Principal, error) {
	return nil, context.DeadlineExceeded
}

// TestPublicPathsBypassAuthentication verifies that health (always) and
// /metrics (when MetricsPublic) remain reachable behind an authenticator
// that rejects credential-less requests.
func TestPublicPathsBypassAuthentication(t *testing.T) {
	config := DefaultServerConfig()
	config.Mode = gin.TestMode
	config.Authenticator = denyAllAuthenticator{}
	config.MetricsPublic = true
	router := newRESTServer(t, config).Router()

	for _, path := range []string{"/health", "/metrics"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Errorf("GET %s behind deny-all authenticator = %d, want 200", path, w.Code)
		}
	}

	// Non-public paths must still be denied.
	req := httptest.NewRequest(http.MethodGet, "/api/v1/objects/k", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("GET object behind deny-all authenticator = %d, want 401", w.Code)
	}
}

// TestIsPublicPath verifies the public-path matrix. Swagger is intentionally
// NOT public: it must require authentication.
func TestIsPublicPath(t *testing.T) {
	tests := []struct {
		path          string
		metricsPublic bool
		want          bool
	}{
		{"/health", false, true},
		{"/swagger/index.html", false, false},
		{"/metrics", false, false},
		{"/metrics", true, true},
		{"/api/v1/objects/key", false, false},
	}
	for _, tt := range tests {
		if got := isPublicPath(tt.path, tt.metricsPublic); got != tt.want {
			t.Errorf("isPublicPath(%q, %v) = %v, want %v", tt.path, tt.metricsPublic, got, tt.want)
		}
	}
}

// TestIsAuthzExemptPath verifies the authorization exemption matrix: all
// public paths are exempt, and swagger is additionally exempt from
// authorization (but not from authentication).
func TestIsAuthzExemptPath(t *testing.T) {
	tests := []struct {
		path          string
		metricsPublic bool
		want          bool
	}{
		{"/health", false, true},
		{"/swagger/index.html", false, true},
		{"/metrics", false, false},
		{"/metrics", true, true},
		{"/api/v1/objects/key", false, false},
	}
	for _, tt := range tests {
		if got := isAuthzExemptPath(tt.path, tt.metricsPublic); got != tt.want {
			t.Errorf("isAuthzExemptPath(%q, %v) = %v, want %v", tt.path, tt.metricsPublic, got, tt.want)
		}
	}
}

// TestSwaggerRequiresAuthentication verifies that /swagger/* is no longer
// exempt from authentication: an authenticator that rejects credential-less
// requests must deny access to the API documentation.
func TestSwaggerRequiresAuthentication(t *testing.T) {
	config := DefaultServerConfig()
	config.Mode = gin.TestMode
	config.Authenticator = denyAllAuthenticator{}
	router := newRESTServer(t, config).Router()

	req := httptest.NewRequest(http.MethodGet, "/swagger/index.html", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("GET /swagger/index.html behind deny-all authenticator = %d, want %d", w.Code, http.StatusUnauthorized)
	}
}

// TestSwaggerExemptFromAuthorization verifies that an authenticated principal
// may reach swagger regardless of RBAC permissions: swagger requires
// authentication but no specific authorization.
func TestSwaggerExemptFromAuthorization(t *testing.T) {
	router := newRESTServerDenyAll(t, false).Router()

	req := httptest.NewRequest(http.MethodGet, "/swagger/index.html", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code == http.StatusForbidden || w.Code == http.StatusUnauthorized {
		t.Errorf("GET /swagger/index.html for authenticated principal = %d, want neither 401 nor 403", w.Code)
	}
}

// TestRESTListVsReadSeparation verifies that listing the objects collection is
// authorized as ActionList while reading an object is authorized as
// ActionRead, so a principal holding only one of the two permissions cannot
// perform the other operation.
func TestRESTListVsReadSeparation(t *testing.T) {
	listPaths := []string{"/objects", "/api/v1/objects"}
	readPaths := []string{"/objects/some-key", "/api/v1/objects/some-key"}

	t.Run("list-only principal", func(t *testing.T) {
		router := newRESTServerWithRolePermissions(t, map[string][]string{
			"reader": {adapters.ActionList},
		}).Router()

		for _, path := range listPaths {
			req := httptest.NewRequest(http.MethodGet, path, nil)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
			if w.Code != http.StatusOK {
				t.Errorf("GET %s with list-only principal = %d, want %d", path, w.Code, http.StatusOK)
			}
		}

		for _, path := range readPaths {
			req := httptest.NewRequest(http.MethodGet, path, nil)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
			if w.Code != http.StatusForbidden {
				t.Errorf("GET %s with list-only principal = %d, want %d", path, w.Code, http.StatusForbidden)
			}
		}
	})

	t.Run("read-only principal", func(t *testing.T) {
		router := newRESTServerWithRolePermissions(t, map[string][]string{
			"reader": {adapters.ActionRead},
		}).Router()

		for _, path := range readPaths {
			// A missing key yields 404, but importantly NOT 403 — the
			// authorizer permitted the read action.
			req := httptest.NewRequest(http.MethodGet, path, nil)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
			if w.Code == http.StatusForbidden {
				t.Errorf("GET %s with read-only principal = 403, want read permitted", path)
			}
		}

		for _, path := range listPaths {
			req := httptest.NewRequest(http.MethodGet, path, nil)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
			if w.Code != http.StatusForbidden {
				t.Errorf("GET %s with read-only principal = %d, want %d", path, w.Code, http.StatusForbidden)
			}
		}
	})
}
