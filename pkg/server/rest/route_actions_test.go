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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/auditlog"
)

// recordingAuthorizer records the last (action, resource) and refuses every
// request, so no handler runs.
type recordingAuthorizer struct {
	called           bool
	action, resource string
}

func (a *recordingAuthorizer) Authorize(_ context.Context, _ *adapters.Principal, action, resource string) error {
	a.called, a.action, a.resource = true, action, resource
	return adapters.ErrUnauthorized
}

func routeActionRouter(authz adapters.Authorizer) *gin.Engine {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set(principalContextKey, &adapters.Principal{ID: "p", Name: "p"})
		c.Next()
	})
	router.Use(AuthorizationMiddleware(authz, adapters.NewNoOpLogger(), auditlog.NewNoOpAuditLogger(), false))
	SetupRoutes(router, &Handler{})
	return router
}

// TestRouteActions_EveryRouteHasAnAction keeps routeActions in step with
// SetupRoutes: a route added without an action would be refused.
func TestRouteActions_EveryRouteHasAnAction(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	SetupRoutes(router, &Handler{})
	for _, r := range router.Routes() {
		if r.Path == "/health" || r.Path == "/swagger/*any" {
			continue // exempt from authorization
		}
		key := r.Method + " " + r.Path
		if len(r.Path) >= len(apiV1Prefix) && r.Path[:len(apiV1Prefix)] == apiV1Prefix {
			key = r.Method + " " + r.Path[len(apiV1Prefix):]
		}
		if _, ok := routeActions[key]; !ok {
			t.Errorf("route %s %s has no action in routeActions", r.Method, r.Path)
		}
	}
}

func TestRouteActions_FromRouteNotKey(t *testing.T) {
	cases := []struct {
		method, path, action, resource string
	}{
		{http.MethodPut, "/objects/a/exists/b", adapters.ActionWrite, "a/exists/b"},
		{http.MethodDelete, "/api/v1/objects/snap/metadata.json", adapters.ActionDelete, "snap/metadata.json"},
		{http.MethodPut, "/api/v1/objects/a/metadata/b", adapters.ActionWrite, "a/metadata/b"},
		{http.MethodGet, "/objects/a/archive/b", adapters.ActionRead, "a/archive/b"},
		{http.MethodPut, "/objects/a/policies/b", adapters.ActionWrite, "a/policies/b"},
		{http.MethodPut, "/api/v1/objects/a/replication/b", adapters.ActionWrite, "a/replication/b"},
		{http.MethodGet, "/api/v1/objects", adapters.ActionList, ""},
		{http.MethodHead, "/exists/a/b", adapters.ActionRead, "a/b"},
		{http.MethodGet, "/api/v1/metadata/a/objects/b", adapters.ActionRead, "a/objects/b"},
		{http.MethodPut, "/metadata/a/b", adapters.ActionWrite, "a/b"},
		{http.MethodPost, "/api/v1/archive", adapters.ActionAdmin, adapters.ResourcePolicy},
		{http.MethodDelete, "/policies/x", adapters.ActionAdmin, adapters.ResourcePolicy},
		{http.MethodPost, "/api/v1/replication/trigger", adapters.ActionAdmin, adapters.ResourceReplication},
		{http.MethodGet, "/metrics", adapters.ActionRead, ""},
	}
	for _, tc := range cases {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			authz := &recordingAuthorizer{}
			w := httptest.NewRecorder()
			routeActionRouter(authz).ServeHTTP(w, httptest.NewRequest(tc.method, tc.path, nil))
			if !authz.called {
				t.Fatalf("authorizer not called (status %d)", w.Code)
			}
			if authz.action != tc.action || authz.resource != tc.resource {
				t.Fatalf("authorized as (%q, %q), want (%q, %q)", authz.action, authz.resource, tc.action, tc.resource)
			}
			if w.Code != http.StatusForbidden {
				t.Fatalf("status = %d, want 403", w.Code)
			}
		})
	}
}

func TestRouteActions_UnmatchedRouteRefused(t *testing.T) {
	for _, tc := range []struct{ method, path string }{
		{http.MethodGet, "/nope"},
		{http.MethodPost, "/objects/a"},
		{http.MethodDelete, "/api/v1/metadata/a"},
	} {
		authz := &recordingAuthorizer{}
		w := httptest.NewRecorder()
		routeActionRouter(authz).ServeHTTP(w, httptest.NewRequest(tc.method, tc.path, nil))
		if authz.called {
			t.Errorf("%s %s: authorizer called with (%q, %q), want refusal before it", tc.method, tc.path, authz.action, authz.resource)
		}
		if w.Code != http.StatusForbidden {
			t.Errorf("%s %s: status = %d, want 403", tc.method, tc.path, w.Code)
		}
	}
}
