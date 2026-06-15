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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

// newCORSTestRouter builds a minimal gin engine with the CORS middleware and a
// trivial GET handler, used to exercise CORSMiddleware in isolation.
func newCORSTestRouter(allowedOrigins []string) *gin.Engine {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(CORSMiddleware(allowedOrigins))
	router.GET("/", func(c *gin.Context) {
		c.Status(http.StatusOK)
	})
	return router
}

func TestCORSMiddleware_DefaultAllowsAllWithoutCredentials(t *testing.T) {
	router := newCORSTestRouter(nil)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Origin", "https://example.com")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Fatalf("expected ACAO '*', got %q", got)
	}
	if got := rec.Header().Get("Access-Control-Allow-Credentials"); got != "" {
		t.Fatalf("expected no credentials header in wildcard mode, got %q", got)
	}
	if got := rec.Header().Get("Access-Control-Allow-Methods"); got == "" {
		t.Fatal("expected Allow-Methods header to be set")
	}
	if got := rec.Header().Get("Access-Control-Expose-Headers"); got == "" {
		t.Fatal("expected Expose-Headers header to be set")
	}
}

func TestCORSMiddleware_ExplicitWildcard(t *testing.T) {
	router := newCORSTestRouter([]string{"*"})

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Origin", "https://example.com")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Fatalf("expected ACAO '*', got %q", got)
	}
	if got := rec.Header().Get("Access-Control-Allow-Credentials"); got != "" {
		t.Fatalf("expected no credentials header for explicit wildcard, got %q", got)
	}
}

func TestCORSMiddleware_AllowlistHitEchoesOrigin(t *testing.T) {
	router := newCORSTestRouter([]string{"https://app.example.com", "https://admin.example.com"})

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Origin", "https://admin.example.com")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "https://admin.example.com" {
		t.Fatalf("expected echoed origin, got %q", got)
	}
	if got := rec.Header().Get("Access-Control-Allow-Credentials"); got != "true" {
		t.Fatalf("expected credentials header for allowlisted origin, got %q", got)
	}
	if got := rec.Header().Get("Vary"); got != "Origin" {
		t.Fatalf("expected Vary: Origin, got %q", got)
	}
}

func TestCORSMiddleware_AllowlistMissNoACAO(t *testing.T) {
	router := newCORSTestRouter([]string{"https://app.example.com"})

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Origin", "https://evil.example.com")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Fatalf("expected no ACAO header for non-allowlisted origin, got %q", got)
	}
	if got := rec.Header().Get("Access-Control-Allow-Credentials"); got != "" {
		t.Fatalf("expected no credentials header for non-allowlisted origin, got %q", got)
	}
}

func TestCORSMiddleware_AllowlistNoOriginHeader(t *testing.T) {
	router := newCORSTestRouter([]string{"https://app.example.com"})

	// No Origin header present: nothing should be echoed.
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Fatalf("expected no ACAO header when no Origin sent, got %q", got)
	}
}

func TestCORSMiddleware_OptionsPreflight(t *testing.T) {
	router := newCORSTestRouter(nil)

	req := httptest.NewRequest(http.MethodOptions, "/", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected 204 for OPTIONS preflight, got %d", rec.Code)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Fatalf("expected ACAO '*' on preflight, got %q", got)
	}
}

func TestOriginAllowed(t *testing.T) {
	allow := []string{"https://a.com", "https://b.com"}
	cases := []struct {
		origin string
		want   bool
	}{
		{"https://a.com", true},
		{"https://b.com", true},
		{"https://c.com", false},
		{"", false},
	}
	for _, tc := range cases {
		if got := originAllowed(tc.origin, allow); got != tc.want {
			t.Errorf("originAllowed(%q) = %v, want %v", tc.origin, got, tc.want)
		}
	}
}
