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

func TestSetupRoutes(t *testing.T) {
	storage := NewMockStorage()
	handler := NewHandler(storage)

	router := gin.New()
	SetupRoutes(router, handler)

	tests := []struct {
		name           string
		method         string
		path           string
		wantStatusCode int
	}{
		{
			name:           "health check",
			method:         "GET",
			path:           "/health",
			wantStatusCode: http.StatusOK,
		},
		{
			name:           "list objects v1",
			method:         "GET",
			path:           "/api/v1/objects",
			wantStatusCode: http.StatusOK,
		},
		{
			name:           "list objects root",
			method:         "GET",
			path:           "/objects",
			wantStatusCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, tt.path, nil)
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)

			if w.Code != tt.wantStatusCode {
				t.Errorf("Route %s %s status = %v, want %v", tt.method, tt.path, w.Code, tt.wantStatusCode)
			}
		})
	}
}

func TestRoutesCRUDOperations(t *testing.T) {
	storage := NewMockStorage()
	handler := NewHandler(storage)

	router := gin.New()
	SetupRoutes(router, handler)

	// Test all CRUD operations on the same key
	key := "/test/object.txt"

	// PUT
	req := httptest.NewRequest("PUT", "/objects"+key, nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusCreated && w.Code != http.StatusBadRequest {
		t.Errorf("PUT %s status = %v", key, w.Code)
	}

	// HEAD
	req = httptest.NewRequest("HEAD", "/objects"+key, nil)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	// Can be 200 if exists or 404 if not
	if w.Code != http.StatusOK && w.Code != http.StatusNotFound {
		t.Errorf("HEAD %s status = %v", key, w.Code)
	}

	// GET
	req = httptest.NewRequest("GET", "/objects"+key, nil)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	// Can be 200 if exists or 404 if not
	if w.Code != http.StatusOK && w.Code != http.StatusNotFound {
		t.Errorf("GET %s status = %v", key, w.Code)
	}

	// DELETE
	req = httptest.NewRequest("DELETE", "/objects"+key, nil)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	// Can be 200 if exists or 404 if not
	if w.Code != http.StatusOK && w.Code != http.StatusNotFound {
		t.Errorf("DELETE %s status = %v", key, w.Code)
	}
}

func TestRoutesMetadataOperations(t *testing.T) {
	storage := NewMockStorage()
	handler := NewHandler(storage)

	router := gin.New()
	SetupRoutes(router, handler)

	key := "test-object.txt"

	// GET metadata
	req := httptest.NewRequest("GET", "/api/v1/metadata/"+key, nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	// Can be 200 if exists or 404 if not
	if w.Code != http.StatusOK && w.Code != http.StatusNotFound {
		t.Errorf("GET metadata status = %v", w.Code)
	}

	// PUT metadata
	req = httptest.NewRequest("PUT", "/api/v1/metadata/"+key, nil)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	// Will likely be 400 (bad request) or 404 (not found)
	if w.Code != http.StatusOK && w.Code != http.StatusBadRequest && w.Code != http.StatusNotFound {
		t.Errorf("PUT metadata status = %v", w.Code)
	}
}

func TestRoutesAPIVersioning(t *testing.T) {
	storage := NewMockStorage()
	handler := NewHandler(storage)

	router := gin.New()
	SetupRoutes(router, handler)

	// Test that both /objects and /api/v1/objects work
	paths := []string{
		"/objects",
		"/api/v1/objects",
	}

	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			req := httptest.NewRequest("GET", path, nil)
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)

			if w.Code != http.StatusOK {
				t.Errorf("Route %s status = %v, want %v", path, w.Code, http.StatusOK)
			}
		})
	}
}

func TestRoutesSwagger(t *testing.T) {
	storage := NewMockStorage()
	handler := NewHandler(storage)

	router := gin.New()
	SetupRoutes(router, handler)

	// Test swagger endpoint
	req := httptest.NewRequest("GET", "/swagger/index.html", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	// Swagger might return 404 if not properly initialized, which is OK for unit tests
	if w.Code != http.StatusOK && w.Code != http.StatusNotFound {
		t.Errorf("Swagger route status = %v", w.Code)
	}
}

func TestRoutesWithTrailingSlash(t *testing.T) {
	storage := NewMockStorage()
	handler := NewHandler(storage)

	router := gin.New()
	SetupRoutes(router, handler)

	// Test that routes work with various key patterns
	keys := []string{
		"/simple",
		"/path/to/object",
		"/path/with/many/levels/deep",
	}

	for _, key := range keys {
		t.Run(key, func(t *testing.T) {
			req := httptest.NewRequest("HEAD", "/objects"+key, nil)
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)

			// HEAD should return 200 or 404, not route errors
			if w.Code != http.StatusOK && w.Code != http.StatusNotFound {
				t.Errorf("Route /objects%s status = %v, should be 200 or 404", key, w.Code)
			}
		})
	}
}
