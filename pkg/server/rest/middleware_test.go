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
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/jeremyhahn/go-objstore/pkg/adapters"
)

func TestCORSMiddleware(t *testing.T) {
	router := gin.New()
	router.Use(CORSMiddleware())
	router.GET("/test", func(c *gin.Context) {
		c.String(http.StatusOK, "OK")
	})

	tests := []struct {
		name           string
		method         string
		wantStatusCode int
		checkHeaders   map[string]string
	}{
		{
			name:           "OPTIONS request",
			method:         "OPTIONS",
			wantStatusCode: http.StatusNoContent,
			checkHeaders: map[string]string{
				"Access-Control-Allow-Origin":  "*",
				"Access-Control-Allow-Methods": "POST, OPTIONS, GET, PUT, DELETE, HEAD",
			},
		},
		{
			name:           "GET request",
			method:         "GET",
			wantStatusCode: http.StatusOK,
			checkHeaders: map[string]string{
				"Access-Control-Allow-Origin": "*",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, "/test", nil)
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)

			if w.Code != tt.wantStatusCode {
				t.Errorf("CORSMiddleware() status = %v, want %v", w.Code, tt.wantStatusCode)
			}

			for header, expectedValue := range tt.checkHeaders {
				actualValue := w.Header().Get(header)
				if !contains(actualValue, expectedValue) {
					t.Errorf("CORSMiddleware() header %s = %v, want to contain %v", header, actualValue, expectedValue)
				}
			}
		})
	}
}

func TestLoggingMiddleware(t *testing.T) {
	router := gin.New()
	logger := adapters.NewNoOpLogger()
	router.Use(LoggingMiddleware(logger))
	router.GET("/test", func(c *gin.Context) {
		c.String(http.StatusOK, "OK")
	})

	req := httptest.NewRequest("GET", "/test", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("LoggingMiddleware() status = %v, want %v", w.Code, http.StatusOK)
	}
}

func TestErrorHandlingMiddleware(t *testing.T) {
	router := gin.New()
	router.Use(ErrorHandlingMiddleware())

	// Test panic recovery
	router.GET("/panic", func(c *gin.Context) {
		panic("test panic")
	})

	// Test normal operation
	router.GET("/normal", func(c *gin.Context) {
		c.String(http.StatusOK, "OK")
	})

	tests := []struct {
		name           string
		path           string
		wantStatusCode int
	}{
		{
			name:           "panic recovery",
			path:           "/panic",
			wantStatusCode: http.StatusInternalServerError,
		},
		{
			name:           "normal operation",
			path:           "/normal",
			wantStatusCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", tt.path, nil)
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)

			if w.Code != tt.wantStatusCode {
				t.Errorf("ErrorHandlingMiddleware() status = %v, want %v", w.Code, tt.wantStatusCode)
			}
		})
	}
}

func TestRequestSizeLimitMiddleware(t *testing.T) {
	tests := []struct {
		name           string
		maxSize        int64
		bodySize       int
		method         string
		wantStatusCode int
	}{
		{
			name:           "within limit PUT",
			maxSize:        1024,
			bodySize:       512,
			method:         "PUT",
			wantStatusCode: http.StatusOK,
		},
		{
			name:           "exceeds limit PUT",
			maxSize:        100,
			bodySize:       200,
			method:         "PUT",
			wantStatusCode: http.StatusRequestEntityTooLarge,
		},
		{
			name:           "GET request not checked",
			maxSize:        100,
			bodySize:       0,
			method:         "GET",
			wantStatusCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			router := gin.New()
			router.Use(RequestSizeLimitMiddleware(tt.maxSize))
			router.PUT("/test", func(c *gin.Context) {
				c.String(http.StatusOK, "OK")
			})
			router.GET("/test", func(c *gin.Context) {
				c.String(http.StatusOK, "OK")
			})

			body := strings.Repeat("a", tt.bodySize)
			req := httptest.NewRequest(tt.method, "/test", strings.NewReader(body))
			req.Header.Set("Content-Length", string(rune(tt.bodySize)))
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)

			if w.Code != tt.wantStatusCode {
				t.Errorf("RequestSizeLimitMiddleware() status = %v, want %v", w.Code, tt.wantStatusCode)
			}
		})
	}
}

func TestRequestSizeLimitMiddlewarePOST(t *testing.T) {
	router := gin.New()
	router.Use(RequestSizeLimitMiddleware(100))
	router.POST("/test", func(c *gin.Context) {
		c.String(http.StatusOK, "OK")
	})

	body := strings.Repeat("a", 200)
	req := httptest.NewRequest("POST", "/test", strings.NewReader(body))
	req.Header.Set("Content-Length", "200")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("RequestSizeLimitMiddleware() POST status = %v, want %v", w.Code, http.StatusRequestEntityTooLarge)
	}
}
