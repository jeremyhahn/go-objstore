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
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/audit"
	"google.golang.org/grpc/metadata"
)

// MockAuthenticator for testing authentication
type MockAuthenticator struct {
	shouldFail bool
	principal  adapters.Principal
}

func (m *MockAuthenticator) AuthenticateHTTP(ctx context.Context, r *http.Request) (*adapters.Principal, error) {
	if m.shouldFail {
		return nil, errors.New("authentication failed")
	}
	return &m.principal, nil
}

func (m *MockAuthenticator) AuthenticateGRPC(ctx context.Context, md metadata.MD) (*adapters.Principal, error) {
	return &adapters.Principal{}, nil
}

func (m *MockAuthenticator) AuthenticateMTLS(ctx context.Context, state *tls.ConnectionState) (*adapters.Principal, error) {
	return &adapters.Principal{}, nil
}

func (m *MockAuthenticator) ValidatePermission(ctx context.Context, principal *adapters.Principal, resource, action string) error {
	return nil
}

// Test AuthenticationMiddleware with successful authentication
func TestAuthenticationMiddlewareSuccess(t *testing.T) {
	router := gin.New()
	logger := adapters.NewNoOpLogger()
	auditLogger := audit.NewNoOpAuditLogger()
	authenticator := &MockAuthenticator{
		shouldFail: false,
		principal: adapters.Principal{
			ID:   "user123",
			Name: "testuser",
		},
	}

	router.Use(AuthenticationMiddleware(authenticator, logger, auditLogger))
	router.GET("/test", func(c *gin.Context) {
		// Check that principal is set in context
		principalVal, exists := c.Get("principal")
		if !exists {
			t.Error("Principal should be set in context")
		}

		// Middleware stores a *Principal (pointer)
		principal, ok := principalVal.(*adapters.Principal)
		if !ok {
			t.Errorf("Principal should be of type *adapters.Principal, got %T", principalVal)
			c.String(http.StatusOK, "OK")
			return
		}

		if principal.ID != "user123" {
			t.Errorf("Principal ID = %v, want user123", principal.ID)
		}

		if principal.Name != "testuser" {
			t.Errorf("Principal Name = %v, want testuser", principal.Name)
		}

		c.String(http.StatusOK, "OK")
	})

	req := httptest.NewRequest("GET", "/test", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("AuthenticationMiddleware() with valid auth status = %v, want %v", w.Code, http.StatusOK)
	}
}

// Test AuthenticationMiddleware with failed authentication
func TestAuthenticationMiddlewareFailed(t *testing.T) {
	router := gin.New()
	logger := adapters.NewNoOpLogger()
	auditLogger := audit.NewNoOpAuditLogger()
	authenticator := &MockAuthenticator{
		shouldFail: true,
	}

	router.Use(AuthenticationMiddleware(authenticator, logger, auditLogger))
	router.GET("/test", func(c *gin.Context) {
		t.Error("Handler should not be called after auth failure")
		c.String(http.StatusOK, "OK")
	})

	req := httptest.NewRequest("GET", "/test", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("AuthenticationMiddleware() with failed auth status = %v, want %v", w.Code, http.StatusUnauthorized)
	}
}

// Test AuthenticationMiddleware with nil audit logger
func TestAuthenticationMiddlewareNilAuditLogger(t *testing.T) {
	router := gin.New()
	logger := adapters.NewNoOpLogger()
	authenticator := &MockAuthenticator{
		shouldFail: false,
		principal: adapters.Principal{
			ID:   "user123",
			Name: "testuser",
		},
	}

	router.Use(AuthenticationMiddleware(authenticator, logger, nil))
	router.GET("/test", func(c *gin.Context) {
		c.String(http.StatusOK, "OK")
	})

	req := httptest.NewRequest("GET", "/test", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("AuthenticationMiddleware() with nil audit logger status = %v, want %v", w.Code, http.StatusOK)
	}
}

// Test AuthenticationMiddleware auth failure with nil audit logger
func TestAuthenticationMiddlewareFailedNilAuditLogger(t *testing.T) {
	router := gin.New()
	logger := adapters.NewNoOpLogger()
	authenticator := &MockAuthenticator{
		shouldFail: true,
	}

	router.Use(AuthenticationMiddleware(authenticator, logger, nil))
	router.GET("/test", func(c *gin.Context) {
		c.String(http.StatusOK, "OK")
	})

	req := httptest.NewRequest("GET", "/test", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("AuthenticationMiddleware() failed with nil audit logger status = %v, want %v", w.Code, http.StatusUnauthorized)
	}
}

// Test LoggingMiddleware with 4xx status code
func TestLoggingMiddleware4xxStatus(t *testing.T) {
	router := gin.New()
	logger := adapters.NewNoOpLogger()
	router.Use(LoggingMiddleware(logger))
	router.GET("/test", func(c *gin.Context) {
		c.String(http.StatusBadRequest, "Bad Request")
	})

	req := httptest.NewRequest("GET", "/test", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("LoggingMiddleware() 4xx status = %v, want %v", w.Code, http.StatusBadRequest)
	}
}

// Test LoggingMiddleware with 5xx status code
func TestLoggingMiddleware5xxStatus(t *testing.T) {
	router := gin.New()
	logger := adapters.NewNoOpLogger()
	router.Use(LoggingMiddleware(logger))
	router.GET("/test", func(c *gin.Context) {
		c.String(http.StatusInternalServerError, "Internal Server Error")
	})

	req := httptest.NewRequest("GET", "/test", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("LoggingMiddleware() 5xx status = %v, want %v", w.Code, http.StatusInternalServerError)
	}
}

// Test LoggingMiddleware with 2xx status code
func TestLoggingMiddleware2xxStatus(t *testing.T) {
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
		t.Errorf("LoggingMiddleware() 2xx status = %v, want %v", w.Code, http.StatusOK)
	}
}

// Test LoggingMiddleware with 3xx status code
func TestLoggingMiddleware3xxStatus(t *testing.T) {
	router := gin.New()
	logger := adapters.NewNoOpLogger()
	router.Use(LoggingMiddleware(logger))
	router.GET("/test", func(c *gin.Context) {
		c.Redirect(http.StatusMovedPermanently, "/other")
	})

	req := httptest.NewRequest("GET", "/test", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusMovedPermanently {
		t.Errorf("LoggingMiddleware() 3xx status = %v, want %v", w.Code, http.StatusMovedPermanently)
	}
}

// Test CORS middleware with different request methods
func TestCORSMiddlewarePUT(t *testing.T) {
	router := gin.New()
	router.Use(CORSMiddleware())
	router.PUT("/test", func(c *gin.Context) {
		c.String(http.StatusOK, "OK")
	})

	req := httptest.NewRequest("PUT", "/test", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("CORSMiddleware() PUT status = %v, want %v", w.Code, http.StatusOK)
	}

	if w.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Error("CORS headers should be set for PUT")
	}
}

func TestCORSMiddlewareDELETE(t *testing.T) {
	router := gin.New()
	router.Use(CORSMiddleware())
	router.DELETE("/test", func(c *gin.Context) {
		c.String(http.StatusOK, "OK")
	})

	req := httptest.NewRequest("DELETE", "/test", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("CORSMiddleware() DELETE status = %v, want %v", w.Code, http.StatusOK)
	}

	if w.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Error("CORS headers should be set for DELETE")
	}
}

func TestCORSMiddlewareHEAD(t *testing.T) {
	router := gin.New()
	router.Use(CORSMiddleware())
	router.HEAD("/test", func(c *gin.Context) {
		c.String(http.StatusOK, "OK")
	})

	req := httptest.NewRequest("HEAD", "/test", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("CORSMiddleware() HEAD status = %v, want %v", w.Code, http.StatusOK)
	}

	if w.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Error("CORS headers should be set for HEAD")
	}
}

// Test RequestSizeLimitMiddleware with exact size limit
func TestRequestSizeLimitMiddlewareExactLimit(t *testing.T) {
	router := gin.New()
	router.Use(RequestSizeLimitMiddleware(100))
	router.PUT("/test", func(c *gin.Context) {
		c.String(http.StatusOK, "OK")
	})

	body := strings.Repeat("a", 100)
	req := httptest.NewRequest("PUT", "/test", strings.NewReader(body))
	req.Header.Set("Content-Length", "100")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("RequestSizeLimitMiddleware() exact limit status = %v, want %v", w.Code, http.StatusOK)
	}
}

// Test RequestSizeLimitMiddleware with DELETE method (should not check)
func TestRequestSizeLimitMiddlewareDELETE(t *testing.T) {
	router := gin.New()
	router.Use(RequestSizeLimitMiddleware(100))
	router.DELETE("/test", func(c *gin.Context) {
		c.String(http.StatusOK, "OK")
	})

	req := httptest.NewRequest("DELETE", "/test", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("RequestSizeLimitMiddleware() DELETE status = %v, want %v", w.Code, http.StatusOK)
	}
}

// Test ErrorHandlingMiddleware with no panic
func TestErrorHandlingMiddlewareNoPanic(t *testing.T) {
	router := gin.New()
	router.Use(ErrorHandlingMiddleware())
	router.GET("/test", func(c *gin.Context) {
		c.String(http.StatusOK, "OK")
	})

	req := httptest.NewRequest("GET", "/test", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("ErrorHandlingMiddleware() no panic status = %v, want %v", w.Code, http.StatusOK)
	}
}
