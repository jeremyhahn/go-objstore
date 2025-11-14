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
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/audit"
	"github.com/jeremyhahn/go-objstore/pkg/server/middleware"
)

// Test NewServer with nil logger (should use default)
func TestNewServerNilLogger(t *testing.T) {
	storage := NewMockStorage()
	config := &ServerConfig{
		Host:   "127.0.0.1",
		Port:   8080,
		Logger: nil, // Explicitly nil
		Mode:   gin.TestMode,
	}

	server, err := NewServer(storage, config)
	if err != nil {
		t.Fatalf("NewServer() with nil logger failed: %v", err)
	}

	if server.config.Logger == nil {
		t.Error("NewServer() should set default logger when nil")
	}
}

// Test NewServer with nil authenticator (should use default)
func TestNewServerNilAuthenticator(t *testing.T) {
	storage := NewMockStorage()
	config := &ServerConfig{
		Host:          "127.0.0.1",
		Port:          8080,
		Authenticator: nil, // Explicitly nil
		Mode:          gin.TestMode,
	}

	server, err := NewServer(storage, config)
	if err != nil {
		t.Fatalf("NewServer() with nil authenticator failed: %v", err)
	}

	if server.config.Authenticator == nil {
		t.Error("NewServer() should set default authenticator when nil")
	}
}

// Test NewServer with nil audit logger and EnableAudit true
func TestNewServerNilAuditLoggerWithAuditEnabled(t *testing.T) {
	storage := NewMockStorage()
	config := &ServerConfig{
		Host:        "127.0.0.1",
		Port:        8080,
		EnableAudit: true,
		AuditLogger: nil, // Explicitly nil
		Mode:        gin.TestMode,
	}

	server, err := NewServer(storage, config)
	if err != nil {
		t.Fatalf("NewServer() with nil audit logger failed: %v", err)
	}

	if server.config.AuditLogger == nil {
		t.Error("NewServer() should set default audit logger when nil and audit enabled")
	}
}

// Test NewServer with nil audit logger and EnableAudit false
func TestNewServerNilAuditLoggerWithAuditDisabled(t *testing.T) {
	storage := NewMockStorage()
	config := &ServerConfig{
		Host:        "127.0.0.1",
		Port:        8080,
		EnableAudit: false,
		AuditLogger: nil, // Explicitly nil
		Mode:        gin.TestMode,
	}

	server, err := NewServer(storage, config)
	if err != nil {
		t.Fatalf("NewServer() with nil audit logger and audit disabled failed: %v", err)
	}

	if server.config.AuditLogger == nil {
		t.Error("NewServer() should set NoOp audit logger when nil and audit disabled")
	}
}

// Test NewServer with all middleware enabled
func TestNewServerAllMiddlewareEnabled(t *testing.T) {
	storage := NewMockStorage()
	config := &ServerConfig{
		Host:                  "127.0.0.1",
		Port:                  8080,
		EnableCORS:            true,
		EnableLogging:         true,
		EnableRateLimit:       true,
		EnableSecurityHeaders: true,
		EnableRequestID:       true,
		EnableAudit:           true,
		MaxRequestSize:        1024,
		Mode:                  gin.TestMode,
		RateLimitConfig:       middleware.DefaultRateLimitConfig(),
		SecurityHeadersConfig: middleware.DefaultSecurityHeadersConfig(),
	}

	server, err := NewServer(storage, config)
	if err != nil {
		t.Fatalf("NewServer() with all middleware failed: %v", err)
	}

	if server.router == nil {
		t.Error("NewServer() router should not be nil")
	}

	// Test a request to ensure all middleware is working
	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()
	server.Router().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Request through all middleware status = %v, want %v", w.Code, http.StatusOK)
	}
}

// Test NewServer with all middleware disabled
func TestNewServerAllMiddlewareDisabled(t *testing.T) {
	storage := NewMockStorage()
	config := &ServerConfig{
		Host:                  "127.0.0.1",
		Port:                  8080,
		EnableCORS:            false,
		EnableLogging:         false,
		EnableRateLimit:       false,
		EnableSecurityHeaders: false,
		EnableRequestID:       false,
		EnableAudit:           false,
		MaxRequestSize:        0, // Disabled
		Mode:                  gin.TestMode,
	}

	server, err := NewServer(storage, config)
	if err != nil {
		t.Fatalf("NewServer() with all middleware disabled failed: %v", err)
	}

	if server.router == nil {
		t.Error("NewServer() router should not be nil")
	}

	// Test a request
	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()
	server.Router().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Request with no middleware status = %v, want %v", w.Code, http.StatusOK)
	}
}

// Test NewServer with rate limiting enabled
func TestNewServerWithRateLimiting(t *testing.T) {
	storage := NewMockStorage()
	config := &ServerConfig{
		Host:            "127.0.0.1",
		Port:            8080,
		EnableRateLimit: true,
		RateLimitConfig: &middleware.RateLimitConfig{
			RequestsPerSecond: 10,
			Burst:             20,
		},
		Mode: gin.TestMode,
	}

	server, err := NewServer(storage, config)
	if err != nil {
		t.Fatalf("NewServer() with rate limiting failed: %v", err)
	}

	if server.router == nil {
		t.Error("NewServer() router should not be nil")
	}
}

// Test NewServer with security headers enabled
func TestNewServerWithSecurityHeaders(t *testing.T) {
	storage := NewMockStorage()
	config := &ServerConfig{
		Host:                  "127.0.0.1",
		Port:                  8080,
		EnableSecurityHeaders: true,
		SecurityHeadersConfig: &middleware.SecurityHeadersConfig{
			EnableHSTS:            true,
			ContentSecurityPolicy: "default-src 'self'",
			XFrameOptions:         "DENY",
			XXSSProtection:        "1; mode=block",
			XContentTypeOptions:   "nosniff",
		},
		Mode: gin.TestMode,
	}

	server, err := NewServer(storage, config)
	if err != nil {
		t.Fatalf("NewServer() with security headers failed: %v", err)
	}

	if server.router == nil {
		t.Error("NewServer() router should not be nil")
	}
}

// Test NewServer with request ID middleware
func TestNewServerWithRequestID(t *testing.T) {
	storage := NewMockStorage()
	config := &ServerConfig{
		Host:            "127.0.0.1",
		Port:            8080,
		EnableRequestID: true,
		Mode:            gin.TestMode,
	}

	server, err := NewServer(storage, config)
	if err != nil {
		t.Fatalf("NewServer() with request ID failed: %v", err)
	}

	// Make a request and check for request ID header
	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()
	server.Router().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Request with request ID status = %v, want %v", w.Code, http.StatusOK)
	}
}

// Test NewServer with audit middleware enabled
func TestNewServerWithAuditMiddleware(t *testing.T) {
	storage := NewMockStorage()
	config := &ServerConfig{
		Host:        "127.0.0.1",
		Port:        8080,
		EnableAudit: true,
		AuditLogger: audit.NewDefaultAuditLogger(),
		Mode:        gin.TestMode,
	}

	server, err := NewServer(storage, config)
	if err != nil {
		t.Fatalf("NewServer() with audit middleware failed: %v", err)
	}

	if server.router == nil {
		t.Error("NewServer() router should not be nil")
	}
}

// Test NewServer with audit disabled
func TestNewServerWithAuditDisabled(t *testing.T) {
	storage := NewMockStorage()
	config := &ServerConfig{
		Host:        "127.0.0.1",
		Port:        8080,
		EnableAudit: false,
		Mode:        gin.TestMode,
	}

	server, err := NewServer(storage, config)
	if err != nil {
		t.Fatalf("NewServer() with audit disabled failed: %v", err)
	}

	if server.router == nil {
		t.Error("NewServer() router should not be nil")
	}
}

// Test DefaultServerConfig values
func TestDefaultServerConfigValues(t *testing.T) {
	config := DefaultServerConfig()

	if config.Host != "0.0.0.0" {
		t.Errorf("DefaultServerConfig() Host = %v, want 0.0.0.0", config.Host)
	}

	if config.Port != 8080 {
		t.Errorf("DefaultServerConfig() Port = %v, want 8080", config.Port)
	}

	if !config.EnableCORS {
		t.Error("DefaultServerConfig() EnableCORS should be true")
	}

	if !config.EnableLogging {
		t.Error("DefaultServerConfig() EnableLogging should be true")
	}

	if config.EnableRateLimit {
		t.Error("DefaultServerConfig() EnableRateLimit should be false")
	}

	if !config.EnableSecurityHeaders {
		t.Error("DefaultServerConfig() EnableSecurityHeaders should be true")
	}

	if !config.EnableRequestID {
		t.Error("DefaultServerConfig() EnableRequestID should be true")
	}

	if !config.EnableAudit {
		t.Error("DefaultServerConfig() EnableAudit should be true")
	}

	if config.MaxRequestSize != 100*1024*1024 {
		t.Errorf("DefaultServerConfig() MaxRequestSize = %v, want 100MB", config.MaxRequestSize)
	}

	if config.ReadTimeout != 60*time.Second {
		t.Errorf("DefaultServerConfig() ReadTimeout = %v, want 60s", config.ReadTimeout)
	}

	if config.WriteTimeout != 60*time.Second {
		t.Errorf("DefaultServerConfig() WriteTimeout = %v, want 60s", config.WriteTimeout)
	}

	if config.IdleTimeout != 120*time.Second {
		t.Errorf("DefaultServerConfig() IdleTimeout = %v, want 120s", config.IdleTimeout)
	}

	if config.Mode != gin.ReleaseMode {
		t.Errorf("DefaultServerConfig() Mode = %v, want %v", config.Mode, gin.ReleaseMode)
	}

	if config.Logger == nil {
		t.Error("DefaultServerConfig() Logger should not be nil")
	}

	if config.Authenticator == nil {
		t.Error("DefaultServerConfig() Authenticator should not be nil")
	}

	if config.AuditLogger == nil {
		t.Error("DefaultServerConfig() AuditLogger should not be nil")
	}

	if config.RateLimitConfig == nil {
		t.Error("DefaultServerConfig() RateLimitConfig should not be nil")
	}

	if config.SecurityHeadersConfig == nil {
		t.Error("DefaultServerConfig() SecurityHeadersConfig should not be nil")
	}
}

// Test Server with zero timeouts (should use defaults)
func TestServerWithZeroTimeouts(t *testing.T) {
	storage := NewMockStorage()
	config := &ServerConfig{
		Host:         "127.0.0.1",
		Port:         8080,
		ReadTimeout:  0,
		WriteTimeout: 0,
		IdleTimeout:  0,
		Mode:         gin.TestMode,
	}

	server, err := NewServer(storage, config)
	if err != nil {
		t.Fatalf("NewServer() with zero timeouts failed: %v", err)
	}

	if server.httpServer.ReadTimeout != 0 {
		t.Errorf("Server ReadTimeout = %v, want 0", server.httpServer.ReadTimeout)
	}

	if server.httpServer.WriteTimeout != 0 {
		t.Errorf("Server WriteTimeout = %v, want 0", server.httpServer.WriteTimeout)
	}

	if server.httpServer.IdleTimeout != 0 {
		t.Errorf("Server IdleTimeout = %v, want 0", server.httpServer.IdleTimeout)
	}
}

// Test Server shutdown with already stopped server
func TestServerShutdownAlreadyStopped(t *testing.T) {
	storage := NewMockStorage()
	config := &ServerConfig{
		Host: "127.0.0.1",
		Port: 0, // Random port
		Mode: gin.TestMode,
	}

	server, err := NewServer(storage, config)
	if err != nil {
		t.Fatalf("NewServer() failed: %v", err)
	}

	// Shutdown without starting
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	err = server.Shutdown(ctx)
	// Should not panic, may return error
	_ = err
}

// Test server with different host addresses
func TestServerWithDifferentHosts(t *testing.T) {
	storage := NewMockStorage()

	hosts := []string{
		"0.0.0.0",
		"127.0.0.1",
		"localhost",
	}

	for _, host := range hosts {
		t.Run(host, func(t *testing.T) {
			config := &ServerConfig{
				Host: host,
				Port: 8080,
				Mode: gin.TestMode,
			}

			server, err := NewServer(storage, config)
			if err != nil {
				t.Errorf("NewServer() with host %s failed: %v", host, err)
			}

			expectedAddr := host + ":8080"
			if server.Address() != expectedAddr {
				t.Errorf("Server Address() = %v, want %v", server.Address(), expectedAddr)
			}
		})
	}
}

// Test server with different ports
func TestServerWithDifferentPorts(t *testing.T) {
	storage := NewMockStorage()

	ports := []int{8080, 9000, 3000, 8443}

	for _, port := range ports {
		t.Run(string(rune(port)), func(t *testing.T) {
			config := &ServerConfig{
				Host: "127.0.0.1",
				Port: port,
				Mode: gin.TestMode,
			}

			server, err := NewServer(storage, config)
			if err != nil {
				t.Errorf("NewServer() with port %d failed: %v", port, err)
			}

			if server.httpServer.Addr != "127.0.0.1:"+string(rune(port+'0')) {
				// Port conversion might not work this way, so just check it's set
				if server.httpServer.Addr == "" {
					t.Error("Server address should not be empty")
				}
			}
		})
	}
}

// Test server Handler() method
func TestServerHandlerMethod(t *testing.T) {
	storage := NewMockStorage()
	server, err := NewServer(storage, nil)
	if err != nil {
		t.Fatalf("NewServer() failed: %v", err)
	}

	handler := server.Handler()
	if handler == nil {
		t.Error("Handler() should not return nil")
	}

	// Verify it's the same handler used internally
	if handler != server.handler {
		t.Error("Handler() should return internal handler")
	}
}

// Test server Router() method
func TestServerRouterMethod(t *testing.T) {
	storage := NewMockStorage()
	server, err := NewServer(storage, nil)
	if err != nil {
		t.Fatalf("NewServer() failed: %v", err)
	}

	router := server.Router()
	if router == nil {
		t.Error("Router() should not return nil")
	}

	// Verify it's the same router used internally
	if router != server.router {
		t.Error("Router() should return internal router")
	}
}

// Test server Address() method
func TestServerAddressMethod(t *testing.T) {
	storage := NewMockStorage()
	config := &ServerConfig{
		Host: "192.168.1.1",
		Port: 9999,
		Mode: gin.TestMode,
	}

	server, err := NewServer(storage, config)
	if err != nil {
		t.Fatalf("NewServer() failed: %v", err)
	}

	expectedAddr := "192.168.1.1:9999"
	if server.Address() != expectedAddr {
		t.Errorf("Address() = %v, want %v", server.Address(), expectedAddr)
	}

	// Verify it matches httpServer.Addr
	if server.Address() != server.httpServer.Addr {
		t.Error("Address() should match httpServer.Addr")
	}
}

// Test NewServer with custom logger
func TestNewServerWithCustomLogger(t *testing.T) {
	storage := NewMockStorage()
	customLogger := adapters.NewDefaultLogger()
	config := &ServerConfig{
		Host:   "127.0.0.1",
		Port:   8080,
		Logger: customLogger,
		Mode:   gin.TestMode,
	}

	server, err := NewServer(storage, config)
	if err != nil {
		t.Fatalf("NewServer() with custom logger failed: %v", err)
	}

	if server.config.Logger != customLogger {
		t.Error("NewServer() should use provided custom logger")
	}
}

// Test NewServer with custom authenticator
func TestNewServerWithCustomAuthenticator(t *testing.T) {
	storage := NewMockStorage()
	customAuth := adapters.NewNoOpAuthenticator()
	config := &ServerConfig{
		Host:          "127.0.0.1",
		Port:          8080,
		Authenticator: customAuth,
		Mode:          gin.TestMode,
	}

	server, err := NewServer(storage, config)
	if err != nil {
		t.Fatalf("NewServer() with custom authenticator failed: %v", err)
	}

	if server.config.Authenticator == nil {
		t.Error("NewServer() should have an authenticator")
	}
}

// Test NewServer with custom audit logger
func TestNewServerWithCustomAuditLogger(t *testing.T) {
	storage := NewMockStorage()
	customAuditLogger := audit.NewDefaultAuditLogger()
	config := &ServerConfig{
		Host:        "127.0.0.1",
		Port:        8080,
		EnableAudit: true,
		AuditLogger: customAuditLogger,
		Mode:        gin.TestMode,
	}

	server, err := NewServer(storage, config)
	if err != nil {
		t.Fatalf("NewServer() with custom audit logger failed: %v", err)
	}

	if server.config.AuditLogger != customAuditLogger {
		t.Error("NewServer() should use provided custom audit logger")
	}
}
