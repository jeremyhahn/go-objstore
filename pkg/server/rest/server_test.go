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
)

func TestDefaultServerConfig(t *testing.T) {
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

	if config.MaxRequestSize != 100*1024*1024 {
		t.Errorf("DefaultServerConfig() MaxRequestSize = %v, want %v", config.MaxRequestSize, 100*1024*1024)
	}

	if config.Mode != gin.ReleaseMode {
		t.Errorf("DefaultServerConfig() Mode = %v, want %v", config.Mode, gin.ReleaseMode)
	}
}

func TestNewServer(t *testing.T) {
	storage := NewMockStorage()

	tests := []struct {
		name    string
		config  *ServerConfig
		wantErr bool
	}{
		{
			name:    "with default config",
			config:  nil,
			wantErr: false,
		},
		{
			name:    "with custom config",
			config:  DefaultServerConfig(),
			wantErr: false,
		},
		{
			name: "with custom port",
			config: &ServerConfig{
				Host:          "127.0.0.1",
				Port:          9000,
				EnableCORS:    false,
				EnableLogging: false,
				Mode:          gin.TestMode,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server, err := NewServer(storage, tt.config)

			if (err != nil) != tt.wantErr {
				t.Errorf("NewServer() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if err == nil {
				if server.router == nil {
					t.Error("NewServer() router should not be nil")
				}

				if server.handler == nil {
					t.Error("NewServer() handler should not be nil")
				}

				if server.httpServer == nil {
					t.Error("NewServer() httpServer should not be nil")
				}
			}
		})
	}
}

func TestServerRouter(t *testing.T) {
	storage := NewMockStorage()
	server, err := NewServer(storage, nil)
	if err != nil {
		t.Fatalf("NewServer() failed: %v", err)
	}

	router := server.Router()
	if router == nil {
		t.Error("Router() should not return nil")
	}
}

func TestServerHandler(t *testing.T) {
	storage := NewMockStorage()
	server, err := NewServer(storage, nil)
	if err != nil {
		t.Fatalf("NewServer() failed: %v", err)
	}

	handler := server.Handler()
	if handler == nil {
		t.Error("Handler() should not return nil")
	}
}

func TestServerAddress(t *testing.T) {
	storage := NewMockStorage()
	config := &ServerConfig{
		Host: "127.0.0.1",
		Port: 9000,
	}

	server, err := NewServer(storage, config)
	if err != nil {
		t.Fatalf("NewServer() failed: %v", err)
	}

	addr := server.Address()
	expectedAddr := "127.0.0.1:9000"
	if addr != expectedAddr {
		t.Errorf("Address() = %v, want %v", addr, expectedAddr)
	}
}

func TestServerWithAllMiddleware(t *testing.T) {
	storage := NewMockStorage()
	config := &ServerConfig{
		Host:           "0.0.0.0",
		Port:           8080,
		EnableCORS:     true,
		EnableLogging:  true,
		MaxRequestSize: 1024,
		Mode:           gin.TestMode,
	}

	server, err := NewServer(storage, config)
	if err != nil {
		t.Fatalf("NewServer() failed: %v", err)
	}

	// Test that all middleware is properly registered by making a request
	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()

	server.Router().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Server request status = %v, want %v", w.Code, http.StatusOK)
	}

	// Check CORS headers
	if w.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Error("CORS middleware not properly configured")
	}
}

func TestServerWithoutMiddleware(t *testing.T) {
	storage := NewMockStorage()
	config := &ServerConfig{
		Host:           "0.0.0.0",
		Port:           8080,
		EnableCORS:     false,
		EnableLogging:  false,
		MaxRequestSize: 0,
		Mode:           gin.TestMode,
	}

	server, err := NewServer(storage, config)
	if err != nil {
		t.Fatalf("NewServer() failed: %v", err)
	}

	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()

	server.Router().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Server request status = %v, want %v", w.Code, http.StatusOK)
	}
}

func TestServerShutdown(t *testing.T) {
	storage := NewMockStorage()
	config := &ServerConfig{
		Host: "127.0.0.1",
		Port: 0, // Use random port
		Mode: gin.TestMode,
	}

	server, err := NewServer(storage, config)
	if err != nil {
		t.Fatalf("NewServer() failed: %v", err)
	}

	// Start server in background
	go func() {
		// We expect this to fail since we're using port 0 and immediately shutting down
		// This is fine for testing the shutdown mechanism
		server.Start()
	}()

	// Give it a moment to start
	time.Sleep(10 * time.Millisecond)

	// Test shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = server.Shutdown(ctx)
	// We don't check the error here because the server might not have fully started
	// The important thing is that Shutdown() doesn't panic or hang
	_ = err
}

func TestServerModes(t *testing.T) {
	storage := NewMockStorage()

	modes := []string{gin.DebugMode, gin.ReleaseMode, gin.TestMode}

	for _, mode := range modes {
		t.Run(mode, func(t *testing.T) {
			config := &ServerConfig{
				Host: "0.0.0.0",
				Port: 8080,
				Mode: mode,
			}

			server, err := NewServer(storage, config)
			if err != nil {
				t.Errorf("NewServer() with mode %s failed: %v", mode, err)
			}

			if server == nil {
				t.Errorf("NewServer() with mode %s returned nil", mode)
			}
		})
	}
}

func TestServerCustomTimeouts(t *testing.T) {
	storage := NewMockStorage()
	config := &ServerConfig{
		Host:         "0.0.0.0",
		Port:         8080,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
		Mode:         gin.TestMode,
	}

	server, err := NewServer(storage, config)
	if err != nil {
		t.Fatalf("NewServer() failed: %v", err)
	}

	if server.httpServer.ReadTimeout != 30*time.Second {
		t.Errorf("ReadTimeout = %v, want %v", server.httpServer.ReadTimeout, 30*time.Second)
	}

	if server.httpServer.WriteTimeout != 30*time.Second {
		t.Errorf("WriteTimeout = %v, want %v", server.httpServer.WriteTimeout, 30*time.Second)
	}

	if server.httpServer.IdleTimeout != 60*time.Second {
		t.Errorf("IdleTimeout = %v, want %v", server.httpServer.IdleTimeout, 60*time.Second)
	}
}
