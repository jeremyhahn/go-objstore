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

package unix

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/objstore"
)

func TestNewServer(t *testing.T) {
	storage := NewMockStorage()
	initTestFacade(t, storage)

	tests := []struct {
		name    string
		config  *ServerConfig
		wantErr bool
	}{
		{
			name: "valid config",
			config: &ServerConfig{
				SocketPath: "/tmp/test-valid.sock",
				Logger:     &mockLogger{},
			},
			wantErr: false,
		},
		{
			name:    "nil config uses defaults",
			config:  nil,
			wantErr: false,
		},
		{
			name: "empty socket path",
			config: &ServerConfig{
				SocketPath: "",
				Logger:     &mockLogger{},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Re-initialize facade for each test
			objstore.Reset()
			initTestFacade(t, storage)

			server, err := NewServer(tt.config)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error but got nil")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if server == nil {
					t.Error("server is nil")
				}
			}
		})
	}
}

func TestNewServerRequiresFacade(t *testing.T) {
	// Reset facade to uninitialized state
	objstore.Reset()

	_, err := NewServer(&ServerConfig{
		SocketPath: "/tmp/test.sock",
		Logger:     &mockLogger{},
	})

	if err != ErrNotInitialized {
		t.Errorf("expected ErrNotInitialized, got %v", err)
	}
}

func TestServerSocketPath(t *testing.T) {
	storage := NewMockStorage()
	socketPath := tempSocketPath(t)
	server := createTestServer(t, storage, socketPath)
	defer cleanupSocket(t, socketPath)

	if server.SocketPath() != socketPath {
		t.Errorf("got socket path %q, want %q", server.SocketPath(), socketPath)
	}
}

func TestServerStartShutdown(t *testing.T) {
	storage := NewMockStorage()
	socketPath := tempSocketPath(t)
	server := createTestServer(t, storage, socketPath)
	defer cleanupSocket(t, socketPath)

	// Create context with cancel
	ctx, cancel := context.WithCancel(context.Background())

	// Start server in background
	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Start(ctx)
	}()

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)

	// Verify socket file exists
	if _, err := os.Stat(socketPath); os.IsNotExist(err) {
		t.Error("socket file was not created")
	}

	// Cancel context to trigger shutdown
	cancel()

	// Wait for shutdown
	select {
	case err := <-errCh:
		if err != nil {
			t.Logf("server shutdown with error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Error("server shutdown timed out")
	}

	// Verify socket file is removed after shutdown
	time.Sleep(100 * time.Millisecond)
	if _, err := os.Stat(socketPath); !os.IsNotExist(err) {
		t.Error("socket file was not removed after shutdown")
	}
}

func TestServerShutdown(t *testing.T) {
	storage := NewMockStorage()
	socketPath := tempSocketPath(t)
	server := createTestServer(t, storage, socketPath)
	defer cleanupSocket(t, socketPath)

	// Create context with cancel
	ctx, cancel := context.WithCancel(context.Background())

	// Start server in background
	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Start(ctx)
	}()

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)

	// Explicit shutdown
	if err := server.Shutdown(context.Background()); err != nil {
		t.Errorf("shutdown error: %v", err)
	}

	// Cancel context
	cancel()

	// Wait for goroutine to finish
	select {
	case <-errCh:
		// Expected
	case <-time.After(2 * time.Second):
		t.Error("server shutdown timed out")
	}
}

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	if config.SocketPath != "/var/run/objstore.sock" {
		t.Errorf("got socket path %q, want %q", config.SocketPath, "/var/run/objstore.sock")
	}

	if config.SocketPermissions != 0660 {
		t.Errorf("got permissions %o, want %o", config.SocketPermissions, 0660)
	}

	if config.Logger == nil {
		t.Error("logger is nil")
	}
}

func TestProcessRequest(t *testing.T) {
	storage := NewMockStorage()
	socketPath := tempSocketPath(t)
	server := createTestServer(t, storage, socketPath)
	defer cleanupSocket(t, socketPath)

	tests := []struct {
		name       string
		request    string
		wantErrNil bool
		errCode    int
	}{
		{
			name:       "valid request",
			request:    `{"jsonrpc":"2.0","method":"ping","id":1}`,
			wantErrNil: true,
		},
		{
			name:       "invalid JSON",
			request:    `{invalid json}`,
			wantErrNil: false,
			errCode:    ErrCodeParseError,
		},
		{
			name:       "invalid JSON-RPC version",
			request:    `{"jsonrpc":"1.0","method":"ping","id":1}`,
			wantErrNil: false,
			errCode:    ErrCodeInvalidRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := server.processRequest(context.Background(), []byte(tt.request))

			if tt.wantErrNil {
				if resp.Error != nil {
					t.Errorf("unexpected error: %s", resp.Error.Message)
				}
			} else {
				if resp.Error == nil {
					t.Error("expected error but got nil")
					return
				}
				if resp.Error.Code != tt.errCode {
					t.Errorf("got error code %d, want %d", resp.Error.Code, tt.errCode)
				}
			}
		})
	}
}
