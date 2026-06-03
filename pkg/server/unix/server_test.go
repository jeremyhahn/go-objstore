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
	"bytes"
	"context"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/objstore"
	"github.com/jeremyhahn/go-objstore/pkg/server/jsonrpc"
	"github.com/jeremyhahn/go-objstore/pkg/server/middleware"
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

// TestStartRefusesNonSocketPath verifies that Start refuses to remove a
// SocketPath that exists but is not a Unix domain socket, leaving the file
// intact.
func TestStartRefusesNonSocketPath(t *testing.T) {
	storage := NewMockStorage()
	socketPath := tempSocketPath(t)
	defer cleanupSocket(t, socketPath)

	content := []byte("precious data")
	if err := os.WriteFile(socketPath, content, 0600); err != nil {
		t.Fatalf("failed to create regular file: %v", err)
	}

	server := createTestServer(t, storage, socketPath)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := server.Start(ctx)
	if err == nil {
		t.Fatal("expected error when socket path is a regular file")
	}
	if !strings.Contains(err.Error(), "is not a socket") {
		t.Errorf("error should mention the path is not a socket, got: %v", err)
	}

	// The regular file must not have been deleted or modified.
	got, readErr := os.ReadFile(socketPath)
	if readErr != nil {
		t.Fatalf("regular file was removed: %v", readErr)
	}
	if !bytes.Equal(got, content) {
		t.Errorf("regular file content changed: got %q, want %q", got, content)
	}
}

// TestShutdownSkipsNonSocketPath verifies that Shutdown does not delete a
// SocketPath that is no longer a socket and does not fail on cleanup.
func TestShutdownSkipsNonSocketPath(t *testing.T) {
	storage := NewMockStorage()
	socketPath := tempSocketPath(t)
	defer cleanupSocket(t, socketPath)

	server := createTestServer(t, storage, socketPath)

	content := []byte("precious data")
	if err := os.WriteFile(socketPath, content, 0600); err != nil {
		t.Fatalf("failed to create regular file: %v", err)
	}

	if err := server.Shutdown(context.Background()); err != nil {
		t.Errorf("shutdown error: %v", err)
	}

	got, readErr := os.ReadFile(socketPath)
	if readErr != nil {
		t.Fatalf("regular file was removed: %v", readErr)
	}
	if !bytes.Equal(got, content) {
		t.Errorf("regular file content changed: got %q, want %q", got, content)
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

// TestFirstReadDeadline verifies that a client that connects and never sends
// a request is disconnected after the configured read deadline — the deadline
// must be armed before the first read, not after it.
func TestFirstReadDeadline(t *testing.T) {
	storage := NewMockStorage()
	socketPath := tempSocketPath(t)
	defer cleanupSocket(t, socketPath)

	initTestFacade(t, storage)
	server, err := NewServer(&ServerConfig{
		SocketPath:   socketPath,
		Backend:      "",
		Logger:       &mockLogger{},
		ReadDeadline: 200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- server.Start(ctx) }()
	time.Sleep(100 * time.Millisecond)

	// Connect and send nothing. The server must close the connection once the
	// first read deadline expires; observe that as a read returning (EOF or
	// timeout-driven close) well before the test deadline.
	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	buf := make([]byte, 1)
	readDone := make(chan error, 1)
	go func() {
		_, err := conn.Read(buf)
		readDone <- err
	}()

	select {
	case err := <-readDone:
		if err == nil {
			t.Error("expected connection close, got data")
		}
	case <-time.After(2 * time.Second):
		t.Error("idle connection was not closed by the first-read deadline")
	}
}

// TestUnixRateLimit verifies that a configured rate limit rejects burst
// requests with the shared rate-limited JSON-RPC code.
func TestUnixRateLimit(t *testing.T) {
	storage := NewMockStorage()
	initTestFacade(t, storage)
	server, err := NewServer(&ServerConfig{
		SocketPath:      tempSocketPath(t),
		Logger:          &mockLogger{},
		EnableRateLimit: true,
		RateLimitConfig: &middleware.RateLimitConfig{RequestsPerSecond: 1, Burst: 1},
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	defer server.Shutdown(context.Background())

	req := []byte(`{"jsonrpc":"2.0","method":"health","params":{},"id":1}`)

	first := server.processRequest(context.Background(), req)
	if first.Error != nil {
		t.Fatalf("first request should pass, got %+v", first.Error)
	}

	second := server.processRequest(context.Background(), req)
	if second.Error == nil || second.Error.Code != jsonrpc.CodeRateLimited {
		t.Errorf("second request should be rate limited with %d, got %+v", jsonrpc.CodeRateLimited, second.Error)
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
