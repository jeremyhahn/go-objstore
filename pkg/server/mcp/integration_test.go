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

package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

// TestServer_StartHTTP_Integration tests HTTP server startup and shutdown
func TestServer_StartHTTP_Integration(t *testing.T) {
	storage := NewMockStorage()
	storage.PutWithContext(context.Background(), "test.txt", strings.NewReader("hello world"))
	initTestFacade(t, storage)

	// A fixed port is shared by every process on the host. On a CI runner
	// where jobs run concurrently on the host network, a second copy of this
	// suite binds it first and the requests below reach an unrelated server --
	// which answers 405, not the JSON-RPC this test expects. Let the kernel
	// pick a free port on loopback instead.
	address := freeLoopbackAddr(t)

	server, err := NewServer(&ServerConfig{
		Mode:        ModeHTTP,
		HTTPAddress: address,
		Backend:     "",
	})
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}

	// Start server in background
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errChan := make(chan error, 1)
	go func() {
		errChan <- server.Start(ctx)
	}()

	// Wait for the listener rather than assuming it is up within a fixed
	// sleep, which fails on a loaded runner and wastes time on an idle one.
	endpoint := "http://" + address
	waitForListener(t, address, errChan)

	// Test initialize request
	initReq := JSONRPCRequest{
		JSONRPC: "2.0",
		Method:  "initialize",
		ID:      1,
	}

	reqBody, _ := json.Marshal(initReq)
	resp, err := http.Post(endpoint, "application/json", bytes.NewBuffer(reqBody))
	if err != nil {
		// In CI the server must come up; fail loudly instead of masking breakage.
		if os.Getenv("CI") != "" {
			t.Fatalf("Server not ready: %v", err)
		}
		// Locally the environment may legitimately lack prerequisites.
		t.Skipf("Server not ready: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	var initResp JSONRPCResponse
	if err := json.NewDecoder(resp.Body).Decode(&initResp); err != nil {
		t.Errorf("failed to decode response: %v", err)
	}

	if initResp.JSONRPC != "2.0" {
		t.Errorf("expected jsonrpc 2.0, got %s", initResp.JSONRPC)
	}

	// Test tools/call request
	toolsCallReq := JSONRPCRequest{
		JSONRPC: "2.0",
		Method:  "tools/call",
		ID:      2,
	}

	params := map[string]any{
		"name": "objstore_get",
		"arguments": map[string]any{
			"key": "test.txt",
		},
	}
	paramsJSON, _ := json.Marshal(params)
	toolsCallReq.Params = paramsJSON

	reqBody, _ = json.Marshal(toolsCallReq)
	resp, err = http.Post(endpoint, "application/json", bytes.NewBuffer(reqBody))
	if err != nil {
		t.Errorf("failed to call tool: %v", err)
	} else {
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("expected status 200, got %d", resp.StatusCode)
		}
	}

	// Stop server
	cancel()

	// Wait for server to stop
	select {
	case err := <-errChan:
		if err != nil && err != context.Canceled {
			t.Errorf("unexpected server error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Error("server did not stop in time")
	}
}

// TestServer_InvalidMode tests invalid server mode
func TestServer_InvalidMode(t *testing.T) {
	storage := NewMockStorage()
	initTestFacade(t, storage)

	server, err := NewServer(&ServerConfig{
		Mode:    ServerMode("invalid"),
		Backend: "",
	})
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err = server.Start(ctx)
	if err == nil {
		t.Error("expected error for invalid mode")
	}
	if !strings.Contains(err.Error(), "unknown server mode") {
		t.Errorf("expected 'unknown server mode' error, got: %v", err)
	}
}

// TestHTTPHandler_FullFlow tests complete HTTP request flow
func TestHTTPHandler_FullFlow(t *testing.T) {
	storage := NewMockStorage()
	storage.PutWithContext(context.Background(), "test.txt", strings.NewReader("test data"))
	server := createTestServer(t, storage, ModeHTTP)
	httpHandler := NewHTTPHandler(server)

	// Test full flow: initialize -> tools/list -> tools/call
	tests := []struct {
		name   string
		method string
		params any
	}{
		{
			name:   "initialize",
			method: "initialize",
			params: map[string]any{
				"protocolVersion": "2024-11-05",
			},
		},
		{
			name:   "tools/list",
			method: "tools/list",
		},
		{
			name:   "tools/call",
			method: "tools/call",
			params: map[string]any{
				"name": "objstore_exists",
				"arguments": map[string]any{
					"key": "test.txt",
				},
			},
		},
		{
			name:   "resources/list",
			method: "resources/list",
		},
		{
			name:   "resources/read",
			method: "resources/read",
			params: map[string]any{
				"uri": "objstore://test.txt",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := JSONRPCRequest{
				JSONRPC: "2.0",
				Method:  tt.method,
				ID:      1,
			}

			if tt.params != nil {
				paramsJSON, _ := json.Marshal(tt.params)
				req.Params = paramsJSON
			}

			reqBody, _ := json.Marshal(req)
			httpReq, _ := http.NewRequest(http.MethodPost, "/", bytes.NewBuffer(reqBody))

			rec := &mockResponseWriter{
				header: make(http.Header),
				body:   &bytes.Buffer{},
			}

			httpHandler.ServeHTTP(rec, httpReq)

			if rec.statusCode != 0 && rec.statusCode != http.StatusOK {
				t.Errorf("expected status 200, got %d", rec.statusCode)
			}

			var resp JSONRPCResponse
			if err := json.Unmarshal(rec.body.Bytes(), &resp); err != nil {
				t.Errorf("failed to decode response: %v", err)
			}

			if resp.JSONRPC != "2.0" {
				t.Errorf("expected jsonrpc 2.0, got %s", resp.JSONRPC)
			}

			if resp.Error != nil {
				t.Errorf("unexpected error: %+v", resp.Error)
			}
		})
	}
}

// TestResourceManager_EdgeCases tests edge cases in resource management
func TestResourceManager_EdgeCases(t *testing.T) {
	storage := NewMockStorage()
	manager := createTestResourceManager(t, storage, "prefix/")

	// Test with empty key
	_, _, err := manager.ReadResource(context.Background(), "objstore://")
	if err == nil {
		t.Error("expected error for empty key")
	}

	// Test extractName with various inputs
	tests := []struct {
		input    string
		expected string
	}{
		{"", ""},
		{"single", "single"},
		{"a/b/c/d", "d"},
		{"trailing/", ""},
	}

	for _, tt := range tests {
		result := manager.extractName(tt.input)
		if result != tt.expected {
			t.Errorf("extractName(%q) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}

// mockResponseWriter implements http.ResponseWriter for testing
type mockResponseWriter struct {
	header     http.Header
	body       *bytes.Buffer
	statusCode int
}

func (m *mockResponseWriter) Header() http.Header {
	return m.header
}

func (m *mockResponseWriter) Write(data []byte) (int, error) {
	return m.body.Write(data)
}

func (m *mockResponseWriter) WriteHeader(statusCode int) {
	m.statusCode = statusCode
}

// freeLoopbackAddr reserves a loopback port, releases it, and returns the
// address. The gap between release and rebind is far smaller a risk than a
// constant every concurrent run shares.
func freeLoopbackAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve a port: %v", err)
	}
	defer l.Close()
	return l.Addr().String()
}

// waitForListener blocks until the server accepts connections, or fails the
// test if it exits or never comes up.
func waitForListener(t *testing.T, address string, errChan <-chan error) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-errChan:
			t.Fatalf("server exited before it was ready: %v", err)
		default:
		}
		conn, err := net.DialTimeout("tcp", address, 100*time.Millisecond)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("server did not start listening on %s", address)
}
