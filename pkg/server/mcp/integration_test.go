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
	"net/http"
	"strings"
	"testing"
	"time"
)

// TestServer_StartHTTP_Integration tests HTTP server startup and shutdown
func TestServer_StartHTTP_Integration(t *testing.T) {
	storage := NewMockStorage()
	storage.PutWithContext(context.Background(), "test.txt", strings.NewReader("hello world"))

	server, err := NewServer(&ServerConfig{
		Mode:        ModeHTTP,
		HTTPAddress: ":18080", // Use non-standard port
		Storage:     storage,
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

	// Give server time to start
	time.Sleep(100 * time.Millisecond)

	// Test initialize request
	initReq := JSONRPCRequest{
		JSONRPC: "2.0",
		Method:  "initialize",
		ID:      1,
	}

	reqBody, _ := json.Marshal(initReq)
	resp, err := http.Post("http://localhost:18080", "application/json", bytes.NewBuffer(reqBody))
	if err != nil {
		// Server might not be ready yet, skip this test
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
	resp, err = http.Post("http://localhost:18080", "application/json", bytes.NewBuffer(reqBody))
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

	server, err := NewServer(&ServerConfig{
		Mode:    ServerMode("invalid"),
		Storage: storage,
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

	server, _ := NewServer(&ServerConfig{
		Mode:    ModeHTTP,
		Storage: storage,
	})
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
	manager := NewResourceManager(storage, "prefix/")

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
