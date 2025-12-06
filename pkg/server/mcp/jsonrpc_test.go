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
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jeremyhahn/go-objstore/pkg/version"
	"github.com/sourcegraph/jsonrpc2"
)

func TestRPCHandler_HandleInitialize(t *testing.T) {
	storage := NewMockStorage()
	server := createTestServer(t, storage, ModeStdio)
	handler := NewRPCHandler(server)

	initParams := map[string]any{
		"protocolVersion": "2024-11-05",
		"capabilities":    map[string]any{},
		"clientInfo": map[string]any{
			"name":    "test-client",
			"version": version.Get(),
		},
	}

	paramsJSON, _ := json.Marshal(initParams)
	rawParams := json.RawMessage(paramsJSON)

	result, err := handler.handleInitialize(context.Background(), &rawParams)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	resultMap, ok := result.(map[string]any)
	if !ok {
		t.Fatal("expected result to be a map")
	}

	if resultMap["protocolVersion"] == "" {
		t.Error("expected protocolVersion to be set")
	}

	if resultMap["capabilities"] == nil {
		t.Error("expected capabilities to be set")
	}

	if resultMap["serverInfo"] == nil {
		t.Error("expected serverInfo to be set")
	}
}

func TestRPCHandler_HandleInitializeNilParams(t *testing.T) {
	storage := NewMockStorage()
	server := createTestServer(t, storage, ModeStdio)
	handler := NewRPCHandler(server)

	result, err := handler.handleInitialize(context.Background(), nil)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if result == nil {
		t.Error("expected result to be non-nil")
	}
}

func TestRPCHandler_HandleToolsList(t *testing.T) {
	storage := NewMockStorage()
	server := createTestServer(t, storage, ModeStdio)
	handler := NewRPCHandler(server)

	result, err := handler.handleToolsList(context.Background())
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	resultMap, ok := result.(map[string]any)
	if !ok {
		t.Fatal("expected result to be a map")
	}

	tools, ok := resultMap["tools"].([]Tool)
	if !ok {
		t.Fatal("expected tools to be a slice")
	}

	if len(tools) != 19 {
		t.Errorf("expected 19 tools, got %d", len(tools))
	}
}

func TestRPCHandler_HandleToolsCall(t *testing.T) {
	storage := NewMockStorage()
	server := createTestServer(t, storage, ModeStdio)
	handler := NewRPCHandler(server)

	tests := []struct {
		name      string
		params    map[string]any
		wantError bool
	}{
		{
			name: "valid tool call",
			params: map[string]any{
				"name": "objstore_put",
				"arguments": map[string]any{
					"key":  "test.txt",
					"data": "hello",
				},
			},
			wantError: false,
		},
		{
			name: "invalid tool name",
			params: map[string]any{
				"name":      "invalid_tool",
				"arguments": map[string]any{},
			},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			paramsJSON, _ := json.Marshal(tt.params)
			rawParams := json.RawMessage(paramsJSON)

			result, err := handler.handleToolsCall(context.Background(), &rawParams)
			if tt.wantError {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			resultMap, ok := result.(map[string]any)
			if !ok {
				t.Fatal("expected result to be a map")
			}

			content, ok := resultMap["content"].([]map[string]any)
			if !ok {
				t.Fatal("expected content to be a slice")
			}

			if len(content) == 0 {
				t.Error("expected content to have at least one item")
			}
		})
	}
}

func TestRPCHandler_HandleToolsCallInvalidParams(t *testing.T) {
	storage := NewMockStorage()
	server := createTestServer(t, storage, ModeStdio)
	handler := NewRPCHandler(server)

	// Test with nil params
	_, err := handler.handleToolsCall(context.Background(), nil)
	if err == nil {
		t.Error("expected error for nil params")
	}

	// Test with invalid JSON
	invalidJSON := json.RawMessage([]byte("invalid"))
	_, err = handler.handleToolsCall(context.Background(), &invalidJSON)
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestRPCHandler_HandleResourcesList(t *testing.T) {
	storage := NewMockStorage()
	server := createTestServer(t, storage, ModeStdio)
	handler := NewRPCHandler(server)

	// Add test objects
	storage.PutWithContext(context.Background(), "file1.txt", strings.NewReader("data1"))
	storage.PutWithContext(context.Background(), "file2.txt", strings.NewReader("data2"))

	result, err := handler.handleResourcesList(context.Background(), nil)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	resultMap, ok := result.(map[string]any)
	if !ok {
		t.Fatal("expected result to be a map")
	}

	resources, ok := resultMap["resources"].([]Resource)
	if !ok {
		t.Fatal("expected resources to be a slice")
	}

	if len(resources) != 2 {
		t.Errorf("expected 2 resources, got %d", len(resources))
	}
}

func TestRPCHandler_HandleResourcesRead(t *testing.T) {
	storage := NewMockStorage()
	server := createTestServer(t, storage, ModeStdio)
	handler := NewRPCHandler(server)

	// Add test object
	storage.PutWithContext(context.Background(), "test.txt", strings.NewReader("hello world"))

	tests := []struct {
		name      string
		params    map[string]any
		wantError bool
	}{
		{
			name: "valid read",
			params: map[string]any{
				"uri": "objstore://test.txt",
			},
			wantError: false,
		},
		{
			name: "non-existent resource",
			params: map[string]any{
				"uri": "objstore://nonexistent.txt",
			},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			paramsJSON, _ := json.Marshal(tt.params)
			rawParams := json.RawMessage(paramsJSON)

			result, err := handler.handleResourcesRead(context.Background(), &rawParams)
			if tt.wantError {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			resultMap, ok := result.(map[string]any)
			if !ok {
				t.Fatal("expected result to be a map")
			}

			contents, ok := resultMap["contents"].([]map[string]any)
			if !ok {
				t.Fatal("expected contents to be a slice")
			}

			if len(contents) == 0 {
				t.Error("expected contents to have at least one item")
			}
		})
	}
}

func TestRPCHandler_HandleResourcesReadInvalidParams(t *testing.T) {
	storage := NewMockStorage()
	server := createTestServer(t, storage, ModeStdio)
	handler := NewRPCHandler(server)

	// Test with nil params
	_, err := handler.handleResourcesRead(context.Background(), nil)
	if err == nil {
		t.Error("expected error for nil params")
	}

	// Test with invalid JSON
	invalidJSON := json.RawMessage([]byte("invalid"))
	_, err = handler.handleResourcesRead(context.Background(), &invalidJSON)
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestRPCHandler_Handle(t *testing.T) {
	storage := NewMockStorage()
	server := createTestServer(t, storage, ModeStdio)
	handler := NewRPCHandler(server)

	tests := []struct {
		name      string
		method    string
		wantError bool
	}{
		{
			name:      "initialize",
			method:    "initialize",
			wantError: false,
		},
		{
			name:      "tools/list",
			method:    "tools/list",
			wantError: false,
		},
		{
			name:      "ping",
			method:    "ping",
			wantError: false,
		},
		{
			name:      "unknown method",
			method:    "unknown/method",
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &jsonrpc2.Request{
				Method: tt.method,
			}

			result, err := handler.Handle(context.Background(), nil, req)
			if tt.wantError {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if result == nil {
				t.Error("expected result to be non-nil")
			}
		})
	}
}

func TestHTTPHandler_ServeHTTP(t *testing.T) {
	storage := NewMockStorage()
	server := createTestServer(t, storage, ModeHTTP)
	httpHandler := NewHTTPHandler(server)

	tests := []struct {
		name           string
		method         string
		body           string
		expectedStatus int
	}{
		{
			name:   "valid initialize request",
			method: http.MethodPost,
			body: `{
				"jsonrpc": "2.0",
				"method": "initialize",
				"id": 1
			}`,
			expectedStatus: http.StatusOK,
		},
		{
			name:   "valid tools/list request",
			method: http.MethodPost,
			body: `{
				"jsonrpc": "2.0",
				"method": "tools/list",
				"id": 2
			}`,
			expectedStatus: http.StatusOK,
		},
		{
			name:           "invalid method",
			method:         http.MethodGet,
			body:           "",
			expectedStatus: http.StatusMethodNotAllowed,
		},
		{
			name:           "invalid JSON",
			method:         http.MethodPost,
			body:           "invalid json",
			expectedStatus: http.StatusOK, // JSON-RPC returns OK with error in body
		},
		{
			name:   "invalid JSON-RPC version",
			method: http.MethodPost,
			body: `{
				"jsonrpc": "1.0",
				"method": "initialize",
				"id": 1
			}`,
			expectedStatus: http.StatusOK, // JSON-RPC returns OK with error in body
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, "/", bytes.NewBufferString(tt.body))
			rec := httptest.NewRecorder()

			httpHandler.ServeHTTP(rec, req)

			if rec.Code != tt.expectedStatus {
				t.Errorf("expected status %d, got %d", tt.expectedStatus, rec.Code)
			}

			if tt.expectedStatus == http.StatusOK {
				var response JSONRPCResponse
				if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
					t.Errorf("failed to decode response: %v", err)
				}
				if response.JSONRPC != "2.0" {
					t.Errorf("expected jsonrpc 2.0, got %s", response.JSONRPC)
				}
			}
		})
	}
}

func TestHTTPHandler_WriteError(t *testing.T) {
	storage := NewMockStorage()
	server := createTestServer(t, storage, ModeHTTP)
	httpHandler := NewHTTPHandler(server)

	rec := httptest.NewRecorder()
	httpHandler.writeError(rec, ErrCodeInternalError, "test error")

	if rec.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d", http.StatusOK, rec.Code)
	}

	var response JSONRPCResponse
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Errorf("failed to decode response: %v", err)
	}

	if response.Error == nil {
		t.Error("expected error to be set")
	}

	if response.Error.Code != ErrCodeInternalError {
		t.Errorf("expected error code %d, got %d", ErrCodeInternalError, response.Error.Code)
	}

	if response.Error.Message != "test error" {
		t.Errorf("expected error message 'test error', got '%s'", response.Error.Message)
	}
}

func TestHTTPHandler_WriteErrorWithID(t *testing.T) {
	storage := NewMockStorage()
	server := createTestServer(t, storage, ModeHTTP)
	httpHandler := NewHTTPHandler(server)

	rec := httptest.NewRecorder()
	httpHandler.writeErrorWithID(rec, 123, ErrCodeInvalidRequest, "invalid request")

	var response JSONRPCResponse
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Errorf("failed to decode response: %v", err)
	}

	if response.ID != float64(123) {
		t.Errorf("expected ID 123, got %v", response.ID)
	}

	if response.Error == nil {
		t.Error("expected error to be set")
	}
}

func TestJSONRPCErrorCodes(t *testing.T) {
	tests := []struct {
		name string
		code int
	}{
		{"ParseError", ErrCodeParseError},
		{"InvalidRequest", ErrCodeInvalidRequest},
		{"MethodNotFound", ErrCodeMethodNotFound},
		{"InvalidParams", ErrCodeInvalidParams},
		{"InternalError", ErrCodeInternalError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.code >= 0 {
				t.Errorf("expected negative error code, got %d", tt.code)
			}
		})
	}
}

func TestHandleResourcesListInvalidJSON(t *testing.T) {
	storage := NewMockStorage()
	server := createTestServer(t, storage, ModeStdio)
	handler := NewRPCHandler(server)

	// Test with invalid JSON in params
	invalidJSON := json.RawMessage([]byte(`not valid json`))
	_, err := handler.handleResourcesList(context.Background(), &invalidJSON)
	if err == nil {
		t.Error("expected error for invalid JSON params")
	}
}

func TestHTTPHandler_ServeHTTP_PingRequest(t *testing.T) {
	storage := NewMockStorage()
	server := createTestServer(t, storage, ModeHTTP)
	httpHandler := NewHTTPHandler(server)

	req := httptest.NewRequest("POST", "/", bytes.NewBufferString(`{
		"jsonrpc": "2.0",
		"method": "ping",
		"id": 1
	}`))
	rec := httptest.NewRecorder()

	httpHandler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}

	var response JSONRPCResponse
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Errorf("failed to decode response: %v", err)
	}

	if response.Error != nil {
		t.Errorf("expected no error, got: %v", response.Error)
	}
}
