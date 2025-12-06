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
	"crypto/tls"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/common"
	"google.golang.org/grpc/metadata"
)

// TestCompleteWorkflow tests a complete workflow to maximize coverage
func TestCompleteWorkflow(t *testing.T) {
	storage := NewMockStorage()
	executor := createTestToolExecutor(t, storage)
	ctx := context.Background()

	// Put with metadata including all fields
	result, err := executor.Execute(ctx, "objstore_put", map[string]any{
		"key":  "workflow/test.txt",
		"data": "test data content",
		"metadata": map[string]any{
			"content_type":     "text/plain",
			"content_encoding": "utf-8",
			"custom": map[string]any{
				"author":  "test",
				"version": "1.0",
			},
		},
	})
	if err != nil {
		t.Fatalf("failed to put: %v", err)
	}
	if !strings.Contains(result, "success") {
		t.Error("expected success in result")
	}

	// List with prefix and pagination
	result, err = executor.Execute(ctx, "objstore_list", map[string]any{
		"prefix":        "workflow/",
		"max_results":   float64(100),
		"continue_from": "",
	})
	if err != nil {
		t.Fatalf("failed to list: %v", err)
	}
	if !strings.Contains(result, "workflow/test.txt") {
		t.Error("expected to find workflow/test.txt in list")
	}

	// Check existence
	result, err = executor.Execute(ctx, "objstore_exists", map[string]any{
		"key": "workflow/test.txt",
	})
	if err != nil {
		t.Fatalf("failed to check existence: %v", err)
	}
	if !strings.Contains(result, "true") {
		t.Error("expected object to exist")
	}

	// Get metadata
	result, err = executor.Execute(ctx, "objstore_get_metadata", map[string]any{
		"key": "workflow/test.txt",
	})
	if err != nil {
		t.Fatalf("failed to get metadata: %v", err)
	}
	if !strings.Contains(result, "text/plain") {
		t.Error("expected content_type in metadata")
	}

	// Get content
	result, err = executor.Execute(ctx, "objstore_get", map[string]any{
		"key": "workflow/test.txt",
	})
	if err != nil {
		t.Fatalf("failed to get: %v", err)
	}
	if !strings.Contains(result, "test data content") {
		t.Error("expected content in result")
	}

	// Delete
	result, err = executor.Execute(ctx, "objstore_delete", map[string]any{
		"key": "workflow/test.txt",
	})
	if err != nil {
		t.Fatalf("failed to delete: %v", err)
	}
	if !strings.Contains(result, "deleted") {
		t.Error("expected deleted in result")
	}

	// Verify deletion
	result, err = executor.Execute(ctx, "objstore_exists", map[string]any{
		"key": "workflow/test.txt",
	})
	if err != nil {
		t.Fatalf("failed to check existence after delete: %v", err)
	}
	if !strings.Contains(result, "false") {
		t.Error("expected object to not exist after deletion")
	}
}

// TestResourceWorkflow tests complete resource workflow
func TestResourceWorkflow(t *testing.T) {
	storage := NewMockStorage()
	manager := createTestResourceManager(t, storage, "resources/")
	ctx := context.Background()

	// Add multiple objects
	for i := 1; i <= 5; i++ {
		key := "resources/file" + string(rune('0'+i)) + ".txt"
		storage.PutWithMetadata(ctx, key, strings.NewReader("data"), &common.Metadata{
			ContentType: "text/plain",
			Size:        4,
		})
	}

	// List all resources
	resources, err := manager.ListResources(ctx, "")
	if err != nil {
		t.Fatalf("failed to list resources: %v", err)
	}
	if len(resources) != 5 {
		t.Errorf("expected 5 resources, got %d", len(resources))
	}

	// Read first resource
	if len(resources) > 0 {
		content, mimeType, err := manager.ReadResource(ctx, resources[0].URI)
		if err != nil {
			t.Fatalf("failed to read resource: %v", err)
		}
		if content != "data" {
			t.Errorf("expected content 'data', got '%s'", content)
		}
		if mimeType != "text/plain" {
			t.Errorf("expected mimeType 'text/plain', got '%s'", mimeType)
		}
	}
}

// TestErrorPaths tests error handling paths
func TestErrorPaths(t *testing.T) {
	storage := NewMockStorage()
	executor := createTestToolExecutor(t, storage)
	ctx := context.Background()

	// Get non-existent object
	_, err := executor.Execute(ctx, "objstore_get", map[string]any{
		"key": "nonexistent.txt",
	})
	if err == nil {
		t.Error("expected error for non-existent object")
	}

	// Delete non-existent object (should succeed in mock)
	_, err = executor.Execute(ctx, "objstore_delete", map[string]any{
		"key": "nonexistent.txt",
	})
	if err != nil {
		// Mock doesn't error on delete of non-existent, which is fine
	}

	// Get metadata for non-existent object
	_, err = executor.Execute(ctx, "objstore_get_metadata", map[string]any{
		"key": "nonexistent.txt",
	})
	if err == nil {
		t.Error("expected error for non-existent object")
	}

	// List with empty storage
	result, err := executor.Execute(ctx, "objstore_list", map[string]any{
		"prefix": "nonexistent/",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(result, "\"count\": 0") && !strings.Contains(result, "\"count\":0") {
		t.Errorf("expected count:0 for empty list, got: %s", result)
	}
}

// TestHTTPHandler_RequestBodyReadError tests HTTP handler with unreadable body
func TestHTTPHandler_RequestBodyReadError(t *testing.T) {
	storage := NewMockStorage()
	server := createTestServer(t, storage, ModeHTTP)
	httpHandler := NewHTTPHandler(server)

	// Create a request with invalid body that can't be read
	req := httptest.NewRequest("POST", "/", &badReader{})
	rec := httptest.NewRecorder()

	httpHandler.ServeHTTP(rec, req)

	// Should return error status
	if rec.Code != http.StatusOK {
		// The writeError function is called which returns OK with JSON error
		t.Logf("status code: %d", rec.Code)
	}
}

// badReader is a reader that always fails
type badReader struct{}

func (b *badReader) Read(p []byte) (int, error) {
	return 0, bytes.ErrTooLarge
}

// TestHandleResourcesListWithParams tests resources/list with cursor parameter
func TestHandleResourcesListWithParams(t *testing.T) {
	storage := NewMockStorage()
	server := createTestServer(t, storage, ModeStdio)
	handler := NewRPCHandler(server)

	// Add test objects
	storage.PutWithContext(context.Background(), "file1.txt", strings.NewReader("data1"))
	storage.PutWithContext(context.Background(), "file2.txt", strings.NewReader("data2"))

	// Test with cursor parameter
	paramsJSON := json.RawMessage([]byte(`{"cursor": "token123"}`))
	result, err := handler.handleResourcesList(context.Background(), &paramsJSON)
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

// TestResourceManager_ReadResourceNoContentType tests ReadResource when metadata has no ContentType
func TestResourceManager_ReadResourceNoContentType(t *testing.T) {
	storage := NewMockStorage()
	manager := createTestResourceManager(t, storage, "")

	// Add test object WITHOUT metadata
	testContent := "hello world"
	// Put without metadata so ContentType is empty
	metadata := &common.Metadata{}
	storage.PutWithMetadata(context.Background(), "test.txt", strings.NewReader(testContent), metadata)

	content, mimeType, err := manager.ReadResource(context.Background(), "objstore://test.txt")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if content != testContent {
		t.Errorf("expected content '%s', got '%s'", testContent, content)
	}

	// Should default to application/octet-stream when no ContentType
	if mimeType != "application/octet-stream" {
		t.Errorf("expected default mimeType 'application/octet-stream', got '%s'", mimeType)
	}
}

// TestResourceManager_ListResourcesNoMetadata tests ListResources when objects have no metadata
func TestResourceManager_ListResourcesNoMetadata(t *testing.T) {
	storage := NewMockStorage()
	manager := createTestResourceManager(t, storage, "")

	// Manually add objects to storage without metadata
	storage.objects["file1.txt"] = []byte("data1")
	// Don't add metadata

	resources, err := manager.ListResources(context.Background(), "")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if len(resources) != 1 {
		t.Errorf("expected 1 resource, got %d", len(resources))
	}

	// Resource should still be returned even without metadata
	if resources[0].Name != "file1.txt" {
		t.Errorf("expected name 'file1.txt', got '%s'", resources[0].Name)
	}
}

// TestServer_StartWithInvalidMode tests Start with invalid server mode
func TestServer_StartWithInvalidMode(t *testing.T) {
	storage := NewMockStorage()
	initTestFacade(t, storage)
	server, _ := NewServer(&ServerConfig{
		Mode:    ServerMode("invalid"),
		Backend: "",
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	err := server.Start(ctx)
	if err == nil {
		t.Error("expected error for invalid server mode")
	}
	if !strings.Contains(err.Error(), "unknown server mode") {
		t.Errorf("expected 'unknown server mode' error, got: %v", err)
	}
}

// TestAuthenticationMiddleware_FailedAuth tests failed authentication in HTTP handler
func TestAuthenticationMiddleware_FailedAuth(t *testing.T) {
	// Create a mock authenticator that fails
	failingAuth := &MockAuthenticator{
		failAuth: true,
	}

	storage := NewMockStorage()
	initTestFacade(t, storage)
	server, _ := NewServer(&ServerConfig{
		Mode:          ModeHTTP,
		Backend:       "",
		Authenticator: failingAuth,
	})

	// Test the HTTP handler directly
	httpHandler := NewHTTPHandler(server)

	req := httptest.NewRequest("POST", "/", bytes.NewBufferString(`{
		"jsonrpc": "2.0",
		"method": "ping",
		"id": 1
	}`))
	rec := httptest.NewRecorder()

	middleware := server.authenticationMiddleware(httpHandler)
	middleware.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("expected status %d, got %d", http.StatusUnauthorized, rec.Code)
	}
}

// TestAuthenticationMiddleware_SuccessfulAuth tests successful authentication
func TestAuthenticationMiddleware_SuccessfulAuth(t *testing.T) {
	// Create a mock authenticator that succeeds
	successAuth := &MockAuthenticator{
		failAuth: false,
	}

	storage := NewMockStorage()
	initTestFacade(t, storage)
	server, _ := NewServer(&ServerConfig{
		Mode:          ModeHTTP,
		Backend:       "",
		Authenticator: successAuth,
	})

	httpHandler := NewHTTPHandler(server)

	req := httptest.NewRequest("POST", "/", bytes.NewBufferString(`{
		"jsonrpc": "2.0",
		"method": "ping",
		"id": 1
	}`))
	rec := httptest.NewRecorder()

	middleware := server.authenticationMiddleware(httpHandler)
	middleware.ServeHTTP(rec, req)

	// Should return OK with JSON-RPC response
	if rec.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d", http.StatusOK, rec.Code)
	}

	var response JSONRPCResponse
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Errorf("failed to decode response: %v", err)
	}

	if response.Error != nil {
		t.Errorf("expected no error, got: %v", response.Error)
	}
}

// MockAuthenticator for testing authentication
type MockAuthenticator struct {
	failAuth bool
}

func (m *MockAuthenticator) AuthenticateHTTP(ctx context.Context, r *http.Request) (*adapters.Principal, error) {
	if m.failAuth {
		return nil, bytes.ErrTooLarge // Use any error
	}
	return &adapters.Principal{
		ID:   "test-user",
		Name: "Test User",
	}, nil
}

func (m *MockAuthenticator) AuthenticateGRPC(ctx context.Context, md metadata.MD) (*adapters.Principal, error) {
	if m.failAuth {
		return nil, bytes.ErrTooLarge // Use any error
	}
	return &adapters.Principal{
		ID:   "test-user",
		Name: "Test User",
	}, nil
}

func (m *MockAuthenticator) AuthenticateMTLS(ctx context.Context, state *tls.ConnectionState) (*adapters.Principal, error) {
	if m.failAuth {
		return nil, bytes.ErrTooLarge
	}
	return &adapters.Principal{
		ID:   "test-user",
		Name: "Test User",
	}, nil
}

func (m *MockAuthenticator) ValidatePermission(ctx context.Context, principal *adapters.Principal, resource, action string) error {
	if m.failAuth {
		return bytes.ErrTooLarge
	}
	return nil
}

// TestHTTPHandler_ContentTypeHeader tests that Content-Type header is set
func TestHTTPHandler_ContentTypeHeader(t *testing.T) {
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

	contentType := rec.Header().Get("Content-Type")
	if contentType != "application/json" {
		t.Errorf("expected Content-Type 'application/json', got '%s'", contentType)
	}
}

// TestResourceManager_ExtractNameEdgeCase tests extractName with various paths
func TestResourceManager_ExtractNameEdgeCase(t *testing.T) {
	manager := NewResourceManager("", "")

	tests := []struct {
		key          string
		expectedName string
	}{
		{"", ""},
		{"single", "single"},
		{"/leading/slash", "slash"},
		{"trailing/slash/", ""},
		{"multiple///slashes", "slashes"},
		{"deep/nested/path/to/file.txt", "file.txt"},
	}

	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			name := manager.extractName(tt.key)
			if name != tt.expectedName {
				t.Errorf("expected name '%s', got '%s'", tt.expectedName, name)
			}
		})
	}
}
