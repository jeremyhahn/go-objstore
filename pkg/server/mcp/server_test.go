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
	"context"
	"strings"
	"testing"
)

func TestNewServer(t *testing.T) {
	storage := NewMockStorage()

	tests := []struct {
		name      string
		config    *ServerConfig
		wantError bool
	}{
		{
			name: "valid config",
			config: &ServerConfig{
				Mode:    ModeStdio,
				Storage: storage,
			},
			wantError: false,
		},
		{
			name: "nil storage",
			config: &ServerConfig{
				Mode: ModeStdio,
			},
			wantError: true,
		},
		{
			name: "http mode with address",
			config: &ServerConfig{
				Mode:        ModeHTTP,
				HTTPAddress: ":8080",
				Storage:     storage,
			},
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server, err := NewServer(tt.config)
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
			if server == nil {
				t.Error("expected server to be created")
			}
		})
	}
}

func TestServer_ListTools(t *testing.T) {
	storage := NewMockStorage()
	server, err := NewServer(&ServerConfig{
		Mode:    ModeStdio,
		Storage: storage,
	})
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}

	tools := server.ListTools()
	if len(tools) != 19 {
		t.Errorf("expected 19 tools, got %d", len(tools))
	}

	toolNames := make(map[string]bool)
	for _, tool := range tools {
		toolNames[tool.Name] = true
	}

	expectedTools := []string{
		"objstore_put",
		"objstore_get",
		"objstore_delete",
		"objstore_list",
		"objstore_exists",
		"objstore_get_metadata",
	}

	for _, expected := range expectedTools {
		if !toolNames[expected] {
			t.Errorf("expected tool %s not found", expected)
		}
	}
}

func TestServer_CallTool(t *testing.T) {
	storage := NewMockStorage()
	server, err := NewServer(&ServerConfig{
		Mode:    ModeStdio,
		Storage: storage,
	})
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}

	// Test valid tool call
	result, err := server.CallTool(context.Background(), "objstore_put", map[string]any{
		"key":  "test.txt",
		"data": "hello world",
	})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if result == "" {
		t.Error("expected non-empty result")
	}

	// Test invalid tool name
	_, err = server.CallTool(context.Background(), "invalid_tool", map[string]any{})
	if err == nil {
		t.Error("expected error for invalid tool")
	}
	if !strings.Contains(err.Error(), "unknown tool") {
		t.Errorf("expected 'unknown tool' error, got: %v", err)
	}
}

func TestServer_ListResources(t *testing.T) {
	storage := NewMockStorage()
	server, err := NewServer(&ServerConfig{
		Mode:    ModeStdio,
		Storage: storage,
	})
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}

	// Add some test objects
	storage.PutWithContext(context.Background(), "file1.txt", strings.NewReader("data1"))
	storage.PutWithContext(context.Background(), "file2.txt", strings.NewReader("data2"))

	resources, err := server.ListResources(context.Background(), "")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if len(resources) != 2 {
		t.Errorf("expected 2 resources, got %d", len(resources))
	}
}

func TestServer_ReadResource(t *testing.T) {
	storage := NewMockStorage()
	server, err := NewServer(&ServerConfig{
		Mode:    ModeStdio,
		Storage: storage,
	})
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}

	// Add a test object
	testContent := "hello world"
	storage.PutWithContext(context.Background(), "test.txt", strings.NewReader(testContent))

	content, mimeType, err := server.ReadResource(context.Background(), "objstore://test.txt")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if content != testContent {
		t.Errorf("expected content '%s', got '%s'", testContent, content)
	}

	if mimeType == "" {
		t.Error("expected mimeType to be set")
	}

	// Test non-existent resource
	_, _, err = server.ReadResource(context.Background(), "objstore://nonexistent.txt")
	if err == nil {
		t.Error("expected error for non-existent resource")
	}
}

func TestServerMode_String(t *testing.T) {
	tests := []struct {
		mode     ServerMode
		expected string
	}{
		{ModeStdio, "stdio"},
		{ModeHTTP, "http"},
	}

	for _, tt := range tests {
		t.Run(string(tt.mode), func(t *testing.T) {
			if string(tt.mode) != tt.expected {
				t.Errorf("expected '%s', got '%s'", tt.expected, string(tt.mode))
			}
		})
	}
}

func TestStdioReadWriteCloser(t *testing.T) {
	reader := strings.NewReader("test data")
	writer := &strings.Builder{}

	rw := &stdioReadWriteCloser{
		reader: reader,
		writer: writer,
	}

	// Test Read
	buf := make([]byte, 4)
	n, err := rw.Read(buf)
	if err != nil {
		t.Errorf("unexpected read error: %v", err)
	}
	if n != 4 {
		t.Errorf("expected to read 4 bytes, got %d", n)
	}
	if string(buf) != "test" {
		t.Errorf("expected 'test', got '%s'", string(buf))
	}

	// Test Write
	n, err = rw.Write([]byte("hello"))
	if err != nil {
		t.Errorf("unexpected write error: %v", err)
	}
	if n != 5 {
		t.Errorf("expected to write 5 bytes, got %d", n)
	}
	if writer.String() != "hello" {
		t.Errorf("expected 'hello', got '%s'", writer.String())
	}

	// Test Close (should not actually close anything)
	err = rw.Close()
	if err != nil {
		t.Errorf("unexpected close error: %v", err)
	}
}

func TestServerConfig_DefaultValues(t *testing.T) {
	storage := NewMockStorage()
	server, err := NewServer(&ServerConfig{
		Mode:    ModeStdio,
		Storage: storage,
	})
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}

	// Verify default resource prefix is set
	if server.config.ResourcePrefix != "" {
		t.Errorf("expected empty resource prefix, got '%s'", server.config.ResourcePrefix)
	}
}

func TestServer_WithResourcePrefix(t *testing.T) {
	storage := NewMockStorage()
	prefix := "data/"

	server, err := NewServer(&ServerConfig{
		Mode:           ModeStdio,
		Storage:        storage,
		ResourcePrefix: prefix,
	})
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}

	if server.config.ResourcePrefix != prefix {
		t.Errorf("expected resource prefix '%s', got '%s'", prefix, server.config.ResourcePrefix)
	}

	// Add test objects
	storage.PutWithContext(context.Background(), "data/file1.txt", strings.NewReader("data1"))
	storage.PutWithContext(context.Background(), "data/file2.txt", strings.NewReader("data2"))
	storage.PutWithContext(context.Background(), "other/file3.txt", strings.NewReader("data3"))

	// List resources should only return those with the prefix
	resources, err := server.ListResources(context.Background(), "")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// Should only get files with "data/" prefix
	if len(resources) != 2 {
		t.Errorf("expected 2 resources with prefix, got %d", len(resources))
	}
}
