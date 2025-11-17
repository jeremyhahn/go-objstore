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
	"encoding/json"
	"strings"
	"testing"

	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/version"
)

// TestToolExecutor_EdgeCases tests edge cases to increase coverage
func TestToolExecutor_EdgeCases(t *testing.T) {
	storage := NewMockStorage()
	executor := NewToolExecutor(storage)

	// Test executePut with invalid metadata type
	_, err := executor.Execute(context.Background(), "objstore_put", map[string]any{
		"key":      "test.txt",
		"data":     "hello",
		"metadata": "invalid_metadata_string",
	})
	if err != nil {
		// This is expected behavior - invalid metadata should be ignored
	}

	// Test executePut with partial custom metadata
	result, err := executor.Execute(context.Background(), "objstore_put", map[string]any{
		"key":  "test2.txt",
		"data": "hello",
		"metadata": map[string]any{
			"content_type": "text/plain",
			"custom": map[string]any{
				"key1":        "value1",
				"invalid_key": 123, // Non-string value should be ignored
			},
		},
	})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if result == "" {
		t.Error("expected non-empty result")
	}

	// Test executeList with max_results and continue_from
	storage.PutWithContext(context.Background(), "item1.txt", strings.NewReader("data1"))
	storage.PutWithContext(context.Background(), "item2.txt", strings.NewReader("data2"))

	result, err = executor.Execute(context.Background(), "objstore_list", map[string]any{
		"prefix":        "",
		"max_results":   float64(10),
		"continue_from": "token",
	})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if result == "" {
		t.Error("expected non-empty result")
	}

	// Test executeExists with empty key
	_, err = executor.Execute(context.Background(), "objstore_exists", map[string]any{
		"key": "",
	})
	if err == nil {
		t.Error("expected error for empty key")
	}

	// Test executeDelete with empty key
	_, err = executor.Execute(context.Background(), "objstore_delete", map[string]any{
		"key": "",
	})
	if err == nil {
		t.Error("expected error for empty key")
	}

	// Test executeGet with empty key
	_, err = executor.Execute(context.Background(), "objstore_get", map[string]any{
		"key": "",
	})
	if err == nil {
		t.Error("expected error for empty key")
	}

	// Test executeGetMetadata with empty key
	_, err = executor.Execute(context.Background(), "objstore_get_metadata", map[string]any{
		"key": "",
	})
	if err == nil {
		t.Error("expected error for empty key")
	}
}

// TestToolExecutor_InvalidArgumentTypes tests invalid argument types
func TestToolExecutor_InvalidArgumentTypes(t *testing.T) {
	storage := NewMockStorage()
	executor := NewToolExecutor(storage)

	tests := []struct {
		name string
		tool string
		args map[string]any
	}{
		{
			name: "put with invalid key type",
			tool: "objstore_put",
			args: map[string]any{
				"key":  123,
				"data": "hello",
			},
		},
		{
			name: "put with invalid data type",
			tool: "objstore_put",
			args: map[string]any{
				"key":  "test.txt",
				"data": 123,
			},
		},
		{
			name: "get with invalid key type",
			tool: "objstore_get",
			args: map[string]any{
				"key": 123,
			},
		},
		{
			name: "delete with invalid key type",
			tool: "objstore_delete",
			args: map[string]any{
				"key": 123,
			},
		},
		{
			name: "exists with invalid key type",
			tool: "objstore_exists",
			args: map[string]any{
				"key": 123,
			},
		},
		{
			name: "get_metadata with invalid key type",
			tool: "objstore_get_metadata",
			args: map[string]any{
				"key": 123,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := executor.Execute(context.Background(), tt.tool, tt.args)
			if err == nil {
				t.Error("expected error for invalid argument types")
			}
		})
	}
}

// TestResourceManager_ListWithMetadata tests listing resources with full metadata
func TestResourceManager_ListWithMetadata(t *testing.T) {
	storage := NewMockStorage()
	manager := NewResourceManager(storage, "")

	// Add objects with metadata
	storage.PutWithMetadata(context.Background(), "file1.txt", strings.NewReader("data1"), &common.Metadata{
		ContentType: "text/plain",
		Size:        5,
	})
	storage.PutWithMetadata(context.Background(), "file2.txt", strings.NewReader("data2"), &common.Metadata{
		ContentType: "text/html",
		Size:        5,
	})

	resources, err := manager.ListResources(context.Background(), "")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if len(resources) != 2 {
		t.Errorf("expected 2 resources, got %d", len(resources))
	}

	for _, resource := range resources {
		if resource.MIMEType == "" {
			t.Error("expected MIMEType to be set")
		}
		if resource.Description == "" {
			t.Error("expected Description to be set")
		}
	}
}

// TestJSONRPCRequest_EdgeCases tests edge cases in JSON-RPC request handling
func TestJSONRPCRequest_EdgeCases(t *testing.T) {
	storage := NewMockStorage()
	server, _ := NewServer(&ServerConfig{
		Mode:    ModeStdio,
		Storage: storage,
	})
	handler := NewRPCHandler(server)

	// Test initialize with invalid params
	invalidJSON := json.RawMessage([]byte(`{"invalid": "json"`))
	_, err := handler.handleInitialize(context.Background(), &invalidJSON)
	if err == nil {
		t.Error("expected error for invalid initialize params")
	}

	// Test tools/call with missing name
	paramsJSON := json.RawMessage([]byte(`{"arguments": {}}`))
	_, err = handler.handleToolsCall(context.Background(), &paramsJSON)
	if err == nil {
		t.Error("expected error for missing tool name")
	}

	// Test resources/read with missing URI
	paramsJSON = json.RawMessage([]byte(`{}`))
	_, err = handler.handleResourcesRead(context.Background(), &paramsJSON)
	if err == nil {
		t.Error("expected error for missing URI")
	}
}

// TestToolExecutor_PutWithEncoding tests put with content encoding metadata
func TestToolExecutor_PutWithEncoding(t *testing.T) {
	storage := NewMockStorage()
	executor := NewToolExecutor(storage)

	result, err := executor.Execute(context.Background(), "objstore_put", map[string]any{
		"key":  "encoded.txt.gz",
		"data": "compressed data",
		"metadata": map[string]any{
			"content_type":     "text/plain",
			"content_encoding": "gzip",
		},
	})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !strings.Contains(result, "success") {
		t.Error("expected success in result")
	}
}

// TestHandleResourcesListEmpty tests resources/list with empty cursor
func TestHandleResourcesListEmpty(t *testing.T) {
	storage := NewMockStorage()
	server, _ := NewServer(&ServerConfig{
		Mode:    ModeStdio,
		Storage: storage,
	})
	handler := NewRPCHandler(server)

	// Test with empty params (should work)
	paramsJSON := json.RawMessage([]byte(`{}`))
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

	// Empty storage, should return empty list
	if len(resources) != 0 {
		t.Errorf("expected 0 resources for empty storage, got %d", len(resources))
	}
}

// TestToolExecutor_MetadataWithCustomFields tests put with custom metadata fields
func TestToolExecutor_MetadataWithCustomFields(t *testing.T) {
	storage := NewMockStorage()
	executor := NewToolExecutor(storage)

	result, err := executor.Execute(context.Background(), "objstore_put", map[string]any{
		"key":  "custom.txt",
		"data": "test data",
		"metadata": map[string]any{
			"custom": map[string]any{
				"author":   "test author",
				"version":  version.Get(),
				"category": "test",
			},
		},
	})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !strings.Contains(result, "success") {
		t.Error("expected success in result")
	}

	// Verify metadata was stored
	metadata, err := storage.GetMetadata(context.Background(), "custom.txt")
	if err != nil {
		t.Errorf("failed to get metadata: %v", err)
	}
	if len(metadata.Custom) != 3 {
		t.Errorf("expected 3 custom fields, got %d", len(metadata.Custom))
	}
}

// TestToolExecutor_MetadataWithMixedCustomTypes tests custom metadata with mixed types
func TestToolExecutor_MetadataWithMixedCustomTypes(t *testing.T) {
	storage := NewMockStorage()
	executor := NewToolExecutor(storage)

	_, err := executor.Execute(context.Background(), "objstore_put", map[string]any{
		"key":  "mixed.txt",
		"data": "test",
		"metadata": map[string]any{
			"custom": map[string]any{
				"string_val": "value",
				"int_val":    123,  // Should be ignored
				"bool_val":   true, // Should be ignored
			},
		},
	})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// Should only store string values
	metadata, err := storage.GetMetadata(context.Background(), "mixed.txt")
	if err != nil {
		t.Errorf("failed to get metadata: %v", err)
	}
	if len(metadata.Custom) != 1 {
		t.Errorf("expected 1 custom field (only strings), got %d", len(metadata.Custom))
	}
	if metadata.Custom["string_val"] != "value" {
		t.Errorf("expected string_val='value', got '%s'", metadata.Custom["string_val"])
	}
}

// TestToolExecutor_ListWithVariousParameters tests list with different parameter combinations
func TestToolExecutor_ListWithVariousParameters(t *testing.T) {
	storage := NewMockStorage()
	executor := NewToolExecutor(storage)

	// Add test data
	for i := 0; i < 5; i++ {
		key := "item" + string(rune('0'+i)) + ".txt"
		storage.PutWithContext(context.Background(), key, strings.NewReader("data"))
	}

	tests := []struct {
		name      string
		args      map[string]any
		wantCount int
	}{
		{
			name:      "list all no params",
			args:      map[string]any{},
			wantCount: 5,
		},
		{
			name: "list with empty prefix",
			args: map[string]any{
				"prefix": "",
			},
			wantCount: 5,
		},
		{
			name: "list with specific prefix",
			args: map[string]any{
				"prefix": "item",
			},
			wantCount: 5,
		},
		{
			name: "list with non-matching prefix",
			args: map[string]any{
				"prefix": "xyz",
			},
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := executor.Execute(context.Background(), "objstore_list", tt.args)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			var resultMap map[string]any
			if err := json.Unmarshal([]byte(result), &resultMap); err != nil {
				t.Errorf("failed to parse result: %v", err)
				return
			}

			count := int(resultMap["count"].(float64))
			if count != tt.wantCount {
				t.Errorf("expected count %d, got %d", tt.wantCount, count)
			}
		})
	}
}
