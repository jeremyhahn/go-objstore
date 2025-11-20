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

package server_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	errMCPRPC = errors.New("RPC error")

	mcpServerAddr string
	mcpClient     *http.Client
)

// JSONRPCRequest represents a JSON-RPC 2.0 request
type JSONRPCRequest struct {
	JSONRPC string         `json:"jsonrpc"`
	Method  string         `json:"method"`
	Params  map[string]any `json:"params,omitempty"`
	ID      any            `json:"id"`
}

// JSONRPCResponse represents a JSON-RPC 2.0 response
type JSONRPCResponse struct {
	JSONRPC string         `json:"jsonrpc"`
	Result  map[string]any `json:"result,omitempty"`
	Error   *JSONRPCError  `json:"error,omitempty"`
	ID      any            `json:"id"`
}

// JSONRPCError represents a JSON-RPC 2.0 error
type JSONRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

// setupMCPClient initializes the MCP HTTP client
func setupMCPClient() {
	mcpServerAddr = os.Getenv("MCP_SERVER_ADDR")
	if mcpServerAddr == "" {
		mcpServerAddr = "http://localhost:8081"
	}

	mcpClient = &http.Client{
		Timeout: 30 * time.Second,
	}

	// Wait for server to be ready
	maxRetries := 30
	for i := 0; i < maxRetries; i++ {
		resp, err := mcpClient.Get(mcpServerAddr + "/health")
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			return
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(1 * time.Second)
	}
}

func init() {
	setupMCPClient()
}

// callJSONRPC makes a JSON-RPC call to the MCP server
func callJSONRPC(method string, params map[string]any) (*JSONRPCResponse, error) {
	req := JSONRPCRequest{
		JSONRPC: "2.0",
		Method:  method,
		Params:  params,
		ID:      1,
	}

	reqBody, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	httpReq, err := http.NewRequest(http.MethodPost, mcpServerAddr+"/jsonrpc", bytes.NewReader(reqBody))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	httpResp, err := mcpClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer httpResp.Body.Close()

	var resp JSONRPCResponse
	if err := json.NewDecoder(httpResp.Body).Decode(&resp); err != nil {
		return nil, err
	}

	return &resp, nil
}

// TestMCPProtocolCompliance tests JSON-RPC 2.0 protocol compliance
func TestMCPProtocolCompliance(t *testing.T) {
	t.Run("jsonrpc version field", func(t *testing.T) {
		resp, err := callJSONRPC("tools/list", nil)
		require.NoError(t, err)
		assert.Equal(t, "2.0", resp.JSONRPC)
	})

	t.Run("request id echoed in response", func(t *testing.T) {
		req := JSONRPCRequest{
			JSONRPC: "2.0",
			Method:  "tools/list",
			ID:      12345,
		}

		reqBody, err := json.Marshal(req)
		require.NoError(t, err)

		httpReq, err := http.NewRequest(http.MethodPost, mcpServerAddr+"/jsonrpc", bytes.NewReader(reqBody))
		require.NoError(t, err)
		httpReq.Header.Set("Content-Type", "application/json")

		httpResp, err := mcpClient.Do(httpReq)
		require.NoError(t, err)
		defer httpResp.Body.Close()

		var resp JSONRPCResponse
		err = json.NewDecoder(httpResp.Body).Decode(&resp)
		require.NoError(t, err)

		assert.Equal(t, float64(12345), resp.ID)
	})

	t.Run("invalid json returns error", func(t *testing.T) {
		httpReq, err := http.NewRequest(http.MethodPost, mcpServerAddr+"/jsonrpc", bytes.NewReader([]byte("{invalid json")))
		require.NoError(t, err)
		httpReq.Header.Set("Content-Type", "application/json")

		httpResp, err := mcpClient.Do(httpReq)
		require.NoError(t, err)
		defer httpResp.Body.Close()

		var resp JSONRPCResponse
		err = json.NewDecoder(httpResp.Body).Decode(&resp)
		require.NoError(t, err)

		assert.NotNil(t, resp.Error)
		assert.Equal(t, -32700, resp.Error.Code) // Parse error
	})

	t.Run("unknown method returns error", func(t *testing.T) {
		resp, err := callJSONRPC("unknown/method", nil)
		require.NoError(t, err)
		assert.NotNil(t, resp.Error)
		assert.Equal(t, -32601, resp.Error.Code) // Method not found
	})
}

// TestMCPToolsList tests the tools/list method
func TestMCPToolsList(t *testing.T) {
	t.Run("list all tools", func(t *testing.T) {
		resp, err := callJSONRPC("tools/list", nil)
		require.NoError(t, err)
		assert.Nil(t, resp.Error)
		assert.NotNil(t, resp.Result)

		tools, ok := resp.Result["tools"].([]any)
		require.True(t, ok)

		// Verify all 6 MCP tools are present
		expectedTools := []string{
			"objstore_put",
			"objstore_get",
			"objstore_delete",
			"objstore_list",
			"objstore_exists",
			"objstore_get_metadata",
		}

		toolNames := make(map[string]bool)
		for _, tool := range tools {
			toolMap := tool.(map[string]any)
			name := toolMap["name"].(string)
			toolNames[name] = true

			// Verify tool has required fields
			assert.NotEmpty(t, toolMap["description"])
			assert.NotNil(t, toolMap["inputSchema"])
		}

		for _, expectedTool := range expectedTools {
			assert.True(t, toolNames[expectedTool], "Tool %s should be present", expectedTool)
		}
	})
}

// TestMCPToolObjectstorePut tests the objstore_put tool
func TestMCPToolObjectstorePut(t *testing.T) {
	t.Run("put object via MCP", func(t *testing.T) {
		params := map[string]any{
			"name": "objstore_put",
			"arguments": map[string]any{
				"key":  "test/mcp/simple.txt",
				"data": "Hello, MCP!",
			},
		}

		resp, err := callJSONRPC("tools/call", params)
		require.NoError(t, err)
		assert.Nil(t, resp.Error)

		result := resp.Result["content"].([]any)[0].(map[string]any)
		text := result["text"].(string)

		var putResult map[string]any
		err = json.Unmarshal([]byte(text), &putResult)
		require.NoError(t, err)

		assert.Equal(t, true, putResult["success"])
		assert.Equal(t, "test/mcp/simple.txt", putResult["key"])
	})

	t.Run("put with metadata", func(t *testing.T) {
		params := map[string]any{
			"name": "objstore_put",
			"arguments": map[string]any{
				"key":  "test/mcp/metadata.txt",
				"data": "Data with metadata",
				"metadata": map[string]any{
					"content_type": "text/plain",
					"custom": map[string]any{
						"author": "test",
					},
				},
			},
		}

		resp, err := callJSONRPC("tools/call", params)
		require.NoError(t, err)
		assert.Nil(t, resp.Error)
	})

	t.Run("put without required key", func(t *testing.T) {
		params := map[string]any{
			"name": "objstore_put",
			"arguments": map[string]any{
				"data": "test data",
			},
		}

		resp, err := callJSONRPC("tools/call", params)
		require.NoError(t, err)
		assert.NotNil(t, resp.Error)
	})
}

// TestMCPToolObjectstoreGet tests the objstore_get tool
func TestMCPToolObjectstoreGet(t *testing.T) {
	// Setup: put an object first
	putParams := map[string]any{
		"name": "objstore_put",
		"arguments": map[string]any{
			"key":  "test/mcp/get.txt",
			"data": "Content to retrieve",
		},
	}
	_, err := callJSONRPC("tools/call", putParams)
	require.NoError(t, err)

	t.Run("get existing object", func(t *testing.T) {
		params := map[string]any{
			"name": "objstore_get",
			"arguments": map[string]any{
				"key": "test/mcp/get.txt",
			},
		}

		resp, err := callJSONRPC("tools/call", params)
		require.NoError(t, err)
		assert.Nil(t, resp.Error)

		result := resp.Result["content"].([]any)[0].(map[string]any)
		text := result["text"].(string)

		var getResult map[string]any
		err = json.Unmarshal([]byte(text), &getResult)
		require.NoError(t, err)

		assert.Equal(t, true, getResult["success"])
		assert.Equal(t, "Content to retrieve", getResult["data"])
	})

	t.Run("get non-existent object", func(t *testing.T) {
		params := map[string]any{
			"name": "objstore_get",
			"arguments": map[string]any{
				"key": "test/mcp/non-existent.txt",
			},
		}

		resp, err := callJSONRPC("tools/call", params)
		require.NoError(t, err)
		assert.NotNil(t, resp.Error)
	})
}

// TestMCPToolObjectstoreDelete tests the objstore_delete tool
func TestMCPToolObjectstoreDelete(t *testing.T) {
	// Setup: put an object
	putParams := map[string]any{
		"name": "objstore_put",
		"arguments": map[string]any{
			"key":  "test/mcp/delete.txt",
			"data": "To be deleted",
		},
	}
	_, err := callJSONRPC("tools/call", putParams)
	require.NoError(t, err)

	t.Run("delete existing object", func(t *testing.T) {
		params := map[string]any{
			"name": "objstore_delete",
			"arguments": map[string]any{
				"key": "test/mcp/delete.txt",
			},
		}

		resp, err := callJSONRPC("tools/call", params)
		require.NoError(t, err)
		assert.Nil(t, resp.Error)

		result := resp.Result["content"].([]any)[0].(map[string]any)
		text := result["text"].(string)

		var delResult map[string]any
		err = json.Unmarshal([]byte(text), &delResult)
		require.NoError(t, err)

		assert.Equal(t, true, delResult["success"])
		assert.Equal(t, true, delResult["deleted"])
	})
}

// TestMCPToolObjectstoreList tests the objstore_list tool
func TestMCPToolObjectstoreList(t *testing.T) {
	// Setup: put multiple objects
	keys := []string{
		"test/mcp/list/file1.txt",
		"test/mcp/list/file2.txt",
		"test/mcp/list/file3.txt",
	}

	for _, key := range keys {
		params := map[string]any{
			"name": "objstore_put",
			"arguments": map[string]any{
				"key":  key,
				"data": fmt.Sprintf("content of %s", key),
			},
		}
		_, err := callJSONRPC("tools/call", params)
		require.NoError(t, err)
	}

	t.Run("list with prefix", func(t *testing.T) {
		params := map[string]any{
			"name": "objstore_list",
			"arguments": map[string]any{
				"prefix": "test/mcp/list/",
			},
		}

		resp, err := callJSONRPC("tools/call", params)
		require.NoError(t, err)
		assert.Nil(t, resp.Error)

		result := resp.Result["content"].([]any)[0].(map[string]any)
		text := result["text"].(string)

		var listResult map[string]any
		err = json.Unmarshal([]byte(text), &listResult)
		require.NoError(t, err)

		assert.Equal(t, true, listResult["success"])
		assert.GreaterOrEqual(t, int(listResult["count"].(float64)), 3)
	})

	t.Run("list with max_results", func(t *testing.T) {
		params := map[string]any{
			"name": "objstore_list",
			"arguments": map[string]any{
				"prefix":      "test/mcp/list/",
				"max_results": 2,
			},
		}

		resp, err := callJSONRPC("tools/call", params)
		require.NoError(t, err)
		assert.Nil(t, resp.Error)

		result := resp.Result["content"].([]any)[0].(map[string]any)
		text := result["text"].(string)

		var listResult map[string]any
		err = json.Unmarshal([]byte(text), &listResult)
		require.NoError(t, err)

		keys := listResult["keys"].([]any)
		assert.LessOrEqual(t, len(keys), 2)
	})
}

// TestMCPToolObjectstoreExists tests the objstore_exists tool
func TestMCPToolObjectstoreExists(t *testing.T) {
	// Setup: put an object
	putParams := map[string]any{
		"name": "objstore_put",
		"arguments": map[string]any{
			"key":  "test/mcp/exists.txt",
			"data": "exists test",
		},
	}
	_, err := callJSONRPC("tools/call", putParams)
	require.NoError(t, err)

	t.Run("exists for existing object", func(t *testing.T) {
		params := map[string]any{
			"name": "objstore_exists",
			"arguments": map[string]any{
				"key": "test/mcp/exists.txt",
			},
		}

		resp, err := callJSONRPC("tools/call", params)
		require.NoError(t, err)
		assert.Nil(t, resp.Error)

		result := resp.Result["content"].([]any)[0].(map[string]any)
		text := result["text"].(string)

		var existsResult map[string]any
		err = json.Unmarshal([]byte(text), &existsResult)
		require.NoError(t, err)

		assert.Equal(t, true, existsResult["exists"])
	})

	t.Run("exists for non-existent object", func(t *testing.T) {
		params := map[string]any{
			"name": "objstore_exists",
			"arguments": map[string]any{
				"key": "test/mcp/non-existent-exists.txt",
			},
		}

		resp, err := callJSONRPC("tools/call", params)
		require.NoError(t, err)
		assert.Nil(t, resp.Error)

		result := resp.Result["content"].([]any)[0].(map[string]any)
		text := result["text"].(string)

		var existsResult map[string]any
		err = json.Unmarshal([]byte(text), &existsResult)
		require.NoError(t, err)

		assert.Equal(t, false, existsResult["exists"])
	})
}

// TestMCPToolObjectstoreGetMetadata tests the objstore_get_metadata tool
func TestMCPToolObjectstoreGetMetadata(t *testing.T) {
	// Setup: put an object with metadata
	putParams := map[string]any{
		"name": "objstore_put",
		"arguments": map[string]any{
			"key":  "test/mcp/get-metadata.txt",
			"data": "test content",
			"metadata": map[string]any{
				"content_type": "text/plain",
				"custom": map[string]any{
					"key1": "value1",
				},
			},
		},
	}
	_, err := callJSONRPC("tools/call", putParams)
	require.NoError(t, err)

	t.Run("get metadata for existing object", func(t *testing.T) {
		params := map[string]any{
			"name": "objstore_get_metadata",
			"arguments": map[string]any{
				"key": "test/mcp/get-metadata.txt",
			},
		}

		resp, err := callJSONRPC("tools/call", params)
		require.NoError(t, err)
		assert.Nil(t, resp.Error)

		result := resp.Result["content"].([]any)[0].(map[string]any)
		text := result["text"].(string)

		var metadataResult map[string]any
		err = json.Unmarshal([]byte(text), &metadataResult)
		require.NoError(t, err)

		assert.Equal(t, true, metadataResult["success"])
		assert.NotNil(t, metadataResult["size"])
		assert.Equal(t, "text/plain", metadataResult["content_type"])
	})
}

// TestMCPResourceManagement tests MCP resource management
func TestMCPResourceManagement(t *testing.T) {
	t.Run("list resources", func(t *testing.T) {
		resp, err := callJSONRPC("resources/list", nil)
		require.NoError(t, err)
		assert.Nil(t, resp.Error)

		resources, ok := resp.Result["resources"].([]any)
		assert.True(t, ok)
		assert.NotNil(t, resources)
	})

	t.Run("read resource", func(t *testing.T) {
		// First, put an object to create a resource
		putParams := map[string]any{
			"name": "objstore_put",
			"arguments": map[string]any{
				"key":  "test/mcp/resource.txt",
				"data": "resource content",
			},
		}
		_, err := callJSONRPC("tools/call", putParams)
		require.NoError(t, err)

		// Try to read the resource
		params := map[string]any{
			"uri": "objstore://test/mcp/resource.txt",
		}

		resp, err := callJSONRPC("resources/read", params)
		require.NoError(t, err)
		// Result depends on implementation
		if resp.Error == nil {
			assert.NotNil(t, resp.Result)
		}
	})
}

// TestMCPConcurrency tests concurrent MCP operations
func TestMCPConcurrency(t *testing.T) {
	t.Run("concurrent tool calls", func(t *testing.T) {
		numOps := 20
		errChan := make(chan error, numOps)

		for i := 0; i < numOps; i++ {
			go func(index int) {
				params := map[string]any{
					"name": "objstore_put",
					"arguments": map[string]any{
						"key":  fmt.Sprintf("test/mcp/concurrent/%d.txt", index),
						"data": fmt.Sprintf("content %d", index),
					},
				}

				resp, err := callJSONRPC("tools/call", params)
				if err != nil {
					errChan <- err
					return
				}
				if resp.Error != nil {
					errChan <- fmt.Errorf("%w: %s", errMCPRPC, resp.Error.Message)
					return
				}
				errChan <- nil
			}(i)
		}

		for i := 0; i < numOps; i++ {
			err := <-errChan
			assert.NoError(t, err)
		}
	})
}
