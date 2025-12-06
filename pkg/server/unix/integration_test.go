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
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"net"
	"testing"
	"time"
)

// sendRequest sends a JSON-RPC request and returns the response
func sendRequest(t *testing.T, conn net.Conn, req *Request) *Response {
	t.Helper()

	// Marshal request
	reqBytes, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("failed to marshal request: %v", err)
	}

	// Send request with newline
	_, err = conn.Write(append(reqBytes, '\n'))
	if err != nil {
		t.Fatalf("failed to write request: %v", err)
	}

	// Read response
	reader := bufio.NewReader(conn)
	line, err := reader.ReadBytes('\n')
	if err != nil {
		t.Fatalf("failed to read response: %v", err)
	}

	// Unmarshal response
	var resp Response
	if err := json.Unmarshal(line, &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	return &resp
}

func TestIntegration_PingPong(t *testing.T) {
	storage := NewMockStorage()
	socketPath := tempSocketPath(t)
	server := createTestServer(t, storage, socketPath)
	defer cleanupSocket(t, socketPath)

	// Start server
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go server.Start(ctx)
	time.Sleep(100 * time.Millisecond)

	// Connect to server
	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	// Send ping request
	resp := sendRequest(t, conn, &Request{
		JSONRPC: jsonRPCVersion,
		Method:  MethodPing,
		ID:      1,
	})

	if resp.Error != nil {
		t.Fatalf("unexpected error: %s", resp.Error.Message)
	}

	// Check result
	resultBytes, _ := json.Marshal(resp.Result)
	var result HealthResult
	if err := json.Unmarshal(resultBytes, &result); err != nil {
		t.Fatalf("failed to unmarshal result: %v", err)
	}

	if result.Status != "ok" {
		t.Errorf("got status %q, want %q", result.Status, "ok")
	}
}

func TestIntegration_PutGetDelete(t *testing.T) {
	storage := NewMockStorage()
	socketPath := tempSocketPath(t)
	server := createTestServer(t, storage, socketPath)
	defer cleanupSocket(t, socketPath)

	// Start server
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go server.Start(ctx)
	time.Sleep(100 * time.Millisecond)

	// Connect to server
	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	testKey := "test/integration.txt"
	testData := "Hello, Unix Socket!"

	// PUT
	putParams, _ := json.Marshal(PutParams{
		Key:  testKey,
		Data: base64.StdEncoding.EncodeToString([]byte(testData)),
	})

	putResp := sendRequest(t, conn, &Request{
		JSONRPC: jsonRPCVersion,
		Method:  MethodPut,
		Params:  putParams,
		ID:      1,
	})

	if putResp.Error != nil {
		t.Fatalf("PUT failed: %s", putResp.Error.Message)
	}

	// EXISTS
	existsParams, _ := json.Marshal(ExistsParams{Key: testKey})
	existsResp := sendRequest(t, conn, &Request{
		JSONRPC: jsonRPCVersion,
		Method:  MethodExists,
		Params:  existsParams,
		ID:      2,
	})

	if existsResp.Error != nil {
		t.Fatalf("EXISTS failed: %s", existsResp.Error.Message)
	}

	existsResultBytes, _ := json.Marshal(existsResp.Result)
	var existsResult ExistsResult
	json.Unmarshal(existsResultBytes, &existsResult)

	if !existsResult.Exists {
		t.Error("EXISTS returned false, expected true")
	}

	// GET
	getParams, _ := json.Marshal(GetParams{Key: testKey})
	getResp := sendRequest(t, conn, &Request{
		JSONRPC: jsonRPCVersion,
		Method:  MethodGet,
		Params:  getParams,
		ID:      3,
	})

	if getResp.Error != nil {
		t.Fatalf("GET failed: %s", getResp.Error.Message)
	}

	getResultBytes, _ := json.Marshal(getResp.Result)
	var getResult GetResult
	json.Unmarshal(getResultBytes, &getResult)

	decoded, _ := base64.StdEncoding.DecodeString(getResult.Data)
	if string(decoded) != testData {
		t.Errorf("GET returned %q, want %q", string(decoded), testData)
	}

	// DELETE
	deleteParams, _ := json.Marshal(DeleteParams{Key: testKey})
	deleteResp := sendRequest(t, conn, &Request{
		JSONRPC: jsonRPCVersion,
		Method:  MethodDelete,
		Params:  deleteParams,
		ID:      4,
	})

	if deleteResp.Error != nil {
		t.Fatalf("DELETE failed: %s", deleteResp.Error.Message)
	}

	// Verify deleted
	existsResp2 := sendRequest(t, conn, &Request{
		JSONRPC: jsonRPCVersion,
		Method:  MethodExists,
		Params:  existsParams,
		ID:      5,
	})

	existsResultBytes2, _ := json.Marshal(existsResp2.Result)
	var existsResult2 ExistsResult
	json.Unmarshal(existsResultBytes2, &existsResult2)

	if existsResult2.Exists {
		t.Error("EXISTS returned true after delete, expected false")
	}
}

func TestIntegration_List(t *testing.T) {
	storage := NewMockStorage()
	storage.objects["test/file1.txt"] = []byte("data1")
	storage.objects["test/file2.txt"] = []byte("data2")
	storage.objects["other/file3.txt"] = []byte("data3")

	socketPath := tempSocketPath(t)
	server := createTestServer(t, storage, socketPath)
	defer cleanupSocket(t, socketPath)

	// Start server
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go server.Start(ctx)
	time.Sleep(100 * time.Millisecond)

	// Connect to server
	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	// LIST with prefix
	listParams, _ := json.Marshal(ListParams{Prefix: "test/"})
	listResp := sendRequest(t, conn, &Request{
		JSONRPC: jsonRPCVersion,
		Method:  MethodList,
		Params:  listParams,
		ID:      1,
	})

	if listResp.Error != nil {
		t.Fatalf("LIST failed: %s", listResp.Error.Message)
	}

	listResultBytes, _ := json.Marshal(listResp.Result)
	var listResult ListResult
	json.Unmarshal(listResultBytes, &listResult)

	if len(listResult.Objects) != 2 {
		t.Errorf("LIST returned %d objects, want 2", len(listResult.Objects))
	}
}

func TestIntegration_PutWithMetadata(t *testing.T) {
	storage := NewMockStorage()
	socketPath := tempSocketPath(t)
	server := createTestServer(t, storage, socketPath)
	defer cleanupSocket(t, socketPath)

	// Start server
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go server.Start(ctx)
	time.Sleep(100 * time.Millisecond)

	// Connect to server
	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	testKey := "test/with-metadata.txt"

	// PUT with metadata
	putParams, _ := json.Marshal(PutParams{
		Key:  testKey,
		Data: base64.StdEncoding.EncodeToString([]byte("content")),
		Metadata: &MetadataParams{
			ContentType:     "text/plain",
			ContentEncoding: "utf-8",
			Custom: map[string]string{
				"author": "test",
			},
		},
	})

	putResp := sendRequest(t, conn, &Request{
		JSONRPC: jsonRPCVersion,
		Method:  MethodPut,
		Params:  putParams,
		ID:      1,
	})

	if putResp.Error != nil {
		t.Fatalf("PUT failed: %s", putResp.Error.Message)
	}

	// GET METADATA
	metaParams, _ := json.Marshal(GetMetadataParams{Key: testKey})
	metaResp := sendRequest(t, conn, &Request{
		JSONRPC: jsonRPCVersion,
		Method:  MethodGetMetadata,
		Params:  metaParams,
		ID:      2,
	})

	if metaResp.Error != nil {
		t.Fatalf("GET_METADATA failed: %s", metaResp.Error.Message)
	}

	metaResultBytes, _ := json.Marshal(metaResp.Result)
	var metaResult MetadataParams
	json.Unmarshal(metaResultBytes, &metaResult)

	if metaResult.ContentType != "text/plain" {
		t.Errorf("got content-type %q, want %q", metaResult.ContentType, "text/plain")
	}
}

func TestIntegration_UpdateMetadata(t *testing.T) {
	storage := NewMockStorage()
	storage.objects["test/update-meta.txt"] = []byte("data")
	storage.metadata["test/update-meta.txt"] = nil

	socketPath := tempSocketPath(t)
	server := createTestServer(t, storage, socketPath)
	defer cleanupSocket(t, socketPath)

	// Start server
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go server.Start(ctx)
	time.Sleep(100 * time.Millisecond)

	// Connect to server
	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	// UPDATE METADATA
	updateParams, _ := json.Marshal(UpdateMetadataParams{
		Key: "test/update-meta.txt",
		Metadata: &MetadataParams{
			ContentType: "application/json",
		},
	})

	updateResp := sendRequest(t, conn, &Request{
		JSONRPC: jsonRPCVersion,
		Method:  MethodUpdateMetadata,
		Params:  updateParams,
		ID:      1,
	})

	if updateResp.Error != nil {
		t.Fatalf("UPDATE_METADATA failed: %s", updateResp.Error.Message)
	}
}

func TestIntegration_ErrorResponses(t *testing.T) {
	storage := NewMockStorage()
	socketPath := tempSocketPath(t)
	server := createTestServer(t, storage, socketPath)
	defer cleanupSocket(t, socketPath)

	// Start server
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go server.Start(ctx)
	time.Sleep(100 * time.Millisecond)

	// Connect to server
	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	tests := []struct {
		name    string
		req     *Request
		errCode int
	}{
		{
			name: "method not found",
			req: &Request{
				JSONRPC: jsonRPCVersion,
				Method:  "nonexistent",
				ID:      1,
			},
			errCode: ErrCodeMethodNotFound,
		},
		{
			name: "get missing key parameter",
			req: &Request{
				JSONRPC: jsonRPCVersion,
				Method:  MethodGet,
				Params:  json.RawMessage(`{"key":""}`),
				ID:      2,
			},
			errCode: ErrCodeInvalidParams,
		},
		{
			name: "get non-existent key",
			req: &Request{
				JSONRPC: jsonRPCVersion,
				Method:  MethodGet,
				Params:  json.RawMessage(`{"key":"nonexistent"}`),
				ID:      3,
			},
			errCode: ErrCodeInternalError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := sendRequest(t, conn, tt.req)

			if resp.Error == nil {
				t.Error("expected error but got nil")
				return
			}

			if resp.Error.Code != tt.errCode {
				t.Errorf("got error code %d, want %d", resp.Error.Code, tt.errCode)
			}
		})
	}
}

func TestIntegration_MultipleConnections(t *testing.T) {
	storage := NewMockStorage()
	socketPath := tempSocketPath(t)
	server := createTestServer(t, storage, socketPath)
	defer cleanupSocket(t, socketPath)

	// Start server
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go server.Start(ctx)
	time.Sleep(100 * time.Millisecond)

	// Connect multiple clients
	numClients := 5
	conns := make([]net.Conn, numClients)
	for i := 0; i < numClients; i++ {
		conn, err := net.Dial("unix", socketPath)
		if err != nil {
			t.Fatalf("failed to connect client %d: %v", i, err)
		}
		defer conn.Close()
		conns[i] = conn
	}

	// Each client sends a ping
	for i, conn := range conns {
		resp := sendRequest(t, conn, &Request{
			JSONRPC: jsonRPCVersion,
			Method:  MethodPing,
			ID:      i,
		})

		if resp.Error != nil {
			t.Errorf("client %d: unexpected error: %s", i, resp.Error.Message)
		}
	}
}

func TestIntegration_MultipleRequests(t *testing.T) {
	storage := NewMockStorage()
	socketPath := tempSocketPath(t)
	server := createTestServer(t, storage, socketPath)
	defer cleanupSocket(t, socketPath)

	// Start server
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go server.Start(ctx)
	time.Sleep(100 * time.Millisecond)

	// Connect to server
	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	// Send multiple requests on same connection
	numRequests := 10
	for i := 0; i < numRequests; i++ {
		resp := sendRequest(t, conn, &Request{
			JSONRPC: jsonRPCVersion,
			Method:  MethodPing,
			ID:      i,
		})

		if resp.Error != nil {
			t.Errorf("request %d: unexpected error: %s", i, resp.Error.Message)
		}

		if resp.ID != float64(i) {
			t.Errorf("request %d: got ID %v, want %d", i, resp.ID, i)
		}
	}
}
