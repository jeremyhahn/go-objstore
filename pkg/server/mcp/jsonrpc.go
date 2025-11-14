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
	"fmt"
	"io"
	"net/http"

	"github.com/jeremyhahn/go-objstore/pkg/version"
	"github.com/sourcegraph/jsonrpc2"
)

// JSONRPCRequest represents a JSON-RPC 2.0 request
type JSONRPCRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
	ID      any             `json:"id"`
}

// JSONRPCResponse represents a JSON-RPC 2.0 response
type JSONRPCResponse struct {
	JSONRPC string        `json:"jsonrpc"`
	Result  any           `json:"result,omitempty"`
	Error   *JSONRPCError `json:"error,omitempty"`
	ID      any           `json:"id"`
}

// JSONRPCError represents a JSON-RPC 2.0 error
type JSONRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

// Standard JSON-RPC error codes
const (
	ErrCodeParseError     = -32700
	ErrCodeInvalidRequest = -32600
	ErrCodeMethodNotFound = -32601
	ErrCodeInvalidParams  = -32602
	ErrCodeInternalError  = -32603
)

// RPCHandler handles JSON-RPC requests
type RPCHandler struct {
	server *Server
}

// NewRPCHandler creates a new RPC handler
func NewRPCHandler(server *Server) *RPCHandler {
	return &RPCHandler{
		server: server,
	}
}

// Handle processes a JSON-RPC request
func (h *RPCHandler) Handle(ctx context.Context, conn *jsonrpc2.Conn, req *jsonrpc2.Request) (any, error) {
	switch req.Method {
	case "initialize":
		return h.handleInitialize(ctx, req.Params)
	case "tools/list":
		return h.handleToolsList(ctx)
	case "tools/call":
		return h.handleToolsCall(ctx, req.Params)
	case "resources/list":
		return h.handleResourcesList(ctx, req.Params)
	case "resources/read":
		return h.handleResourcesRead(ctx, req.Params)
	case "ping":
		return map[string]string{"status": "ok"}, nil
	default:
		return nil, &jsonrpc2.Error{
			Code:    ErrCodeMethodNotFound,
			Message: fmt.Sprintf("method not found: %s", req.Method),
		}
	}
}

// handleInitialize handles the initialize request
func (h *RPCHandler) handleInitialize(ctx context.Context, params *json.RawMessage) (any, error) {
	var initParams struct {
		ProtocolVersion string         `json:"protocolVersion"`
		Capabilities    map[string]any `json:"capabilities"`
		ClientInfo      struct {
			Name    string `json:"name"`
			Version string `json:"version"`
		} `json:"clientInfo"`
	}

	if params != nil {
		if err := json.Unmarshal(*params, &initParams); err != nil {
			return nil, &jsonrpc2.Error{
				Code:    ErrCodeInvalidParams,
				Message: "invalid initialize parameters",
			}
		}
	}

	return map[string]any{
		"protocolVersion": "2024-11-05",
		"capabilities": map[string]any{
			"tools": map[string]any{
				"listChanged": false,
			},
			"resources": map[string]any{
				"subscribe":   false,
				"listChanged": false,
			},
		},
		"serverInfo": map[string]string{
			"name":    "go-objstore-mcp",
			"version": version.Get(),
		},
	}, nil
}

// handleToolsList handles the tools/list request
func (h *RPCHandler) handleToolsList(ctx context.Context) (any, error) {
	tools := h.server.ListTools()
	return map[string]any{
		"tools": tools,
	}, nil
}

// handleToolsCall handles the tools/call request
func (h *RPCHandler) handleToolsCall(ctx context.Context, params *json.RawMessage) (any, error) {
	if params == nil {
		return nil, &jsonrpc2.Error{
			Code:    ErrCodeInvalidParams,
			Message: "missing parameters",
		}
	}

	var callParams struct {
		Name      string         `json:"name"`
		Arguments map[string]any `json:"arguments"`
	}

	if err := json.Unmarshal(*params, &callParams); err != nil {
		return nil, &jsonrpc2.Error{
			Code:    ErrCodeInvalidParams,
			Message: "invalid call parameters",
		}
	}

	result, err := h.server.CallTool(ctx, callParams.Name, callParams.Arguments)
	if err != nil {
		return nil, &jsonrpc2.Error{
			Code:    ErrCodeInternalError,
			Message: err.Error(),
		}
	}

	return map[string]any{
		"content": []map[string]any{
			{
				"type": "text",
				"text": result,
			},
		},
	}, nil
}

// handleResourcesList handles the resources/list request
func (h *RPCHandler) handleResourcesList(ctx context.Context, params *json.RawMessage) (any, error) {
	var listParams struct {
		Cursor string `json:"cursor,omitempty"`
	}

	if params != nil && len(*params) > 0 {
		if err := json.Unmarshal(*params, &listParams); err != nil {
			return nil, &jsonrpc2.Error{
				Code:    ErrCodeInvalidParams,
				Message: "invalid list parameters",
			}
		}
	}

	resources, err := h.server.ListResources(ctx, listParams.Cursor)
	if err != nil {
		return nil, &jsonrpc2.Error{
			Code:    ErrCodeInternalError,
			Message: err.Error(),
		}
	}

	return map[string]any{
		"resources": resources,
	}, nil
}

// handleResourcesRead handles the resources/read request
func (h *RPCHandler) handleResourcesRead(ctx context.Context, params *json.RawMessage) (any, error) {
	if params == nil {
		return nil, &jsonrpc2.Error{
			Code:    ErrCodeInvalidParams,
			Message: "missing parameters",
		}
	}

	var readParams struct {
		URI string `json:"uri"`
	}

	if err := json.Unmarshal(*params, &readParams); err != nil {
		return nil, &jsonrpc2.Error{
			Code:    ErrCodeInvalidParams,
			Message: "invalid read parameters",
		}
	}

	content, mimeType, err := h.server.ReadResource(ctx, readParams.URI)
	if err != nil {
		return nil, &jsonrpc2.Error{
			Code:    ErrCodeInternalError,
			Message: err.Error(),
		}
	}

	return map[string]any{
		"contents": []map[string]any{
			{
				"uri":      readParams.URI,
				"mimeType": mimeType,
				"text":     content,
			},
		},
	}, nil
}

// HTTPHandler provides HTTP transport for JSON-RPC
type HTTPHandler struct {
	handler *RPCHandler
}

// NewHTTPHandler creates a new HTTP handler
func NewHTTPHandler(server *Server) *HTTPHandler {
	return &HTTPHandler{
		handler: NewRPCHandler(server),
	}
}

// ServeHTTP implements http.Handler
func (h *HTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		h.writeError(w, ErrCodeParseError, "failed to read request body")
		return
	}
	defer r.Body.Close()

	var req JSONRPCRequest
	if err := json.Unmarshal(body, &req); err != nil {
		h.writeError(w, ErrCodeParseError, "invalid JSON")
		return
	}

	if req.JSONRPC != "2.0" {
		h.writeErrorWithID(w, req.ID, ErrCodeInvalidRequest, "invalid JSON-RPC version")
		return
	}

	// Create jsonrpc2 request
	jsonrpc2Req := &jsonrpc2.Request{
		Method: req.Method,
		Params: &req.Params,
	}

	result, err := h.handler.Handle(r.Context(), nil, jsonrpc2Req)

	resp := JSONRPCResponse{
		JSONRPC: "2.0",
		ID:      req.ID,
	}

	if err != nil {
		if rpcErr, ok := err.(*jsonrpc2.Error); ok {
			resp.Error = &JSONRPCError{
				Code:    int(rpcErr.Code),
				Message: rpcErr.Message,
				Data:    rpcErr.Data,
			}
		} else {
			resp.Error = &JSONRPCError{
				Code:    ErrCodeInternalError,
				Message: err.Error(),
			}
		}
	} else {
		resp.Result = result
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		// Log error but response already started
		// Note: In production, you'd want to use a proper logger here
		http.Error(w, "failed to encode response", http.StatusInternalServerError)
	}
}

// writeError writes a JSON-RPC error response
func (h *HTTPHandler) writeError(w http.ResponseWriter, code int, message string) {
	h.writeErrorWithID(w, nil, code, message)
}

// writeErrorWithID writes a JSON-RPC error response with request ID
func (h *HTTPHandler) writeErrorWithID(w http.ResponseWriter, id any, code int, message string) {
	resp := JSONRPCResponse{
		JSONRPC: "2.0",
		Error: &JSONRPCError{
			Code:    code,
			Message: message,
		},
		ID: id,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		// Log error but response headers already sent
		// Note: In production, you'd want to use a proper logger here
	}
}
