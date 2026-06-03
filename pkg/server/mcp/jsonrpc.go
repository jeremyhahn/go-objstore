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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"
	"unicode/utf8"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/audit"
	"github.com/jeremyhahn/go-objstore/pkg/common"
	servererrors "github.com/jeremyhahn/go-objstore/pkg/server/errors"
	"github.com/jeremyhahn/go-objstore/pkg/server/jsonrpc"
	"github.com/jeremyhahn/go-objstore/pkg/server/metrics"
	"github.com/jeremyhahn/go-objstore/pkg/server/middleware"
	"github.com/jeremyhahn/go-objstore/pkg/version"
	"github.com/sourcegraph/jsonrpc2"
)

const (
	jsonRPCVersion = jsonrpc.Version

	// methodToolsCall is the JSON-RPC method name for invoking an MCP tool.
	methodToolsCall = "tools/call"
)

// JSON Schema field keys and values used when building MCP tool definitions
// and tool result payloads.
const (
	schemaType        = "type"
	schemaObject      = "object"
	schemaString      = "string"
	schemaProperties  = "properties"
	schemaRequired    = "required"
	schemaDescription = "description"

	fieldKey                 = "key"
	fieldPrefix              = "prefix"
	fieldSuccess             = "success"
	fieldMessage             = "message"
	fieldPolicyID            = "policy_id"
	fieldEnabled             = "enabled"
	fieldSourceBackend       = "source_backend"
	fieldDestinationBackend  = "destination_backend"
	fieldDestinationSettings = "destination_settings"
)

// JSONRPCRequest, JSONRPCResponse, and JSONRPCError are the JSON-RPC 2.0
// envelope types shared with the unix transport via pkg/server/jsonrpc. Kept
// as local aliases for source compatibility.
type (
	// JSONRPCRequest represents a JSON-RPC 2.0 request.
	JSONRPCRequest = jsonrpc.Request
	// JSONRPCResponse represents a JSON-RPC 2.0 response.
	JSONRPCResponse = jsonrpc.Response
	// JSONRPCError represents a JSON-RPC 2.0 error.
	JSONRPCError = jsonrpc.Error
)

// JSON-RPC error codes, shared with the unix transport via
// pkg/server/jsonrpc. Kept as local aliases for source compatibility.
const (
	ErrCodeParseError     = jsonrpc.CodeParseError
	ErrCodeInvalidRequest = jsonrpc.CodeInvalidRequest
	ErrCodeMethodNotFound = jsonrpc.CodeMethodNotFound
	ErrCodeInvalidParams  = jsonrpc.CodeInvalidParams
	ErrCodeInternalError  = jsonrpc.CodeInternal

	// ErrCodeForbidden is the implementation-defined code for authorization
	// denials, distinct from malformed-request errors.
	ErrCodeForbidden = jsonrpc.CodeForbidden
)

// RPCHandler handles JSON-RPC requests
type RPCHandler struct {
	server       *Server
	enforceAuthz bool
}

// NewRPCHandler creates a new RPC handler. When enforceAuthz is true the
// server's Authorizer is evaluated on every request; the anonymous principal
// (no credential) is used since stdio carries no identity token.
func NewRPCHandler(server *Server) *RPCHandler {
	return &RPCHandler{
		server:       server,
		enforceAuthz: server.config.EnforceStdioAuthz,
	}
}

// Handle processes a JSON-RPC request
func (h *RPCHandler) Handle(ctx context.Context, conn *jsonrpc2.Conn, req *jsonrpc2.Request) (result any, err error) {
	// Every request carries a request ID for tracing; stdio has no header to
	// receive one, so generate it here.
	ctx, _ = middleware.EnsureRequestID(ctx)

	start := time.Now()
	defer func() {
		if rec := recover(); rec != nil {
			slog.ErrorContext(ctx, "[MCP] Panic recovered",
				slog.Any("panic", rec),
				slog.String("method", req.Method),
			)
			err = &jsonrpc2.Error{
				Code:    ErrCodeInternalError,
				Message: "internal server error",
			}
		}
		if h.server.config.EnableAudit && h.server.config.AuditLogger != nil {
			audit.LogRPC(ctx, h.server.config.AuditLogger, "mcp-stdio", req.Method, anonymousMCPPrincipal(), start, err)
		}
	}()

	// Rate limit the stdio pipe with a single shared bucket.
	if h.server.rateLimiter != nil && !h.server.rateLimiter.AllowKey("stdio") {
		return nil, &jsonrpc2.Error{
			Code:    jsonrpc.CodeRateLimited,
			Message: "rate limit exceeded",
		}
	}

	// For stdio mode, enforce authorization when configured to do so.
	// HTTP mode is always authorized by authenticationMiddleware before Handle is called.
	if h.enforceAuthz {
		action, resource := stdioActionResource(req)
		principal := anonymousMCPPrincipal()
		if err = h.server.config.Authorizer.Authorize(ctx, principal, action, resource); err != nil {
			return nil, &jsonrpc2.Error{
				Code:    ErrCodeForbidden,
				Message: "forbidden",
			}
		}
	}

	switch req.Method {
	case "initialize":
		return h.handleInitialize(ctx, req.Params)
	case "tools/list":
		return h.handleToolsList(ctx)
	case methodToolsCall:
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

// anonymousMCPPrincipal returns the anonymous principal used for stdio authz.
func anonymousMCPPrincipal() *adapters.Principal {
	return &adapters.Principal{ID: "anonymous", Name: "Anonymous", Type: "anonymous"}
}

// stdioActionResource derives a (action, resource) pair from a JSON-RPC
// request for use by the stdio authorizer.
func stdioActionResource(req *jsonrpc2.Request) (action, resource string) {
	if req.Method != methodToolsCall || req.Params == nil {
		return adapters.ActionRead, ""
	}
	var p struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(*req.Params, &p); err != nil || p.Name == "" {
		return adapters.ActionAdmin, adapters.ResourcePolicy
	}
	if act, ok := mcpToolActions[p.Name]; ok {
		return act, p.Name
	}
	return adapters.ActionAdmin, adapters.ResourcePolicy
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
		// Map backend errors through the shared taxonomy so not-found and
		// permission errors surface with their proper JSON-RPC codes.
		code, message := servererrors.JSONRPCError(err)
		return nil, &jsonrpc2.Error{
			Code:    int64(code),
			Message: message,
		}
	}

	return map[string]any{
		"content": []map[string]any{
			{
				schemaType: "text",
				"text":     result,
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
		// Map through the shared taxonomy: classifies the error and sanitizes
		// the message (no internal paths/details leak to clients).
		code, message := servererrors.JSONRPCError(err)
		return nil, &jsonrpc2.Error{
			Code:    int64(code),
			Message: message,
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
		// Map through the shared taxonomy: classifies the error and sanitizes
		// the message (no internal paths/details leak to clients).
		code, message := servererrors.JSONRPCError(err)
		return nil, &jsonrpc2.Error{
			Code:    int64(code),
			Message: message,
		}
	}

	// Per the MCP spec, text resources use "text" and binary resources use
	// "blob" (base64). Returning binary bytes through "text" would corrupt
	// them in JSON encoding.
	entry := map[string]any{
		"uri":      readParams.URI,
		"mimeType": mimeType,
	}
	if utf8.ValidString(content) {
		entry["text"] = content
	} else {
		entry["blob"] = base64.StdEncoding.EncodeToString([]byte(content))
	}

	return map[string]any{
		"contents": []map[string]any{entry},
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
	start := time.Now()
	outcome := "error"
	defer func() {
		if rec := recover(); rec != nil {
			slog.ErrorContext(r.Context(), "[MCP HTTP] Panic recovered", slog.Any("panic", rec))
			h.writeError(w, ErrCodeInternalError, "internal server error")
		}
		metrics.Default.RecordRequest(metrics.TransportMCP, outcome, time.Since(start))
	}()

	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		h.writeError(w, ErrCodeParseError, "failed to read request body")
		return
	}
	defer func() { _ = r.Body.Close() }()

	// Shared parse + version validation with the unix transport.
	req, parseErr := jsonrpc.ParseRequest(body)
	if parseErr != nil {
		h.writeErrorWithID(w, parseErr.ID, parseErr.Error.Code, parseErr.Error.Message)
		return
	}

	// Create jsonrpc2 request
	jsonrpc2Req := &jsonrpc2.Request{
		Method: req.Method,
		Params: &req.Params,
	}

	result, err := h.handler.Handle(r.Context(), nil, jsonrpc2Req)

	resp := JSONRPCResponse{
		JSONRPC: jsonRPCVersion,
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
				Message: common.SanitizeErrorMessage(err),
			}
		}
	} else {
		resp.Result = result
		outcome = "ok"
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
		JSONRPC: jsonRPCVersion,
		Error: &JSONRPCError{
			Code:    code,
			Message: message,
		},
		ID: id,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp) // Ignore error, response headers already sent
}
