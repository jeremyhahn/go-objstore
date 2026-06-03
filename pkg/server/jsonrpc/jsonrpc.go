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

// Package jsonrpc provides the JSON-RPC 2.0 envelope, error codes, and
// parsing shared by the unix-socket and MCP transports. Transport-specific
// concerns (framing, peer credentials, MCP tool semantics) remain in the
// respective server packages.
package jsonrpc

import "encoding/json"

// Version is the JSON-RPC protocol version.
const Version = "2.0"

// Request represents a JSON-RPC 2.0 request.
type Request struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
	ID      any             `json:"id"`
}

// Response represents a JSON-RPC 2.0 response.
type Response struct {
	JSONRPC string `json:"jsonrpc"`
	Result  any    `json:"result,omitempty"`
	Error   *Error `json:"error,omitempty"`
	ID      any    `json:"id"`
}

// Error represents a JSON-RPC 2.0 error object.
type Error struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

// Standard JSON-RPC 2.0 error codes plus the implementation-defined codes
// used by the objstore transports. The implementation-defined range
// (-32000..-32099) is reserved by the spec for server errors.
const (
	CodeParseError     = -32700
	CodeInvalidRequest = -32600
	CodeMethodNotFound = -32601
	CodeInvalidParams  = -32602
	CodeInternal       = -32603

	// CodeForbidden reports an authorization denial.
	CodeForbidden = -32001
	// CodeUnauthenticated reports an authentication failure.
	CodeUnauthenticated = -32002
	// CodeUnavailable reports a backend or dependency outage (retryable).
	CodeUnavailable = -32003
	// CodeNotFound reports a missing object, metadata entry, or policy.
	CodeNotFound = -32004
	// CodeAlreadyExists reports a conflict with an existing resource.
	CodeAlreadyExists = -32005
	// CodeRateLimited reports a rate-limit rejection.
	CodeRateLimited = -32029
)

// NewResponse builds a success response for the given request ID.
func NewResponse(id, result any) *Response {
	return &Response{JSONRPC: Version, Result: result, ID: id}
}

// NewError builds an error response for the given request ID.
func NewError(id any, code int, message string) *Response {
	return &Response{
		JSONRPC: Version,
		Error:   &Error{Code: code, Message: message},
		ID:      id,
	}
}

// ParseRequest decodes and validates a JSON-RPC 2.0 request. On failure it
// returns nil and a ready-to-send error response (parse error or invalid
// version), so transports share identical validation behavior.
func ParseRequest(data []byte) (*Request, *Response) {
	var req Request
	if err := json.Unmarshal(data, &req); err != nil {
		return nil, NewError(nil, CodeParseError, "invalid JSON")
	}
	if req.JSONRPC != Version {
		return nil, NewError(req.ID, CodeInvalidRequest, "invalid JSON-RPC version")
	}
	return &req, nil
}
