// Copyright (c) 2025 Jeremy Hahn
// Copyright (c) 2025 Automate The Things, LLC
//
// This file is part of go-objstore.
//
// go-objstore is dual-licensed under AGPL-3.0 and a Commercial License.

package objstore

import (
	"encoding/json"
	"fmt"
)

// rpcRequest is the JSON-RPC 2.0 request envelope shared by the MCP and unix
// socket clients.
type rpcRequest struct {
	JSONRPC string `json:"jsonrpc"`
	Method  string `json:"method"`
	Params  any    `json:"params,omitempty"`
	ID      int64  `json:"id"`
}

// rpcResponse is the JSON-RPC 2.0 response envelope shared by the MCP and
// unix socket clients.
type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
	ID      any             `json:"id"`
}

// rpcError carries a JSON-RPC 2.0 error object.
type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

// JSON-RPC application error codes shared by the unix and MCP transports.
// The authoritative list lives in pkg/server/jsonrpc.
const (
	rpcCodeForbidden       = -32001
	rpcCodeUnauthenticated = -32002
	rpcCodeNotFound        = -32004
	rpcCodeAlreadyExists   = -32005
	rpcCodeRateLimited     = -32029
	rpcCodeInvalidParams   = -32602
)

// asSDKError converts a JSON-RPC error object into an SDK error, wrapping the
// matching sentinel when one exists so callers can use errors.Is.  transport
// is the error-message prefix ("rpc" for unix, "mcp" for MCP).
func (e *rpcError) asSDKError(transport string) error {
	var sentinel error
	switch e.Code {
	case rpcCodeInvalidParams:
		sentinel = ErrInvalidArgument
	case rpcCodeUnauthenticated:
		sentinel = ErrUnauthenticated
	case rpcCodeForbidden:
		sentinel = ErrPermissionDenied
	case rpcCodeNotFound:
		sentinel = ErrObjectNotFound
	case rpcCodeAlreadyExists:
		sentinel = ErrAlreadyExists
	case rpcCodeRateLimited:
		sentinel = ErrRateLimited
	default:
		return fmt.Errorf("%s error %d: %s", transport, e.Code, e.Message)
	}
	return fmt.Errorf("%s error %d: %s: %w", transport, e.Code, e.Message, sentinel)
}

// rpcMetadataParams converts Metadata into the metadata object shape shared
// by the unix protocol (MetadataParams) and MCP tool arguments.
func rpcMetadataParams(m *Metadata) map[string]any {
	out := map[string]any{}
	if m.ContentType != "" {
		out["content_type"] = m.ContentType
	}
	if m.ContentEncoding != "" {
		out["content_encoding"] = m.ContentEncoding
	}
	if len(m.Custom) > 0 {
		out["custom"] = m.Custom
	}
	return out
}
