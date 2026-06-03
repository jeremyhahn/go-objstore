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
	"errors"

	"github.com/jeremyhahn/go-objstore/pkg/server/jsonrpc"
)

var (
	// ErrSocketPathRequired is returned when socket path is not provided
	ErrSocketPathRequired = errors.New("socket path is required")

	// ErrSocketPathNotSocket is returned when the configured socket path
	// exists but is not a Unix domain socket.
	ErrSocketPathNotSocket = errors.New("socket path exists and is not a socket")

	// ErrNotInitialized is returned when facade is not initialized
	ErrNotInitialized = errors.New("objstore facade not initialized")

	// ErrInvalidRequest is returned for malformed requests
	ErrInvalidRequest = errors.New("invalid request")

	// ErrMethodNotFound is returned when method doesn't exist
	ErrMethodNotFound = errors.New("method not found")

	// ErrInvalidParams is returned for invalid parameters
	ErrInvalidParams = errors.New("invalid parameters")

	// ErrServerClosed is returned when server is closed
	ErrServerClosed = errors.New("server closed")

	// ErrRequestFailed wraps the JSON-RPC error message of a failed request
	// for audit logging.
	ErrRequestFailed = errors.New("request failed")
)

// JSON-RPC 2.0 error codes, shared with the MCP transport via
// pkg/server/jsonrpc. Kept as local aliases for source compatibility.
const (
	ErrCodeParseError     = jsonrpc.CodeParseError
	ErrCodeInvalidRequest = jsonrpc.CodeInvalidRequest
	ErrCodeMethodNotFound = jsonrpc.CodeMethodNotFound
	ErrCodeInvalidParams  = jsonrpc.CodeInvalidParams
	ErrCodeInternalError  = jsonrpc.CodeInternal
)
