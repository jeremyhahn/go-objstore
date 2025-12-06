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

import "errors"

var (
	// ErrSocketPathRequired is returned when socket path is not provided
	ErrSocketPathRequired = errors.New("socket path is required")

	// ErrNotInitialized is returned when facade is not initialized
	ErrNotInitialized = errors.New("objstore facade not initialized")

	// ErrInvalidRequest is returned for malformed requests
	ErrInvalidRequest = errors.New("invalid request")

	// ErrMethodNotFound is returned when method doesn't exist
	ErrMethodNotFound = errors.New("method not found")

	// ErrInvalidParams is returned for invalid parameters
	ErrInvalidParams = errors.New("invalid parameters")

	// ErrObjectNotFound is returned when object doesn't exist
	ErrObjectNotFound = errors.New("object not found")

	// ErrPolicyNotFound is returned when policy doesn't exist
	ErrPolicyNotFound = errors.New("policy not found")

	// ErrServerClosed is returned when server is closed
	ErrServerClosed = errors.New("server closed")
)

// JSON-RPC 2.0 error codes
const (
	ErrCodeParseError     = -32700
	ErrCodeInvalidRequest = -32600
	ErrCodeMethodNotFound = -32601
	ErrCodeInvalidParams  = -32602
	ErrCodeInternalError  = -32603
)
