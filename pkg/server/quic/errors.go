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

package quic

import "errors"

var (
	// ErrInvalidAddr is returned when the address is invalid.
	ErrInvalidAddr = errors.New("invalid address")

	// ErrTLSConfigRequired is returned when TLS configuration is missing.
	ErrTLSConfigRequired = errors.New("TLS configuration is required")

	// ErrServerNotStarted is returned when operations are attempted on a non-running server.
	ErrServerNotStarted = errors.New("server not started")

	// ErrServerAlreadyStarted is returned when attempting to start an already running server.
	ErrServerAlreadyStarted = errors.New("server already started")

	// ErrInvalidKey is returned when the object key is invalid.
	ErrInvalidKey = errors.New("invalid object key")

	// ErrObjectNotFound is returned when an object is not found.
	ErrObjectNotFound = errors.New("object not found")

	// ErrRequestBodyTooLarge is returned when the request body exceeds the maximum size.
	ErrRequestBodyTooLarge = errors.New("request body too large")

	// ErrInvalidContentLength is returned when the Content-Length header is invalid.
	ErrInvalidContentLength = errors.New("invalid content-length header")

	// ErrMethodNotAllowed is returned when an HTTP method is not allowed for the endpoint.
	ErrMethodNotAllowed = errors.New("method not allowed")
)
