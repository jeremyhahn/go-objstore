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

package server

// Server-wide limits and configuration constants
const (
	// MaxListLimit is the maximum number of objects that can be returned in a single list operation
	MaxListLimit = 1000

	// DefaultListLimit is the default number of objects returned in a list operation
	DefaultListLimit = 100

	// MaxUploadSize is the maximum size of a single object upload in bytes (1 GB)
	MaxUploadSize = 1 * 1024 * 1024 * 1024

	// MaxMetadataSize is the maximum size of metadata in bytes (1 MB)
	MaxMetadataSize = 1 * 1024 * 1024

	// MaxKeyLength is the maximum length of an object key
	MaxKeyLength = 1024

	// MaxPrefixLength is the maximum length of a prefix filter
	MaxPrefixLength = 512

	// MaxDelimiterLength is the maximum length of a delimiter
	MaxDelimiterLength = 10

	// MaxContinueTokenLength is the maximum length of a continuation token
	MaxContinueTokenLength = 2048

	// HealthCheckTimeout is the timeout for health check operations in milliseconds
	HealthCheckTimeout = 5000

	// DefaultRequestTimeout is the default timeout for operations in seconds
	DefaultRequestTimeout = 30

	// MaxConcurrentRequests is the maximum number of concurrent requests allowed
	MaxConcurrentRequests = 1000

	// BufferSize is the size of the buffer used for streaming operations in bytes (64 KB)
	BufferSize = 64 * 1024
)
