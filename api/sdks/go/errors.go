// Copyright (c) 2025 Jeremy Hahn
// Copyright (c) 2025 Automate The Things, LLC
//
// This file is part of go-objstore.
//
// go-objstore is dual-licensed under AGPL-3.0 and a Commercial License.

package objstore

import "errors"

var (
	// ErrInvalidProtocol is returned when an unsupported protocol is specified.
	ErrInvalidProtocol = errors.New("invalid protocol")

	// ErrConnectionFailed is returned when connection to server fails.
	ErrConnectionFailed = errors.New("connection failed")

	// ErrObjectNotFound is returned when the requested object doesn't exist.
	ErrObjectNotFound = errors.New("object not found")

	// ErrInvalidConfig is returned when the client configuration is invalid.
	ErrInvalidConfig = errors.New("invalid configuration")

	// ErrStreamingNotSupported is returned when streaming is not supported by the protocol.
	ErrStreamingNotSupported = errors.New("streaming not supported")

	// ErrPolicyNotFound is returned when a policy doesn't exist.
	ErrPolicyNotFound = errors.New("policy not found")

	// ErrOperationFailed is returned when an operation fails.
	ErrOperationFailed = errors.New("operation failed")

	// ErrNotSupported is returned when an operation is not supported by the protocol.
	ErrNotSupported = errors.New("operation not supported")

	// ErrInvalidKey is returned when a key is empty or invalid.
	ErrInvalidKey = errors.New("invalid key: key cannot be empty")

	// ErrInvalidData is returned when data is nil or invalid.
	ErrInvalidData = errors.New("invalid data: data cannot be nil")

	// ErrInvalidPolicyID is returned when a policy ID is empty or invalid.
	ErrInvalidPolicyID = errors.New("invalid policy ID: policy ID cannot be empty")

	// ErrInvalidPolicy is returned when a policy is nil or invalid.
	ErrInvalidPolicy = errors.New("invalid policy: policy cannot be nil")

	// ErrInvalidMetadata is returned when metadata is nil or invalid.
	ErrInvalidMetadata = errors.New("invalid metadata: metadata cannot be nil")

	// ErrTimeout is returned when an operation times out (retryable).
	ErrTimeout = errors.New("operation timeout")

	// ErrTemporaryFailure is returned for temporary failures that should be retried.
	ErrTemporaryFailure = errors.New("temporary failure")
)
