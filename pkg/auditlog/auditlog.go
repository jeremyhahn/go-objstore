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

// Package auditlog defines the audit logging contract: the event types, the
// AuditLogger interface and a no-op default. It deliberately has no
// dependencies beyond the standard library and pkg/adapters, so storage
// backends can accept an audit logger without linking the HTTP, gRPC or
// serialization stacks. The slog-backed logger and the HTTP/gRPC middleware
// that implement and drive this contract live in pkg/audit.
package auditlog

import (
	"context"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
)

// EventType represents the type of audit event
type EventType string

const (
	// EventAuthFailure indicates an authentication failure
	EventAuthFailure EventType = "AUTH_FAILURE"

	// EventAuthSuccess indicates successful authentication
	EventAuthSuccess EventType = "AUTH_SUCCESS"

	// EventObjectCreated indicates an object was created/uploaded
	EventObjectCreated EventType = "OBJECT_CREATED"

	// EventObjectDeleted indicates an object was deleted
	EventObjectDeleted EventType = "OBJECT_DELETED"

	// EventObjectAccessed indicates an object was read/accessed
	EventObjectAccessed EventType = "OBJECT_ACCESSED"

	// EventObjectMetadataUpdated indicates object metadata was modified
	EventObjectMetadataUpdated EventType = "OBJECT_METADATA_UPDATED"

	// EventObjectArchived indicates an object was archived
	EventObjectArchived EventType = "OBJECT_ARCHIVED"

	// EventPolicyChanged indicates a lifecycle policy was changed
	EventPolicyChanged EventType = "POLICY_CHANGED"

	// EventBucketCreated indicates a bucket was created
	EventBucketCreated EventType = "BUCKET_CREATED"

	// EventBucketDeleted indicates a bucket was deleted
	EventBucketDeleted EventType = "BUCKET_DELETED"

	// EventListObjects indicates objects were listed
	EventListObjects EventType = "LIST_OBJECTS"
)

// Result represents the outcome of an audited operation
type Result string

const (
	// ResultSuccess indicates the operation succeeded
	ResultSuccess Result = "SUCCESS"

	// ResultFailure indicates the operation failed
	ResultFailure Result = "FAILURE"
)

// AuditEvent represents a single audit log entry
type AuditEvent struct {
	// Timestamp when the event occurred
	Timestamp time.Time `json:"timestamp"`

	// EventType categorizes the type of event
	EventType EventType `json:"event_type"`

	// UserID is the identifier for the user/principal
	UserID string `json:"user_id,omitempty"`

	// Principal is the name of the authenticated user/service
	Principal string `json:"principal,omitempty"`

	// Resource identifies the target resource (bucket, key, etc.)
	Resource string `json:"resource,omitempty"`

	// Bucket is the bucket name if applicable
	Bucket string `json:"bucket,omitempty"`

	// Key is the object key if applicable
	Key string `json:"key,omitempty"`

	// Action describes what was attempted
	Action string `json:"action"`

	// Result indicates success or failure
	Result Result `json:"result"`

	// ErrorMessage contains error details if the operation failed
	ErrorMessage string `json:"error_message,omitempty"`

	// IPAddress is the client's IP address
	IPAddress string `json:"ip_address,omitempty"`

	// RequestID uniquely identifies the request
	RequestID string `json:"request_id,omitempty"`

	// Method is the HTTP method or gRPC method name
	Method string `json:"method,omitempty"`

	// StatusCode is the HTTP status code or gRPC status code
	StatusCode int `json:"status_code,omitempty"`

	// BytesTransferred indicates the amount of data transferred
	BytesTransferred int64 `json:"bytes_transferred,omitempty"`

	// Duration is how long the operation took
	Duration time.Duration `json:"duration,omitempty"`

	// Metadata contains additional event-specific data
	Metadata map[string]any `json:"metadata,omitempty"`
}

// AuditLogger defines the interface for audit logging
type AuditLogger interface {
	// LogEvent logs a generic audit event
	LogEvent(ctx context.Context, event *AuditEvent) error

	// LogAuthFailure logs authentication failures
	LogAuthFailure(ctx context.Context, userID, principal, ipAddress, requestID, reason string) error

	// LogAuthSuccess logs successful authentication
	LogAuthSuccess(ctx context.Context, userID, principal, ipAddress, requestID string) error

	// LogObjectAccess logs object read operations
	LogObjectAccess(ctx context.Context, userID, principal, bucket, key, ipAddress, requestID string, result Result, err error) error

	// LogObjectMutation logs object create/update/delete operations
	LogObjectMutation(ctx context.Context, eventType EventType, userID, principal, bucket, key, ipAddress, requestID string, bytesTransferred int64, result Result, err error) error

	// LogPolicyChange logs lifecycle policy modifications
	LogPolicyChange(ctx context.Context, userID, principal, bucket, policyID, ipAddress, requestID string, result Result, err error) error

	// SetLevel sets the minimum audit level (for filtering)
	SetLevel(level adapters.LogLevel)

	// GetLevel returns the current audit level
	GetLevel() adapters.LogLevel
}

// NoOpAuditLogger is an audit logger that discards all events
type NoOpAuditLogger struct {
	level adapters.LogLevel
}

// NewNoOpAuditLogger creates a new no-op audit logger
func NewNoOpAuditLogger() AuditLogger {
	return &NoOpAuditLogger{level: adapters.InfoLevel}
}

func (n *NoOpAuditLogger) LogEvent(ctx context.Context, event *AuditEvent) error {
	return nil
}

func (n *NoOpAuditLogger) LogAuthFailure(ctx context.Context, userID, principal, ipAddress, requestID, reason string) error {
	return nil
}

func (n *NoOpAuditLogger) LogAuthSuccess(ctx context.Context, userID, principal, ipAddress, requestID string) error {
	return nil
}

func (n *NoOpAuditLogger) LogObjectAccess(ctx context.Context, userID, principal, bucket, key, ipAddress, requestID string, result Result, err error) error {
	return nil
}

func (n *NoOpAuditLogger) LogObjectMutation(ctx context.Context, eventType EventType, userID, principal, bucket, key, ipAddress, requestID string, bytesTransferred int64, result Result, err error) error {
	return nil
}

func (n *NoOpAuditLogger) LogPolicyChange(ctx context.Context, userID, principal, bucket, policyID, ipAddress, requestID string, result Result, err error) error {
	return nil
}

func (n *NoOpAuditLogger) SetLevel(level adapters.LogLevel) {
	n.level = level
}

func (n *NoOpAuditLogger) GetLevel() adapters.LogLevel {
	return n.level
}
