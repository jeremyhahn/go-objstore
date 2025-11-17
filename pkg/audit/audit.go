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

// Package audit provides comprehensive audit logging for object storage operations.
package audit

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
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

// OutputFormat specifies the format for audit log output
type OutputFormat string

const (
	// FormatJSON outputs audit logs in JSON format
	FormatJSON OutputFormat = "json"

	// FormatText outputs audit logs in human-readable text format
	FormatText OutputFormat = "text"
)

// Config holds configuration for the audit logger
type Config struct {
	// Enabled determines if audit logging is active
	Enabled bool

	// Format specifies the output format (JSON or text)
	Format OutputFormat

	// Level sets the minimum log level
	Level adapters.LogLevel

	// Output specifies where to write logs (defaults to stdout)
	Output io.Writer

	// IncludeMetadata determines if extra metadata should be logged
	IncludeMetadata bool
}

// DefaultConfig returns a Config with sensible defaults
func DefaultConfig() *Config {
	return &Config{
		Enabled:         true,
		Format:          FormatJSON,
		Level:           adapters.InfoLevel,
		Output:          os.Stdout,
		IncludeMetadata: true,
	}
}

// DefaultAuditLogger implements AuditLogger using slog
type DefaultAuditLogger struct {
	config *Config
	logger *slog.Logger
	level  adapters.LogLevel
}

// NewDefaultAuditLogger creates a new audit logger with default configuration
func NewDefaultAuditLogger() AuditLogger {
	return NewAuditLogger(DefaultConfig())
}

// NewAuditLogger creates a new audit logger with the specified configuration
func NewAuditLogger(config *Config) AuditLogger {
	if config == nil {
		config = DefaultConfig()
	}

	if config.Output == nil {
		config.Output = os.Stdout
	}

	var handler slog.Handler
	opts := &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}

	switch config.Format {
	case FormatJSON:
		handler = slog.NewJSONHandler(config.Output, opts)
	case FormatText:
		handler = slog.NewTextHandler(config.Output, opts)
	default:
		handler = slog.NewJSONHandler(config.Output, opts)
	}

	return &DefaultAuditLogger{
		config: config,
		logger: slog.New(handler),
		level:  config.Level,
	}
}

// LogEvent logs a generic audit event
func (a *DefaultAuditLogger) LogEvent(ctx context.Context, event *AuditEvent) error {
	if !a.config.Enabled {
		return nil
	}

	if event == nil {
		return nil
	}

	// Set timestamp if not already set
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}

	// Convert event to JSON for structured logging
	attrs := []slog.Attr{
		slog.Time("timestamp", event.Timestamp),
		slog.String("event_type", string(event.EventType)),
		slog.String("action", event.Action),
		slog.String("result", string(event.Result)),
	}

	if event.UserID != "" {
		attrs = append(attrs, slog.String("user_id", event.UserID))
	}
	if event.Principal != "" {
		attrs = append(attrs, slog.String("principal", event.Principal))
	}
	if event.Resource != "" {
		attrs = append(attrs, slog.String("resource", event.Resource))
	}
	if event.Bucket != "" {
		attrs = append(attrs, slog.String("bucket", event.Bucket))
	}
	if event.Key != "" {
		attrs = append(attrs, slog.String("key", event.Key))
	}
	if event.ErrorMessage != "" {
		attrs = append(attrs, slog.String("error", event.ErrorMessage))
	}
	if event.IPAddress != "" {
		attrs = append(attrs, slog.String("ip_address", event.IPAddress))
	}
	if event.RequestID != "" {
		attrs = append(attrs, slog.String("request_id", event.RequestID))
	}
	if event.Method != "" {
		attrs = append(attrs, slog.String("method", event.Method))
	}
	if event.StatusCode > 0 {
		attrs = append(attrs, slog.Int("status_code", event.StatusCode))
	}
	if event.BytesTransferred > 0 {
		attrs = append(attrs, slog.Int64("bytes_transferred", event.BytesTransferred))
	}
	if event.Duration > 0 {
		attrs = append(attrs, slog.Duration("duration", event.Duration))
	}
	if a.config.IncludeMetadata && event.Metadata != nil && len(event.Metadata) > 0 {
		metadataJSON, _ := json.Marshal(event.Metadata) //nolint:errcheck // marshaling simple map types is safe
		attrs = append(attrs, slog.String("metadata", string(metadataJSON)))
	}

	msg := "Audit event: " + event.Action
	a.logger.LogAttrs(ctx, slog.LevelInfo, msg, attrs...)

	return nil
}

// LogAuthFailure logs authentication failures
func (a *DefaultAuditLogger) LogAuthFailure(ctx context.Context, userID, principal, ipAddress, requestID, reason string) error {
	event := &AuditEvent{
		Timestamp:    time.Now(),
		EventType:    EventAuthFailure,
		UserID:       userID,
		Principal:    principal,
		Action:       "authenticate",
		Result:       ResultFailure,
		ErrorMessage: reason,
		IPAddress:    ipAddress,
		RequestID:    requestID,
	}
	return a.LogEvent(ctx, event)
}

// LogAuthSuccess logs successful authentication
func (a *DefaultAuditLogger) LogAuthSuccess(ctx context.Context, userID, principal, ipAddress, requestID string) error {
	event := &AuditEvent{
		Timestamp: time.Now(),
		EventType: EventAuthSuccess,
		UserID:    userID,
		Principal: principal,
		Action:    "authenticate",
		Result:    ResultSuccess,
		IPAddress: ipAddress,
		RequestID: requestID,
	}
	return a.LogEvent(ctx, event)
}

// LogObjectAccess logs object read operations
func (a *DefaultAuditLogger) LogObjectAccess(ctx context.Context, userID, principal, bucket, key, ipAddress, requestID string, result Result, err error) error {
	event := &AuditEvent{
		Timestamp: time.Now(),
		EventType: EventObjectAccessed,
		UserID:    userID,
		Principal: principal,
		Bucket:    bucket,
		Key:       key,
		Resource:  bucket + "/" + key,
		Action:    "get_object",
		Result:    result,
		IPAddress: ipAddress,
		RequestID: requestID,
	}

	if err != nil {
		event.ErrorMessage = err.Error()
	}

	return a.LogEvent(ctx, event)
}

// LogObjectMutation logs object create/update/delete operations
func (a *DefaultAuditLogger) LogObjectMutation(ctx context.Context, eventType EventType, userID, principal, bucket, key, ipAddress, requestID string, bytesTransferred int64, result Result, err error) error {
	action := "modify_object"
	switch eventType {
	case EventObjectCreated:
		action = "put_object"
	case EventObjectDeleted:
		action = "delete_object"
	case EventObjectMetadataUpdated:
		action = "update_metadata"
	}

	event := &AuditEvent{
		Timestamp:        time.Now(),
		EventType:        eventType,
		UserID:           userID,
		Principal:        principal,
		Bucket:           bucket,
		Key:              key,
		Resource:         bucket + "/" + key,
		Action:           action,
		Result:           result,
		IPAddress:        ipAddress,
		RequestID:        requestID,
		BytesTransferred: bytesTransferred,
	}

	if err != nil {
		event.ErrorMessage = err.Error()
	}

	return a.LogEvent(ctx, event)
}

// LogPolicyChange logs lifecycle policy modifications
func (a *DefaultAuditLogger) LogPolicyChange(ctx context.Context, userID, principal, bucket, policyID, ipAddress, requestID string, result Result, err error) error {
	event := &AuditEvent{
		Timestamp: time.Now(),
		EventType: EventPolicyChanged,
		UserID:    userID,
		Principal: principal,
		Bucket:    bucket,
		Resource:  bucket + "/policy/" + policyID,
		Action:    "update_lifecycle_policy",
		Result:    result,
		IPAddress: ipAddress,
		RequestID: requestID,
		Metadata: map[string]any{
			"policy_id": policyID,
		},
	}

	if err != nil {
		event.ErrorMessage = err.Error()
	}

	return a.LogEvent(ctx, event)
}

// SetLevel sets the minimum audit level
func (a *DefaultAuditLogger) SetLevel(level adapters.LogLevel) {
	a.level = level
}

// GetLevel returns the current audit level
func (a *DefaultAuditLogger) GetLevel() adapters.LogLevel {
	return a.level
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
