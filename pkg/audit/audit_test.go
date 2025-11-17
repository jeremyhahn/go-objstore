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

package audit

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
)

func TestNewDefaultAuditLogger(t *testing.T) {
	logger := NewDefaultAuditLogger()
	if logger == nil {
		t.Fatal("Expected non-nil logger")
	}

	// Verify it implements the interface
	var _ AuditLogger = logger
}

func TestNewAuditLogger(t *testing.T) {
	tests := []struct {
		name   string
		config *Config
	}{
		{
			name:   "nil config uses defaults",
			config: nil,
		},
		{
			name: "JSON format",
			config: &Config{
				Enabled: true,
				Format:  FormatJSON,
				Level:   adapters.InfoLevel,
			},
		},
		{
			name: "text format",
			config: &Config{
				Enabled: true,
				Format:  FormatText,
				Level:   adapters.InfoLevel,
			},
		},
		{
			name: "disabled logger",
			config: &Config{
				Enabled: false,
				Format:  FormatJSON,
				Level:   adapters.ErrorLevel,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := NewAuditLogger(tt.config)
			if logger == nil {
				t.Fatal("Expected non-nil logger")
			}
		})
	}
}

func TestLogEvent(t *testing.T) {
	var buf bytes.Buffer
	config := &Config{
		Enabled:         true,
		Format:          FormatJSON,
		Level:           adapters.InfoLevel,
		Output:          &mockFile{Buffer: &buf},
		IncludeMetadata: true,
	}

	logger := NewAuditLogger(config)
	ctx := context.Background()

	event := &AuditEvent{
		Timestamp:        time.Now(),
		EventType:        EventObjectCreated,
		UserID:           "user123",
		Principal:        "john.doe",
		Bucket:           "my-bucket",
		Key:              "my-key",
		Action:           "put_object",
		Result:           ResultSuccess,
		IPAddress:        "192.168.1.1",
		RequestID:        "req-123",
		Method:           "PUT",
		StatusCode:       201,
		BytesTransferred: 1024,
		Duration:         100 * time.Millisecond,
		Metadata: map[string]any{
			"content_type": "application/json",
		},
	}

	err := logger.LogEvent(ctx, event)
	if err != nil {
		t.Fatalf("LogEvent failed: %v", err)
	}

	// Verify output contains expected fields
	output := buf.String()
	if !strings.Contains(output, "OBJECT_CREATED") {
		t.Error("Expected output to contain event type")
	}
	if !strings.Contains(output, "user123") {
		t.Error("Expected output to contain user ID")
	}
	if !strings.Contains(output, "my-bucket") {
		t.Error("Expected output to contain bucket")
	}
	if !strings.Contains(output, "my-key") {
		t.Error("Expected output to contain key")
	}
}

func TestLogAuthFailure(t *testing.T) {
	var buf bytes.Buffer
	config := &Config{
		Enabled: true,
		Format:  FormatJSON,
		Level:   adapters.InfoLevel,
		Output:  &mockFile{Buffer: &buf},
	}

	logger := NewAuditLogger(config)
	ctx := context.Background()

	err := logger.LogAuthFailure(ctx, "user123", "john.doe", "192.168.1.1", "req-123", "invalid credentials")
	if err != nil {
		t.Fatalf("LogAuthFailure failed: %v", err)
	}

	output := buf.String()
	if !strings.Contains(output, "AUTH_FAILURE") {
		t.Error("Expected output to contain AUTH_FAILURE")
	}
	if !strings.Contains(output, "invalid credentials") {
		t.Error("Expected output to contain error reason")
	}
}

func TestLogAuthSuccess(t *testing.T) {
	var buf bytes.Buffer
	config := &Config{
		Enabled: true,
		Format:  FormatJSON,
		Level:   adapters.InfoLevel,
		Output:  &mockFile{Buffer: &buf},
	}

	logger := NewAuditLogger(config)
	ctx := context.Background()

	err := logger.LogAuthSuccess(ctx, "user123", "john.doe", "192.168.1.1", "req-123")
	if err != nil {
		t.Fatalf("LogAuthSuccess failed: %v", err)
	}

	output := buf.String()
	if !strings.Contains(output, "AUTH_SUCCESS") {
		t.Error("Expected output to contain AUTH_SUCCESS")
	}
	if !strings.Contains(output, "SUCCESS") {
		t.Error("Expected output to contain SUCCESS result")
	}
}

func TestLogObjectAccess(t *testing.T) {
	var buf bytes.Buffer
	config := &Config{
		Enabled: true,
		Format:  FormatJSON,
		Level:   adapters.InfoLevel,
		Output:  &mockFile{Buffer: &buf},
	}

	logger := NewAuditLogger(config)
	ctx := context.Background()

	tests := []struct {
		name   string
		result Result
		err    error
	}{
		{
			name:   "successful access",
			result: ResultSuccess,
			err:    nil,
		},
		{
			name:   "failed access",
			result: ResultFailure,
			err:    os.ErrNotExist,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf.Reset()
			err := logger.LogObjectAccess(ctx, "user123", "john.doe", "bucket", "key", "192.168.1.1", "req-123", tt.result, tt.err)
			if err != nil {
				t.Fatalf("LogObjectAccess failed: %v", err)
			}

			output := buf.String()
			if !strings.Contains(output, "OBJECT_ACCESSED") {
				t.Error("Expected output to contain OBJECT_ACCESSED")
			}
			if !strings.Contains(output, string(tt.result)) {
				t.Errorf("Expected output to contain result %s", tt.result)
			}
			if tt.err != nil && !strings.Contains(output, "not exist") {
				t.Error("Expected output to contain error message")
			}
		})
	}
}

func TestLogObjectMutation(t *testing.T) {
	var buf bytes.Buffer
	config := &Config{
		Enabled: true,
		Format:  FormatJSON,
		Level:   adapters.InfoLevel,
		Output:  &mockFile{Buffer: &buf},
	}

	logger := NewAuditLogger(config)
	ctx := context.Background()

	tests := []struct {
		name      string
		eventType EventType
		result    Result
		err       error
	}{
		{
			name:      "object created",
			eventType: EventObjectCreated,
			result:    ResultSuccess,
			err:       nil,
		},
		{
			name:      "object deleted",
			eventType: EventObjectDeleted,
			result:    ResultSuccess,
			err:       nil,
		},
		{
			name:      "metadata updated",
			eventType: EventObjectMetadataUpdated,
			result:    ResultSuccess,
			err:       nil,
		},
		{
			name:      "failed creation",
			eventType: EventObjectCreated,
			result:    ResultFailure,
			err:       os.ErrPermission,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf.Reset()
			err := logger.LogObjectMutation(ctx, tt.eventType, "user123", "john.doe", "bucket", "key", "192.168.1.1", "req-123", 1024, tt.result, tt.err)
			if err != nil {
				t.Fatalf("LogObjectMutation failed: %v", err)
			}

			output := buf.String()
			if !strings.Contains(output, string(tt.eventType)) {
				t.Errorf("Expected output to contain event type %s", tt.eventType)
			}
			if !strings.Contains(output, string(tt.result)) {
				t.Errorf("Expected output to contain result %s", tt.result)
			}
		})
	}
}

func TestLogPolicyChange(t *testing.T) {
	var buf bytes.Buffer
	config := &Config{
		Enabled:         true,
		Format:          FormatJSON,
		Level:           adapters.InfoLevel,
		Output:          &mockFile{Buffer: &buf},
		IncludeMetadata: true,
	}

	logger := NewAuditLogger(config)
	ctx := context.Background()

	err := logger.LogPolicyChange(ctx, "user123", "john.doe", "bucket", "policy-1", "192.168.1.1", "req-123", ResultSuccess, nil)
	if err != nil {
		t.Fatalf("LogPolicyChange failed: %v", err)
	}

	output := buf.String()
	if !strings.Contains(output, "POLICY_CHANGED") {
		t.Error("Expected output to contain POLICY_CHANGED")
	}
	if !strings.Contains(output, "policy-1") {
		t.Error("Expected output to contain policy ID in metadata")
	}
}

func TestDisabledLogger(t *testing.T) {
	var buf bytes.Buffer
	config := &Config{
		Enabled: false,
		Format:  FormatJSON,
		Level:   adapters.InfoLevel,
		Output:  &mockFile{Buffer: &buf},
	}

	logger := NewAuditLogger(config)
	ctx := context.Background()

	event := &AuditEvent{
		EventType: EventObjectCreated,
		Action:    "test",
		Result:    ResultSuccess,
	}

	err := logger.LogEvent(ctx, event)
	if err != nil {
		t.Fatalf("LogEvent failed: %v", err)
	}

	// Should not log anything when disabled
	if buf.Len() > 0 {
		t.Error("Expected no output from disabled logger")
	}
}

func TestNoOpAuditLogger(t *testing.T) {
	logger := NewNoOpAuditLogger()
	ctx := context.Background()

	// All methods should succeed without error
	err := logger.LogEvent(ctx, &AuditEvent{})
	if err != nil {
		t.Errorf("NoOp LogEvent returned error: %v", err)
	}

	err = logger.LogAuthFailure(ctx, "user", "principal", "ip", "req", "reason")
	if err != nil {
		t.Errorf("NoOp LogAuthFailure returned error: %v", err)
	}

	err = logger.LogAuthSuccess(ctx, "user", "principal", "ip", "req")
	if err != nil {
		t.Errorf("NoOp LogAuthSuccess returned error: %v", err)
	}

	err = logger.LogObjectAccess(ctx, "user", "principal", "bucket", "key", "ip", "req", ResultSuccess, nil)
	if err != nil {
		t.Errorf("NoOp LogObjectAccess returned error: %v", err)
	}

	err = logger.LogObjectMutation(ctx, EventObjectCreated, "user", "principal", "bucket", "key", "ip", "req", 0, ResultSuccess, nil)
	if err != nil {
		t.Errorf("NoOp LogObjectMutation returned error: %v", err)
	}

	err = logger.LogPolicyChange(ctx, "user", "principal", "bucket", "policy", "ip", "req", ResultSuccess, nil)
	if err != nil {
		t.Errorf("NoOp LogPolicyChange returned error: %v", err)
	}
}

func TestLevelManagement(t *testing.T) {
	logger := NewDefaultAuditLogger()

	// Test SetLevel and GetLevel
	levels := []adapters.LogLevel{
		adapters.DebugLevel,
		adapters.InfoLevel,
		adapters.WarnLevel,
		adapters.ErrorLevel,
	}

	for _, level := range levels {
		logger.SetLevel(level)
		if logger.GetLevel() != level {
			t.Errorf("Expected level %v, got %v", level, logger.GetLevel())
		}
	}
}

func TestAuditEventJSON(t *testing.T) {
	event := &AuditEvent{
		Timestamp:        time.Now(),
		EventType:        EventObjectCreated,
		UserID:           "user123",
		Principal:        "john.doe",
		Bucket:           "bucket",
		Key:              "key",
		Action:           "put_object",
		Result:           ResultSuccess,
		IPAddress:        "192.168.1.1",
		RequestID:        "req-123",
		Method:           "PUT",
		StatusCode:       201,
		BytesTransferred: 1024,
		Duration:         100 * time.Millisecond,
		Metadata: map[string]any{
			"test": "value",
		},
	}

	// Marshal to JSON
	data, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("Failed to marshal event: %v", err)
	}

	// Unmarshal back
	var decoded AuditEvent
	err = json.Unmarshal(data, &decoded)
	if err != nil {
		t.Fatalf("Failed to unmarshal event: %v", err)
	}

	// Verify key fields
	if decoded.EventType != event.EventType {
		t.Errorf("EventType mismatch: got %s, want %s", decoded.EventType, event.EventType)
	}
	if decoded.UserID != event.UserID {
		t.Errorf("UserID mismatch: got %s, want %s", decoded.UserID, event.UserID)
	}
	if decoded.Result != event.Result {
		t.Errorf("Result mismatch: got %s, want %s", decoded.Result, event.Result)
	}
}

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	if !config.Enabled {
		t.Error("Expected Enabled to be true")
	}
	if config.Format != FormatJSON {
		t.Error("Expected Format to be JSON")
	}
	if config.Level != adapters.InfoLevel {
		t.Error("Expected Level to be Info")
	}
	if config.Output != os.Stdout {
		t.Error("Expected Output to be stdout")
	}
	if !config.IncludeMetadata {
		t.Error("Expected IncludeMetadata to be true")
	}
}

func TestEventTypeString(t *testing.T) {
	tests := []struct {
		eventType EventType
		expected  string
	}{
		{EventAuthFailure, "AUTH_FAILURE"},
		{EventAuthSuccess, "AUTH_SUCCESS"},
		{EventObjectCreated, "OBJECT_CREATED"},
		{EventObjectDeleted, "OBJECT_DELETED"},
		{EventObjectAccessed, "OBJECT_ACCESSED"},
		{EventObjectMetadataUpdated, "OBJECT_METADATA_UPDATED"},
		{EventPolicyChanged, "POLICY_CHANGED"},
		{EventBucketCreated, "BUCKET_CREATED"},
		{EventBucketDeleted, "BUCKET_DELETED"},
		{EventListObjects, "LIST_OBJECTS"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			if string(tt.eventType) != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, string(tt.eventType))
			}
		})
	}
}

func TestResultString(t *testing.T) {
	if string(ResultSuccess) != "SUCCESS" {
		t.Errorf("Expected SUCCESS, got %s", string(ResultSuccess))
	}
	if string(ResultFailure) != "FAILURE" {
		t.Errorf("Expected FAILURE, got %s", string(ResultFailure))
	}
}

// mockFile implements a simple in-memory writer for testing
type mockFile struct {
	*bytes.Buffer
}

func (m *mockFile) Close() error { return nil }
