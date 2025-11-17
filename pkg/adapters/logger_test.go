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

package adapters

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
)

func TestLogLevel(t *testing.T) {
	tests := []struct {
		level    LogLevel
		expected string
	}{
		{DebugLevel, "DEBUG"},
		{InfoLevel, "INFO"},
		{WarnLevel, "WARN"},
		{ErrorLevel, "ERROR"},
		{LogLevel(999), "UNKNOWN"}, // Test unknown/default case
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			if got := tt.level.String(); got != tt.expected {
				t.Errorf("LogLevel.String() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestDefaultLogger(t *testing.T) {
	// Capture log output
	var buf bytes.Buffer
	logger := NewDefaultLogger()
	defaultLogger, ok := logger.(*DefaultLogger)
	if !ok {
		t.Fatal("Failed to cast logger to *DefaultLogger")
	}
	// Create slog handler that writes to buffer
	handler := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	defaultLogger.logger = slog.New(handler)

	ctx := context.Background()

	t.Run("Debug", func(t *testing.T) {
		buf.Reset()
		logger.SetLevel(DebugLevel)
		logger.Debug(ctx, "test debug message")
		output := buf.String()
		if !strings.Contains(output, "level=DEBUG") || !strings.Contains(output, "test debug message") {
			t.Errorf("Debug log missing expected content, got: %s", output)
		}
	})

	t.Run("Info", func(t *testing.T) {
		buf.Reset()
		logger.SetLevel(InfoLevel)
		logger.Info(ctx, "test info message")
		output := buf.String()
		if !strings.Contains(output, "level=INFO") || !strings.Contains(output, "test info message") {
			t.Errorf("Info log missing expected content, got: %s", output)
		}
	})

	t.Run("Warn", func(t *testing.T) {
		buf.Reset()
		logger.SetLevel(WarnLevel)
		logger.Warn(ctx, "test warn message")
		output := buf.String()
		if !strings.Contains(output, "level=WARN") || !strings.Contains(output, "test warn message") {
			t.Errorf("Warn log missing expected content, got: %s", output)
		}
	})

	t.Run("Error", func(t *testing.T) {
		buf.Reset()
		logger.SetLevel(ErrorLevel)
		logger.Error(ctx, "test error message")
		output := buf.String()
		if !strings.Contains(output, "level=ERROR") || !strings.Contains(output, "test error message") {
			t.Errorf("Error log missing expected content, got: %s", output)
		}
	})
}

func TestDefaultLoggerWithFields(t *testing.T) {
	var buf bytes.Buffer
	logger := NewDefaultLogger()
	defaultLogger, ok := logger.(*DefaultLogger)
	if !ok {
		t.Fatal("Failed to cast logger to *DefaultLogger")
	}
	// Create slog handler that writes to buffer
	handler := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	defaultLogger.logger = slog.New(handler)
	logger.SetLevel(DebugLevel)

	ctx := context.Background()
	logger.Info(ctx, "test message",
		Field{Key: "key1", Value: "value1"},
		Field{Key: "key2", Value: 42},
	)

	output := buf.String()
	if !strings.Contains(output, "key1=value1") {
		t.Errorf("Log missing field key1=value1, got: %s", output)
	}
	if !strings.Contains(output, "key2=42") {
		t.Errorf("Log missing field key2=42, got: %s", output)
	}
}

func TestDefaultLoggerLogLevel(t *testing.T) {
	var buf bytes.Buffer
	logger := NewDefaultLogger()
	defaultLogger, ok := logger.(*DefaultLogger)
	if !ok {
		t.Fatal("Failed to cast logger to *DefaultLogger")
	}
	// Create slog handler that writes to buffer
	handler := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	defaultLogger.logger = slog.New(handler)

	ctx := context.Background()

	// Set level to Info
	logger.SetLevel(InfoLevel)

	// Debug should not log
	buf.Reset()
	logger.Debug(ctx, "debug message")
	if buf.Len() > 0 {
		t.Errorf("Debug message logged when level was Info")
	}

	// Info should log
	buf.Reset()
	logger.Info(ctx, "info message")
	if buf.Len() == 0 {
		t.Errorf("Info message not logged when level was Info")
	}

	// Warn should log
	buf.Reset()
	logger.Warn(ctx, "warn message")
	if buf.Len() == 0 {
		t.Errorf("Warn message not logged when level was Info")
	}

	// Error should log
	buf.Reset()
	logger.Error(ctx, "error message")
	if buf.Len() == 0 {
		t.Errorf("Error message not logged when level was Info")
	}
}

func TestDefaultLoggerWithFieldsMethod(t *testing.T) {
	var buf bytes.Buffer
	logger := NewDefaultLogger()
	defaultLogger, ok := logger.(*DefaultLogger)
	if !ok {
		t.Fatal("Failed to cast logger to *DefaultLogger")
	}
	// Create slog handler that writes to buffer
	handler := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	defaultLogger.logger = slog.New(handler)
	logger.SetLevel(DebugLevel)

	// Create logger with fields
	loggerWithFields := logger.WithFields(
		Field{Key: "service", Value: "test"},
		Field{Key: "version", Value: "1.0"},
	)

	ctx := context.Background()
	loggerWithFields.Info(ctx, "test message")

	output := buf.String()
	if !strings.Contains(output, "service=test") {
		t.Errorf("Log missing field service=test, got: %s", output)
	}
	if !strings.Contains(output, "version=1.0") {
		t.Errorf("Log missing field version=1.0, got: %s", output)
	}
}

func TestDefaultLoggerGetLevel(t *testing.T) {
	logger := NewDefaultLogger()

	// Default level should be Info
	if logger.GetLevel() != InfoLevel {
		t.Errorf("Default log level should be Info, got %v", logger.GetLevel())
	}

	// Set and get level
	logger.SetLevel(DebugLevel)
	if logger.GetLevel() != DebugLevel {
		t.Errorf("Log level should be Debug after setting, got %v", logger.GetLevel())
	}
}

func TestDefaultLoggerWithContext(t *testing.T) {
	logger := NewDefaultLogger()
	ctx := context.Background()

	// Test WithContext creates a new logger
	loggerWithContext := logger.WithContext(ctx)
	if loggerWithContext == nil {
		t.Fatal("WithContext should return a logger")
	}

	// Verify the new logger works
	loggerWithContext.Info(ctx, "test message with context")
}

func TestDefaultLoggerNilContext(t *testing.T) {
	var buf bytes.Buffer
	logger := NewDefaultLogger()
	defaultLogger, ok := logger.(*DefaultLogger)
	if !ok {
		t.Fatal("Failed to cast logger to *DefaultLogger")
	}
	handler := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	defaultLogger.logger = slog.New(handler)
	logger.SetLevel(DebugLevel)

	t.Run("Log with nil context", func(t *testing.T) {
		buf.Reset()
		logger.Info(nil, "test with nil context")
		output := buf.String()
		if !strings.Contains(output, "test with nil context") {
			t.Errorf("Log should work with nil context, got: %s", output)
		}
	})

	t.Run("Log with logger context when call context is nil", func(t *testing.T) {
		buf.Reset()
		ctx := context.Background()
		loggerWithCtx := logger.WithContext(ctx)
		loggerWithCtx.Info(nil, "test with logger context")
		output := buf.String()
		if !strings.Contains(output, "test with logger context") {
			t.Errorf("Log should use logger context when call context is nil, got: %s", output)
		}
	})

	t.Run("Log with unknown log level", func(t *testing.T) {
		buf.Reset()
		ctx := context.Background()
		// Call log() with an unknown log level by casting
		defaultLogger, _ := logger.(*DefaultLogger)
		defaultLogger.log(LogLevel(999), ctx, "test unknown level")
		output := buf.String()
		if !strings.Contains(output, "test unknown level") {
			t.Errorf("Log should handle unknown level, got: %s", output)
		}
	})
}

func TestNoOpLogger(t *testing.T) {
	logger := NewNoOpLogger()
	ctx := context.Background()

	// These should all work without panicking or producing output
	logger.Debug(ctx, "test")
	logger.Info(ctx, "test")
	logger.Warn(ctx, "test")
	logger.Error(ctx, "test")

	// Test all methods with fields to ensure coverage
	logger.Debug(ctx, "debug message", Field{Key: "key", Value: "value"})
	logger.Info(ctx, "info message", Field{Key: "key", Value: "value"})
	logger.Warn(ctx, "warn message", Field{Key: "key", Value: "value"})
	logger.Error(ctx, "error message", Field{Key: "key", Value: "value"})

	// Test with fields
	loggerWithFields := logger.WithFields(Field{Key: "test", Value: "value"})
	loggerWithFields.Info(ctx, "test")
	loggerWithFields.Debug(ctx, "test debug")
	loggerWithFields.Warn(ctx, "test warn")
	loggerWithFields.Error(ctx, "test error")

	// Test with context
	loggerWithContext := logger.WithContext(ctx)
	loggerWithContext.Info(ctx, "test with context")
	loggerWithContext.Debug(ctx, "debug with context")
	loggerWithContext.Warn(ctx, "warn with context")
	loggerWithContext.Error(ctx, "error with context")

	// Test level operations
	logger.SetLevel(DebugLevel)
	if logger.GetLevel() != DebugLevel {
		t.Errorf("NoOpLogger level should be DebugLevel after SetLevel, got %v", logger.GetLevel())
	}
}
