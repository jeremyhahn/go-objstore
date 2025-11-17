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

// Package adapters provides interfaces for pluggable logging and authentication.
package adapters

import (
	"context"
	"log/slog"
	"os"
)

// LogLevel represents the severity level of a log message.
type LogLevel int

const (
	// DebugLevel for detailed debugging information.
	DebugLevel LogLevel = iota
	// InfoLevel for general informational messages.
	InfoLevel
	// WarnLevel for warning messages.
	WarnLevel
	// ErrorLevel for error messages.
	ErrorLevel
)

// String returns the string representation of the log level.
func (l LogLevel) String() string {
	switch l {
	case DebugLevel:
		return "DEBUG"
	case InfoLevel:
		return "INFO"
	case WarnLevel:
		return "WARN"
	case ErrorLevel:
		return "ERROR"
	default:
		return "UNKNOWN"
	}
}

// Field represents a structured logging field (key-value pair).
type Field struct {
	Key   string
	Value any
}

// Logger defines the interface for pluggable logging implementations.
// Applications can implement this interface to integrate the library with
// their native logging frameworks (e.g., zap, zerolog, logrus).
type Logger interface {
	// Debug logs a debug-level message with optional fields.
	Debug(ctx context.Context, msg string, fields ...Field)

	// Info logs an info-level message with optional fields.
	Info(ctx context.Context, msg string, fields ...Field)

	// Warn logs a warning-level message with optional fields.
	Warn(ctx context.Context, msg string, fields ...Field)

	// Error logs an error-level message with optional fields.
	Error(ctx context.Context, msg string, fields ...Field)

	// WithFields returns a new Logger with the given fields added to all log entries.
	WithFields(fields ...Field) Logger

	// WithContext returns a new Logger with the given context.
	WithContext(ctx context.Context) Logger

	// SetLevel sets the minimum log level that will be output.
	SetLevel(level LogLevel)

	// GetLevel returns the current log level.
	GetLevel() LogLevel
}

// DefaultLogger is a simple implementation using Go's standard slog package.
type DefaultLogger struct {
	logger *slog.Logger
	level  LogLevel
	fields []Field
	ctx    context.Context
}

// NewDefaultLogger creates a new default logger instance using slog.
func NewDefaultLogger() Logger {
	opts := &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}
	handler := slog.NewJSONHandler(os.Stdout, opts)
	return &DefaultLogger{
		logger: slog.New(handler),
		level:  InfoLevel,
		fields: make([]Field, 0),
	}
}

// Debug logs a debug-level message.
func (l *DefaultLogger) Debug(ctx context.Context, msg string, fields ...Field) {
	if l.level <= DebugLevel {
		l.log(DebugLevel, ctx, msg, fields...)
	}
}

// Info logs an info-level message.
func (l *DefaultLogger) Info(ctx context.Context, msg string, fields ...Field) {
	if l.level <= InfoLevel {
		l.log(InfoLevel, ctx, msg, fields...)
	}
}

// Warn logs a warning-level message.
func (l *DefaultLogger) Warn(ctx context.Context, msg string, fields ...Field) {
	if l.level <= WarnLevel {
		l.log(WarnLevel, ctx, msg, fields...)
	}
}

// Error logs an error-level message.
func (l *DefaultLogger) Error(ctx context.Context, msg string, fields ...Field) {
	if l.level <= ErrorLevel {
		l.log(ErrorLevel, ctx, msg, fields...)
	}
}

// WithFields returns a new logger with additional fields.
func (l *DefaultLogger) WithFields(fields ...Field) Logger {
	newFields := make([]Field, len(l.fields)+len(fields))
	copy(newFields, l.fields)
	copy(newFields[len(l.fields):], fields)

	return &DefaultLogger{
		logger: l.logger,
		level:  l.level,
		fields: newFields,
		ctx:    l.ctx,
	}
}

// WithContext returns a new logger with the given context.
func (l *DefaultLogger) WithContext(ctx context.Context) Logger {
	return &DefaultLogger{
		logger: l.logger,
		level:  l.level,
		fields: l.fields,
		ctx:    ctx,
	}
}

// SetLevel sets the minimum log level.
// Note: The level filtering is done in the log() method, not at the handler level.
func (l *DefaultLogger) SetLevel(level LogLevel) {
	l.level = level
}

// GetLevel returns the current log level.
func (l *DefaultLogger) GetLevel() LogLevel {
	return l.level
}

// log is the internal method that formats and writes log entries using slog.
func (l *DefaultLogger) log(level LogLevel, ctx context.Context, msg string, fields ...Field) {
	// Combine instance fields with provided fields
	allFields := make([]Field, 0, len(l.fields)+len(fields))
	allFields = append(allFields, l.fields...)
	allFields = append(allFields, fields...)

	// Use context if available
	if ctx == nil && l.ctx != nil {
		ctx = l.ctx
	}
	if ctx == nil {
		ctx = context.Background()
	}

	// Convert our Fields to slog.Attr
	attrs := make([]slog.Attr, len(allFields))
	for i, field := range allFields {
		attrs[i] = slog.Any(field.Key, field.Value)
	}

	// Map our LogLevel to slog.Level
	var slogLevel slog.Level
	switch level {
	case DebugLevel:
		slogLevel = slog.LevelDebug
	case InfoLevel:
		slogLevel = slog.LevelInfo
	case WarnLevel:
		slogLevel = slog.LevelWarn
	case ErrorLevel:
		slogLevel = slog.LevelError
	default:
		slogLevel = slog.LevelInfo
	}

	// Log with slog
	l.logger.LogAttrs(ctx, slogLevel, msg, attrs...)
}

// NoOpLogger is a logger that discards all log messages.
// Useful for testing or when logging is not desired.
type NoOpLogger struct {
	level LogLevel
}

// NewNoOpLogger creates a new no-op logger.
func NewNoOpLogger() Logger {
	return &NoOpLogger{level: ErrorLevel}
}

func (l *NoOpLogger) Debug(ctx context.Context, msg string, fields ...Field) {}
func (l *NoOpLogger) Info(ctx context.Context, msg string, fields ...Field)  {}
func (l *NoOpLogger) Warn(ctx context.Context, msg string, fields ...Field)  {}
func (l *NoOpLogger) Error(ctx context.Context, msg string, fields ...Field) {}
func (l *NoOpLogger) WithFields(fields ...Field) Logger                      { return l }
func (l *NoOpLogger) WithContext(ctx context.Context) Logger                 { return l }
func (l *NoOpLogger) SetLevel(level LogLevel)                                { l.level = level }
func (l *NoOpLogger) GetLevel() LogLevel                                     { return l.level }
