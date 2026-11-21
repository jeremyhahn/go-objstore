package serverconfig

import (
	"context"
	"log/slog"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
)

// slogLogger hands the replication manager's log entries to the server's
// operational logger (slog's default), so a failed scheduled sync is
// reported with its policy and cause instead of being discarded.
type slogLogger struct {
	attrs []any
	level adapters.LogLevel
}

// newSlogLogger returns a replication logger writing through slog.Default.
func newSlogLogger() adapters.Logger {
	return &slogLogger{level: adapters.DebugLevel}
}

func (l *slogLogger) log(ctx context.Context, level adapters.LogLevel, slogLevel slog.Level, msg string, fields []adapters.Field) {
	if level < l.level {
		return
	}
	args := make([]any, 0, len(l.attrs)+2*len(fields))
	args = append(args, l.attrs...)
	for _, f := range fields {
		args = append(args, f.Key, f.Value)
	}
	slog.Default().Log(ctx, slogLevel, msg, args...)
}

func (l *slogLogger) Debug(ctx context.Context, msg string, fields ...adapters.Field) {
	l.log(ctx, adapters.DebugLevel, slog.LevelDebug, msg, fields)
}

func (l *slogLogger) Info(ctx context.Context, msg string, fields ...adapters.Field) {
	l.log(ctx, adapters.InfoLevel, slog.LevelInfo, msg, fields)
}

func (l *slogLogger) Warn(ctx context.Context, msg string, fields ...adapters.Field) {
	l.log(ctx, adapters.WarnLevel, slog.LevelWarn, msg, fields)
}

func (l *slogLogger) Error(ctx context.Context, msg string, fields ...adapters.Field) {
	l.log(ctx, adapters.ErrorLevel, slog.LevelError, msg, fields)
}

func (l *slogLogger) WithFields(fields ...adapters.Field) adapters.Logger {
	attrs := make([]any, 0, len(l.attrs)+2*len(fields))
	attrs = append(attrs, l.attrs...)
	for _, f := range fields {
		attrs = append(attrs, f.Key, f.Value)
	}
	return &slogLogger{attrs: attrs, level: l.level}
}

func (l *slogLogger) WithContext(_ context.Context) adapters.Logger {
	return l
}

func (l *slogLogger) SetLevel(level adapters.LogLevel) {
	l.level = level
}

func (l *slogLogger) GetLevel() adapters.LogLevel {
	return l.level
}
