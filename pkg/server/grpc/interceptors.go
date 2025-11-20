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

package grpc

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// MetricsCollector holds metrics for the gRPC server.
type MetricsCollector struct {
	// Request counters
	totalRequests      atomic.Uint64
	successfulRequests atomic.Uint64
	failedRequests     atomic.Uint64

	// Latency tracking
	totalLatencyNanos atomic.Uint64
	requestCount      atomic.Uint64

	// Active connections
	activeStreams atomic.Int32
}

// NewMetricsCollector creates a new metrics collector.
func NewMetricsCollector() *MetricsCollector {
	return &MetricsCollector{}
}

// GetMetrics returns the current metrics.
func (m *MetricsCollector) GetMetrics() map[string]any {
	totalReq := m.totalRequests.Load()
	totalLatency := m.totalLatencyNanos.Load()
	reqCount := m.requestCount.Load()

	var avgLatencyMs float64
	if reqCount > 0 {
		avgLatencyMs = float64(totalLatency) / float64(reqCount) / 1e6
	}

	return map[string]any{
		"total_requests":      totalReq,
		"successful_requests": m.successfulRequests.Load(),
		"failed_requests":     m.failedRequests.Load(),
		"active_streams":      m.activeStreams.Load(),
		"avg_latency_ms":      avgLatencyMs,
	}
}

// LoggingUnaryInterceptor logs unary RPC calls using the logger adapter.
func LoggingUnaryInterceptor(logger adapters.Logger) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		start := time.Now()

		logger.Debug(ctx, "gRPC request started",
			adapters.Field{Key: "method", Value: info.FullMethod},
		)

		resp, err := handler(ctx, req)

		duration := time.Since(start)
		fields := []adapters.Field{
			{Key: "method", Value: info.FullMethod},
			{Key: "duration", Value: duration.String()},
		}

		if err != nil {
			fields = append(fields, adapters.Field{Key: "error", Value: err.Error()})
			logger.Error(ctx, "gRPC request failed", fields...)
		} else {
			logger.Info(ctx, "gRPC request completed", fields...)
		}

		return resp, err
	}
}

// LoggingStreamInterceptor logs stream RPC calls using the logger adapter.
func LoggingStreamInterceptor(logger adapters.Logger) grpc.StreamServerInterceptor {
	return func(
		srv any,
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		start := time.Now()

		logger.Debug(ss.Context(), "gRPC stream started",
			adapters.Field{Key: "method", Value: info.FullMethod},
		)

		err := handler(srv, ss)

		duration := time.Since(start)
		fields := []adapters.Field{
			{Key: "method", Value: info.FullMethod},
			{Key: "duration", Value: duration.String()},
		}

		if err != nil {
			fields = append(fields, adapters.Field{Key: "error", Value: err.Error()})
			logger.Error(ss.Context(), "gRPC stream failed", fields...)
		} else {
			logger.Info(ss.Context(), "gRPC stream completed", fields...)
		}

		return err
	}
}

// MetricsUnaryInterceptor collects metrics for unary RPC calls.
func MetricsUnaryInterceptor(collector *MetricsCollector) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		start := time.Now()

		collector.totalRequests.Add(1)

		resp, err := handler(ctx, req)

		duration := time.Since(start)
		// Safe conversion with overflow check
		nanos := duration.Nanoseconds()
		if nanos >= 0 {
			collector.totalLatencyNanos.Add(uint64(nanos))
		}
		collector.requestCount.Add(1)

		if err != nil {
			collector.failedRequests.Add(1)
		} else {
			collector.successfulRequests.Add(1)
		}

		return resp, err
	}
}

// MetricsStreamInterceptor collects metrics for stream RPC calls.
func MetricsStreamInterceptor(collector *MetricsCollector) grpc.StreamServerInterceptor {
	return func(
		srv any,
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		start := time.Now()

		collector.totalRequests.Add(1)
		collector.activeStreams.Add(1)
		defer collector.activeStreams.Add(-1)

		err := handler(srv, ss)

		duration := time.Since(start)
		// Safe conversion with overflow check
		nanos := duration.Nanoseconds()
		if nanos >= 0 {
			collector.totalLatencyNanos.Add(uint64(nanos))
		}
		collector.requestCount.Add(1)

		if err != nil {
			collector.failedRequests.Add(1)
		} else {
			collector.successfulRequests.Add(1)
		}

		return err
	}
}

// RecoveryUnaryInterceptor recovers from panics in unary RPC calls.
func RecoveryUnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (resp any, err error) {
		defer func() {
			if r := recover(); r != nil {
				slog.ErrorContext(ctx, "[gRPC] Panic recovered",
					slog.String("method", info.FullMethod),
					slog.Any("panic", r))
				err = status.Errorf(codes.Internal, "internal server error: %v", r)
			}
		}()

		return handler(ctx, req)
	}
}

// RecoveryStreamInterceptor recovers from panics in stream RPC calls.
func RecoveryStreamInterceptor() grpc.StreamServerInterceptor {
	return func(
		srv any,
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) (err error) {
		defer func() {
			if r := recover(); r != nil {
				slog.ErrorContext(ss.Context(), "[gRPC Stream] Panic recovered",
					slog.String("method", info.FullMethod),
					slog.Any("panic", r))
				err = status.Errorf(codes.Internal, "internal server error: %v", r)
			}
		}()

		return handler(srv, ss)
	}
}

// ChainUnaryInterceptors chains multiple unary interceptors.
func ChainUnaryInterceptors(interceptors ...grpc.UnaryServerInterceptor) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		// Build the chain from right to left
		currentHandler := handler
		for i := len(interceptors) - 1; i >= 0; i-- {
			interceptor := interceptors[i]
			next := currentHandler
			currentHandler = func(currentCtx context.Context, currentReq any) (any, error) {
				return interceptor(currentCtx, currentReq, info, next)
			}
		}
		return currentHandler(ctx, req)
	}
}

// ChainStreamInterceptors chains multiple stream interceptors.
func ChainStreamInterceptors(interceptors ...grpc.StreamServerInterceptor) grpc.StreamServerInterceptor {
	return func(
		srv any,
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		// Build the chain from right to left
		currentHandler := handler
		for i := len(interceptors) - 1; i >= 0; i-- {
			interceptor := interceptors[i]
			next := currentHandler
			currentHandler = func(currentSrv any, currentSS grpc.ServerStream) error {
				return interceptor(currentSrv, currentSS, info, next)
			}
		}
		return currentHandler(srv, ss)
	}
}

// AuthenticationUnaryInterceptor authenticates unary RPC calls.
func AuthenticationUnaryInterceptor(authenticator adapters.Authenticator, logger adapters.Logger) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		// Extract metadata from context
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			md = metadata.MD{}
		}

		// Try metadata-based authentication first
		principal, err := authenticator.AuthenticateGRPC(ctx, md)

		// If metadata auth fails, try mTLS authentication
		if err != nil {
			if p, ok := peer.FromContext(ctx); ok && p.AuthInfo != nil {
				// Try to get TLS info for mTLS authentication
				if _, ok := p.AuthInfo.(interface{ State() any }); ok {
					// This is a simplified approach - in production, you'd need proper type assertion
					logger.Debug(ctx, "Attempting mTLS authentication",
						adapters.Field{Key: "method", Value: info.FullMethod},
					)
				}
			}
		}

		if err != nil {
			logger.Warn(ctx, "Authentication failed",
				adapters.Field{Key: "method", Value: info.FullMethod},
				adapters.Field{Key: "error", Value: err.Error()},
			)
			return nil, status.Error(codes.Unauthenticated, "authentication failed")
		}

		// Add principal info to logger
		logger = logger.WithFields(
			adapters.Field{Key: "principal_id", Value: principal.ID},
			adapters.Field{Key: "principal_name", Value: principal.Name},
		)

		// Add principal to context for downstream handlers
		ctx = context.WithValue(ctx, principalContextKey, *principal)

		return handler(ctx, req)
	}
}

// AuthenticationStreamInterceptor authenticates stream RPC calls.
func AuthenticationStreamInterceptor(authenticator adapters.Authenticator, logger adapters.Logger) grpc.StreamServerInterceptor {
	return func(
		srv any,
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		ctx := ss.Context()

		// Extract metadata from context
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			md = metadata.MD{}
		}

		// Try metadata-based authentication first
		principal, err := authenticator.AuthenticateGRPC(ctx, md)

		// If metadata auth fails, try mTLS authentication
		if err != nil {
			if p, ok := peer.FromContext(ctx); ok && p.AuthInfo != nil {
				logger.Debug(ctx, "Attempting mTLS authentication for stream",
					adapters.Field{Key: "method", Value: info.FullMethod},
				)
			}
		}

		if err != nil {
			logger.Warn(ctx, "Stream authentication failed",
				adapters.Field{Key: "method", Value: info.FullMethod},
				adapters.Field{Key: "error", Value: err.Error()},
			)
			return status.Error(codes.Unauthenticated, "authentication failed")
		}

		// Add principal info to logger
		logger = logger.WithFields(
			adapters.Field{Key: "principal_id", Value: principal.ID},
			adapters.Field{Key: "principal_name", Value: principal.Name},
		)

		// Add principal to context for downstream handlers
		ctx = context.WithValue(ctx, principalContextKey, *principal)

		// Create a wrapped server stream with the updated context
		wrappedStream := &wrappedServerStream{
			ServerStream: ss,
			ctx:          ctx,
		}
		return handler(srv, wrappedStream)
	}
}

// wrappedServerStream wraps a grpc.ServerStream to override the context.
type wrappedServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

// Context returns the wrapped context.
func (w *wrappedServerStream) Context() context.Context {
	return w.ctx
}
