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

package middleware

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// contextKey is a custom type for context keys to avoid collisions
type contextKey string

const (
	// RequestIDHeader is the header name for request IDs
	RequestIDHeader = "X-Request-ID"

	// RequestIDContextKey is the context key for storing request IDs
	RequestIDContextKey contextKey = "request_id"

	// GRPCRequestIDKey is the metadata key for gRPC request IDs
	GRPCRequestIDKey = "x-request-id"
)

// generateRequestID generates a unique request ID
func generateRequestID() string {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		// Use time-based UUID-like format instead of weak random
		return fmt.Sprintf("%d-%d", time.Now().UnixNano(), os.Getpid())
	}
	return hex.EncodeToString(bytes)
}

// RequestIDMiddleware creates a Gin middleware that generates/extracts request IDs
func RequestIDMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Check if request ID already exists in header
		requestID := c.GetHeader(RequestIDHeader)

		// Generate new ID if not present
		if requestID == "" {
			requestID = generateRequestID()
		}

		// Store in context for use by handlers and other middleware
		c.Set(RequestIDContextKey, requestID)

		// Add to response headers
		c.Header(RequestIDHeader, requestID)

		// Add to Gin context's request context for logging
		ctx := context.WithValue(c.Request.Context(), RequestIDContextKey, requestID)
		c.Request = c.Request.WithContext(ctx)

		c.Next()
	}
}

// GetRequestIDFromGinContext retrieves the request ID from a Gin context
func GetRequestIDFromGinContext(c *gin.Context) string {
	if requestID, exists := c.Get(RequestIDContextKey); exists {
		if id, ok := requestID.(string); ok {
			return id
		}
	}
	return ""
}

// GetRequestIDFromContext retrieves the request ID from a standard context
func GetRequestIDFromContext(ctx context.Context) string {
	if requestID := ctx.Value(RequestIDContextKey); requestID != nil {
		if id, ok := requestID.(string); ok {
			return id
		}
	}
	return ""
}

// RequestIDUnaryInterceptor creates a gRPC unary interceptor for request ID handling
func RequestIDUnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		// Extract request ID from incoming metadata
		requestID := extractRequestIDFromMetadata(ctx)

		// Generate new ID if not present
		if requestID == "" {
			requestID = generateRequestID()
		}

		// Add to context for use by handlers
		ctx = context.WithValue(ctx, RequestIDContextKey, requestID)

		// Add to outgoing metadata (response headers)
		_ = grpc.SetHeader(ctx, metadata.Pairs(GRPCRequestIDKey, requestID)) // Ignore error, continue processing

		return handler(ctx, req)
	}
}

// RequestIDStreamInterceptor creates a gRPC stream interceptor for request ID handling
func RequestIDStreamInterceptor() grpc.StreamServerInterceptor {
	return func(
		srv any,
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		ctx := ss.Context()

		// Extract request ID from incoming metadata
		requestID := extractRequestIDFromMetadata(ctx)

		// Generate new ID if not present
		if requestID == "" {
			requestID = generateRequestID()
		}

		// Add to context for use by handlers
		ctx = context.WithValue(ctx, RequestIDContextKey, requestID)

		// Add to outgoing metadata (response headers)
		_ = grpc.SetHeader(ctx, metadata.Pairs(GRPCRequestIDKey, requestID)) // Ignore error, continue processing

		// Wrap the server stream with our context
		wrappedStream := &requestIDServerStream{
			ServerStream: ss,
			ctx:          ctx,
		}

		return handler(srv, wrappedStream)
	}
}

// extractRequestIDFromMetadata extracts request ID from gRPC metadata
func extractRequestIDFromMetadata(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}

	if values := md.Get(GRPCRequestIDKey); len(values) > 0 {
		return values[0]
	}

	return ""
}

// requestIDServerStream wraps grpc.ServerStream to provide a custom context
type requestIDServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

// Context returns the wrapper's context
func (s *requestIDServerStream) Context() context.Context {
	return s.ctx
}
