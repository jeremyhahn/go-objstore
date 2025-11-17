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
	"context"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// Context keys for storing audit logger and request info
type contextKey string

const (
	// AuditLoggerKey is the context key for the audit logger
	AuditLoggerKey contextKey = "audit_logger"

	// RequestIDKey is the context key for the request ID
	RequestIDKey contextKey = "request_id"

	// RequestStartTimeKey is the context key for request start time
	RequestStartTimeKey contextKey = "request_start_time"
)

// GetAuditLogger retrieves the audit logger from the context
func GetAuditLogger(ctx context.Context) AuditLogger {
	if logger, ok := ctx.Value(AuditLoggerKey).(AuditLogger); ok {
		return logger
	}
	return NewNoOpAuditLogger()
}

// GetRequestID retrieves the request ID from the context
func GetRequestID(ctx context.Context) string {
	if id, ok := ctx.Value(RequestIDKey).(string); ok {
		return id
	}
	return ""
}

// AuditMiddleware creates a Gin middleware for audit logging
func AuditMiddleware(auditLogger AuditLogger) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Generate or extract request ID
		requestID := c.GetHeader("X-Request-ID")
		if requestID == "" {
			requestID = uuid.New().String()
			c.Header("X-Request-ID", requestID)
		}

		// Record start time
		startTime := time.Now()

		// Store audit logger and request info in context
		c.Set("audit_logger", auditLogger)
		c.Set("request_id", requestID)
		c.Set("request_start_time", startTime)

		// Process request
		c.Next()

		// After request is processed, log the event
		duration := time.Since(startTime)
		statusCode := c.Writer.Status()
		method := c.Request.Method
		path := c.Request.URL.Path
		clientIP := c.ClientIP()

		// Extract principal from context if available
		principal := ""
		userID := ""
		if principalValue, exists := c.Get("principal"); exists {
			if p, ok := principalValue.(adapters.Principal); ok {
				principal = p.Name
				userID = p.ID
			}
		}

		// Determine event type based on method and path
		eventType := determineEventType(method, path)

		// Determine result
		result := ResultSuccess
		errorMessage := ""
		if statusCode >= 400 {
			result = ResultFailure
			if len(c.Errors) > 0 {
				errorMessage = c.Errors.Last().Error()
			}
		}

		// Extract resource information
		bucket, key := extractResourceInfo(c)

		// Log the audit event
		event := &AuditEvent{
			Timestamp:    startTime,
			EventType:    eventType,
			UserID:       userID,
			Principal:    principal,
			Bucket:       bucket,
			Key:          key,
			Action:       method + " " + path,
			Result:       result,
			ErrorMessage: errorMessage,
			IPAddress:    clientIP,
			RequestID:    requestID,
			Method:       method,
			StatusCode:   statusCode,
			Duration:     duration,
		}

		// Only log if it's a meaningful operation (not health checks, etc.)
		if shouldAuditRequest(path, method) {
			_ = auditLogger.LogEvent(c.Request.Context(), event) // #nosec G104 -- Audit logging errors are logged internally, should not block operations
		}
	}
}

// AuditUnaryInterceptor creates a gRPC unary interceptor for audit logging
func AuditUnaryInterceptor(auditLogger AuditLogger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		// Generate request ID
		requestID := uuid.New().String()

		// Extract metadata
		md, _ := metadata.FromIncomingContext(ctx)
		if ids := md.Get("x-request-id"); len(ids) > 0 {
			requestID = ids[0]
		}

		// Add request ID to outgoing metadata
		ctx = metadata.AppendToOutgoingContext(ctx, "x-request-id", requestID)

		// Record start time
		startTime := time.Now()

		// Store audit logger and request info in context
		ctx = context.WithValue(ctx, AuditLoggerKey, auditLogger)
		ctx = context.WithValue(ctx, RequestIDKey, requestID)
		ctx = context.WithValue(ctx, RequestStartTimeKey, startTime)

		// Extract client IP
		clientIP := extractClientIP(ctx)

		// Extract principal if available (would be set by auth interceptor)
		principal := ""
		userID := ""
		if principalValue := ctx.Value("principal"); principalValue != nil {
			if p, ok := principalValue.(adapters.Principal); ok {
				principal = p.Name
				userID = p.ID
			}
		}

		// Call the handler
		resp, err := handler(ctx, req)

		// Calculate duration
		duration := time.Since(startTime)

		// Determine result
		result := ResultSuccess
		errorMessage := ""
		statusCode := 0
		if err != nil {
			result = ResultFailure
			errorMessage = err.Error()
			if st, ok := status.FromError(err); ok {
				statusCode = int(st.Code())
			}
		}

		// Determine event type based on method
		eventType := determineGRPCEventType(info.FullMethod)

		// Extract resource information from request
		bucket, key := extractGRPCResourceInfo(req)

		// Log the audit event
		event := &AuditEvent{
			Timestamp:    startTime,
			EventType:    eventType,
			UserID:       userID,
			Principal:    principal,
			Bucket:       bucket,
			Key:          key,
			Action:       info.FullMethod,
			Result:       result,
			ErrorMessage: errorMessage,
			IPAddress:    clientIP,
			RequestID:    requestID,
			Method:       info.FullMethod,
			StatusCode:   statusCode,
			Duration:     duration,
		}

		// Only log meaningful operations
		if shouldAuditGRPCMethod(info.FullMethod) {
			_ = auditLogger.LogEvent(ctx, event) // #nosec G104 -- Audit logging errors are logged internally, should not block operations
		}

		return resp, err
	}
}

// AuditStreamInterceptor creates a gRPC stream interceptor for audit logging
func AuditStreamInterceptor(auditLogger AuditLogger) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		// Generate request ID
		requestID := uuid.New().String()

		// Extract metadata
		ctx := ss.Context()
		md, _ := metadata.FromIncomingContext(ctx)
		if ids := md.Get("x-request-id"); len(ids) > 0 {
			requestID = ids[0]
		}

		// Record start time
		startTime := time.Now()

		// Store audit logger and request info in context
		ctx = context.WithValue(ctx, AuditLoggerKey, auditLogger)
		ctx = context.WithValue(ctx, RequestIDKey, requestID)
		ctx = context.WithValue(ctx, RequestStartTimeKey, startTime)

		// Extract client IP
		clientIP := extractClientIP(ctx)

		// Extract principal if available
		principal := ""
		userID := ""
		if principalValue := ctx.Value("principal"); principalValue != nil {
			if p, ok := principalValue.(adapters.Principal); ok {
				principal = p.Name
				userID = p.ID
			}
		}

		// Wrap the stream with our context
		wrappedStream := &auditServerStream{
			ServerStream: ss,
			ctx:          ctx,
		}

		// Call the handler
		err := handler(srv, wrappedStream)

		// Calculate duration
		duration := time.Since(startTime)

		// Determine result
		result := ResultSuccess
		errorMessage := ""
		statusCode := 0
		if err != nil {
			result = ResultFailure
			errorMessage = err.Error()
			if st, ok := status.FromError(err); ok {
				statusCode = int(st.Code())
			}
		}

		// Determine event type
		eventType := determineGRPCEventType(info.FullMethod)

		// Log the audit event
		event := &AuditEvent{
			Timestamp:    startTime,
			EventType:    eventType,
			UserID:       userID,
			Principal:    principal,
			Action:       info.FullMethod,
			Result:       result,
			ErrorMessage: errorMessage,
			IPAddress:    clientIP,
			RequestID:    requestID,
			Method:       info.FullMethod,
			StatusCode:   statusCode,
			Duration:     duration,
		}

		// Only log meaningful operations
		if shouldAuditGRPCMethod(info.FullMethod) {
			_ = auditLogger.LogEvent(ctx, event) // #nosec G104 -- Audit logging errors are logged internally, should not block operations
		}

		return err
	}
}

// auditServerStream wraps grpc.ServerStream to provide a custom context
type auditServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *auditServerStream) Context() context.Context {
	return s.ctx
}

// Helper functions

func determineEventType(method, path string) EventType {
	switch method {
	case "GET":
		if contains(path, "/metadata") {
			return EventObjectAccessed
		}
		if contains(path, "/objects") {
			if contains(path, "?") || path == "/objects" {
				return EventListObjects
			}
			return EventObjectAccessed
		}
	case "PUT":
		if contains(path, "/metadata") {
			return EventObjectMetadataUpdated
		}
		if contains(path, "/lifecycle") || contains(path, "/policy") {
			return EventPolicyChanged
		}
		return EventObjectCreated
	case "POST":
		return EventObjectCreated
	case "DELETE":
		if contains(path, "/bucket") {
			return EventBucketDeleted
		}
		return EventObjectDeleted
	}
	return EventObjectAccessed
}

func determineGRPCEventType(fullMethod string) EventType {
	if contains(fullMethod, "Get") {
		if contains(fullMethod, "Metadata") {
			return EventObjectAccessed
		}
		return EventObjectAccessed
	}
	if contains(fullMethod, "Put") {
		return EventObjectCreated
	}
	if contains(fullMethod, "Delete") {
		return EventObjectDeleted
	}
	if contains(fullMethod, "List") {
		return EventListObjects
	}
	if contains(fullMethod, "UpdateMetadata") {
		return EventObjectMetadataUpdated
	}
	return EventObjectAccessed
}

func extractResourceInfo(c *gin.Context) (bucket, key string) {
	// Extract from URL parameters
	key = c.Param("key")
	if key != "" && key[0] == '/' {
		key = key[1:]
	}

	// For this implementation, bucket is not explicitly in the path
	// You may need to adjust this based on your URL structure
	bucket = c.Query("bucket")
	if bucket == "" {
		bucket = "default"
	}

	return bucket, key
}

func extractGRPCResourceInfo(req any) (bucket, key string) {
	// Use type assertion to extract key from common request types
	type KeyGetter interface {
		GetKey() string
	}

	if kg, ok := req.(KeyGetter); ok {
		key = kg.GetKey()
	}

	// Bucket extraction would depend on your protobuf definitions
	bucket = "default"

	return bucket, key
}

func extractClientIP(ctx context.Context) string {
	// Try to get peer info
	if p, ok := peer.FromContext(ctx); ok {
		return p.Addr.String()
	}

	// Try to get from metadata
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if ips := md.Get("x-forwarded-for"); len(ips) > 0 {
			return ips[0]
		}
		if ips := md.Get("x-real-ip"); len(ips) > 0 {
			return ips[0]
		}
	}

	return "unknown"
}

func shouldAuditRequest(path, method string) bool {
	// Don't audit health checks and metrics
	if path == "/health" || path == "/metrics" || path == "/ping" {
		return false
	}
	// Audit all other requests
	return true
}

func shouldAuditGRPCMethod(fullMethod string) bool {
	// Don't audit health checks
	if contains(fullMethod, "Health") {
		return false
	}
	// Audit all other methods
	return true
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && (s[:len(substr)] == substr || s[len(s)-len(substr):] == substr || containsSubstring(s, substr)))
}

func containsSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
