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

package rest

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/audit"
)

// CORSMiddleware handles Cross-Origin Resource Sharing
func CORSMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		c.Writer.Header().Set("Access-Control-Allow-Credentials", "true")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization, accept, origin, Cache-Control, X-Requested-With")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS, GET, PUT, DELETE, HEAD")
		c.Writer.Header().Set("Access-Control-Expose-Headers", "Content-Length, ETag, Last-Modified")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}

		c.Next()
	}
}

// LoggingMiddleware logs incoming requests and their response times
func LoggingMiddleware(logger adapters.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Start timer
		startTime := time.Now()

		// Process request
		c.Next()

		// Calculate latency
		latency := time.Since(startTime)

		// Log request details
		statusCode := c.Writer.Status()
		method := c.Request.Method
		path := c.Request.URL.Path
		clientIP := c.ClientIP()

		// Use the logger adapter
		fields := []adapters.Field{
			{Key: "method", Value: method},
			{Key: "path", Value: path},
			{Key: "status", Value: statusCode},
			{Key: "latency", Value: latency.String()},
			{Key: "client_ip", Value: clientIP},
		}

		switch {
		case statusCode >= 500:
			logger.Error(c.Request.Context(), "HTTP request completed", fields...)
		case statusCode >= 400:
			logger.Warn(c.Request.Context(), "HTTP request completed", fields...)
		default:
			logger.Info(c.Request.Context(), "HTTP request completed", fields...)
		}
	}
}

// ErrorHandlingMiddleware catches panics and returns proper error responses
func ErrorHandlingMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if err := recover(); err != nil {
				slog.ErrorContext(c.Request.Context(), "Panic recovered",
					slog.Any("panic", err))
				RespondWithError(c, 500, "Internal server error")
				c.Abort()
			}
		}()

		c.Next()
	}
}

// RequestSizeLimitMiddleware limits the maximum size of request bodies
func RequestSizeLimitMiddleware(maxSize int64) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Only enforce for PUT/POST requests with bodies
		if c.Request.Method == "PUT" || c.Request.Method == "POST" {
			// Get Content-Length header if present
			if c.Request.ContentLength > maxSize {
				RespondWithError(c, 413, "Request entity too large")
				c.Abort()
				return
			}

			// Limit the reader to prevent excessive memory usage
			c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxSize)
		}

		c.Next()
	}
}

// AuthenticationMiddleware authenticates HTTP requests using the provided authenticator
func AuthenticationMiddleware(authenticator adapters.Authenticator, logger adapters.Logger, auditLogger audit.AuditLogger) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Authenticate the request
		principal, err := authenticator.AuthenticateHTTP(c.Request.Context(), c.Request)
		requestID := audit.GetRequestID(c.Request.Context())

		if err != nil {
			logger.Warn(c.Request.Context(), "Authentication failed",
				adapters.Field{Key: "error", Value: err.Error()},
				adapters.Field{Key: "path", Value: c.Request.URL.Path},
				adapters.Field{Key: "method", Value: c.Request.Method},
			)

			// Audit log authentication failure
			if auditLogger != nil {
				_ = auditLogger.LogAuthFailure(c.Request.Context(), "", "", c.ClientIP(), requestID, err.Error()) // #nosec G104 -- Audit logging errors are logged internally, should not block operations
			}

			RespondWithError(c, 401, "Unauthorized")
			c.Abort()
			return
		}

		// Store principal in context for use by handlers
		c.Set("principal", principal)

		// Audit log successful authentication
		if auditLogger != nil {
			_ = auditLogger.LogAuthSuccess(c.Request.Context(), principal.ID, principal.Name, c.ClientIP(), requestID) // #nosec G104 -- Audit logging errors are logged internally, should not block operations
		}

		// Add principal info to logger context
		logger = logger.WithFields(
			adapters.Field{Key: "principal_id", Value: principal.ID},
			adapters.Field{Key: "principal_name", Value: principal.Name},
		)

		c.Next()
	}
}
