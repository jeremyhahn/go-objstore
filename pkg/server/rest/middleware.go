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
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/audit"
	"github.com/jeremyhahn/go-objstore/pkg/server/metrics"
)

// MetricsMiddleware records each request into the shared metrics registry,
// labeled by the "rest" transport and the response status code. The /metrics
// endpoint itself is not recorded so scrapes do not inflate the counters.
func MetricsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.Request.URL.Path == "/metrics" {
			c.Next()
			return
		}
		start := time.Now()
		c.Next()
		metrics.Default.RecordRequest(metrics.TransportREST, strconv.Itoa(c.Writer.Status()), time.Since(start))
	}
}

// principalContextKey is the gin context key under which the authenticated
// principal is stored by AuthenticationMiddleware.
const principalContextKey = "principal"

// CORSMiddleware handles Cross-Origin Resource Sharing.
//
// The allowedOrigins parameter controls which origins may access the API:
//
//   - When empty/nil (or equal to ["*"]), the middleware allows all origins by
//     sending "Access-Control-Allow-Origin: *". In this permissive mode the
//     "Access-Control-Allow-Credentials" header is NOT sent, since the wildcard
//     origin combined with credentials is invalid per the Fetch standard.
//   - When set to a specific allowlist, the middleware echoes the request's
//     Origin header only if it is present in the allowlist (along with
//     "Vary: Origin"), and in that case it is safe to also send
//     "Access-Control-Allow-Credentials: true". Requests whose Origin is not
//     allowlisted receive no "Access-Control-Allow-Origin" header, so the
//     browser blocks the cross-origin response.
func CORSMiddleware(allowedOrigins []string) gin.HandlerFunc {
	allowAll := len(allowedOrigins) == 0 || (len(allowedOrigins) == 1 && allowedOrigins[0] == "*")

	return func(c *gin.Context) {
		header := c.Writer.Header()

		if allowAll {
			// Permissive default: wildcard origin, no credentials (the wildcard
			// plus credentials combination is invalid and unsafe).
			header.Set("Access-Control-Allow-Origin", "*")
		} else {
			origin := c.Request.Header.Get("Origin")
			if originAllowed(origin, allowedOrigins) {
				header.Set("Access-Control-Allow-Origin", origin)
				header.Add("Vary", "Origin")
				header.Set("Access-Control-Allow-Credentials", "true")
			}
		}

		header.Set("Access-Control-Allow-Headers", "Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization, accept, origin, Cache-Control, X-Requested-With")
		header.Set("Access-Control-Allow-Methods", "POST, OPTIONS, GET, PUT, DELETE, HEAD")
		header.Set("Access-Control-Expose-Headers", "Content-Length, ETag, Last-Modified")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}

		c.Next()
	}
}

// originAllowed reports whether the given request origin is present in the
// allowlist. An empty origin never matches.
func originAllowed(origin string, allowedOrigins []string) bool {
	if origin == "" {
		return false
	}
	for _, allowed := range allowedOrigins {
		if allowed == origin {
			return true
		}
	}
	return false
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

// AuthenticationMiddleware authenticates HTTP requests using the provided
// authenticator. Public paths (/health, and /metrics when metricsPublic is
// set) bypass authentication entirely so they remain reachable behind
// restrictive authenticators (e.g. Prometheus scrapers and load-balancer
// health checks carry no credentials). Swagger documentation is not public
// and requires authentication.
func AuthenticationMiddleware(authenticator adapters.Authenticator, logger adapters.Logger, auditLogger audit.AuditLogger, metricsPublic bool) gin.HandlerFunc {
	return func(c *gin.Context) {
		if isPublicPath(c.Request.URL.Path, metricsPublic) {
			c.Next()
			return
		}

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
		c.Set(principalContextKey, principal)

		// Audit log successful authentication
		if auditLogger != nil {
			_ = auditLogger.LogAuthSuccess(c.Request.Context(), principal.ID, principal.Name, c.ClientIP(), requestID) // #nosec G104 -- Audit logging errors are logged internally, should not block operations
		}

		c.Next()
	}
}

// AuthorizationMiddleware enforces authorization on authenticated requests using
// the provided authorizer. It must run AFTER AuthenticationMiddleware so that the
// principal is present in the gin context. It derives the (action, resource) pair
// from the HTTP method and route, then calls authorizer.Authorize. On denial it
// responds with 403 Forbidden. The default authorizer (NoOpAuthorizer) allows
// everything, preserving prior behavior.
func AuthorizationMiddleware(authorizer adapters.Authorizer, logger adapters.Logger, auditLogger audit.AuditLogger, metricsPublic bool) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Public paths and swagger are exempt from authorization; swagger still
		// requires authentication, enforced by AuthenticationMiddleware.
		if isAuthzExemptPath(c.Request.URL.Path, metricsPublic) {
			c.Next()
			return
		}

		value, exists := c.Get(principalContextKey)
		principal, _ := value.(*adapters.Principal)
		if !exists || principal == nil {
			// No principal means authentication did not run or failed; deny.
			RespondWithError(c, http.StatusForbidden, "Forbidden")
			c.Abort()
			return
		}

		action, resource := deriveActionResource(c)

		if err := authorizer.Authorize(c.Request.Context(), principal, action, resource); err != nil {
			logger.Warn(c.Request.Context(), "Authorization denied",
				adapters.Field{Key: "error", Value: err.Error()},
				adapters.Field{Key: "path", Value: c.Request.URL.Path},
				adapters.Field{Key: "method", Value: c.Request.Method},
				adapters.Field{Key: "action", Value: action},
				adapters.Field{Key: "resource", Value: resource},
				adapters.Field{Key: "principal_id", Value: principal.ID},
			)

			// Reuse the auth-failure audit hook to record the denial.
			if auditLogger != nil {
				requestID := audit.GetRequestID(c.Request.Context())
				_ = auditLogger.LogAuthFailure(c.Request.Context(), principal.ID, principal.Name, c.ClientIP(), requestID, "authorization denied: "+err.Error()) // #nosec G104 -- Audit logging errors are logged internally, should not block operations
			}

			RespondWithError(c, http.StatusForbidden, "Forbidden")
			c.Abort()
			return
		}

		c.Next()
	}
}

// isPublicPath reports whether the path bypasses authentication entirely.
// Only /health is always public; /metrics is public when the server is
// configured with MetricsPublic. Swagger documentation requires
// authentication and is therefore never public.
func isPublicPath(path string, metricsPublic bool) bool {
	if path == "/metrics" {
		return metricsPublic
	}
	return path == "/health"
}

// isAuthzExemptPath reports whether the path is exempt from authorization.
// All public (unauthenticated) paths are exempt, as is /swagger, which
// requires authentication but no specific permission.
func isAuthzExemptPath(path string, metricsPublic bool) bool {
	return isPublicPath(path, metricsPublic) || strings.HasPrefix(path, "/swagger")
}

// deriveActionResource maps an HTTP request to a (action, resource) pair using
// the route taxonomy. Object and metadata operations use the object key as the
// resource; management operations use the resource category constants.
func deriveActionResource(c *gin.Context) (action, resource string) {
	path := c.Request.URL.Path
	method := c.Request.Method

	switch {
	case strings.Contains(path, "/replication"):
		return adapters.ActionAdmin, adapters.ResourceReplication
	case strings.Contains(path, "/policies"):
		return adapters.ActionAdmin, adapters.ResourcePolicy
	case strings.Contains(path, "/archive"):
		// Archive acts on an object key; key is supplied in the request body so
		// the route param is unavailable here. Use the policy resource category.
		return adapters.ActionAdmin, adapters.ResourcePolicy
	case method == http.MethodGet && c.Param("key") == "" && strings.HasSuffix(path, "/objects"):
		// GET on the bare objects collection (/objects, /api/v1/objects) is a
		// list operation with no specific resource.
		return adapters.ActionList, ""
	}

	// Object key is carried in the "key" route param for /objects, /exists,
	// and /metadata routes.
	key := strings.TrimPrefix(c.Param("key"), "/")

	switch {
	case strings.Contains(path, "/exists"):
		return adapters.ActionRead, key
	case strings.Contains(path, "/metadata"):
		if method == http.MethodPut {
			return adapters.ActionWrite, key
		}
		return adapters.ActionRead, key
	default:
		// /objects/*key
		switch method {
		case http.MethodPut:
			return adapters.ActionWrite, key
		case http.MethodDelete:
			return adapters.ActionDelete, key
		default:
			return adapters.ActionRead, key
		}
	}
}
