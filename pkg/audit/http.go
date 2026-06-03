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
	"net"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jeremyhahn/go-objstore/pkg/adapters"
)

// httpStatusRecorder captures the response status code for audit logging.
type httpStatusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *httpStatusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (r *httpStatusRecorder) Write(b []byte) (int, error) {
	return r.ResponseWriter.Write(b)
}

// AuditHTTPMiddleware is the net/http counterpart of AuditMiddleware, used by
// transports without gin (QUIC, MCP HTTP). It records one audit event per
// auditable request using the same event-type and path taxonomy.
func AuditHTTPMiddleware(auditLogger AuditLogger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requestID := GetRequestID(r.Context())
			if requestID == "" {
				requestID = sanitizeRequestID(r.Header.Get("X-Request-ID"))
			}
			if requestID == "" {
				requestID = uuid.New().String()
			}

			start := time.Now()
			ctx := context.WithValue(r.Context(), AuditLoggerKey, auditLogger)
			ctx = context.WithValue(ctx, RequestIDKey, requestID)
			rec := &httpStatusRecorder{ResponseWriter: w, status: http.StatusOK}

			next.ServeHTTP(rec, r.WithContext(ctx))

			if !shouldAuditRequest(r.URL.Path, r.Method) {
				return
			}

			clientIP := r.RemoteAddr
			if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
				clientIP = host
			}

			result := ResultSuccess
			if rec.status >= 400 {
				result = ResultFailure
			}

			event := &AuditEvent{
				Timestamp:  start,
				EventType:  determineEventType(r.Method, r.URL.Path),
				Action:     r.Method + " " + r.URL.Path,
				Result:     result,
				IPAddress:  clientIP,
				RequestID:  requestID,
				Method:     r.Method,
				StatusCode: rec.status,
				Duration:   time.Since(start),
			}
			_ = auditLogger.LogEvent(ctx, event) // #nosec G104 -- Audit logging errors are logged internally, should not block operations
		})
	}
}

// LogRPC records one audit event for a JSON-RPC request on a non-HTTP
// transport (unix socket, MCP stdio). The transport string labels the source
// (e.g. "unix", "mcp-stdio").
func LogRPC(ctx context.Context, auditLogger AuditLogger, transport, method string, principal *adapters.Principal, start time.Time, rpcErr error) {
	if auditLogger == nil {
		return
	}

	result := ResultSuccess
	errorMessage := ""
	if rpcErr != nil {
		result = ResultFailure
		errorMessage = rpcErr.Error()
	}

	principalName := ""
	userID := ""
	if principal != nil {
		principalName = principal.Name
		userID = principal.ID
	}

	event := &AuditEvent{
		Timestamp:    start,
		EventType:    determineRPCEventType(method),
		UserID:       userID,
		Principal:    principalName,
		Action:       transport + " " + method,
		Result:       result,
		ErrorMessage: errorMessage,
		RequestID:    GetRequestID(ctx),
		Method:       method,
		Duration:     time.Since(start),
	}
	_ = auditLogger.LogEvent(ctx, event) // #nosec G104 -- Audit logging errors are logged internally, should not block operations
}

// determineRPCEventType maps a JSON-RPC method name (unix protocol or MCP tool
// name) to an audit event type.
func determineRPCEventType(method string) EventType {
	switch {
	case contains(method, "put") || contains(method, "archive"):
		return EventObjectCreated
	case contains(method, "delete") || contains(method, "remove"):
		return EventObjectDeleted
	case contains(method, "update_metadata"):
		return EventObjectMetadataUpdated
	case contains(method, "list"):
		return EventListObjects
	case contains(method, "policy") || contains(method, "policies") || contains(method, "replication"):
		return EventPolicyChanged
	default:
		return EventObjectAccessed
	}
}
