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
	"fmt"
	"net"
	"net/http"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
)

// RequestIDHTTPMiddleware is the net/http counterpart of RequestIDMiddleware,
// used by transports without gin (QUIC, MCP HTTP). Inbound X-Request-ID values
// are sanitized; invalid or absent values are replaced with a fresh ID. The ID
// is stored in the request context and echoed in the response header.
func RequestIDHTTPMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := sanitizeRequestID(r.Header.Get(RequestIDHeader))
		if requestID == "" {
			requestID = generateRequestID()
		}

		w.Header().Set(RequestIDHeader, requestID)
		ctx := context.WithValue(r.Context(), RequestIDContextKey, requestID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// EnsureRequestID returns a context carrying a request ID, generating one if
// absent. Used by non-HTTP transports (unix socket, MCP stdio) so every
// request is traceable.
func EnsureRequestID(ctx context.Context) (context.Context, string) {
	if id := GetRequestIDFromContext(ctx); id != "" {
		return ctx, id
	}
	id := generateRequestID()
	return context.WithValue(ctx, RequestIDContextKey, id), id
}

// HTTPMiddleware returns a net/http middleware enforcing this rate limiter,
// used by transports without gin (QUIC, MCP HTTP). The client key is the
// remote address host when per-IP limiting is enabled.
func (l *RateLimiter) HTTPMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		clientIP := r.RemoteAddr
		if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
			clientIP = host
		}

		if !l.AllowKey(clientIP) {
			l.logger.Warn(r.Context(), "Rate limit exceeded",
				adapters.Field{Key: "client_ip", Value: clientIP},
				adapters.Field{Key: "path", Value: r.URL.Path},
				adapters.Field{Key: "method", Value: r.Method},
			)

			w.Header().Set("X-RateLimit-Limit", fmt.Sprintf("%.0f", l.config.RequestsPerSecond))
			w.Header().Set("X-RateLimit-Burst", fmt.Sprintf("%d", l.config.Burst))
			w.Header().Set("Retry-After", "1")
			http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
			return
		}

		next.ServeHTTP(w, r)
	})
}
