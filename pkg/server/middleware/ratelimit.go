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
	"sync"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// RateLimitConfig holds rate limiting configuration
type RateLimitConfig struct {
	// RequestsPerSecond is the number of requests allowed per second
	RequestsPerSecond float64

	// Burst is the maximum burst size
	Burst int

	// PerIP enables per-IP rate limiting (default: false = global rate limit)
	PerIP bool
}

// DefaultRateLimitConfig returns a rate limit config with sensible defaults
func DefaultRateLimitConfig() *RateLimitConfig {
	return &RateLimitConfig{
		RequestsPerSecond: 100,
		Burst:             200,
		PerIP:             false,
	}
}

// clientEntry holds a per-IP limiter with its last-seen timestamp for TTL eviction.
// lastSeen stores UnixNano so the hot path can refresh it without a write lock.
type clientEntry struct {
	limiter  *rate.Limiter
	lastSeen atomic.Int64
}

// idleClientTTL is the duration after which an idle per-IP limiter is evicted.
const idleClientTTL = 5 * time.Minute

// evictInterval controls how often the sweep runs.
const evictInterval = idleClientTTL

// RateLimiter is a stoppable rate limiter shared across transports. Create one
// per server with NewRateLimiter, attach it via GinMiddleware, HTTPMiddleware,
// UnaryInterceptor, or StreamInterceptor, and call Stop during server shutdown
// to terminate the background eviction goroutine.
type RateLimiter struct {
	config   *RateLimitConfig
	global   *rate.Limiter
	clients  map[string]*clientEntry
	mu       sync.RWMutex
	logger   adapters.Logger
	stopCh   chan struct{}
	stopOnce sync.Once
}

// NewRateLimiter creates a stoppable rate limiter.
func NewRateLimiter(config *RateLimitConfig, logger adapters.Logger) *RateLimiter {
	if config == nil {
		config = DefaultRateLimitConfig()
	}
	if logger == nil {
		logger = adapters.NewDefaultLogger()
	}

	l := &RateLimiter{
		config:  config,
		clients: make(map[string]*clientEntry),
		logger:  logger,
		stopCh:  make(chan struct{}),
	}

	if !config.PerIP {
		l.global = rate.NewLimiter(rate.Limit(config.RequestsPerSecond), config.Burst)
	} else {
		// Start a background goroutine to sweep idle limiters.
		go l.sweepLoop()
	}

	return l
}

// Stop terminates the background eviction goroutine. Safe to call multiple times.
func (l *RateLimiter) Stop() {
	l.stopOnce.Do(func() {
		close(l.stopCh)
	})
}

// sweepLoop periodically evicts per-IP limiters that have been idle longer than
// idleClientTTL, until Stop is called.
func (l *RateLimiter) sweepLoop() {
	ticker := time.NewTicker(evictInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			l.evictIdle()
		case <-l.stopCh:
			return
		}
	}
}

// evictIdle removes per-IP entries that have not been seen within idleClientTTL.
func (l *RateLimiter) evictIdle() {
	cutoff := time.Now().Add(-idleClientTTL).UnixNano()
	l.mu.Lock()
	defer l.mu.Unlock()
	for ip, entry := range l.clients {
		if entry.lastSeen.Load() < cutoff {
			delete(l.clients, ip)
		}
	}
}

// getLimiter returns the appropriate rate limiter for the client
func (l *RateLimiter) getLimiter(clientIP string) *rate.Limiter {
	if !l.config.PerIP {
		return l.global
	}

	now := time.Now().UnixNano()

	l.mu.RLock()
	entry, exists := l.clients[clientIP]
	l.mu.RUnlock()

	if exists {
		// Refresh last-seen atomically; no write lock on the hot path.
		entry.lastSeen.Store(now)
		return entry.limiter
	}

	// Create new limiter for this client
	l.mu.Lock()
	defer l.mu.Unlock()

	// Double-check after acquiring write lock
	if entry, exists := l.clients[clientIP]; exists {
		entry.lastSeen.Store(now)
		return entry.limiter
	}

	entry = &clientEntry{limiter: rate.NewLimiter(rate.Limit(l.config.RequestsPerSecond), l.config.Burst)}
	entry.lastSeen.Store(now)
	l.clients[clientIP] = entry

	return entry.limiter
}

// AllowKey reports whether a request identified by key is within the rate
// limit. Intended for non-HTTP transports (unix socket, MCP stdio).
func (l *RateLimiter) AllowKey(key string) bool {
	return l.getLimiter(key).Allow()
}

// GinMiddleware returns a Gin middleware enforcing this rate limiter.
func (l *RateLimiter) GinMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		clientIP := c.ClientIP()

		if !l.AllowKey(clientIP) {
			l.logger.Warn(c.Request.Context(), "Rate limit exceeded",
				adapters.Field{Key: "client_ip", Value: clientIP},
				adapters.Field{Key: "path", Value: c.Request.URL.Path},
				adapters.Field{Key: "method", Value: c.Request.Method},
			)

			c.Header("X-RateLimit-Limit", fmt.Sprintf("%.0f", l.config.RequestsPerSecond))
			c.Header("X-RateLimit-Burst", fmt.Sprintf("%d", l.config.Burst))
			c.Header("Retry-After", "1")

			c.JSON(http.StatusTooManyRequests, gin.H{
				"error":   "Rate limit exceeded",
				"message": "Too many requests, please try again later",
			})
			c.Abort()
			return
		}

		c.Next()
	}
}

// grpcClientKey derives the rate-limit key for a gRPC request. When per-IP
// limiting is enabled, the peer address host identifies the client; otherwise
// a single global key is used. Falls back to the global key when the peer is
// unavailable (e.g. in-process transports).
func grpcClientKey(ctx context.Context, perIP bool) string {
	if !perIP {
		return "global"
	}
	p, ok := peer.FromContext(ctx)
	if !ok || p.Addr == nil {
		return "global"
	}
	host, _, err := net.SplitHostPort(p.Addr.String())
	if err != nil {
		return p.Addr.String()
	}
	return host
}

// UnaryInterceptor returns a gRPC unary interceptor enforcing this rate limiter.
func (l *RateLimiter) UnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		key := grpcClientKey(ctx, l.config.PerIP)

		if !l.AllowKey(key) {
			l.logger.Warn(ctx, "gRPC rate limit exceeded",
				adapters.Field{Key: "method", Value: info.FullMethod},
				adapters.Field{Key: "client", Value: key},
			)

			return nil, status.Errorf(codes.ResourceExhausted,
				"rate limit exceeded: too many requests")
		}

		return handler(ctx, req)
	}
}

// StreamInterceptor returns a gRPC stream interceptor enforcing this rate limiter.
func (l *RateLimiter) StreamInterceptor() grpc.StreamServerInterceptor {
	return func(
		srv any,
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		ctx := ss.Context()
		key := grpcClientKey(ctx, l.config.PerIP)

		if !l.AllowKey(key) {
			l.logger.Warn(ctx, "gRPC stream rate limit exceeded",
				adapters.Field{Key: "method", Value: info.FullMethod},
				adapters.Field{Key: "client", Value: key},
			)

			return status.Errorf(codes.ResourceExhausted,
				"rate limit exceeded: too many requests")
		}

		return handler(srv, ss)
	}
}

// RateLimitMiddleware creates a Gin middleware for rate limiting HTTP requests.
//
// Deprecated: Use NewRateLimiter and GinMiddleware so the limiter can be
// stopped during server shutdown.
func RateLimitMiddleware(config *RateLimitConfig, logger adapters.Logger) gin.HandlerFunc {
	return NewRateLimiter(config, logger).GinMiddleware()
}

// RateLimitUnaryInterceptor creates a gRPC unary interceptor for rate limiting.
//
// Deprecated: Use NewRateLimiter and UnaryInterceptor so unary and stream
// interceptors share one limiter that can be stopped during server shutdown.
func RateLimitUnaryInterceptor(config *RateLimitConfig, logger adapters.Logger) grpc.UnaryServerInterceptor {
	return NewRateLimiter(config, logger).UnaryInterceptor()
}

// RateLimitStreamInterceptor creates a gRPC stream interceptor for rate limiting.
//
// Deprecated: Use NewRateLimiter and StreamInterceptor so unary and stream
// interceptors share one limiter that can be stopped during server shutdown.
func RateLimitStreamInterceptor(config *RateLimitConfig, logger adapters.Logger) grpc.StreamServerInterceptor {
	return NewRateLimiter(config, logger).StreamInterceptor()
}
