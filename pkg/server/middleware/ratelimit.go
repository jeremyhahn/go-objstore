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
	"net/http"
	"sync"

	"github.com/gin-gonic/gin"
	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
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

// rateLimiter manages rate limiting state
type rateLimiter struct {
	config  *RateLimitConfig
	global  *rate.Limiter
	clients map[string]*rate.Limiter
	mu      sync.RWMutex
	logger  adapters.Logger
}

// newRateLimiter creates a new rate limiter
func newRateLimiter(config *RateLimitConfig, logger adapters.Logger) *rateLimiter {
	if config == nil {
		config = DefaultRateLimitConfig()
	}

	rl := &rateLimiter{
		config:  config,
		clients: make(map[string]*rate.Limiter),
		logger:  logger,
	}

	if !config.PerIP {
		rl.global = rate.NewLimiter(rate.Limit(config.RequestsPerSecond), config.Burst)
	}

	return rl
}

// getLimiter returns the appropriate rate limiter for the client
func (rl *rateLimiter) getLimiter(clientIP string) *rate.Limiter {
	if !rl.config.PerIP {
		return rl.global
	}

	rl.mu.RLock()
	limiter, exists := rl.clients[clientIP]
	rl.mu.RUnlock()

	if exists {
		return limiter
	}

	// Create new limiter for this client
	rl.mu.Lock()
	defer rl.mu.Unlock()

	// Double-check after acquiring write lock
	if limiter, exists := rl.clients[clientIP]; exists {
		return limiter
	}

	limiter = rate.NewLimiter(rate.Limit(rl.config.RequestsPerSecond), rl.config.Burst)
	rl.clients[clientIP] = limiter

	return limiter
}

// RateLimitMiddleware creates a Gin middleware for rate limiting HTTP requests
func RateLimitMiddleware(config *RateLimitConfig, logger adapters.Logger) gin.HandlerFunc {
	if logger == nil {
		logger = adapters.NewDefaultLogger()
	}

	limiter := newRateLimiter(config, logger)

	return func(c *gin.Context) {
		clientIP := c.ClientIP()
		rl := limiter.getLimiter(clientIP)

		if !rl.Allow() {
			logger.Warn(c.Request.Context(), "Rate limit exceeded",
				adapters.Field{Key: "client_ip", Value: clientIP},
				adapters.Field{Key: "path", Value: c.Request.URL.Path},
				adapters.Field{Key: "method", Value: c.Request.Method},
			)

			c.Header("X-RateLimit-Limit", fmt.Sprintf("%.0f", limiter.config.RequestsPerSecond))
			c.Header("X-RateLimit-Burst", fmt.Sprintf("%d", limiter.config.Burst))
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

// RateLimitUnaryInterceptor creates a gRPC unary interceptor for rate limiting
func RateLimitUnaryInterceptor(config *RateLimitConfig, logger adapters.Logger) grpc.UnaryServerInterceptor {
	if logger == nil {
		logger = adapters.NewDefaultLogger()
	}

	limiter := newRateLimiter(config, logger)

	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		// For gRPC, we use a global rate limit by default
		// In production, you could extract client info from peer or metadata
		clientIP := "global"
		rl := limiter.getLimiter(clientIP)

		if !rl.Allow() {
			logger.Warn(ctx, "gRPC rate limit exceeded",
				adapters.Field{Key: "method", Value: info.FullMethod},
			)

			return nil, status.Errorf(codes.ResourceExhausted,
				"rate limit exceeded: too many requests")
		}

		return handler(ctx, req)
	}
}

// RateLimitStreamInterceptor creates a gRPC stream interceptor for rate limiting
func RateLimitStreamInterceptor(config *RateLimitConfig, logger adapters.Logger) grpc.StreamServerInterceptor {
	if logger == nil {
		logger = adapters.NewDefaultLogger()
	}

	limiter := newRateLimiter(config, logger)

	return func(
		srv any,
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		ctx := ss.Context()

		// For gRPC, we use a global rate limit by default
		clientIP := "global"
		rl := limiter.getLimiter(clientIP)

		if !rl.Allow() {
			logger.Warn(ctx, "gRPC stream rate limit exceeded",
				adapters.Field{Key: "method", Value: info.FullMethod},
			)

			return status.Errorf(codes.ResourceExhausted,
				"rate limit exceeded: too many requests")
		}

		return handler(srv, ss)
	}
}
