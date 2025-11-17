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
	"crypto/tls"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/audit"
	"github.com/jeremyhahn/go-objstore/pkg/server/middleware"
	"google.golang.org/grpc"
)

// ServerOptions contains configuration options for the gRPC server.
type ServerOptions struct {
	// Address is the server address in the format "host:port"
	Address string

	// TLSConfig is the TLS configuration for secure connections
	TLSConfig *tls.Config

	// MaxConcurrentStreams is the maximum number of concurrent streams per connection
	MaxConcurrentStreams uint32

	// MaxReceiveMessageSize is the maximum message size in bytes the server can receive
	MaxReceiveMessageSize int

	// MaxSendMessageSize is the maximum message size in bytes the server can send
	MaxSendMessageSize int

	// ConnectionTimeout is the timeout for establishing connections
	ConnectionTimeout time.Duration

	// KeepAliveTime is the duration after which a keepalive ping is sent
	KeepAliveTime time.Duration

	// KeepAliveTimeout is the duration the server waits for keepalive ping ack
	KeepAliveTimeout time.Duration

	// EnableReflection enables gRPC server reflection for debugging
	EnableReflection bool

	// EnableHealthCheck enables the health check service
	EnableHealthCheck bool

	// EnableMetrics enables metrics collection via interceptors
	EnableMetrics bool

	// EnableLogging enables request/response logging via interceptors
	EnableLogging bool

	// EnableRateLimit enables rate limiting via interceptors
	EnableRateLimit bool

	// RateLimitConfig is the rate limiting configuration
	RateLimitConfig *middleware.RateLimitConfig

	// EnableRequestID enables request ID tracking via interceptors
	EnableRequestID bool

	// UnaryInterceptors is a list of additional unary interceptors to apply
	UnaryInterceptors []grpc.UnaryServerInterceptor

	// StreamInterceptors is a list of additional stream interceptors to apply
	StreamInterceptors []grpc.StreamServerInterceptor

	// ChunkSize is the size of data chunks for streaming operations (default: 64KB)
	ChunkSize int

	// Logger is the pluggable logger adapter (default: DefaultLogger)
	Logger adapters.Logger

	// Authenticator is the pluggable authentication adapter (default: NoOpAuthenticator)
	Authenticator adapters.Authenticator

	// AdapterTLSConfig is the TLS/mTLS configuration using the adapter (preferred over TLSConfig)
	AdapterTLSConfig *adapters.TLSConfig

	// AuditLogger is the audit logger for tracking security events (default: enabled with JSON format)
	AuditLogger audit.AuditLogger

	// EnableAudit enables audit logging (default: true)
	EnableAudit bool
}

// DefaultServerOptions returns the default server options.
func DefaultServerOptions() *ServerOptions {
	return &ServerOptions{
		Address:               ":50051",
		MaxConcurrentStreams:  100,
		MaxReceiveMessageSize: 10 * 1024 * 1024, // 10MB
		MaxSendMessageSize:    10 * 1024 * 1024, // 10MB
		ConnectionTimeout:     30 * time.Second,
		KeepAliveTime:         2 * time.Hour,
		KeepAliveTimeout:      20 * time.Second,
		EnableReflection:      false,
		EnableHealthCheck:     true,
		EnableMetrics:         true,
		EnableLogging:         true,
		EnableRateLimit:       false, // Disabled by default
		RateLimitConfig:       middleware.DefaultRateLimitConfig(),
		EnableRequestID:       true,
		ChunkSize:             64 * 1024, // 64KB
		UnaryInterceptors:     []grpc.UnaryServerInterceptor{},
		StreamInterceptors:    []grpc.StreamServerInterceptor{},
		Logger:                adapters.NewDefaultLogger(),
		Authenticator:         adapters.NewNoOpAuthenticator(),
		AdapterTLSConfig:      nil, // No TLS by default
		AuditLogger:           audit.NewDefaultAuditLogger(),
		EnableAudit:           true,
	}
}

// ServerOption is a function that modifies ServerOptions.
type ServerOption func(*ServerOptions)

// WithAddress sets the server address.
func WithAddress(addr string) ServerOption {
	return func(o *ServerOptions) {
		o.Address = addr
	}
}

// WithTLS enables TLS with the given configuration.
func WithTLS(config *tls.Config) ServerOption {
	return func(o *ServerOptions) {
		o.TLSConfig = config
	}
}

// WithMaxConcurrentStreams sets the maximum number of concurrent streams.
func WithMaxConcurrentStreams(max uint32) ServerOption {
	return func(o *ServerOptions) {
		o.MaxConcurrentStreams = max
	}
}

// WithMaxMessageSize sets both max receive and send message sizes.
func WithMaxMessageSize(size int) ServerOption {
	return func(o *ServerOptions) {
		o.MaxReceiveMessageSize = size
		o.MaxSendMessageSize = size
	}
}

// WithConnectionTimeout sets the connection timeout.
func WithConnectionTimeout(timeout time.Duration) ServerOption {
	return func(o *ServerOptions) {
		o.ConnectionTimeout = timeout
	}
}

// WithKeepAlive sets the keepalive parameters.
func WithKeepAlive(time, timeout time.Duration) ServerOption {
	return func(o *ServerOptions) {
		o.KeepAliveTime = time
		o.KeepAliveTimeout = timeout
	}
}

// WithReflection enables or disables server reflection.
func WithReflection(enable bool) ServerOption {
	return func(o *ServerOptions) {
		o.EnableReflection = enable
	}
}

// WithHealthCheck enables or disables the health check service.
func WithHealthCheck(enable bool) ServerOption {
	return func(o *ServerOptions) {
		o.EnableHealthCheck = enable
	}
}

// WithMetrics enables or disables metrics collection.
func WithMetrics(enable bool) ServerOption {
	return func(o *ServerOptions) {
		o.EnableMetrics = enable
	}
}

// WithLogging enables or disables request/response logging.
func WithLogging(enable bool) ServerOption {
	return func(o *ServerOptions) {
		o.EnableLogging = enable
	}
}

// WithUnaryInterceptor adds a unary interceptor.
func WithUnaryInterceptor(interceptor grpc.UnaryServerInterceptor) ServerOption {
	return func(o *ServerOptions) {
		o.UnaryInterceptors = append(o.UnaryInterceptors, interceptor)
	}
}

// WithStreamInterceptor adds a stream interceptor.
func WithStreamInterceptor(interceptor grpc.StreamServerInterceptor) ServerOption {
	return func(o *ServerOptions) {
		o.StreamInterceptors = append(o.StreamInterceptors, interceptor)
	}
}

// WithChunkSize sets the chunk size for streaming operations.
func WithChunkSize(size int) ServerOption {
	return func(o *ServerOptions) {
		o.ChunkSize = size
	}
}

// WithLogger sets the logger adapter.
func WithLogger(logger adapters.Logger) ServerOption {
	return func(o *ServerOptions) {
		o.Logger = logger
	}
}

// WithAuthenticator sets the authentication adapter.
func WithAuthenticator(auth adapters.Authenticator) ServerOption {
	return func(o *ServerOptions) {
		o.Authenticator = auth
	}
}

// WithAdapterTLS sets the TLS configuration using the adapter.
func WithAdapterTLS(config *adapters.TLSConfig) ServerOption {
	return func(o *ServerOptions) {
		o.AdapterTLSConfig = config
	}
}

// WithRateLimit enables or disables rate limiting.
func WithRateLimit(enable bool, config *middleware.RateLimitConfig) ServerOption {
	return func(o *ServerOptions) {
		o.EnableRateLimit = enable
		if config != nil {
			o.RateLimitConfig = config
		}
	}
}

// WithRequestID enables or disables request ID tracking.
func WithRequestID(enable bool) ServerOption {
	return func(o *ServerOptions) {
		o.EnableRequestID = enable
	}
}

// WithAuditLogger sets the audit logger.
func WithAuditLogger(logger audit.AuditLogger) ServerOption {
	return func(o *ServerOptions) {
		o.AuditLogger = logger
	}
}

// WithAudit enables or disables audit logging.
func WithAudit(enable bool) ServerOption {
	return func(o *ServerOptions) {
		o.EnableAudit = enable
	}
}
