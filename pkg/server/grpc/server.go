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
	"crypto/tls"
	"net"

	objstorepb "github.com/jeremyhahn/go-objstore/api/proto"
	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/audit"
	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/server/middleware"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
)

// Server represents a gRPC server for object storage operations.
type Server struct {
	objstorepb.UnimplementedObjectStoreServer

	storage    common.Storage
	opts       *ServerOptions
	grpcServer *grpc.Server
	listener   net.Listener
	metrics    *MetricsCollector
}

// NewServer creates a new gRPC server instance.
func NewServer(storage common.Storage, options ...ServerOption) (*Server, error) {
	if storage == nil {
		return nil, ErrStorageRequired
	}

	opts := DefaultServerOptions()
	for _, opt := range options {
		opt(opts)
	}

	server := &Server{
		storage: storage,
		opts:    opts,
		metrics: NewMetricsCollector(),
	}

	return server, nil
}

// Start starts the gRPC server.
func (s *Server) Start() error {
	// Create listener
	listener, err := net.Listen("tcp", s.opts.Address)
	if err != nil {
		return err
	}
	s.listener = listener

	// Build server options
	serverOpts := s.buildServerOptions()

	// Create gRPC server
	s.grpcServer = grpc.NewServer(serverOpts...)

	// Register the ObjectStore service
	objstorepb.RegisterObjectStoreServer(s.grpcServer, s)

	// Register health check service
	healthServer := health.NewServer()
	grpc_health_v1.RegisterHealthServer(s.grpcServer, healthServer)
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	healthServer.SetServingStatus("objstore.ObjectStore", grpc_health_v1.HealthCheckResponse_SERVING)

	// Enable reflection if configured
	if s.opts.EnableReflection {
		reflection.Register(s.grpcServer)
		s.opts.Logger.Info(context.TODO(), "gRPC server reflection enabled")
	}

	s.opts.Logger.Info(context.TODO(), "Starting gRPC server",
		adapters.Field{Key: "address", Value: s.opts.Address},
	)

	// Start serving (this blocks)
	if err := s.grpcServer.Serve(listener); err != nil {
		return err
	}

	return nil
}

// Stop gracefully stops the gRPC server.
func (s *Server) Stop() {
	if s.grpcServer != nil {
		s.opts.Logger.Info(context.TODO(), "Gracefully stopping gRPC server")
		s.grpcServer.GracefulStop()
		s.opts.Logger.Info(context.TODO(), "gRPC server stopped")
	}
}

// ForceStop forcefully stops the gRPC server.
func (s *Server) ForceStop() {
	if s.grpcServer != nil {
		s.opts.Logger.Warn(context.TODO(), "Force stopping gRPC server")
		s.grpcServer.Stop()
		s.opts.Logger.Info(context.TODO(), "gRPC server stopped")
	}
}

// GetMetrics returns the current server metrics.
func (s *Server) GetMetrics() map[string]any {
	return s.metrics.GetMetrics()
}

// GetAddress returns the server's listening address.
func (s *Server) GetAddress() string {
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return s.opts.Address
}

// buildServerOptions constructs the gRPC server options based on configuration.
func (s *Server) buildServerOptions() []grpc.ServerOption {
	var opts []grpc.ServerOption

	// Add TLS credentials if configured (prefer AdapterTLSConfig)
	if s.opts.AdapterTLSConfig != nil {
		tlsConfig, err := s.opts.AdapterTLSConfig.Build()
		if err != nil {
			s.opts.Logger.Error(context.TODO(), "Failed to build TLS config",
				adapters.Field{Key: "error", Value: err.Error()},
			)
		} else {
			creds := credentials.NewTLS(tlsConfig)
			opts = append(opts, grpc.Creds(creds))
			s.opts.Logger.Info(context.TODO(), "gRPC TLS enabled",
				adapters.Field{Key: "tls_mode", Value: s.opts.AdapterTLSConfig.Mode},
			)
		}
	} else if s.opts.TLSConfig != nil {
		creds := credentials.NewTLS(s.opts.TLSConfig)
		opts = append(opts, grpc.Creds(creds))
		s.opts.Logger.Info(context.TODO(), "gRPC TLS enabled (legacy config)")
	}

	// Set max concurrent streams
	opts = append(opts, grpc.MaxConcurrentStreams(s.opts.MaxConcurrentStreams))

	// Set max message sizes
	opts = append(opts,
		grpc.MaxRecvMsgSize(s.opts.MaxReceiveMessageSize),
		grpc.MaxSendMsgSize(s.opts.MaxSendMessageSize),
	)

	// Set keepalive parameters
	opts = append(opts, grpc.KeepaliveParams(keepalive.ServerParameters{
		Time:    s.opts.KeepAliveTime,
		Timeout: s.opts.KeepAliveTimeout,
	}))

	// Build interceptor chains
	// Order: recovery → request ID → rate limit → auth → logging → metrics → custom
	unaryInterceptors := []grpc.UnaryServerInterceptor{
		RecoveryUnaryInterceptor(), // Always add recovery first
	}

	streamInterceptors := []grpc.StreamServerInterceptor{
		RecoveryStreamInterceptor(), // Always add recovery first
	}

	// Add request ID interceptors if enabled
	if s.opts.EnableRequestID {
		unaryInterceptors = append(unaryInterceptors, middleware.RequestIDUnaryInterceptor())
		streamInterceptors = append(streamInterceptors, middleware.RequestIDStreamInterceptor())
	}

	// Add rate limiting interceptors if enabled
	if s.opts.EnableRateLimit {
		unaryInterceptors = append(unaryInterceptors, middleware.RateLimitUnaryInterceptor(s.opts.RateLimitConfig, s.opts.Logger))
		streamInterceptors = append(streamInterceptors, middleware.RateLimitStreamInterceptor(s.opts.RateLimitConfig, s.opts.Logger))
	}

	// Add audit interceptors if enabled (should be before auth to catch all requests)
	if s.opts.EnableAudit && s.opts.AuditLogger != nil {
		unaryInterceptors = append(unaryInterceptors, audit.AuditUnaryInterceptor(s.opts.AuditLogger))
		streamInterceptors = append(streamInterceptors, audit.AuditStreamInterceptor(s.opts.AuditLogger))
	}

	// Add authentication interceptors (always enabled, uses NoOpAuthenticator by default)
	unaryInterceptors = append(unaryInterceptors, AuthenticationUnaryInterceptor(s.opts.Authenticator, s.opts.Logger))
	streamInterceptors = append(streamInterceptors, AuthenticationStreamInterceptor(s.opts.Authenticator, s.opts.Logger))

	// Add logging interceptors
	if s.opts.EnableLogging {
		unaryInterceptors = append(unaryInterceptors, LoggingUnaryInterceptor(s.opts.Logger))
		streamInterceptors = append(streamInterceptors, LoggingStreamInterceptor(s.opts.Logger))
	}

	// Add metrics interceptors
	if s.opts.EnableMetrics {
		unaryInterceptors = append(unaryInterceptors, MetricsUnaryInterceptor(s.metrics))
		streamInterceptors = append(streamInterceptors, MetricsStreamInterceptor(s.metrics))
	}

	// Add custom interceptors
	unaryInterceptors = append(unaryInterceptors, s.opts.UnaryInterceptors...)
	streamInterceptors = append(streamInterceptors, s.opts.StreamInterceptors...)

	// Chain all interceptors
	if len(unaryInterceptors) > 0 {
		opts = append(opts, grpc.ChainUnaryInterceptor(unaryInterceptors...))
	}

	if len(streamInterceptors) > 0 {
		opts = append(opts, grpc.ChainStreamInterceptor(streamInterceptors...))
	}

	return opts
}

// WithTLSFromFiles is a helper to create a TLS config from certificate files.
func WithTLSFromFiles(certFile, keyFile string) (ServerOption, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}

	return WithTLS(tlsConfig), nil
}
