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

package quic

import (
	"context"
	"net"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/audit"
	"github.com/jeremyhahn/go-objstore/pkg/server/middleware"
	"github.com/quic-go/quic-go/http3"
)

// Server represents a QUIC/HTTP3 server for object storage.
type Server struct {
	opts        *Options
	handler     *Handler
	server      *http3.Server
	rateLimiter *middleware.RateLimiter
	mu          sync.RWMutex
	running     atomic.Bool
	addr        net.Addr
}

// New creates a new QUIC server with the given options.
// The ObjstoreFacade must be initialized before calling this function.
func New(opts *Options) (*Server, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	handler, err := NewHandler(
		opts.Backend,
		opts.MaxRequestBodySize,
		opts.ReadTimeout,
		opts.WriteTimeout,
		opts.Logger,
		opts.Authenticator,
		opts.Authorizer,
		opts.AllowedOrigins,
	)
	if err != nil {
		return nil, err
	}

	// Wrap the handler with the shared middleware stack. Order (outermost
	// first): request ID → rate limit → audit → handler, matching the REST
	// server's ordering.
	var h http.Handler = handler
	var rateLimiter *middleware.RateLimiter
	if opts.EnableAudit && opts.AuditLogger != nil {
		h = audit.AuditHTTPMiddleware(opts.AuditLogger)(h)
	}
	if opts.EnableRateLimit {
		rateLimiter = middleware.NewRateLimiter(opts.RateLimitConfig, opts.Logger)
		h = rateLimiter.HTTPMiddleware(h)
	}
	if opts.EnableRequestID {
		h = middleware.RequestIDHTTPMiddleware(h)
	}

	server := &http3.Server{
		Addr:       opts.Addr,
		TLSConfig:  opts.TLSConfig,
		QUICConfig: opts.QUICConfig,
		Handler:    h,
	}

	return &Server{
		opts:        opts,
		handler:     handler,
		server:      server,
		rateLimiter: rateLimiter,
	}, nil
}

// Start starts the QUIC server.
func (s *Server) Start() error {
	if s.running.Load() {
		return ErrServerAlreadyStarted
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Create UDP listener
	addr, err := net.ResolveUDPAddr("udp", s.opts.Addr)
	if err != nil {
		return err
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return err
	}

	// Store the actual listening address
	s.addr = conn.LocalAddr()

	// Mark server as running
	s.running.Store(true)

	s.opts.Logger.Info(context.Background(), "Starting QUIC/HTTP3 server",
		adapters.Field{Key: "address", Value: s.addr.String()},
		adapters.Field{Key: "tls", Value: "required"},
	)

	// Start serving in a goroutine
	go func() {
		err := s.server.Serve(conn)
		if err != nil && s.running.Load() {
			// Only log error if server is supposed to be running
			s.opts.Logger.Error(context.Background(), "QUIC server error",
				adapters.Field{Key: fieldError, Value: err.Error()},
			)
		}
		s.running.Store(false)
	}()

	return nil
}

// Stop gracefully stops the QUIC server. It calls Shutdown(ctx) to allow
// in-flight requests to drain; if the context expires before the drain
// completes, it falls back to Close() for an immediate stop.
func (s *Server) Stop(ctx context.Context) error {
	if !s.running.Load() {
		return ErrServerNotStarted
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.opts.Logger.Info(ctx, "Stopping QUIC/HTTP3 server")

	if s.rateLimiter != nil {
		s.rateLimiter.Stop()
	}

	err := s.server.Shutdown(ctx)
	if err != nil {
		// Context expired or Shutdown failed; force-close.
		_ = s.server.Close()
		if ctx.Err() != nil {
			s.opts.Logger.Warn(ctx, "QUIC/HTTP3 graceful shutdown timed out; forced close")
		} else {
			return err
		}
	}

	s.opts.Logger.Info(ctx, "QUIC/HTTP3 server stopped")

	s.running.Store(false)
	return nil
}

// Addr returns the actual address the server is listening on.
// Returns nil if the server is not running.
func (s *Server) Addr() net.Addr {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.addr
}

// IsRunning returns true if the server is currently running.
func (s *Server) IsRunning() bool {
	return s.running.Load()
}

// Handler returns the HTTP handler used by the server.
// This is useful for testing.
func (s *Server) Handler() *Handler {
	return s.handler
}

// Options returns the server options.
func (s *Server) Options() *Options {
	return s.opts
}

// ListenAndServe is a convenience method that creates and starts a server.
func ListenAndServe(opts *Options) error {
	server, err := New(opts)
	if err != nil {
		return err
	}

	if err := server.Start(); err != nil {
		return err
	}

	// Block indefinitely
	select {}
}

// Serve starts serving with custom UDP connection.
// This is useful when you need more control over the UDP listener.
func (s *Server) Serve(conn net.PacketConn) error {
	if s.running.Load() {
		return ErrServerAlreadyStarted
	}

	s.mu.Lock()
	s.running.Store(true)
	s.addr = conn.LocalAddr()
	s.mu.Unlock()

	err := s.server.Serve(conn)
	s.running.Store(false)
	return err
}

// ServeConn serves a single QUIC connection.
// This is useful for testing or custom connection handling.
func (s *Server) ServeConn(conn net.PacketConn) error {
	return s.server.Serve(conn)
}
