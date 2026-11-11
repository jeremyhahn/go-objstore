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
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/audit"
	"github.com/jeremyhahn/go-objstore/pkg/auditlog"
	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/factory"
	"github.com/jeremyhahn/go-objstore/pkg/server/middleware"
)

// Server represents the REST API server
type Server struct {
	router      *gin.Engine
	httpServer  *http.Server
	handler     *Handler
	config      *ServerConfig
	rateLimiter *middleware.RateLimiter

	// serving is closed by Start once httpServer is fully configured and the
	// blocking serve call is about to be made. Closing a channel publishes
	// every write the closing goroutine made beforehand, so this is the
	// happens-before edge a caller needs before reading httpServer — notably
	// httpServer.TLSConfig, which Start assigns.
	//
	// It means "Start has committed to serving", not "the listener is bound",
	// and that is the guarantee callers actually need: http.Server makes the
	// remainder deterministic, because Serve refuses to track a listener once
	// shutdown has begun. A Shutdown issued after this point therefore either
	// stops a running server or makes the imminent Serve return
	// ErrServerClosed — never the silent no-op that racing Shutdown against
	// an unstarted server produces.
	serving     chan struct{}
	servingOnce sync.Once
}

// ServerConfig holds server configuration
type ServerConfig struct {
	// Host is the hostname to bind to (default: "0.0.0.0")
	Host string

	// TrustedProxies is the list of CIDR ranges or IP addresses that the
	// server trusts as upstream proxies when reading X-Forwarded-For /
	// X-Real-IP headers. Defaults to nil (no trusted proxies), meaning the
	// client IP is always the direct peer address. Set this only when the
	// server is known to sit behind a trusted load balancer.
	TrustedProxies []string

	// Port is the port to listen on (default: 8080)
	Port int

	// EnableCORS enables CORS middleware
	EnableCORS bool

	// AllowedOrigins is the list of origins permitted by the CORS middleware.
	// When empty/nil (or set to ["*"]), all origins are allowed without
	// credentials. When set to a specific allowlist, only those origins are
	// echoed back and credentials are permitted. See CORSMiddleware.
	AllowedOrigins []string

	// EnableLogging enables request logging middleware
	EnableLogging bool

	// EnableRateLimit enables rate limiting middleware
	EnableRateLimit bool

	// RateLimitConfig is the rate limiting configuration
	RateLimitConfig *middleware.RateLimitConfig

	// EnableSecurityHeaders enables security headers middleware
	EnableSecurityHeaders bool

	// SecurityHeadersConfig is the security headers configuration
	SecurityHeadersConfig *middleware.SecurityHeadersConfig

	// EnableRequestID enables request ID middleware
	EnableRequestID bool

	// MaxRequestSize is the maximum request body size in bytes (default: 100MB)
	MaxRequestSize int64

	// ReadTimeout is the maximum duration for reading the entire request
	ReadTimeout time.Duration

	// WriteTimeout is the maximum duration before timing out writes of the response
	WriteTimeout time.Duration

	// IdleTimeout is the maximum amount of time to wait for the next request
	IdleTimeout time.Duration

	// Mode sets the Gin mode: "debug", "release", or "test" (default: "release")
	Mode string

	// Logger is the pluggable logger adapter (default: DefaultLogger)
	Logger adapters.Logger

	// Authenticator is the pluggable authentication adapter (default: NoOpAuthenticator)
	Authenticator adapters.Authenticator

	// Authorizer is the pluggable authorization adapter (default: NoOpAuthorizer = allow-all)
	Authorizer adapters.Authorizer

	// TLSConfig is the TLS/mTLS configuration (default: nil = no TLS)
	TLSConfig *adapters.TLSConfig

	// AuditLogger is the audit logger for tracking security events (default: enabled with JSON format)
	AuditLogger auditlog.AuditLogger

	// EnableAudit enables audit logging (default: true)
	EnableAudit bool

	// MetricsPublic exempts the /metrics endpoint from authorization when true.
	// The default (false) requires Prometheus scrapers to present credentials
	// accepted by the configured authorizer.
	MetricsPublic bool

	// AllowClientLocalArchiveDestinations lets an archive call or a lifecycle
	// policy name a local (filesystem path) archiver. Off by default: the
	// client would choose a directory the service user writes into.
	AllowClientLocalArchiveDestinations bool
}

// DefaultServerConfig returns a ServerConfig with sensible defaults
func DefaultServerConfig() *ServerConfig {
	return &ServerConfig{
		Host:                  "0.0.0.0",
		Port:                  8080,
		EnableCORS:            true,
		EnableLogging:         true,
		EnableRateLimit:       false, // Disabled by default
		RateLimitConfig:       middleware.DefaultRateLimitConfig(),
		EnableSecurityHeaders: true,
		SecurityHeadersConfig: middleware.DefaultSecurityHeadersConfig(),
		EnableRequestID:       true,
		MaxRequestSize:        100 * 1024 * 1024, // 100MB
		ReadTimeout:           60 * time.Second,
		WriteTimeout:          60 * time.Second,
		IdleTimeout:           120 * time.Second,
		Mode:                  gin.ReleaseMode,
		Logger:                adapters.NewDefaultLogger(),
		Authenticator:         adapters.NewNoOpAuthenticator(),
		Authorizer:            adapters.NewNoOpAuthorizer(),
		TLSConfig:             nil, // No TLS by default
		AuditLogger:           audit.NewDefaultAuditLogger(),
		EnableAudit:           true,
		MetricsPublic:         false, // /metrics requires authorization by default
	}
}

// NewServer creates a new REST API server
func NewServer(storage common.Storage, config *ServerConfig) (*Server, error) {
	if config == nil {
		config = DefaultServerConfig()
	}

	// Set defaults for nil fields
	if config.Logger == nil {
		config.Logger = adapters.NewDefaultLogger()
	}
	if config.Authenticator == nil {
		config.Authenticator = adapters.NewNoOpAuthenticator()
	}
	if config.Authorizer == nil {
		config.Authorizer = adapters.NewNoOpAuthorizer()
	}
	if config.AuditLogger == nil {
		if config.EnableAudit {
			config.AuditLogger = audit.NewDefaultAuditLogger()
		} else {
			config.AuditLogger = auditlog.NewNoOpAuditLogger()
		}
	}

	// Set Gin mode
	gin.SetMode(config.Mode)

	// Create router
	router := gin.New()

	// Configure trusted proxies. Default to no trusted proxies so that
	// X-Forwarded-For is not blindly trusted. This must be called before any
	// middleware that reads ClientIP().
	if err := router.SetTrustedProxies(config.TrustedProxies); err != nil {
		return nil, &TrustedProxiesError{Proxies: config.TrustedProxies, Err: err}
	}

	// Add recovery middleware (always enabled)
	router.Use(gin.Recovery())

	// Add error handling middleware
	router.Use(ErrorHandlingMiddleware())

	// Record request metrics for every request (before auth, so rejected
	// requests are counted too). Exposed at GET /metrics.
	router.Use(MetricsMiddleware())

	// Middleware order: request ID → rate limit → security headers → CORS → auth → logging → size limit

	// Add request ID middleware if enabled (should be first to track all requests)
	if config.EnableRequestID {
		router.Use(middleware.RequestIDMiddleware())
	}

	// Add rate limiting middleware if enabled. The limiter is stored on the
	// server so its eviction goroutine is stopped during Shutdown.
	var rateLimiter *middleware.RateLimiter
	if config.EnableRateLimit {
		rateLimiter = middleware.NewRateLimiter(config.RateLimitConfig, config.Logger)
		router.Use(rateLimiter.GinMiddleware())
	}

	// Add security headers middleware if enabled
	if config.EnableSecurityHeaders {
		router.Use(middleware.SecurityHeadersMiddleware(config.SecurityHeadersConfig))
	}

	// Add CORS middleware if enabled
	if config.EnableCORS {
		router.Use(CORSMiddleware(config.AllowedOrigins))
	}

	// Add audit middleware if enabled (should be before auth to catch all requests)
	if config.EnableAudit && config.AuditLogger != nil {
		router.Use(audit.AuditMiddleware(config.AuditLogger))
	}

	// Add authentication middleware (always enabled, uses NoOpAuthenticator by default)
	router.Use(AuthenticationMiddleware(config.Authenticator, config.Logger, config.AuditLogger, config.MetricsPublic))

	// Add authorization middleware (always enabled, uses NoOpAuthorizer by default).
	// Runs after authentication so the principal is available. Health and swagger
	// routes are registered without these middlewares applied selectively below;
	// since this is a global middleware, the health route still passes through the
	// allow-all default. AuthorizationMiddleware only denies when a restrictive
	// authorizer is configured.
	router.Use(AuthorizationMiddleware(config.Authorizer, config.Logger, config.AuditLogger, config.MetricsPublic))

	// Add logging middleware if enabled
	if config.EnableLogging {
		router.Use(LoggingMiddleware(config.Logger))
	}

	// Add request size limit middleware
	if config.MaxRequestSize > 0 {
		router.Use(RequestSizeLimitMiddleware(config.MaxRequestSize))
	}

	// Create handler (uses facade with default backend)
	handler, err := NewHandler("")
	if err != nil {
		return nil, &HandlerInitError{Err: err}
	}
	handler.wireOpts = factory.WireOptions{AllowLocalDestinations: config.AllowClientLocalArchiveDestinations}

	// Setup routes
	SetupRoutes(router, handler)

	// Create HTTP server. JoinHostPort brackets an IPv6 host ("::1" →
	// "[::1]:8080"), which a bare "%s:%d" would leave unparseable.
	addr := net.JoinHostPort(config.Host, strconv.Itoa(config.Port))
	httpServer := &http.Server{
		Addr:              addr,
		Handler:           router,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       config.ReadTimeout,
		WriteTimeout:      config.WriteTimeout,
		IdleTimeout:       config.IdleTimeout,
		MaxHeaderBytes:    1 << 20, // 1 MB
	}

	server := &Server{
		router:      router,
		httpServer:  httpServer,
		handler:     handler,
		config:      config,
		rateLimiter: rateLimiter,
		serving:     make(chan struct{}),
	}

	return server, nil
}

// beginServing publishes the server's start-up state to anyone waiting on
// serving. sync.Once is not defensive clutter: a second Start must fail on its
// own terms — "address already in use", or ErrServerClosed after a Shutdown —
// and closing an already-closed channel would turn that ordinary error into a
// panic.
func (s *Server) beginServing() {
	s.servingOnce.Do(func() { close(s.serving) })
}

// Start starts the REST API server
func (s *Server) Start() error {
	// Build TLS config if provided. Build returns a nil *tls.Config when the
	// adapter config is disabled (the zero value); serve plaintext in that case.
	if s.config.TLSConfig != nil {
		tlsConfig, err := s.config.TLSConfig.Build()
		if err != nil {
			return err
		}
		if tlsConfig != nil {
			// Production floor: never serve TLS below 1.2 regardless of how the
			// adapter config was constructed.
			if tlsConfig.MinVersion < tls.VersionTLS12 {
				tlsConfig.MinVersion = tls.VersionTLS12
			}
			s.httpServer.TLSConfig = tlsConfig

			s.config.Logger.Info(context.Background(), "Starting REST API server with TLS",
				adapters.Field{Key: "address", Value: s.httpServer.Addr},
				adapters.Field{Key: "tls_mode", Value: s.config.TLSConfig.Mode},
			)

			// ListenAndServeTLS requires empty cert/key params when using TLSConfig
			s.beginServing()
			return s.httpServer.ListenAndServeTLS("", "")
		}
	}

	s.config.Logger.Info(context.Background(), "Starting REST API server",
		adapters.Field{Key: "address", Value: s.httpServer.Addr},
	)
	s.beginServing()
	return s.httpServer.ListenAndServe()
}

// Shutdown gracefully shuts down the server
func (s *Server) Shutdown(ctx context.Context) error {
	s.config.Logger.Info(ctx, "Shutting down REST API server")
	if s.rateLimiter != nil {
		s.rateLimiter.Stop()
	}
	return s.httpServer.Shutdown(ctx)
}

// Router returns the underlying Gin router (useful for testing)
func (s *Server) Router() *gin.Engine {
	return s.router
}

// Handler returns the HTTP handler
func (s *Server) Handler() *Handler {
	return s.handler
}

// Address returns the server address
func (s *Server) Address() string {
	return s.httpServer.Addr
}
