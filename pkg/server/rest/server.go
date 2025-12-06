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
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/audit"
	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/server/middleware"
)

// Server represents the REST API server
type Server struct {
	router     *gin.Engine
	httpServer *http.Server
	handler    *Handler
	config     *ServerConfig
}

// ServerConfig holds server configuration
type ServerConfig struct {
	// Host is the hostname to bind to (default: "0.0.0.0")
	Host string

	// Port is the port to listen on (default: 8080)
	Port int

	// EnableCORS enables CORS middleware
	EnableCORS bool

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

	// TLSConfig is the TLS/mTLS configuration (default: nil = no TLS)
	TLSConfig *adapters.TLSConfig

	// AuditLogger is the audit logger for tracking security events (default: enabled with JSON format)
	AuditLogger audit.AuditLogger

	// EnableAudit enables audit logging (default: true)
	EnableAudit bool
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
		TLSConfig:             nil, // No TLS by default
		AuditLogger:           audit.NewDefaultAuditLogger(),
		EnableAudit:           true,
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
	if config.AuditLogger == nil {
		if config.EnableAudit {
			config.AuditLogger = audit.NewDefaultAuditLogger()
		} else {
			config.AuditLogger = audit.NewNoOpAuditLogger()
		}
	}

	// Set Gin mode
	gin.SetMode(config.Mode)

	// Create router
	router := gin.New()

	// Add recovery middleware (always enabled)
	router.Use(gin.Recovery())

	// Add error handling middleware
	router.Use(ErrorHandlingMiddleware())

	// Middleware order: request ID → rate limit → security headers → CORS → auth → logging → size limit

	// Add request ID middleware if enabled (should be first to track all requests)
	if config.EnableRequestID {
		router.Use(middleware.RequestIDMiddleware())
	}

	// Add rate limiting middleware if enabled
	if config.EnableRateLimit {
		router.Use(middleware.RateLimitMiddleware(config.RateLimitConfig, config.Logger))
	}

	// Add security headers middleware if enabled
	if config.EnableSecurityHeaders {
		router.Use(middleware.SecurityHeadersMiddleware(config.SecurityHeadersConfig))
	}

	// Add CORS middleware if enabled
	if config.EnableCORS {
		router.Use(CORSMiddleware())
	}

	// Add audit middleware if enabled (should be before auth to catch all requests)
	if config.EnableAudit && config.AuditLogger != nil {
		router.Use(audit.AuditMiddleware(config.AuditLogger))
	}

	// Add authentication middleware (always enabled, uses NoOpAuthenticator by default)
	router.Use(AuthenticationMiddleware(config.Authenticator, config.Logger, config.AuditLogger))

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
		return nil, fmt.Errorf("failed to create handler: %w", err)
	}

	// Setup routes
	SetupRoutes(router, handler)

	// Create HTTP server
	addr := fmt.Sprintf("%s:%d", config.Host, config.Port)
	httpServer := &http.Server{
		Addr:         addr,
		Handler:      router,
		ReadTimeout:  config.ReadTimeout,
		WriteTimeout: config.WriteTimeout,
		IdleTimeout:  config.IdleTimeout,
	}

	server := &Server{
		router:     router,
		httpServer: httpServer,
		handler:    handler,
		config:     config,
	}

	return server, nil
}

// Start starts the REST API server
func (s *Server) Start() error {
	// Build TLS config if provided
	if s.config.TLSConfig != nil {
		tlsConfig, err := s.config.TLSConfig.Build()
		if err != nil {
			return err
		}
		s.httpServer.TLSConfig = tlsConfig

		s.config.Logger.Info(context.TODO(), "Starting REST API server with TLS",
			adapters.Field{Key: "address", Value: s.httpServer.Addr},
			adapters.Field{Key: "tls_mode", Value: s.config.TLSConfig.Mode},
		)

		// ListenAndServeTLS requires empty cert/key params when using TLSConfig
		return s.httpServer.ListenAndServeTLS("", "")
	}

	s.config.Logger.Info(context.TODO(), "Starting REST API server",
		adapters.Field{Key: "address", Value: s.httpServer.Addr},
	)
	return s.httpServer.ListenAndServe()
}

// Shutdown gracefully shuts down the server
func (s *Server) Shutdown(ctx context.Context) error {
	s.config.Logger.Info(ctx, "Shutting down REST API server")
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
