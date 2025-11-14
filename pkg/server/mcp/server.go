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

package mcp

import (
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/sourcegraph/jsonrpc2"
)

// ServerMode defines the transport mode for the MCP server
type ServerMode string

const (
	// ModeStdio runs the server over stdin/stdout
	ModeStdio ServerMode = "stdio"
	// ModeHTTP runs the server over HTTP
	ModeHTTP ServerMode = "http"
)

// ServerConfig holds the server configuration
type ServerConfig struct {
	Mode           ServerMode
	HTTPAddress    string
	Storage        common.Storage
	ResourcePrefix string

	// Logger is the pluggable logger adapter (default: DefaultLogger)
	Logger adapters.Logger

	// Authenticator is the pluggable authentication adapter for HTTP mode
	// (default: NoOpAuthenticator). Not used for stdio mode.
	Authenticator adapters.Authenticator

	// TLSConfig is the TLS/mTLS configuration for HTTP mode (optional)
	TLSConfig *adapters.TLSConfig
}

// Server is the main MCP server
type Server struct {
	config          *ServerConfig
	toolRegistry    *ToolRegistry
	toolExecutor    *ToolExecutor
	resourceManager *ResourceManager
}

// NewServer creates a new MCP server
func NewServer(config *ServerConfig) (*Server, error) {
	if config.Storage == nil {
		return nil, ErrStorageRequired
	}

	// Set default resource prefix if not provided
	if config.ResourcePrefix == "" {
		config.ResourcePrefix = ""
	}

	// Set default logger if not provided
	if config.Logger == nil {
		config.Logger = adapters.NewDefaultLogger()
	}

	// Set default authenticator if not provided (for HTTP mode)
	if config.Authenticator == nil {
		config.Authenticator = adapters.NewNoOpAuthenticator()
	}

	// Initialize components
	toolRegistry := NewToolRegistry()
	toolRegistry.RegisterDefaultTools()

	toolExecutor := NewToolExecutor(config.Storage)
	resourceManager := NewResourceManager(config.Storage, config.ResourcePrefix)

	return &Server{
		config:          config,
		toolRegistry:    toolRegistry,
		toolExecutor:    toolExecutor,
		resourceManager: resourceManager,
	}, nil
}

// Start starts the MCP server
func (s *Server) Start(ctx context.Context) error {
	switch s.config.Mode {
	case ModeStdio:
		return s.startStdio(ctx)
	case ModeHTTP:
		return s.startHTTP(ctx)
	default:
		return ErrUnknownServerMode
	}
}

// startStdio starts the server in stdio mode
func (s *Server) startStdio(ctx context.Context) error {
	s.config.Logger.Info(ctx, "Starting MCP server in stdio mode")

	handler := NewRPCHandler(s)

	// Create a stream for stdin/stdout
	stream := jsonrpc2.NewBufferedStream(&stdioReadWriteCloser{
		reader: os.Stdin,
		writer: os.Stdout,
	}, jsonrpc2.VSCodeObjectCodec{})

	conn := jsonrpc2.NewConn(ctx, stream, jsonrpc2.HandlerWithError(handler.Handle))

	// Wait for context cancellation or connection close
	<-conn.DisconnectNotify()
	s.config.Logger.Info(ctx, "MCP server (stdio mode) stopped")
	return nil
}

// startHTTP starts the server in HTTP mode
func (s *Server) startHTTP(ctx context.Context) error {
	address := s.config.HTTPAddress
	if address == "" {
		address = ":8080"
	}

	// Create HTTP mux for routing
	mux := http.NewServeMux()

	// Health endpoint (no authentication required)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK")) // #nosec G104 -- Error writing to response is non-critical for health check
	})

	// JSON-RPC handler with authentication
	jsonrpcHandler := NewHTTPHandler(s)
	mux.Handle("/", s.authenticationMiddleware(jsonrpcHandler))

	server := &http.Server{
		Addr:              address,
		Handler:           mux,
		ReadHeaderTimeout: 30 * time.Second, // Prevent slowloris attacks
		ReadTimeout:       60 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	// Configure TLS if provided
	if s.config.TLSConfig != nil {
		tlsConfig, err := s.config.TLSConfig.Build()
		if err != nil {
			return err
		}
		server.TLSConfig = tlsConfig
	}

	// Start server in a goroutine
	errChan := make(chan error, 1)
	go func() {
		listener, err := net.Listen("tcp", address)
		if err != nil {
			errChan <- err
			return
		}

		if s.config.TLSConfig != nil {
			s.config.Logger.Info(ctx, "Starting MCP server in HTTP mode with TLS",
				adapters.Field{Key: "address", Value: address},
			)
			if err := server.ServeTLS(listener, "", ""); err != nil && err != http.ErrServerClosed {
				errChan <- err
			}
		} else {
			s.config.Logger.Info(ctx, "Starting MCP server in HTTP mode",
				adapters.Field{Key: "address", Value: address},
			)
			if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
				errChan <- err
			}
		}
	}()

	// Wait for context cancellation or error
	select {
	case <-ctx.Done():
		s.config.Logger.Info(ctx, "Stopping MCP server (HTTP mode)")
		return server.Shutdown(context.Background())
	case err := <-errChan:
		return err
	}
}

// authenticationMiddleware wraps an HTTP handler with authentication
func (s *Server) authenticationMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Authenticate the request
		principal, err := s.config.Authenticator.AuthenticateHTTP(r.Context(), r)
		if err != nil {
			s.config.Logger.Warn(r.Context(), "MCP HTTP authentication failed",
				adapters.Field{Key: "error", Value: err.Error()},
				adapters.Field{Key: "path", Value: r.URL.Path},
				adapters.Field{Key: "method", Value: r.Method},
			)
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		// Store principal in context
		ctx := context.WithValue(r.Context(), "principal", principal)
		r = r.WithContext(ctx)

		// Add principal info to logger
		s.config.Logger = s.config.Logger.WithFields(
			adapters.Field{Key: "principal_id", Value: principal.ID},
			adapters.Field{Key: "principal_name", Value: principal.Name},
		)

		next.ServeHTTP(w, r)
	})
}

// ListTools returns all available tools
func (s *Server) ListTools() []Tool {
	return s.toolRegistry.ListTools()
}

// CallTool executes a tool
func (s *Server) CallTool(ctx context.Context, name string, args map[string]any) (string, error) {
	// Verify tool exists
	if _, ok := s.toolRegistry.GetTool(name); !ok {
		return "", ErrUnknownTool
	}

	return s.toolExecutor.Execute(ctx, name, args)
}

// ListResources returns available resources
func (s *Server) ListResources(ctx context.Context, cursor string) ([]Resource, error) {
	return s.resourceManager.ListResources(ctx, cursor)
}

// ReadResource reads a resource's content
func (s *Server) ReadResource(ctx context.Context, uri string) (string, string, error) {
	return s.resourceManager.ReadResource(ctx, uri)
}

// stdioReadWriteCloser wraps stdin/stdout for use with jsonrpc2
type stdioReadWriteCloser struct {
	reader io.Reader
	writer io.Writer
}

func (rw *stdioReadWriteCloser) Read(p []byte) (int, error) {
	return rw.reader.Read(p)
}

func (rw *stdioReadWriteCloser) Write(p []byte) (int, error) {
	return rw.writer.Write(p)
}

func (rw *stdioReadWriteCloser) Close() error {
	// Don't actually close stdin/stdout
	return nil
}
