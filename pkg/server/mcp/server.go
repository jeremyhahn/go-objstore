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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/objstore"
	"github.com/sourcegraph/jsonrpc2"
)

// contextKey is a custom type for context keys to avoid collisions
type contextKey string

const principalContextKey contextKey = "principal"

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
	ResourcePrefix string

	// Logger is the pluggable logger adapter (default: DefaultLogger)
	Logger adapters.Logger

	// Authenticator is the pluggable authentication adapter for HTTP mode
	// (default: NoOpAuthenticator). Not used for stdio mode.
	Authenticator adapters.Authenticator

	// Authorizer is the pluggable authorization adapter for HTTP mode
	// (default: NoOpAuthorizer = allow-all). Not used for stdio mode.
	Authorizer adapters.Authorizer

	// TLSConfig is the TLS/mTLS configuration for HTTP mode (optional)
	TLSConfig *adapters.TLSConfig

	// Backend is the name of the backend to use when using the facade.
	// If empty, the default backend is used.
	Backend string
}

// Server is the main MCP server
type Server struct {
	config          *ServerConfig
	toolRegistry    *ToolRegistry
	toolExecutor    *ToolExecutor
	resourceManager *ResourceManager
}

// NewServer creates a new MCP server using the ObjstoreFacade.
// The facade must be initialized before calling this function.
func NewServer(config *ServerConfig) (*Server, error) {
	// Verify facade is initialized
	if !objstore.IsInitialized() {
		return nil, objstore.ErrNotInitialized
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

	// Set default authorizer if not provided (for HTTP mode)
	if config.Authorizer == nil {
		config.Authorizer = adapters.NewNoOpAuthorizer()
	}

	// Initialize components
	toolRegistry := NewToolRegistry()
	toolRegistry.RegisterDefaultTools()

	toolExecutor := NewToolExecutor(config.Backend)
	resourceManager := NewResourceManager(config.Backend, config.ResourcePrefix)

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
			if err := server.ServeTLS(listener, "", ""); err != nil && !errors.Is(err, http.ErrServerClosed) {
				errChan <- err
			}
		} else {
			s.config.Logger.Info(ctx, "Starting MCP server in HTTP mode",
				adapters.Field{Key: "address", Value: address},
			)
			if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
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

		// Store principal in context and enrich a request-local logger.
		// Do NOT assign back to s.config.Logger — that would mutate shared
		// server state and cause a data race under concurrent requests.
		ctx := context.WithValue(r.Context(), principalContextKey, principal)
		r = r.WithContext(ctx)

		reqLogger := s.config.Logger.WithFields(
			adapters.Field{Key: "principal_id", Value: principal.ID},
			adapters.Field{Key: "principal_name", Value: principal.Name},
		)

		// Authorize the request. Peek at the JSON-RPC body to derive the
		// MCP method (and tool name for tools/call), then restore the body
		// for the downstream handler.
		action, resource, body := s.deriveMCPActionResource(r)
		r.Body = io.NopCloser(bytes.NewReader(body))
		if err := s.config.Authorizer.Authorize(ctx, principal, action, resource); err != nil {
			reqLogger.Warn(ctx, "MCP HTTP authorization denied",
				adapters.Field{Key: "error", Value: err.Error()},
				adapters.Field{Key: "path", Value: r.URL.Path},
				adapters.Field{Key: "action", Value: action},
			)
			http.Error(w, "Forbidden", http.StatusForbidden)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// mcpToolActions maps an MCP tool name to its required action per the standard
// taxonomy. Tools not present default to admin (deny-safe).
var mcpToolActions = map[string]string{
	"objstore_get":             adapters.ActionRead,
	"objstore_exists":          adapters.ActionRead,
	"objstore_get_metadata":    adapters.ActionRead,
	"objstore_list":            adapters.ActionList,
	"objstore_put":             adapters.ActionWrite,
	"objstore_update_metadata": adapters.ActionWrite,
	"objstore_delete":          adapters.ActionDelete,
}

// deriveMCPActionResource reads the request body and maps the MCP JSON-RPC
// method (and tool name for tools/call) to an (action, resource) pair. It
// returns the consumed body bytes so the caller can restore r.Body. Read-only
// protocol methods (initialize, tools/list, resources/*, ping) map to read.
func (s *Server) deriveMCPActionResource(r *http.Request) (action, resource string, body []byte) {
	body, _ = io.ReadAll(r.Body) // #nosec G104 -- body re-read errors surface downstream as invalid JSON
	_ = r.Body.Close()

	var parsed struct {
		Method string `json:"method"`
		Params struct {
			Name string `json:"name"`
		} `json:"params"`
	}
	_ = json.Unmarshal(body, &parsed) // #nosec G104 -- malformed bodies are rejected by the downstream handler

	if parsed.Method != methodToolsCall {
		// initialize, tools/list, resources/list, resources/read, ping.
		return adapters.ActionRead, "", body
	}

	tool := parsed.Params.Name
	if act, ok := mcpToolActions[tool]; ok {
		return act, tool, body
	}
	// Lifecycle/replication tools and any unmapped tool require admin.
	switch {
	case strings.Contains(tool, "replication"):
		return adapters.ActionAdmin, adapters.ResourceReplication, body
	default:
		return adapters.ActionAdmin, adapters.ResourcePolicy, body
	}
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
