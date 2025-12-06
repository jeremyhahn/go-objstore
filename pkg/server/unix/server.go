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

package unix

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"os"
	"sync"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/objstore"
)

// ServerConfig holds Unix socket server configuration
type ServerConfig struct {
	// SocketPath is the path to the Unix socket file
	SocketPath string

	// SocketPermissions is the file mode for the socket (default: 0660)
	SocketPermissions os.FileMode

	// Backend is the name of the backend to use (empty = default backend)
	Backend string

	// Logger is the pluggable logger adapter
	Logger adapters.Logger
}

// DefaultConfig returns default server configuration
func DefaultConfig() *ServerConfig {
	return &ServerConfig{
		SocketPath:        "/var/run/objstore.sock",
		SocketPermissions: 0660,
		Logger:            adapters.NewDefaultLogger(),
	}
}

// Server represents the Unix socket server
type Server struct {
	config   *ServerConfig
	listener net.Listener
	handler  *Handler
	mu       sync.Mutex
	closed   bool
	wg       sync.WaitGroup
}

// NewServer creates a new Unix socket server
// The facade must be initialized before calling this function
func NewServer(config *ServerConfig) (*Server, error) {
	if !objstore.IsInitialized() {
		return nil, ErrNotInitialized
	}

	if config == nil {
		config = DefaultConfig()
	}

	if config.SocketPath == "" {
		return nil, ErrSocketPathRequired
	}

	if config.Logger == nil {
		config.Logger = adapters.NewDefaultLogger()
	}

	if config.SocketPermissions == 0 {
		config.SocketPermissions = 0660
	}

	handler := NewHandler(config.Backend, config.Logger)

	return &Server{
		config:  config,
		handler: handler,
	}, nil
}

// Start starts the Unix socket server
func (s *Server) Start(ctx context.Context) error {
	// Remove existing socket if it exists
	if err := os.Remove(s.config.SocketPath); err != nil && !os.IsNotExist(err) {
		return err
	}

	// Create Unix socket listener
	listener, err := net.Listen("unix", s.config.SocketPath)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.listener = listener
	s.mu.Unlock()

	// Set socket permissions
	if err := os.Chmod(s.config.SocketPath, s.config.SocketPermissions); err != nil {
		listener.Close()
		return err
	}

	s.config.Logger.Info(ctx, "Starting Unix socket server",
		adapters.Field{Key: "socket", Value: s.config.SocketPath},
	)

	// Accept connections
	go s.acceptLoop(ctx)

	// Wait for context cancellation
	<-ctx.Done()
	return s.Shutdown(context.Background())
}

// acceptLoop accepts incoming connections
func (s *Server) acceptLoop(ctx context.Context) {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			s.mu.Lock()
			closed := s.closed
			s.mu.Unlock()
			if closed {
				return
			}
			s.config.Logger.Warn(ctx, "Accept error",
				adapters.Field{Key: "error", Value: err.Error()},
			)
			continue
		}

		s.wg.Add(1)
		go s.handleConnection(ctx, conn)
	}
}

// handleConnection handles a single client connection
func (s *Server) handleConnection(ctx context.Context, conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()

	s.config.Logger.Debug(ctx, "New client connected")

	scanner := bufio.NewScanner(conn)
	// Increase buffer size for large requests
	const maxScanTokenSize = 10 * 1024 * 1024 // 10MB
	buf := make([]byte, maxScanTokenSize)
	scanner.Buffer(buf, maxScanTokenSize)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		response := s.processRequest(ctx, line)
		responseBytes, err := json.Marshal(response)
		if err != nil {
			s.config.Logger.Error(ctx, "Failed to marshal response",
				adapters.Field{Key: "error", Value: err.Error()},
			)
			continue
		}

		// Write response followed by newline
		responseBytes = append(responseBytes, '\n')
		if _, err := conn.Write(responseBytes); err != nil {
			s.config.Logger.Error(ctx, "Failed to write response",
				adapters.Field{Key: "error", Value: err.Error()},
			)
			return
		}
	}

	if err := scanner.Err(); err != nil {
		s.config.Logger.Debug(ctx, "Client disconnected",
			adapters.Field{Key: "error", Value: err.Error()},
		)
	}
}

// processRequest processes a JSON-RPC request
func (s *Server) processRequest(ctx context.Context, data []byte) *Response {
	var req Request
	if err := json.Unmarshal(data, &req); err != nil {
		return &Response{
			JSONRPC: jsonRPCVersion,
			Error: &RPCError{
				Code:    ErrCodeParseError,
				Message: "invalid JSON",
			},
			ID: nil,
		}
	}

	if req.JSONRPC != jsonRPCVersion {
		return &Response{
			JSONRPC: jsonRPCVersion,
			Error: &RPCError{
				Code:    ErrCodeInvalidRequest,
				Message: "invalid JSON-RPC version",
			},
			ID: req.ID,
		}
	}

	return s.handler.Handle(ctx, &req)
}

// Shutdown gracefully shuts down the server
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	s.closed = true
	listener := s.listener
	s.mu.Unlock()

	s.config.Logger.Info(ctx, "Shutting down Unix socket server")

	if listener != nil {
		listener.Close()
	}

	// Wait for active connections to finish
	s.wg.Wait()

	// Remove socket file
	if err := os.Remove(s.config.SocketPath); err != nil && !os.IsNotExist(err) {
		s.config.Logger.Warn(ctx, "Failed to remove socket file",
			adapters.Field{Key: "error", Value: err.Error()},
		)
	}

	return nil
}

// SocketPath returns the socket path
func (s *Server) SocketPath() string {
	return s.config.SocketPath
}
