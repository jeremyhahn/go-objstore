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
	"errors"
	"net"
	"os"
	"sync"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/objstore"
)

// ErrPeerCredUnsupported indicates that peer credentials could not be obtained
// for the connection (e.g. on a platform without SO_PEERCRED, or for a
// non-Unix connection). Callers fall back to the configured Authenticator when
// this is returned.
var ErrPeerCredUnsupported = errors.New("unix: peer credentials not supported on this platform/connection")

// principalCtxKey is the context key under which a peer-credential principal is
// carried from the connection layer to the request handler.
type principalCtxKey struct{}

// withPrincipal returns a copy of ctx carrying the given principal.
func withPrincipal(ctx context.Context, p *adapters.Principal) context.Context {
	return context.WithValue(ctx, principalCtxKey{}, p)
}

// principalFromContext returns the peer-credential principal stored in ctx, if any.
func principalFromContext(ctx context.Context) (*adapters.Principal, bool) {
	p, ok := ctx.Value(principalCtxKey{}).(*adapters.Principal)
	return p, ok
}

// DisablePeerCredentials returns a *bool suitable for
// ServerConfig.UsePeerCredentials to explicitly turn peer-credential
// authentication off.
func DisablePeerCredentials() *bool { b := false; return &b }

// EnablePeerCredentials returns a *bool suitable for
// ServerConfig.UsePeerCredentials to explicitly turn peer-credential
// authentication on (this is also the default when left unset).
func EnablePeerCredentials() *bool { b := true; return &b }

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

	// Authenticator is the pluggable authentication adapter (default: NoOpAuthenticator).
	//
	// Unix-domain-socket transport carries no HTTP/gRPC/mTLS credential object,
	// so the server cannot present transport credentials to the authenticator.
	// With the default NoOpAuthenticator every request is treated as the
	// anonymous principal. Custom authenticators are still honored for their
	// principal shape, but principals here are derived without transport
	// credentials. Richer unix authN (e.g. SO_PEERCRED-based identity) is a
	// follow-up.
	Authenticator adapters.Authenticator

	// Authorizer is the pluggable authorization adapter (default: NoOpAuthorizer = allow-all).
	Authorizer adapters.Authorizer

	// UsePeerCredentials, when set, controls whether the request principal is
	// derived from the connecting peer's OS credentials (SO_PEERCRED: uid/gid/pid)
	// instead of the configured Authenticator.
	//
	// A nil value (the zero value, i.e. unset) is treated as TRUE: peer
	// credentials are the natural, zero-config identity for a Unix domain socket
	// and are strictly more informative than an anonymous principal. Use
	// DisablePeerCredentials() to opt out, or EnablePeerCredentials() to be
	// explicit.
	//
	// The socket file's permission bits (SocketPermissions, default 0660) remain
	// the primary access gate: the OS only lets processes that can open the
	// socket connect at all. Peer credentials add the authenticated identity
	// (uid/gid and primary-group role) on top, which the Authorizer then uses for
	// per-method decisions. With the default NoOpAuthorizer every method is still
	// allowed, so enabling peer credentials does not change access on its own;
	// supply an RBACAuthorizer keyed on the peer's group/role to restrict by OS
	// identity.
	//
	// If peer credentials cannot be extracted (non-Linux platforms, or a non-Unix
	// connection), the server falls back to the configured Authenticator.
	UsePeerCredentials *bool
}

// DefaultConfig returns default server configuration
func DefaultConfig() *ServerConfig {
	return &ServerConfig{
		SocketPath:        "/var/run/objstore.sock",
		SocketPermissions: 0660,
		Logger:            adapters.NewDefaultLogger(),
		Authenticator:     adapters.NewNoOpAuthenticator(),
		Authorizer:        adapters.NewNoOpAuthorizer(),
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
	// usePeerCred is the resolved value of ServerConfig.UsePeerCredentials
	// (nil/unset defaults to true).
	usePeerCred bool
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

	if config.Authenticator == nil {
		config.Authenticator = adapters.NewNoOpAuthenticator()
	}

	if config.Authorizer == nil {
		config.Authorizer = adapters.NewNoOpAuthorizer()
	}

	handler := NewHandler(config.Backend, config.Logger, config.Authenticator, config.Authorizer)

	// Peer-credential authentication defaults to enabled when left unset.
	usePeerCred := true
	if config.UsePeerCredentials != nil {
		usePeerCred = *config.UsePeerCredentials
	}

	return &Server{
		config:      config,
		handler:     handler,
		usePeerCred: usePeerCred,
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
		// Close the listener we just opened; the chmod failure is the error we
		// surface, so a close failure here is intentionally ignored.
		_ = listener.Close()
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
				adapters.Field{Key: fieldError, Value: err.Error()},
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

	// Extract the peer's OS credentials once per connection and carry the
	// resulting principal in the context so the handler can authorize by the
	// connecting process's identity. The socket file permissions remain the
	// primary access gate; peer credentials supply the principal identity. If
	// extraction is unsupported (non-Linux, non-Unix conn), fall back to the
	// configured Authenticator by leaving the principal out of the context.
	if s.usePeerCred {
		if principal, err := peerCredPrincipal(conn); err != nil {
			if !errors.Is(err, ErrPeerCredUnsupported) {
				s.config.Logger.Warn(ctx, "peer credential extraction failed",
					adapters.Field{Key: fieldError, Value: err.Error()},
				)
			}
		} else {
			ctx = withPrincipal(ctx, principal)
		}
	}

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
				adapters.Field{Key: fieldError, Value: err.Error()},
			)
			continue
		}

		// Write response followed by newline
		responseBytes = append(responseBytes, '\n')
		if _, err := conn.Write(responseBytes); err != nil {
			s.config.Logger.Error(ctx, "Failed to write response",
				adapters.Field{Key: fieldError, Value: err.Error()},
			)
			return
		}
	}

	if err := scanner.Err(); err != nil {
		s.config.Logger.Debug(ctx, "Client disconnected",
			adapters.Field{Key: fieldError, Value: err.Error()},
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
		if err := listener.Close(); err != nil {
			s.config.Logger.Warn(ctx, "Failed to close listener",
				adapters.Field{Key: fieldError, Value: err.Error()},
			)
		}
	}

	// Wait for active connections to finish
	s.wg.Wait()

	// Remove socket file
	if err := os.Remove(s.config.SocketPath); err != nil && !os.IsNotExist(err) {
		s.config.Logger.Warn(ctx, "Failed to remove socket file",
			adapters.Field{Key: fieldError, Value: err.Error()},
		)
	}

	return nil
}

// SocketPath returns the socket path
func (s *Server) SocketPath() string {
	return s.config.SocketPath
}
