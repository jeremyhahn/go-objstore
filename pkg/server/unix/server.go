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
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/audit"
	"github.com/jeremyhahn/go-objstore/pkg/objstore"
	"github.com/jeremyhahn/go-objstore/pkg/server/jsonrpc"
	"github.com/jeremyhahn/go-objstore/pkg/server/metrics"
	"github.com/jeremyhahn/go-objstore/pkg/server/middleware"
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

// defaultReadDeadline is applied to each scan iteration on a Unix socket
// connection to prevent a slow or stalled client from holding the goroutine
// indefinitely.
const defaultReadDeadline = 30 * time.Second

// defaultMaxConnections is the default limit for concurrent Unix connections.
const defaultMaxConnections = 100

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

	// MaxConnections is the maximum number of simultaneous connections the
	// server will serve concurrently. Additional connections are accepted but
	// block until a slot opens. Zero or negative values use the default (100).
	MaxConnections int

	// ReadDeadline is the per-request read deadline applied to each scan
	// iteration. Zero uses the default (30 s).
	ReadDeadline time.Duration

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

	// EnableRateLimit enables rate limiting (default: false). Requests are
	// keyed by the peer's OS identity (uid) when peer credentials are
	// available, otherwise a single shared bucket applies.
	EnableRateLimit bool

	// RateLimitConfig is the rate limiting configuration.
	RateLimitConfig *middleware.RateLimitConfig

	// EnableAudit enables audit logging (default: false).
	EnableAudit bool

	// AuditLogger is the audit logger used when EnableAudit is set.
	AuditLogger audit.AuditLogger
}

// DefaultConfig returns default server configuration
func DefaultConfig() *ServerConfig {
	return &ServerConfig{
		SocketPath:        "/var/run/objstore.sock",
		SocketPermissions: 0660,
		Logger:            adapters.NewDefaultLogger(),
		Authenticator:     adapters.NewNoOpAuthenticator(),
		Authorizer:        adapters.NewNoOpAuthorizer(),
		MaxConnections:    defaultMaxConnections,
		ReadDeadline:      defaultReadDeadline,
	}
}

// Server represents the Unix socket server
type Server struct {
	config       *ServerConfig
	listener     net.Listener
	handler      *Handler
	rateLimiter  *middleware.RateLimiter
	mu           sync.Mutex
	closed       bool
	wg           sync.WaitGroup
	connSem      chan struct{} // semaphore bounding concurrent connections
	readDeadline time.Duration
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

	maxConns := config.MaxConnections
	if maxConns <= 0 {
		maxConns = defaultMaxConnections
	}

	readDeadline := config.ReadDeadline
	if readDeadline <= 0 {
		readDeadline = defaultReadDeadline
	}

	if config.EnableAudit && config.AuditLogger == nil {
		config.AuditLogger = audit.NewDefaultAuditLogger()
	}

	var rateLimiter *middleware.RateLimiter
	if config.EnableRateLimit {
		rateLimiter = middleware.NewRateLimiter(config.RateLimitConfig, config.Logger)
	}

	return &Server{
		config:       config,
		handler:      handler,
		rateLimiter:  rateLimiter,
		connSem:      make(chan struct{}, maxConns),
		readDeadline: readDeadline,
		usePeerCred:  usePeerCred,
	}, nil
}

// removeStaleSocket removes the file at path only when it is a Unix domain
// socket left over from a previous run. It returns nil when the path does not
// exist. When the path exists but is not a socket it leaves the file untouched
// and returns an error, so a misconfigured SocketPath never deletes an
// unrelated file.
func removeStaleSocket(path string) error {
	fi, err := os.Lstat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if fi.Mode()&os.ModeSocket == 0 {
		return fmt.Errorf("%w: %s", ErrSocketPathNotSocket, path)
	}
	return os.Remove(path)
}

// Start starts the Unix socket server
func (s *Server) Start(ctx context.Context) error {
	// Remove a stale socket from a previous run, refusing to delete anything
	// that is not a socket.
	if err := removeStaleSocket(s.config.SocketPath); err != nil {
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

		// Acquire the concurrency slot BEFORE spawning the goroutine so
		// MaxConnections bounds goroutines and file descriptors, not just
		// concurrent processing. Excess connections queue in the kernel's
		// accept backlog instead of accumulating one goroutine each.
		s.connSem <- struct{}{}
		s.wg.Add(1)
		go s.handleConnection(ctx, conn)
	}
}

// handleConnection handles a single client connection
func (s *Server) handleConnection(ctx context.Context, conn net.Conn) {
	defer func() { <-s.connSem }()
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

	// Set the initial read deadline before the first Scan so a client that
	// connects and never sends a request cannot hold the goroutine forever.
	if err := conn.SetReadDeadline(time.Now().Add(s.readDeadline)); err != nil {
		s.config.Logger.Warn(ctx, "Failed to set read deadline",
			adapters.Field{Key: fieldError, Value: err.Error()},
		)
	}

	for scanner.Scan() {
		// Refresh the read deadline after each request so a slow client
		// cannot hold the goroutine indefinitely.
		if err := conn.SetReadDeadline(time.Now().Add(s.readDeadline)); err != nil {
			s.config.Logger.Warn(ctx, "Failed to set read deadline",
				adapters.Field{Key: fieldError, Value: err.Error()},
			)
		}

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
func (s *Server) processRequest(ctx context.Context, data []byte) (resp *Response) {
	// Every request carries a request ID for tracing; the unix transport has
	// no header to receive one, so generate it here.
	ctx, _ = middleware.EnsureRequestID(ctx)

	start := time.Now()
	method := ""
	defer func() {
		if rec := recover(); rec != nil {
			slog.ErrorContext(ctx, "[Unix] Panic recovered", slog.Any("panic", rec))
			resp = jsonrpc.NewError(nil, ErrCodeInternalError, "internal server error")
		}
		outcome := "ok"
		var auditErr error
		if resp != nil && resp.Error != nil {
			outcome = "error"
			auditErr = fmt.Errorf("%w: %s", ErrRequestFailed, resp.Error.Message)
		}
		metrics.Default.RecordRequest(metrics.TransportUnix, outcome, time.Since(start))
		if s.config.EnableAudit && s.config.AuditLogger != nil && method != "" {
			principal, _ := principalFromContext(ctx)
			audit.LogRPC(ctx, s.config.AuditLogger, "unix", method, principal, start, auditErr)
		}
	}()

	// Shared parse + version validation with the MCP transport.
	req, parseErr := jsonrpc.ParseRequest(data)
	if parseErr != nil {
		return parseErr
	}
	method = req.Method

	// Rate limit, keyed by the peer's OS identity when available.
	if s.rateLimiter != nil {
		key := "unix"
		if principal, ok := principalFromContext(ctx); ok && principal != nil && principal.ID != "" {
			key = principal.ID
		}
		if !s.rateLimiter.AllowKey(key) {
			return jsonrpc.NewError(req.ID, jsonrpc.CodeRateLimited, "rate limit exceeded")
		}
	}

	return s.handler.Handle(ctx, req)
}

// Shutdown gracefully shuts down the server
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	s.closed = true
	listener := s.listener
	s.mu.Unlock()

	if s.rateLimiter != nil {
		s.rateLimiter.Stop()
	}

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

	// Remove the socket file. Cleanup must not fail shutdown: only remove the
	// path when it is still a socket (silently skip otherwise) and log any
	// removal failure instead of returning it.
	if fi, err := os.Lstat(s.config.SocketPath); err == nil && fi.Mode()&os.ModeSocket != 0 {
		if err := os.Remove(s.config.SocketPath); err != nil {
			s.config.Logger.Warn(ctx, "Failed to remove socket file",
				adapters.Field{Key: fieldError, Value: err.Error()},
			)
		}
	}

	return nil
}

// SocketPath returns the socket path
func (s *Server) SocketPath() string {
	return s.config.SocketPath
}
