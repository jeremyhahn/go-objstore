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

package serverconfig

import (
	"io"
	"os"

	"github.com/jeremyhahn/go-objstore/pkg/audit"
	grpcserver "github.com/jeremyhahn/go-objstore/pkg/server/grpc"
	mcpserver "github.com/jeremyhahn/go-objstore/pkg/server/mcp"
	"github.com/jeremyhahn/go-objstore/pkg/server/middleware"
	quicserver "github.com/jeremyhahn/go-objstore/pkg/server/quic"
	restserver "github.com/jeremyhahn/go-objstore/pkg/server/rest"
	unixserver "github.com/jeremyhahn/go-objstore/pkg/server/unix"
)

// These builders hand each transport its options struct populated from the
// resolved configuration: the configured listen address and TLS, plus the
// cross-transport middleware (rate limiting and audit) from a Middleware.
// Every binary builds its transports only through them, so a setting the
// loader accepts is applied the same way by the combined server and by each
// single-transport server.

// Middleware is the cross-transport middleware every transport applies:
// rate_limit.* and audit.enabled, with one audit logger shared by all of a
// process's transports.
type Middleware struct {
	RateLimit       bool
	RateLimitConfig *middleware.RateLimitConfig
	Audit           bool
	AuditLogger     audit.AuditLogger
}

// Middleware builds the shared middleware. When audit is enabled it creates
// the process's audit logger, writing to AuditOutput.
func (c *Config) Middleware() Middleware {
	mw := Middleware{
		RateLimit:       c.RateLimit.Enabled,
		RateLimitConfig: c.RateLimitConfig(),
		Audit:           c.Audit.Enabled,
	}
	if mw.Audit {
		auditConfig := audit.DefaultConfig()
		auditConfig.Output = c.AuditOutput()
		mw.AuditLogger = audit.NewAuditLogger(auditConfig)
	}
	return mw
}

// AuditOutput is where audit records are written: standard output, except
// when MCP runs in stdio mode, where standard output is the JSON-RPC stream
// and audit records go to standard error instead.
func (c *Config) AuditOutput() io.Writer {
	if mcpserver.ServerMode(c.MCP.Mode) == mcpserver.ModeStdio {
		return os.Stderr
	}
	return os.Stdout
}

// RateLimitConfig returns the shared rate limiting configuration.
func (c *Config) RateLimitConfig() *middleware.RateLimitConfig {
	return &middleware.RateLimitConfig{
		RequestsPerSecond: c.RateLimit.RPS,
		Burst:             c.RateLimit.Burst,
		PerIP:             c.RateLimit.PerClient,
	}
}

// GRPCOptions returns the gRPC server options: the configured address, the
// shared middleware and, when grpc.tls_cert and grpc.tls_key are set, the TLS
// credentials loaded from those files. A configured key pair that cannot be
// loaded is a *TLSCredentialsError, never a silent fallback to plaintext.
func (c *Config) GRPCOptions(mw Middleware) ([]grpcserver.ServerOption, error) {
	opts := []grpcserver.ServerOption{
		grpcserver.WithAddress(c.GRPC.Addr),
		grpcserver.WithRateLimit(mw.RateLimit, mw.RateLimitConfig),
		grpcserver.WithAudit(mw.Audit),
	}
	if mw.AuditLogger != nil {
		opts = append(opts, grpcserver.WithAuditLogger(mw.AuditLogger))
	}
	if !c.GRPCTLSEnabled() {
		return opts, nil
	}
	tlsOpt, err := grpcserver.WithTLSFromFiles(c.GRPC.TLSCert, c.GRPC.TLSKey)
	if err != nil {
		return nil, &TLSCredentialsError{Transport: "grpc", CertFile: c.GRPC.TLSCert, KeyFile: c.GRPC.TLSKey, Err: err}
	}
	return append(opts, tlsOpt), nil
}

// GRPCTLSEnabled reports whether the gRPC listener is configured for TLS.
func (c *Config) GRPCTLSEnabled() bool {
	return c.GRPC.TLSCert != "" && c.GRPC.TLSKey != ""
}

// RESTServerConfig returns the REST server configuration: the package
// defaults with the configured host, port and metrics exposure and the shared
// middleware.
func (c *Config) RESTServerConfig(mw Middleware) *restserver.ServerConfig {
	sc := restserver.DefaultServerConfig()
	sc.Host = c.REST.Host
	sc.Port = c.REST.Port
	sc.MetricsPublic = c.REST.MetricsPublic
	sc.EnableRateLimit = mw.RateLimit
	sc.RateLimitConfig = mw.RateLimitConfig
	sc.EnableAudit = mw.Audit
	if mw.AuditLogger != nil {
		sc.AuditLogger = mw.AuditLogger
	}
	return sc
}

// QUICOptions returns the QUIC server options: the package defaults with the
// configured address, body limit, timeouts and stream limits and the shared
// middleware. TLS is loaded by the caller from QUIC.TLSCert/TLSKey or
// QUIC.SelfSigned.
func (c *Config) QUICOptions(mw Middleware) *quicserver.Options {
	opts := quicserver.DefaultOptions().
		WithAddr(c.QUIC.Addr).
		WithMaxRequestBodySize(c.QUIC.MaxBodySize).
		WithTimeouts(c.QUIC.ReadTimeout, c.QUIC.WriteTimeout, c.QUIC.IdleTimeout).
		WithStreamLimits(c.QUIC.MaxStreams, c.QUIC.MaxStreams)
	if mw.RateLimit {
		opts = opts.WithRateLimit(mw.RateLimitConfig)
	}
	if mw.Audit {
		opts = opts.WithAudit(mw.AuditLogger)
	}
	return opts
}

// MCPServerConfig returns the MCP server configuration with the configured
// mode and HTTP address and the shared middleware.
func (c *Config) MCPServerConfig(mw Middleware) (*mcpserver.ServerConfig, error) {
	mode, err := c.MCPMode()
	if err != nil {
		return nil, err
	}
	sc := mcpserver.DefaultServerConfig()
	sc.Mode = mode
	sc.HTTPAddress = c.MCP.Addr
	sc.EnableRateLimit = mw.RateLimit
	sc.RateLimitConfig = mw.RateLimitConfig
	sc.EnableAudit = mw.Audit
	sc.AuditLogger = mw.AuditLogger
	return sc, nil
}

// UnixServerConfig returns the Unix socket server configuration with the
// configured socket path, the default backend and the shared middleware.
func (c *Config) UnixServerConfig(mw Middleware) *unixserver.ServerConfig {
	sc := unixserver.DefaultConfig()
	sc.SocketPath = c.Unix.Socket
	sc.Backend = "default"
	sc.EnableRateLimit = mw.RateLimit
	sc.RateLimitConfig = mw.RateLimitConfig
	sc.EnableAudit = mw.Audit
	sc.AuditLogger = mw.AuditLogger
	return sc
}
