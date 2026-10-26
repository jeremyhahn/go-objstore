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
	"flag"
	"time"
)

// Each Bind*Flags function registers one binary's command line. Flag names
// are the binaries' established names; every flag points at a Config field,
// defaults to that field's current value (the defaults layer during parsing)
// and names the config key and environment variable that set the same field.

// BindServerFlags registers the flags of objstore-server, which runs every
// transport in one process.
func BindServerFlags(fs *flag.FlagSet, c *Config) {
	bindBackendFlags(fs, c)

	boolVar(fs, &c.GRPC.Enabled, "grpc", "grpc.enabled", "Enable gRPC server")
	boolVar(fs, &c.REST.Enabled, "rest", "rest.enabled", "Enable REST server")
	boolVar(fs, &c.QUIC.Enabled, "quic", "quic.enabled", "Enable QUIC/HTTP3 server")
	boolVar(fs, &c.MCP.Enabled, "mcp", "mcp.enabled", "Enable MCP server")
	boolVar(fs, &c.Unix.Enabled, "unix", "unix.enabled", "Enable Unix socket server")

	stringVar(fs, &c.GRPC.Addr, "grpc-addr", "grpc.addr", "gRPC server address")
	stringVar(fs, &c.GRPC.TLSCert, "grpc-tls-cert", "grpc.tls_cert", "gRPC TLS certificate file")
	stringVar(fs, &c.GRPC.TLSKey, "grpc-tls-key", "grpc.tls_key", "gRPC TLS key file")
	intVar(fs, &c.GRPC.MaxRecvMsgSize, "grpc-max-recv-msg-size", "grpc.max_recv_msg_size", "gRPC maximum received message size in bytes")
	intVar(fs, &c.GRPC.MaxSendMsgSize, "grpc-max-send-msg-size", "grpc.max_send_msg_size", "gRPC maximum sent message size in bytes")

	stringVar(fs, &c.REST.Host, "rest-host", "rest.host", "REST server host (interface to bind)")
	intVar(fs, &c.REST.Port, "rest-port", "rest.port", "REST server port")
	boolVar(fs, &c.REST.MetricsPublic, "metrics-public", "rest.metrics_public", "Expose /metrics without authorization")

	stringVar(fs, &c.QUIC.Addr, "quic-addr", "quic.addr", "QUIC server address")
	stringVar(fs, &c.QUIC.TLSCert, "quic-tls-cert", "quic.tls_cert", "QUIC TLS certificate file")
	stringVar(fs, &c.QUIC.TLSKey, "quic-tls-key", "quic.tls_key", "QUIC TLS key file")
	boolVar(fs, &c.QUIC.SelfSigned, "quic-self-signed", "quic.self_signed", "Use self-signed cert for QUIC (testing only)")

	stringVar(fs, &c.MCP.Mode, "mcp-mode", "mcp.mode", "MCP mode: stdio or http")
	stringVar(fs, &c.MCP.Addr, "mcp-addr", "mcp.addr", "MCP HTTP server address")

	stringVar(fs, &c.Unix.Socket, "unix-socket", "unix.socket", "Unix socket path")

	bindMiddlewareFlags(fs, c)
}

// BindGRPCServerFlags registers the flags of objstore-grpc-server.
func BindGRPCServerFlags(fs *flag.FlagSet, c *Config) {
	bindBackendFlags(fs, c)
	stringVar(fs, &c.GRPC.Addr, "addr", "grpc.addr", "gRPC server address")
	stringVar(fs, &c.GRPC.TLSCert, "tls-cert", "grpc.tls_cert", "TLS certificate file")
	stringVar(fs, &c.GRPC.TLSKey, "tls-key", "grpc.tls_key", "TLS key file")
	intVar(fs, &c.GRPC.MaxRecvMsgSize, "max-recv-msg-size", "grpc.max_recv_msg_size", "Maximum received message size in bytes")
	intVar(fs, &c.GRPC.MaxSendMsgSize, "max-send-msg-size", "grpc.max_send_msg_size", "Maximum sent message size in bytes")
	bindMiddlewareFlags(fs, c)
}

// BindRESTServerFlags registers the flags of objstore-rest-server.
func BindRESTServerFlags(fs *flag.FlagSet, c *Config) {
	bindBackendFlags(fs, c)
	stringVar(fs, &c.REST.Host, "host", "rest.host", "REST server host")
	intVar(fs, &c.REST.Port, "port", "rest.port", "REST server port")
	boolVar(fs, &c.REST.MetricsPublic, "metrics-public", "rest.metrics_public", "Expose /metrics without authorization")
	bindMiddlewareFlags(fs, c)
}

// BindQUICServerFlags registers the flags of objstore-quic-server.
func BindQUICServerFlags(fs *flag.FlagSet, c *Config) {
	bindBackendFlags(fs, c)
	stringVar(fs, &c.QUIC.Addr, "addr", "quic.addr", "UDP address to listen on")
	stringVar(fs, &c.QUIC.TLSCert, "tlscert", "quic.tls_cert", "Path to TLS certificate file")
	stringVar(fs, &c.QUIC.TLSKey, "tlskey", "quic.tls_key", "Path to TLS private key file")
	int64Var(fs, &c.QUIC.MaxBodySize, "maxbodysize", "quic.max_body_size", "Maximum request body size in bytes")
	durationVar(fs, &c.QUIC.ReadTimeout, "readtimeout", "quic.read_timeout", "Read timeout")
	durationVar(fs, &c.QUIC.WriteTimeout, "writetimeout", "quic.write_timeout", "Write timeout")
	durationVar(fs, &c.QUIC.IdleTimeout, "idletimeout", "quic.idle_timeout", "Idle timeout")
	int64Var(fs, &c.QUIC.MaxStreams, "maxstreams", "quic.max_streams", "Maximum bidirectional streams per connection")
	boolVar(fs, &c.QUIC.SelfSigned, "selfsigned", "quic.self_signed", "Use self-signed certificate (for testing only)")
	bindMiddlewareFlags(fs, c)
}

// BindMCPServerFlags registers the flags of objstore-mcp-server.
func BindMCPServerFlags(fs *flag.FlagSet, c *Config) {
	bindBackendFlags(fs, c)
	stringVar(fs, &c.MCP.Mode, "mode", "mcp.mode", "Server mode: stdio or http")
	stringVar(fs, &c.MCP.Addr, "addr", "mcp.addr", "HTTP server address (only for http mode)")
	bindMiddlewareFlags(fs, c)
}

// bindMiddlewareFlags registers the cross-transport middleware flags every
// server binary accepts.
func bindMiddlewareFlags(fs *flag.FlagSet, c *Config) {
	boolVar(fs, &c.RateLimit.Enabled, "rate-limit", "rate_limit.enabled", "Enable rate limiting on all transports")
	float64Var(fs, &c.RateLimit.RPS, "rate-limit-rps", "rate_limit.rps", "Rate limit requests per second")
	intVar(fs, &c.RateLimit.Burst, "rate-limit-burst", "rate_limit.burst", "Rate limit burst size")
	boolVar(fs, &c.RateLimit.PerClient, "rate-limit-per-client", "rate_limit.per_client", "Rate limit per client instead of globally")
	boolVar(fs, &c.Audit.Enabled, "audit", "audit.enabled", "Enable audit logging on all transports")
}

func bindBackendFlags(fs *flag.FlagSet, c *Config) {
	stringVar(fs, &c.Backend, "backend", "backend", "Storage backend (local, s3, gcs, azure)")
	stringVar(fs, &c.Path, "path", "path", "Storage path for local backend")
}

func stringVar(fs *flag.FlagSet, p *string, name, key, usage string) {
	fs.StringVar(p, name, *p, usage+envHint(key))
}

func boolVar(fs *flag.FlagSet, p *bool, name, key, usage string) {
	fs.BoolVar(p, name, *p, usage+envHint(key))
}

func intVar(fs *flag.FlagSet, p *int, name, key, usage string) {
	fs.IntVar(p, name, *p, usage+envHint(key))
}

func int64Var(fs *flag.FlagSet, p *int64, name, key, usage string) {
	fs.Int64Var(p, name, *p, usage+envHint(key))
}

func float64Var(fs *flag.FlagSet, p *float64, name, key, usage string) {
	fs.Float64Var(p, name, *p, usage+envHint(key))
}

func durationVar(fs *flag.FlagSet, p *time.Duration, name, key, usage string) {
	fs.DurationVar(p, name, *p, usage+envHint(key))
}
