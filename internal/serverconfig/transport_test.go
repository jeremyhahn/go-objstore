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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/auditlog"
	"github.com/jeremyhahn/go-objstore/pkg/objstore"
	grpcserver "github.com/jeremyhahn/go-objstore/pkg/server/grpc"
	"github.com/jeremyhahn/go-objstore/pkg/server/middleware"
	restserver "github.com/jeremyhahn/go-objstore/pkg/server/rest"
)

// writeKeyPair writes a self-signed P-256 certificate and key as PEM files.
func writeKeyPair(t *testing.T) (certFile, keyFile string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "serverconfig-test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	dir := t.TempDir()
	certFile = filepath.Join(dir, "server.crt")
	keyFile = filepath.Join(dir, "server.key")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	return certFile, keyFile
}

// applyGRPC resolves cfg into the gRPC server options struct.
func applyGRPC(t *testing.T, cfg *Config, mw Middleware) *grpcserver.ServerOptions {
	t.Helper()
	opts, err := cfg.GRPCOptions(mw)
	if err != nil {
		t.Fatalf("GRPCOptions: %v", err)
	}
	o := grpcserver.DefaultServerOptions()
	for _, opt := range opts {
		opt(o)
	}
	return o
}

// TestGRPCOptions_TLSFromEveryPath proves a gRPC key pair supplied through
// the file, the environment, or either binary's flags reaches the gRPC
// server options as TLS credentials, for the combined and standalone server.
func TestGRPCOptions_TLSFromEveryPath(t *testing.T) {
	cert, key := writeKeyPair(t)
	cases := []struct {
		name string
		env  map[string]string
		yaml string
		args []string
		bind BindFunc
	}{
		{name: "objstore-server file", yaml: "grpc:\n  tls_cert: " + cert + "\n  tls_key: " + key + "\n", bind: BindServerFlags},
		{name: "objstore-server env", env: map[string]string{"OBJSTORE_GRPC_TLS_CERT": cert, "OBJSTORE_GRPC_TLS_KEY": key}, bind: BindServerFlags},
		{name: "objstore-server flags", args: []string{"-grpc-tls-cert", cert, "-grpc-tls-key", key}, bind: BindServerFlags},
		{name: "objstore-grpc-server env", env: map[string]string{"OBJSTORE_GRPC_TLS_CERT": cert, "OBJSTORE_GRPC_TLS_KEY": key}, bind: BindGRPCServerFlags},
		{name: "objstore-grpc-server flags", args: []string{"-tls-cert", cert, "-tls-key", key}, bind: BindGRPCServerFlags},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			for k, v := range c.env {
				t.Setenv(k, v)
			}
			args := c.args
			if c.yaml != "" {
				args = append([]string{"-config", writeConfig(t, c.yaml)}, args...)
			}
			cfg := mustLoad(t, args, Defaults(), c.bind)
			if !cfg.GRPCTLSEnabled() {
				t.Fatal("GRPCTLSEnabled() = false with a configured key pair")
			}
			o := applyGRPC(t, cfg, cfg.Middleware())
			if o.TLSConfig == nil {
				t.Fatal("gRPC options carry no TLS config: the listener would serve plaintext")
			}
			if len(o.TLSConfig.Certificates) != 1 || o.TLSConfig.MinVersion < tls.VersionTLS12 {
				t.Errorf("TLS config = %d certs, min version %x; want 1 cert, >= TLS 1.2",
					len(o.TLSConfig.Certificates), o.TLSConfig.MinVersion)
			}
			if o.Address != cfg.GRPC.Addr {
				t.Errorf("Address = %q, want %q", o.Address, cfg.GRPC.Addr)
			}
		})
	}
}

func TestGRPCOptions_PlaintextWithoutKeyPair(t *testing.T) {
	t.Setenv("OBJSTORE_GRPC_ADDR", "127.0.0.1:7443")
	cfg := mustLoad(t, nil, Defaults(), BindServerFlags)
	if cfg.GRPCTLSEnabled() {
		t.Error("GRPCTLSEnabled() = true without a key pair")
	}
	o := applyGRPC(t, cfg, cfg.Middleware())
	if o.TLSConfig != nil {
		t.Error("TLS config set without a configured key pair")
	}
	if o.Address != "127.0.0.1:7443" {
		t.Errorf("Address = %q, want 127.0.0.1:7443", o.Address)
	}
}

func TestGRPCOptions_UnloadableKeyPair(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "absent.pem")
	cfg := mustLoad(t, []string{"-grpc-tls-cert", missing, "-grpc-tls-key", missing}, Defaults(), BindServerFlags)
	_, err := cfg.GRPCOptions(cfg.Middleware())
	var tce *TLSCredentialsError
	if !errors.As(err, &tce) {
		t.Fatalf("err = %v (%T), want *TLSCredentialsError", err, err)
	}
	if tce.Transport != "grpc" || tce.CertFile != missing || tce.KeyFile != missing {
		t.Errorf("TLSCredentialsError = %+v, want grpc and the configured files", tce)
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Errorf("TLSCredentialsError does not unwrap to the file error: %v", err)
	}
}

// transportMiddleware is the middleware a transport's options struct ended
// up with.
type transportMiddleware struct {
	rateLimit   bool
	rateConfig  *middleware.RateLimitConfig
	audit       bool
	auditLogger auditlog.AuditLogger
}

// buildTransport resolves each transport's real options struct from cfg and
// reports the middleware it carries.
var buildTransport = map[string]func(t *testing.T, cfg *Config, mw Middleware) transportMiddleware{
	"grpc": func(t *testing.T, cfg *Config, mw Middleware) transportMiddleware {
		o := applyGRPC(t, cfg, mw)
		return transportMiddleware{o.EnableRateLimit, o.RateLimitConfig, o.EnableAudit, o.AuditLogger}
	},
	"rest": func(_ *testing.T, cfg *Config, mw Middleware) transportMiddleware {
		sc := cfg.RESTServerConfig(mw)
		return transportMiddleware{sc.EnableRateLimit, sc.RateLimitConfig, sc.EnableAudit, sc.AuditLogger}
	},
	"quic": func(_ *testing.T, cfg *Config, mw Middleware) transportMiddleware {
		o := cfg.QUICOptions(mw)
		return transportMiddleware{o.EnableRateLimit, o.RateLimitConfig, o.EnableAudit, o.AuditLogger}
	},
	"mcp": func(t *testing.T, cfg *Config, mw Middleware) transportMiddleware {
		sc, err := cfg.MCPServerConfig(mw)
		if err != nil {
			t.Fatalf("MCPServerConfig: %v", err)
		}
		return transportMiddleware{sc.EnableRateLimit, sc.RateLimitConfig, sc.EnableAudit, sc.AuditLogger}
	},
	"unix": func(_ *testing.T, cfg *Config, mw Middleware) transportMiddleware {
		sc := cfg.UnixServerConfig(mw)
		return transportMiddleware{sc.EnableRateLimit, sc.RateLimitConfig, sc.EnableAudit, sc.AuditLogger}
	},
}

// TestMiddleware_ReachesEveryTransport proves rate_limit.* and
// audit.enabled, set through the environment or a binary's flags, are
// applied to the options struct of every transport each binary serves — on
// and off.
func TestMiddleware_ReachesEveryTransport(t *testing.T) {
	binaries := []struct {
		name       string
		defaults   Config
		bind       BindFunc
		transports []string
	}{
		{"objstore-server", Defaults(), BindServerFlags, []string{"grpc", "rest", "quic", "mcp", "unix"}},
		{"objstore-grpc-server", Defaults(), BindGRPCServerFlags, []string{"grpc"}},
		{"objstore-rest-server", RESTServerDefaults(), BindRESTServerFlags, []string{"rest"}},
		{"objstore-quic-server", QUICServerDefaults(), BindQUICServerFlags, []string{"quic"}},
		{"objstore-mcp-server", Defaults(), BindMCPServerFlags, []string{"mcp"}},
	}
	settings := []struct {
		name      string
		env       map[string]string
		args      []string
		rateLimit bool
		audit     bool
	}{
		{
			name: "env enables rate limit, disables audit",
			env: map[string]string{
				"OBJSTORE_RATE_LIMIT_ENABLED": "true", "OBJSTORE_RATE_LIMIT_RPS": "7",
				"OBJSTORE_RATE_LIMIT_BURST": "9", "OBJSTORE_RATE_LIMIT_PER_CLIENT": "true",
				"OBJSTORE_AUDIT_ENABLED": "false",
			},
			rateLimit: true, audit: false,
		},
		{
			name:      "flags enable rate limit, disable audit",
			args:      []string{"-rate-limit", "-rate-limit-rps", "7", "-rate-limit-burst", "9", "-rate-limit-per-client", "-audit=false"},
			rateLimit: true, audit: false,
		},
		{
			name:      "flags override env",
			env:       map[string]string{"OBJSTORE_RATE_LIMIT_ENABLED": "true", "OBJSTORE_AUDIT_ENABLED": "false"},
			args:      []string{"-rate-limit=false", "-audit", "-rate-limit-rps", "7", "-rate-limit-burst", "9", "-rate-limit-per-client"},
			rateLimit: false, audit: true,
		},
	}
	for _, b := range binaries {
		for _, st := range settings {
			t.Run(b.name+"/"+st.name, func(t *testing.T) {
				for k, v := range st.env {
					t.Setenv(k, v)
				}
				cfg := mustLoad(t, st.args, b.defaults, b.bind)
				mw := cfg.Middleware()
				if (mw.AuditLogger != nil) != st.audit {
					t.Fatalf("audit logger present = %v, want %v", mw.AuditLogger != nil, st.audit)
				}
				for _, name := range b.transports {
					got := buildTransport[name](t, cfg, mw)
					if got.rateLimit != st.rateLimit {
						t.Errorf("%s: rate limiting = %v, want %v", name, got.rateLimit, st.rateLimit)
					}
					if st.rateLimit {
						rc := got.rateConfig
						if rc == nil || rc.RequestsPerSecond != 7 || rc.Burst != 9 || !rc.PerIP {
							t.Errorf("%s: rate limit config = %+v, want 7 rps, burst 9, per client", name, rc)
						}
					}
					if got.audit != st.audit {
						t.Errorf("%s: audit = %v, want %v", name, got.audit, st.audit)
					}
					if st.audit && got.auditLogger != mw.AuditLogger {
						t.Errorf("%s: audit logger is not the process's shared logger", name)
					}
				}
			})
		}
	}
}

func TestAuditOutput_StdioModeKeepsStdoutForJSONRPC(t *testing.T) {
	cfg := mustLoad(t, []string{"-mode", "stdio"}, Defaults(), BindMCPServerFlags)
	if cfg.AuditOutput() != os.Stderr {
		t.Error("audit output in MCP stdio mode must be stderr: stdout carries JSON-RPC")
	}
	cfg = mustLoad(t, nil, Defaults(), BindMCPServerFlags)
	if cfg.AuditOutput() != os.Stdout {
		t.Error("audit output in MCP http mode should be stdout")
	}
}

// TestRESTHost_IPv6ReachesListenerAddress drives an IPv6 bind host from each
// config path through RESTServerConfig into the REST server and checks the
// address that server listens on is a valid IPv6 TCP address. Binding the
// socket is covered by the server integration suite.
func TestRESTHost_IPv6ReachesListenerAddress(t *testing.T) {
	objstore.Reset()
	t.Cleanup(objstore.Reset)
	if err := objstore.Initialize(&objstore.FacadeConfig{
		BackendConfigs: map[string]objstore.BackendConfig{
			"default": {Type: "local", Settings: map[string]string{"path": t.TempDir()}},
		},
		DefaultBackend: "default",
	}); err != nil {
		t.Fatalf("initialize facade: %v", err)
	}

	cases := []struct {
		name string
		env  map[string]string
		yaml string
		args []string
	}{
		{name: "file", yaml: "rest:\n  host: \"::1\"\n  port: 9443\n"},
		{name: "env", env: map[string]string{"OBJSTORE_REST_HOST": "::1", "OBJSTORE_REST_PORT": "9443"}},
		{name: "flags", args: []string{"-rest-host", "::1", "-rest-port", "9443"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			for k, v := range c.env {
				t.Setenv(k, v)
			}
			args := c.args
			if c.yaml != "" {
				args = append([]string{"-config", writeConfig(t, c.yaml)}, args...)
			}
			cfg := mustLoad(t, args, Defaults(), BindServerFlags)
			if got := cfg.RESTAddr(); got != "[::1]:9443" {
				t.Errorf("RESTAddr() = %q, want [::1]:9443", got)
			}
			srv, err := restserver.NewServer(nil, cfg.RESTServerConfig(cfg.Middleware()))
			if err != nil {
				t.Fatalf("NewServer: %v", err)
			}
			if srv.Address() != "[::1]:9443" {
				t.Errorf("listener address = %q, want [::1]:9443", srv.Address())
			}
			addr, err := net.ResolveTCPAddr("tcp", srv.Address())
			if err != nil {
				t.Fatalf("listener address %q is not a TCP address: %v", srv.Address(), err)
			}
			if !addr.IP.Equal(net.IPv6loopback) || addr.Port != 9443 {
				t.Errorf("listener address resolves to %v, want [::1]:9443", addr)
			}
		})
	}
}
