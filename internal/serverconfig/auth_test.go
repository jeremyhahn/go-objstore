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
	"context"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"os"
	"slices"
	"strconv"
	"testing"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	grpcserver "github.com/jeremyhahn/go-objstore/pkg/server/grpc"
	mcpserver "github.com/jeremyhahn/go-objstore/pkg/server/mcp"
)

// exampleAuth is the backup/prune example the docs ship.
func exampleAuth() AuthConfig {
	return AuthConfig{
		ClientCA: "/etc/objstore/client-ca.pem",
		Roles: map[string][]string{
			"backup": {adapters.ActionRead, adapters.ActionWrite, adapters.ActionList},
			"prune":  {adapters.ActionDelete},
		},
		Identities: []IdentityConfig{
			{Subject: "backup-agent", Roles: []string{"backup"}},
			{Subject: "prune-agent", Roles: []string{"prune"}},
			{Subject: "uid:1000", Roles: []string{"backup", "prune"}},
		},
	}
}

// authConfig is Defaults() with auth set and TLS on every network listener.
func authConfig() Config {
	c := Defaults()
	c.Auth = exampleAuth()
	c.GRPC.TLSCert, c.GRPC.TLSKey = "grpc.crt", "grpc.key"
	c.REST.TLSCert, c.REST.TLSKey = "rest.crt", "rest.key"
	c.QUIC.TLSCert, c.QUIC.TLSKey = "quic.crt", "quic.key"
	c.MCP.Mode = string(mcpserver.ModeHTTP)
	c.MCP.TLSCert, c.MCP.TLSKey = "mcp.crt", "mcp.key"
	return c
}

func TestValidate_AuthValid(t *testing.T) {
	c := authConfig()
	if err := c.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestValidate_NoAuthKeepsDefaults(t *testing.T) {
	c := Defaults()
	if c.Auth.Enabled() {
		t.Fatal("Defaults() enables auth")
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if c.Authorizer() != nil || c.CertAuthenticator() != nil {
		t.Error("without auth the servers must keep their NoOp defaults")
	}
	if c.listenerTLS("", "") != nil {
		t.Error("no certificate must mean no TLS")
	}
}

func TestValidate_AuthProblems(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
		want   []string
	}{
		{
			name: "network listeners without TLS",
			mutate: func(c *Config) {
				c.GRPC.TLSCert, c.GRPC.TLSKey = "", ""
				c.REST.TLSCert, c.REST.TLSKey = "", ""
				c.QUIC.TLSCert, c.QUIC.TLSKey = "", ""
				c.MCP.TLSCert, c.MCP.TLSKey = "", ""
			},
			want: []string{"grpc.tls_cert/grpc.tls_key", "rest.tls_cert/rest.tls_key", "quic.tls_cert/quic.tls_key", "mcp.tls_cert/mcp.tls_key"},
		},
		{
			name:   "disabled listener needs no TLS",
			mutate: func(c *Config) { c.REST.Enabled = false; c.REST.TLSCert, c.REST.TLSKey = "", "" },
		},
		{
			name:   "mcp stdio needs no TLS",
			mutate: func(c *Config) { c.MCP.Mode = string(mcpserver.ModeStdio); c.MCP.TLSCert, c.MCP.TLSKey = "", "" },
		},
		{
			name:   "missing client CA",
			mutate: func(c *Config) { c.Auth.ClientCA = "" },
			want:   []string{"auth.client_ca"},
		},
		{
			name: "unix only needs no client CA",
			mutate: func(c *Config) {
				c.GRPC.Enabled, c.REST.Enabled, c.QUIC.Enabled, c.MCP.Enabled = false, false, false, false
				c.Unix.Enabled = true
				c.Auth.ClientCA = ""
			},
		},
		{
			name:   "unknown role in identities",
			mutate: func(c *Config) { c.Auth.Identities[0].Roles = []string{"bakup"} },
			want:   []string{"auth.identities[0].roles"},
		},
		{
			name:   "unknown action",
			mutate: func(c *Config) { c.Auth.Roles["prune"] = []string{"remove"} },
			want:   []string{"auth.roles.prune"},
		},
		{
			name:   "role without actions",
			mutate: func(c *Config) { c.Auth.Roles["prune"] = nil },
			want:   []string{"auth.roles.prune"},
		},
		{
			name:   "no roles",
			mutate: func(c *Config) { c.Auth.Roles = nil; c.Auth.Identities = nil },
			want:   []string{"auth.roles"},
		},
		{
			name:   "bad uid",
			mutate: func(c *Config) { c.Auth.Identities[2].Subject = "uid:alice" },
			want:   []string{"auth.identities[2].subject"},
		},
		{
			name:   "duplicate and empty subjects",
			mutate: func(c *Config) { c.Auth.Identities[1].Subject = "backup-agent"; c.Auth.Identities[2].Subject = "" },
			want:   []string{"auth.identities[1].subject", "auth.identities[2].subject"},
		},
		{
			name:   "identity without roles",
			mutate: func(c *Config) { c.Auth.Identities[0].Roles = nil },
			want:   []string{"auth.identities[0].roles"},
		},
		{
			name:   "half a REST pair without auth",
			mutate: func(c *Config) { c.Auth = AuthConfig{}; c.REST.TLSKey = "" },
			want:   []string{"rest.tls_cert/rest.tls_key"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := authConfig()
			c.Auth = exampleAuth() // fresh map and slice per case
			tt.mutate(&c)
			got := invalidKeys(c.Validate())
			if !slices.Equal(got, tt.want) {
				t.Errorf("invalid keys = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestLoad_AuthSection(t *testing.T) {
	// A certificate subject keeps its case and dots: identities is a list,
	// not a map keyed by subject.
	file := writeConfig(t, `
grpc: {enabled: false}
rest: {enabled: false}
quic: {enabled: false}
mcp: {enabled: false}
unix: {enabled: true}
auth:
  client_ca: /etc/objstore/client-ca.pem
  roles:
    backup: [read, write, list]
    prune: [delete]
  identities:
    - subject: Backup.Agent.example.com
      roles: [backup]
    - subject: "uid:1000"
      roles: [prune]
`)
	cfg := mustLoad(t, []string{"-config", file}, Defaults(), BindServerFlags)
	if cfg.Auth.ClientCA != "/etc/objstore/client-ca.pem" {
		t.Errorf("client_ca = %q", cfg.Auth.ClientCA)
	}
	if got := cfg.Auth.Roles["backup"]; !slices.Equal(got, []string{"read", "write", "list"}) {
		t.Errorf("roles.backup = %q", got)
	}
	want := []IdentityConfig{
		{Subject: "Backup.Agent.example.com", Roles: []string{"backup"}},
		{Subject: "uid:1000", Roles: []string{"prune"}},
	}
	if len(cfg.Auth.Identities) != len(want) {
		t.Fatalf("identities = %+v", cfg.Auth.Identities)
	}
	for i := range want {
		if cfg.Auth.Identities[i].Subject != want[i].Subject || !slices.Equal(cfg.Auth.Identities[i].Roles, want[i].Roles) {
			t.Errorf("identities[%d] = %+v, want %+v", i, cfg.Auth.Identities[i], want[i])
		}
	}
}

func certState(cn string) *tls.ConnectionState {
	cert := &x509.Certificate{Subject: pkix.Name{CommonName: cn}}
	return &tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}, VerifiedChains: [][]*x509.Certificate{{cert}}}
}

func TestCertAuthenticator_RolesFromIdentities(t *testing.T) {
	c := authConfig()
	authn, authz := c.CertAuthenticator(), c.Authorizer()
	ctx := context.Background()

	check := func(cn, action string, allowed bool) {
		t.Helper()
		p, err := authn.AuthenticateMTLS(ctx, certState(cn))
		if err != nil {
			t.Fatalf("AuthenticateMTLS(%q): %v", cn, err)
		}
		if p.ID != cn {
			t.Errorf("principal ID = %q, want %q", p.ID, cn)
		}
		err = authz.Authorize(ctx, p, action, "object")
		if (err == nil) != allowed {
			t.Errorf("%s %s: err = %v, want allowed=%v", cn, action, err, allowed)
		}
	}
	for _, a := range []string{adapters.ActionRead, adapters.ActionWrite, adapters.ActionList} {
		check("backup-agent", a, true)
		check("prune-agent", a, false)
		check("stranger", a, false)
	}
	check("backup-agent", adapters.ActionDelete, false)
	check("prune-agent", adapters.ActionDelete, true)
	check("stranger", adapters.ActionDelete, false)
	check("backup-agent", adapters.ActionAdmin, false)
	check("uid:1000", adapters.ActionRead, false) // a certificate cannot claim a local uid

	if _, err := authn.AuthenticateMTLS(ctx, &tls.ConnectionState{}); err == nil {
		t.Error("no certificate must be refused")
	}
	if _, err := authn.AuthenticateMTLS(ctx, certState("")); err == nil {
		t.Error("a certificate without a common name must be refused")
	}
}

func TestListenerTLS_MutualWhenAuthSet(t *testing.T) {
	c := authConfig()
	got := c.listenerTLS("s.crt", "s.key")
	if got.Mode != adapters.TLSModeMutual || got.ClientAuth != tls.RequireAndVerifyClientCert ||
		got.ClientCAFile != c.Auth.ClientCA || got.ServerCertFile != "s.crt" || got.ServerKeyFile != "s.key" {
		t.Errorf("listenerTLS = %+v", got)
	}
	c.Auth = AuthConfig{}
	if got := c.listenerTLS("s.crt", "s.key"); got.Mode != adapters.TLSModeServer || got.ClientCAFile != "" {
		t.Errorf("listenerTLS without auth = %+v", got)
	}
}

func TestGRPCAndQUICOptions_WireAuth(t *testing.T) {
	c := authConfig()
	opts, err := c.GRPCOptions(c.Middleware())
	if err != nil {
		t.Fatalf("GRPCOptions: %v", err)
	}
	g := grpcserver.DefaultServerOptions()
	for _, o := range opts {
		o(g)
	}
	if g.AdapterTLSConfig == nil || g.AdapterTLSConfig.Mode != adapters.TLSModeMutual {
		t.Errorf("grpc TLS = %+v, want mutual", g.AdapterTLSConfig)
	}
	if _, ok := g.Authenticator.(*adapters.MTLSAuthenticator); !ok {
		t.Errorf("grpc authenticator = %T", g.Authenticator)
	}
	if _, ok := g.Authorizer.(*adapters.RBACAuthorizer); !ok {
		t.Errorf("grpc authorizer = %T", g.Authorizer)
	}

	q := c.QUICOptions(c.Middleware())
	if q.AdapterTLSConfig == nil || q.AdapterTLSConfig.Mode != adapters.TLSModeMutual || q.AdapterTLSConfig.MinVersion != tls.VersionTLS13 {
		t.Errorf("quic TLS = %+v, want mutual TLS 1.3", q.AdapterTLSConfig)
	}
	if _, ok := q.Authenticator.(*adapters.MTLSAuthenticator); !ok {
		t.Errorf("quic authenticator = %T", q.Authenticator)
	}
	if _, ok := q.Authorizer.(*adapters.RBACAuthorizer); !ok {
		t.Errorf("quic authorizer = %T", q.Authorizer)
	}

	// Without auth the defaults stay NoOp and no adapter TLS is forced.
	d := Defaults()
	if q := d.QUICOptions(d.Middleware()); q.AdapterTLSConfig != nil {
		t.Errorf("quic without auth: adapter TLS = %+v", q.AdapterTLSConfig)
	}
}

func TestRESTAndMCPHTTP_WireAuth(t *testing.T) {
	c := authConfig()
	r := c.RESTServerConfig(c.Middleware())
	if r.TLSConfig == nil || r.TLSConfig.Mode != adapters.TLSModeMutual || r.TLSConfig.ServerCertFile != "rest.crt" {
		t.Errorf("rest TLS = %+v, want mutual", r.TLSConfig)
	}
	if _, ok := r.Authenticator.(*adapters.MTLSAuthenticator); !ok {
		t.Errorf("rest authenticator = %T", r.Authenticator)
	}
	if _, ok := r.Authorizer.(*adapters.RBACAuthorizer); !ok {
		t.Errorf("rest authorizer = %T", r.Authorizer)
	}

	m, err := c.MCPServerConfig(c.Middleware())
	if err != nil {
		t.Fatalf("MCPServerConfig: %v", err)
	}
	if m.TLSConfig == nil || m.TLSConfig.Mode != adapters.TLSModeMutual || m.TLSConfig.ServerCertFile != "mcp.crt" {
		t.Errorf("mcp TLS = %+v, want mutual", m.TLSConfig)
	}
	if _, ok := m.Authenticator.(*adapters.MTLSAuthenticator); !ok {
		t.Errorf("mcp authenticator = %T", m.Authenticator)
	}
	if _, ok := m.Authorizer.(*adapters.RBACAuthorizer); !ok {
		t.Errorf("mcp authorizer = %T", m.Authorizer)
	}

	// REST TLS without auth is server TLS with the NoOp defaults.
	d := Defaults()
	d.REST.TLSCert, d.REST.TLSKey = "rest.crt", "rest.key"
	r = d.RESTServerConfig(d.Middleware())
	if r.TLSConfig == nil || r.TLSConfig.Mode != adapters.TLSModeServer {
		t.Errorf("rest TLS without auth = %+v", r.TLSConfig)
	}
	if _, ok := r.Authorizer.(*adapters.RBACAuthorizer); ok {
		t.Error("rest without auth must keep the NoOp authorizer")
	}
}

func TestUnixAndMCPStdio_AuthorizeByUID(t *testing.T) {
	c := authConfig()
	c.Auth.Identities[2].Subject = "uid:" + strconv.Itoa(os.Getuid())
	c.Auth.Identities[2].Roles = []string{"backup"}
	ctx := context.Background()

	u := c.UnixServerConfig(c.Middleware())
	if u.UsePeerCredentials == nil || !*u.UsePeerCredentials {
		t.Error("unix must use peer credentials when auth is set")
	}
	peer := func(id string, roles ...string) *adapters.Principal {
		return &adapters.Principal{ID: id, Type: "unix-peer", Roles: roles}
	}
	if err := u.Authorizer.Authorize(ctx, peer("uid:"+strconv.Itoa(os.Getuid())), adapters.ActionWrite, "object"); err != nil {
		t.Errorf("mapped uid write: %v", err)
	}
	if err := u.Authorizer.Authorize(ctx, peer("uid:"+strconv.Itoa(os.Getuid())), adapters.ActionDelete, "object"); err == nil {
		t.Error("mapped backup uid must not delete")
	}
	// An unmapped uid is refused even when its OS group is named like a role.
	if err := u.Authorizer.Authorize(ctx, peer("uid:4242", "backup"), adapters.ActionRead, "object"); err == nil {
		t.Error("unmapped uid must be refused")
	}
	if err := u.Authorizer.Authorize(ctx, nil, adapters.ActionRead, "object"); err == nil {
		t.Error("no principal must be refused")
	}

	c.MCP.Mode = string(mcpserver.ModeStdio)
	m, err := c.MCPServerConfig(c.Middleware())
	if err != nil {
		t.Fatalf("MCPServerConfig: %v", err)
	}
	if !m.EnforceStdioAuthz || m.TLSConfig != nil {
		t.Errorf("mcp stdio: EnforceStdioAuthz=%v TLS=%+v", m.EnforceStdioAuthz, m.TLSConfig)
	}
	// stdio authorizes the process owner whatever principal it is handed.
	if err := m.Authorizer.Authorize(ctx, &adapters.Principal{ID: "anonymous"}, adapters.ActionList, "object"); err != nil {
		t.Errorf("process owner list: %v", err)
	}
	if err := m.Authorizer.Authorize(ctx, &adapters.Principal{ID: "anonymous"}, adapters.ActionDelete, "object"); err == nil {
		t.Error("process owner with backup must not delete")
	}
	c.Auth.Identities = c.Auth.Identities[:2]
	m, _ = c.MCPServerConfig(c.Middleware())
	if err := m.Authorizer.Authorize(ctx, &adapters.Principal{ID: "anonymous"}, adapters.ActionList, "object"); err == nil {
		t.Error("an unmapped process owner must be refused on stdio")
	}
}
