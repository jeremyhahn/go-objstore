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
	"sort"
	"strconv"
	"strings"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	mcpserver "github.com/jeremyhahn/go-objstore/pkg/server/mcp"
)

// UIDSubjectPrefix marks an identity subject that names a local user by
// numeric uid (for example "uid:1000"). The Unix socket and MCP stdio
// listeners identify their peer this way; the TLS listeners identify a
// client by the common name of its verified certificate.
const UIDSubjectPrefix = "uid:"

// AnonymousSubject is the principal ID a transport gives a peer it cannot
// identify; an identity with this subject is refused.
const AnonymousSubject = "anonymous"

// wildcardAction grants every action (the RBACAuthorizer convention).
const wildcardAction = "*"

// AuthConfig is the `auth` section. When it is set (any field non-empty),
// every enabled listener authenticates its clients and authorizes each
// request through an RBAC authorizer built from Roles; when it is empty the
// servers keep their NoOp (allow-all) defaults.
type AuthConfig struct {
	// ClientCA is the PEM file of the CA that signs client certificates.
	// The gRPC, QUIC, REST and MCP HTTP listeners require and verify a
	// client certificate against it.
	ClientCA string `mapstructure:"client_ca"`
	// Roles maps a role name to the actions it permits ("read", "write",
	// "delete", "list", "admin", or "*" for all).
	Roles map[string][]string `mapstructure:"roles"`
	// Identities maps a certificate common name, or "uid:N" for a local
	// user, to its roles. A client that matches no identity has no roles
	// and every request it makes is refused.
	Identities []IdentityConfig `mapstructure:"identities"`
}

// IdentityConfig binds one client identity to its roles. It is a list
// entry rather than a map key because configuration keys are
// case-folded and split on dots, which would corrupt a certificate subject.
type IdentityConfig struct {
	// Subject is the certificate common name, or "uid:N".
	Subject string `mapstructure:"subject"`
	// Roles lists role names defined in auth.roles.
	Roles []string `mapstructure:"roles"`
}

// Enabled reports whether the auth section is set.
func (a AuthConfig) Enabled() bool {
	return a.ClientCA != "" || len(a.Roles) > 0 || len(a.Identities) > 0
}

// knownActions are the action names the servers pass to Authorize.
var knownActions = map[string]bool{
	adapters.ActionRead:   true,
	adapters.ActionWrite:  true,
	adapters.ActionDelete: true,
	adapters.ActionList:   true,
	adapters.ActionAdmin:  true,
	wildcardAction:        true,
}

// tlsListener is one enabled TCP (or QUIC) listener and its TLS key pair.
type tlsListener struct {
	section   string
	cert, key string
}

// tlsListeners returns the enabled listeners that serve over the network.
func (c *Config) tlsListeners() []tlsListener {
	var out []tlsListener
	if c.GRPC.Enabled {
		out = append(out, tlsListener{"grpc", c.GRPC.TLSCert, c.GRPC.TLSKey})
	}
	if c.REST.Enabled {
		out = append(out, tlsListener{"rest", c.REST.TLSCert, c.REST.TLSKey})
	}
	if c.QUIC.Enabled {
		out = append(out, tlsListener{"quic", c.QUIC.TLSCert, c.QUIC.TLSKey})
	}
	if c.MCP.Enabled && c.MCP.Mode == string(mcpserver.ModeHTTP) {
		out = append(out, tlsListener{"mcp", c.MCP.TLSCert, c.MCP.TLSKey})
	}
	return out
}

// validateAuth checks the REST and MCP TLS pairs and, when auth is set, the
// auth section and that every enabled network listener can do mutual TLS.
func (c *Config) validateAuth() []error {
	var errs []error
	for _, pair := range []tlsListener{
		{"rest", c.REST.TLSCert, c.REST.TLSKey},
		{"mcp", c.MCP.TLSCert, c.MCP.TLSKey},
	} {
		if (pair.cert == "") != (pair.key == "") {
			errs = append(errs, &InvalidSettingError{
				Key:    pair.section + ".tls_cert/" + pair.section + ".tls_key",
				Value:  pair.cert + "/" + pair.key,
				Reason: "certificate and key must be set together",
			})
		}
	}
	if !c.Auth.Enabled() {
		return errs
	}
	listeners := c.tlsListeners()
	for _, l := range listeners {
		if l.cert == "" || l.key == "" {
			errs = append(errs, &InvalidSettingError{
				Key:    l.section + ".tls_cert/" + l.section + ".tls_key",
				Value:  l.cert + "/" + l.key,
				Reason: "must be set when auth is set: the listener requires client certificates",
			})
		}
	}
	if len(listeners) > 0 && c.Auth.ClientCA == "" {
		errs = append(errs, &InvalidSettingError{
			Key:    "auth.client_ca",
			Reason: "must be set when auth is set and a gRPC, REST, QUIC or MCP HTTP listener is enabled",
		})
	}
	if len(c.Auth.Roles) == 0 {
		errs = append(errs, &InvalidSettingError{Key: "auth.roles", Reason: "must define at least one role"})
	}
	for _, role := range sortedKeys(c.Auth.Roles) {
		actions := c.Auth.Roles[role]
		if len(actions) == 0 {
			errs = append(errs, &InvalidSettingError{Key: "auth.roles." + role, Reason: "must list at least one action"})
		}
		for _, a := range actions {
			if !knownActions[a] {
				errs = append(errs, &InvalidSettingError{
					Key: "auth.roles." + role, Value: a,
					Reason: "unknown action: must be one of " + joinQuoted(adapters.ActionRead, adapters.ActionWrite,
						adapters.ActionDelete, adapters.ActionList, adapters.ActionAdmin, wildcardAction),
				})
			}
		}
	}
	seen := make(map[string]bool, len(c.Auth.Identities))
	for i, id := range c.Auth.Identities {
		key := "auth.identities[" + strconv.Itoa(i) + "]"
		switch {
		case id.Subject == "":
			errs = append(errs, &InvalidSettingError{Key: key + ".subject", Reason: "must not be empty"})
		case id.Subject == AnonymousSubject:
			// A transport that cannot identify its peer (e.g. a Unix socket
			// peer whose credentials cannot be read) yields this principal;
			// granting it roles would grant them to unidentified clients.
			errs = append(errs, &InvalidSettingError{Key: key + ".subject", Value: id.Subject, Reason: "is the unauthenticated principal and cannot hold roles"})
		case seen[id.Subject]:
			errs = append(errs, &InvalidSettingError{Key: key + ".subject", Value: id.Subject, Reason: "duplicate subject"})
		}
		seen[id.Subject] = true
		if uid, ok := strings.CutPrefix(id.Subject, UIDSubjectPrefix); ok {
			if _, err := strconv.ParseUint(uid, 10, 32); err != nil {
				errs = append(errs, &InvalidSettingError{
					Key: key + ".subject", Value: id.Subject, Reason: "uid must be a non-negative integer", Err: err,
				})
			}
		}
		if len(id.Roles) == 0 {
			errs = append(errs, &InvalidSettingError{Key: key + ".roles", Reason: "must list at least one role"})
		}
		for _, r := range id.Roles {
			if _, ok := c.Auth.Roles[r]; !ok {
				errs = append(errs, &InvalidSettingError{Key: key + ".roles", Value: r, Reason: "unknown role: not defined in auth.roles"})
			}
		}
	}
	return errs
}

func sortedKeys(m map[string][]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// identityRoles returns a copy of the roles bound to subject, or nil.
func (a AuthConfig) identityRoles(subject string) []string {
	for _, id := range a.Identities {
		if id.Subject == subject {
			return append([]string(nil), id.Roles...)
		}
	}
	return nil
}

// Authorizer returns the RBAC authorizer built from auth.roles, or nil
// when auth is not set (the servers then keep their NoOp default).
func (c *Config) Authorizer() adapters.Authorizer {
	if !c.Auth.Enabled() {
		return nil
	}
	roles := make(map[string][]string, len(c.Auth.Roles))
	for r, actions := range c.Auth.Roles {
		roles[r] = append([]string(nil), actions...)
	}
	return adapters.NewRBACAuthorizer(roles)
}

// CertAuthenticator returns the authenticator of the TLS listeners, or nil
// when auth is not set. The TLS layer verifies the client certificate
// against auth.client_ca; the principal is the leaf certificate's common
// name with the roles auth.identities binds to it (none when unmapped).
func (c *Config) CertAuthenticator() adapters.Authenticator {
	if !c.Auth.Enabled() {
		return nil
	}
	auth := c.Auth
	return adapters.NewMTLSAuthenticator(func(_ context.Context, cert *x509.Certificate) (*adapters.Principal, error) {
		cn := cert.Subject.CommonName
		if cn == "" {
			return nil, adapters.ErrInvalidCredentials
		}
		var roles []string
		// A "uid:N" subject names a local user: a certificate cannot claim it.
		if !strings.HasPrefix(cn, UIDSubjectPrefix) {
			roles = auth.identityRoles(cn)
		}
		return &adapters.Principal{ID: cn, Name: cn, Type: "certificate", Roles: roles}, nil
	}, nil)
}

// identityAuthorizer authorizes by the roles auth.identities binds to the
// principal's ID ("uid:N" on the Unix socket), ignoring any roles the
// principal already carries (the Unix peer principal carries its OS group
// name, which must not grant a role of the same name). When subject is set
// it is used in place of the principal (MCP stdio: the process owner).
func (c *Config) identityAuthorizer(subject string) adapters.Authorizer {
	return &identityAuthorizer{inner: c.Authorizer(), auth: c.Auth, subject: subject}
}

type identityAuthorizer struct {
	inner   adapters.Authorizer
	auth    AuthConfig
	subject string
}

// Authorize implements adapters.Authorizer.
func (a *identityAuthorizer) Authorize(ctx context.Context, principal *adapters.Principal, action, resource string) error {
	id := a.subject
	if id == "" && principal != nil {
		id = principal.ID
	}
	mapped := &adapters.Principal{ID: id, Name: id, Type: "identity", Roles: a.auth.identityRoles(id)}
	return a.inner.Authorize(ctx, mapped, action, resource)
}

// listenerTLS returns the TLS configuration of a listener with the given
// key pair: nil when no certificate is set, server TLS without auth, and
// mutual TLS requiring a client certificate signed by auth.client_ca when
// auth is set.
func (c *Config) listenerTLS(cert, key string) *adapters.TLSConfig {
	if cert == "" || key == "" {
		return nil
	}
	t := adapters.NewTLSConfig()
	t.Mode = adapters.TLSModeServer
	t.ServerCertFile = cert
	t.ServerKeyFile = key
	if c.Auth.Enabled() {
		t.Mode = adapters.TLSModeMutual
		t.ClientCAFile = c.Auth.ClientCA
		t.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return t
}
