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

package adapters

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/http"

	"google.golang.org/grpc/metadata"
)

var (
	// ErrUnauthorized is returned when authentication fails.
	ErrUnauthorized = errors.New("unauthorized")

	// ErrInvalidCredentials is returned when credentials are invalid.
	ErrInvalidCredentials = errors.New("invalid credentials")

	// ErrMissingCredentials is returned when required credentials are missing.
	ErrMissingCredentials = errors.New("missing credentials")

	// ErrInsufficientPermissions is returned when the authenticated principal lacks required permissions.
	ErrInsufficientPermissions = errors.New("insufficient permissions")

	// ErrMTLSNotSupported is returned when mTLS auth is not supported by authenticator.
	ErrMTLSNotSupported = errors.New("mTLS authentication not supported")

	// ErrMTLSRequiresPeer is returned when mTLS auth requires peer.Peer but wasn't provided.
	ErrMTLSRequiresPeer = errors.New("mTLS requires peer.Peer")
)

// Principal represents an authenticated entity (user, service, etc.).
type Principal struct {
	// ID is the unique identifier for this principal.
	ID string

	// Name is the human-readable name.
	Name string

	// Type indicates the principal type (e.g., "user", "service", "system").
	Type string

	// Roles contains the roles assigned to this principal.
	Roles []string

	// Attributes contains additional custom attributes.
	Attributes map[string]any
}

// HasRole checks if the principal has the specified role.
func (p *Principal) HasRole(role string) bool {
	for _, r := range p.Roles {
		if r == role {
			return true
		}
	}
	return false
}

// Authenticator defines the interface for pluggable authentication implementations.
// Applications can implement this interface to integrate their native authentication
// mechanisms (e.g., OAuth, JWT, API keys, mTLS, custom).
type Authenticator interface {
	// AuthenticateHTTP authenticates an HTTP request and returns the authenticated principal.
	// Returns ErrUnauthorized if authentication fails.
	AuthenticateHTTP(ctx context.Context, req *http.Request) (*Principal, error)

	// AuthenticateGRPC authenticates a gRPC request using metadata and returns the authenticated principal.
	// Returns ErrUnauthorized if authentication fails.
	AuthenticateGRPC(ctx context.Context, md metadata.MD) (*Principal, error)

	// AuthenticateMTLS authenticates using mTLS certificate information.
	// Returns ErrUnauthorized if authentication fails.
	AuthenticateMTLS(ctx context.Context, state *tls.ConnectionState) (*Principal, error)

	// ValidatePermission checks if the principal has permission to perform the specified action on the resource.
	// Returns ErrInsufficientPermissions if the principal lacks the required permission.
	ValidatePermission(ctx context.Context, principal *Principal, resource, action string) error
}

// NoOpAuthenticator is an authenticator that allows all requests (no authentication).
// Useful for development or when authentication is handled externally.
type NoOpAuthenticator struct{}

// NewNoOpAuthenticator creates a new no-op authenticator.
func NewNoOpAuthenticator() *NoOpAuthenticator {
	return &NoOpAuthenticator{}
}

// AuthenticateHTTP allows all HTTP requests.
func (a *NoOpAuthenticator) AuthenticateHTTP(ctx context.Context, req *http.Request) (*Principal, error) {
	return &Principal{
		ID:   "anonymous",
		Name: "Anonymous",
		Type: "anonymous",
	}, nil
}

// AuthenticateGRPC allows all gRPC requests.
func (a *NoOpAuthenticator) AuthenticateGRPC(ctx context.Context, md metadata.MD) (*Principal, error) {
	return &Principal{
		ID:   "anonymous",
		Name: "Anonymous",
		Type: "anonymous",
	}, nil
}

// AuthenticateMTLS allows all mTLS connections.
func (a *NoOpAuthenticator) AuthenticateMTLS(ctx context.Context, state *tls.ConnectionState) (*Principal, error) {
	return &Principal{
		ID:   "anonymous",
		Name: "Anonymous",
		Type: "anonymous",
	}, nil
}

// ValidatePermission allows all operations.
func (a *NoOpAuthenticator) ValidatePermission(ctx context.Context, principal *Principal, resource, action string) error {
	return nil
}

// BearerTokenAuthenticator is a simple token-based authenticator.
// Useful for API key or JWT validation.
type BearerTokenAuthenticator struct {
	// ValidateToken is a function that validates a token and returns a principal.
	ValidateToken func(ctx context.Context, token string) (*Principal, error)
}

// NewBearerTokenAuthenticator creates a new bearer token authenticator.
func NewBearerTokenAuthenticator(validateFunc func(ctx context.Context, token string) (*Principal, error)) *BearerTokenAuthenticator {
	return &BearerTokenAuthenticator{
		ValidateToken: validateFunc,
	}
}

// AuthenticateHTTP authenticates using the Authorization header.
func (a *BearerTokenAuthenticator) AuthenticateHTTP(ctx context.Context, req *http.Request) (*Principal, error) {
	token := req.Header.Get("Authorization")
	if token == "" {
		return nil, ErrMissingCredentials
	}

	// Strip "Bearer " prefix if present
	if len(token) > 7 && token[:7] == "Bearer " {
		token = token[7:]
	}

	return a.ValidateToken(ctx, token)
}

// AuthenticateGRPC authenticates using gRPC metadata.
func (a *BearerTokenAuthenticator) AuthenticateGRPC(ctx context.Context, md metadata.MD) (*Principal, error) {
	tokens := md.Get("authorization")
	if len(tokens) == 0 {
		return nil, ErrMissingCredentials
	}

	token := tokens[0]
	// Strip "Bearer " prefix if present
	if len(token) > 7 && token[:7] == "Bearer " {
		token = token[7:]
	}

	return a.ValidateToken(ctx, token)
}

// AuthenticateMTLS is not supported for bearer token auth.
func (a *BearerTokenAuthenticator) AuthenticateMTLS(ctx context.Context, state *tls.ConnectionState) (*Principal, error) {
	return nil, ErrMTLSNotSupported
}

// ValidatePermission performs basic role-based access control.
func (a *BearerTokenAuthenticator) ValidatePermission(ctx context.Context, principal *Principal, resource, action string) error {
	// Basic implementation - can be extended by wrapping this authenticator
	if principal.HasRole("admin") {
		return nil
	}
	return nil
}

// MTLSAuthenticator authenticates using mTLS certificates.
type MTLSAuthenticator struct {
	// ExtractPrincipal extracts principal information from a client certificate.
	ExtractPrincipal func(ctx context.Context, cert *x509.Certificate) (*Principal, error)

	// RequiredRoots is the CA certificate pool for validating client certificates.
	RequiredRoots *x509.CertPool
}

// NewMTLSAuthenticator creates a new mTLS authenticator.
func NewMTLSAuthenticator(extractFunc func(ctx context.Context, cert *x509.Certificate) (*Principal, error), rootPool *x509.CertPool) *MTLSAuthenticator {
	return &MTLSAuthenticator{
		ExtractPrincipal: extractFunc,
		RequiredRoots:    rootPool,
	}
}

// AuthenticateHTTP authenticates using TLS client certificates from HTTP request.
func (a *MTLSAuthenticator) AuthenticateHTTP(ctx context.Context, req *http.Request) (*Principal, error) {
	if req.TLS == nil || len(req.TLS.PeerCertificates) == 0 {
		return nil, ErrMissingCredentials
	}

	return a.ExtractPrincipal(ctx, req.TLS.PeerCertificates[0])
}

// AuthenticateGRPC authenticates using TLS client certificates from gRPC context.
func (a *MTLSAuthenticator) AuthenticateGRPC(ctx context.Context, md metadata.MD) (*Principal, error) {
	// gRPC mTLS authentication typically happens at the transport layer
	// The certificate info should be in the context's peer info
	return nil, ErrMTLSRequiresPeer
}

// AuthenticateMTLS authenticates using TLS connection state.
func (a *MTLSAuthenticator) AuthenticateMTLS(ctx context.Context, state *tls.ConnectionState) (*Principal, error) {
	if len(state.PeerCertificates) == 0 {
		return nil, ErrMissingCredentials
	}

	return a.ExtractPrincipal(ctx, state.PeerCertificates[0])
}

// ValidatePermission performs basic validation.
func (a *MTLSAuthenticator) ValidatePermission(ctx context.Context, principal *Principal, resource, action string) error {
	// Basic implementation - can be extended
	return nil
}

// CompositeAuthenticator allows multiple authentication methods to be tried in order.
type CompositeAuthenticator struct {
	authenticators []Authenticator
}

// NewCompositeAuthenticator creates a new composite authenticator.
func NewCompositeAuthenticator(authenticators ...Authenticator) *CompositeAuthenticator {
	return &CompositeAuthenticator{
		authenticators: authenticators,
	}
}

// AuthenticateHTTP tries each authenticator in order until one succeeds.
func (a *CompositeAuthenticator) AuthenticateHTTP(ctx context.Context, req *http.Request) (*Principal, error) {
	var lastErr error
	for _, auth := range a.authenticators {
		principal, err := auth.AuthenticateHTTP(ctx, req)
		if err == nil {
			return principal, nil
		}
		lastErr = err
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, ErrUnauthorized
}

// AuthenticateGRPC tries each authenticator in order until one succeeds.
func (a *CompositeAuthenticator) AuthenticateGRPC(ctx context.Context, md metadata.MD) (*Principal, error) {
	var lastErr error
	for _, auth := range a.authenticators {
		principal, err := auth.AuthenticateGRPC(ctx, md)
		if err == nil {
			return principal, nil
		}
		lastErr = err
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, ErrUnauthorized
}

// AuthenticateMTLS tries each authenticator in order until one succeeds.
func (a *CompositeAuthenticator) AuthenticateMTLS(ctx context.Context, state *tls.ConnectionState) (*Principal, error) {
	var lastErr error
	for _, auth := range a.authenticators {
		principal, err := auth.AuthenticateMTLS(ctx, state)
		if err == nil {
			return principal, nil
		}
		lastErr = err
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, ErrUnauthorized
}

// ValidatePermission uses the first authenticator for permission validation.
func (a *CompositeAuthenticator) ValidatePermission(ctx context.Context, principal *Principal, resource, action string) error {
	if len(a.authenticators) > 0 {
		return a.authenticators[0].ValidatePermission(ctx, principal, resource, action)
	}
	return nil
}
