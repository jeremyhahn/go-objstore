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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net/http"
	"testing"
	"time"

	"google.golang.org/grpc/metadata"
)

func TestPrincipal_HasRole(t *testing.T) {
	principal := &Principal{
		ID:    "user1",
		Name:  "Test User",
		Type:  "user",
		Roles: []string{"admin", "user", "viewer"},
	}

	tests := []struct {
		role     string
		expected bool
	}{
		{"admin", true},
		{"user", true},
		{"viewer", true},
		{"editor", false},
		{"superadmin", false},
	}

	for _, tt := range tests {
		t.Run(tt.role, func(t *testing.T) {
			if got := principal.HasRole(tt.role); got != tt.expected {
				t.Errorf("HasRole(%s) = %v, want %v", tt.role, got, tt.expected)
			}
		})
	}
}

func TestNoOpAuthenticator(t *testing.T) {
	auth := NewNoOpAuthenticator()
	ctx := context.Background()

	t.Run("AuthenticateHTTP", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/test", nil)
		principal, err := auth.AuthenticateHTTP(ctx, req)
		if err != nil {
			t.Errorf("NoOpAuthenticator.AuthenticateHTTP() error = %v, want nil", err)
		}
		if principal.ID != "anonymous" {
			t.Errorf("NoOpAuthenticator principal ID = %s, want anonymous", principal.ID)
		}
	})

	t.Run("AuthenticateGRPC", func(t *testing.T) {
		md := metadata.MD{}
		principal, err := auth.AuthenticateGRPC(ctx, md)
		if err != nil {
			t.Errorf("NoOpAuthenticator.AuthenticateGRPC() error = %v, want nil", err)
		}
		if principal.ID != "anonymous" {
			t.Errorf("NoOpAuthenticator principal ID = %s, want anonymous", principal.ID)
		}
	})

	t.Run("AuthenticateMTLS", func(t *testing.T) {
		state := &tls.ConnectionState{}
		principal, err := auth.AuthenticateMTLS(ctx, state)
		if err != nil {
			t.Errorf("NoOpAuthenticator.AuthenticateMTLS() error = %v, want nil", err)
		}
		if principal.ID != "anonymous" {
			t.Errorf("NoOpAuthenticator principal ID = %s, want anonymous", principal.ID)
		}
	})

}

func TestBearerTokenAuthenticator_HTTP(t *testing.T) {
	validateFunc := func(ctx context.Context, token string) (*Principal, error) {
		if token == "valid-token" {
			return &Principal{
				ID:   "user1",
				Name: "Test User",
				Type: "user",
			}, nil
		}
		return nil, ErrInvalidCredentials
	}

	auth := NewBearerTokenAuthenticator(validateFunc)
	ctx := context.Background()

	t.Run("Valid token", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/test", nil)
		req.Header.Set("Authorization", "Bearer valid-token")

		principal, err := auth.AuthenticateHTTP(ctx, req)
		if err != nil {
			t.Errorf("AuthenticateHTTP() error = %v, want nil", err)
		}
		if principal.ID != "user1" {
			t.Errorf("principal.ID = %s, want user1", principal.ID)
		}
	})

	t.Run("Valid token without Bearer prefix", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/test", nil)
		req.Header.Set("Authorization", "valid-token")

		principal, err := auth.AuthenticateHTTP(ctx, req)
		if err != nil {
			t.Errorf("AuthenticateHTTP() error = %v, want nil", err)
		}
		if principal.ID != "user1" {
			t.Errorf("principal.ID = %s, want user1", principal.ID)
		}
	})

	t.Run("Invalid token", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/test", nil)
		req.Header.Set("Authorization", "Bearer invalid-token")

		_, err := auth.AuthenticateHTTP(ctx, req)
		if !errors.Is(err, ErrInvalidCredentials) {
			t.Errorf("AuthenticateHTTP() error = %v, want ErrInvalidCredentials", err)
		}
	})

	t.Run("Missing token", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/test", nil)

		_, err := auth.AuthenticateHTTP(ctx, req)
		if !errors.Is(err, ErrMissingCredentials) {
			t.Errorf("AuthenticateHTTP() error = %v, want ErrMissingCredentials", err)
		}
	})
}

func TestBearerTokenAuthenticator_gRPC(t *testing.T) {
	validateFunc := func(ctx context.Context, token string) (*Principal, error) {
		if token == "valid-token" {
			return &Principal{
				ID:   "user1",
				Name: "Test User",
				Type: "user",
			}, nil
		}
		return nil, ErrInvalidCredentials
	}

	auth := NewBearerTokenAuthenticator(validateFunc)
	ctx := context.Background()

	t.Run("Valid token", func(t *testing.T) {
		md := metadata.New(map[string]string{
			"authorization": "Bearer valid-token",
		})

		principal, err := auth.AuthenticateGRPC(ctx, md)
		if err != nil {
			t.Errorf("AuthenticateGRPC() error = %v, want nil", err)
		}
		if principal.ID != "user1" {
			t.Errorf("principal.ID = %s, want user1", principal.ID)
		}
	})

	t.Run("Invalid token", func(t *testing.T) {
		md := metadata.New(map[string]string{
			"authorization": "Bearer invalid-token",
		})

		_, err := auth.AuthenticateGRPC(ctx, md)
		if !errors.Is(err, ErrInvalidCredentials) {
			t.Errorf("AuthenticateGRPC() error = %v, want ErrInvalidCredentials", err)
		}
	})

	t.Run("Missing token", func(t *testing.T) {
		md := metadata.MD{}

		_, err := auth.AuthenticateGRPC(ctx, md)
		if !errors.Is(err, ErrMissingCredentials) {
			t.Errorf("AuthenticateGRPC() error = %v, want ErrMissingCredentials", err)
		}
	})

	t.Run("AuthenticateMTLS returns error", func(t *testing.T) {
		_, err := auth.AuthenticateMTLS(ctx, nil)
		if err == nil {
			t.Error("AuthenticateMTLS() should return error for BearerTokenAuthenticator")
		}
	})

}

func TestMTLSAuthenticator(t *testing.T) {
	extractFunc := func(ctx context.Context, cert *x509.Certificate) (*Principal, error) {
		if cert.Subject.CommonName == "valid-cert" {
			return &Principal{
				ID:   "cert-user",
				Name: cert.Subject.CommonName,
				Type: "certificate",
			}, nil
		}
		return nil, ErrInvalidCredentials
	}

	auth := NewMTLSAuthenticator(extractFunc, nil)
	ctx := context.Background()

	t.Run("Valid certificate", func(t *testing.T) {
		cert := &x509.Certificate{
			Subject: pkix.Name{CommonName: "valid-cert"},
		}
		state := &tls.ConnectionState{
			PeerCertificates: []*x509.Certificate{cert},
		}

		principal, err := auth.AuthenticateMTLS(ctx, state)
		if err != nil {
			t.Errorf("AuthenticateMTLS() error = %v, want nil", err)
		}
		if principal.ID != "cert-user" {
			t.Errorf("principal.ID = %s, want cert-user", principal.ID)
		}
	})

	t.Run("Invalid certificate", func(t *testing.T) {
		cert := &x509.Certificate{
			Subject: pkix.Name{CommonName: "invalid-cert"},
		}
		state := &tls.ConnectionState{
			PeerCertificates: []*x509.Certificate{cert},
		}

		_, err := auth.AuthenticateMTLS(ctx, state)
		if !errors.Is(err, ErrInvalidCredentials) {
			t.Errorf("AuthenticateMTLS() error = %v, want ErrInvalidCredentials", err)
		}
	})

	t.Run("Missing certificate", func(t *testing.T) {
		state := &tls.ConnectionState{
			PeerCertificates: []*x509.Certificate{},
		}

		_, err := auth.AuthenticateMTLS(ctx, state)
		if !errors.Is(err, ErrMissingCredentials) {
			t.Errorf("AuthenticateMTLS() error = %v, want ErrMissingCredentials", err)
		}
	})

	t.Run("AuthenticateHTTP", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/test", nil)
		cert := &x509.Certificate{
			Subject: pkix.Name{CommonName: "valid-cert"},
		}
		req.TLS = &tls.ConnectionState{
			PeerCertificates: []*x509.Certificate{cert},
		}

		principal, err := auth.AuthenticateHTTP(ctx, req)
		if err != nil {
			t.Errorf("AuthenticateHTTP() error = %v, want nil", err)
		}
		if principal.ID != "cert-user" {
			t.Errorf("principal.ID = %s, want cert-user", principal.ID)
		}
	})

	t.Run("AuthenticateHTTP missing TLS", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/test", nil)
		req.TLS = nil

		_, err := auth.AuthenticateHTTP(ctx, req)
		if !errors.Is(err, ErrMissingCredentials) {
			t.Errorf("AuthenticateHTTP() error = %v, want ErrMissingCredentials", err)
		}
	})

	t.Run("AuthenticateHTTP empty certificates", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/test", nil)
		req.TLS = &tls.ConnectionState{
			PeerCertificates: []*x509.Certificate{},
		}

		_, err := auth.AuthenticateHTTP(ctx, req)
		if !errors.Is(err, ErrMissingCredentials) {
			t.Errorf("AuthenticateHTTP() error = %v, want ErrMissingCredentials", err)
		}
	})

	t.Run("AuthenticateGRPC returns error", func(t *testing.T) {
		md := metadata.MD{}
		_, err := auth.AuthenticateGRPC(ctx, md)
		if err == nil {
			t.Error("AuthenticateGRPC() should return error for MTLSAuthenticator")
		}
	})

}

func TestCompositeAuthenticator(t *testing.T) {
	tokenAuth := NewBearerTokenAuthenticator(func(ctx context.Context, token string) (*Principal, error) {
		if token == "valid-token" {
			return &Principal{ID: "token-user", Type: "token"}, nil
		}
		return nil, ErrInvalidCredentials
	})

	mtlsAuth := NewMTLSAuthenticator(func(ctx context.Context, cert *x509.Certificate) (*Principal, error) {
		if cert.Subject.CommonName == "valid-cert" {
			return &Principal{ID: "cert-user", Type: "certificate"}, nil
		}
		return nil, ErrInvalidCredentials
	}, nil)

	auth := NewCompositeAuthenticator(tokenAuth, mtlsAuth)
	ctx := context.Background()

	t.Run("Token authentication succeeds", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/test", nil)
		req.Header.Set("Authorization", "Bearer valid-token")

		principal, err := auth.AuthenticateHTTP(ctx, req)
		if err != nil {
			t.Errorf("AuthenticateHTTP() error = %v, want nil", err)
		}
		if principal.Type != "token" {
			t.Errorf("principal.Type = %s, want token", principal.Type)
		}
	})

	t.Run("mTLS authentication succeeds when token fails", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/test", nil)
		cert := &x509.Certificate{
			Subject: pkix.Name{CommonName: "valid-cert"},
		}
		req.TLS = &tls.ConnectionState{
			PeerCertificates: []*x509.Certificate{cert},
		}

		principal, err := auth.AuthenticateHTTP(ctx, req)
		if err != nil {
			t.Errorf("AuthenticateHTTP() error = %v, want nil", err)
		}
		if principal.Type != "certificate" {
			t.Errorf("principal.Type = %s, want certificate", principal.Type)
		}
	})

	t.Run("All authentication methods fail", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "/test", nil)

		_, err := auth.AuthenticateHTTP(ctx, req)
		if err == nil {
			t.Error("AuthenticateHTTP() error = nil, want error")
		}
	})

	t.Run("AuthenticateGRPC with valid token", func(t *testing.T) {
		md := metadata.MD{
			"authorization": []string{"Bearer valid-token"},
		}

		principal, err := auth.AuthenticateGRPC(ctx, md)
		if err != nil {
			t.Errorf("AuthenticateGRPC() error = %v, want nil", err)
		}
		if principal.Type != "token" {
			t.Errorf("principal.Type = %s, want token", principal.Type)
		}
	})

	t.Run("AuthenticateMTLS with valid certificate", func(t *testing.T) {
		cert := &x509.Certificate{
			Subject: pkix.Name{CommonName: "valid-cert"},
		}
		state := &tls.ConnectionState{
			PeerCertificates: []*x509.Certificate{cert},
		}

		principal, err := auth.AuthenticateMTLS(ctx, state)
		if err != nil {
			t.Errorf("AuthenticateMTLS() error = %v, want nil", err)
		}
		if principal.Type != "certificate" {
			t.Errorf("principal.Type = %s, want certificate", principal.Type)
		}
	})

	t.Run("AuthenticateGRPC all methods fail", func(t *testing.T) {
		md := metadata.MD{
			"authorization": []string{"Bearer invalid-token"},
		}

		_, err := auth.AuthenticateGRPC(ctx, md)
		if err == nil {
			t.Error("AuthenticateGRPC() error = nil, want error when all methods fail")
		}
	})

	t.Run("AuthenticateMTLS all methods fail", func(t *testing.T) {
		cert := &x509.Certificate{
			Subject: pkix.Name{CommonName: "invalid-cert"},
		}
		state := &tls.ConnectionState{
			PeerCertificates: []*x509.Certificate{cert},
		}

		_, err := auth.AuthenticateMTLS(ctx, state)
		if err == nil {
			t.Error("AuthenticateMTLS() error = nil, want error when all methods fail")
		}
	})

	t.Run("AuthenticateHTTP with no authenticators returns ErrUnauthorized", func(t *testing.T) {
		emptyAuth := NewCompositeAuthenticator()
		req, _ := http.NewRequest("GET", "/test", nil)

		_, err := emptyAuth.AuthenticateHTTP(ctx, req)
		if !errors.Is(err, ErrUnauthorized) {
			t.Errorf("AuthenticateHTTP() with no authenticators should return ErrUnauthorized, got %v", err)
		}
	})

	t.Run("AuthenticateGRPC with no authenticators returns ErrUnauthorized", func(t *testing.T) {
		emptyAuth := NewCompositeAuthenticator()
		md := metadata.MD{}

		_, err := emptyAuth.AuthenticateGRPC(ctx, md)
		if !errors.Is(err, ErrUnauthorized) {
			t.Errorf("AuthenticateGRPC() with no authenticators should return ErrUnauthorized, got %v", err)
		}
	})

	t.Run("AuthenticateMTLS with no authenticators returns ErrUnauthorized", func(t *testing.T) {
		emptyAuth := NewCompositeAuthenticator()
		state := &tls.ConnectionState{}

		_, err := emptyAuth.AuthenticateMTLS(ctx, state)
		if !errors.Is(err, ErrUnauthorized) {
			t.Errorf("AuthenticateMTLS() with no authenticators should return ErrUnauthorized, got %v", err)
		}
	})
}

func TestNoOpAuthorizer(t *testing.T) {
	authz := NewNoOpAuthorizer()
	ctx := context.Background()

	cases := []struct {
		name      string
		principal *Principal
		action    string
		resource  string
	}{
		{"nil principal", nil, ActionRead, ResourceObject},
		{"read object", &Principal{ID: "u"}, ActionRead, "obj-key"},
		{"write object", &Principal{ID: "u"}, ActionWrite, "obj-key"},
		{"admin policy", &Principal{ID: "u"}, ActionAdmin, ResourcePolicy},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := authz.Authorize(ctx, tc.principal, tc.action, tc.resource); err != nil {
				t.Errorf("NoOpAuthorizer.Authorize() = %v, want nil", err)
			}
		})
	}
}

func TestRBACAuthorizer(t *testing.T) {
	ctx := context.Background()
	authz := NewRBACAuthorizer(map[string][]string{
		"reader": {ActionRead, ActionList},
		"writer": {ActionWrite},
		"admin":  {wildcardPermission},
	})

	t.Run("nil principal is denied", func(t *testing.T) {
		if err := authz.Authorize(ctx, nil, ActionRead, ResourceObject); !errors.Is(err, ErrInsufficientPermissions) {
			t.Errorf("Authorize(nil) = %v, want ErrInsufficientPermissions", err)
		}
	})

	t.Run("role grants matching action", func(t *testing.T) {
		p := &Principal{ID: "r", Roles: []string{"reader"}}
		if err := authz.Authorize(ctx, p, ActionRead, "key"); err != nil {
			t.Errorf("reader read = %v, want nil", err)
		}
		if err := authz.Authorize(ctx, p, ActionList, ""); err != nil {
			t.Errorf("reader list = %v, want nil", err)
		}
	})

	t.Run("role denies non-granted action", func(t *testing.T) {
		p := &Principal{ID: "r", Roles: []string{"reader"}}
		if err := authz.Authorize(ctx, p, ActionWrite, "key"); !errors.Is(err, ErrInsufficientPermissions) {
			t.Errorf("reader write = %v, want ErrInsufficientPermissions", err)
		}
		if err := authz.Authorize(ctx, p, ActionDelete, "key"); !errors.Is(err, ErrInsufficientPermissions) {
			t.Errorf("reader delete = %v, want ErrInsufficientPermissions", err)
		}
	})

	t.Run("wildcard grants any action", func(t *testing.T) {
		p := &Principal{ID: "a", Roles: []string{"admin"}}
		for _, action := range []string{ActionRead, ActionWrite, ActionDelete, ActionList, ActionAdmin} {
			if err := authz.Authorize(ctx, p, action, ResourcePolicy); err != nil {
				t.Errorf("admin %s = %v, want nil", action, err)
			}
		}
	})

	t.Run("multi-role union of permissions", func(t *testing.T) {
		p := &Principal{ID: "rw", Roles: []string{"reader", "writer"}}
		if err := authz.Authorize(ctx, p, ActionRead, "key"); err != nil {
			t.Errorf("reader+writer read = %v, want nil", err)
		}
		if err := authz.Authorize(ctx, p, ActionWrite, "key"); err != nil {
			t.Errorf("reader+writer write = %v, want nil", err)
		}
		if err := authz.Authorize(ctx, p, ActionDelete, "key"); !errors.Is(err, ErrInsufficientPermissions) {
			t.Errorf("reader+writer delete = %v, want ErrInsufficientPermissions", err)
		}
	})

	t.Run("unknown role is denied", func(t *testing.T) {
		p := &Principal{ID: "x", Roles: []string{"nobody"}}
		if err := authz.Authorize(ctx, p, ActionRead, "key"); !errors.Is(err, ErrInsufficientPermissions) {
			t.Errorf("unknown role = %v, want ErrInsufficientPermissions", err)
		}
	})

	t.Run("no roles is denied", func(t *testing.T) {
		p := &Principal{ID: "x"}
		if err := authz.Authorize(ctx, p, ActionRead, "key"); !errors.Is(err, ErrInsufficientPermissions) {
			t.Errorf("no roles = %v, want ErrInsufficientPermissions", err)
		}
	})

	t.Run("input map is copied", func(t *testing.T) {
		src := map[string][]string{"reader": {ActionRead}}
		a := NewRBACAuthorizer(src)
		src["reader"] = []string{ActionWrite}
		p := &Principal{ID: "r", Roles: []string{"reader"}}
		if err := a.Authorize(ctx, p, ActionRead, "key"); err != nil {
			t.Errorf("mutating source map affected authorizer: read = %v, want nil", err)
		}
	})
}

// Compile-time checks that the constructors return types implementing Authorizer.
var (
	_ Authorizer = (*NoOpAuthorizer)(nil)
	_ Authorizer = (*RBACAuthorizer)(nil)
)

// generateCAAndClientCert creates a self-signed CA and a client certificate
// signed by it, for AuthenticateMTLS chain-verification tests. It reuses
// generateTestCert (tls_test.go) for the CA.
func generateCAAndClientCert(t *testing.T) (caCert, clientCert *x509.Certificate) {
	t.Helper()

	_, caKeyPEM, caCert, err := generateTestCert(true)
	if err != nil {
		t.Fatalf("Failed to generate CA cert: %v", err)
	}

	caKeyBlock, _ := pem.Decode(caKeyPEM)
	caKey, err := x509.ParseECPrivateKey(caKeyBlock.Bytes)
	if err != nil {
		t.Fatalf("Failed to parse CA key: %v", err)
	}

	clientKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("Failed to generate client key: %v", err)
	}

	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		t.Fatalf("Failed to generate serial number: %v", err)
	}

	clientTemplate := x509.Certificate{
		SerialNumber:          serialNumber,
		Subject:               pkix.Name{CommonName: "client-user"},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}

	clientCertDER, err := x509.CreateCertificate(rand.Reader, &clientTemplate, caCert, &clientKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("Failed to create client cert: %v", err)
	}

	clientCert, err = x509.ParseCertificate(clientCertDER)
	if err != nil {
		t.Fatalf("Failed to parse client cert: %v", err)
	}

	return caCert, clientCert
}

func TestMTLSAuthenticator_RequiredRoots(t *testing.T) {
	extractFunc := func(ctx context.Context, cert *x509.Certificate) (*Principal, error) {
		return &Principal{
			ID:   cert.Subject.CommonName,
			Name: cert.Subject.CommonName,
			Type: "certificate",
		}, nil
	}

	caCert, clientCert := generateCAAndClientCert(t)
	roots := x509.NewCertPool()
	roots.AddCert(caCert)

	auth := NewMTLSAuthenticator(extractFunc, roots)
	ctx := context.Background()

	t.Run("accepts certificate chaining to required roots", func(t *testing.T) {
		state := &tls.ConnectionState{
			PeerCertificates: []*x509.Certificate{clientCert},
		}

		principal, err := auth.AuthenticateMTLS(ctx, state)
		if err != nil {
			t.Fatalf("AuthenticateMTLS() error = %v, want nil", err)
		}
		if principal.ID != "client-user" {
			t.Errorf("principal.ID = %s, want client-user", principal.ID)
		}
	})

	t.Run("rejects certificate not chaining to required roots", func(t *testing.T) {
		// A self-signed certificate that does not chain to the CA pool.
		_, _, selfSigned, err := generateTestCert(false)
		if err != nil {
			t.Fatalf("Failed to generate self-signed cert: %v", err)
		}
		state := &tls.ConnectionState{
			PeerCertificates: []*x509.Certificate{selfSigned},
		}

		_, err = auth.AuthenticateMTLS(ctx, state)
		if !errors.Is(err, ErrInvalidCredentials) {
			t.Errorf("AuthenticateMTLS() error = %v, want ErrInvalidCredentials", err)
		}
	})

	t.Run("trusts non-empty VerifiedChains from the TLS handshake", func(t *testing.T) {
		state := &tls.ConnectionState{
			PeerCertificates: []*x509.Certificate{clientCert},
			VerifiedChains:   [][]*x509.Certificate{{clientCert, caCert}},
		}

		principal, err := auth.AuthenticateMTLS(ctx, state)
		if err != nil {
			t.Fatalf("AuthenticateMTLS() error = %v, want nil", err)
		}
		if principal.ID != "client-user" {
			t.Errorf("principal.ID = %s, want client-user", principal.ID)
		}
	})

	t.Run("nil RequiredRoots preserves no-verification behavior", func(t *testing.T) {
		_, _, selfSigned, err := generateTestCert(false)
		if err != nil {
			t.Fatalf("Failed to generate self-signed cert: %v", err)
		}
		noRoots := NewMTLSAuthenticator(extractFunc, nil)
		state := &tls.ConnectionState{
			PeerCertificates: []*x509.Certificate{selfSigned},
		}

		principal, err := noRoots.AuthenticateMTLS(ctx, state)
		if err != nil {
			t.Fatalf("AuthenticateMTLS() error = %v, want nil", err)
		}
		if principal.ID != selfSigned.Subject.CommonName {
			t.Errorf("principal.ID = %s, want %s", principal.ID, selfSigned.Subject.CommonName)
		}
	})
}
