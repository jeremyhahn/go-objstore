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
	"crypto/x509/pkix"
	"errors"
	"net/http"
	"testing"

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

	t.Run("ValidatePermission", func(t *testing.T) {
		principal := &Principal{ID: "test"}
		err := auth.ValidatePermission(ctx, principal, "resource", "action")
		if err != nil {
			t.Errorf("NoOpAuthenticator.ValidatePermission() error = %v, want nil", err)
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

	t.Run("ValidatePermission always succeeds", func(t *testing.T) {
		principal := &Principal{ID: "user1", Type: "user"}
		err := auth.ValidatePermission(ctx, principal, "any-resource", "any-action")
		if err != nil {
			t.Errorf("ValidatePermission() should not return error, got %v", err)
		}
	})

	t.Run("ValidatePermission with admin role", func(t *testing.T) {
		principal := &Principal{ID: "admin1", Type: "user", Roles: []string{"admin"}}
		err := auth.ValidatePermission(ctx, principal, "any-resource", "any-action")
		if err != nil {
			t.Errorf("ValidatePermission() should not return error for admin, got %v", err)
		}
	})

	t.Run("ValidatePermission with non-admin role", func(t *testing.T) {
		principal := &Principal{ID: "user1", Type: "user", Roles: []string{"viewer"}}
		err := auth.ValidatePermission(ctx, principal, "any-resource", "any-action")
		if err != nil {
			t.Errorf("ValidatePermission() should not return error for non-admin, got %v", err)
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

	t.Run("ValidatePermission succeeds", func(t *testing.T) {
		principal := &Principal{ID: "cert-user", Type: "certificate"}
		err := auth.ValidatePermission(ctx, principal, "any-resource", "any-action")
		if err != nil {
			t.Errorf("ValidatePermission() should not return error, got %v", err)
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

	t.Run("ValidatePermission succeeds", func(t *testing.T) {
		principal := &Principal{ID: "user", Type: "token"}
		err := auth.ValidatePermission(ctx, principal, "resource", "action")
		if err != nil {
			t.Errorf("ValidatePermission() should not return error, got %v", err)
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

	t.Run("ValidatePermission with empty authenticators", func(t *testing.T) {
		emptyAuth := NewCompositeAuthenticator()
		principal := &Principal{ID: "user", Type: "token"}
		err := emptyAuth.ValidatePermission(ctx, principal, "resource", "action")
		if err != nil {
			t.Errorf("ValidatePermission() with empty authenticators should not return error, got %v", err)
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
