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

package grpc

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
)

func TestWithConnectionTimeout(t *testing.T) {
	opts := DefaultServerOptions()
	timeout := 60 * time.Second
	WithConnectionTimeout(timeout)(opts)

	if opts.ConnectionTimeout != timeout {
		t.Errorf("Expected timeout %v, got %v", timeout, opts.ConnectionTimeout)
	}
}

func TestWithKeepAlive(t *testing.T) {
	opts := DefaultServerOptions()
	keepAliveTime := 1 * time.Hour
	keepAliveTimeout := 30 * time.Second
	WithKeepAlive(keepAliveTime, keepAliveTimeout)(opts)

	if opts.KeepAliveTime != keepAliveTime {
		t.Errorf("Expected KeepAliveTime %v, got %v", keepAliveTime, opts.KeepAliveTime)
	}

	if opts.KeepAliveTimeout != keepAliveTimeout {
		t.Errorf("Expected KeepAliveTimeout %v, got %v", keepAliveTimeout, opts.KeepAliveTimeout)
	}
}

func TestWithHealthCheck(t *testing.T) {
	opts := DefaultServerOptions()
	WithHealthCheck(false)(opts)

	if opts.EnableHealthCheck {
		t.Error("Expected health check to be disabled")
	}

	WithHealthCheck(true)(opts)

	if !opts.EnableHealthCheck {
		t.Error("Expected health check to be enabled")
	}
}

func TestWithUnaryInterceptor(t *testing.T) {
	opts := DefaultServerOptions()

	interceptor := LoggingUnaryInterceptor(opts.Logger)

	WithUnaryInterceptor(interceptor)(opts)

	if len(opts.UnaryInterceptors) != 1 {
		t.Errorf("Expected 1 unary interceptor, got %d", len(opts.UnaryInterceptors))
	}
}

func TestWithStreamInterceptor(t *testing.T) {
	opts := DefaultServerOptions()

	interceptor := LoggingStreamInterceptor(opts.Logger)

	WithStreamInterceptor(interceptor)(opts)

	if len(opts.StreamInterceptors) != 1 {
		t.Errorf("Expected 1 stream interceptor, got %d", len(opts.StreamInterceptors))
	}
}

func TestMultipleOptions(t *testing.T) {
	opts := DefaultServerOptions()

	WithAddress(":9090")(opts)
	WithMaxConcurrentStreams(500)(opts)
	WithChunkSize(128 * 1024)(opts)
	WithReflection(true)(opts)
	WithMetrics(true)(opts)
	WithLogging(true)(opts)

	if opts.Address != ":9090" {
		t.Errorf("Address not set correctly")
	}

	if opts.MaxConcurrentStreams != 500 {
		t.Errorf("MaxConcurrentStreams not set correctly")
	}

	if opts.ChunkSize != 128*1024 {
		t.Errorf("ChunkSize not set correctly")
	}

	if !opts.EnableReflection {
		t.Error("Reflection should be enabled")
	}

	if !opts.EnableMetrics {
		t.Error("Metrics should be enabled")
	}

	if !opts.EnableLogging {
		t.Error("Logging should be enabled")
	}
}

func TestWithLogger(t *testing.T) {
	opts := DefaultServerOptions()
	logger := adapters.NewDefaultLogger()

	WithLogger(logger)(opts)

	if opts.Logger == nil {
		t.Error("Logger should be set")
	}
}

func TestWithAuthenticator(t *testing.T) {
	opts := DefaultServerOptions()
	auth := adapters.NewNoOpAuthenticator()

	WithAuthenticator(auth)(opts)

	if opts.Authenticator == nil {
		t.Error("Authenticator should be set")
	}
}

func TestWithAdapterTLS(t *testing.T) {
	opts := DefaultServerOptions()
	tlsConfig := adapters.NewTLSConfig()

	WithAdapterTLS(tlsConfig)(opts)

	if opts.AdapterTLSConfig == nil {
		t.Error("AdapterTLSConfig should be set")
	}
}

func TestWithTLSFromFiles(t *testing.T) {
	// Create temporary directory for certificates
	tmpDir := t.TempDir()
	certFile := tmpDir + "/cert.pem"
	keyFile := tmpDir + "/key.pem"

	// Generate a self-signed certificate for testing
	cert, key := generateTestCert(t)

	// Write cert and key to temporary files
	if err := os.WriteFile(certFile, cert, 0644); err != nil {
		t.Fatalf("Failed to write cert file: %v", err)
	}
	if err := os.WriteFile(keyFile, key, 0600); err != nil {
		t.Fatalf("Failed to write key file: %v", err)
	}

	t.Run("Valid certificates", func(t *testing.T) {
		opt, err := WithTLSFromFiles(certFile, keyFile)
		if err != nil {
			t.Errorf("WithTLSFromFiles() error = %v, want nil", err)
		}
		if opt == nil {
			t.Error("Expected non-nil option")
		}

		// Apply the option to verify it works
		opts := DefaultServerOptions()
		opt(opts)
		if opts.TLSConfig == nil {
			t.Error("TLS config should be set")
		}
	})

	t.Run("Invalid cert file", func(t *testing.T) {
		_, err := WithTLSFromFiles("nonexistent.pem", keyFile)
		if err == nil {
			t.Error("Expected error for nonexistent cert file")
		}
	})

	t.Run("Invalid key file", func(t *testing.T) {
		_, err := WithTLSFromFiles(certFile, "nonexistent.key")
		if err == nil {
			t.Error("Expected error for nonexistent key file")
		}
	})
}

// generateTestCert generates a self-signed certificate for testing
func generateTestCert(t *testing.T) (certPEM, keyPEM []byte) {
	// Generate a private key
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("Failed to generate private key: %v", err)
	}

	// Create a certificate template
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"Test"},
			CommonName:   "localhost",
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}

	// Create the certificate
	derBytes, err := x509.CreateCertificate(rand.Reader, template, template, &privateKey.PublicKey, privateKey)
	if err != nil {
		t.Fatalf("Failed to create certificate: %v", err)
	}

	// Encode certificate to PEM
	certPEM = pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: derBytes,
	})

	// Encode private key to PEM
	keyPEM = pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(privateKey),
	})

	return certPEM, keyPEM
}
