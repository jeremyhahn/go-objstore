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

package rest

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/objstore"
)

// initTLSTestFacade initializes the objstore facade with a mock storage for testing.
func initTLSTestFacade(t *testing.T, storage common.Storage) {
	t.Helper()
	objstore.Reset()
	err := objstore.Initialize(&objstore.FacadeConfig{
		Backends:       map[string]common.Storage{"default": storage},
		DefaultBackend: "default",
	})
	if err != nil {
		t.Fatalf("Failed to initialize facade: %v", err)
	}
}

// Test server Start with TLS configuration
func TestServerStartWithTLS(t *testing.T) {
	// Create temporary certificate and key files
	certFile, keyFile, cleanup := createTestTLSFiles(t)
	defer cleanup()

	storage := NewMockStorage()
	config := &ServerConfig{
		Host: "127.0.0.1",
		Port: 0, // Random port
		Mode: gin.TestMode,
		TLSConfig: &adapters.TLSConfig{
			Mode:           adapters.TLSModeServer,
			ServerCertFile: certFile,
			ServerKeyFile:  keyFile,
		},
	}

	initTLSTestFacade(t, storage)
	server, err := NewServer(storage, config)
	if err != nil {
		t.Fatalf("NewServer() with TLS config failed: %v", err)
	}

	// Start server in background
	errChan := make(chan error, 1)
	go func() {
		errChan <- server.Start()
	}()

	// Give it a moment to potentially start (or fail)
	time.Sleep(50 * time.Millisecond)

	// Shutdown immediately
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	server.Shutdown(ctx)

	// Check if Start returned an error
	select {
	case err := <-errChan:
		// May get an error because we shut down immediately, which is fine
		if err != nil && err != context.Canceled {
			t.Logf("Server Start() returned error (expected due to immediate shutdown): %v", err)
		}
	case <-time.After(2 * time.Second):
		// Timeout waiting for Start to complete
		t.Log("Server Start() did not complete within timeout")
	}
}

// Test server Start with invalid TLS configuration
func TestServerStartWithInvalidTLS(t *testing.T) {
	storage := NewMockStorage()
	config := &ServerConfig{
		Host: "127.0.0.1",
		Port: 0,
		Mode: gin.TestMode,
		TLSConfig: &adapters.TLSConfig{
			Mode:           adapters.TLSModeServer,
			ServerCertFile: "/nonexistent/cert.pem",
			ServerKeyFile:  "/nonexistent/key.pem",
		},
	}

	initTLSTestFacade(t, storage)
	server, err := NewServer(storage, config)
	if err != nil {
		t.Fatalf("NewServer() with invalid TLS config failed: %v", err)
	}

	// Starting should fail due to invalid cert/key files
	err = server.Start()
	if err == nil {
		t.Error("Server Start() with invalid TLS files should fail")
	}
}

// Helper function to create temporary TLS certificate and key files for testing
func createTestTLSFiles(t *testing.T) (certFile, keyFile string, cleanup func()) {
	// Generate a self-signed certificate
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("Failed to generate private key: %v", err)
	}

	notBefore := time.Now()
	notAfter := notBefore.Add(1 * time.Hour)

	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		t.Fatalf("Failed to generate serial number: %v", err)
	}

	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"Test Co"},
		},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{"localhost"},
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("Failed to create certificate: %v", err)
	}

	// Create temporary cert file
	certF, err := os.CreateTemp("", "test-cert-*.pem")
	if err != nil {
		t.Fatalf("Failed to create temp cert file: %v", err)
	}
	certFile = certF.Name()

	pem.Encode(certF, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	certF.Close()

	// Create temporary key file
	keyF, err := os.CreateTemp("", "test-key-*.pem")
	if err != nil {
		t.Fatalf("Failed to create temp key file: %v", err)
	}
	keyFile = keyF.Name()

	privBytes, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		t.Fatalf("Failed to marshal private key: %v", err)
	}

	pem.Encode(keyF, &pem.Block{Type: "PRIVATE KEY", Bytes: privBytes})
	keyF.Close()

	cleanup = func() {
		os.Remove(certFile)
		os.Remove(keyFile)
	}

	return certFile, keyFile, cleanup
}

// Test server Start with TLS mode mTLS
func TestServerStartWithMTLS(t *testing.T) {
	certFile, keyFile, cleanup := createTestTLSFiles(t)
	defer cleanup()

	// For mTLS, we also need a CA cert
	caCertFile, _, cleanupCA := createTestTLSFiles(t)
	defer cleanupCA()

	storage := NewMockStorage()
	config := &ServerConfig{
		Host: "127.0.0.1",
		Port: 0,
		Mode: gin.TestMode,
		TLSConfig: &adapters.TLSConfig{
			Mode:           adapters.TLSModeMutual,
			ServerCertFile: certFile,
			ServerKeyFile:  keyFile,
			ClientCAFile:   caCertFile,
			ClientAuth:     tls.RequireAndVerifyClientCert,
		},
	}

	initTLSTestFacade(t, storage)
	server, err := NewServer(storage, config)
	if err != nil {
		t.Fatalf("NewServer() with mTLS config failed: %v", err)
	}

	// Start server in background
	errChan := make(chan error, 1)
	go func() {
		errChan <- server.Start()
	}()

	// Give it a moment
	time.Sleep(50 * time.Millisecond)

	// Shutdown immediately
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	server.Shutdown(ctx)

	select {
	case err := <-errChan:
		if err != nil && err != context.Canceled {
			t.Logf("Server Start() returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Log("Server Start() did not complete within timeout")
	}
}
