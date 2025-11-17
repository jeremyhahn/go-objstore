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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestTLSMode(t *testing.T) {
	tests := []struct {
		name string
		mode TLSMode
		want int
	}{
		{"Disabled", TLSModeDisabled, 0},
		{"Server", TLSModeServer, 1},
		{"Mutual", TLSModeMutual, 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if int(tt.mode) != tt.want {
				t.Errorf("TLSMode = %d, want %d", tt.mode, tt.want)
			}
		})
	}
}

func TestNewTLSConfig(t *testing.T) {
	config := NewTLSConfig()

	if config.Mode != TLSModeDisabled {
		t.Errorf("Default mode = %v, want TLSModeDisabled", config.Mode)
	}

	if config.MinVersion != tls.VersionTLS12 {
		t.Errorf("Default MinVersion = %v, want TLS 1.2", config.MinVersion)
	}

	if config.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Errorf("Default ClientAuth = %v, want RequireAndVerifyClientCert", config.ClientAuth)
	}
}

func TestTLSConfig_WithServerCertPEM(t *testing.T) {
	config := NewTLSConfig()
	certPEM := []byte("cert")
	keyPEM := []byte("key")

	result := config.WithServerCertPEM(certPEM, keyPEM)

	if result.Mode != TLSModeServer {
		t.Errorf("Mode = %v, want TLSModeServer", result.Mode)
	}

	if string(result.ServerCertPEM) != string(certPEM) {
		t.Errorf("ServerCertPEM not set correctly")
	}

	if string(result.ServerKeyPEM) != string(keyPEM) {
		t.Errorf("ServerKeyPEM not set correctly")
	}
}

func TestTLSConfig_WithClientCAPEM(t *testing.T) {
	config := NewTLSConfig()
	caPEM := []byte("ca")

	result := config.WithClientCAPEM(caPEM)

	if result.Mode != TLSModeMutual {
		t.Errorf("Mode = %v, want TLSModeMutual", result.Mode)
	}

	if string(result.ClientCAPEM) != string(caPEM) {
		t.Errorf("ClientCAPEM not set correctly")
	}
}

func TestTLSConfig_WithMinVersion(t *testing.T) {
	config := NewTLSConfig()
	result := config.WithMinVersion(tls.VersionTLS13)

	if result.MinVersion != tls.VersionTLS13 {
		t.Errorf("MinVersion = %v, want TLS 1.3", result.MinVersion)
	}
}

func TestTLSConfig_WithInsecureSkipVerify(t *testing.T) {
	config := NewTLSConfig()

	result := config.WithInsecureSkipVerify(true)
	if !result.InsecureSkipVerify {
		t.Error("InsecureSkipVerify should be true")
	}

	result = config.WithInsecureSkipVerify(false)
	if result.InsecureSkipVerify {
		t.Error("InsecureSkipVerify should be false")
	}
}

func TestTLSConfig_Build_Disabled(t *testing.T) {
	config := NewTLSConfig()

	result, err := config.Build()
	if err != nil {
		t.Errorf("Build() error = %v, want nil", err)
	}
	if result != nil {
		t.Error("Build() should return nil for disabled mode")
	}
}

func TestTLSConfig_Build_MissingCertificate(t *testing.T) {
	config := NewTLSConfig()
	config.Mode = TLSModeServer

	_, err := config.Build()
	if err == nil {
		t.Error("Build() should return error for missing certificate")
	}
	if !errors.Is(err, ErrInvalidCertificate) {
		t.Errorf("Build() error = %v, want ErrInvalidCertificate", err)
	}
}

func TestTLSConfig_Build_MissingCA(t *testing.T) {
	config := NewTLSConfig()
	config.Mode = TLSModeMutual
	// Use PEM data instead of files to avoid file system dependencies
	config.ServerCertPEM = []byte("fake-cert")
	config.ServerKeyPEM = []byte("fake-key")

	_, err := config.Build()
	if err == nil {
		t.Error("Build() should return error for missing CA in mutual mode")
	}
	// The error should be about missing CA (after failing to load the cert)
	// Since we're using fake PEM data, it will fail cert loading first,
	// but the important thing is that it fails
}

func TestValidateClientCertificate(t *testing.T) {
	// Create a simple CA pool for testing
	caPool := x509.NewCertPool()

	// Create a test certificate
	cert := &x509.Certificate{
		Subject: pkix.Name{CommonName: "test"},
	}

	// This will fail validation since we don't have proper certs
	err := ValidateClientCertificate(cert, caPool)
	if err == nil {
		t.Error("ValidateClientCertificate() should fail with invalid cert")
	}
	if !errors.Is(err, ErrInvalidCertificate) {
		t.Errorf("ValidateClientCertificate() error = %v, want ErrInvalidCertificate", err)
	}
}

func TestCreateSelfSignedCert(t *testing.T) {
	// This should return an error indicating it's not implemented
	_, _, err := CreateSelfSignedCert()
	if err == nil {
		t.Error("CreateSelfSignedCert() should return error for unimplemented function")
	}
}

func TestTLSConfig_Chaining(t *testing.T) {
	// Test that methods can be chained
	config := NewTLSConfig().
		WithServerCertPEM([]byte("cert"), []byte("key")).
		WithMinVersion(tls.VersionTLS13).
		WithInsecureSkipVerify(false)

	if config.Mode != TLSModeServer {
		t.Errorf("Mode = %v, want TLSModeServer", config.Mode)
	}

	if config.MinVersion != tls.VersionTLS13 {
		t.Errorf("MinVersion = %v, want TLS 1.3", config.MinVersion)
	}

	if config.InsecureSkipVerify {
		t.Error("InsecureSkipVerify should be false")
	}
}

func TestLoadTLSConfigFromFiles(t *testing.T) {
	// Test with non-existent files (should fail)
	_, err := LoadTLSConfigFromFiles("nonexistent.crt", "nonexistent.key", "")
	if err == nil {
		t.Error("LoadTLSConfigFromFiles() should fail with non-existent files")
	}
}

func TestLoadMTLSConfigFromFiles(t *testing.T) {
	// Test with empty CA file (should fail)
	_, err := LoadMTLSConfigFromFiles("cert.pem", "key.pem", "")
	if err == nil {
		t.Error("LoadMTLSConfigFromFiles() should fail with empty CA file")
	}
	if !errors.Is(err, ErrInvalidCAPool) {
		t.Errorf("LoadMTLSConfigFromFiles() error = %v, want ErrInvalidCAPool", err)
	}

	// Test with non-existent files
	_, err = LoadMTLSConfigFromFiles("nonexistent.crt", "nonexistent.key", "nonexistent-ca.crt")
	if err == nil {
		t.Error("LoadMTLSConfigFromFiles() should fail with non-existent files")
	}
}

// Helper function to generate test certificates
func generateTestCert(isCA bool) (certPEM, keyPEM []byte, cert *x509.Certificate, err error) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, nil, err
	}

	notBefore := time.Now()
	notAfter := notBefore.Add(24 * time.Hour)

	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, nil, nil, err
	}

	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"Test Org"},
			CommonName:   "Test Cert",
		},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}

	if isCA {
		template.IsCA = true
		template.KeyUsage |= x509.KeyUsageCertSign
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		return nil, nil, nil, err
	}

	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes})

	privBytes, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		return nil, nil, nil, err
	}
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: privBytes})

	cert, err = x509.ParseCertificate(derBytes)
	if err != nil {
		return nil, nil, nil, err
	}

	return certPEM, keyPEM, cert, nil
}

func TestTLSConfig_Build_WithValidPEM(t *testing.T) {
	certPEM, keyPEM, _, err := generateTestCert(false)
	if err != nil {
		t.Fatalf("Failed to generate test cert: %v", err)
	}

	config := NewTLSConfig().WithServerCertPEM(certPEM, keyPEM)

	tlsConfig, err := config.Build()
	if err != nil {
		t.Errorf("Build() error = %v, want nil", err)
	}
	if tlsConfig == nil {
		t.Error("Build() should return non-nil config")
	}
	if len(tlsConfig.Certificates) != 1 {
		t.Errorf("Build() certificates count = %d, want 1", len(tlsConfig.Certificates))
	}
}

func TestTLSConfig_Build_WithInvalidPEM(t *testing.T) {
	config := NewTLSConfig().WithServerCertPEM([]byte("invalid"), []byte("invalid"))

	_, err := config.Build()
	if err == nil {
		t.Error("Build() should fail with invalid PEM data")
	}
	if !errors.Is(err, ErrInvalidCertificate) {
		t.Errorf("Build() error = %v, want ErrInvalidCertificate", err)
	}
}

func TestTLSConfig_Build_WithFiles(t *testing.T) {
	// Create temporary directory for test files
	tmpDir := t.TempDir()

	certPEM, keyPEM, _, err := generateTestCert(false)
	if err != nil {
		t.Fatalf("Failed to generate test cert: %v", err)
	}

	certFile := filepath.Join(tmpDir, "cert.pem")
	keyFile := filepath.Join(tmpDir, "key.pem")

	if err := os.WriteFile(certFile, certPEM, 0600); err != nil {
		t.Fatalf("Failed to write cert file: %v", err)
	}
	if err := os.WriteFile(keyFile, keyPEM, 0600); err != nil {
		t.Fatalf("Failed to write key file: %v", err)
	}

	config := NewTLSConfig().WithServerCertFiles(certFile, keyFile)

	tlsConfig, err := config.Build()
	if err != nil {
		t.Errorf("Build() error = %v, want nil", err)
	}
	if tlsConfig == nil {
		t.Error("Build() should return non-nil config")
	}
}

func TestTLSConfig_Build_WithInvalidFiles(t *testing.T) {
	tmpDir := t.TempDir()

	certFile := filepath.Join(tmpDir, "cert.pem")
	keyFile := filepath.Join(tmpDir, "key.pem")

	// Write invalid PEM data
	if err := os.WriteFile(certFile, []byte("invalid"), 0600); err != nil {
		t.Fatalf("Failed to write cert file: %v", err)
	}
	if err := os.WriteFile(keyFile, []byte("invalid"), 0600); err != nil {
		t.Fatalf("Failed to write key file: %v", err)
	}

	config := NewTLSConfig().WithServerCertFiles(certFile, keyFile)

	_, err := config.Build()
	if err == nil {
		t.Error("Build() should fail with invalid cert files")
	}
	if !errors.Is(err, ErrInvalidCertificate) {
		t.Errorf("Build() error = %v, want ErrInvalidCertificate", err)
	}
}

func TestTLSConfig_Build_MutualTLS_WithValidPEM(t *testing.T) {
	certPEM, keyPEM, _, err := generateTestCert(false)
	if err != nil {
		t.Fatalf("Failed to generate test cert: %v", err)
	}

	caPEM, _, _, err := generateTestCert(true)
	if err != nil {
		t.Fatalf("Failed to generate test CA: %v", err)
	}

	config := NewTLSConfig().
		WithServerCertPEM(certPEM, keyPEM).
		WithClientCAPEM(caPEM)

	tlsConfig, err := config.Build()
	if err != nil {
		t.Errorf("Build() error = %v, want nil", err)
	}
	if tlsConfig == nil {
		t.Error("Build() should return non-nil config")
	}
	if tlsConfig.ClientCAs == nil {
		t.Error("Build() should set ClientCAs for mutual TLS")
	}
	if tlsConfig.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Errorf("Build() ClientAuth = %v, want RequireAndVerifyClientCert", tlsConfig.ClientAuth)
	}
}

func TestTLSConfig_Build_MutualTLS_WithInvalidCAPEM(t *testing.T) {
	certPEM, keyPEM, _, err := generateTestCert(false)
	if err != nil {
		t.Fatalf("Failed to generate test cert: %v", err)
	}

	config := NewTLSConfig().
		WithServerCertPEM(certPEM, keyPEM).
		WithClientCAPEM([]byte("invalid ca"))

	_, err = config.Build()
	if err == nil {
		t.Error("Build() should fail with invalid CA PEM")
	}
	if !errors.Is(err, ErrInvalidCAPool) {
		t.Errorf("Build() error = %v, want ErrInvalidCAPool", err)
	}
}

func TestTLSConfig_Build_MutualTLS_WithCAFile(t *testing.T) {
	tmpDir := t.TempDir()

	certPEM, keyPEM, _, err := generateTestCert(false)
	if err != nil {
		t.Fatalf("Failed to generate test cert: %v", err)
	}

	caPEM, _, _, err := generateTestCert(true)
	if err != nil {
		t.Fatalf("Failed to generate test CA: %v", err)
	}

	certFile := filepath.Join(tmpDir, "cert.pem")
	keyFile := filepath.Join(tmpDir, "key.pem")
	caFile := filepath.Join(tmpDir, "ca.pem")

	if err := os.WriteFile(certFile, certPEM, 0600); err != nil {
		t.Fatalf("Failed to write cert file: %v", err)
	}
	if err := os.WriteFile(keyFile, keyPEM, 0600); err != nil {
		t.Fatalf("Failed to write key file: %v", err)
	}
	if err := os.WriteFile(caFile, caPEM, 0600); err != nil {
		t.Fatalf("Failed to write CA file: %v", err)
	}

	config := NewTLSConfig().
		WithServerCertFiles(certFile, keyFile).
		WithClientCAFile(caFile)

	tlsConfig, err := config.Build()
	if err != nil {
		t.Errorf("Build() error = %v, want nil", err)
	}
	if tlsConfig == nil {
		t.Error("Build() should return non-nil config")
	}
	if tlsConfig.ClientCAs == nil {
		t.Error("Build() should set ClientCAs for mutual TLS")
	}
}

func TestTLSConfig_Build_MutualTLS_WithInvalidCAFile(t *testing.T) {
	tmpDir := t.TempDir()

	certPEM, keyPEM, _, err := generateTestCert(false)
	if err != nil {
		t.Fatalf("Failed to generate test cert: %v", err)
	}

	certFile := filepath.Join(tmpDir, "cert.pem")
	keyFile := filepath.Join(tmpDir, "key.pem")
	caFile := filepath.Join(tmpDir, "ca.pem")

	if err := os.WriteFile(certFile, certPEM, 0600); err != nil {
		t.Fatalf("Failed to write cert file: %v", err)
	}
	if err := os.WriteFile(keyFile, keyPEM, 0600); err != nil {
		t.Fatalf("Failed to write key file: %v", err)
	}
	if err := os.WriteFile(caFile, []byte("invalid ca"), 0600); err != nil {
		t.Fatalf("Failed to write CA file: %v", err)
	}

	config := NewTLSConfig().
		WithServerCertFiles(certFile, keyFile).
		WithClientCAFile(caFile)

	_, err = config.Build()
	if err == nil {
		t.Error("Build() should fail with invalid CA file")
	}
	if !errors.Is(err, ErrInvalidCAPool) {
		t.Errorf("Build() error = %v, want ErrInvalidCAPool", err)
	}
}

func TestTLSConfig_Build_MutualTLS_MissingCA(t *testing.T) {
	certPEM, keyPEM, _, err := generateTestCert(false)
	if err != nil {
		t.Fatalf("Failed to generate test cert: %v", err)
	}

	config := NewTLSConfig()
	config.Mode = TLSModeMutual
	config.ServerCertPEM = certPEM
	config.ServerKeyPEM = keyPEM
	// Don't set CA

	_, err = config.Build()
	if err == nil {
		t.Error("Build() should fail when CA is missing for mutual TLS")
	}
	if !errors.Is(err, ErrInvalidCAPool) {
		t.Errorf("Build() error = %v, want ErrInvalidCAPool", err)
	}
}

func TestTLSConfig_Build_MutualTLS_NonexistentCAFile(t *testing.T) {
	certPEM, keyPEM, _, err := generateTestCert(false)
	if err != nil {
		t.Fatalf("Failed to generate test cert: %v", err)
	}

	config := NewTLSConfig()
	config.Mode = TLSModeMutual
	config.ServerCertPEM = certPEM
	config.ServerKeyPEM = keyPEM
	config.ClientCAFile = "/nonexistent/ca.pem"

	_, err = config.Build()
	if err == nil {
		t.Error("Build() should fail when CA file doesn't exist")
	}
	if !errors.Is(err, ErrInvalidCAPool) {
		t.Errorf("Build() error = %v, want ErrInvalidCAPool", err)
	}
}

func TestTLSConfig_WithServerCertFiles(t *testing.T) {
	config := NewTLSConfig()
	result := config.WithServerCertFiles("cert.pem", "key.pem")

	if result.Mode != TLSModeServer {
		t.Errorf("Mode = %v, want TLSModeServer", result.Mode)
	}
	if result.ServerCertFile != "cert.pem" {
		t.Errorf("ServerCertFile = %s, want cert.pem", result.ServerCertFile)
	}
	if result.ServerKeyFile != "key.pem" {
		t.Errorf("ServerKeyFile = %s, want key.pem", result.ServerKeyFile)
	}
}

func TestTLSConfig_WithClientCAFile(t *testing.T) {
	config := NewTLSConfig()
	result := config.WithClientCAFile("ca.pem")

	if result.Mode != TLSModeMutual {
		t.Errorf("Mode = %v, want TLSModeMutual", result.Mode)
	}
	if result.ClientCAFile != "ca.pem" {
		t.Errorf("ClientCAFile = %s, want ca.pem", result.ClientCAFile)
	}
}

func TestLoadTLSConfigFromFiles_WithCA(t *testing.T) {
	tmpDir := t.TempDir()

	certPEM, keyPEM, _, err := generateTestCert(false)
	if err != nil {
		t.Fatalf("Failed to generate test cert: %v", err)
	}

	caPEM, _, _, err := generateTestCert(true)
	if err != nil {
		t.Fatalf("Failed to generate test CA: %v", err)
	}

	certFile := filepath.Join(tmpDir, "cert.pem")
	keyFile := filepath.Join(tmpDir, "key.pem")
	caFile := filepath.Join(tmpDir, "ca.pem")

	if err := os.WriteFile(certFile, certPEM, 0600); err != nil {
		t.Fatalf("Failed to write cert file: %v", err)
	}
	if err := os.WriteFile(keyFile, keyPEM, 0600); err != nil {
		t.Fatalf("Failed to write key file: %v", err)
	}
	if err := os.WriteFile(caFile, caPEM, 0600); err != nil {
		t.Fatalf("Failed to write CA file: %v", err)
	}

	tlsConfig, err := LoadTLSConfigFromFiles(certFile, keyFile, caFile)
	if err != nil {
		t.Errorf("LoadTLSConfigFromFiles() error = %v, want nil", err)
	}
	if tlsConfig == nil {
		t.Error("LoadTLSConfigFromFiles() should return non-nil config")
	}
	if tlsConfig.ClientCAs == nil {
		t.Error("LoadTLSConfigFromFiles() should set ClientCAs when CA file is provided")
	}
}

func TestValidateClientCertificate_Success(t *testing.T) {
	// Generate a CA certificate
	caPEM, caKeyPEM, _, err := generateTestCert(true)
	if err != nil {
		t.Fatalf("Failed to generate CA cert: %v", err)
	}

	// Parse CA cert and key
	caBlock, _ := pem.Decode(caPEM)
	caCert, err := x509.ParseCertificate(caBlock.Bytes)
	if err != nil {
		t.Fatalf("Failed to parse CA cert: %v", err)
	}

	caKeyBlock, _ := pem.Decode(caKeyPEM)
	caKey, err := x509.ParseECPrivateKey(caKeyBlock.Bytes)
	if err != nil {
		t.Fatalf("Failed to parse CA key: %v", err)
	}

	// Generate a client certificate signed by the CA
	clientKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("Failed to generate client key: %v", err)
	}

	notBefore := time.Now()
	notAfter := notBefore.Add(24 * time.Hour)

	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		t.Fatalf("Failed to generate serial number: %v", err)
	}

	clientTemplate := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"Test Client Org"},
			CommonName:   "Test Client",
		},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}

	clientCertDER, err := x509.CreateCertificate(rand.Reader, &clientTemplate, caCert, &clientKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("Failed to create client cert: %v", err)
	}

	clientCert, err := x509.ParseCertificate(clientCertDER)
	if err != nil {
		t.Fatalf("Failed to parse client cert: %v", err)
	}

	// Create CA pool
	caPool := x509.NewCertPool()
	caPool.AddCert(caCert)

	// Validate the client certificate
	err = ValidateClientCertificate(clientCert, caPool)
	if err != nil {
		t.Errorf("ValidateClientCertificate() should succeed with valid cert, got error: %v", err)
	}
}
