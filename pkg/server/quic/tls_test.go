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

package quic

import (
	"crypto/tls"
	"os"
	"path/filepath"
	"testing"
)

func TestGenerateSelfSignedCert(t *testing.T) {
	config, err := GenerateSelfSignedCert()
	if err != nil {
		t.Fatalf("Failed to generate self-signed cert: %v", err)
	}

	if config == nil {
		t.Fatal("Expected non-nil TLS config")
	}

	if len(config.Certificates) != 1 {
		t.Errorf("Expected 1 certificate, got %d", len(config.Certificates))
	}

	if config.MinVersion != tls.VersionTLS13 {
		t.Errorf("Expected TLS 1.3, got version %d", config.MinVersion)
	}

	if config.MaxVersion != tls.VersionTLS13 {
		t.Errorf("Expected TLS 1.3, got version %d", config.MaxVersion)
	}

	if len(config.NextProtos) != 1 || config.NextProtos[0] != "h3" {
		t.Errorf("Expected h3 ALPN, got %v", config.NextProtos)
	}

	if config.ClientAuth != tls.NoClientCert {
		t.Errorf("Expected NoClientCert, got %v", config.ClientAuth)
	}

	// Verify cipher suites
	expectedCiphers := []uint16{
		tls.TLS_AES_128_GCM_SHA256,
		tls.TLS_AES_256_GCM_SHA384,
		tls.TLS_CHACHA20_POLY1305_SHA256,
	}

	if len(config.CipherSuites) != len(expectedCiphers) {
		t.Errorf("Expected %d cipher suites, got %d", len(expectedCiphers), len(config.CipherSuites))
	}
}

func TestNewTLSConfig(t *testing.T) {
	tmpDir := t.TempDir()
	certFile := filepath.Join(tmpDir, "cert.pem")
	keyFile := filepath.Join(tmpDir, "key.pem")

	// Generate and save test certificate
	config, err := GenerateSelfSignedCert()
	if err != nil {
		t.Fatalf("Failed to generate test cert: %v", err)
	}

	err = SaveCertificateToPEM(certFile, keyFile, &config.Certificates[0])
	if err != nil {
		t.Fatalf("Failed to save cert: %v", err)
	}

	// Test loading certificate
	loadedConfig, err := NewTLSConfig(certFile, keyFile)
	if err != nil {
		t.Fatalf("Failed to load TLS config: %v", err)
	}

	if loadedConfig == nil {
		t.Fatal("Expected non-nil TLS config")
	}

	if len(loadedConfig.Certificates) != 1 {
		t.Errorf("Expected 1 certificate, got %d", len(loadedConfig.Certificates))
	}

	if loadedConfig.MinVersion != tls.VersionTLS13 {
		t.Errorf("Expected TLS 1.3, got version %d", loadedConfig.MinVersion)
	}

	if len(loadedConfig.NextProtos) != 1 || loadedConfig.NextProtos[0] != "h3" {
		t.Errorf("Expected h3 ALPN, got %v", loadedConfig.NextProtos)
	}
}

func TestNewTLSConfig_InvalidFiles(t *testing.T) {
	tests := []struct {
		name     string
		certFile string
		keyFile  string
	}{
		{
			name:     "non-existent files",
			certFile: "/nonexistent/cert.pem",
			keyFile:  "/nonexistent/key.pem",
		},
		{
			name:     "empty paths",
			certFile: "",
			keyFile:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewTLSConfig(tt.certFile, tt.keyFile)
			if err == nil {
				t.Error("Expected error for invalid certificate files")
			}
		})
	}
}

func TestNewTLSConfigWithClientAuth(t *testing.T) {
	tmpDir := t.TempDir()
	certFile := filepath.Join(tmpDir, "cert.pem")
	keyFile := filepath.Join(tmpDir, "key.pem")
	caCertFile := filepath.Join(tmpDir, "ca.pem")

	// Generate and save test certificate
	config, err := GenerateSelfSignedCert()
	if err != nil {
		t.Fatalf("Failed to generate test cert: %v", err)
	}

	err = SaveCertificateToPEM(certFile, keyFile, &config.Certificates[0])
	if err != nil {
		t.Fatalf("Failed to save cert: %v", err)
	}

	// Use the same cert as CA for testing
	err = SaveCertificateToPEM(caCertFile, filepath.Join(tmpDir, "ca-key.pem"), &config.Certificates[0])
	if err != nil {
		t.Fatalf("Failed to save CA cert: %v", err)
	}

	// Test loading with client auth
	loadedConfig, err := NewTLSConfigWithClientAuth(certFile, keyFile, caCertFile)
	if err != nil {
		t.Fatalf("Failed to load TLS config with client auth: %v", err)
	}

	if loadedConfig == nil {
		t.Fatal("Expected non-nil TLS config")
	}

	if loadedConfig.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Errorf("Expected RequireAndVerifyClientCert, got %v", loadedConfig.ClientAuth)
	}

	if loadedConfig.ClientCAs == nil {
		t.Error("Expected non-nil ClientCAs")
	}
}

func TestNewTLSConfigWithClientAuth_InvalidCA(t *testing.T) {
	tmpDir := t.TempDir()
	certFile := filepath.Join(tmpDir, "cert.pem")
	keyFile := filepath.Join(tmpDir, "key.pem")

	// Generate and save test certificate
	config, err := GenerateSelfSignedCert()
	if err != nil {
		t.Fatalf("Failed to generate test cert: %v", err)
	}

	err = SaveCertificateToPEM(certFile, keyFile, &config.Certificates[0])
	if err != nil {
		t.Fatalf("Failed to save cert: %v", err)
	}

	// Test with non-existent CA file
	_, err = NewTLSConfigWithClientAuth(certFile, keyFile, "/nonexistent/ca.pem")
	if err == nil {
		t.Error("Expected error for non-existent CA file")
	}
}

func TestSaveCertificateToPEM(t *testing.T) {
	tmpDir := t.TempDir()
	certFile := filepath.Join(tmpDir, "cert.pem")
	keyFile := filepath.Join(tmpDir, "key.pem")

	// Generate test certificate
	config, err := GenerateSelfSignedCert()
	if err != nil {
		t.Fatalf("Failed to generate test cert: %v", err)
	}

	// Save certificate
	err = SaveCertificateToPEM(certFile, keyFile, &config.Certificates[0])
	if err != nil {
		t.Fatalf("Failed to save certificate: %v", err)
	}

	// Check that files were created
	if _, err := os.Stat(certFile); os.IsNotExist(err) {
		t.Error("Certificate file was not created")
	}

	if _, err := os.Stat(keyFile); os.IsNotExist(err) {
		t.Error("Key file was not created")
	}

	// Verify we can load the saved certificate
	_, err = tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		t.Errorf("Failed to load saved certificate: %v", err)
	}
}

func TestGenerateAndSaveSelfSignedCert(t *testing.T) {
	tmpDir := t.TempDir()
	certFile := filepath.Join(tmpDir, "cert.pem")
	keyFile := filepath.Join(tmpDir, "key.pem")

	// Generate and save
	err := GenerateAndSaveSelfSignedCert(certFile, keyFile)
	if err != nil {
		t.Fatalf("Failed to generate and save cert: %v", err)
	}

	// Check that files were created
	if _, err := os.Stat(certFile); os.IsNotExist(err) {
		t.Error("Certificate file was not created")
	}

	if _, err := os.Stat(keyFile); os.IsNotExist(err) {
		t.Error("Key file was not created")
	}

	// Verify we can load the certificate
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		t.Errorf("Failed to load generated certificate: %v", err)
	}

	if len(cert.Certificate) == 0 {
		t.Error("Expected certificate data")
	}
}

func TestTLSConfigALPN(t *testing.T) {
	config, err := GenerateSelfSignedCert()
	if err != nil {
		t.Fatalf("Failed to generate cert: %v", err)
	}

	// Verify ALPN includes h3 for HTTP/3
	found := false
	for _, proto := range config.NextProtos {
		if proto == "h3" {
			found = true
			break
		}
	}

	if !found {
		t.Error("Expected h3 in NextProtos for HTTP/3")
	}
}

func TestTLSConfigCipherSuites(t *testing.T) {
	config, err := GenerateSelfSignedCert()
	if err != nil {
		t.Fatalf("Failed to generate cert: %v", err)
	}

	// Verify only TLS 1.3 cipher suites are included
	validCiphers := map[uint16]bool{
		tls.TLS_AES_128_GCM_SHA256:       true,
		tls.TLS_AES_256_GCM_SHA384:       true,
		tls.TLS_CHACHA20_POLY1305_SHA256: true,
	}

	for _, cipher := range config.CipherSuites {
		if !validCiphers[cipher] {
			t.Errorf("Unexpected cipher suite: 0x%X", cipher)
		}
	}
}
