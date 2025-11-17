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
	"os"
	"path/filepath"
	"testing"
)

func TestSaveCertificateToPEM_InvalidDirectory(t *testing.T) {
	config, err := GenerateSelfSignedCert()
	if err != nil {
		t.Fatalf("Failed to generate cert: %v", err)
	}

	// Try to save to invalid directory
	err = SaveCertificateToPEM("/nonexistent/dir/cert.pem", "/nonexistent/dir/key.pem", &config.Certificates[0])
	if err == nil {
		t.Error("Expected error when saving to invalid directory")
	}
}

func TestSaveCertificateToPEM_ReadOnlyDirectory(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("Skipping test when running as root")
	}

	config, err := GenerateSelfSignedCert()
	if err != nil {
		t.Fatalf("Failed to generate cert: %v", err)
	}

	tmpDir := t.TempDir()
	readOnlyDir := filepath.Join(tmpDir, "readonly")
	if err := os.Mkdir(readOnlyDir, 0555); err != nil {
		t.Fatalf("Failed to create read-only directory: %v", err)
	}

	certFile := filepath.Join(readOnlyDir, "cert.pem")
	keyFile := filepath.Join(readOnlyDir, "key.pem")

	// Try to save to read-only directory
	err = SaveCertificateToPEM(certFile, keyFile, &config.Certificates[0])
	if err == nil {
		t.Error("Expected error when saving to read-only directory")
	}
}

func TestGenerateAndSaveSelfSignedCert_InvalidPath(t *testing.T) {
	err := GenerateAndSaveSelfSignedCert("/nonexistent/dir/cert.pem", "/nonexistent/dir/key.pem")
	if err == nil {
		t.Error("Expected error for invalid path")
	}
}

func TestNewTLSConfigWithClientAuth_InvalidCertContent(t *testing.T) {
	tmpDir := t.TempDir()
	certFile := filepath.Join(tmpDir, "cert.pem")
	keyFile := filepath.Join(tmpDir, "key.pem")
	caCertFile := filepath.Join(tmpDir, "ca.pem")

	// Generate valid cert and key
	config, err := GenerateSelfSignedCert()
	if err != nil {
		t.Fatalf("Failed to generate cert: %v", err)
	}

	err = SaveCertificateToPEM(certFile, keyFile, &config.Certificates[0])
	if err != nil {
		t.Fatalf("Failed to save cert: %v", err)
	}

	// Create invalid CA file
	if err := os.WriteFile(caCertFile, []byte("invalid cert data"), 0644); err != nil {
		t.Fatalf("Failed to write invalid CA cert: %v", err)
	}

	// Should fail to parse CA cert
	_, err = NewTLSConfigWithClientAuth(certFile, keyFile, caCertFile)
	if err == nil {
		t.Error("Expected error for invalid CA certificate content")
	}
}

func TestGenerateSelfSignedCert_VerifyFields(t *testing.T) {
	config, err := GenerateSelfSignedCert()
	if err != nil {
		t.Fatalf("Failed to generate cert: %v", err)
	}

	// Verify the certificate has expected properties
	if len(config.Certificates) != 1 {
		t.Errorf("Expected 1 certificate, got %d", len(config.Certificates))
	}

	cert := config.Certificates[0]
	if len(cert.Certificate) == 0 {
		t.Error("Expected certificate data")
	}

	if cert.PrivateKey == nil {
		t.Error("Expected private key")
	}

	// Verify session tickets are enabled (for performance)
	if config.SessionTicketsDisabled {
		t.Error("Expected session tickets to be enabled")
	}

	// Verify server cipher preference
	if !config.PreferServerCipherSuites {
		t.Error("Expected server cipher preference")
	}
}
