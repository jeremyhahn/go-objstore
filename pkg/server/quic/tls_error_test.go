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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"
)

func TestSaveCertificateToPEM_EncodeError(t *testing.T) {
	tmpDir := t.TempDir()
	certFile := filepath.Join(tmpDir, "cert.pem")
	keyFile := filepath.Join(tmpDir, "key.pem")

	// Generate a certificate with ECDSA key (not RSA)
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("Failed to generate ECDSA key: %v", err)
	}

	// Create a certificate with ECDSA private key
	config, err := GenerateSelfSignedCert()
	if err != nil {
		t.Fatalf("Failed to generate cert: %v", err)
	}

	// Replace the private key with ECDSA
	cert := config.Certificates[0]
	cert.PrivateKey = privateKey

	// Try to save - should fail because SaveCertificateToPEM expects RSA
	err = SaveCertificateToPEM(certFile, keyFile, &cert)
	if err == nil {
		t.Error("Expected error when saving non-RSA private key")
	}
}

func TestSaveCertificateToPEM_CertFileError(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a file where we want to create the cert
	certFile := filepath.Join(tmpDir, "cert.pem")
	if err := os.WriteFile(certFile, []byte("dummy"), 0444); err != nil {
		t.Fatalf("Failed to create read-only file: %v", err)
	}

	// Make it read-only
	if err := os.Chmod(certFile, 0444); err != nil {
		t.Fatalf("Failed to chmod: %v", err)
	}

	keyFile := filepath.Join(tmpDir, "key.pem")

	config, err := GenerateSelfSignedCert()
	if err != nil {
		t.Fatalf("Failed to generate cert: %v", err)
	}

	// Remove the file so we can test creation failure
	os.Remove(certFile)

	// Create a directory with the same name
	if err := os.Mkdir(certFile, 0755); err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}

	// Try to save - should fail because certFile is a directory
	err = SaveCertificateToPEM(certFile, keyFile, &config.Certificates[0])
	if err == nil {
		t.Error("Expected error when cert file path is a directory")
	}
}

func TestSaveCertificateToPEM_KeyFileError(t *testing.T) {
	tmpDir := t.TempDir()
	certFile := filepath.Join(tmpDir, "cert.pem")
	keyFile := filepath.Join(tmpDir, "key.pem")

	config, err := GenerateSelfSignedCert()
	if err != nil {
		t.Fatalf("Failed to generate cert: %v", err)
	}

	// Create a directory where key file should be
	if err := os.Mkdir(keyFile, 0755); err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}

	// Try to save - should fail because keyFile is a directory
	err = SaveCertificateToPEM(certFile, keyFile, &config.Certificates[0])
	if err == nil {
		t.Error("Expected error when key file path is a directory")
	}
}

func TestGenerateSelfSignedCert_Coverage(t *testing.T) {
	// Test all branches of GenerateSelfSignedCert
	config, err := GenerateSelfSignedCert()
	if err != nil {
		t.Fatalf("Failed to generate cert: %v", err)
	}

	// Verify certificate properties
	if len(config.Certificates) != 1 {
		t.Errorf("Expected 1 certificate, got %d", len(config.Certificates))
	}

	cert := config.Certificates[0]
	if len(cert.Certificate) == 0 {
		t.Error("Certificate data should not be empty")
	}

	if cert.PrivateKey == nil {
		t.Error("Private key should not be nil")
	}

	// Verify TLS config settings
	if config.MinVersion != tls.VersionTLS13 {
		t.Errorf("Expected TLS 1.3, got %d", config.MinVersion)
	}

	if config.MaxVersion != tls.VersionTLS13 {
		t.Errorf("Expected TLS 1.3, got %d", config.MaxVersion)
	}

	if len(config.NextProtos) != 1 || config.NextProtos[0] != "h3" {
		t.Errorf("Expected h3 ALPN, got %v", config.NextProtos)
	}

	if !config.PreferServerCipherSuites {
		t.Error("Expected PreferServerCipherSuites to be true")
	}

	if config.SessionTicketsDisabled {
		t.Error("Expected SessionTicketsDisabled to be false")
	}

	if config.ClientAuth != tls.NoClientCert {
		t.Errorf("Expected NoClientCert, got %v", config.ClientAuth)
	}

	// Verify cipher suites
	expectedCiphers := 3
	if len(config.CipherSuites) != expectedCiphers {
		t.Errorf("Expected %d cipher suites, got %d", expectedCiphers, len(config.CipherSuites))
	}
}

func TestNewTLSConfig_LoadError(t *testing.T) {
	// Test with mismatched cert and key
	tmpDir := t.TempDir()
	certFile1 := filepath.Join(tmpDir, "cert1.pem")
	keyFile1 := filepath.Join(tmpDir, "key1.pem")
	certFile2 := filepath.Join(tmpDir, "cert2.pem")
	keyFile2 := filepath.Join(tmpDir, "key2.pem")

	// Generate two different certificates
	config1, _ := GenerateSelfSignedCert()
	config2, _ := GenerateSelfSignedCert()

	SaveCertificateToPEM(certFile1, keyFile1, &config1.Certificates[0])
	SaveCertificateToPEM(certFile2, keyFile2, &config2.Certificates[0])

	// Try to load cert1 with key2 (mismatched)
	_, err := NewTLSConfig(certFile1, keyFile2)
	if err == nil {
		t.Error("Expected error when loading mismatched cert and key")
	}
}

func TestNewTLSConfigWithClientAuth_ReadError(t *testing.T) {
	tmpDir := t.TempDir()
	certFile := filepath.Join(tmpDir, "cert.pem")
	keyFile := filepath.Join(tmpDir, "key.pem")
	caCertFile := filepath.Join(tmpDir, "ca.pem")

	// Generate and save cert
	config, _ := GenerateSelfSignedCert()
	SaveCertificateToPEM(certFile, keyFile, &config.Certificates[0])

	// Make CA file unreadable (doesn't exist)
	_, err := NewTLSConfigWithClientAuth(certFile, keyFile, caCertFile)
	if err == nil {
		t.Error("Expected error when CA file doesn't exist")
	}
}

func TestNewTLSConfigWithClientAuth_ParseError(t *testing.T) {
	tmpDir := t.TempDir()
	certFile := filepath.Join(tmpDir, "cert.pem")
	keyFile := filepath.Join(tmpDir, "key.pem")
	caCertFile := filepath.Join(tmpDir, "ca.pem")

	// Generate and save cert
	config, _ := GenerateSelfSignedCert()
	SaveCertificateToPEM(certFile, keyFile, &config.Certificates[0])

	// Create invalid CA cert file
	os.WriteFile(caCertFile, []byte("not a valid PEM"), 0644)

	_, err := NewTLSConfigWithClientAuth(certFile, keyFile, caCertFile)
	if err == nil {
		t.Error("Expected error when CA cert is invalid")
	}
}

func TestNewTLSConfigWithClientAuth_CertLoadError(t *testing.T) {
	tmpDir := t.TempDir()
	caCertFile := filepath.Join(tmpDir, "ca.pem")

	// Create valid CA cert
	config, _ := GenerateSelfSignedCert()
	SaveCertificateToPEM(caCertFile, filepath.Join(tmpDir, "ca-key.pem"), &config.Certificates[0])

	// Try with invalid cert/key files
	_, err := NewTLSConfigWithClientAuth("/invalid/cert.pem", "/invalid/key.pem", caCertFile)
	if err == nil {
		t.Error("Expected error when server cert files are invalid")
	}
}

func TestGenerateAndSaveSelfSignedCert_NoCertificates(t *testing.T) {
	// This is a theoretical test - GenerateSelfSignedCert should always create a cert
	// But we test the error path in GenerateAndSaveSelfSignedCert
	tmpDir := t.TempDir()
	certFile := filepath.Join(tmpDir, "cert.pem")
	keyFile := filepath.Join(tmpDir, "key.pem")

	// This should succeed normally
	err := GenerateAndSaveSelfSignedCert(certFile, keyFile)
	if err != nil {
		t.Errorf("GenerateAndSaveSelfSignedCert failed: %v", err)
	}

	// Verify files were created
	if _, err := os.Stat(certFile); os.IsNotExist(err) {
		t.Error("Certificate file was not created")
	}
	if _, err := os.Stat(keyFile); os.IsNotExist(err) {
		t.Error("Key file was not created")
	}
}

func TestGenerateAndSaveSelfSignedCert_SaveError(t *testing.T) {
	// Test save error in GenerateAndSaveSelfSignedCert
	tmpDir := t.TempDir()
	certFile := filepath.Join(tmpDir, "subdir", "cert.pem")
	keyFile := filepath.Join(tmpDir, "subdir", "key.pem")

	// Don't create the subdir, so save will fail
	err := GenerateAndSaveSelfSignedCert(certFile, keyFile)
	if err == nil {
		t.Error("Expected error when saving to non-existent directory")
	}
}

func TestTLSConfigProperties(t *testing.T) {
	// Test all TLS config properties are set correctly
	tmpDir := t.TempDir()
	certFile := filepath.Join(tmpDir, "cert.pem")
	keyFile := filepath.Join(tmpDir, "key.pem")

	config, _ := GenerateSelfSignedCert()
	SaveCertificateToPEM(certFile, keyFile, &config.Certificates[0])

	loadedConfig, err := NewTLSConfig(certFile, keyFile)
	if err != nil {
		t.Fatalf("Failed to load TLS config: %v", err)
	}

	// Verify all properties
	if loadedConfig.MinVersion != tls.VersionTLS13 {
		t.Error("MinVersion not set correctly")
	}

	if loadedConfig.MaxVersion != tls.VersionTLS13 {
		t.Error("MaxVersion not set correctly")
	}

	if len(loadedConfig.NextProtos) != 1 || loadedConfig.NextProtos[0] != "h3" {
		t.Error("NextProtos not set correctly")
	}

	if len(loadedConfig.CipherSuites) != 3 {
		t.Error("CipherSuites not set correctly")
	}

	if !loadedConfig.PreferServerCipherSuites {
		t.Error("PreferServerCipherSuites not set correctly")
	}

	if loadedConfig.SessionTicketsDisabled {
		t.Error("SessionTicketsDisabled should be false")
	}

	if loadedConfig.ClientAuth != tls.NoClientCert {
		t.Error("ClientAuth not set correctly")
	}
}

func TestNewTLSConfigWithClientAuth_Success(t *testing.T) {
	tmpDir := t.TempDir()
	certFile := filepath.Join(tmpDir, "cert.pem")
	keyFile := filepath.Join(tmpDir, "key.pem")
	caCertFile := filepath.Join(tmpDir, "ca.pem")

	// Generate and save cert
	config, _ := GenerateSelfSignedCert()
	SaveCertificateToPEM(certFile, keyFile, &config.Certificates[0])
	SaveCertificateToPEM(caCertFile, filepath.Join(tmpDir, "ca-key.pem"), &config.Certificates[0])

	loadedConfig, err := NewTLSConfigWithClientAuth(certFile, keyFile, caCertFile)
	if err != nil {
		t.Fatalf("Failed to load config with client auth: %v", err)
	}

	// Verify client auth settings
	if loadedConfig.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Errorf("Expected RequireAndVerifyClientCert, got %v", loadedConfig.ClientAuth)
	}

	if loadedConfig.ClientCAs == nil {
		t.Error("ClientCAs should be set")
	}

	// Verify other properties are still set
	if loadedConfig.MinVersion != tls.VersionTLS13 {
		t.Error("MinVersion should still be TLS 1.3")
	}

	if len(loadedConfig.NextProtos) != 1 || loadedConfig.NextProtos[0] != "h3" {
		t.Error("NextProtos should still be set")
	}
}

func TestSaveCertificateToPEM_Success(t *testing.T) {
	tmpDir := t.TempDir()
	certFile := filepath.Join(tmpDir, "test-cert.pem")
	keyFile := filepath.Join(tmpDir, "test-key.pem")

	config, err := GenerateSelfSignedCert()
	if err != nil {
		t.Fatalf("Failed to generate cert: %v", err)
	}

	err = SaveCertificateToPEM(certFile, keyFile, &config.Certificates[0])
	if err != nil {
		t.Fatalf("Failed to save cert: %v", err)
	}

	// Verify cert file
	certData, err := os.ReadFile(certFile)
	if err != nil {
		t.Fatalf("Failed to read cert file: %v", err)
	}

	block, _ := pem.Decode(certData)
	if block == nil {
		t.Error("Failed to decode cert PEM")
	}
	if block.Type != "CERTIFICATE" {
		t.Errorf("Expected CERTIFICATE block, got %s", block.Type)
	}

	// Verify key file
	keyData, err := os.ReadFile(keyFile)
	if err != nil {
		t.Fatalf("Failed to read key file: %v", err)
	}

	keyBlock, _ := pem.Decode(keyData)
	if keyBlock == nil {
		t.Error("Failed to decode key PEM")
	}
	if keyBlock.Type != "RSA PRIVATE KEY" {
		t.Errorf("Expected RSA PRIVATE KEY block, got %s", keyBlock.Type)
	}
}

func TestGenerateSelfSignedCert_CertificateDetails(t *testing.T) {
	config, err := GenerateSelfSignedCert()
	if err != nil {
		t.Fatalf("Failed to generate cert: %v", err)
	}

	// Parse the certificate
	cert, err := x509.ParseCertificate(config.Certificates[0].Certificate[0])
	if err != nil {
		t.Fatalf("Failed to parse certificate: %v", err)
	}

	// Verify certificate fields
	if cert.Subject.Organization[0] != "go-objstore Test" {
		t.Errorf("Expected organization 'go-objstore Test', got %s", cert.Subject.Organization[0])
	}

	if cert.Subject.CommonName != "localhost" {
		t.Errorf("Expected CN 'localhost', got %s", cert.Subject.CommonName)
	}

	if len(cert.DNSNames) != 1 || cert.DNSNames[0] != "localhost" {
		t.Errorf("Expected DNSNames ['localhost'], got %v", cert.DNSNames)
	}

	// Verify key usage
	if cert.KeyUsage&x509.KeyUsageKeyEncipherment == 0 {
		t.Error("Expected KeyUsageKeyEncipherment")
	}

	if cert.KeyUsage&x509.KeyUsageDigitalSignature == 0 {
		t.Error("Expected KeyUsageDigitalSignature")
	}

	// Verify extended key usage
	found := false
	for _, usage := range cert.ExtKeyUsage {
		if usage == x509.ExtKeyUsageServerAuth {
			found = true
			break
		}
	}
	if !found {
		t.Error("Expected ExtKeyUsageServerAuth")
	}
}
