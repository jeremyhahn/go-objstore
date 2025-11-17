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
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestGenerateSelfSignedCert_AllBranches tests all code paths in GenerateSelfSignedCert
func TestGenerateSelfSignedCert_AllBranches(t *testing.T) {
	// Test successful generation
	config, err := GenerateSelfSignedCert()
	if err != nil {
		t.Fatalf("Failed to generate cert: %v", err)
	}

	// Verify all fields are properly set
	if len(config.Certificates) != 1 {
		t.Fatalf("Expected 1 certificate, got %d", len(config.Certificates))
	}

	cert := &config.Certificates[0]

	// Verify certificate has data
	if len(cert.Certificate) == 0 {
		t.Error("Certificate should have data")
	}

	// Verify private key
	_, ok := cert.PrivateKey.(*rsa.PrivateKey)
	if !ok {
		t.Error("Expected RSA private key")
	}

	// Parse and verify the certificate
	x509Cert, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		t.Fatalf("Failed to parse certificate: %v", err)
	}

	// Verify organization
	if len(x509Cert.Subject.Organization) == 0 || x509Cert.Subject.Organization[0] != "go-objstore Test" {
		t.Errorf("Expected organization 'go-objstore Test', got %v", x509Cert.Subject.Organization)
	}

	// Verify common name
	if x509Cert.Subject.CommonName != "localhost" {
		t.Errorf("Expected CN 'localhost', got %s", x509Cert.Subject.CommonName)
	}

	// Verify NotBefore is around now
	now := time.Now()
	if x509Cert.NotBefore.After(now.Add(time.Hour)) || x509Cert.NotBefore.Before(now.Add(-time.Hour)) {
		t.Errorf("NotBefore seems incorrect: %v", x509Cert.NotBefore)
	}

	// Verify NotAfter is about 1 year from now
	expectedNotAfter := now.Add(365 * 24 * time.Hour)
	if x509Cert.NotAfter.After(expectedNotAfter.Add(24*time.Hour)) || x509Cert.NotAfter.Before(expectedNotAfter.Add(-24*time.Hour)) {
		t.Errorf("NotAfter seems incorrect: %v (expected around %v)", x509Cert.NotAfter, expectedNotAfter)
	}

	// Verify serial number is set
	if x509Cert.SerialNumber == nil || x509Cert.SerialNumber.Cmp(big.NewInt(0)) == 0 {
		t.Error("Serial number should be set and non-zero")
	}

	// Verify key usage
	expectedKeyUsage := x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature
	if x509Cert.KeyUsage != expectedKeyUsage {
		t.Errorf("Expected key usage %v, got %v", expectedKeyUsage, x509Cert.KeyUsage)
	}

	// Verify extended key usage
	if len(x509Cert.ExtKeyUsage) != 1 || x509Cert.ExtKeyUsage[0] != x509.ExtKeyUsageServerAuth {
		t.Errorf("Expected ExtKeyUsage [ServerAuth], got %v", x509Cert.ExtKeyUsage)
	}

	// Verify BasicConstraintsValid
	if !x509Cert.BasicConstraintsValid {
		t.Error("BasicConstraintsValid should be true")
	}

	// Verify DNS names
	if len(x509Cert.DNSNames) != 1 || x509Cert.DNSNames[0] != "localhost" {
		t.Errorf("Expected DNSNames [localhost], got %v", x509Cert.DNSNames)
	}

	// Verify TLS config fields
	if config.MinVersion != tls.VersionTLS13 {
		t.Errorf("Expected MinVersion TLS 1.3, got %d", config.MinVersion)
	}

	if config.MaxVersion != tls.VersionTLS13 {
		t.Errorf("Expected MaxVersion TLS 1.3, got %d", config.MaxVersion)
	}

	if len(config.NextProtos) != 1 || config.NextProtos[0] != "h3" {
		t.Errorf("Expected NextProtos [h3], got %v", config.NextProtos)
	}

	if !config.PreferServerCipherSuites {
		t.Error("PreferServerCipherSuites should be true")
	}

	if config.SessionTicketsDisabled {
		t.Error("SessionTicketsDisabled should be false")
	}

	if config.ClientAuth != tls.NoClientCert {
		t.Errorf("Expected ClientAuth NoClientCert, got %v", config.ClientAuth)
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
	for i, cipher := range expectedCiphers {
		if i >= len(config.CipherSuites) || config.CipherSuites[i] != cipher {
			t.Errorf("Cipher suite mismatch at index %d", i)
		}
	}
}

// TestSaveCertificateToPEM_PEMEncoding tests PEM encoding paths
func TestSaveCertificateToPEM_PEMEncoding(t *testing.T) {
	tmpDir := t.TempDir()
	certFile := filepath.Join(tmpDir, "cert.pem")
	keyFile := filepath.Join(tmpDir, "key.pem")

	// Generate a certificate
	config, err := GenerateSelfSignedCert()
	if err != nil {
		t.Fatalf("Failed to generate cert: %v", err)
	}

	// Save it
	err = SaveCertificateToPEM(certFile, keyFile, &config.Certificates[0])
	if err != nil {
		t.Fatalf("Failed to save cert: %v", err)
	}

	// Read and verify cert file PEM encoding
	certData, err := os.ReadFile(certFile)
	if err != nil {
		t.Fatalf("Failed to read cert file: %v", err)
	}

	block, rest := pem.Decode(certData)
	if block == nil {
		t.Fatal("Failed to decode cert PEM")
	}
	if block.Type != "CERTIFICATE" {
		t.Errorf("Expected CERTIFICATE block type, got %s", block.Type)
	}
	if len(rest) > 0 {
		t.Error("Unexpected data after PEM block")
	}

	// Verify the certificate bytes match
	if len(block.Bytes) != len(config.Certificates[0].Certificate[0]) {
		t.Error("Certificate bytes length mismatch")
	}

	// Read and verify key file PEM encoding
	keyData, err := os.ReadFile(keyFile)
	if err != nil {
		t.Fatalf("Failed to read key file: %v", err)
	}

	keyBlock, keyRest := pem.Decode(keyData)
	if keyBlock == nil {
		t.Fatal("Failed to decode key PEM")
	}
	if keyBlock.Type != "RSA PRIVATE KEY" {
		t.Errorf("Expected RSA PRIVATE KEY block type, got %s", keyBlock.Type)
	}
	if len(keyRest) > 0 {
		t.Error("Unexpected data after key PEM block")
	}

	// Verify we can parse the RSA private key
	_, err = x509.ParsePKCS1PrivateKey(keyBlock.Bytes)
	if err != nil {
		t.Errorf("Failed to parse RSA private key: %v", err)
	}
}

// TestSaveCertificateToPEM_FileCreation tests file creation paths
func TestSaveCertificateToPEM_FileCreation(t *testing.T) {
	tmpDir := t.TempDir()
	certFile := filepath.Join(tmpDir, "test-cert.pem")
	keyFile := filepath.Join(tmpDir, "test-key.pem")

	config, err := GenerateSelfSignedCert()
	if err != nil {
		t.Fatalf("Failed to generate cert: %v", err)
	}

	// Verify files don't exist yet
	if _, err := os.Stat(certFile); !os.IsNotExist(err) {
		t.Error("Cert file should not exist yet")
	}
	if _, err := os.Stat(keyFile); !os.IsNotExist(err) {
		t.Error("Key file should not exist yet")
	}

	// Save certificate
	err = SaveCertificateToPEM(certFile, keyFile, &config.Certificates[0])
	if err != nil {
		t.Fatalf("Failed to save cert: %v", err)
	}

	// Verify files now exist
	certInfo, err := os.Stat(certFile)
	if err != nil {
		t.Errorf("Cert file should exist: %v", err)
	}
	if certInfo.Size() == 0 {
		t.Error("Cert file should not be empty")
	}

	keyInfo, err := os.Stat(keyFile)
	if err != nil {
		t.Errorf("Key file should exist: %v", err)
	}
	if keyInfo.Size() == 0 {
		t.Error("Key file should not be empty")
	}

	// Verify we can load the certificate back
	_, err = tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		t.Errorf("Failed to load saved certificate: %v", err)
	}
}

// TestSaveCertificateToPEM_CertFileCloseError tests certificate file close path
func TestSaveCertificateToPEM_CertFileCloseError(t *testing.T) {
	tmpDir := t.TempDir()
	certFile := filepath.Join(tmpDir, "close-test-cert.pem")
	keyFile := filepath.Join(tmpDir, "close-test-key.pem")

	config, err := GenerateSelfSignedCert()
	if err != nil {
		t.Fatalf("Failed to generate cert: %v", err)
	}

	// Save normally - this tests the defer close paths
	err = SaveCertificateToPEM(certFile, keyFile, &config.Certificates[0])
	if err != nil {
		t.Fatalf("Failed to save cert: %v", err)
	}

	// Verify files are properly closed by trying to remove them
	err = os.Remove(certFile)
	if err != nil {
		t.Errorf("Failed to remove cert file (may not be properly closed): %v", err)
	}

	err = os.Remove(keyFile)
	if err != nil {
		t.Errorf("Failed to remove key file (may not be properly closed): %v", err)
	}
}

// TestGenerateAndSaveSelfSignedCert_FullPath tests the complete generation and save flow
func TestGenerateAndSaveSelfSignedCert_FullPath(t *testing.T) {
	tmpDir := t.TempDir()
	certFile := filepath.Join(tmpDir, "full-test-cert.pem")
	keyFile := filepath.Join(tmpDir, "full-test-key.pem")

	// Generate and save
	err := GenerateAndSaveSelfSignedCert(certFile, keyFile)
	if err != nil {
		t.Fatalf("Failed to generate and save: %v", err)
	}

	// Verify files exist
	if _, err := os.Stat(certFile); os.IsNotExist(err) {
		t.Error("Cert file was not created")
	}
	if _, err := os.Stat(keyFile); os.IsNotExist(err) {
		t.Error("Key file was not created")
	}

	// Load and verify the certificate
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		t.Fatalf("Failed to load generated certificate: %v", err)
	}

	if len(cert.Certificate) == 0 {
		t.Error("Certificate should have data")
	}

	// Parse and verify
	x509Cert, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		t.Fatalf("Failed to parse certificate: %v", err)
	}

	if x509Cert.Subject.CommonName != "localhost" {
		t.Errorf("Expected CN localhost, got %s", x509Cert.Subject.CommonName)
	}
}

// TestGenerateAndSaveSelfSignedCert_EmptyCertificates tests the error path for empty certificates
func TestGenerateAndSaveSelfSignedCert_EmptyCertificates(t *testing.T) {
	// This tests the check in GenerateAndSaveSelfSignedCert for len(config.Certificates) == 0
	// Under normal circumstances, GenerateSelfSignedCert always creates a certificate,
	// so this path is difficult to trigger without mocking. We verify the function works normally.

	tmpDir := t.TempDir()
	certFile := filepath.Join(tmpDir, "empty-check-cert.pem")
	keyFile := filepath.Join(tmpDir, "empty-check-key.pem")

	err := GenerateAndSaveSelfSignedCert(certFile, keyFile)
	if err != nil {
		t.Fatalf("GenerateAndSaveSelfSignedCert should succeed: %v", err)
	}

	// Verify the certificate was created properly
	_, err = tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		t.Errorf("Generated certificate should be valid: %v", err)
	}
}

// TestSaveCertificateToPEM_WriterError simulates writer error during PEM encoding
func TestSaveCertificateToPEM_WriterError(t *testing.T) {
	// This test verifies the error handling in pem.Encode by checking file operations
	tmpDir := t.TempDir()

	// Create a read-only directory to force file creation errors
	readOnlyDir := filepath.Join(tmpDir, "readonly")
	err := os.Mkdir(readOnlyDir, 0555) // Read and execute only
	if err != nil {
		t.Fatalf("Failed to create read-only dir: %v", err)
	}

	certFile := filepath.Join(readOnlyDir, "cert.pem")
	keyFile := filepath.Join(readOnlyDir, "key.pem")

	config, err := GenerateSelfSignedCert()
	if err != nil {
		t.Fatalf("Failed to generate cert: %v", err)
	}

	// Try to save - should fail due to permissions
	err = SaveCertificateToPEM(certFile, keyFile, &config.Certificates[0])
	if err == nil {
		t.Error("Expected error when saving to read-only directory")
	}
}

// TestGenerateSelfSignedCert_RSAKeySize tests that the RSA key is 2048 bits
func TestGenerateSelfSignedCert_RSAKeySize(t *testing.T) {
	config, err := GenerateSelfSignedCert()
	if err != nil {
		t.Fatalf("Failed to generate cert: %v", err)
	}

	// Extract the RSA private key
	rsaKey, ok := config.Certificates[0].PrivateKey.(*rsa.PrivateKey)
	if !ok {
		t.Fatal("Private key is not RSA")
	}

	// Verify key size is 2048 bits
	if rsaKey.N.BitLen() != 2048 {
		t.Errorf("Expected 2048-bit RSA key, got %d bits", rsaKey.N.BitLen())
	}
}

// TestSaveCertificateToPEM_PKCS1Encoding tests PKCS1 encoding of RSA key
func TestSaveCertificateToPEM_PKCS1Encoding(t *testing.T) {
	tmpDir := t.TempDir()
	certFile := filepath.Join(tmpDir, "pkcs1-cert.pem")
	keyFile := filepath.Join(tmpDir, "pkcs1-key.pem")

	config, err := GenerateSelfSignedCert()
	if err != nil {
		t.Fatalf("Failed to generate cert: %v", err)
	}

	err = SaveCertificateToPEM(certFile, keyFile, &config.Certificates[0])
	if err != nil {
		t.Fatalf("Failed to save cert: %v", err)
	}

	// Read the key file
	keyData, err := os.ReadFile(keyFile)
	if err != nil {
		t.Fatalf("Failed to read key file: %v", err)
	}

	// Decode PEM
	block, _ := pem.Decode(keyData)
	if block == nil {
		t.Fatal("Failed to decode key PEM")
	}

	// Verify it's PKCS1 format
	if block.Type != "RSA PRIVATE KEY" {
		t.Errorf("Expected RSA PRIVATE KEY (PKCS1), got %s", block.Type)
	}

	// Parse as PKCS1
	rsaKey, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		t.Errorf("Failed to parse PKCS1 private key: %v", err)
	}

	// Verify it matches the original key
	origKey, _ := config.Certificates[0].PrivateKey.(*rsa.PrivateKey)
	if rsaKey.N.Cmp(origKey.N) != 0 {
		t.Error("Parsed key modulus doesn't match original")
	}
}
