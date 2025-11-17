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
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
)

// NewTLSConfig creates a new TLS 1.3 configuration for QUIC.
// This requires certificates for production use.
func NewTLSConfig(certFile, keyFile string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS13, // QUIC requires TLS 1.3
		MaxVersion:   tls.VersionTLS13,
		NextProtos:   []string{"h3"}, // HTTP/3 ALPN
		CipherSuites: []uint16{
			tls.TLS_AES_128_GCM_SHA256,
			tls.TLS_AES_256_GCM_SHA384,
			tls.TLS_CHACHA20_POLY1305_SHA256,
		},
		// Prefer server cipher suites
		PreferServerCipherSuites: true,
		// Session tickets for connection resumption
		SessionTicketsDisabled: false,
		// Client authentication (optional, can be required for mTLS)
		ClientAuth: tls.NoClientCert,
	}, nil
}

// NewTLSConfigWithClientAuth creates a TLS 1.3 configuration that requires client certificates.
func NewTLSConfigWithClientAuth(certFile, keyFile, caCertFile string) (*tls.Config, error) {
	config, err := NewTLSConfig(certFile, keyFile)
	if err != nil {
		return nil, err
	}

	// Load CA certificate for client verification
	caCert, err := os.ReadFile(caCertFile) // #nosec G304 -- TLS certificate paths are controlled by application configuration
	if err != nil {
		return nil, err
	}

	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		return nil, adapters.ErrInvalidCAPool
	}

	config.ClientCAs = caCertPool
	config.ClientAuth = tls.RequireAndVerifyClientCert

	return config, nil
}

// GenerateSelfSignedCert generates a self-signed certificate for testing.
// This should NOT be used in production.
func GenerateSelfSignedCert() (*tls.Config, error) {
	// Generate private key
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, err
	}

	// Create certificate template
	notBefore := time.Now()
	notAfter := notBefore.Add(365 * 24 * time.Hour) // Valid for 1 year

	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		return nil, err
	}

	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"go-objstore Test"},
			CommonName:   "localhost",
		},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{"localhost"},
	}

	// Create self-signed certificate
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		return nil, err
	}

	// Create TLS certificate
	tlsCert := tls.Certificate{
		Certificate: [][]byte{certDER},
		PrivateKey:  priv,
	}

	return &tls.Config{
		Certificates:             []tls.Certificate{tlsCert},
		MinVersion:               tls.VersionTLS13,
		MaxVersion:               tls.VersionTLS13,
		NextProtos:               []string{"h3"},
		PreferServerCipherSuites: true,
		SessionTicketsDisabled:   false,
		ClientAuth:               tls.NoClientCert,
		CipherSuites: []uint16{
			tls.TLS_AES_128_GCM_SHA256,
			tls.TLS_AES_256_GCM_SHA384,
			tls.TLS_CHACHA20_POLY1305_SHA256,
		},
	}, nil
}

// SaveCertificateToPEM saves a certificate and private key to PEM files.
func SaveCertificateToPEM(certFile, keyFile string, cert *tls.Certificate) error {
	// Save certificate
	certOut, err := os.Create(certFile) // #nosec G304 -- TLS certificate paths are controlled by application configuration
	if err != nil {
		return err
	}
	defer certOut.Close()

	if err := pem.Encode(certOut, &pem.Block{
		Type:  "CERTIFICATE",
		Bytes: cert.Certificate[0],
	}); err != nil {
		return err
	}

	// Save private key
	keyOut, err := os.Create(keyFile) // #nosec G304 -- TLS certificate paths are controlled by application configuration
	if err != nil {
		return err
	}
	defer keyOut.Close()

	privKey, ok := cert.PrivateKey.(*rsa.PrivateKey)
	if !ok {
		return adapters.ErrInvalidCertificate
	}

	privBytes := x509.MarshalPKCS1PrivateKey(privKey)
	if err := pem.Encode(keyOut, &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: privBytes,
	}); err != nil {
		return err
	}

	return nil
}

// GenerateAndSaveSelfSignedCert generates and saves a self-signed certificate.
func GenerateAndSaveSelfSignedCert(certFile, keyFile string) error {
	config, err := GenerateSelfSignedCert()
	if err != nil {
		return err
	}

	if len(config.Certificates) == 0 {
		return adapters.ErrInvalidCertificate
	}

	return SaveCertificateToPEM(certFile, keyFile, &config.Certificates[0])
}
