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
	"crypto/tls"
	"crypto/x509"
	"errors"
	"os"
)

var (
	// ErrInvalidCertificate is returned when a certificate is invalid.
	ErrInvalidCertificate = errors.New("invalid certificate")

	// ErrInvalidCAPool is returned when the CA pool is invalid.
	ErrInvalidCAPool = errors.New("invalid CA pool")
)

// TLSMode defines the TLS configuration mode.
type TLSMode int

const (
	// TLSModeDisabled disables TLS entirely.
	TLSModeDisabled TLSMode = iota

	// TLSModeServer enables TLS with server certificate only.
	TLSModeServer

	// TLSModeMutual enables mTLS (mutual TLS) requiring client certificates.
	TLSModeMutual
)

// TLSConfig holds TLS/mTLS configuration.
type TLSConfig struct {
	// Mode specifies the TLS mode (disabled, server, mutual).
	Mode TLSMode

	// ServerCertFile is the path to the server certificate file (PEM format).
	ServerCertFile string

	// ServerKeyFile is the path to the server private key file (PEM format).
	ServerKeyFile string

	// ServerCertPEM is the server certificate in PEM format (alternative to ServerCertFile).
	ServerCertPEM []byte

	// ServerKeyPEM is the server private key in PEM format (alternative to ServerKeyFile).
	ServerKeyPEM []byte

	// ClientCAFile is the path to the CA certificate file for validating client certificates.
	ClientCAFile string

	// ClientCAPEM is the CA certificate in PEM format (alternative to ClientCAFile).
	ClientCAPEM []byte

	// MinVersion specifies the minimum TLS version (default: TLS 1.2).
	MinVersion uint16

	// MaxVersion specifies the maximum TLS version (0 = no max).
	MaxVersion uint16

	// CipherSuites specifies the list of enabled cipher suites (nil = Go defaults).
	CipherSuites []uint16

	// ClientAuth specifies the client authentication mode for mTLS.
	// Default for mutual mode is RequireAndVerifyClientCert.
	ClientAuth tls.ClientAuthType

	// InsecureSkipVerify skips verification of client certificates (not recommended for production).
	InsecureSkipVerify bool
}

// NewTLSConfig creates a TLS configuration with secure defaults.
func NewTLSConfig() *TLSConfig {
	return &TLSConfig{
		Mode:       TLSModeDisabled,
		MinVersion: tls.VersionTLS12, // Secure default
		ClientAuth: tls.RequireAndVerifyClientCert,
	}
}

// WithServerCertFiles sets the server certificate and key from files.
func (c *TLSConfig) WithServerCertFiles(certFile, keyFile string) *TLSConfig {
	c.Mode = TLSModeServer
	c.ServerCertFile = certFile
	c.ServerKeyFile = keyFile
	return c
}

// WithServerCertPEM sets the server certificate and key from PEM data.
func (c *TLSConfig) WithServerCertPEM(certPEM, keyPEM []byte) *TLSConfig {
	c.Mode = TLSModeServer
	c.ServerCertPEM = certPEM
	c.ServerKeyPEM = keyPEM
	return c
}

// WithClientCAFile enables mTLS with client CA from file.
func (c *TLSConfig) WithClientCAFile(caFile string) *TLSConfig {
	c.Mode = TLSModeMutual
	c.ClientCAFile = caFile
	return c
}

// WithClientCAPEM enables mTLS with client CA from PEM data.
func (c *TLSConfig) WithClientCAPEM(caPEM []byte) *TLSConfig {
	c.Mode = TLSModeMutual
	c.ClientCAPEM = caPEM
	return c
}

// WithMinVersion sets the minimum TLS version.
func (c *TLSConfig) WithMinVersion(version uint16) *TLSConfig {
	c.MinVersion = version
	return c
}

// WithInsecureSkipVerify disables client certificate verification (use with caution).
func (c *TLSConfig) WithInsecureSkipVerify(skip bool) *TLSConfig {
	c.InsecureSkipVerify = skip
	return c
}

// Build creates a *tls.Config from the TLSConfig.
func (c *TLSConfig) Build() (*tls.Config, error) {
	if c.Mode == TLSModeDisabled {
		return nil, nil
	}

	config := &tls.Config{
		MinVersion:         c.MinVersion,
		MaxVersion:         c.MaxVersion,
		CipherSuites:       c.CipherSuites,
		InsecureSkipVerify: c.InsecureSkipVerify, // #nosec G402 -- Configurable option for testing/development, defaults to false
	}

	// Load server certificate
	var cert tls.Certificate
	var err error

	if len(c.ServerCertPEM) > 0 && len(c.ServerKeyPEM) > 0 {
		cert, err = tls.X509KeyPair(c.ServerCertPEM, c.ServerKeyPEM)
		if err != nil {
			return nil, ErrInvalidCertificate
		}
	} else if c.ServerCertFile != "" && c.ServerKeyFile != "" {
		cert, err = tls.LoadX509KeyPair(c.ServerCertFile, c.ServerKeyFile)
		if err != nil {
			return nil, ErrInvalidCertificate
		}
	} else {
		return nil, ErrInvalidCertificate
	}

	config.Certificates = []tls.Certificate{cert}

	// Configure mTLS if enabled
	if c.Mode == TLSModeMutual {
		// Load client CA pool
		var caPool *x509.CertPool

		if len(c.ClientCAPEM) > 0 {
			caPool = x509.NewCertPool()
			if !caPool.AppendCertsFromPEM(c.ClientCAPEM) {
				return nil, ErrInvalidCAPool
			}
		} else if c.ClientCAFile != "" {
			caData, err := os.ReadFile(c.ClientCAFile)
			if err != nil {
				return nil, ErrInvalidCAPool
			}
			caPool = x509.NewCertPool()
			if !caPool.AppendCertsFromPEM(caData) {
				return nil, ErrInvalidCAPool
			}
		} else {
			return nil, ErrInvalidCAPool
		}

		config.ClientCAs = caPool
		config.ClientAuth = c.ClientAuth
	}

	return config, nil
}

// LoadTLSConfigFromFiles is a convenience function to create a TLS config from files.
func LoadTLSConfigFromFiles(certFile, keyFile, caFile string) (*tls.Config, error) {
	tlsConfig := NewTLSConfig().WithServerCertFiles(certFile, keyFile)

	if caFile != "" {
		tlsConfig.WithClientCAFile(caFile)
	}

	return tlsConfig.Build()
}

// LoadMTLSConfigFromFiles is a convenience function to create an mTLS config from files.
func LoadMTLSConfigFromFiles(certFile, keyFile, caFile string) (*tls.Config, error) {
	if caFile == "" {
		return nil, ErrInvalidCAPool
	}

	tlsConfig := NewTLSConfig().
		WithServerCertFiles(certFile, keyFile).
		WithClientCAFile(caFile)

	return tlsConfig.Build()
}

// CreateSelfSignedCert generates a self-signed certificate for testing purposes.
// This should NEVER be used in production.
func CreateSelfSignedCert() (certPEM, keyPEM []byte, err error) {
	// This is a placeholder - in a real implementation, we'd use crypto/x509 to generate
	// a self-signed certificate. For now, we'll return an error indicating this needs to be implemented.
	return nil, nil, errors.New("self-signed certificate generation not implemented - use proper certificates")
}

// ValidateClientCertificate validates a client certificate against the CA pool.
func ValidateClientCertificate(cert *x509.Certificate, caPool *x509.CertPool) error {
	opts := x509.VerifyOptions{
		Roots:     caPool,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}

	if _, err := cert.Verify(opts); err != nil {
		return ErrInvalidCertificate
	}

	return nil
}
