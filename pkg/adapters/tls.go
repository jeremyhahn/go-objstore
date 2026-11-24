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

	"github.com/jeremyhahn/go-enclave"
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

	// ServerKeyPEM is the server private key in PEM format.
	//
	// Deprecated: the key sits in ordinary heap memory; use WithServerKeyEnclave. When both are set, the enclave wins.
	ServerKeyPEM []byte

	// serverKeyEnclave holds the server private key in locked, zeroized
	// memory. It is unexported so that the key can only be reached through
	// the enclave API, never copied out of the struct by a consumer.
	serverKeyEnclave enclave.ByteAccess

	// releaseErr collects the errors from destroying a replaced enclave in
	// the builder chain, which has no error return. Destroy reports it.
	releaseErr error

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

// selectServerKeySource clears every key source except keepEnclave, destroying a discarded enclave and zeroizing ServerKeyPEM, so a rotation never serves a superseded key.
func (c *TLSConfig) selectServerKeySource(keepEnclave enclave.ByteAccess) {
	if c.serverKeyEnclave != nil && c.serverKeyEnclave != keepEnclave {
		c.releaseErr = errors.Join(c.releaseErr, c.serverKeyEnclave.Destroy())
	}
	c.serverKeyEnclave = nil

	enclave.Wipe(c.ServerKeyPEM)
	c.ServerKeyPEM = nil

	c.ServerCertPEM = nil
	c.ServerCertFile = ""
	c.ServerKeyFile = ""
}

// WithServerCertFiles selects the file source, destroying any enclave and zeroizing ServerKeyPEM.
func (c *TLSConfig) WithServerCertFiles(certFile, keyFile string) *TLSConfig {
	c.Mode = TLSModeServer
	c.selectServerKeySource(nil)
	c.ServerCertFile = certFile
	c.ServerKeyFile = keyFile
	return c
}

// WithServerCertPEM selects the PEM source, destroying any enclave and clearing file paths.
//
// Deprecated: keyPEM stays in heap memory; use WithServerKeyEnclave.
func (c *TLSConfig) WithServerCertPEM(certPEM, keyPEM []byte) *TLSConfig {
	c.Mode = TLSModeServer

	// Copy the key first: the config wipes only memory it allocated.
	key := append([]byte(nil), keyPEM...)

	c.selectServerKeySource(nil)
	c.ServerCertPEM = certPEM
	c.ServerKeyPEM = key
	return c
}

// WithServerKeyEnclave sets the certificate from PEM and takes ownership of the key enclave (call Destroy); it destroys any previous enclave and zeroizes the config's ServerKeyPEM copy.
func (c *TLSConfig) WithServerKeyEnclave(certPEM []byte, keyEnclave enclave.ByteAccess) *TLSConfig {
	c.Mode = TLSModeServer
	c.selectServerKeySource(keyEnclave)
	c.ServerCertPEM = certPEM
	c.serverKeyEnclave = keyEnclave
	return c
}

// ServerKeyEnclave returns the enclave holding the server private key, or nil
// when the key was supplied through the deprecated plain-memory path or from
// a file.
func (c *TLSConfig) ServerKeyEnclave() enclave.ByteAccess {
	return c.serverKeyEnclave
}

// Destroy zeroizes the key material this configuration owns, ServerKeyPEM included; idempotent.
func (c *TLSConfig) Destroy() error {
	err := c.releaseErr
	if c.serverKeyEnclave != nil {
		err = errors.Join(err, c.serverKeyEnclave.Destroy())
	}
	c.releaseErr = nil
	c.serverKeyEnclave = nil
	enclave.Wipe(c.ServerKeyPEM)
	c.ServerKeyPEM = nil
	return err
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

	minVersion := c.MinVersion
	if minVersion == 0 {
		minVersion = tls.VersionTLS12
	}

	config := &tls.Config{
		MinVersion:         minVersion,
		MaxVersion:         c.MaxVersion,
		CipherSuites:       c.CipherSuites,
		InsecureSkipVerify: c.InsecureSkipVerify, // #nosec G402 -- Configurable option for testing/development, defaults to false
	}

	// Load server certificate
	var cert tls.Certificate
	var err error

	switch {
	case c.serverKeyEnclave != nil:
		// A selected enclave is the only key source; a destroyed one is an error, never a fallback to ServerKeyPEM.
		if len(c.ServerCertPEM) == 0 {
			return nil, ErrInvalidCertificate
		}
		cert, err = keyPairFromEnclave(c.ServerCertPEM, c.serverKeyEnclave)
		if errors.Is(err, enclave.ErrEnclaveDestroyed) {
			return nil, err
		}
		if err != nil {
			return nil, ErrInvalidCertificate
		}
	case len(c.ServerCertPEM) > 0 && len(c.ServerKeyPEM) > 0:
		cert, err = tls.X509KeyPair(c.ServerCertPEM, c.ServerKeyPEM)
		if err != nil {
			return nil, ErrInvalidCertificate
		}
	case c.ServerCertFile != "" && c.ServerKeyFile != "":
		cert, err = loadKeyPairProtected(c.ServerCertFile, c.ServerKeyFile)
		if err != nil {
			return nil, ErrInvalidCertificate
		}
	default:
		return nil, ErrInvalidCertificate
	}

	config.Certificates = []tls.Certificate{cert}

	// Configure mTLS if enabled
	if c.Mode == TLSModeMutual {
		// Load client CA pool
		var caPool *x509.CertPool

		switch {
		case len(c.ClientCAPEM) > 0:
			caPool = x509.NewCertPool()
			if !caPool.AppendCertsFromPEM(c.ClientCAPEM) {
				return nil, ErrInvalidCAPool
			}
		case c.ClientCAFile != "":
			caData, err := os.ReadFile(c.ClientCAFile)
			if err != nil {
				return nil, ErrInvalidCAPool
			}
			caPool = x509.NewCertPool()
			if !caPool.AppendCertsFromPEM(caData) {
				return nil, ErrInvalidCAPool
			}
		default:
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

// keyPairFromEnclave parses a certificate/key pair, reading the private key
// directly out of the locked region so that no additional plain-memory copy
// of the key is made.
func keyPairFromEnclave(certPEM []byte, keyEnclave enclave.ByteAccess) (tls.Certificate, error) {
	var cert tls.Certificate
	if keyEnclave == nil {
		return cert, enclave.ErrEnclaveDestroyed
	}
	err := keyEnclave.WithBytes(func(keyPEM []byte) error {
		var pairErr error
		cert, pairErr = tls.X509KeyPair(certPEM, keyPEM)
		return pairErr
	})
	return cert, err
}

// loadKeyPairProtected reads the key file into an enclave and zeroizes it once the pair is parsed.
func loadKeyPairProtected(certFile, keyFile string) (cert tls.Certificate, err error) {
	certPEM, err := os.ReadFile(certFile) // #nosec G304 -- TLS certificate paths are controlled by application configuration
	if err != nil {
		return tls.Certificate{}, err
	}

	keyPEM, err := os.ReadFile(keyFile) // #nosec G304 -- TLS key paths are controlled by application configuration
	if err != nil {
		return tls.Certificate{}, err
	}

	// NewMemory takes ownership of keyPEM and wipes it on every return
	// path, so the PEM-encoded key never survives in unprotected memory
	// whether the allocation succeeds or fails.
	keyEnclave, err := enclave.NewMemory(keyPEM)
	if err != nil {
		return tls.Certificate{}, err
	}
	defer func() {
		if derr := keyEnclave.Destroy(); derr != nil {
			err = errors.Join(err, derr)
		}
	}()

	return keyPairFromEnclave(certPEM, keyEnclave)
}
