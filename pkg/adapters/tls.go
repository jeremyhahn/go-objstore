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

	// ServerKeyPEM is the server private key in PEM format (alternative to ServerKeyFile).
	//
	// Deprecated: ServerKeyPEM holds private key material in ordinary heap
	// memory for the lifetime of the TLSConfig — pageable to swap, visible in
	// a core dump, and never zeroized. Use WithServerKeyEnclave, which keeps
	// the key in a locked, zeroized memory enclave ([enclave.ByteAccess]). This field
	// remains for compatibility and is removed in the next major version;
	// when both are set, the enclave wins.
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

// selectServerKeySource clears every server certificate and private-key source
// this config holds, so that choosing one source cannot leave another attached
// for Build to prefer. Build ranks the enclave first, so a stale enclave left
// behind by a later WithServerCertFiles or WithServerCertPEM would silently
// keep serving the superseded key — a certificate rotation that appears to
// have happened and has not.
//
// Any enclave being discarded is destroyed rather than merely dropped, and the
// deprecated ServerKeyPEM is zeroized, so replacing a source removes the key
// material it held instead of leaving it resident.
//
// keepEnclave names an enclave the caller is about to install, which must not
// be destroyed on the way through. ServerKeyPEM needs no such exemption: the
// config always holds its own copy of the key (see WithServerCertPEM), so
// wiping it can never reach memory the caller still owns or is about to
// install.
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

// WithServerCertFiles sets the server certificate and key from files.
//
// It selects the file source: any previously configured enclave is destroyed
// and any deprecated ServerKeyPEM is zeroized, so Build uses the files rather
// than a superseded key.
func (c *TLSConfig) WithServerCertFiles(certFile, keyFile string) *TLSConfig {
	c.Mode = TLSModeServer
	c.selectServerKeySource(nil)
	c.ServerCertFile = certFile
	c.ServerKeyFile = keyFile
	return c
}

// WithServerCertPEM sets the server certificate and key from PEM data.
//
// It selects the PEM source: any previously configured enclave is destroyed
// and any previously configured file paths are cleared, so Build uses keyPEM
// rather than a superseded key.
//
// Deprecated: keyPEM is retained in ordinary heap memory. Use
// WithServerKeyEnclave, which takes ownership of the key bytes and holds them
// in a locked, zeroized memory enclave ([enclave.ByteAccess]). This method remains for
// compatibility and is removed in the next major version.
func (c *TLSConfig) WithServerCertPEM(certPEM, keyPEM []byte) *TLSConfig {
	c.Mode = TLSModeServer

	// Copy the key before clearing the previous source. The config owns its
	// copy, so replacing or destroying it wipes only memory this config
	// allocated — never the caller's array, and never a replacement that
	// happens to alias the value being replaced.
	key := append([]byte(nil), keyPEM...)

	c.selectServerKeySource(nil)
	c.ServerCertPEM = certPEM
	c.ServerKeyPEM = key
	return c
}

// WithServerKeyEnclave sets the server certificate from PEM data and the
// server private key from a locked, zeroized memory enclave.
//
// This is the protected replacement for WithServerCertPEM. The certificate is
// public and stays a plain []byte; only the private key is enclaved. The
// TLSConfig TAKES OWNERSHIP of keyEnclave — call [TLSConfig.Destroy] when the
// configuration is no longer needed to zeroize it. Any previously set
// enclave is destroyed here.
//
// Build reads the key out of the enclave for exactly as long as
// crypto/tls needs to parse the key pair.
//
// A caller migrating off the deprecated path already has a complete private
// key in ServerKeyPEM, so this ZEROIZES that field and clears it. Build merely
// ignoring the old copy would leave the key resident in ordinary heap memory
// for the config's lifetime, which is the thing the protected path exists to
// prevent — selecting the enclave has to remove the plain copy, not just stop
// reading it. Only the config's own copy is wiped; the caller's slice is left
// alone, and a caller that still holds one is responsible for wiping it.
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

// Destroy zeroizes the private key material this configuration owns. It is
// idempotent and safe to defer. The deprecated ServerKeyPEM field is wiped
// too, so that a caller migrating away from it does not leave the old copy
// behind; the certificate and CA fields are public data and are left alone.
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
		// Once an enclave has been selected for the private key it is the ONLY
		// source. A destroyed enclave — it can be destroyed independently of
		// this call, e.g. by a rotation or a teardown elsewhere — reports
		// itself rather than falling back to the deprecated ServerKeyPEM, which
		// would serve TLS with stale, unprotected key material after the
		// protected path was chosen.
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

// loadKeyPairProtected is the protected replacement for tls.LoadX509KeyPair.
// The standard library reads the key file into an ordinary heap buffer that is
// never wiped; this reads it into a memory enclave instead, so the PEM-encoded
// key exists in unprotected memory for no longer than the read itself and is
// zeroized as soon as the pair is parsed.
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
