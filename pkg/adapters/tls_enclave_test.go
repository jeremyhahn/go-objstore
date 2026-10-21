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
	"bytes"
	"crypto/tls"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/jeremyhahn/go-enclave"
)

func newKeyEnclave(t *testing.T, keyPEM []byte) enclave.ByteAccess {
	t.Helper()
	e, err := enclave.NewMemory(keyPEM)
	if err != nil {
		t.Fatalf("NewMemory: %v", err)
	}
	return e
}

func TestTLSConfig_WithServerKeyEnclave(t *testing.T) {
	certPEM, keyPEM, _, err := generateTestCert(false)
	if err != nil {
		t.Fatalf("generateTestCert: %v", err)
	}
	keyCopy := append([]byte(nil), keyPEM...)

	config := NewTLSConfig().WithServerKeyEnclave(certPEM, newKeyEnclave(t, keyPEM))
	defer config.Destroy()

	if config.Mode != TLSModeServer {
		t.Errorf("Mode = %v, want TLSModeServer", config.Mode)
	}
	if !bytes.Equal(config.ServerCertPEM, certPEM) {
		t.Error("ServerCertPEM not set")
	}
	if config.ServerKeyPEM != nil {
		t.Error("ServerKeyPEM must stay empty on the protected path")
	}
	if eq, err := config.ServerKeyEnclave().EqualTo(keyCopy); err != nil || !eq {
		t.Error("enclave does not hold the key")
	}
	// NewMemory takes ownership, so the caller's slice is wiped.
	if !bytes.Equal(keyPEM, make([]byte, len(keyPEM))) {
		t.Error("caller's key slice was not wiped")
	}
}

func TestTLSConfig_BuildFromEnclave(t *testing.T) {
	certPEM, keyPEM, _, err := generateTestCert(false)
	if err != nil {
		t.Fatalf("generateTestCert: %v", err)
	}

	config := NewTLSConfig().WithServerKeyEnclave(certPEM, newKeyEnclave(t, keyPEM))
	defer config.Destroy()

	tlsConfig, err := config.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if len(tlsConfig.Certificates) != 1 {
		t.Fatalf("Certificates = %d, want 1", len(tlsConfig.Certificates))
	}
	if tlsConfig.MinVersion != tls.VersionTLS12 {
		t.Errorf("MinVersion = %d, want %d", tlsConfig.MinVersion, tls.VersionTLS12)
	}
}

func TestTLSConfig_BuildFromEnclave_InvalidKey(t *testing.T) {
	certPEM, _, _, err := generateTestCert(false)
	if err != nil {
		t.Fatalf("generateTestCert: %v", err)
	}

	config := NewTLSConfig().WithServerKeyEnclave(certPEM, newKeyEnclave(t, []byte("not a key")))
	defer config.Destroy()

	if _, err := config.Build(); !errors.Is(err, ErrInvalidCertificate) {
		t.Errorf("Build err = %v, want ErrInvalidCertificate", err)
	}
}

func TestTLSConfig_EnclaveWinsOverDeprecatedField(t *testing.T) {
	certPEM, keyPEM, _, err := generateTestCert(false)
	if err != nil {
		t.Fatalf("generateTestCert: %v", err)
	}

	config := NewTLSConfig().WithServerKeyEnclave(certPEM, newKeyEnclave(t, keyPEM))
	defer config.Destroy()
	// A stale value on the deprecated path must not be able to win; if it did,
	// Build would fail to parse it.
	config.ServerKeyPEM = []byte("stale-plaintext-key")

	if _, err := config.Build(); err != nil {
		t.Fatalf("Build: %v", err)
	}
}

func TestTLSConfig_WithServerKeyEnclave_ReplacesPrevious(t *testing.T) {
	certPEM, keyPEM, _, err := generateTestCert(false)
	if err != nil {
		t.Fatalf("generateTestCert: %v", err)
	}

	first := newKeyEnclave(t, []byte("first-key"))
	config := NewTLSConfig().WithServerKeyEnclave(certPEM, first)
	config.WithServerKeyEnclave(certPEM, newKeyEnclave(t, keyPEM))
	defer config.Destroy()

	if first.IsAlive() {
		t.Error("the replaced enclave was not destroyed")
	}
	if _, err := config.Build(); err != nil {
		t.Fatalf("Build with the replacement key: %v", err)
	}

	// Re-setting the same enclave must not destroy it.
	current := config.ServerKeyEnclave()
	config.WithServerKeyEnclave(certPEM, current)
	if !current.IsAlive() {
		t.Error("re-setting the same enclave destroyed it")
	}
}

func TestTLSConfig_Destroy(t *testing.T) {
	certPEM, keyPEM, _, err := generateTestCert(false)
	if err != nil {
		t.Fatalf("generateTestCert: %v", err)
	}

	keyEnclave := newKeyEnclave(t, keyPEM)
	config := NewTLSConfig().WithServerKeyEnclave(certPEM, keyEnclave)
	// A caller mid-migration may still carry the deprecated copy.
	config.ServerKeyPEM = []byte("legacy-plaintext-key")

	config.Destroy()
	config.Destroy() // idempotent

	if keyEnclave.IsAlive() {
		t.Error("enclave still alive after Destroy")
	}
	if config.ServerKeyEnclave() != nil {
		t.Error("ServerKeyEnclave not cleared after Destroy")
	}
	if config.ServerKeyPEM != nil {
		t.Error("deprecated ServerKeyPEM not cleared")
	}
	if !bytes.Equal(config.ServerCertPEM, certPEM) {
		t.Error("Destroy must not touch the public certificate")
	}
	if _, err := config.Build(); !errors.Is(err, ErrInvalidCertificate) {
		t.Errorf("Build after Destroy err = %v, want ErrInvalidCertificate", err)
	}
}

// TestTLSConfig_DestroyWithoutEnclave covers the nil ByteAccess paths: a
// config that never held an enclave destroys cleanly, and reading a key pair
// from a nil enclave fails closed instead of panicking.
func TestTLSConfig_DestroyWithoutEnclave(t *testing.T) {
	config := NewTLSConfig()
	config.ServerKeyPEM = []byte("legacy-plaintext-key")
	if err := config.Destroy(); err != nil {
		t.Errorf("Destroy without enclave err = %v, want nil", err)
	}
	if config.ServerKeyPEM != nil {
		t.Error("deprecated ServerKeyPEM not cleared")
	}

	if _, err := keyPairFromEnclave(nil, nil); !errors.Is(err, enclave.ErrEnclaveDestroyed) {
		t.Errorf("keyPairFromEnclave(nil) err = %v, want ErrEnclaveDestroyed", err)
	}
}

func TestTLSConfig_BuildFromFiles_UsesProtectedLoader(t *testing.T) {
	certPEM, keyPEM, _, err := generateTestCert(false)
	if err != nil {
		t.Fatalf("generateTestCert: %v", err)
	}

	dir := t.TempDir()
	certFile := filepath.Join(dir, "server.crt")
	keyFile := filepath.Join(dir, "server.key")
	if err := os.WriteFile(certFile, certPEM, 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	if err := os.WriteFile(keyFile, keyPEM, 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}

	config := NewTLSConfig().WithServerCertFiles(certFile, keyFile)
	tlsConfig, err := config.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if len(tlsConfig.Certificates) != 1 {
		t.Fatalf("Certificates = %d, want 1", len(tlsConfig.Certificates))
	}
}

func TestLoadKeyPairProtected_Errors(t *testing.T) {
	certPEM, keyPEM, _, err := generateTestCert(false)
	if err != nil {
		t.Fatalf("generateTestCert: %v", err)
	}

	dir := t.TempDir()
	certFile := filepath.Join(dir, "server.crt")
	keyFile := filepath.Join(dir, "server.key")
	emptyKey := filepath.Join(dir, "empty.key")
	if err := os.WriteFile(certFile, certPEM, 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	if err := os.WriteFile(keyFile, keyPEM, 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	if err := os.WriteFile(emptyKey, nil, 0o600); err != nil {
		t.Fatalf("write empty key: %v", err)
	}

	tests := []struct {
		name     string
		certFile string
		keyFile  string
		wantErr  error
	}{
		{"missing cert", filepath.Join(dir, "absent.crt"), keyFile, os.ErrNotExist},
		{"missing key", certFile, filepath.Join(dir, "absent.key"), os.ErrNotExist},
		{"empty key", certFile, emptyKey, enclave.ErrEmptyEnclave},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := loadKeyPairProtected(tt.certFile, tt.keyFile); !errors.Is(err, tt.wantErr) {
				t.Errorf("err = %v, want %v", err, tt.wantErr)
			}

			config := NewTLSConfig().WithServerCertFiles(tt.certFile, tt.keyFile)
			if _, err := config.Build(); !errors.Is(err, ErrInvalidCertificate) {
				t.Errorf("Build err = %v, want ErrInvalidCertificate", err)
			}
		})
	}
}

// TestTLSConfig_BuildDestroyedEnclaveDoesNotFallBack proves Build fails CLOSED
// on the key. ServerKeyPEM here is a VALID key for the same certificate, so a
// fallback would produce a working *tls.Config and the caller would never
// learn that the protected key it configured was gone — serving TLS from
// unprotected key material after the protected path was chosen.
func TestTLSConfig_BuildDestroyedEnclaveDoesNotFallBack(t *testing.T) {
	certPEM, keyPEM, _, err := generateTestCert(false)
	if err != nil {
		t.Fatalf("generateTestCert: %v", err)
	}
	keyCopy := append([]byte(nil), keyPEM...)

	config := NewTLSConfig().WithServerKeyEnclave(certPEM, newKeyEnclave(t, keyPEM))
	config.ServerKeyPEM = keyCopy

	// Sanity: with the enclave alive the pair builds, so a later failure is
	// the destroyed enclave and not a bad fixture.
	if _, err := config.Build(); err != nil {
		t.Fatalf("Build with a live enclave: %v", err)
	}

	config.ServerKeyEnclave().Destroy()

	tlsConfig, err := config.Build()
	if !errors.Is(err, enclave.ErrEnclaveDestroyed) {
		t.Fatalf("Build err = %v, want ErrEnclaveDestroyed", err)
	}
	if tlsConfig != nil {
		t.Error("Build returned a usable config from the deprecated plain key")
	}
}

// TestTLSConfig_WithServerKeyEnclave_WipesLegacyServerKeyPEM covers the
// incremental-migration path: a caller that already supplied the key through
// the deprecated ServerKeyPEM and then switches to the enclave. Build ignoring
// the old copy is not the same as the copy being gone — it would stay resident
// in ordinary heap memory for the config's lifetime, which is exactly what the
// protected path exists to prevent. The assertion is on the backing array, not
// the field, so nilling the field alone does not satisfy it.
func TestTLSConfig_WithServerKeyEnclave_WipesLegacyServerKeyPEM(t *testing.T) {
	certPEM, keyPEM, _, err := generateTestCert(false)
	if err != nil {
		t.Fatalf("generateTestCert: %v", err)
	}
	legacy := append([]byte(nil), keyPEM...)
	forEnclave := append([]byte(nil), keyPEM...)

	config := NewTLSConfig().WithServerCertPEM(certPEM, legacy)

	// Keep a reference to the same backing array the config holds.
	backing := config.ServerKeyPEM
	zeroes := make([]byte, len(backing))
	if bytes.Equal(backing, zeroes) {
		t.Fatal("fixture key is already all zeroes; the assertion below would be vacuous")
	}

	config.WithServerKeyEnclave(certPEM, newKeyEnclave(t, forEnclave))
	defer config.Destroy()

	if config.ServerKeyPEM != nil {
		t.Error("ServerKeyPEM was not cleared when the enclave was selected")
	}
	if !bytes.Equal(backing, zeroes) {
		t.Error("the legacy private key is still readable in its original backing array")
	}

	// The protected path still works after the legacy copy is gone.
	if _, err := config.Build(); err != nil {
		t.Fatalf("Build after migrating to the enclave: %v", err)
	}
}

// TestTLSConfig_SourceTransitions covers reconfiguring a TLSConfig that already
// holds an enclave. Build ranks the enclave first, so a setter that leaves the
// old one attached serves the superseded key — a certificate rotation that
// looks like it happened and did not. Each case asserts on the certificate
// Build actually produces, so "the field was cleared" cannot pass for "the new
// credentials are in use".
func TestTLSConfig_SourceTransitions(t *testing.T) {
	// Two independent certificates, so the wrong one is detectable.
	certA, keyA, _, err := generateTestCert(false)
	if err != nil {
		t.Fatalf("generateTestCert A: %v", err)
	}
	certB, keyB, _, err := generateTestCert(false)
	if err != nil {
		t.Fatalf("generateTestCert B: %v", err)
	}
	if bytes.Equal(certA, certB) {
		t.Fatal("fixtures are identical; the assertions below would be vacuous")
	}

	expected, err := tls.X509KeyPair(certB, keyB)
	if err != nil {
		t.Fatalf("X509KeyPair for the replacement: %v", err)
	}

	assertServesB := func(t *testing.T, config *TLSConfig) {
		t.Helper()
		built, err := config.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		if len(built.Certificates) != 1 {
			t.Fatalf("got %d certificates, want 1", len(built.Certificates))
		}
		if !bytes.Equal(built.Certificates[0].Certificate[0], expected.Certificate[0]) {
			t.Error("Build served the superseded certificate, not the replacement")
		}
	}

	t.Run("enclave to PEM", func(t *testing.T) {
		config := NewTLSConfig().WithServerKeyEnclave(certA, newKeyEnclave(t, append([]byte(nil), keyA...)))
		stale := config.ServerKeyEnclave()

		config.WithServerCertPEM(certB, append([]byte(nil), keyB...))
		defer config.Destroy()

		if config.ServerKeyEnclave() != nil {
			t.Error("the old enclave is still attached after selecting PEM")
		}
		if stale.IsAlive() {
			t.Error("the superseded enclave was dropped without being destroyed")
		}
		assertServesB(t, config)
	})

	t.Run("enclave to files", func(t *testing.T) {
		dir := t.TempDir()
		certFile := filepath.Join(dir, "cert.pem")
		keyFile := filepath.Join(dir, "key.pem")
		if err := os.WriteFile(certFile, certB, 0o600); err != nil {
			t.Fatalf("write cert: %v", err)
		}
		if err := os.WriteFile(keyFile, keyB, 0o600); err != nil {
			t.Fatalf("write key: %v", err)
		}

		config := NewTLSConfig().WithServerKeyEnclave(certA, newKeyEnclave(t, append([]byte(nil), keyA...)))
		stale := config.ServerKeyEnclave()

		config.WithServerCertFiles(certFile, keyFile)
		defer config.Destroy()

		if config.ServerKeyEnclave() != nil {
			t.Error("the old enclave is still attached after selecting files")
		}
		if stale.IsAlive() {
			t.Error("the superseded enclave was dropped without being destroyed")
		}
		if config.ServerCertPEM != nil {
			t.Error("the superseded inline certificate is still set")
		}
		assertServesB(t, config)
	})

	t.Run("PEM replaced by PEM wipes the superseded key", func(t *testing.T) {
		first := append([]byte(nil), keyA...)
		config := NewTLSConfig().WithServerCertPEM(certA, first)
		backing := config.ServerKeyPEM
		zeroes := make([]byte, len(backing))

		config.WithServerCertPEM(certB, append([]byte(nil), keyB...))
		defer config.Destroy()

		if !bytes.Equal(backing, zeroes) {
			t.Error("the superseded private key is still readable in its backing array")
		}
		assertServesB(t, config)
	})

	t.Run("an overlapping subslice replacement is not wiped", func(t *testing.T) {
		// The replacement shares the old backing array but starts at a
		// DIFFERENT element, so a start-address-only aliasing test calls the
		// two unrelated and zeroes the bytes being installed. Build is not
		// asserted here — an offset subslice is not a parseable key — because
		// the property under test is that the installed bytes survive.
		buf := append([]byte(nil), keyB...)
		const offset = 8
		config := NewTLSConfig().WithServerCertPEM(certB, buf)

		replacement := buf[offset:]
		want := append([]byte(nil), replacement...)

		config.WithServerCertPEM(certB, replacement)
		defer config.Destroy()

		if !bytes.Equal(config.ServerKeyPEM, want) {
			t.Error("the replacement key was wiped before it was installed")
		}
	})

	t.Run("re-setting the same PEM does not wipe it", func(t *testing.T) {
		key := append([]byte(nil), keyB...)
		config := NewTLSConfig().WithServerCertPEM(certB, key)
		config.WithServerCertPEM(certB, key)
		defer config.Destroy()

		assertServesB(t, config)
	})

	t.Run("re-setting the same enclave does not destroy it", func(t *testing.T) {
		e := newKeyEnclave(t, append([]byte(nil), keyB...))
		config := NewTLSConfig().WithServerKeyEnclave(certB, e)
		config.WithServerKeyEnclave(certB, e)
		defer config.Destroy()

		if !e.IsAlive() {
			t.Fatal("re-setting the same enclave destroyed it")
		}
		assertServesB(t, config)
	})
}
