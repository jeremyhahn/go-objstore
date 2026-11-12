// Package testcerts generates throwaway TLS material (an ECDSA P-256 CA plus a
// server and a client certificate signed by it) for tests, so that no key or
// certificate fixture is tracked in the repository.
package testcerts

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// ServerDNSNames and ServerIPs are the SANs of the generated server certificate.
var (
	ServerDNSNames = []string{"localhost", "*.localhost", "quic-server", "rest-server", "grpc-server", "mcp-server", "unix-server"}
	ServerIPs      = []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")}
)

// Paths lists the files written by Write.
type Paths struct {
	CACert, CAKey         string
	ServerCert, ServerKey string
	ClientCert, ClientKey string
}

// Write generates a CA, a server and a client pair in dir (created if missing).
// Files: ca.crt ca.key server.crt server.key client.crt client.key.
func Write(dir string) (Paths, error) {
	p := Paths{
		CACert: filepath.Join(dir, "ca.crt"), CAKey: filepath.Join(dir, "ca.key"),
		ServerCert: filepath.Join(dir, "server.crt"), ServerKey: filepath.Join(dir, "server.key"),
		ClientCert: filepath.Join(dir, "client.crt"), ClientKey: filepath.Join(dir, "client.key"),
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return p, err
	}
	now := time.Now()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return p, err
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          serial(),
		Subject:               pkix.Name{CommonName: "objstore test CA"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(48 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		return p, err
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		return p, err
	}
	if err := writePair(p.CACert, p.CAKey, caDER, caKey); err != nil {
		return p, err
	}

	leaf := func(certPath, keyPath string, tmpl *x509.Certificate) error {
		k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			return err
		}
		tmpl.SerialNumber = serial()
		tmpl.NotBefore = now.Add(-time.Hour)
		tmpl.NotAfter = now.Add(48 * time.Hour)
		tmpl.KeyUsage = x509.KeyUsageDigitalSignature
		der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &k.PublicKey, caKey)
		if err != nil {
			return err
		}
		return writePair(certPath, keyPath, der, k)
	}
	if err := leaf(p.ServerCert, p.ServerKey, &x509.Certificate{
		Subject:     pkix.Name{CommonName: "localhost"},
		DNSNames:    ServerDNSNames,
		IPAddresses: ServerIPs,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}); err != nil {
		return p, err
	}
	err = leaf(p.ClientCert, p.ClientKey, &x509.Certificate{
		Subject:     pkix.Name{CommonName: "client"},
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	})
	return p, err
}

// WriteT generates the material into t.TempDir() and returns the paths.
func WriteT(t testing.TB) Paths {
	t.Helper()
	p, err := Write(t.TempDir())
	if err != nil {
		t.Fatalf("generate test certificates: %v", err)
	}
	return p
}

func serial() *big.Int {
	n, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		panic(fmt.Sprintf("testcerts: serial: %v", err))
	}
	return n
}

func writePair(certPath, keyPath string, der []byte, key *ecdsa.PrivateKey) error {
	if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o644); err != nil {
		return err
	}
	kb, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return err
	}
	// 0644 so a container running as another user can read the bind mount;
	// the material is throwaway and lives in a temporary directory.
	return os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kb}), 0o644)
}
