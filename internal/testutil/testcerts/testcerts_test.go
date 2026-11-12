package testcerts

import (
	"crypto/tls"
	"crypto/x509"
	"os"
	"testing"
)

func TestWriteChainsToCA(t *testing.T) {
	p := WriteT(t)
	if _, err := tls.LoadX509KeyPair(p.ServerCert, p.ServerKey); err != nil {
		t.Fatalf("server pair: %v", err)
	}
	if _, err := tls.LoadX509KeyPair(p.ClientCert, p.ClientKey); err != nil {
		t.Fatalf("client pair: %v", err)
	}
	pemBytes, err := os.ReadFile(p.CACert)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pemBytes) {
		t.Fatal("CA not parsed")
	}
	pair, _ := tls.LoadX509KeyPair(p.ServerCert, p.ServerKey)
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	if _, err := leaf.Verify(x509.VerifyOptions{Roots: pool, DNSName: "quic-server"}); err != nil {
		t.Fatalf("verify: %v", err)
	}
}
