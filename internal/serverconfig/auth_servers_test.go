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

package serverconfig

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/quic-go/quic-go/http3"

	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/factory"
	"github.com/jeremyhahn/go-objstore/pkg/objstore"
	grpcserver "github.com/jeremyhahn/go-objstore/pkg/server/grpc"
	mcpserver "github.com/jeremyhahn/go-objstore/pkg/server/mcp"
	quicserver "github.com/jeremyhahn/go-objstore/pkg/server/quic"
	restserver "github.com/jeremyhahn/go-objstore/pkg/server/rest"
)

// testPKI holds the files and parsed material of a generated CA, a server
// certificate for 127.0.0.1 and one client certificate per identity.
type testPKI struct {
	caFile, serverCert, serverKey string
	pool                          *x509.CertPool
	clients                       map[string]tls.Certificate
}

// clientIdentities are the client certificate subjects: backup-agent and
// prune-agent are configured identities, stranger is signed by the CA but
// has no identity.
var clientIdentities = []string{"backup-agent", "prune-agent", "stranger"}

func writePEM(t *testing.T, path, typ string, der []byte) {
	t.Helper()
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// newTestPKI writes the CA, the server certificate and the client
// certificates into t.TempDir().
func newTestPKI(t *testing.T) *testPKI {
	t.Helper()
	dir := t.TempDir()
	now := time.Now()
	serial := int64(1)
	newKey := func() *ecdsa.PrivateKey {
		k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatalf("generate key: %v", err)
		}
		return k
	}
	caKey := newKey()
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(serial),
		Subject:               pkix.Name{CommonName: "objstore test CA"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create CA: %v", err)
	}
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatalf("parse CA: %v", err)
	}
	p := &testPKI{
		caFile:     filepath.Join(dir, "ca.pem"),
		serverCert: filepath.Join(dir, "server.pem"),
		serverKey:  filepath.Join(dir, "server-key.pem"),
		pool:       x509.NewCertPool(),
		clients:    map[string]tls.Certificate{},
	}
	p.pool.AddCert(ca)
	writePEM(t, p.caFile, "CERTIFICATE", caDER)

	issue := func(cn string, usage x509.ExtKeyUsage, ips []net.IP) ([]byte, *ecdsa.PrivateKey) {
		serial++
		key := newKey()
		tmpl := &x509.Certificate{
			SerialNumber: big.NewInt(serial),
			Subject:      pkix.Name{CommonName: cn},
			NotBefore:    now.Add(-time.Hour),
			NotAfter:     now.Add(time.Hour),
			KeyUsage:     x509.KeyUsageDigitalSignature,
			ExtKeyUsage:  []x509.ExtKeyUsage{usage},
			IPAddresses:  ips,
		}
		der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
		if err != nil {
			t.Fatalf("issue %s: %v", cn, err)
		}
		return der, key
	}

	srvDER, srvKey := issue("127.0.0.1", x509.ExtKeyUsageServerAuth, []net.IP{net.IPv4(127, 0, 0, 1)})
	srvKeyDER, err := x509.MarshalECPrivateKey(srvKey)
	if err != nil {
		t.Fatalf("marshal server key: %v", err)
	}
	writePEM(t, p.serverCert, "CERTIFICATE", srvDER)
	writePEM(t, p.serverKey, "EC PRIVATE KEY", srvKeyDER)

	for _, cn := range clientIdentities {
		der, key := issue(cn, x509.ExtKeyUsageClientAuth, nil)
		keyDER, err := x509.MarshalECPrivateKey(key)
		if err != nil {
			t.Fatalf("marshal %s key: %v", cn, err)
		}
		certFile := filepath.Join(dir, cn+".pem")
		keyFile := filepath.Join(dir, cn+"-key.pem")
		writePEM(t, certFile, "CERTIFICATE", der)
		writePEM(t, keyFile, "EC PRIVATE KEY", keyDER)
		pair, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			t.Fatalf("load %s pair: %v", cn, err)
		}
		p.clients[cn] = pair
	}
	return p
}

// clientTLS is a client config that trusts the test CA and presents the
// named client certificate; an empty name presents none.
func (p *testPKI) clientTLS(name string, alpn ...string) *tls.Config {
	c := &tls.Config{RootCAs: p.pool, ServerName: "127.0.0.1", NextProtos: alpn, MinVersion: tls.VersionTLS12}
	if name != "" {
		c.Certificates = []tls.Certificate{p.clients[name]}
	}
	return c
}

func freeTCPAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

func freeUDPAddr(t *testing.T) string {
	t.Helper()
	l, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen udp: %v", err)
	}
	addr := l.LocalAddr().String()
	_ = l.Close()
	return addr
}

// authServers are the in-process listeners started with auth set.
type authServers struct {
	pki                   *testPKI
	grpc, rest, mcp, quic string
}

// startAuthServers starts gRPC, REST, MCP HTTP and QUIC on loopback ports
// with auth set, the way cmd/objstore-server does, over a local backend.
func startAuthServers(t *testing.T) *authServers {
	t.Helper()
	p := newTestPKI(t)

	storage, err := factory.NewStorage("local", map[string]string{"path": t.TempDir()}, common.Credentials{})
	if err != nil {
		t.Fatalf("NewStorage: %v", err)
	}
	objstore.Reset()
	if err := objstore.Initialize(&objstore.FacadeConfig{
		Backends:       map[string]common.Storage{"default": storage},
		DefaultBackend: "default",
	}); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	t.Cleanup(objstore.Reset)

	c := authConfig()
	c.Auth.ClientCA = p.caFile
	// backup-agent may read, write and list; prune-agent may also delete.
	c.Auth.Identities = []IdentityConfig{
		{Subject: "backup-agent", Roles: []string{"backup"}},
		{Subject: "prune-agent", Roles: []string{"backup", "prune"}},
	}
	s := &authServers{pki: p, grpc: freeTCPAddr(t), rest: freeTCPAddr(t), mcp: freeTCPAddr(t), quic: freeUDPAddr(t)}
	c.GRPC.Addr, c.MCP.Addr, c.QUIC.Addr = s.grpc, s.mcp, s.quic
	host, port, _ := net.SplitHostPort(s.rest)
	c.REST.Host = host
	c.REST.Port, _ = strconv.Atoi(port)
	c.GRPC.TLSCert, c.GRPC.TLSKey = p.serverCert, p.serverKey
	c.REST.TLSCert, c.REST.TLSKey = p.serverCert, p.serverKey
	c.QUIC.TLSCert, c.QUIC.TLSKey = p.serverCert, p.serverKey
	c.MCP.TLSCert, c.MCP.TLSKey = p.serverCert, p.serverKey
	mw := c.Middleware()

	opts, err := c.GRPCOptions(mw)
	if err != nil {
		t.Fatalf("GRPCOptions: %v", err)
	}
	grpcSrv, err := grpcserver.NewServer(opts...)
	if err != nil {
		t.Fatalf("grpc NewServer: %v", err)
	}
	go func() { _ = grpcSrv.Start() }()
	t.Cleanup(grpcSrv.ForceStop)

	restSrv, err := restserver.NewServer(storage, c.RESTServerConfig(mw))
	if err != nil {
		t.Fatalf("rest NewServer: %v", err)
	}
	go func() { _ = restSrv.Start() }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = restSrv.Shutdown(ctx)
	})

	// As cmd/objstore-server does, the caller's TLS config comes from the
	// server key pair only: the auth options must still require a client
	// certificate.
	quicTLS, err := quicserver.NewTLSConfig(p.serverCert, p.serverKey)
	if err != nil {
		t.Fatalf("quic NewTLSConfig: %v", err)
	}
	quicSrv, err := quicserver.New(c.QUICOptions(mw).WithTLSConfig(quicTLS))
	if err != nil {
		t.Fatalf("quic New: %v", err)
	}
	go func() { _ = quicSrv.Start() }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = quicSrv.Stop(ctx)
	})

	mcpCfg, err := c.MCPServerConfig(mw)
	if err != nil {
		t.Fatalf("MCPServerConfig: %v", err)
	}
	mcpSrv, err := mcpserver.NewServer(mcpCfg)
	if err != nil {
		t.Fatalf("mcp NewServer: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = mcpSrv.Start(ctx) }()
	t.Cleanup(cancel)

	for _, addr := range []string{s.grpc, s.rest, s.mcp} {
		waitTCP(t, addr)
	}
	return s
}

func waitTCP(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("server on %s did not start", addr)
}

// tlsProbe dials addr, completes the client side of the handshake, sends a
// request and reads the reply. It returns nil when the server answered, so
// the connection was accepted, and the error otherwise. Under TLS 1.3 the
// server verifies the client certificate after the client's handshake ends,
// so a refusal shows on the first read.
func tlsProbe(addr string, cfg *tls.Config) error {
	d := &net.Dialer{Timeout: 5 * time.Second}
	conn, err := tls.DialWithDialer(d, "tcp", addr, cfg)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	var req string
	if len(cfg.NextProtos) > 0 && cfg.NextProtos[0] == "h2" {
		req = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n\x00\x00\x00\x04\x00\x00\x00\x00\x00"
	} else {
		req = "GET /health HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n"
	}
	if _, err := conn.Write([]byte(req)); err != nil {
		return err
	}
	buf := make([]byte, 64)
	if _, err := conn.Read(buf); err != nil {
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			return nil // the server kept the connection open: it accepted it
		}
		return err
	}
	return nil
}

// quicProbe sends one HTTP/3 request; nil means the server answered.
func quicProbe(addr string, cfg *tls.Config) error {
	tr := &http3.Transport{TLSClientConfig: cfg}
	defer func() { _ = tr.Close() }()
	client := &http.Client{Transport: tr, Timeout: 5 * time.Second}
	resp, err := client.Get("https://" + addr + "/health")
	if err != nil {
		return err
	}
	_ = resp.Body.Close()
	return nil
}

func TestServers_RefuseHandshakeWithoutClientCertificate(t *testing.T) {
	s := startAuthServers(t)
	tcp := []struct {
		name, addr, alpn string
	}{
		{"grpc", s.grpc, "h2"},
		{"rest", s.rest, "http/1.1"},
		{"mcp_http", s.mcp, "http/1.1"},
	}
	for _, tc := range tcp {
		t.Run(tc.name, func(t *testing.T) {
			if err := tlsProbe(tc.addr, s.pki.clientTLS("backup-agent", tc.alpn)); err != nil {
				t.Fatalf("handshake with a client certificate failed: %v", err)
			}
			if err := tlsProbe(tc.addr, s.pki.clientTLS("", tc.alpn)); err == nil {
				t.Fatal("listener accepted a TLS connection without a client certificate")
			}
		})
	}
	t.Run("quic", func(t *testing.T) {
		var err error
		for range 50 { // the UDP listener has no readiness signal
			if err = quicProbe(s.quic, s.pki.clientTLS("backup-agent", "h3")); err == nil {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if err != nil {
			t.Fatalf("QUIC request with a client certificate failed: %v", err)
		}
		if err := quicProbe(s.quic, s.pki.clientTLS("", "h3")); err == nil {
			t.Fatal("QUIC listener accepted a connection without a client certificate")
		}
	})
}
