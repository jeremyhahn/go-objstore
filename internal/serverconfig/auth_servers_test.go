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
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/quic-go/quic-go/http3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	objstorepb "github.com/jeremyhahn/go-objstore/api/proto"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/factory"
	"github.com/jeremyhahn/go-objstore/pkg/objstore"
	grpcserver "github.com/jeremyhahn/go-objstore/pkg/server/grpc"
	"github.com/jeremyhahn/go-objstore/pkg/server/jsonrpc"
	mcpserver "github.com/jeremyhahn/go-objstore/pkg/server/mcp"
	quicserver "github.com/jeremyhahn/go-objstore/pkg/server/quic"
	restserver "github.com/jeremyhahn/go-objstore/pkg/server/rest"
	unixserver "github.com/jeremyhahn/go-objstore/pkg/server/unix"
)

// testPKI holds the files and parsed material of a generated CA, a server
// certificate for 127.0.0.1 and one client certificate per identity.
type testPKI struct {
	caFile, serverCert, serverKey string
	pool                          *x509.CertPool
	clients                       map[string]tls.Certificate
}

// clientIdentities are the client certificate subjects: backup-agent and
// prune-agent are configured identities, reader holds read only, stranger is
// signed by the CA but has no identity.
var clientIdentities = []string{"backup-agent", "prune-agent", "reader", "stranger"}

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
	// backup-agent may read, write and list; prune-agent may also delete;
	// reader may only read.
	c.Auth.Roles["readonly"] = []string{adapters.ActionRead}
	c.Auth.Identities = []IdentityConfig{
		{Subject: "backup-agent", Roles: []string{"backup"}},
		{Subject: "prune-agent", Roles: []string{"backup", "prune"}},
		{Subject: "reader", Roles: []string{"readonly"}},
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

// restCall makes one REST request as the named client certificate and
// returns the status code.
func restCall(t *testing.T, s *authServers, who, method, path, body string) int {
	t.Helper()
	client := &http.Client{
		Timeout:   5 * time.Second,
		Transport: &http.Transport{TLSClientConfig: s.pki.clientTLS(who)},
	}
	defer client.CloseIdleConnections()
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, "https://"+s.rest+path, rd)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/octet-stream")
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("%s %s as %s: %v", method, path, who, err)
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

func isRefused(code int) bool {
	return code == http.StatusUnauthorized || code == http.StatusForbidden
}

func TestServers_RESTEnforcesRBACPerClientCertificate(t *testing.T) {
	s := startAuthServers(t)

	for _, who := range []string{"backup-agent", "prune-agent"} {
		t.Run(who+"_put_get_list", func(t *testing.T) {
			key := "/objects/" + who + "/data.txt"
			if code := restCall(t, s, who, http.MethodPut, key, "payload of "+who); code/100 != 2 {
				t.Fatalf("PUT = %d, want 2xx", code)
			}
			if code := restCall(t, s, who, http.MethodGet, key, ""); code != http.StatusOK {
				t.Fatalf("GET = %d, want 200", code)
			}
			if code := restCall(t, s, who, http.MethodGet, "/objects", ""); code != http.StatusOK {
				t.Fatalf("list = %d, want 200", code)
			}
		})
	}

	t.Run("backup-agent_delete_refused", func(t *testing.T) {
		key := "/objects/backup-agent/data.txt"
		if code := restCall(t, s, "backup-agent", http.MethodDelete, key, ""); code != http.StatusForbidden {
			t.Fatalf("DELETE = %d, want 403", code)
		}
		if code := restCall(t, s, "backup-agent", http.MethodGet, key, ""); code != http.StatusOK {
			t.Fatalf("GET after refused DELETE = %d, want 200: the object must remain", code)
		}
	})

	t.Run("prune-agent_delete_allowed", func(t *testing.T) {
		key := "/objects/backup-agent/data.txt"
		if code := restCall(t, s, "prune-agent", http.MethodDelete, key, ""); code/100 != 2 {
			t.Fatalf("DELETE = %d, want 2xx", code)
		}
		if code := restCall(t, s, "prune-agent", http.MethodGet, key, ""); code != http.StatusNotFound {
			t.Fatalf("GET after DELETE = %d, want 404", code)
		}
	})

	t.Run("stranger_refused", func(t *testing.T) {
		calls := []struct{ method, path, body string }{
			{http.MethodPut, "/objects/stranger/data.txt", "payload"},
			{http.MethodGet, "/objects/prune-agent/data.txt", ""},
			{http.MethodGet, "/objects", ""},
			{http.MethodDelete, "/objects/prune-agent/data.txt", ""},
		}
		for _, c := range calls {
			if code := restCall(t, s, "stranger", c.method, c.path, c.body); !isRefused(code) {
				t.Errorf("%s %s as stranger = %d, want 401 or 403", c.method, c.path, code)
			}
		}
		if code := restCall(t, s, "prune-agent", http.MethodGet, "/objects/prune-agent/data.txt", ""); code != http.StatusOK {
			t.Fatalf("GET after stranger DELETE = %d, want 200: the object must remain", code)
		}
	})
}

// TestServers_RESTActionFromRouteNotKey checks that an object key containing
// a route word ("/exists", "/metadata", "/archive", ...) does not change the
// action the request needs: the action comes from the matched route.
func TestServers_RESTActionFromRouteNotKey(t *testing.T) {
	s := startAuthServers(t)
	trick := []string{"/objects/a/exists/b", "/objects/a/metadata/b", "/objects/snap/metadata.json"}
	for _, key := range trick {
		if code := restCall(t, s, "backup-agent", http.MethodPut, key, "seed"); code/100 != 2 {
			t.Fatalf("seed PUT %s = %d, want 2xx", key, code)
		}
	}

	t.Run("reader_refused_write_and_delete", func(t *testing.T) {
		for _, key := range trick {
			if code := restCall(t, s, "reader", http.MethodPut, key+".new", "payload"); code != http.StatusForbidden {
				t.Errorf("PUT %s.new as reader = %d, want 403", key, code)
			}
			if code := restCall(t, s, "reader", http.MethodGet, key+".new", ""); code != http.StatusNotFound {
				t.Errorf("GET %s.new after refused PUT = %d, want 404: nothing may be stored", key, code)
			}
			if code := restCall(t, s, "reader", http.MethodDelete, key, ""); code != http.StatusForbidden {
				t.Errorf("DELETE %s as reader = %d, want 403", key, code)
			}
			if code := restCall(t, s, "reader", http.MethodGet, key, ""); code != http.StatusOK {
				t.Errorf("GET %s after refused DELETE = %d, want 200: the object must remain", key, code)
			}
		}
	})

	t.Run("backup-agent_refused_delete", func(t *testing.T) {
		for _, key := range trick {
			if code := restCall(t, s, "backup-agent", http.MethodDelete, key, ""); code != http.StatusForbidden {
				t.Errorf("DELETE %s as backup-agent = %d, want 403", key, code)
			}
			if code := restCall(t, s, "backup-agent", http.MethodGet, key, ""); code != http.StatusOK {
				t.Errorf("GET %s after refused DELETE = %d, want 200: the object must remain", key, code)
			}
		}
	})

	t.Run("backup-agent_admin_words_in_key", func(t *testing.T) {
		for _, key := range []string{"/objects/a/archive/b", "/objects/a/policies/b", "/objects/a/replication/b",
			"/api/v1/objects/a/archive/b", "/api/v1/objects/a/policies/b", "/api/v1/objects/a/replication/b"} {
			if code := restCall(t, s, "backup-agent", http.MethodPut, key, "payload"); code/100 != 2 {
				t.Errorf("PUT %s as backup-agent = %d, want 2xx", key, code)
			}
			if code := restCall(t, s, "backup-agent", http.MethodGet, key, ""); code != http.StatusOK {
				t.Errorf("GET %s as backup-agent = %d, want 200", key, code)
			}
		}
	})
}

// TestServers_GRPCHealthForAnyIdentity covers #239: over gRPC, an
// authenticated client that holds no roles (stranger) may call the objstore
// Health RPC but is still refused Put.
func TestServers_GRPCHealthForAnyIdentity(t *testing.T) {
	s := startAuthServers(t)
	conn, err := grpc.NewClient(s.grpc, grpc.WithTransportCredentials(credentials.NewTLS(s.pki.clientTLS("stranger"))))
	if err != nil {
		t.Fatalf("grpc NewClient: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client := objstorepb.NewObjectStoreClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := client.Health(ctx, &objstorepb.HealthRequest{}); err != nil {
		t.Errorf("Health as stranger = %v, want nil", err)
	}
	_, err = client.Put(ctx, &objstorepb.PutRequest{Key: "stranger/data.txt", Data: []byte("payload")})
	if status.Code(err) != codes.PermissionDenied {
		t.Errorf("Put as stranger = %v, want PermissionDenied", err)
	}
}

// startAuthUnix starts the Unix socket server built by UnixServerConfig with
// auth set and the given identities, and returns a connection to it.
func startAuthUnix(t *testing.T, identities []IdentityConfig) net.Conn {
	t.Helper()
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
	c.Auth.Identities = identities
	c.Unix.Socket = filepath.Join(t.TempDir(), "objstore.sock")
	srv, err := unixserver.NewServer(c.UnixServerConfig(c.Middleware()))
	if err != nil {
		t.Fatalf("unix NewServer: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = srv.Start(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	select {
	case <-srv.Ready():
	case <-time.After(5 * time.Second):
		t.Fatal("unix server not ready")
	}
	conn, err := net.Dial("unix", c.Unix.Socket)
	if err != nil {
		t.Fatalf("dial unix: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// unixCall sends one JSON-RPC request on conn and returns the response.
func unixCall(t *testing.T, conn net.Conn, r *bufio.Reader, id int, method string, params any) *jsonrpc.Response {
	t.Helper()
	req := jsonrpc.Request{JSONRPC: jsonrpc.Version, Method: method, ID: id}
	if params != nil {
		raw, err := json.Marshal(params)
		if err != nil {
			t.Fatalf("marshal params: %v", err)
		}
		req.Params = raw
	}
	line, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set deadline: %v", err)
	}
	if _, err := conn.Write(append(line, '\n')); err != nil {
		t.Fatalf("write request: %v", err)
	}
	reply, err := r.ReadBytes('\n')
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	var resp jsonrpc.Response
	if err := json.Unmarshal(reply, &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	return &resp
}

func TestServers_UnixEnforcesRBACPerPeerUID(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("SO_PEERCRED peer credentials are Linux only")
	}
	self := UIDSubjectPrefix + strconv.Itoa(os.Getuid())
	put := unixserver.PutParams{Key: "unix/data.txt", Data: "cGF5bG9hZA=="} // "payload"
	get := unixserver.GetParams{Key: "unix/data.txt"}
	del := unixserver.DeleteParams{Key: "unix/data.txt"}

	t.Run("unmapped_uid_refused", func(t *testing.T) {
		// Only another uid is mapped: this process's uid has no roles.
		conn := startAuthUnix(t, []IdentityConfig{
			{Subject: UIDSubjectPrefix + strconv.Itoa(os.Getuid()+1), Roles: []string{"backup", "prune"}},
		})
		r := bufio.NewReader(conn)
		calls := []struct {
			method string
			params any
		}{
			{unixserver.MethodPut, put},
			{unixserver.MethodGet, get},
			{unixserver.MethodList, unixserver.ListParams{}},
			{unixserver.MethodDelete, del},
		}
		for i, c := range calls {
			resp := unixCall(t, conn, r, i+1, c.method, c.params)
			if resp.Error == nil || resp.Error.Code != jsonrpc.CodeForbidden {
				t.Errorf("%s as unmapped %s = %+v, want error code %d", c.method, self, resp.Error, jsonrpc.CodeForbidden)
			}
		}
	})

	t.Run("mapped_uid_allowed_by_role", func(t *testing.T) {
		// This process's uid holds backup only: put, get and list are
		// allowed, delete is refused.
		conn := startAuthUnix(t, []IdentityConfig{{Subject: self, Roles: []string{"backup"}}})
		r := bufio.NewReader(conn)
		for i, c := range []struct {
			method string
			params any
		}{
			{unixserver.MethodPut, put},
			{unixserver.MethodGet, get},
			{unixserver.MethodList, unixserver.ListParams{}},
		} {
			if resp := unixCall(t, conn, r, i+1, c.method, c.params); resp.Error != nil {
				t.Fatalf("%s as %s = %+v, want success", c.method, self, resp.Error)
			}
		}
		resp := unixCall(t, conn, r, 4, unixserver.MethodDelete, del)
		if resp.Error == nil || resp.Error.Code != jsonrpc.CodeForbidden {
			t.Fatalf("delete as %s with backup only = %+v, want error code %d", self, resp.Error, jsonrpc.CodeForbidden)
		}
	})
}
