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

package client

import (
	"fmt"
	"net"
	"net/http"
	"testing"

	quicserver "github.com/jeremyhahn/go-objstore/pkg/server/quic"
	"github.com/quic-go/quic-go/http3"
)

// http3TestServer is a genuine HTTP/3-over-UDP test server, the QUIC
// counterpart of httptest.Server. The QUIC CLI client speaks real HTTP/3, so
// plain httptest TCP servers cannot stand in for it.
type http3TestServer struct {
	URL  string
	srv  *http3.Server
	conn *net.UDPConn
}

func (s *http3TestServer) Close() {
	_ = s.srv.Close()
	_ = s.conn.Close()
}

// newHTTP3TestServer starts an HTTP/3 server with a self-signed certificate
// on a loopback UDP port and returns it. Pair with a client created via
// newQUICTestClient (which skips certificate verification).
func newHTTP3TestServer(t *testing.T, handler http.Handler) *http3TestServer {
	t.Helper()

	tlsConfig, err := quicserver.GenerateSelfSignedCert()
	if err != nil {
		t.Fatalf("generate self-signed cert: %v", err)
	}

	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatalf("listen udp: %v", err)
	}

	srv := &http3.Server{
		Handler:   handler,
		TLSConfig: http3.ConfigureTLSConfig(tlsConfig),
	}
	go func() { _ = srv.Serve(conn) }()

	return &http3TestServer{
		URL:  fmt.Sprintf("https://%s", conn.LocalAddr().String()),
		srv:  srv,
		conn: conn,
	}
}

// newQUICTestClient creates a QUIC client for an http3TestServer, skipping
// certificate verification (self-signed test cert).
func newQUICTestClient(t *testing.T, serverURL string) *QUICClient {
	t.Helper()
	client, err := NewQUICClient(&Config{ServerURL: serverURL, InsecureSkipVerify: true})
	if err != nil {
		t.Fatalf("create QUIC client: %v", err)
	}
	return client
}
