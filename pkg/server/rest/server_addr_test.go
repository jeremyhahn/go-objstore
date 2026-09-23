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

package rest

import (
	"net"
	"testing"

	"github.com/gin-gonic/gin"
)

// TestNewServer_ListenAddressJoinsHostPort verifies the listener address is
// built with host:port joining rules, so an IPv6 host is bracketed and splits
// back to the configured host. Binding an IPv6 socket is covered by the server
// integration suite (test/integration/server).
func TestNewServer_ListenAddressJoinsHostPort(t *testing.T) {
	cases := []struct {
		host string
		port int
		want string
	}{
		{"127.0.0.1", 8080, "127.0.0.1:8080"},
		{"localhost", 8080, "localhost:8080"},
		{"", 8080, ":8080"},
		{"::1", 8080, "[::1]:8080"},
		{"fe80::1%eth0", 9443, "[fe80::1%eth0]:9443"},
	}
	for _, c := range cases {
		t.Run(c.want, func(t *testing.T) {
			storage := NewMockStorage()
			initTestFacade(t, storage)
			srv, err := NewServer(storage, &ServerConfig{Host: c.host, Port: c.port, Mode: gin.TestMode})
			if err != nil {
				t.Fatalf("NewServer: %v", err)
			}
			if got := srv.Address(); got != c.want {
				t.Errorf("Address() = %q, want %q", got, c.want)
			}
			host, _, err := net.SplitHostPort(srv.Address())
			if err != nil {
				t.Fatalf("listener address %q is not host:port: %v", srv.Address(), err)
			}
			if host != c.host {
				t.Errorf("listener host = %q, want the configured %q", host, c.host)
			}
		})
	}
}
