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

//go:build integration

package server_test

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/internal/serverconfig"
	"github.com/jeremyhahn/go-objstore/pkg/objstore"
	restserver "github.com/jeremyhahn/go-objstore/pkg/server/rest"
)

// TestRESTServer_IPv6HostFromConfig starts the REST server exactly as
// objstore-server does — configuration resolved by serverconfig from
// OBJSTORE_* environment variables, then RESTServerConfig — with an IPv6 bind
// host, and serves /health over [::1]. IPv6 is a requirement of this
// environment (docker-compose.yml enables it); its absence fails the test.
func TestRESTServer_IPv6HostFromConfig(t *testing.T) {
	// Reserve a free port on the IPv6 loopback, then hand it to the server.
	probe, err := net.Listen("tcp", "[::1]:0")
	if err != nil {
		t.Fatalf("IPv6 loopback is required by the server integration environment: %v", err)
	}
	port := probe.Addr().(*net.TCPAddr).Port
	if err := probe.Close(); err != nil {
		t.Fatalf("release probe port: %v", err)
	}

	t.Setenv("OBJSTORE_REST_HOST", "::1")
	t.Setenv("OBJSTORE_REST_PORT", strconv.Itoa(port))
	t.Setenv("OBJSTORE_PATH", t.TempDir())
	cfg, err := serverconfig.Load("objstore-server", nil, serverconfig.Defaults(), serverconfig.BindServerFlags)
	if err != nil {
		t.Fatalf("load configuration: %v", err)
	}

	objstore.Reset()
	t.Cleanup(objstore.Reset)
	if err := objstore.Initialize(&objstore.FacadeConfig{
		BackendConfigs: map[string]objstore.BackendConfig{
			"default": {Type: cfg.Backend, Settings: map[string]string{"path": cfg.Path}},
		},
		DefaultBackend: "default",
	}); err != nil {
		t.Fatalf("initialize facade: %v", err)
	}

	srv, err := restserver.NewServer(nil, cfg.RESTServerConfig(cfg.Middleware()))
	if err != nil {
		t.Fatalf("create REST server: %v", err)
	}
	// stopped is closed once Start returns; serveErr is its result.
	var serveErr error
	stopped := make(chan struct{})
	go func() {
		serveErr = srv.Start()
		close(stopped)
	}()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			t.Errorf("shutdown: %v", err)
		}
		<-stopped
		if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			t.Errorf("serve: %v", serveErr)
		}
	})

	// Poll the real condition — the listener answering /health — paced by a
	// ticker and bounded by a deadline; stop early if Start returns.
	url := "http://" + cfg.RESTAddr() + "/health"
	client := &http.Client{Timeout: 2 * time.Second}
	deadline := time.NewTimer(15 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	var lastErr error
	for {
		resp, err := client.Get(url)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("GET %s = %d, want 200", url, resp.StatusCode)
			}
			return
		}
		lastErr = err
		select {
		case <-stopped:
			t.Fatalf("REST server stopped before serving %s: %v", cfg.RESTAddr(), serveErr)
		case <-deadline.C:
			t.Fatalf("REST server did not answer on %s: %v", url, lastErr)
		case <-tick.C:
		}
	}
}
