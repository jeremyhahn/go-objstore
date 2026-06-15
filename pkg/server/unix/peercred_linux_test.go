//go:build linux

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

package unix

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// mustJSON marshals v to a json.RawMessage, failing the test on error.
func mustJSON(t *testing.T, v any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("failed to marshal params: %v", err)
	}
	return b
}

// recordingAuthorizer captures the principal it last saw and delegates the
// decision to an inner authorizer.
type recordingAuthorizer struct {
	inner adapters.Authorizer
	mu    sync.Mutex
	last  *adapters.Principal
}

func (r *recordingAuthorizer) Authorize(ctx context.Context, p *adapters.Principal, action, resource string) error {
	r.mu.Lock()
	r.last = p
	r.mu.Unlock()
	return r.inner.Authorize(ctx, p, action, resource)
}

func (r *recordingAuthorizer) principal() *adapters.Principal {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.last
}

// attrString returns a principal attribute as a string for assertions.
func attrString(p *adapters.Principal, key string) string {
	if p == nil || p.Attributes == nil {
		return ""
	}
	if v, ok := p.Attributes[key].(string); ok {
		return v
	}
	return ""
}

// startPeerCredServer initializes the facade and starts a Unix socket server
// over a real socket so the kernel populates SO_PEERCRED, then returns a live
// client connection. The server uses the given config (socket path, logger, and
// backend are filled in automatically).
func startPeerCredServer(t *testing.T, storage common.Storage, cfg ServerConfig) net.Conn {
	t.Helper()
	initTestFacade(t, storage)

	// Use a unique socket path per call so a single test can start more than one
	// server (e.g. a probe server plus the server under test) without collisions.
	socketPath := filepath.Join(t.TempDir(), "peercred.sock")
	cfg.SocketPath = socketPath
	if cfg.Logger == nil {
		cfg.Logger = &mockLogger{}
	}
	server, err := NewServer(&cfg)
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = server.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		cleanupSocket(t, socketPath)
	})

	// Wait for the socket to appear.
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, statErr := os.Stat(socketPath); statErr == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("server socket %q did not appear", socketPath)
		}
		time.Sleep(5 * time.Millisecond)
	}

	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// TestPeerCredExtractsCurrentProcessIdentity verifies that the server derives a
// principal from the connecting process's OS credentials via SO_PEERCRED.
func TestPeerCredExtractsCurrentProcessIdentity(t *testing.T) {
	rec := &recordingAuthorizer{inner: &adapters.NoOpAuthorizer{}}
	conn := startPeerCredServer(t, NewMockStorage(), ServerConfig{Authorizer: rec})

	resp := sendRequest(t, conn, &Request{JSONRPC: jsonRPCVersion, Method: MethodList, ID: 1})
	if resp.Error != nil {
		t.Fatalf("list failed: %+v", resp.Error)
	}

	p := rec.principal()
	if p == nil {
		t.Fatal("authorizer never saw a principal")
	}
	if p.Type != "unix-peer" {
		t.Fatalf("expected type unix-peer, got %q", p.Type)
	}
	wantUID := strconv.Itoa(os.Getuid())
	if got := attrString(p, "uid"); got != wantUID {
		t.Fatalf("expected uid %q, got %q (attrs=%v)", wantUID, got, p.Attributes)
	}
	if p.ID != "uid:"+wantUID {
		t.Fatalf("expected ID uid:%s, got %q", wantUID, p.ID)
	}
	wantGID := strconv.Itoa(os.Getgid())
	if got := attrString(p, "gid"); got != wantGID {
		t.Fatalf("expected gid %q, got %q", wantGID, got)
	}
	if attrString(p, "pid") == "" {
		t.Fatal("expected a non-empty pid attribute")
	}
	if len(p.Roles) == 0 {
		t.Fatal("expected at least one role derived from the primary group")
	}
}

// TestPeerCredBackwardCompatNoOp verifies that with peer credentials enabled
// (the default) and the default NoOpAuthorizer, all RPCs still succeed and the
// authorizer observes the real peer uid rather than an anonymous principal.
func TestPeerCredBackwardCompatNoOp(t *testing.T) {
	rec := &recordingAuthorizer{inner: &adapters.NoOpAuthorizer{}}
	conn := startPeerCredServer(t, NewMockStorage(), ServerConfig{Authorizer: rec})

	if resp := sendRequest(t, conn, &Request{JSONRPC: jsonRPCVersion, Method: MethodList, ID: 1}); resp.Error != nil {
		t.Fatalf("list should succeed under NoOp authorizer, got %+v", resp.Error)
	}

	p := rec.principal()
	if p == nil || attrString(p, "uid") != strconv.Itoa(os.Getuid()) {
		t.Fatalf("expected authorizer to see real peer uid, got %+v", p)
	}
	if p.ID == "anonymous" {
		t.Fatal("expected a peer-cred principal, not anonymous")
	}
}

// TestPeerCredRBACByRole verifies that an RBACAuthorizer keyed on the peer's
// derived role (its primary OS group / gid fallback) allows actions the role
// has and denies actions it lacks.
func TestPeerCredRBACByRole(t *testing.T) {
	// Discover the role the server derives for this process so the RBAC rules
	// key off the same value the peer-cred extractor produces.
	probe := &recordingAuthorizer{inner: &adapters.NoOpAuthorizer{}}
	probeConn := startPeerCredServer(t, NewMockStorage(), ServerConfig{Authorizer: probe})
	if resp := sendRequest(t, probeConn, &Request{JSONRPC: jsonRPCVersion, Method: MethodList, ID: 1}); resp.Error != nil {
		t.Fatalf("probe list failed: %+v", resp.Error)
	}
	p := probe.principal()
	if p == nil || len(p.Roles) == 0 {
		t.Fatalf("probe did not yield a role: %+v", p)
	}
	role := p.Roles[0]

	// Grant only read/list to the peer's role; write (put) must be denied.
	authz := adapters.NewRBACAuthorizer(map[string][]string{
		role: {adapters.ActionRead, adapters.ActionList},
	})
	conn := startPeerCredServer(t, NewMockStorage(), ServerConfig{Authorizer: authz})

	if resp := sendRequest(t, conn, &Request{JSONRPC: jsonRPCVersion, Method: MethodList, ID: 1}); resp.Error != nil {
		t.Fatalf("expected list to be allowed for role %q, got %+v", role, resp.Error)
	}
	putParams := mustJSON(t, PutParams{Key: "k", Data: "ZGF0YQ=="})
	resp := sendRequest(t, conn, &Request{JSONRPC: jsonRPCVersion, Method: MethodPut, Params: putParams, ID: 2})
	if resp.Error == nil || resp.Error.Code != ErrCodeForbidden {
		t.Fatalf("expected put to be forbidden for role %q, got %+v", role, resp.Error)
	}
}

// TestPeerCredDisabledFallsBack verifies that disabling peer credentials makes
// the server fall back to the configured Authenticator (anonymous via NoOp).
func TestPeerCredDisabledFallsBack(t *testing.T) {
	rec := &recordingAuthorizer{inner: &adapters.NoOpAuthorizer{}}
	conn := startPeerCredServer(t, NewMockStorage(), ServerConfig{
		Authorizer:         rec,
		UsePeerCredentials: DisablePeerCredentials(),
	})

	if resp := sendRequest(t, conn, &Request{JSONRPC: jsonRPCVersion, Method: MethodList, ID: 1}); resp.Error != nil {
		t.Fatalf("list failed: %+v", resp.Error)
	}
	p := rec.principal()
	if p == nil || p.ID != "anonymous" {
		t.Fatalf("expected anonymous principal when peer creds disabled, got %+v", p)
	}
}
