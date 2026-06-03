//go:build conformance

// Cross-protocol conformance suite: launches one objstore-server process with
// all five transports enabled against a temp-dir local backend, then drives
// the unified Go SDK through an operation matrix asserting that every
// transport observes identical state — including byte-for-byte round trips of
// binary payloads written via one transport and read via every other.
//
// Run with: make conformance-test
// (or: go test -tags conformance -run TestConformance -v ./...)
package objstore

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// conformanceEnv holds the shared server process and per-protocol clients.
type conformanceEnv struct {
	clients map[string]Client
}

// freePort reserves an ephemeral TCP port and returns it. The listener is
// closed before returning, so a small race window exists; acceptable for
// tests on a single host.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// startConformanceServer builds and launches cmd/objstore-server with every
// transport enabled, returning a ready-to-use client per protocol.
func startConformanceServer(t *testing.T) *conformanceEnv {
	t.Helper()

	repoRoot, err := filepath.Abs("../../..")
	if err != nil {
		t.Fatalf("resolve repo root: %v", err)
	}

	binDir := t.TempDir()
	binPath := filepath.Join(binDir, "objstore-server")
	build := exec.Command("go", "build", "-o", binPath, "./cmd/objstore-server")
	build.Dir = repoRoot
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build objstore-server: %v\n%s", err, out)
	}

	storageDir := t.TempDir()
	restPort := freePort(t)
	grpcPort := freePort(t)
	quicPort := freePort(t)
	mcpPort := freePort(t)
	unixSock := filepath.Join(t.TempDir(), "objstore.sock")

	server := exec.Command(binPath,
		"--backend", "local",
		"--path", storageDir,
		"--grpc", "--grpc-addr", fmt.Sprintf("127.0.0.1:%d", grpcPort),
		"--rest", "--rest-port", fmt.Sprintf("%d", restPort),
		"--quic", "--quic-addr", fmt.Sprintf("127.0.0.1:%d", quicPort), "--quic-self-signed",
		"--mcp", "--mcp-mode", "http", "--mcp-addr", fmt.Sprintf("127.0.0.1:%d", mcpPort),
		"--unix", "--unix-socket", unixSock,
	)
	server.Stdout = os.Stderr
	server.Stderr = os.Stderr
	if err := server.Start(); err != nil {
		t.Fatalf("start objstore-server: %v", err)
	}
	t.Cleanup(func() {
		_ = server.Process.Kill()
		_, _ = server.Process.Wait()
	})

	// Wait for readiness: REST /health plus the unix socket file.
	healthURL := fmt.Sprintf("http://127.0.0.1:%d/health", restPort)
	deadline := time.Now().Add(30 * time.Second)
	for {
		if time.Now().After(deadline) {
			t.Fatal("server did not become ready within 30s")
		}
		resp, err := http.Get(healthURL)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				if _, err := os.Stat(unixSock); err == nil {
					break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	configs := map[string]*ClientConfig{
		"rest": {Protocol: ProtocolREST, Address: fmt.Sprintf("127.0.0.1:%d", restPort)},
		"grpc": {Protocol: ProtocolGRPC, Address: fmt.Sprintf("127.0.0.1:%d", grpcPort)},
		"quic": {Protocol: ProtocolQUIC, Address: fmt.Sprintf("127.0.0.1:%d", quicPort), UseTLS: true, InsecureSkipVerify: true},
		"mcp":  {Protocol: ProtocolMCP, Address: fmt.Sprintf("127.0.0.1:%d", mcpPort)},
		"unix": {Protocol: ProtocolUnix, Address: unixSock},
	}

	env := &conformanceEnv{clients: make(map[string]Client, len(configs))}
	for name, cfg := range configs {
		client, err := NewClient(cfg)
		if err != nil {
			t.Fatalf("NewClient(%s): %v", name, err)
		}
		t.Cleanup(func() { _ = client.Close() })
		env.clients[name] = client
	}

	return env
}

// protocolNames returns a stable iteration order.
func (e *conformanceEnv) protocolNames() []string {
	return []string{"rest", "grpc", "quic", "mcp", "unix"}
}

// TestConformance is the cross-protocol drift guard.
func TestConformance(t *testing.T) {
	env := startConformanceServer(t)
	ctx := context.Background()

	payloads := map[string][]byte{
		"empty":   {},
		"text":    []byte("hello conformance"),
		"binary":  {0x00, 0x01, 0xff, 0xfe, 0x80, 0x00, 0x7f},
		"unicode": []byte("héllo wörld — ✓ 日本語"),
		"large":   bytes.Repeat([]byte{0xAB, 0x00, 0xCD}, 350_000), // ~1 MiB with NULs
	}

	t.Run("round_trip_matrix", func(t *testing.T) {
		// Write each payload via every transport, then read it back via every
		// transport — byte-for-byte. This is the guard against encoding drift
		// (the class of bug where one SDK base64s and another does not).
		for _, writer := range env.protocolNames() {
			for payloadName, payload := range payloads {
				if writer == "unix" || writer == "mcp" {
					if payloadName == "empty" {
						// The JSON-RPC transports reject empty payloads at the
						// client validation layer; skip.
						continue
					}
				}
				key := fmt.Sprintf("conformance/%s/%s", writer, payloadName)
				if _, err := env.clients[writer].Put(ctx, key, payload, nil); err != nil {
					t.Errorf("put via %s (%s): %v", writer, payloadName, err)
					continue
				}
				for _, reader := range env.protocolNames() {
					got, err := env.clients[reader].Get(ctx, key)
					if err != nil {
						t.Errorf("get via %s of %s-written %s: %v", reader, writer, payloadName, err)
						continue
					}
					if !bytes.Equal(got.Data, payload) {
						t.Errorf("byte mismatch: wrote via %s, read via %s (%s): got %d bytes, want %d",
							writer, reader, payloadName, len(got.Data), len(payload))
					}
				}
			}
		}
	})

	t.Run("keys_with_slashes", func(t *testing.T) {
		key := "conformance/nested/deeply/key.txt"
		payload := []byte("nested key payload")
		if _, err := env.clients["rest"].Put(ctx, key, payload, nil); err != nil {
			t.Fatalf("put: %v", err)
		}
		for _, reader := range env.protocolNames() {
			got, err := env.clients[reader].Get(ctx, key)
			if err != nil {
				t.Errorf("get via %s: %v", reader, err)
				continue
			}
			if !bytes.Equal(got.Data, payload) {
				t.Errorf("byte mismatch via %s", reader)
			}
		}
	})

	t.Run("exists_and_delete_visibility", func(t *testing.T) {
		key := "conformance/visibility/obj"
		if _, err := env.clients["grpc"].Put(ctx, key, []byte("x"), nil); err != nil {
			t.Fatalf("put: %v", err)
		}
		for _, p := range env.protocolNames() {
			ok, err := env.clients[p].Exists(ctx, key)
			if err != nil || !ok {
				t.Errorf("exists via %s: ok=%v err=%v", p, ok, err)
			}
		}
		if err := env.clients["unix"].Delete(ctx, key); err != nil {
			t.Fatalf("delete via unix: %v", err)
		}
		for _, p := range env.protocolNames() {
			ok, err := env.clients[p].Exists(ctx, key)
			if err != nil {
				t.Errorf("exists via %s after delete: %v", p, err)
				continue
			}
			if ok {
				t.Errorf("object still visible via %s after delete", p)
			}
		}
	})

	t.Run("list_visibility", func(t *testing.T) {
		prefix := "conformance/list/"
		for i := 0; i < 3; i++ {
			key := fmt.Sprintf("%sitem-%d", prefix, i)
			if _, err := env.clients["quic"].Put(ctx, key, []byte("v"), nil); err != nil {
				t.Fatalf("put %s: %v", key, err)
			}
		}
		for _, p := range env.protocolNames() {
			res, err := env.clients[p].List(ctx, &ListOptions{Prefix: prefix})
			if err != nil {
				t.Errorf("list via %s: %v", p, err)
				continue
			}
			if len(res.Objects) != 3 {
				t.Errorf("list via %s: %d objects, want 3", p, len(res.Objects))
			}
		}
	})

	t.Run("get_missing_key_is_not_found", func(t *testing.T) {
		// Error-shape parity: a missing key must surface as a not-found error
		// (not a generic internal error) on every transport.
		for _, p := range env.protocolNames() {
			_, err := env.clients[p].Get(ctx, "conformance/definitely-missing")
			if err == nil {
				t.Errorf("get via %s: expected error for missing key", p)
				continue
			}
			msg := strings.ToLower(err.Error())
			if !strings.Contains(msg, "not found") && !strings.Contains(msg, "notfound") {
				t.Errorf("get via %s: error %q does not indicate not-found", p, err)
			}
		}
	})

	t.Run("replication_policy_parity", func(t *testing.T) {
		// Add a policy via one transport, observe it via the others, remove it
		// via yet another — the policy_id-vs-id class of drift guard.
		destDir := t.TempDir()
		policy := &ReplicationPolicy{
			ID:                   "conf-policy-1",
			SourceBackend:        "local",
			SourcePrefix:         "conformance/",
			DestinationBackend:   "local",
			DestinationSettings:  map[string]string{"path": destDir},
			CheckIntervalSeconds: 60,
			Enabled:              true,
		}
		if err := env.clients["rest"].AddReplicationPolicy(ctx, policy); err != nil {
			t.Fatalf("add policy via rest: %v", err)
		}

		for _, p := range env.protocolNames() {
			policies, err := env.clients[p].GetReplicationPolicies(ctx)
			if err != nil {
				t.Errorf("get policies via %s: %v", p, err)
				continue
			}
			found := false
			for _, got := range policies {
				if got.ID == policy.ID {
					found = true
				}
			}
			if !found {
				t.Errorf("policy %s not visible via %s", policy.ID, p)
			}
		}

		if err := env.clients["grpc"].RemoveReplicationPolicy(ctx, policy.ID); err != nil {
			t.Errorf("remove policy via grpc: %v", err)
		}
	})

	t.Run("health_parity", func(t *testing.T) {
		for _, p := range env.protocolNames() {
			status, err := env.clients[p].Health(ctx)
			if err != nil {
				t.Errorf("health via %s: %v", p, err)
				continue
			}
			if status == nil {
				t.Errorf("health via %s: nil status", p)
			}
		}
	})
}
