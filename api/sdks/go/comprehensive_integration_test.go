// Copyright (c) 2025 Jeremy Hahn
// Copyright (c) 2025 Automate The Things, LLC
//
// This file is part of go-objstore.
//
// go-objstore is dual-licensed under AGPL-3.0 and a Commercial License.

//go:build integration
// +build integration

// Package objstore contains the single comprehensive integration test suite.
//
// This is the canonical OPERATIONS × PROTOCOLS data-driven suite for the Go
// SDK.  Every one of the 19 operations is exercised against every available
// protocol (REST, gRPC, QUIC) with real assertions.  QUIC is enabled with
// InsecureSkipVerify because the integration server runs with --quic-self-signed.
//
// Build tag: integration
// Run:  go test -v -tags=integration -timeout=120s ./...
package objstore

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Environment helpers
// ---------------------------------------------------------------------------

func integrationRESTAddr() string {
	if addr := os.Getenv("OBJSTORE_REST_URL"); addr != "" {
		// Strip scheme so ClientConfig.Address receives host:port only.
		for _, pfx := range []string{"https://", "http://"} {
			if len(addr) >= len(pfx) && addr[:len(pfx)] == pfx {
				return addr[len(pfx):]
			}
		}
		return addr
	}
	return "objstore-server:8080"
}

func integrationGRPCAddr() string {
	if addr := os.Getenv("OBJSTORE_GRPC_HOST"); addr != "" {
		return addr
	}
	return "objstore-server:50051"
}

func integrationQUICAddr() string {
	if addr := os.Getenv("OBJSTORE_QUIC_URL"); addr != "" {
		const pfx = "https://"
		if len(addr) >= len(pfx) && addr[:len(pfx)] == pfx {
			return addr[len(pfx):]
		}
		return addr
	}
	return "objstore-server:4433"
}

// ---------------------------------------------------------------------------
// Protocol descriptor
// ---------------------------------------------------------------------------

type protoDesc struct {
	name  string
	proto Protocol
	addr  string
}

// availableProtocols returns all three protocols.  QUIC uses InsecureSkipVerify
// because the integration server runs with --quic-self-signed.
func availableProtocols() []protoDesc {
	return []protoDesc{
		{name: "REST", proto: ProtocolREST, addr: integrationRESTAddr()},
		{name: "gRPC", proto: ProtocolGRPC, addr: integrationGRPCAddr()},
		{name: "QUIC", proto: ProtocolQUIC, addr: integrationQUICAddr()},
	}
}

// newProtocolClient creates a client for the given protocol descriptor.
func newProtocolClient(t *testing.T, p protoDesc) Client {
	t.Helper()
	cfg := &ClientConfig{
		Protocol:          p.proto,
		Address:           p.addr,
		ConnectionTimeout: 10 * time.Second,
		RequestTimeout:    30 * time.Second,
		MaxRecvMsgSize:    10 * 1024 * 1024,
		MaxSendMsgSize:    10 * 1024 * 1024,
	}
	if p.proto == ProtocolQUIC {
		cfg.UseTLS = true
		cfg.InsecureSkipVerify = true
	}
	client, err := NewClient(cfg)
	require.NoError(t, err, "create client for %s", p.name)
	return client
}

// canonicalReplicationPolicy returns the canonical replication policy payload
// from the spec: source/destination both "local", async mode, 3600 s interval.
// The supplied id must be unique per test run.
func canonicalReplicationPolicy(id string) *ReplicationPolicy {
	srcPath := fmt.Sprintf("/tmp/repl-src-%s", id)
	dstPath := fmt.Sprintf("/tmp/repl-dst-%s", id)
	return &ReplicationPolicy{
		ID:                   id,
		SourceBackend:        "local",
		SourceSettings:       map[string]string{"path": srcPath},
		DestinationBackend:   "local",
		DestinationSettings:  map[string]string{"path": dstPath},
		CheckIntervalSeconds: 3600,
		Enabled:              true,
	}
}

// ---------------------------------------------------------------------------
// Operation descriptors
// ---------------------------------------------------------------------------

// opFunc is a single operation run against a client.
type opFunc func(ctx context.Context, t *testing.T, client Client, proto string)

type operation struct {
	name     string
	category string
	run      opFunc
}

// allOperations returns the canonical table of 19 operations + close.
func allOperations() []operation {
	return []operation{
		// ---------------------------------------------------------------
		// basic
		// ---------------------------------------------------------------
		{
			name:     "put",
			category: "basic",
			run: func(ctx context.Context, t *testing.T, client Client, proto string) {
				t.Helper()
				key := fmt.Sprintf("integ-%s-put-%d", proto, time.Now().UnixNano())
				data := []byte("hello from put")
				t.Cleanup(func() { client.Delete(ctx, key) }) //nolint:errcheck

				result, err := client.Put(ctx, key, data, &Metadata{
					ContentType: "text/plain",
					Custom:      map[string]string{"k": "v"},
				})
				require.NoError(t, err)
				assert.True(t, result.Success, "put.Success must be true")
			},
		},
		{
			name:     "get",
			category: "basic",
			run: func(ctx context.Context, t *testing.T, client Client, proto string) {
				t.Helper()
				key := fmt.Sprintf("integ-%s-get-%d", proto, time.Now().UnixNano())
				want := []byte("round-trip data")
				_, err := client.Put(ctx, key, want, nil)
				require.NoError(t, err, "setup put")
				t.Cleanup(func() { client.Delete(ctx, key) }) //nolint:errcheck

				got, err := client.Get(ctx, key)
				require.NoError(t, err)
				require.NotNil(t, got)
				assert.Equal(t, want, got.Data, "get data must match put data")
			},
		},
		{
			name:     "delete",
			category: "basic",
			run: func(ctx context.Context, t *testing.T, client Client, proto string) {
				t.Helper()
				key := fmt.Sprintf("integ-%s-delete-%d", proto, time.Now().UnixNano())
				_, err := client.Put(ctx, key, []byte("to be deleted"), nil)
				require.NoError(t, err, "setup put")

				require.NoError(t, client.Delete(ctx, key))

				exists, err := client.Exists(ctx, key)
				require.NoError(t, err)
				assert.False(t, exists, "object must not exist after delete")
			},
		},
		{
			name:     "exists",
			category: "basic",
			run: func(ctx context.Context, t *testing.T, client Client, proto string) {
				t.Helper()
				key := fmt.Sprintf("integ-%s-exists-%d", proto, time.Now().UnixNano())
				absent := fmt.Sprintf("integ-%s-absent-%d", proto, time.Now().UnixNano())

				_, err := client.Put(ctx, key, []byte("exists test"), nil)
				require.NoError(t, err, "setup put")
				t.Cleanup(func() { client.Delete(ctx, key) }) //nolint:errcheck

				yes, err := client.Exists(ctx, key)
				require.NoError(t, err)
				assert.True(t, yes, "exists must return true for existing object")

				no, err := client.Exists(ctx, absent)
				require.NoError(t, err)
				assert.False(t, no, "exists must return false for absent object")
			},
		},
		{
			name:     "list",
			category: "basic",
			run: func(ctx context.Context, t *testing.T, client Client, proto string) {
				t.Helper()
				prefix := fmt.Sprintf("integ-%s-list-%d", proto, time.Now().UnixNano())
				keys := []string{
					prefix + "/a",
					prefix + "/b",
					prefix + "/c",
				}
				for _, k := range keys {
					_, err := client.Put(ctx, k, []byte("list item"), nil)
					require.NoError(t, err, "setup put %s", k)
				}
				t.Cleanup(func() {
					for _, k := range keys {
						client.Delete(ctx, k) //nolint:errcheck
					}
				})

				result, err := client.List(ctx, &ListOptions{Prefix: prefix, MaxResults: 100})
				require.NoError(t, err)
				require.NotNil(t, result)

				found := make(map[string]bool)
				for _, obj := range result.Objects {
					found[obj.Key] = true
				}
				for _, k := range keys {
					assert.True(t, found[k], "list must contain key %s", k)
				}
				assert.GreaterOrEqual(t, len(result.Objects), len(keys), "list count must be >= number of put keys")
			},
		},

		// ---------------------------------------------------------------
		// metadata
		// ---------------------------------------------------------------
		{
			name:     "getMetadata",
			category: "metadata",
			run: func(ctx context.Context, t *testing.T, client Client, proto string) {
				t.Helper()
				key := fmt.Sprintf("integ-%s-getmeta-%d", proto, time.Now().UnixNano())
				data := []byte("metadata test")
				_, err := client.Put(ctx, key, data, &Metadata{
					ContentType: "application/octet-stream",
					Custom:      map[string]string{"env": "test"},
				})
				require.NoError(t, err, "setup put")
				t.Cleanup(func() { client.Delete(ctx, key) }) //nolint:errcheck

				meta, err := client.GetMetadata(ctx, key)
				require.NoError(t, err)
				require.NotNil(t, meta)
				assert.Equal(t, int64(len(data)), meta.Size, "metadata.Size must equal data length")
				assert.Equal(t, "application/octet-stream", meta.ContentType, "metadata.ContentType must match")
			},
		},
		{
			name:     "updateMetadata",
			category: "metadata",
			run: func(ctx context.Context, t *testing.T, client Client, proto string) {
				t.Helper()
				key := fmt.Sprintf("integ-%s-updatemeta-%d", proto, time.Now().UnixNano())
				_, err := client.Put(ctx, key, []byte("update metadata test"), &Metadata{
					ContentType: "text/plain",
					Custom:      map[string]string{"version": "1"},
				})
				require.NoError(t, err, "setup put")
				t.Cleanup(func() { client.Delete(ctx, key) }) //nolint:errcheck

				newMeta := &Metadata{
					ContentType: "application/json",
					Custom:      map[string]string{"version": "2", "updated": "true"},
				}
				err = client.UpdateMetadata(ctx, key, newMeta)
				require.NoError(t, err)

				// Read back and assert the new values persisted.
				got, err := client.GetMetadata(ctx, key)
				require.NoError(t, err)
				require.NotNil(t, got)
				assert.Equal(t, "application/json", got.ContentType, "updated ContentType must be read back")
				if got.Custom != nil {
					assert.Equal(t, "2", got.Custom["version"], "custom[version] must be updated value")
				}
			},
		},

		// ---------------------------------------------------------------
		// health
		// ---------------------------------------------------------------
		{
			name:     "health",
			category: "health",
			run: func(ctx context.Context, t *testing.T, client Client, proto string) {
				t.Helper()
				status, err := client.Health(ctx)
				require.NoError(t, err)
				require.NotNil(t, status)
				assert.NotEmpty(t, status.Status, "health.Status must not be empty")
				// Accept "SERVING", "serving", "healthy", etc.
				switch status.Status {
				case "SERVING", "serving", "healthy", "HEALTHY", "OK", "ok":
					// all valid healthy states
				default:
					t.Errorf("unexpected health status %q (expected SERVING/healthy/OK)", status.Status)
				}
			},
		},

		// ---------------------------------------------------------------
		// lifecycle
		// ---------------------------------------------------------------
		{
			name:     "addPolicy",
			category: "lifecycle",
			run: func(ctx context.Context, t *testing.T, client Client, proto string) {
				t.Helper()
				policy := &LifecyclePolicy{
					ID:               fmt.Sprintf("integ-%s-addpol-%d", proto, time.Now().UnixNano()),
					Prefix:           "temp/",
					RetentionSeconds: 3600,
					Action:           "delete",
				}
				t.Cleanup(func() { client.RemovePolicy(ctx, policy.ID) }) //nolint:errcheck

				err := client.AddPolicy(ctx, policy)
				require.NoError(t, err)
			},
		},
		{
			name:     "getPolicies",
			category: "lifecycle",
			run: func(ctx context.Context, t *testing.T, client Client, proto string) {
				t.Helper()
				policy := &LifecyclePolicy{
					ID:               fmt.Sprintf("integ-%s-getpols-%d", proto, time.Now().UnixNano()),
					Prefix:           "temp/",
					RetentionSeconds: 3600,
					Action:           "delete",
				}
				require.NoError(t, client.AddPolicy(ctx, policy), "setup addPolicy")
				t.Cleanup(func() { client.RemovePolicy(ctx, policy.ID) }) //nolint:errcheck

				policies, err := client.GetPolicies(ctx, "")
				require.NoError(t, err)
				require.NotNil(t, policies)

				var found bool
				for _, p := range policies {
					if p.ID == policy.ID {
						found = true
						break
					}
				}
				assert.True(t, found, "getPolicies must contain the added policy id %s", policy.ID)
			},
		},
		{
			name:     "removePolicy",
			category: "lifecycle",
			run: func(ctx context.Context, t *testing.T, client Client, proto string) {
				t.Helper()
				policy := &LifecyclePolicy{
					ID:               fmt.Sprintf("integ-%s-rempol-%d", proto, time.Now().UnixNano()),
					Prefix:           "temp/",
					RetentionSeconds: 3600,
					Action:           "delete",
				}
				require.NoError(t, client.AddPolicy(ctx, policy), "setup addPolicy")

				require.NoError(t, client.RemovePolicy(ctx, policy.ID))

				policies, err := client.GetPolicies(ctx, "")
				require.NoError(t, err)
				for _, p := range policies {
					assert.NotEqual(t, policy.ID, p.ID, "removed policy must not appear in getPolicies")
				}
			},
		},
		{
			name:     "applyPolicies",
			category: "lifecycle",
			run: func(ctx context.Context, t *testing.T, client Client, proto string) {
				t.Helper()
				result, err := client.ApplyPolicies(ctx)
				require.NoError(t, err)
				require.NotNil(t, result)
				assert.GreaterOrEqual(t, result.PoliciesCount, int32(0), "applyPolicies.PoliciesCount must be >= 0")
				assert.GreaterOrEqual(t, result.ObjectsProcessed, int32(0), "applyPolicies.ObjectsProcessed must be >= 0")
			},
		},

		// ---------------------------------------------------------------
		// archive
		// ---------------------------------------------------------------
		{
			name:     "archive",
			category: "archive",
			run: func(ctx context.Context, t *testing.T, client Client, proto string) {
				t.Helper()
				key := fmt.Sprintf("integ-%s-archive-%d", proto, time.Now().UnixNano())
				_, err := client.Put(ctx, key, []byte("archive me"), nil)
				require.NoError(t, err, "setup put")
				t.Cleanup(func() { client.Delete(ctx, key) }) //nolint:errcheck

				err = client.Archive(ctx, key, "glacier", map[string]string{"vault": "test-vault"})
				if err != nil {
					// Archive genuinely requires a glacier/archiver backend.  The
					// local backend does not ship a glacier archiver, so a
					// capability skip is appropriate here.
					t.Skipf("archive not supported on this backend: %v", err)
				}
			},
		},

		// ---------------------------------------------------------------
		// replication  (server now supports these — assert real success)
		// ---------------------------------------------------------------
		{
			name:     "addReplicationPolicy",
			category: "replication",
			run: func(ctx context.Context, t *testing.T, client Client, proto string) {
				t.Helper()
				id := fmt.Sprintf("integ-%s-addrepl-%d", proto, time.Now().UnixNano())
				policy := canonicalReplicationPolicy(id)
				t.Cleanup(func() { client.RemoveReplicationPolicy(ctx, id) }) //nolint:errcheck

				err := client.AddReplicationPolicy(ctx, policy)
				require.NoError(t, err, "addReplicationPolicy must succeed")
			},
		},
		{
			name:     "getReplicationPolicies",
			category: "replication",
			run: func(ctx context.Context, t *testing.T, client Client, proto string) {
				t.Helper()
				id := fmt.Sprintf("integ-%s-getrepls-%d", proto, time.Now().UnixNano())
				policy := canonicalReplicationPolicy(id)
				require.NoError(t, client.AddReplicationPolicy(ctx, policy), "setup addReplicationPolicy")
				t.Cleanup(func() { client.RemoveReplicationPolicy(ctx, id) }) //nolint:errcheck

				policies, err := client.GetReplicationPolicies(ctx)
				require.NoError(t, err)
				require.NotNil(t, policies)
				assert.GreaterOrEqual(t, len(policies), 1, "getReplicationPolicies count must be >= 1")

				var found bool
				for _, p := range policies {
					if p.ID == id {
						found = true
						break
					}
				}
				assert.True(t, found, "getReplicationPolicies must contain the added policy id %s", id)
			},
		},
		{
			name:     "getReplicationPolicy",
			category: "replication",
			run: func(ctx context.Context, t *testing.T, client Client, proto string) {
				t.Helper()
				id := fmt.Sprintf("integ-%s-getrepl-%d", proto, time.Now().UnixNano())
				policy := canonicalReplicationPolicy(id)
				require.NoError(t, client.AddReplicationPolicy(ctx, policy), "setup addReplicationPolicy")
				t.Cleanup(func() { client.RemoveReplicationPolicy(ctx, id) }) //nolint:errcheck

				got, err := client.GetReplicationPolicy(ctx, id)
				require.NoError(t, err)
				require.NotNil(t, got)
				assert.Equal(t, id, got.ID, "getReplicationPolicy.ID must match")
				assert.Equal(t, "local", got.SourceBackend, "getReplicationPolicy.SourceBackend must be local")
				assert.Equal(t, "local", got.DestinationBackend, "getReplicationPolicy.DestinationBackend must be local")
				assert.Equal(t, int64(3600), got.CheckIntervalSeconds, "getReplicationPolicy.CheckIntervalSeconds must be 3600")
			},
		},
		{
			name:     "removeReplicationPolicy",
			category: "replication",
			run: func(ctx context.Context, t *testing.T, client Client, proto string) {
				t.Helper()
				id := fmt.Sprintf("integ-%s-remrepl-%d", proto, time.Now().UnixNano())
				policy := canonicalReplicationPolicy(id)
				require.NoError(t, client.AddReplicationPolicy(ctx, policy), "setup addReplicationPolicy")

				require.NoError(t, client.RemoveReplicationPolicy(ctx, id))

				policies, err := client.GetReplicationPolicies(ctx)
				require.NoError(t, err)
				for _, p := range policies {
					assert.NotEqual(t, id, p.ID, "removed replication policy must not appear in getReplicationPolicies")
				}
			},
		},
		{
			name:     "triggerReplication",
			category: "replication",
			run: func(ctx context.Context, t *testing.T, client Client, proto string) {
				t.Helper()
				id := fmt.Sprintf("integ-%s-trigger-%d", proto, time.Now().UnixNano())
				policy := canonicalReplicationPolicy(id)
				require.NoError(t, client.AddReplicationPolicy(ctx, policy), "setup addReplicationPolicy")
				t.Cleanup(func() { client.RemoveReplicationPolicy(ctx, id) }) //nolint:errcheck

				result, err := client.TriggerReplication(ctx, &TriggerReplicationOptions{PolicyID: id})
				require.NoError(t, err)
				require.NotNil(t, result)
				assert.Equal(t, id, result.PolicyID, "triggerReplication result.PolicyID must match")
				assert.GreaterOrEqual(t, result.Synced, int32(0), "triggerReplication result.Synced must be >= 0")
				assert.GreaterOrEqual(t, result.BytesTotal, int64(0), "triggerReplication result.BytesTotal must be >= 0")
				assert.GreaterOrEqual(t, result.DurationMs, int64(0), "triggerReplication result.DurationMs must be >= 0")
			},
		},
		{
			name:     "getReplicationStatus",
			category: "replication",
			run: func(ctx context.Context, t *testing.T, client Client, proto string) {
				t.Helper()
				id := fmt.Sprintf("integ-%s-status-%d", proto, time.Now().UnixNano())
				policy := canonicalReplicationPolicy(id)
				require.NoError(t, client.AddReplicationPolicy(ctx, policy), "setup addReplicationPolicy")
				t.Cleanup(func() { client.RemoveReplicationPolicy(ctx, id) }) //nolint:errcheck

				status, err := client.GetReplicationStatus(ctx, id)
				require.NoError(t, err)
				require.NotNil(t, status)
				assert.Equal(t, id, status.PolicyID, "getReplicationStatus.PolicyID must match")
				assert.GreaterOrEqual(t, status.TotalObjectsSynced, int64(0), "TotalObjectsSynced must be >= 0")
				assert.GreaterOrEqual(t, status.SyncCount, int64(0), "SyncCount must be >= 0")
			},
		},

		// ---------------------------------------------------------------
		// close / dispose
		// ---------------------------------------------------------------
		{
			name:     "close",
			category: "close",
			run: func(ctx context.Context, t *testing.T, client Client, proto string) {
				t.Helper()
				// Idempotent: first close is part of the shared client lifecycle.
				// Here we create a separate short-lived client and close it twice.
				var (
					p     protoDesc
					found bool
				)
				for _, pd := range availableProtocols() {
					if pd.name == proto {
						p = pd
						found = true
						break
					}
				}
				if !found {
					t.Fatalf("no protocol descriptor for %s", proto)
				}
				c2 := newProtocolClient(t, p)
				assert.NoError(t, c2.Close(), "first Close must not error")
				assert.NoError(t, c2.Close(), "second Close must be idempotent")
			},
		},
	}
}

// ---------------------------------------------------------------------------
// Primary driver: OPERATIONS × PROTOCOLS
// ---------------------------------------------------------------------------

// TestComprehensiveIntegrationAllOperations is the single table-driven suite.
// It runs every operation against every available protocol.
func TestComprehensiveIntegrationAllOperations(t *testing.T) {
	ctx := context.Background()
	protocols := availableProtocols()
	ops := allOperations()

	for _, p := range protocols {
		p := p
		t.Run(p.name, func(t *testing.T) {
			client := newProtocolClient(t, p)
			defer client.Close() //nolint:errcheck

			for _, op := range ops {
				op := op
				t.Run(op.category+"/"+op.name, func(t *testing.T) {
					op.run(ctx, t, client, p.name)
				})
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Cross-protocol consistency
// ---------------------------------------------------------------------------

// TestCrossProtocolConsistency verifies that every ordered (A,B) protocol pair
// is consistent: put via A then get/metadata/delete via B.
func TestCrossProtocolConsistency(t *testing.T) {
	ctx := context.Background()
	protocols := availableProtocols()

	// Build client map lazily — only create what is needed.
	clients := make(map[string]Client, len(protocols))
	for _, p := range protocols {
		p := p
		c := newProtocolClient(t, p)
		t.Cleanup(func() { c.Close() }) //nolint:errcheck
		clients[p.name] = c
	}

	for _, a := range protocols {
		for _, b := range protocols {
			if a.name == b.name {
				continue
			}
			a, b := a, b
			t.Run(fmt.Sprintf("%s->%s", a.name, b.name), func(t *testing.T) {
				clientA := clients[a.name]
				clientB := clients[b.name]
				key := fmt.Sprintf("cross-%s-%s-%d", a.name, b.name, time.Now().UnixNano())
				want := []byte(fmt.Sprintf("cross-protocol data %s->%s", a.name, b.name))
				const contentType = "application/octet-stream"

				// put via A
				res, err := clientA.Put(ctx, key, want, &Metadata{
					ContentType: contentType,
				})
				require.NoError(t, err, "put via %s", a.name)
				assert.True(t, res.Success)
				t.Cleanup(func() { clientA.Delete(ctx, key) }) //nolint:errcheck

				// get via B — assert data equal
				got, err := clientB.Get(ctx, key)
				require.NoError(t, err, "get via %s", b.name)
				require.NotNil(t, got)
				assert.True(t, bytes.Equal(want, got.Data),
					"data mismatch: put via %s, get via %s", a.name, b.name)

				// getMetadata via B — assert size and content_type
				meta, err := clientB.GetMetadata(ctx, key)
				require.NoError(t, err, "getMetadata via %s", b.name)
				require.NotNil(t, meta)
				assert.Equal(t, int64(len(want)), meta.Size,
					"metadata.Size mismatch across %s->%s", a.name, b.name)
				assert.Equal(t, contentType, meta.ContentType,
					"metadata.ContentType mismatch across %s->%s", a.name, b.name)

				// delete via A, then exists via B must be false
				require.NoError(t, clientA.Delete(ctx, key), "delete via %s", a.name)
				exists, err := clientB.Exists(ctx, key)
				require.NoError(t, err, "exists via %s", b.name)
				assert.False(t, exists, "exists must be false after delete via %s (checked via %s)", a.name, b.name)
			})
		}
	}
}

// ---------------------------------------------------------------------------
// Error-handling cases
// ---------------------------------------------------------------------------

// TestIntegrationErrorHandling verifies that every protocol surfaces errors
// correctly for common negative cases.
func TestIntegrationErrorHandling(t *testing.T) {
	ctx := context.Background()

	t.Run("NonexistentObject", func(t *testing.T) {
		for _, p := range availableProtocols() {
			p := p
			t.Run(p.name, func(t *testing.T) {
				client := newProtocolClient(t, p)
				defer client.Close() //nolint:errcheck

				key := fmt.Sprintf("nonexistent-%s-%d", p.name, time.Now().UnixNano())
				_, err := client.Get(ctx, key)
				assert.Error(t, err, "%s: get nonexistent must error", p.name)
			})
		}
	})

	t.Run("EmptyKey", func(t *testing.T) {
		for _, p := range availableProtocols() {
			p := p
			t.Run(p.name, func(t *testing.T) {
				client := newProtocolClient(t, p)
				defer client.Close() //nolint:errcheck

				_, err := client.Put(ctx, "", []byte("data"), nil)
				assert.ErrorIs(t, err, ErrInvalidKey, "%s: put empty key", p.name)

				_, err = client.Get(ctx, "")
				assert.ErrorIs(t, err, ErrInvalidKey, "%s: get empty key", p.name)
			})
		}
	})

	t.Run("RESTObjectNotFound", func(t *testing.T) {
		client := newProtocolClient(t, protoDesc{name: "REST", proto: ProtocolREST, addr: integrationRESTAddr()})
		defer client.Close() //nolint:errcheck

		key := fmt.Sprintf("nonexistent-rest-%d", time.Now().UnixNano())

		_, err := client.Get(ctx, key)
		assert.ErrorIs(t, err, ErrObjectNotFound)

		err = client.UpdateMetadata(ctx, key, &Metadata{ContentType: "text/plain"})
		assert.ErrorIs(t, err, ErrObjectNotFound)
	})

	t.Run("GRPCObjectNotFound", func(t *testing.T) {
		client := newProtocolClient(t, protoDesc{name: "gRPC", proto: ProtocolGRPC, addr: integrationGRPCAddr()})
		defer client.Close() //nolint:errcheck

		key := fmt.Sprintf("nonexistent-grpc-%d", time.Now().UnixNano())
		_, err := client.Get(ctx, key)
		assert.Error(t, err)
	})

	t.Run("NonexistentDeleteIdempotency", func(t *testing.T) {
		// Some backends return success for idempotent deletes; we accept either.
		for _, p := range availableProtocols() {
			p := p
			t.Run(p.name, func(t *testing.T) {
				client := newProtocolClient(t, p)
				defer client.Close() //nolint:errcheck

				key := fmt.Sprintf("nonexistent-del-%s-%d", p.name, time.Now().UnixNano())
				err := client.Delete(ctx, key)
				if err != nil {
					// Either ErrObjectNotFound or similar is acceptable.
					t.Logf("%s: delete nonexistent returned (acceptable): %v", p.name, err)
				}
			})
		}
	})
}

// ---------------------------------------------------------------------------
// Concurrent operations
// ---------------------------------------------------------------------------

// TestIntegrationConcurrentOperations verifies the gRPC client is safe
// for concurrent use.
func TestIntegrationConcurrentOperations(t *testing.T) {
	client := newProtocolClient(t, protoDesc{name: "gRPC", proto: ProtocolGRPC, addr: integrationGRPCAddr()})
	defer client.Close() //nolint:errcheck

	ctx := context.Background()
	const n = 10
	var wg sync.WaitGroup
	wg.Add(n)

	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			key := fmt.Sprintf("concurrent-%d-%d", i, time.Now().UnixNano())
			data := []byte(fmt.Sprintf("goroutine-%d", i))

			_, err := client.Put(ctx, key, data, nil)
			assert.NoError(t, err)

			got, err := client.Get(ctx, key)
			assert.NoError(t, err)
			if err == nil {
				assert.Equal(t, data, got.Data)
			}

			assert.NoError(t, client.Delete(ctx, key))
		}()
	}

	wg.Wait()
}

// ---------------------------------------------------------------------------
// Large objects
// ---------------------------------------------------------------------------

// TestIntegrationLargeObject verifies that a 1 MiB object survives a
// put/get round-trip with byte-exact equality.
func TestIntegrationLargeObject(t *testing.T) {
	for _, p := range availableProtocols() {
		p := p
		t.Run(p.name, func(t *testing.T) {
			cfg := &ClientConfig{
				Protocol:          p.proto,
				Address:           p.addr,
				ConnectionTimeout: 10 * time.Second,
				RequestTimeout:    60 * time.Second,
				MaxRecvMsgSize:    10 * 1024 * 1024,
				MaxSendMsgSize:    10 * 1024 * 1024,
			}
			if p.proto == ProtocolQUIC {
				cfg.UseTLS = true
				cfg.InsecureSkipVerify = true
			}
			client, err := NewClient(cfg)
			require.NoError(t, err)
			defer client.Close() //nolint:errcheck

			ctx := context.Background()
			key := fmt.Sprintf("large-%s-%d", p.name, time.Now().UnixNano())

			data := make([]byte, 1024*1024)
			for i := range data {
				data[i] = byte(i % 256)
			}

			res, err := client.Put(ctx, key, data, nil)
			require.NoError(t, err)
			assert.True(t, res.Success)
			t.Cleanup(func() { client.Delete(ctx, key) }) //nolint:errcheck

			got, err := client.Get(ctx, key)
			require.NoError(t, err)
			assert.Equal(t, len(data), len(got.Data))
			assert.Equal(t, data, got.Data)
		})
	}
}

// ---------------------------------------------------------------------------
// REST-specific (ApplyPolicies)
// ---------------------------------------------------------------------------

// TestIntegrationRESTApplyPolicies verifies the REST /policies/apply endpoint.
func TestIntegrationRESTApplyPolicies(t *testing.T) {
	client := newProtocolClient(t, protoDesc{name: "REST", proto: ProtocolREST, addr: integrationRESTAddr()})
	defer client.Close() //nolint:errcheck

	result, err := client.ApplyPolicies(context.Background())
	require.NoError(t, err)
	assert.NotNil(t, result)
}
