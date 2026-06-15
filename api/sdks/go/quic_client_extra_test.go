// Copyright (c) 2025 Jeremy Hahn
// Copyright (c) 2025 Automate The Things, LLC
//
// This file is part of go-objstore.
//
// go-objstore is dual-licensed under AGPL-3.0 and a Commercial License.

package objstore

// Language-specific QUIC extras beyond the canonical matrix: malformed-body
// decode failures, exists-query handling, full list/replication parsing,
// transport errors, the nil-policy guard, and the Close no-op. These reach
// Go-specific branches in quic_client.go that the canonical cells do not.

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQUICClientBadJSON(t *testing.T) {
	ctx := context.Background()
	bad := func(t *testing.T) *QUICClient {
		return quicServer(t, func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte("not json"))
		})
	}
	t.Run("list", func(t *testing.T) { _, e := bad(t).List(ctx, nil); assert.Error(t, e) })
	t.Run("exists", func(t *testing.T) { _, e := bad(t).Exists(ctx, "k"); assert.Error(t, e) })
	t.Run("health", func(t *testing.T) { _, e := bad(t).Health(ctx); assert.Error(t, e) })
	t.Run("get_policies", func(t *testing.T) { _, e := bad(t).GetPolicies(ctx, ""); assert.Error(t, e) })
	t.Run("apply_policies", func(t *testing.T) { _, e := bad(t).ApplyPolicies(ctx); assert.Error(t, e) })
	t.Run("get_repl_policies", func(t *testing.T) { _, e := bad(t).GetReplicationPolicies(ctx); assert.Error(t, e) })
	t.Run("get_repl_policy", func(t *testing.T) { _, e := bad(t).GetReplicationPolicy(ctx, "r"); assert.Error(t, e) })
	t.Run("trigger", func(t *testing.T) { _, e := bad(t).TriggerReplication(ctx, nil); assert.Error(t, e) })
	t.Run("status", func(t *testing.T) { _, e := bad(t).GetReplicationStatus(ctx, "r"); assert.Error(t, e) })
}

func TestQUICClientExistsQuery(t *testing.T) {
	ctx := context.Background()
	c := quicServer(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "1", r.URL.Query().Get("exists"))
		writeJSON(w, map[string]any{"exists": true})
	})
	ok, err := c.Exists(ctx, "k")
	require.NoError(t, err)
	assert.True(t, ok)
}

func TestQUICClientListFullObjects(t *testing.T) {
	ctx := context.Background()
	c := quicServer(t, func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		assert.Equal(t, "p/", q.Get("prefix"))
		assert.Equal(t, "/", q.Get("delimiter"))
		assert.Equal(t, "10", q.Get("max"))
		assert.Equal(t, "tok", q.Get("continue"))
		writeJSON(w, map[string]any{
			"objects": []map[string]any{
				{"key": "p/a", "metadata": map[string]any{
					"content_type": "text/plain", "size": 100, "etag": "e",
					"custom": map[string]string{"k": "v"},
				}},
				{"key": "p/b"},
			},
			"prefixes":   []string{"p/sub/"},
			"next_token": "next",
			"truncated":  true,
		})
	})
	res, err := c.List(ctx, &ListOptions{Prefix: "p/", Delimiter: "/", MaxResults: 10, ContinueFrom: "tok"})
	require.NoError(t, err)
	require.Len(t, res.Objects, 2)
	assert.Equal(t, int64(100), res.Objects[0].Metadata.Size)
	assert.Equal(t, "v", res.Objects[0].Metadata.Custom["k"])
	assert.Nil(t, res.Objects[1].Metadata)
	assert.Equal(t, []string{"p/sub/"}, res.CommonPrefixes)
	assert.True(t, res.Truncated)
}

func TestQUICClientGetMetadataFullHeaders(t *testing.T) {
	ctx := context.Background()
	c := quicServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("ETag", "e1")
		w.Header().Set("Content-Length", "1024")
		w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
		w.Header().Set("X-Meta-author", "alice")
		w.WriteHeader(http.StatusOK)
	})
	md, err := c.GetMetadata(ctx, "k")
	require.NoError(t, err)
	assert.Equal(t, "e1", md.ETag)
	assert.Equal(t, int64(1024), md.Size)
	assert.False(t, md.LastModified.IsZero())
	assert.Equal(t, "alice", md.Custom["author"])
}

func TestQUICClientReplicationFullParsing(t *testing.T) {
	ctx := context.Background()
	t.Run("policies_opaque_time", func(t *testing.T) {
		c := quicServer(t, func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, map[string]any{
				"success": true,
				"policies": []map[string]any{{
					"id": "r1", "source_backend": "s3", "destination_backend": "gcs",
					"check_interval": 60, "enabled": true, "replication_mode": "opaque",
					"last_sync_time": time.Now().UTC().Format(time.RFC3339),
				}},
			})
		})
		ps, err := c.GetReplicationPolicies(ctx)
		require.NoError(t, err)
		require.Len(t, ps, 1)
		assert.Equal(t, ReplicationModeOpaque, ps[0].ReplicationMode)
		assert.False(t, ps[0].LastSyncTime.IsZero())
	})
	t.Run("trigger_full", func(t *testing.T) {
		c := quicServer(t, func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, "r1", r.URL.Query().Get("policy_id"))
			writeJSON(w, map[string]any{
				"success": true,
				"result": map[string]any{
					"policy_id": "r1", "synced": 3, "deleted": 1, "bytes_total": 1000,
					"duration": "1s", "errors": []string{"warn"},
				},
			})
		})
		res, err := c.TriggerReplication(ctx, &TriggerReplicationOptions{PolicyID: "r1"})
		require.NoError(t, err)
		assert.Equal(t, int64(1000), res.DurationMs)
		assert.Equal(t, []string{"warn"}, res.Errors)
	})
	t.Run("status_full", func(t *testing.T) {
		c := quicServer(t, func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, map[string]any{
				"success": true, "policy_id": "r1", "enabled": true,
				"total_objects_synced": 10, "average_sync_duration": "500ms",
				"last_sync_time": time.Now().UTC().Format(time.RFC3339),
			})
		})
		st, err := c.GetReplicationStatus(ctx, "r1")
		require.NoError(t, err)
		assert.Equal(t, int64(500), st.AverageSyncDurationMs)
		assert.False(t, st.LastSyncTime.IsZero())
	})
}

func TestQUICClientAddReplicationTransparent(t *testing.T) {
	ctx := context.Background()
	c := quicServer(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	require.NoError(t, c.AddReplicationPolicy(ctx, &ReplicationPolicy{
		ID: "r", SourceBackend: "s3", DestinationBackend: "gcs",
		SourcePrefix: "src/", SourceSettings: map[string]string{"a": "b"},
		DestinationSettings: map[string]string{"c": "d"},
		ReplicationMode:     ReplicationModeTransparent,
	}))
}

func TestQUICClientNilPolicyGuards(t *testing.T) {
	ctx := context.Background()
	c := &QUICClient{config: &ClientConfig{}}
	assert.ErrorIs(t, c.AddPolicy(ctx, nil), ErrInvalidConfig)
	assert.ErrorIs(t, c.AddReplicationPolicy(ctx, nil), ErrInvalidConfig)
}

func TestQUICClientCloseNoop(t *testing.T) {
	assert.NoError(t, (&QUICClient{config: &ClientConfig{}}).Close())
}

func TestQUICClientPutETagHeader(t *testing.T) {
	ctx := context.Background()
	// Put with no metadata returns the ETag from the response header.
	c := quicServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", "e9")
		w.WriteHeader(http.StatusOK)
	})
	res, err := c.Put(ctx, "k", []byte("d"), nil)
	require.NoError(t, err)
	assert.Equal(t, "e9", res.ETag)
}

func TestQUICClientTransportError(t *testing.T) {
	ctx := context.Background()
	// A client pointed at an unroutable address exercises the c.httpClient.Do
	// error branch of each operation rather than a status-code branch.
	c := quicServer(t, func(w http.ResponseWriter, r *http.Request) {})
	c.baseURL = "http://127.0.0.1:1" // unroutable port forces a transport error

	if _, e := c.Put(ctx, "k", []byte("d"), nil); e == nil {
		t.Error("Put: expected transport error")
	}
	if _, e := c.Get(ctx, "k"); e == nil {
		t.Error("Get: expected transport error")
	}
	if e := c.Delete(ctx, "k"); e == nil {
		t.Error("Delete: expected transport error")
	}
	if _, e := c.List(ctx, nil); e == nil {
		t.Error("List: expected transport error")
	}
	if _, e := c.Exists(ctx, "k"); e == nil {
		t.Error("Exists: expected transport error")
	}
	if _, e := c.GetMetadata(ctx, "k"); e == nil {
		t.Error("GetMetadata: expected transport error")
	}
	if e := c.UpdateMetadata(ctx, "k", &Metadata{}); e == nil {
		t.Error("UpdateMetadata: expected transport error")
	}
	if _, e := c.Health(ctx); e == nil {
		t.Error("Health: expected transport error")
	}
	if e := c.Archive(ctx, "k", "g", nil); e == nil {
		t.Error("Archive: expected transport error")
	}
	if e := c.AddPolicy(ctx, &LifecyclePolicy{ID: "p"}); e == nil {
		t.Error("AddPolicy: expected transport error")
	}
	if e := c.RemovePolicy(ctx, "p"); e == nil {
		t.Error("RemovePolicy: expected transport error")
	}
	if _, e := c.GetPolicies(ctx, ""); e == nil {
		t.Error("GetPolicies: expected transport error")
	}
	if _, e := c.ApplyPolicies(ctx); e == nil {
		t.Error("ApplyPolicies: expected transport error")
	}
	if e := c.AddReplicationPolicy(ctx, &ReplicationPolicy{ID: "r"}); e == nil {
		t.Error("AddReplicationPolicy: expected transport error")
	}
	if e := c.RemoveReplicationPolicy(ctx, "r"); e == nil {
		t.Error("RemoveReplicationPolicy: expected transport error")
	}
	if _, e := c.GetReplicationPolicies(ctx); e == nil {
		t.Error("GetReplicationPolicies: expected transport error")
	}
	if _, e := c.GetReplicationPolicy(ctx, "r"); e == nil {
		t.Error("GetReplicationPolicy: expected transport error")
	}
	if _, e := c.TriggerReplication(ctx, nil); e == nil {
		t.Error("TriggerReplication: expected transport error")
	}
	if _, e := c.GetReplicationStatus(ctx, "r"); e == nil {
		t.Error("GetReplicationStatus: expected transport error")
	}
}
