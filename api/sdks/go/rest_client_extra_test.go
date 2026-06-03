// Copyright (c) 2025 Jeremy Hahn
// Copyright (c) 2025 Automate The Things, LLC
//
// This file is part of go-objstore.
//
// go-objstore is dual-licensed under AGPL-3.0 and a Commercial License.

package objstore

// Language-specific REST extras beyond the canonical matrix: malformed-body
// decode failures, full metadata/field parsing on reads, and the nil-policy
// guard. These reach Go-specific branches in rest_client.go that the canonical
// cells do not.

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRESTClientBadJSON(t *testing.T) {
	ctx := context.Background()
	bad := func(t *testing.T) *RESTClient {
		return restServer(t, func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte("not json"))
		})
	}
	t.Run("put", func(t *testing.T) {
		_, err := bad(t).Put(ctx, "k", []byte("d"), nil)
		assert.Error(t, err)
	})
	t.Run("list", func(t *testing.T) {
		_, err := bad(t).List(ctx, nil)
		assert.Error(t, err)
	})
	t.Run("health", func(t *testing.T) {
		_, err := bad(t).Health(ctx)
		assert.Error(t, err)
	})
	t.Run("get_policies", func(t *testing.T) {
		_, err := bad(t).GetPolicies(ctx, "")
		assert.Error(t, err)
	})
	t.Run("apply_policies", func(t *testing.T) {
		_, err := bad(t).ApplyPolicies(ctx)
		assert.Error(t, err)
	})
	t.Run("get_replication_policies", func(t *testing.T) {
		_, err := bad(t).GetReplicationPolicies(ctx)
		assert.Error(t, err)
	})
	t.Run("get_replication_policy", func(t *testing.T) {
		_, err := bad(t).GetReplicationPolicy(ctx, "r")
		assert.Error(t, err)
	})
	t.Run("trigger_replication", func(t *testing.T) {
		_, err := bad(t).TriggerReplication(ctx, nil)
		assert.Error(t, err)
	})
	t.Run("get_replication_status", func(t *testing.T) {
		_, err := bad(t).GetReplicationStatus(ctx, "r")
		assert.Error(t, err)
	})
}

func TestRESTClientGetFullMetadata(t *testing.T) {
	ctx := context.Background()
	c := restServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Set("ETag", "e1")
		w.Header().Set("Content-Length", "5")
		w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
		w.Header().Set("X-Object-Metadata", `{"author":"alice"}`)
		_, _ = w.Write([]byte("hello"))
	})
	got, err := c.Get(ctx, "k")
	require.NoError(t, err)
	assert.Equal(t, "gzip", got.Metadata.ContentEncoding)
	assert.Equal(t, "e1", got.Metadata.ETag)
	assert.Equal(t, int64(5), got.Metadata.Size)
	assert.False(t, got.Metadata.LastModified.IsZero())
	assert.Equal(t, "alice", got.Metadata.Custom["author"])
}

func TestRESTClientListFullObjects(t *testing.T) {
	ctx := context.Background()
	c := restServer(t, func(w http.ResponseWriter, r *http.Request) {
		// Assert the query encoding for all four list options.
		q := r.URL.Query()
		assert.Equal(t, "p/", q.Get("prefix"))
		assert.Equal(t, "/", q.Get("delimiter"))
		assert.Equal(t, "10", q.Get("limit"))
		assert.Equal(t, "tok", q.Get("token"))
		writeJSON(w, map[string]any{
			"objects": []map[string]any{
				{"key": "p/a", "size": 100, "etag": "e", "modified": time.Now().UTC().Format(time.RFC3339)},
			},
			"common_prefixes": []string{"p/sub/"},
			"next_token":      "next",
			"truncated":       true,
		})
	})
	res, err := c.List(ctx, &ListOptions{Prefix: "p/", Delimiter: "/", MaxResults: 10, ContinueFrom: "tok"})
	require.NoError(t, err)
	require.Len(t, res.Objects, 1)
	assert.Equal(t, int64(100), res.Objects[0].Metadata.Size)
	assert.False(t, res.Objects[0].Metadata.LastModified.IsZero())
	assert.Equal(t, []string{"p/sub/"}, res.CommonPrefixes)
	assert.True(t, res.Truncated)
}

func TestRESTClientReplicationFullParsing(t *testing.T) {
	ctx := context.Background()
	t.Run("policies_opaque_and_time", func(t *testing.T) {
		c := restServer(t, func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, map[string]any{
				"policies": []map[string]any{{
					"id": "r1", "source_backend": "s3", "destination_backend": "gcs",
					"check_interval_seconds": 60, "enabled": true,
					"replication_mode": "opaque",
					"last_sync_time":   time.Now().UTC().Format(time.RFC3339),
				}},
			})
		})
		ps, err := c.GetReplicationPolicies(ctx)
		require.NoError(t, err)
		require.Len(t, ps, 1)
		assert.Equal(t, ReplicationModeOpaque, ps[0].ReplicationMode)
		assert.False(t, ps[0].LastSyncTime.IsZero())
	})
	t.Run("trigger_full_result", func(t *testing.T) {
		c := restServer(t, func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, map[string]any{
				"success": true,
				"result": map[string]any{
					"policy_id": "r1", "synced": 3, "deleted": 1, "failed": 0,
					"bytes_total": 1000, "duration": "1s", "errors": []string{"warn"},
				},
			})
		})
		res, err := c.TriggerReplication(ctx, &TriggerReplicationOptions{PolicyID: "r1", Parallel: true, WorkerCount: 2})
		require.NoError(t, err)
		assert.Equal(t, int64(1000), res.DurationMs)
		assert.Equal(t, []string{"warn"}, res.Errors)
	})
	t.Run("status_full", func(t *testing.T) {
		c := restServer(t, func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, map[string]any{
				"policy_id": "r1", "enabled": true,
				"total_objects_synced": 10, "total_objects_deleted": 2,
				"total_bytes_synced": 2048, "total_errors": 1,
				"average_sync_duration": "500ms", "sync_count": 4,
				"last_sync_time": time.Now().UTC().Format(time.RFC3339),
			})
		})
		st, err := c.GetReplicationStatus(ctx, "r1")
		require.NoError(t, err)
		assert.Equal(t, int64(500), st.AverageSyncDurationMs)
		assert.False(t, st.LastSyncTime.IsZero())
	})
}

func TestRESTClientPutETagFromBody(t *testing.T) {
	ctx := context.Background()
	// When no ETag header is present, Put falls back to data.etag in the body.
	c := restServer(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{"message": "ok", "data": map[string]any{"etag": "from-body"}})
	})
	res, err := c.Put(ctx, "k", []byte("d"), nil)
	require.NoError(t, err)
	assert.Equal(t, "from-body", res.ETag)
}

func TestRESTClientTransportError(t *testing.T) {
	ctx := context.Background()
	// A client pointed at an unroutable address exercises the c.httpClient.Do
	// error branch of each operation rather than a status-code branch.
	c, err := newRESTClient(&ClientConfig{Protocol: ProtocolREST, Address: "127.0.0.1:1", RequestTimeout: time.Second})
	require.NoError(t, err)

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

func TestRESTClientArchiveAndPolicyRequestShapes(t *testing.T) {
	ctx := context.Background()
	t.Run("archive_body", func(t *testing.T) {
		c := restServer(t, func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, http.MethodPost, r.Method)
			w.WriteHeader(http.StatusOK)
		})
		require.NoError(t, c.Archive(ctx, "k", "glacier", map[string]string{"tier": "deep"}))
	})
	t.Run("add_policy_with_destination", func(t *testing.T) {
		c := restServer(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusCreated) })
		require.NoError(t, c.AddPolicy(ctx, &LifecyclePolicy{
			ID: "p", Action: "archive", DestinationType: "glacier",
			DestinationSettings: map[string]string{"tier": "deep"},
		}))
	})
	t.Run("add_replication_transparent", func(t *testing.T) {
		c := restServer(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
		require.NoError(t, c.AddReplicationPolicy(ctx, &ReplicationPolicy{
			ID: "r", SourceBackend: "s3", DestinationBackend: "gcs",
			SourcePrefix: "src/", SourceSettings: map[string]string{"a": "b"},
			DestinationSettings: map[string]string{"c": "d"},
			ReplicationMode:     ReplicationModeTransparent,
		}))
	})
	t.Run("add_policy_nil", func(t *testing.T) {
		c := restServer(t, func(w http.ResponseWriter, r *http.Request) {})
		assert.ErrorIs(t, c.AddPolicy(ctx, nil), ErrInvalidConfig)
	})
	t.Run("add_replication_nil", func(t *testing.T) {
		c := restServer(t, func(w http.ResponseWriter, r *http.Request) {})
		assert.ErrorIs(t, c.AddReplicationPolicy(ctx, nil), ErrInvalidConfig)
	})
}
