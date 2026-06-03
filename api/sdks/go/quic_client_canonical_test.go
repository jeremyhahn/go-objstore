// Copyright (c) 2025 Jeremy Hahn
// Copyright (c) 2025 Automate The Things, LLC
//
// This file is part of go-objstore.
//
// go-objstore is dual-licensed under AGPL-3.0 and a Commercial License.

package objstore

// This file implements the canonical SDK unit-test matrix for the QUIC
// protocol client. The QUIC client speaks HTTP semantics over its transport,
// so the tests drive it through an httptest server (quicServer) just like
// REST, but assert the QUIC-specific wire scheme (per-key X-Meta-* request
// headers, HEAD-based metadata reads).

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// quicServer starts an httptest server and returns a QUICClient whose HTTP
// client targets it. The QUIC client always builds https:// URLs, so we point
// its baseURL and reuse the test server's (plain HTTP) client.
func quicServer(t *testing.T, h http.HandlerFunc) *QUICClient {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	c, err := newQUICClient(&ClientConfig{
		Protocol:           ProtocolQUIC,
		Address:            srv.Listener.Addr().String(),
		UseTLS:             true,
		InsecureSkipVerify: true,
	})
	require.NoError(t, err)
	// Replace the HTTP/3 transport with a plain HTTP client targeting the
	// httptest listener. DisableCompression avoids Go's transparent gzip
	// handling surfacing as a spurious "unexpected EOF" when a request carries
	// Content-Encoding: gzip.
	c.httpClient = &http.Client{Transport: &http.Transport{DisableCompression: true}}
	c.baseURL = srv.URL
	return c
}

// quicStatus returns a QUICClient whose server always replies with code.
func quicStatus(t *testing.T, code int) *QUICClient {
	return quicServer(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(code) })
}

func TestQUICClientCanonical(t *testing.T) {
	ctx := context.Background()

	// ---------------------------------------------------------------- success

	t.Run("quic_put_success", func(t *testing.T) {
		c := quicServer(t, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("ETag", "e1")
			w.WriteHeader(http.StatusCreated)
		})
		res, err := c.Put(ctx, "k", []byte("d"), &Metadata{ContentType: "text/plain"})
		require.NoError(t, err)
		assert.True(t, res.Success)
		assert.Equal(t, "e1", res.ETag)
	})

	t.Run("quic_get_success", func(t *testing.T) {
		c := quicServer(t, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/plain")
			_, _ = w.Write([]byte("hello"))
		})
		res, err := c.Get(ctx, "k")
		require.NoError(t, err)
		assert.Equal(t, []byte("hello"), res.Data)
		assert.Equal(t, "text/plain", res.Metadata.ContentType)
	})

	t.Run("quic_delete_success", func(t *testing.T) {
		c := quicStatus(t, http.StatusNoContent)
		assert.NoError(t, c.Delete(ctx, "k"))
	})

	t.Run("quic_list_success", func(t *testing.T) {
		c := quicServer(t, func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, map[string]any{
				"objects":   []map[string]any{{"key": "a"}, {"key": "b"}},
				"truncated": false,
			})
		})
		res, err := c.List(ctx, &ListOptions{Prefix: "x/"})
		require.NoError(t, err)
		assert.Len(t, res.Objects, 2)
	})

	t.Run("quic_exists_success", func(t *testing.T) {
		c := quicServer(t, func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, map[string]any{"exists": true})
		})
		ok, err := c.Exists(ctx, "k")
		require.NoError(t, err)
		assert.True(t, ok)
	})

	t.Run("quic_get_metadata_success", func(t *testing.T) {
		c := quicServer(t, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Meta-author", "alice")
			w.WriteHeader(http.StatusOK)
		})
		md, err := c.GetMetadata(ctx, "k")
		require.NoError(t, err)
		assert.Equal(t, "application/json", md.ContentType)
		// The QUIC client lowercases the suffix stripped from the X-Meta- prefix
		// so that "X-Meta-author" (canonicalized by Go's HTTP stack to
		// "X-Meta-Author") is stored under the original lowercase key "author".
		assert.Equal(t, "alice", md.Custom["author"])
	})

	t.Run("quic_update_metadata_success", func(t *testing.T) {
		c := quicStatus(t, http.StatusOK)
		assert.NoError(t, c.UpdateMetadata(ctx, "k", &Metadata{ContentType: "text/html"}))
	})

	t.Run("quic_health_success", func(t *testing.T) {
		c := quicServer(t, func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, map[string]any{"status": "serving", "protocol": "quic"})
		})
		st, err := c.Health(ctx)
		require.NoError(t, err)
		assert.Equal(t, "SERVING", st.Status)
	})

	t.Run("quic_archive_success", func(t *testing.T) {
		c := quicStatus(t, http.StatusOK)
		assert.NoError(t, c.Archive(ctx, "k", "glacier", map[string]string{"vault": "v"}))
	})

	t.Run("quic_add_policy_success", func(t *testing.T) {
		c := quicStatus(t, http.StatusCreated)
		assert.NoError(t, c.AddPolicy(ctx, &LifecyclePolicy{ID: "p1", Action: "archive"}))
	})

	t.Run("quic_remove_policy_success", func(t *testing.T) {
		c := quicStatus(t, http.StatusOK)
		assert.NoError(t, c.RemovePolicy(ctx, "p1"))
	})

	t.Run("quic_get_policies_success", func(t *testing.T) {
		c := quicServer(t, func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, map[string]any{
				"policies": []map[string]any{{"id": "p1", "action": "archive"}},
				"count":    1,
			})
		})
		ps, err := c.GetPolicies(ctx, "x/")
		require.NoError(t, err)
		assert.Len(t, ps, 1)
		assert.Equal(t, "p1", ps[0].ID)
	})

	t.Run("quic_apply_policies_success", func(t *testing.T) {
		c := quicServer(t, func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, map[string]any{"policies_count": 2, "objects_processed": 4})
		})
		res, err := c.ApplyPolicies(ctx)
		require.NoError(t, err)
		assert.Equal(t, int32(2), res.PoliciesCount)
	})

	t.Run("quic_add_replication_policy_success", func(t *testing.T) {
		c := quicStatus(t, http.StatusCreated)
		assert.NoError(t, c.AddReplicationPolicy(ctx, &ReplicationPolicy{ID: "r1", SourceBackend: "local", DestinationBackend: "s3"}))
	})

	t.Run("quic_remove_replication_policy_success", func(t *testing.T) {
		c := quicStatus(t, http.StatusOK)
		assert.NoError(t, c.RemoveReplicationPolicy(ctx, "r1"))
	})

	t.Run("quic_get_replication_policies_success", func(t *testing.T) {
		c := quicServer(t, func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, map[string]any{
				"policies": []map[string]any{{"id": "r1", "source_backend": "local", "destination_backend": "s3"}},
				"count":    1,
			})
		})
		ps, err := c.GetReplicationPolicies(ctx)
		require.NoError(t, err)
		assert.Len(t, ps, 1)
		assert.Equal(t, "r1", ps[0].ID)
	})

	t.Run("quic_get_replication_policy_success", func(t *testing.T) {
		c := quicServer(t, func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, map[string]any{"id": "r1", "source_backend": "local", "destination_backend": "s3"})
		})
		p, err := c.GetReplicationPolicy(ctx, "r1")
		require.NoError(t, err)
		assert.Equal(t, "r1", p.ID)
	})

	t.Run("quic_trigger_replication_success", func(t *testing.T) {
		c := quicServer(t, func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, map[string]any{
				"success": true,
				"result":  map[string]any{"policy_id": "r1", "synced": 7, "duration": "2s"},
			})
		})
		res, err := c.TriggerReplication(ctx, &TriggerReplicationOptions{PolicyID: "r1"})
		require.NoError(t, err)
		assert.Equal(t, int32(7), res.Synced)
	})

	t.Run("quic_get_replication_status_success", func(t *testing.T) {
		c := quicServer(t, func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, map[string]any{
				"policy_id": "r1", "source_backend": "local", "destination_backend": "s3",
				"total_objects_synced": 100, "average_sync_duration": "1s",
			})
		})
		st, err := c.GetReplicationStatus(ctx, "r1")
		require.NoError(t, err)
		assert.Equal(t, int64(100), st.TotalObjectsSynced)
	})

	// ------------------------------------------------------------------ error

	t.Run("quic_put_error", func(t *testing.T) {
		_, err := quicStatus(t, http.StatusInternalServerError).Put(ctx, "k", []byte("d"), nil)
		assert.Error(t, err)
	})
	t.Run("quic_get_error", func(t *testing.T) {
		_, err := quicStatus(t, http.StatusInternalServerError).Get(ctx, "k")
		assert.Error(t, err)
	})
	t.Run("quic_delete_error", func(t *testing.T) {
		assert.Error(t, quicStatus(t, http.StatusInternalServerError).Delete(ctx, "k"))
	})
	t.Run("quic_list_error", func(t *testing.T) {
		_, err := quicStatus(t, http.StatusInternalServerError).List(ctx, nil)
		assert.Error(t, err)
	})
	t.Run("quic_exists_error", func(t *testing.T) {
		_, err := quicStatus(t, http.StatusInternalServerError).Exists(ctx, "k")
		assert.Error(t, err)
	})
	t.Run("quic_get_metadata_error", func(t *testing.T) {
		_, err := quicStatus(t, http.StatusInternalServerError).GetMetadata(ctx, "k")
		assert.Error(t, err)
	})
	t.Run("quic_update_metadata_error", func(t *testing.T) {
		assert.Error(t, quicStatus(t, http.StatusInternalServerError).UpdateMetadata(ctx, "k", &Metadata{}))
	})
	t.Run("quic_health_error", func(t *testing.T) {
		_, err := quicStatus(t, http.StatusInternalServerError).Health(ctx)
		assert.Error(t, err)
	})
	t.Run("quic_archive_error", func(t *testing.T) {
		assert.Error(t, quicStatus(t, http.StatusInternalServerError).Archive(ctx, "k", "glacier", nil))
	})
	t.Run("quic_add_policy_error", func(t *testing.T) {
		assert.Error(t, quicStatus(t, http.StatusInternalServerError).AddPolicy(ctx, &LifecyclePolicy{ID: "p1"}))
	})
	t.Run("quic_remove_policy_error", func(t *testing.T) {
		assert.Error(t, quicStatus(t, http.StatusInternalServerError).RemovePolicy(ctx, "p1"))
	})
	t.Run("quic_get_policies_error", func(t *testing.T) {
		_, err := quicStatus(t, http.StatusInternalServerError).GetPolicies(ctx, "")
		assert.Error(t, err)
	})
	t.Run("quic_apply_policies_error", func(t *testing.T) {
		_, err := quicStatus(t, http.StatusInternalServerError).ApplyPolicies(ctx)
		assert.Error(t, err)
	})
	t.Run("quic_add_replication_policy_error", func(t *testing.T) {
		assert.Error(t, quicStatus(t, http.StatusInternalServerError).AddReplicationPolicy(ctx, &ReplicationPolicy{ID: "r1"}))
	})
	t.Run("quic_remove_replication_policy_error", func(t *testing.T) {
		assert.Error(t, quicStatus(t, http.StatusInternalServerError).RemoveReplicationPolicy(ctx, "r1"))
	})
	t.Run("quic_get_replication_policies_error", func(t *testing.T) {
		_, err := quicStatus(t, http.StatusInternalServerError).GetReplicationPolicies(ctx)
		assert.Error(t, err)
	})
	t.Run("quic_get_replication_policy_error", func(t *testing.T) {
		_, err := quicStatus(t, http.StatusInternalServerError).GetReplicationPolicy(ctx, "r1")
		assert.Error(t, err)
	})
	t.Run("quic_trigger_replication_error", func(t *testing.T) {
		_, err := quicStatus(t, http.StatusInternalServerError).TriggerReplication(ctx, &TriggerReplicationOptions{PolicyID: "r1"})
		assert.Error(t, err)
	})
	t.Run("quic_get_replication_status_error", func(t *testing.T) {
		_, err := quicStatus(t, http.StatusInternalServerError).GetReplicationStatus(ctx, "r1")
		assert.Error(t, err)
	})

	// -------------------------------------------------------------- not_found

	t.Run("quic_get_not_found", func(t *testing.T) {
		_, err := quicStatus(t, http.StatusNotFound).Get(ctx, "k")
		assert.ErrorIs(t, err, ErrObjectNotFound)
	})
	t.Run("quic_delete_not_found", func(t *testing.T) {
		assert.ErrorIs(t, quicStatus(t, http.StatusNotFound).Delete(ctx, "k"), ErrObjectNotFound)
	})
	t.Run("quic_exists_not_found", func(t *testing.T) {
		c := quicServer(t, func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, map[string]any{"exists": false})
		})
		ok, err := c.Exists(ctx, "k")
		require.NoError(t, err)
		assert.False(t, ok)
	})
	t.Run("quic_get_metadata_not_found", func(t *testing.T) {
		_, err := quicStatus(t, http.StatusNotFound).GetMetadata(ctx, "k")
		assert.ErrorIs(t, err, ErrObjectNotFound)
	})
	t.Run("quic_update_metadata_not_found", func(t *testing.T) {
		assert.ErrorIs(t, quicStatus(t, http.StatusNotFound).UpdateMetadata(ctx, "k", &Metadata{}), ErrObjectNotFound)
	})
	t.Run("quic_remove_policy_not_found", func(t *testing.T) {
		assert.ErrorIs(t, quicStatus(t, http.StatusNotFound).RemovePolicy(ctx, "p1"), ErrObjectNotFound)
	})
	t.Run("quic_get_replication_policy_not_found", func(t *testing.T) {
		_, err := quicStatus(t, http.StatusNotFound).GetReplicationPolicy(ctx, "r1")
		assert.ErrorIs(t, err, ErrObjectNotFound)
	})
	t.Run("quic_get_replication_status_not_found", func(t *testing.T) {
		_, err := quicStatus(t, http.StatusNotFound).GetReplicationStatus(ctx, "r1")
		assert.ErrorIs(t, err, ErrObjectNotFound)
	})
	t.Run("quic_remove_replication_policy_not_found", func(t *testing.T) {
		assert.ErrorIs(t, quicStatus(t, http.StatusNotFound).RemoveReplicationPolicy(ctx, "r1"), ErrObjectNotFound)
	})

	// --------------------------------------------------------- cross-cutting

	t.Run("quic_metadata_round_trip", func(t *testing.T) {
		// QUIC puts content fields as standard headers and each custom entry
		// as its own X-Meta-<key> request header; metadata is read via HEAD
		// response headers.
		var captured http.Header
		c := quicServer(t, func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPut {
				captured = r.Header.Clone()
				w.WriteHeader(http.StatusOK)
				return
			}
			w.Header().Set("Content-Type", captured.Get("Content-Type"))
			w.Header().Set("Content-Encoding", captured.Get("Content-Encoding"))
			w.Header().Set("X-Meta-author", captured.Get("X-Meta-author"))
			w.WriteHeader(http.StatusOK)
			if r.Method == http.MethodGet {
				_, _ = w.Write([]byte("body"))
			}
		})

		_, err := c.Put(ctx, "k", []byte("body"), &Metadata{
			ContentType:     "text/plain",
			ContentEncoding: "gzip",
			Custom:          map[string]string{"author": "alice"},
		})
		require.NoError(t, err)
		assert.Equal(t, "text/plain", captured.Get("Content-Type"))
		assert.Equal(t, "gzip", captured.Get("Content-Encoding"))
		assert.Equal(t, "alice", captured.Get("X-Meta-author"))

		got, err := c.Get(ctx, "k")
		require.NoError(t, err)
		assert.Equal(t, "text/plain", got.Metadata.ContentType)
		assert.Equal(t, "gzip", got.Metadata.ContentEncoding)
		// X-Meta-author is canonicalized by Go's HTTP stack; the client
		// lowercases the stripped suffix so the key is stored as "author".
		assert.Equal(t, "alice", got.Metadata.Custom["author"])

		md, err := c.GetMetadata(ctx, "k")
		require.NoError(t, err)
		assert.Equal(t, "alice", md.Custom["author"])
	})

	t.Run("quic_validation_empty_key", func(t *testing.T) {
		// The QUIC client validates keys client-side; an empty key must return
		// ErrInvalidKey without making any network request.
		var serverCalled bool
		c := quicServer(t, func(w http.ResponseWriter, r *http.Request) {
			serverCalled = true
			w.WriteHeader(http.StatusNotFound)
		})
		_, err := c.Get(ctx, "")
		assert.ErrorIs(t, err, ErrInvalidKey)
		assert.False(t, serverCalled, "server must not be called for an empty key")
	})
}
