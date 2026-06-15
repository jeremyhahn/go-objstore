// Copyright (c) 2025 Jeremy Hahn
// Copyright (c) 2025 Automate The Things, LLC
//
// This file is part of go-objstore.
//
// go-objstore is dual-licensed under AGPL-3.0 and a Commercial License.

package objstore

// This file implements the canonical SDK unit-test matrix for the REST
// protocol client. Each of the 19 operations gets a success and an error
// case; nine operations additionally get a not_found case; and the protocol
// gets metadata_round_trip and validation_empty_key cross-cutting cases.
//
// The transport is mocked with an httptest server (restServer) that routes by
// method+path so a single handler can serve a whole round-trip.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// restServer starts an httptest server with the given handler and returns a
// RESTClient pointed at it. The server is closed via t.Cleanup.
func restServer(t *testing.T, h http.HandlerFunc) *RESTClient {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	c, err := newRESTClient(&ClientConfig{Protocol: ProtocolREST, Address: srv.Listener.Addr().String()})
	require.NoError(t, err)
	// DisableCompression avoids Go's transparent gzip handling kicking in when
	// a request carries Content-Encoding: gzip against the httptest server,
	// which would otherwise surface as a spurious "unexpected EOF" on decode.
	c.httpClient = &http.Client{Transport: &http.Transport{DisableCompression: true}}
	return c
}

// restStatus returns a RESTClient whose server always replies with code.
func restStatus(t *testing.T, code int) *RESTClient {
	return restServer(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(code) })
}

// writeJSON writes a 200 JSON response.
func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func TestRESTClientCanonical(t *testing.T) {
	ctx := context.Background()

	// ---------------------------------------------------------------- success

	t.Run("rest_put_success", func(t *testing.T) {
		c := restServer(t, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("ETag", "e1")
			w.WriteHeader(http.StatusCreated)
			writeJSON(w, map[string]any{"message": "created"})
		})
		res, err := c.Put(ctx, "k", []byte("d"), &Metadata{ContentType: "text/plain"})
		require.NoError(t, err)
		assert.True(t, res.Success)
		assert.Equal(t, "e1", res.ETag)
	})

	t.Run("rest_get_success", func(t *testing.T) {
		c := restServer(t, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/plain")
			_, _ = w.Write([]byte("hello"))
		})
		res, err := c.Get(ctx, "k")
		require.NoError(t, err)
		assert.Equal(t, []byte("hello"), res.Data)
		assert.Equal(t, "text/plain", res.Metadata.ContentType)
	})

	t.Run("rest_delete_success", func(t *testing.T) {
		// The server returns 204 No Content for DELETE.
		c := restStatus(t, http.StatusNoContent)
		assert.NoError(t, c.Delete(ctx, "k"))
	})

	t.Run("rest_delete_tolerates_200", func(t *testing.T) {
		// Older servers returned 200 + JSON body.
		c := restStatus(t, http.StatusOK)
		assert.NoError(t, c.Delete(ctx, "k"))
	})

	t.Run("rest_list_success", func(t *testing.T) {
		c := restServer(t, func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, map[string]any{
				"objects":   []map[string]any{{"key": "a", "size": 1}, {"key": "b", "size": 2}},
				"truncated": false,
			})
		})
		res, err := c.List(ctx, &ListOptions{Prefix: "x/"})
		require.NoError(t, err)
		assert.Len(t, res.Objects, 2)
		assert.Equal(t, "a", res.Objects[0].Key)
	})

	t.Run("rest_exists_success", func(t *testing.T) {
		c := restStatus(t, http.StatusOK)
		ok, err := c.Exists(ctx, "k")
		require.NoError(t, err)
		assert.True(t, ok)
	})

	t.Run("rest_get_metadata_success", func(t *testing.T) {
		c := restServer(t, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Content-Length", "42")
			w.WriteHeader(http.StatusOK)
		})
		md, err := c.GetMetadata(ctx, "k")
		require.NoError(t, err)
		assert.Equal(t, "application/json", md.ContentType)
		assert.Equal(t, int64(42), md.Size)
	})

	t.Run("rest_update_metadata_success", func(t *testing.T) {
		c := restStatus(t, http.StatusOK)
		assert.NoError(t, c.UpdateMetadata(ctx, "k", &Metadata{ContentType: "text/html"}))
	})

	t.Run("rest_health_success", func(t *testing.T) {
		c := restServer(t, func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, map[string]any{"status": "serving", "version": "1.0"})
		})
		st, err := c.Health(ctx)
		require.NoError(t, err)
		assert.Equal(t, "SERVING", st.Status)
	})

	t.Run("rest_archive_success", func(t *testing.T) {
		c := restStatus(t, http.StatusOK)
		assert.NoError(t, c.Archive(ctx, "k", "glacier", map[string]string{"vault": "v"}))
	})

	t.Run("rest_add_policy_success", func(t *testing.T) {
		c := restStatus(t, http.StatusCreated)
		assert.NoError(t, c.AddPolicy(ctx, &LifecyclePolicy{ID: "p1", Action: "archive"}))
	})

	t.Run("rest_remove_policy_success", func(t *testing.T) {
		c := restStatus(t, http.StatusOK)
		assert.NoError(t, c.RemovePolicy(ctx, "p1"))
	})

	t.Run("rest_get_policies_success", func(t *testing.T) {
		c := restServer(t, func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, map[string]any{
				"policies": []map[string]any{{"id": "p1", "prefix": "x/", "action": "archive"}},
				"count":    1,
			})
		})
		ps, err := c.GetPolicies(ctx, "x/")
		require.NoError(t, err)
		assert.Len(t, ps, 1)
		assert.Equal(t, "p1", ps[0].ID)
	})

	t.Run("rest_apply_policies_success", func(t *testing.T) {
		c := restServer(t, func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, map[string]any{"policies_count": 3, "objects_processed": 9})
		})
		res, err := c.ApplyPolicies(ctx)
		require.NoError(t, err)
		assert.Equal(t, int32(3), res.PoliciesCount)
	})

	t.Run("rest_add_replication_policy_success", func(t *testing.T) {
		c := restStatus(t, http.StatusCreated)
		assert.NoError(t, c.AddReplicationPolicy(ctx, &ReplicationPolicy{ID: "r1", SourceBackend: "local", DestinationBackend: "s3"}))
	})

	t.Run("rest_remove_replication_policy_success", func(t *testing.T) {
		c := restStatus(t, http.StatusOK)
		assert.NoError(t, c.RemoveReplicationPolicy(ctx, "r1"))
	})

	t.Run("rest_get_replication_policies_success", func(t *testing.T) {
		c := restServer(t, func(w http.ResponseWriter, r *http.Request) {
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

	t.Run("rest_get_replication_policy_success", func(t *testing.T) {
		c := restServer(t, func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, map[string]any{"id": "r1", "source_backend": "local", "destination_backend": "s3"})
		})
		p, err := c.GetReplicationPolicy(ctx, "r1")
		require.NoError(t, err)
		assert.Equal(t, "r1", p.ID)
	})

	t.Run("rest_trigger_replication_success", func(t *testing.T) {
		c := restServer(t, func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, map[string]any{
				"success": true,
				"result":  map[string]any{"policy_id": "r1", "synced": 5, "duration": "5s"},
			})
		})
		res, err := c.TriggerReplication(ctx, &TriggerReplicationOptions{PolicyID: "r1"})
		require.NoError(t, err)
		assert.Equal(t, int32(5), res.Synced)
	})

	t.Run("rest_get_replication_status_success", func(t *testing.T) {
		c := restServer(t, func(w http.ResponseWriter, r *http.Request) {
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

	t.Run("rest_put_error", func(t *testing.T) {
		_, err := restStatus(t, http.StatusInternalServerError).Put(ctx, "k", []byte("d"), nil)
		assert.Error(t, err)
	})
	t.Run("rest_get_error", func(t *testing.T) {
		_, err := restStatus(t, http.StatusInternalServerError).Get(ctx, "k")
		assert.Error(t, err)
	})
	t.Run("rest_delete_error", func(t *testing.T) {
		assert.Error(t, restStatus(t, http.StatusInternalServerError).Delete(ctx, "k"))
	})
	t.Run("rest_list_error", func(t *testing.T) {
		_, err := restStatus(t, http.StatusInternalServerError).List(ctx, nil)
		assert.Error(t, err)
	})
	t.Run("rest_exists_error", func(t *testing.T) {
		// Canonical matrix: only 404 -> false; all other non-200 (including 5xx) -> error.
		ok, err := restStatus(t, http.StatusInternalServerError).Exists(ctx, "k")
		require.Error(t, err)
		assert.False(t, ok)
	})
	t.Run("rest_get_metadata_error", func(t *testing.T) {
		_, err := restStatus(t, http.StatusInternalServerError).GetMetadata(ctx, "k")
		assert.Error(t, err)
	})
	t.Run("rest_update_metadata_error", func(t *testing.T) {
		assert.Error(t, restStatus(t, http.StatusInternalServerError).UpdateMetadata(ctx, "k", &Metadata{}))
	})
	t.Run("rest_health_error", func(t *testing.T) {
		// Health returns an error when the body is not valid JSON.
		c := restServer(t, func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("not json"))
		})
		_, err := c.Health(ctx)
		assert.Error(t, err)
	})
	t.Run("rest_archive_error", func(t *testing.T) {
		assert.Error(t, restStatus(t, http.StatusInternalServerError).Archive(ctx, "k", "glacier", nil))
	})
	t.Run("rest_add_policy_error", func(t *testing.T) {
		assert.Error(t, restStatus(t, http.StatusInternalServerError).AddPolicy(ctx, &LifecyclePolicy{ID: "p1"}))
	})
	t.Run("rest_remove_policy_error", func(t *testing.T) {
		assert.Error(t, restStatus(t, http.StatusInternalServerError).RemovePolicy(ctx, "p1"))
	})
	t.Run("rest_get_policies_error", func(t *testing.T) {
		_, err := restStatus(t, http.StatusInternalServerError).GetPolicies(ctx, "")
		assert.Error(t, err)
	})
	t.Run("rest_apply_policies_error", func(t *testing.T) {
		_, err := restStatus(t, http.StatusInternalServerError).ApplyPolicies(ctx)
		assert.Error(t, err)
	})
	t.Run("rest_add_replication_policy_error", func(t *testing.T) {
		assert.Error(t, restStatus(t, http.StatusInternalServerError).AddReplicationPolicy(ctx, &ReplicationPolicy{ID: "r1"}))
	})
	t.Run("rest_remove_replication_policy_error", func(t *testing.T) {
		assert.Error(t, restStatus(t, http.StatusInternalServerError).RemoveReplicationPolicy(ctx, "r1"))
	})
	t.Run("rest_get_replication_policies_error", func(t *testing.T) {
		_, err := restStatus(t, http.StatusInternalServerError).GetReplicationPolicies(ctx)
		assert.Error(t, err)
	})
	t.Run("rest_get_replication_policy_error", func(t *testing.T) {
		_, err := restStatus(t, http.StatusInternalServerError).GetReplicationPolicy(ctx, "r1")
		assert.Error(t, err)
	})
	t.Run("rest_trigger_replication_error", func(t *testing.T) {
		_, err := restStatus(t, http.StatusInternalServerError).TriggerReplication(ctx, &TriggerReplicationOptions{PolicyID: "r1"})
		assert.Error(t, err)
	})
	t.Run("rest_get_replication_status_error", func(t *testing.T) {
		_, err := restStatus(t, http.StatusInternalServerError).GetReplicationStatus(ctx, "r1")
		assert.Error(t, err)
	})

	// -------------------------------------------------------------- not_found

	t.Run("rest_get_not_found", func(t *testing.T) {
		_, err := restStatus(t, http.StatusNotFound).Get(ctx, "k")
		assert.ErrorIs(t, err, ErrObjectNotFound)
	})
	t.Run("rest_delete_not_found", func(t *testing.T) {
		assert.ErrorIs(t, restStatus(t, http.StatusNotFound).Delete(ctx, "k"), ErrObjectNotFound)
	})
	t.Run("rest_exists_not_found", func(t *testing.T) {
		ok, err := restStatus(t, http.StatusNotFound).Exists(ctx, "k")
		require.NoError(t, err)
		assert.False(t, ok)
	})
	t.Run("rest_get_metadata_not_found", func(t *testing.T) {
		_, err := restStatus(t, http.StatusNotFound).GetMetadata(ctx, "k")
		assert.ErrorIs(t, err, ErrObjectNotFound)
	})
	t.Run("rest_update_metadata_not_found", func(t *testing.T) {
		assert.ErrorIs(t, restStatus(t, http.StatusNotFound).UpdateMetadata(ctx, "k", &Metadata{}), ErrObjectNotFound)
	})
	t.Run("rest_remove_policy_not_found", func(t *testing.T) {
		assert.ErrorIs(t, restStatus(t, http.StatusNotFound).RemovePolicy(ctx, "p1"), ErrObjectNotFound)
	})
	t.Run("rest_get_replication_policy_not_found", func(t *testing.T) {
		_, err := restStatus(t, http.StatusNotFound).GetReplicationPolicy(ctx, "r1")
		assert.ErrorIs(t, err, ErrObjectNotFound)
	})
	t.Run("rest_get_replication_status_not_found", func(t *testing.T) {
		_, err := restStatus(t, http.StatusNotFound).GetReplicationStatus(ctx, "r1")
		assert.ErrorIs(t, err, ErrObjectNotFound)
	})
	t.Run("rest_remove_replication_policy_not_found", func(t *testing.T) {
		assert.ErrorIs(t, restStatus(t, http.StatusNotFound).RemoveReplicationPolicy(ctx, "r1"), ErrObjectNotFound)
	})

	// --------------------------------------------------- canonical sentinels

	t.Run("rest_error_sentinel_mapping", func(t *testing.T) {
		// Every row of the canonical HTTP status table must surface as the
		// matching SDK sentinel via errors.Is.
		cases := []struct {
			status   int
			sentinel error
		}{
			{http.StatusBadRequest, ErrInvalidArgument},
			{http.StatusUnauthorized, ErrUnauthenticated},
			{http.StatusForbidden, ErrPermissionDenied},
			{http.StatusNotFound, ErrObjectNotFound},
			{http.StatusConflict, ErrAlreadyExists},
			{http.StatusTooManyRequests, ErrRateLimited},
		}
		for _, tc := range cases {
			_, err := restStatus(t, tc.status).Get(ctx, "k")
			assert.ErrorIs(t, err, tc.sentinel, "GET status %d", tc.status)

			_, err = restStatus(t, tc.status).Put(ctx, "k", []byte("d"), nil)
			assert.ErrorIs(t, err, tc.sentinel, "PUT status %d", tc.status)
		}

		// Rate limiting keeps the retryable temporary-failure contract.
		_, err := restStatus(t, http.StatusTooManyRequests).Get(ctx, "k")
		assert.ErrorIs(t, err, ErrTemporaryFailure)

		// 5xx stays a plain server error with no sentinel attached.
		_, err = restStatus(t, http.StatusInternalServerError).Get(ctx, "k")
		require.Error(t, err)
		for _, tc := range cases {
			assert.NotErrorIs(t, err, tc.sentinel)
		}
	})

	// --------------------------------------------------------- cross-cutting

	t.Run("rest_metadata_round_trip", func(t *testing.T) {
		// REST puts custom metadata as JSON in X-Object-Metadata and content
		// fields as standard headers; reads parse them back the same way.
		var captured http.Header
		c := restServer(t, func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPut {
				captured = r.Header.Clone()
				writeJSON(w, map[string]any{"message": "ok"})
				return
			}
			// GET and HEAD echo the captured headers back.
			w.Header().Set("Content-Type", captured.Get("Content-Type"))
			w.Header().Set("Content-Encoding", captured.Get("Content-Encoding"))
			w.Header().Set("X-Object-Metadata", captured.Get("X-Object-Metadata"))
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
		// X-Object-Metadata must carry the custom map only, as JSON.
		assert.Equal(t, `{"author":"alice"}`, captured.Get("X-Object-Metadata"))
		assert.Equal(t, "text/plain", captured.Get("Content-Type"))
		assert.Equal(t, "gzip", captured.Get("Content-Encoding"))

		got, err := c.Get(ctx, "k")
		require.NoError(t, err)
		assert.Equal(t, "text/plain", got.Metadata.ContentType)
		assert.Equal(t, "gzip", got.Metadata.ContentEncoding)
		assert.Equal(t, "alice", got.Metadata.Custom["author"])

		md, err := c.GetMetadata(ctx, "k")
		require.NoError(t, err)
		assert.Equal(t, "alice", md.Custom["author"])
	})

	t.Run("rest_validation_empty_key", func(t *testing.T) {
		// The REST client validates keys client-side; an empty key must return
		// ErrInvalidKey without making any network request.
		var serverCalled bool
		c := restServer(t, func(w http.ResponseWriter, r *http.Request) {
			serverCalled = true
			w.WriteHeader(http.StatusNotFound)
		})
		_, err := c.Get(ctx, "")
		assert.ErrorIs(t, err, ErrInvalidKey)
		assert.False(t, serverCalled, "server must not be called for an empty key")
	})
}
