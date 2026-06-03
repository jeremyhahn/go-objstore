// Copyright (c) 2025 Jeremy Hahn
// Copyright (c) 2025 Automate The Things, LLC
//
// This file is part of go-objstore.
//
// go-objstore is dual-licensed under AGPL-3.0 and a Commercial License.

package objstore

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testRPCError lets a test handler return a JSON-RPC error with a specific
// application error code.
type testRPCError struct {
	code int
	msg  string
}

func (e *testRPCError) Error() string { return e.msg }

// unixTestServer is a minimal JSON-RPC 2.0 server listening on a Unix socket
// used only for unit tests.  handler is called for each incoming request and
// must return (result, error).
type unixTestServer struct {
	listener net.Listener
	handler  func(method string, params json.RawMessage) (any, error)
}

func newUnixTestServer(t *testing.T, handler func(method string, params json.RawMessage) (any, error)) (*unixTestServer, string) {
	t.Helper()

	dir := t.TempDir()
	sockPath := filepath.Join(dir, "test.sock")

	ln, err := net.Listen("unix", sockPath)
	require.NoError(t, err)

	srv := &unixTestServer{listener: ln, handler: handler}
	go srv.serve()

	t.Cleanup(func() { ln.Close() })

	return srv, sockPath
}

func (s *unixTestServer) serve() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return // listener closed
		}
		go s.handleConn(conn)
	}
}

func (s *unixTestServer) handleConn(conn net.Conn) {
	defer conn.Close()

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		line := scanner.Bytes()

		var req struct {
			JSONRPC string          `json:"jsonrpc"`
			Method  string          `json:"method"`
			Params  json.RawMessage `json:"params,omitempty"`
			ID      any             `json:"id"`
		}

		if err := json.Unmarshal(line, &req); err != nil {
			continue
		}

		result, rpcErr := s.handler(req.Method, req.Params)

		resp := map[string]any{
			"jsonrpc": "2.0",
			"id":      req.ID,
		}
		if rpcErr != nil {
			code := -32603
			var coded *testRPCError
			if errors.As(rpcErr, &coded) {
				code = coded.code
			}
			resp["error"] = map[string]any{
				"code":    code,
				"message": rpcErr.Error(),
			}
		} else {
			resp["result"] = result
		}

		data, _ := json.Marshal(resp)
		data = append(data, '\n')
		_, _ = conn.Write(data)
	}
}

func newUnixClientForTest(t *testing.T, sockPath string) *UnixClient {
	t.Helper()
	client, err := newUnixClient(&ClientConfig{
		Protocol: ProtocolUnix,
		Address:  sockPath,
	})
	require.NoError(t, err)
	return client
}

// TestUnixClient_Interface verifies compile-time interface satisfaction.
func TestUnixClient_Interface(t *testing.T) {
	var _ Client = (*UnixClient)(nil)
}

// TestUnixClient_NewClient_InvalidConfig checks nil-config guard.
func TestUnixClient_NewClient_InvalidConfig(t *testing.T) {
	_, err := newUnixClient(nil)
	assert.Equal(t, ErrInvalidConfig, err)
}

// TestUnixClient_NewClient_MissingAddress checks empty socket path guard.
func TestUnixClient_NewClient_MissingAddress(t *testing.T) {
	_, err := newUnixClient(&ClientConfig{Protocol: ProtocolUnix, Address: ""})
	assert.ErrorIs(t, err, ErrInvalidConfig)
}

// TestUnixClient_NewClient_DialFailure checks connection failure when socket does not exist.
func TestUnixClient_NewClient_DialFailure(t *testing.T) {
	_, err := newUnixClient(&ClientConfig{
		Protocol: ProtocolUnix,
		Address:  filepath.Join(t.TempDir(), "no-such.sock"),
	})
	assert.ErrorIs(t, err, ErrConnectionFailed)
}

// TestUnixClient_Health exercises the health call.
func TestUnixClient_Health(t *testing.T) {
	_, sockPath := newUnixTestServer(t, func(method string, _ json.RawMessage) (any, error) {
		assert.Equal(t, "health", method)
		return map[string]any{"status": "healthy", "version": "1.0.0"}, nil
	})

	client := newUnixClientForTest(t, sockPath)
	defer client.Close()

	health, err := client.Health(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "healthy", health.Status)
	assert.Equal(t, "1.0.0", health.Message)
}

// TestUnixClient_Put exercises the put call.
func TestUnixClient_Put(t *testing.T) {
	payload := []byte("hello unix")

	_, sockPath := newUnixTestServer(t, func(method string, params json.RawMessage) (any, error) {
		assert.Equal(t, "put", method)

		var p map[string]any
		require.NoError(t, json.Unmarshal(params, &p))
		assert.Equal(t, "test-key", p["key"])

		decoded, err := base64.StdEncoding.DecodeString(p["data"].(string))
		require.NoError(t, err)
		assert.Equal(t, payload, decoded)

		return map[string]any{"success": true}, nil
	})

	client := newUnixClientForTest(t, sockPath)
	defer client.Close()

	result, err := client.Put(context.Background(), "test-key", payload, nil)
	require.NoError(t, err)
	assert.True(t, result.Success)
}

// TestUnixClient_Put_EmptyKey validates key check.
func TestUnixClient_Put_EmptyKey(t *testing.T) {
	_, sockPath := newUnixTestServer(t, func(_ string, _ json.RawMessage) (any, error) {
		return nil, nil
	})
	client := newUnixClientForTest(t, sockPath)
	defer client.Close()

	_, err := client.Put(context.Background(), "", []byte("data"), nil)
	assert.Equal(t, ErrInvalidKey, err)
}

// TestUnixClient_Put_NilData validates data check.
func TestUnixClient_Put_NilData(t *testing.T) {
	_, sockPath := newUnixTestServer(t, func(_ string, _ json.RawMessage) (any, error) {
		return nil, nil
	})
	client := newUnixClientForTest(t, sockPath)
	defer client.Close()

	_, err := client.Put(context.Background(), "k", nil, nil)
	assert.Equal(t, ErrInvalidData, err)
}

// TestUnixClient_Get exercises the get call.
func TestUnixClient_Get(t *testing.T) {
	expected := []byte("retrieved data")

	_, sockPath := newUnixTestServer(t, func(method string, _ json.RawMessage) (any, error) {
		assert.Equal(t, "get", method)
		return map[string]any{
			"data": base64.StdEncoding.EncodeToString(expected),
			"metadata": map[string]any{
				"content_type": "text/plain",
			},
		}, nil
	})

	client := newUnixClientForTest(t, sockPath)
	defer client.Close()

	result, err := client.Get(context.Background(), "my-key")
	require.NoError(t, err)
	assert.Equal(t, expected, result.Data)
	require.NotNil(t, result.Metadata)
	assert.Equal(t, "text/plain", result.Metadata.ContentType)
}

// TestUnixClient_Get_EmptyKey validates key check.
func TestUnixClient_Get_EmptyKey(t *testing.T) {
	_, sockPath := newUnixTestServer(t, func(_ string, _ json.RawMessage) (any, error) {
		return nil, nil
	})
	client := newUnixClientForTest(t, sockPath)
	defer client.Close()

	_, err := client.Get(context.Background(), "")
	assert.Equal(t, ErrInvalidKey, err)
}

// TestUnixClient_Delete exercises the delete call.
func TestUnixClient_Delete(t *testing.T) {
	called := false

	_, sockPath := newUnixTestServer(t, func(method string, params json.RawMessage) (any, error) {
		assert.Equal(t, "delete", method)
		called = true

		var p map[string]string
		require.NoError(t, json.Unmarshal(params, &p))
		assert.Equal(t, "del-key", p["key"])

		return map[string]any{"success": true}, nil
	})

	client := newUnixClientForTest(t, sockPath)
	defer client.Close()

	err := client.Delete(context.Background(), "del-key")
	require.NoError(t, err)
	assert.True(t, called)
}

// TestUnixClient_Delete_EmptyKey validates key check.
func TestUnixClient_Delete_EmptyKey(t *testing.T) {
	_, sockPath := newUnixTestServer(t, func(_ string, _ json.RawMessage) (any, error) {
		return nil, nil
	})
	client := newUnixClientForTest(t, sockPath)
	defer client.Close()

	err := client.Delete(context.Background(), "")
	assert.Equal(t, ErrInvalidKey, err)
}

// TestUnixClient_Exists exercises the exists call.
func TestUnixClient_Exists(t *testing.T) {
	tests := []struct {
		name   string
		exists bool
	}{
		{"found", true},
		{"not found", false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, sockPath := newUnixTestServer(t, func(_ string, _ json.RawMessage) (any, error) {
				return map[string]any{"exists": tc.exists}, nil
			})

			client := newUnixClientForTest(t, sockPath)
			defer client.Close()

			exists, err := client.Exists(context.Background(), "k")
			require.NoError(t, err)
			assert.Equal(t, tc.exists, exists)
		})
	}
}

// TestUnixClient_Exists_EmptyKey validates key check.
func TestUnixClient_Exists_EmptyKey(t *testing.T) {
	_, sockPath := newUnixTestServer(t, func(_ string, _ json.RawMessage) (any, error) {
		return nil, nil
	})
	client := newUnixClientForTest(t, sockPath)
	defer client.Close()

	_, err := client.Exists(context.Background(), "")
	assert.Equal(t, ErrInvalidKey, err)
}

// TestUnixClient_List exercises the list call.
func TestUnixClient_List(t *testing.T) {
	_, sockPath := newUnixTestServer(t, func(method string, _ json.RawMessage) (any, error) {
		assert.Equal(t, "list", method)
		return map[string]any{
			"objects": []map[string]any{
				{"key": "a/1", "size": int64(100), "last_modified": "2025-01-01T00:00:00Z"},
				{"key": "a/2", "size": int64(200), "last_modified": "2025-01-02T00:00:00Z"},
			},
			"next_cursor":  "",
			"is_truncated": false,
		}, nil
	})

	client := newUnixClientForTest(t, sockPath)
	defer client.Close()

	result, err := client.List(context.Background(), &ListOptions{Prefix: "a/"})
	require.NoError(t, err)
	assert.Len(t, result.Objects, 2)
	assert.Equal(t, "a/1", result.Objects[0].Key)
	assert.Equal(t, int64(100), result.Objects[0].Metadata.Size)
	assert.False(t, result.Truncated)
}

// TestUnixClient_GetMetadata exercises the get_metadata call.
func TestUnixClient_GetMetadata(t *testing.T) {
	_, sockPath := newUnixTestServer(t, func(method string, _ json.RawMessage) (any, error) {
		assert.Equal(t, "get_metadata", method)
		return map[string]any{
			"content_type":     "application/json",
			"content_encoding": "gzip",
			"custom":           map[string]any{"env": "prod"},
		}, nil
	})

	client := newUnixClientForTest(t, sockPath)
	defer client.Close()

	meta, err := client.GetMetadata(context.Background(), "obj")
	require.NoError(t, err)
	assert.Equal(t, "application/json", meta.ContentType)
	assert.Equal(t, "gzip", meta.ContentEncoding)
	assert.Equal(t, "prod", meta.Custom["env"])
}

// TestUnixClient_UpdateMetadata exercises the update_metadata call.
func TestUnixClient_UpdateMetadata(t *testing.T) {
	called := false

	_, sockPath := newUnixTestServer(t, func(method string, params json.RawMessage) (any, error) {
		assert.Equal(t, "update_metadata", method)
		called = true

		var p map[string]any
		require.NoError(t, json.Unmarshal(params, &p))
		assert.Equal(t, "obj", p["key"])

		return map[string]any{"success": true}, nil
	})

	client := newUnixClientForTest(t, sockPath)
	defer client.Close()

	err := client.UpdateMetadata(context.Background(), "obj", &Metadata{ContentType: "text/html"})
	require.NoError(t, err)
	assert.True(t, called)
}

// TestUnixClient_UpdateMetadata_NilMetadata validates metadata nil check.
func TestUnixClient_UpdateMetadata_NilMetadata(t *testing.T) {
	_, sockPath := newUnixTestServer(t, func(_ string, _ json.RawMessage) (any, error) {
		return nil, nil
	})
	client := newUnixClientForTest(t, sockPath)
	defer client.Close()

	err := client.UpdateMetadata(context.Background(), "k", nil)
	assert.Equal(t, ErrInvalidMetadata, err)
}

// TestUnixClient_Archive exercises the archive call.
func TestUnixClient_Archive(t *testing.T) {
	_, sockPath := newUnixTestServer(t, func(method string, _ json.RawMessage) (any, error) {
		assert.Equal(t, "archive", method)
		return map[string]any{"success": true}, nil
	})

	client := newUnixClientForTest(t, sockPath)
	defer client.Close()

	err := client.Archive(context.Background(), "obj", "glacier", map[string]string{"vault": "v1"})
	require.NoError(t, err)
}

// TestUnixClient_AddPolicy exercises add_policy.
func TestUnixClient_AddPolicy(t *testing.T) {
	_, sockPath := newUnixTestServer(t, func(method string, _ json.RawMessage) (any, error) {
		assert.Equal(t, "add_policy", method)
		return map[string]any{"success": true}, nil
	})

	client := newUnixClientForTest(t, sockPath)
	defer client.Close()

	err := client.AddPolicy(context.Background(), &LifecyclePolicy{
		ID:               "p1",
		Action:           "delete",
		RetentionSeconds: 86400,
	})
	require.NoError(t, err)
}

// TestUnixClient_AddPolicy_SendsRetentionSeconds pins the wire shape: the
// exact retention_seconds is sent (server-side it takes precedence) alongside
// after_days rounded down for older servers.
func TestUnixClient_AddPolicy_SendsRetentionSeconds(t *testing.T) {
	_, sockPath := newUnixTestServer(t, func(method string, params json.RawMessage) (any, error) {
		assert.Equal(t, "add_policy", method)
		var p map[string]any
		require.NoError(t, json.Unmarshal(params, &p))
		assert.Equal(t, float64(2*86400), p["retention_seconds"])
		assert.Equal(t, float64(2), p["after_days"])
		return map[string]any{"success": true}, nil
	})

	client := newUnixClientForTest(t, sockPath)
	defer client.Close()

	err := client.AddPolicy(context.Background(), &LifecyclePolicy{
		ID:               "p1",
		Action:           "delete",
		RetentionSeconds: 2 * 86400,
	})
	require.NoError(t, err)
}

// TestUnixClient_AddPolicy_SubDayRetention verifies sub-day retention is sent
// exactly via retention_seconds instead of being rejected or truncated.
func TestUnixClient_AddPolicy_SubDayRetention(t *testing.T) {
	_, sockPath := newUnixTestServer(t, func(method string, params json.RawMessage) (any, error) {
		assert.Equal(t, "add_policy", method)
		var p map[string]any
		require.NoError(t, json.Unmarshal(params, &p))
		assert.Equal(t, float64(3600), p["retention_seconds"])
		assert.Equal(t, float64(0), p["after_days"])
		return map[string]any{"success": true}, nil
	})

	client := newUnixClientForTest(t, sockPath)
	defer client.Close()

	err := client.AddPolicy(context.Background(), &LifecyclePolicy{
		ID:               "p1",
		Action:           "delete",
		RetentionSeconds: 3600,
	})
	require.NoError(t, err)
}

// TestUnixClient_RemovePolicy exercises remove_policy.
func TestUnixClient_RemovePolicy(t *testing.T) {
	_, sockPath := newUnixTestServer(t, func(method string, _ json.RawMessage) (any, error) {
		assert.Equal(t, "remove_policy", method)
		return map[string]any{"success": true}, nil
	})

	client := newUnixClientForTest(t, sockPath)
	defer client.Close()

	err := client.RemovePolicy(context.Background(), "p1")
	require.NoError(t, err)
}

// TestUnixClient_RemovePolicy_EmptyID validates policy ID check.
func TestUnixClient_RemovePolicy_EmptyID(t *testing.T) {
	_, sockPath := newUnixTestServer(t, func(_ string, _ json.RawMessage) (any, error) {
		return nil, nil
	})
	client := newUnixClientForTest(t, sockPath)
	defer client.Close()

	err := client.RemovePolicy(context.Background(), "")
	assert.Equal(t, ErrInvalidPolicyID, err)
}

// TestUnixClient_GetPolicies exercises get_policies, pinning the wire shape
// (the unix server returns a BARE JSON array, not an enclosing object) and
// the retention round trip: retention_seconds is preferred when present,
// after_days*86400 is the fallback for older servers.
func TestUnixClient_GetPolicies(t *testing.T) {
	_, sockPath := newUnixTestServer(t, func(method string, _ json.RawMessage) (any, error) {
		assert.Equal(t, "get_policies", method)
		return []map[string]any{
			{"id": "p1", "prefix": "logs/", "action": "delete", "after_days": float64(30)},
			{"id": "p2", "prefix": "tmp/", "action": "delete", "after_days": float64(1), "retention_seconds": float64(90000)},
		}, nil
	})

	client := newUnixClientForTest(t, sockPath)
	defer client.Close()

	policies, err := client.GetPolicies(context.Background(), "")
	require.NoError(t, err)
	require.Len(t, policies, 2)
	assert.Equal(t, "p1", policies[0].ID)
	assert.Equal(t, int64(30*86400), policies[0].RetentionSeconds, "after_days fallback")
	assert.Equal(t, "p2", policies[1].ID)
	assert.Equal(t, int64(90000), policies[1].RetentionSeconds, "exact retention_seconds preferred over after_days")
}

// TestUnixClient_GetPolicies_PropagatesError verifies an RPC failure is
// returned to the caller rather than being swallowed as an empty result.
func TestUnixClient_GetPolicies_PropagatesError(t *testing.T) {
	_, sockPath := newUnixTestServer(t, func(method string, _ json.RawMessage) (any, error) {
		return nil, fmt.Errorf("backend unavailable")
	})

	client := newUnixClientForTest(t, sockPath)
	defer client.Close()

	policies, err := client.GetPolicies(context.Background(), "")
	require.Error(t, err)
	assert.Nil(t, policies)
}

// TestUnixClient_ApplyPolicies exercises apply_policies.
func TestUnixClient_ApplyPolicies(t *testing.T) {
	_, sockPath := newUnixTestServer(t, func(method string, _ json.RawMessage) (any, error) {
		assert.Equal(t, "apply_policies", method)
		return map[string]any{
			"policies_count":    2,
			"objects_processed": 5,
		}, nil
	})

	client := newUnixClientForTest(t, sockPath)
	defer client.Close()

	result, err := client.ApplyPolicies(context.Background())
	require.NoError(t, err)
	assert.True(t, result.Success)
	assert.Equal(t, int32(2), result.PoliciesCount)
	assert.Equal(t, int32(5), result.ObjectsProcessed)
}

// TestUnixClient_AddReplicationPolicy exercises add_replication_policy.
func TestUnixClient_AddReplicationPolicy(t *testing.T) {
	_, sockPath := newUnixTestServer(t, func(method string, _ json.RawMessage) (any, error) {
		assert.Equal(t, "add_replication_policy", method)
		return map[string]any{"success": true}, nil
	})

	client := newUnixClientForTest(t, sockPath)
	defer client.Close()

	err := client.AddReplicationPolicy(context.Background(), &ReplicationPolicy{
		ID:                 "rp1",
		DestinationBackend: "s3",
		Enabled:            true,
	})
	require.NoError(t, err)
}

// TestUnixClient_RemoveReplicationPolicy exercises remove_replication_policy.
func TestUnixClient_RemoveReplicationPolicy(t *testing.T) {
	_, sockPath := newUnixTestServer(t, func(method string, _ json.RawMessage) (any, error) {
		assert.Equal(t, "remove_replication_policy", method)
		return map[string]any{"success": true}, nil
	})

	client := newUnixClientForTest(t, sockPath)
	defer client.Close()

	err := client.RemoveReplicationPolicy(context.Background(), "rp1")
	require.NoError(t, err)
}

// TestUnixClient_GetReplicationPolicies exercises get_replication_policies.
func TestUnixClient_GetReplicationPolicies(t *testing.T) {
	_, sockPath := newUnixTestServer(t, func(method string, _ json.RawMessage) (any, error) {
		assert.Equal(t, "get_replication_policies", method)
		return []map[string]any{
			{
				"id":               "rp1",
				"destination_type": "s3",
				"enabled":          true,
				"replication_mode": "transparent",
			},
		}, nil
	})

	client := newUnixClientForTest(t, sockPath)
	defer client.Close()

	policies, err := client.GetReplicationPolicies(context.Background())
	require.NoError(t, err)
	require.Len(t, policies, 1)
	assert.Equal(t, "rp1", policies[0].ID)
	assert.True(t, policies[0].Enabled)
}

// TestUnixClient_GetReplicationPolicy exercises get_replication_policy.
func TestUnixClient_GetReplicationPolicy(t *testing.T) {
	_, sockPath := newUnixTestServer(t, func(method string, _ json.RawMessage) (any, error) {
		assert.Equal(t, "get_replication_policy", method)
		return map[string]any{
			"id":               "rp1",
			"destination_type": "s3",
			"enabled":          true,
			"replication_mode": "opaque",
		}, nil
	})

	client := newUnixClientForTest(t, sockPath)
	defer client.Close()

	policy, err := client.GetReplicationPolicy(context.Background(), "rp1")
	require.NoError(t, err)
	assert.Equal(t, "rp1", policy.ID)
	assert.Equal(t, ReplicationModeOpaque, policy.ReplicationMode)
}

// TestUnixClient_TriggerReplication exercises trigger_replication and pins the
// wire contract: the policy is identified by "id" (not "policy_id"), matching
// the server's ReplicationPolicyIDParams.
func TestUnixClient_TriggerReplication(t *testing.T) {
	_, sockPath := newUnixTestServer(t, func(method string, params json.RawMessage) (any, error) {
		assert.Equal(t, "trigger_replication", method)
		var p map[string]any
		require.NoError(t, json.Unmarshal(params, &p))
		assert.Equal(t, "rp1", p["id"], "trigger_replication must send the policy id as %q", "id")
		assert.NotContains(t, p, "policy_id")
		return map[string]any{
			"objects_synced":    10,
			"objects_failed":    1,
			"bytes_transferred": int64(1024),
			"errors":            []string{},
		}, nil
	})

	client := newUnixClientForTest(t, sockPath)
	defer client.Close()

	result, err := client.TriggerReplication(context.Background(), &TriggerReplicationOptions{PolicyID: "rp1"})
	require.NoError(t, err)
	assert.Equal(t, int32(10), result.Synced)
	assert.Equal(t, int32(1), result.Failed)
	assert.Equal(t, int64(1024), result.BytesTotal)
}

// TestUnixClient_GetReplicationStatus exercises get_replication_status.
func TestUnixClient_GetReplicationStatus(t *testing.T) {
	_, sockPath := newUnixTestServer(t, func(method string, _ json.RawMessage) (any, error) {
		assert.Equal(t, "get_replication_status", method)
		return map[string]any{
			"policy_id":       "rp1",
			"status":          "active",
			"objects_synced":  int64(50),
			"objects_pending": 0,
			"objects_failed":  0,
			"last_sync_time":  "2025-01-01T00:00:00Z",
		}, nil
	})

	client := newUnixClientForTest(t, sockPath)
	defer client.Close()

	status, err := client.GetReplicationStatus(context.Background(), "rp1")
	require.NoError(t, err)
	assert.Equal(t, "rp1", status.PolicyID)
	assert.Equal(t, int64(50), status.TotalObjectsSynced)
	assert.False(t, status.LastSyncTime.IsZero())
}

// TestUnixClient_RPCError ensures that a server-side RPC error is surfaced.
func TestUnixClient_RPCError(t *testing.T) {
	_, sockPath := newUnixTestServer(t, func(_ string, _ json.RawMessage) (any, error) {
		return nil, os.ErrPermission
	})

	client := newUnixClientForTest(t, sockPath)
	defer client.Close()

	_, err := client.Health(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "rpc error")
}

// TestUnixClient_RPCError_NotFoundSentinel verifies the JSON-RPC NotFound
// code (-32004) maps to the ErrObjectNotFound sentinel.
func TestUnixClient_RPCError_NotFoundSentinel(t *testing.T) {
	_, sockPath := newUnixTestServer(t, func(_ string, _ json.RawMessage) (any, error) {
		return nil, &testRPCError{code: -32004, msg: "object not found: nope"}
	})

	client := newUnixClientForTest(t, sockPath)
	defer client.Close()

	_, err := client.Get(context.Background(), "nope")
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrObjectNotFound)
	assert.Contains(t, err.Error(), "rpc error -32004")
}

// TestUnixClient_RPCError_SentinelMapping verifies every row of the canonical
// JSON-RPC error-code table maps to the matching SDK sentinel via errors.Is.
func TestUnixClient_RPCError_SentinelMapping(t *testing.T) {
	cases := []struct {
		code     int
		sentinel error
	}{
		{-32602, ErrInvalidArgument},
		{-32002, ErrUnauthenticated},
		{-32001, ErrPermissionDenied},
		{-32004, ErrObjectNotFound},
		{-32005, ErrAlreadyExists},
		{-32029, ErrRateLimited},
	}

	for _, tc := range cases {
		_, sockPath := newUnixTestServer(t, func(_ string, _ json.RawMessage) (any, error) {
			return nil, &testRPCError{code: tc.code, msg: "boom"}
		})

		client := newUnixClientForTest(t, sockPath)

		_, err := client.Get(context.Background(), "k")
		require.Error(t, err, "code %d", tc.code)
		assert.ErrorIs(t, err, tc.sentinel, "code %d", tc.code)
		assert.NoError(t, client.Close())
	}

	// Rate limiting keeps the retryable temporary-failure contract.
	_, sockPath := newUnixTestServer(t, func(_ string, _ json.RawMessage) (any, error) {
		return nil, &testRPCError{code: -32029, msg: "slow down"}
	})
	client := newUnixClientForTest(t, sockPath)
	defer client.Close()

	_, err := client.Get(context.Background(), "k")
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrTemporaryFailure)
}

// TestUnixClient_ResponseIDMismatch verifies a response carrying the wrong ID
// poisons the connection: the call errors and the conn is dropped so the next
// call re-dials.
func TestUnixClient_ResponseIDMismatch(t *testing.T) {
	sockPath := filepath.Join(t.TempDir(), "test.sock")
	ln, err := net.Listen("unix", sockPath)
	require.NoError(t, err)
	defer ln.Close()

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				reader := bufio.NewReader(conn)
				for {
					if _, err := reader.ReadBytes('\n'); err != nil {
						return
					}
					// Always answer with an ID that cannot match.
					_, _ = conn.Write([]byte(`{"jsonrpc":"2.0","result":{},"id":999999}` + "\n"))
				}
			}(conn)
		}
	}()

	client := newUnixClientForTest(t, sockPath)
	defer client.Close()

	_, err = client.Health(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not match request id")

	// The poisoned connection must have been dropped.
	client.mu.Lock()
	assert.Nil(t, client.conn)
	assert.Nil(t, client.reader)
	client.mu.Unlock()
}

// TestUnixClient_ReconnectsAfterConnectionLoss verifies the client re-dials
// lazily after the server closes the connection (e.g. the 30s idle read
// deadline), instead of failing every subsequent call.
func TestUnixClient_ReconnectsAfterConnectionLoss(t *testing.T) {
	sockPath := filepath.Join(t.TempDir(), "test.sock")
	ln, err := net.Listen("unix", sockPath)
	require.NoError(t, err)
	defer ln.Close()

	var connCount atomic.Int64
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			if connCount.Add(1) == 1 {
				// Simulate the server's idle timeout: close without responding.
				conn.Close()
				continue
			}
			go func(conn net.Conn) {
				defer conn.Close()
				reader := bufio.NewReader(conn)
				for {
					line, err := reader.ReadBytes('\n')
					if err != nil {
						return
					}
					var req struct {
						ID any `json:"id"`
					}
					_ = json.Unmarshal(line, &req)
					resp, _ := json.Marshal(map[string]any{
						"jsonrpc": "2.0",
						"result":  map[string]any{"status": "healthy", "version": "1.0.0"},
						"id":      req.ID,
					})
					_, _ = conn.Write(append(resp, '\n'))
				}
			}(conn)
		}
	}()

	client := newUnixClientForTest(t, sockPath)
	defer client.Close()

	// First call fails: the server closed the connection.
	_, err = client.Health(context.Background())
	require.Error(t, err)

	// The next call must transparently re-dial and succeed.
	health, err := client.Health(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "healthy", health.Status)
	assert.GreaterOrEqual(t, connCount.Load(), int64(2), "client must have re-dialled")
}

// TestUnixClient_Close verifies Close is idempotent and that a closed client
// rejects further calls instead of re-dialling.
func TestUnixClient_Close(t *testing.T) {
	_, sockPath := newUnixTestServer(t, func(_ string, _ json.RawMessage) (any, error) {
		return nil, nil
	})
	client := newUnixClientForTest(t, sockPath)
	assert.NoError(t, client.Close())
	assert.NoError(t, client.Close())

	_, err := client.Health(context.Background())
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrConnectionFailed)
}

// TestNewClient_Unix verifies the factory dispatches to UnixClient.
func TestNewClient_Unix(t *testing.T) {
	_, sockPath := newUnixTestServer(t, func(_ string, _ json.RawMessage) (any, error) {
		return nil, nil
	})

	client, err := NewClient(&ClientConfig{
		Protocol: ProtocolUnix,
		Address:  sockPath,
	})
	require.NoError(t, err)
	assert.NotNil(t, client)
	_, ok := client.(*UnixClient)
	assert.True(t, ok)
	client.Close()
}
