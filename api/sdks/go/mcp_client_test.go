// Copyright (c) 2025 Jeremy Hahn
// Copyright (c) 2025 Automate The Things, LLC
//
// This file is part of go-objstore.
//
// go-objstore is dual-licensed under AGPL-3.0 and a Commercial License.

package objstore

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mcpTestHandler builds a minimal MCP-compatible httptest handler.  toolResult
// is the JSON string that will be placed in result.content[0].text.  When
// errMsg is non-empty, an error response is returned instead.
func mcpTestHandler(t *testing.T, wantTool string, toolResult string, errMsg string) http.HandlerFunc {
	t.Helper()

	return func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/", r.URL.Path)
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))

		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)

		var req map[string]any
		require.NoError(t, json.Unmarshal(body, &req))

		assert.Equal(t, "2.0", req["jsonrpc"])
		assert.Equal(t, "tools/call", req["method"])

		params, ok := req["params"].(map[string]any)
		require.True(t, ok)

		if wantTool != "" {
			assert.Equal(t, wantTool, params["name"])
		}

		id := req["id"]

		w.Header().Set("Content-Type", "application/json")

		if errMsg != "" {
			json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"error": map[string]any{
					"code":    -32603,
					"message": errMsg,
				},
				"id": id,
			})
			return
		}

		json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"result": map[string]any{
				"content": []map[string]any{
					{"type": "text", "text": toolResult},
				},
			},
			"id": id,
		})
	}
}

func newMCPClientForTest(t *testing.T, serverURL string) *MCPClient {
	t.Helper()

	// Strip scheme because newMCPClient prepends it from UseTLS.
	addr := strings.TrimPrefix(serverURL, "http://")

	client, err := newMCPClient(&ClientConfig{
		Protocol: ProtocolMCP,
		Address:  addr,
	})
	require.NoError(t, err)
	return client
}

// TestMCPClient_Interface verifies compile-time interface satisfaction.
func TestMCPClient_Interface(t *testing.T) {
	var _ Client = (*MCPClient)(nil)
}

// TestMCPClient_NewClient_InvalidConfig checks nil-config guard.
func TestMCPClient_NewClient_InvalidConfig(t *testing.T) {
	_, err := newMCPClient(nil)
	assert.Equal(t, ErrInvalidConfig, err)
}

// TestMCPClient_Health exercises the health tool call.
func TestMCPClient_Health(t *testing.T) {
	result := `{"status":"healthy","version":"1.2.3"}`
	srv := httptest.NewServer(mcpTestHandler(t, "objstore_health", result, ""))
	defer srv.Close()

	client := newMCPClientForTest(t, srv.URL)
	defer client.Close()

	health, err := client.Health(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "healthy", health.Status)
	assert.Equal(t, "1.2.3", health.Message)
}

// TestMCPClient_Put exercises the objstore_put tool call.
func TestMCPClient_Put(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req map[string]any
		json.Unmarshal(body, &req)
		params := req["params"].(map[string]any)
		args := params["arguments"].(map[string]any)

		assert.Equal(t, "objstore_put", params["name"])
		assert.Equal(t, "put-key", args["key"])

		decoded, err := base64.StdEncoding.DecodeString(args["data"].(string))
		require.NoError(t, err)
		assert.Equal(t, []byte("hello mcp"), decoded)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"result": map[string]any{
				"content": []map[string]any{
					{"type": "text", "text": `{"success":true,"key":"put-key","size":9}`},
				},
			},
			"id": req["id"],
		})
	}))
	defer srv.Close()

	client := newMCPClientForTest(t, srv.URL)
	defer client.Close()

	result, err := client.Put(context.Background(), "put-key", []byte("hello mcp"), nil)
	require.NoError(t, err)
	assert.True(t, result.Success)
}

// TestMCPClient_Put_EmptyKey validates key check.
func TestMCPClient_Put_EmptyKey(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer srv.Close()

	client := newMCPClientForTest(t, srv.URL)
	defer client.Close()

	_, err := client.Put(context.Background(), "", []byte("data"), nil)
	assert.Equal(t, ErrInvalidKey, err)
}

// TestMCPClient_Put_NilData validates data nil check.
func TestMCPClient_Put_NilData(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer srv.Close()

	client := newMCPClientForTest(t, srv.URL)
	defer client.Close()

	_, err := client.Put(context.Background(), "k", nil, nil)
	assert.Equal(t, ErrInvalidData, err)
}

// TestMCPClient_Get exercises the objstore_get tool call.
func TestMCPClient_Get(t *testing.T) {
	expected := []byte("some content")
	encoded := base64.StdEncoding.EncodeToString(expected)

	result := `{"success":true,"key":"obj","size":12,"data":"` + encoded + `"}`
	srv := httptest.NewServer(mcpTestHandler(t, "objstore_get", result, ""))
	defer srv.Close()

	client := newMCPClientForTest(t, srv.URL)
	defer client.Close()

	res, err := client.Get(context.Background(), "obj")
	require.NoError(t, err)
	assert.Equal(t, expected, res.Data)
}

// TestMCPClient_Get_BinaryRoundTrip pins the base64 contract for binary data.
func TestMCPClient_Get_BinaryRoundTrip(t *testing.T) {
	expected := []byte{0x00, 0x01, 0xff, 0xfe, 0x80}
	encoded := base64.StdEncoding.EncodeToString(expected)

	result := `{"success":true,"key":"bin","size":5,"data":"` + encoded + `"}`
	srv := httptest.NewServer(mcpTestHandler(t, "objstore_get", result, ""))
	defer srv.Close()

	client := newMCPClientForTest(t, srv.URL)
	defer client.Close()

	res, err := client.Get(context.Background(), "bin")
	require.NoError(t, err)
	assert.Equal(t, expected, res.Data)
}

// TestMCPClient_Get_InvalidBase64Errors verifies a non-base64 payload is a
// hard error, never silently passed through as plain text.
func TestMCPClient_Get_InvalidBase64Errors(t *testing.T) {
	result := `{"success":true,"key":"obj","size":3,"data":"not valid base64!!!"}`
	srv := httptest.NewServer(mcpTestHandler(t, "objstore_get", result, ""))
	defer srv.Close()

	client := newMCPClientForTest(t, srv.URL)
	defer client.Close()

	_, err := client.Get(context.Background(), "obj")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "base64")
}

// TestMCPClient_Get_EmptyKey validates key check.
func TestMCPClient_Get_EmptyKey(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer srv.Close()

	client := newMCPClientForTest(t, srv.URL)
	defer client.Close()

	_, err := client.Get(context.Background(), "")
	assert.Equal(t, ErrInvalidKey, err)
}

// TestMCPClient_Delete exercises the objstore_delete tool call.
func TestMCPClient_Delete(t *testing.T) {
	result := `{"success":true,"key":"del-key","deleted":true}`
	srv := httptest.NewServer(mcpTestHandler(t, "objstore_delete", result, ""))
	defer srv.Close()

	client := newMCPClientForTest(t, srv.URL)
	defer client.Close()

	err := client.Delete(context.Background(), "del-key")
	require.NoError(t, err)
}

// TestMCPClient_Delete_EmptyKey validates key check.
func TestMCPClient_Delete_EmptyKey(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer srv.Close()

	client := newMCPClientForTest(t, srv.URL)
	defer client.Close()

	err := client.Delete(context.Background(), "")
	assert.Equal(t, ErrInvalidKey, err)
}

// TestMCPClient_List exercises the objstore_list tool call.
func TestMCPClient_List(t *testing.T) {
	result := `{"success":true,"prefix":"a/","count":2,"keys":["a/1","a/2"],"truncated":false,"next_token":""}`
	srv := httptest.NewServer(mcpTestHandler(t, "objstore_list", result, ""))
	defer srv.Close()

	client := newMCPClientForTest(t, srv.URL)
	defer client.Close()

	res, err := client.List(context.Background(), &ListOptions{Prefix: "a/"})
	require.NoError(t, err)
	assert.Len(t, res.Objects, 2)
	assert.Equal(t, "a/1", res.Objects[0].Key)
	assert.False(t, res.Truncated)
}

// TestMCPClient_Exists exercises the objstore_exists tool call.
func TestMCPClient_Exists(t *testing.T) {
	tests := []struct {
		name   string
		exists bool
	}{
		{"found", true},
		{"not found", false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			existsJSON := "false"
			if tc.exists {
				existsJSON = "true"
			}
			result := `{"success":true,"key":"k","exists":` + existsJSON + `}`
			srv := httptest.NewServer(mcpTestHandler(t, "objstore_exists", result, ""))
			defer srv.Close()

			client := newMCPClientForTest(t, srv.URL)
			defer client.Close()

			exists, err := client.Exists(context.Background(), "k")
			require.NoError(t, err)
			assert.Equal(t, tc.exists, exists)
		})
	}
}

// TestMCPClient_GetMetadata exercises the objstore_get_metadata tool call.
func TestMCPClient_GetMetadata(t *testing.T) {
	result := `{"success":true,"key":"obj","size":512,"content_type":"application/json","content_encoding":"","last_modified":"2025-01-01T00:00:00Z","etag":"abc123","custom":{"env":"prod"}}`
	srv := httptest.NewServer(mcpTestHandler(t, "objstore_get_metadata", result, ""))
	defer srv.Close()

	client := newMCPClientForTest(t, srv.URL)
	defer client.Close()

	meta, err := client.GetMetadata(context.Background(), "obj")
	require.NoError(t, err)
	assert.Equal(t, "application/json", meta.ContentType)
	assert.Equal(t, int64(512), meta.Size)
	assert.Equal(t, "abc123", meta.ETag)
	assert.Equal(t, "prod", meta.Custom["env"])
	assert.False(t, meta.LastModified.IsZero())
}

// TestMCPClient_UpdateMetadata exercises the objstore_update_metadata tool call.
func TestMCPClient_UpdateMetadata(t *testing.T) {
	result := `{"success":true,"key":"obj","updated":true}`
	srv := httptest.NewServer(mcpTestHandler(t, "objstore_update_metadata", result, ""))
	defer srv.Close()

	client := newMCPClientForTest(t, srv.URL)
	defer client.Close()

	err := client.UpdateMetadata(context.Background(), "obj", &Metadata{ContentType: "text/html"})
	require.NoError(t, err)
}

// TestMCPClient_UpdateMetadata_NilMetadata validates metadata nil check.
func TestMCPClient_UpdateMetadata_NilMetadata(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer srv.Close()

	client := newMCPClientForTest(t, srv.URL)
	defer client.Close()

	err := client.UpdateMetadata(context.Background(), "k", nil)
	assert.Equal(t, ErrInvalidMetadata, err)
}

// TestMCPClient_Archive exercises the objstore_archive tool call.
func TestMCPClient_Archive(t *testing.T) {
	result := `{"success":true,"key":"old.zip","destination":"glacier","archived":true}`
	srv := httptest.NewServer(mcpTestHandler(t, "objstore_archive", result, ""))
	defer srv.Close()

	client := newMCPClientForTest(t, srv.URL)
	defer client.Close()

	err := client.Archive(context.Background(), "old.zip", "glacier", map[string]string{"vault": "v1"})
	require.NoError(t, err)
}

// TestMCPClient_AddPolicy exercises the objstore_add_policy tool call.
func TestMCPClient_AddPolicy(t *testing.T) {
	result := `{"success":true,"id":"p1","added":true}`
	srv := httptest.NewServer(mcpTestHandler(t, "objstore_add_policy", result, ""))
	defer srv.Close()

	client := newMCPClientForTest(t, srv.URL)
	defer client.Close()

	err := client.AddPolicy(context.Background(), &LifecyclePolicy{
		ID:               "p1",
		Action:           "delete",
		RetentionSeconds: 86400,
	})
	require.NoError(t, err)
}

// TestMCPClient_RemovePolicy exercises the objstore_remove_policy tool call.
func TestMCPClient_RemovePolicy(t *testing.T) {
	result := `{"success":true,"id":"p1","removed":true}`
	srv := httptest.NewServer(mcpTestHandler(t, "objstore_remove_policy", result, ""))
	defer srv.Close()

	client := newMCPClientForTest(t, srv.URL)
	defer client.Close()

	err := client.RemovePolicy(context.Background(), "p1")
	require.NoError(t, err)
}

// TestMCPClient_RemovePolicy_EmptyID validates policy ID check.
func TestMCPClient_RemovePolicy_EmptyID(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer srv.Close()

	client := newMCPClientForTest(t, srv.URL)
	defer client.Close()

	err := client.RemovePolicy(context.Background(), "")
	assert.Equal(t, ErrInvalidPolicyID, err)
}

// TestMCPClient_GetPolicies exercises the objstore_get_policies tool call.
func TestMCPClient_GetPolicies(t *testing.T) {
	result := `{"success":true,"policies":[{"id":"p1","prefix":"logs/","retention_seconds":86400,"action":"delete","destination_type":""}],"count":1}`
	srv := httptest.NewServer(mcpTestHandler(t, "objstore_get_policies", result, ""))
	defer srv.Close()

	client := newMCPClientForTest(t, srv.URL)
	defer client.Close()

	policies, err := client.GetPolicies(context.Background(), "")
	require.NoError(t, err)
	require.Len(t, policies, 1)
	assert.Equal(t, "p1", policies[0].ID)
	assert.Equal(t, int64(86400), policies[0].RetentionSeconds)
}

// TestMCPClient_ApplyPolicies exercises the objstore_apply_policies tool call.
func TestMCPClient_ApplyPolicies(t *testing.T) {
	result := `{"success":true,"message":"done","policies_count":2,"objects_processed":7}`
	srv := httptest.NewServer(mcpTestHandler(t, "objstore_apply_policies", result, ""))
	defer srv.Close()

	client := newMCPClientForTest(t, srv.URL)
	defer client.Close()

	res, err := client.ApplyPolicies(context.Background())
	require.NoError(t, err)
	assert.True(t, res.Success)
	assert.Equal(t, int32(2), res.PoliciesCount)
	assert.Equal(t, int32(7), res.ObjectsProcessed)
}

// TestMCPClient_AddReplicationPolicy exercises the objstore_add_replication_policy tool call.
func TestMCPClient_AddReplicationPolicy(t *testing.T) {
	result := `{"success":true,"id":"rp1","message":"added"}`
	srv := httptest.NewServer(mcpTestHandler(t, "objstore_add_replication_policy", result, ""))
	defer srv.Close()

	client := newMCPClientForTest(t, srv.URL)
	defer client.Close()

	err := client.AddReplicationPolicy(context.Background(), &ReplicationPolicy{
		ID:                 "rp1",
		SourceBackend:      "local",
		DestinationBackend: "s3",
		Enabled:            true,
	})
	require.NoError(t, err)
}

// TestMCPClient_RemoveReplicationPolicy exercises the objstore_remove_replication_policy tool call.
func TestMCPClient_RemoveReplicationPolicy(t *testing.T) {
	result := `{"success":true,"id":"rp1","removed":true}`
	srv := httptest.NewServer(mcpTestHandler(t, "objstore_remove_replication_policy", result, ""))
	defer srv.Close()

	client := newMCPClientForTest(t, srv.URL)
	defer client.Close()

	err := client.RemoveReplicationPolicy(context.Background(), "rp1")
	require.NoError(t, err)
}

// TestMCPClient_GetReplicationPolicies exercises the objstore_list_replication_policies tool call.
func TestMCPClient_GetReplicationPolicies(t *testing.T) {
	result := `{"success":true,"policies":[{"id":"rp1","source_backend":"local","destination_backend":"s3","check_interval":3600,"enabled":true,"replication_mode":"transparent"}],"count":1}`
	srv := httptest.NewServer(mcpTestHandler(t, "objstore_list_replication_policies", result, ""))
	defer srv.Close()

	client := newMCPClientForTest(t, srv.URL)
	defer client.Close()

	policies, err := client.GetReplicationPolicies(context.Background())
	require.NoError(t, err)
	require.Len(t, policies, 1)
	assert.Equal(t, "rp1", policies[0].ID)
	assert.Equal(t, "local", policies[0].SourceBackend)
	assert.True(t, policies[0].Enabled)
}

// TestMCPClient_GetReplicationPolicy exercises the objstore_get_replication_policy tool call.
func TestMCPClient_GetReplicationPolicy(t *testing.T) {
	result := `{"id":"rp1","source_backend":"local","destination_backend":"s3","check_interval":3600,"enabled":true,"replication_mode":"opaque"}`
	srv := httptest.NewServer(mcpTestHandler(t, "objstore_get_replication_policy", result, ""))
	defer srv.Close()

	client := newMCPClientForTest(t, srv.URL)
	defer client.Close()

	policy, err := client.GetReplicationPolicy(context.Background(), "rp1")
	require.NoError(t, err)
	assert.Equal(t, "rp1", policy.ID)
	assert.Equal(t, ReplicationModeOpaque, policy.ReplicationMode)
}

// TestMCPClient_TriggerReplication exercises the objstore_trigger_replication tool call.
func TestMCPClient_TriggerReplication(t *testing.T) {
	result := `{"success":true,"result":{"policy_id":"rp1","synced":10,"deleted":0,"failed":1,"bytes_total":2048,"duration":"500ms","errors":[]},"message":"triggered"}`
	srv := httptest.NewServer(mcpTestHandler(t, "objstore_trigger_replication", result, ""))
	defer srv.Close()

	client := newMCPClientForTest(t, srv.URL)
	defer client.Close()

	res, err := client.TriggerReplication(context.Background(), &TriggerReplicationOptions{PolicyID: "rp1"})
	require.NoError(t, err)
	assert.Equal(t, "rp1", res.PolicyID)
	assert.Equal(t, int32(10), res.Synced)
	assert.Equal(t, int32(1), res.Failed)
	assert.Equal(t, int64(2048), res.BytesTotal)
	assert.Equal(t, int64(500), res.DurationMs)
}

// TestMCPClient_GetReplicationStatus exercises the objstore_get_replication_status tool call.
func TestMCPClient_GetReplicationStatus(t *testing.T) {
	result := `{"success":true,"policy_id":"rp1","source_backend":"local","destination_backend":"s3","enabled":true,"total_objects_synced":100,"total_objects_deleted":5,"total_bytes_synced":10240,"total_errors":2,"last_sync_time":"2025-06-01T12:00:00Z","average_sync_duration":"1s","sync_count":10}`
	srv := httptest.NewServer(mcpTestHandler(t, "objstore_get_replication_status", result, ""))
	defer srv.Close()

	client := newMCPClientForTest(t, srv.URL)
	defer client.Close()

	status, err := client.GetReplicationStatus(context.Background(), "rp1")
	require.NoError(t, err)
	assert.Equal(t, "rp1", status.PolicyID)
	assert.Equal(t, int64(100), status.TotalObjectsSynced)
	assert.Equal(t, int64(1000), status.AverageSyncDurationMs)
	assert.False(t, status.LastSyncTime.IsZero())
}

// TestMCPClient_RPCError ensures that a server-side RPC error is surfaced.
func TestMCPClient_RPCError(t *testing.T) {
	srv := httptest.NewServer(mcpTestHandler(t, "", "", "internal error occurred"))
	defer srv.Close()

	client := newMCPClientForTest(t, srv.URL)
	defer client.Close()

	_, err := client.Health(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "mcp error")
}

// TestMCPClient_RPCError_NotFoundSentinel verifies the JSON-RPC NotFound code
// (-32004) maps to the ErrObjectNotFound sentinel.
func TestMCPClient_RPCError_NotFoundSentinel(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req map[string]any
		_ = json.Unmarshal(body, &req)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"error": map[string]any{
				"code":    -32004,
				"message": "object not found: nope",
			},
			"id": req["id"],
		})
	}))
	defer srv.Close()

	client := newMCPClientForTest(t, srv.URL)
	defer client.Close()

	_, err := client.Get(context.Background(), "nope")
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrObjectNotFound)
	assert.Contains(t, err.Error(), "mcp error -32004")
}

// TestMCPClient_RPCError_SentinelMapping verifies every row of the canonical
// JSON-RPC error-code table maps to the matching SDK sentinel via errors.Is.
func TestMCPClient_RPCError_SentinelMapping(t *testing.T) {
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
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			var req map[string]any
			_ = json.Unmarshal(body, &req)

			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"error": map[string]any{
					"code":    tc.code,
					"message": "boom",
				},
				"id": req["id"],
			})
		}))

		client := newMCPClientForTest(t, srv.URL)

		_, err := client.Get(context.Background(), "k")
		require.Error(t, err, "code %d", tc.code)
		assert.ErrorIs(t, err, tc.sentinel, "code %d", tc.code)
		if tc.code == -32029 {
			// Rate limiting keeps the retryable temporary-failure contract.
			assert.ErrorIs(t, err, ErrTemporaryFailure)
		}

		client.Close()
		srv.Close()
	}
}

// TestMCPClient_HTTPStatusSentinelMapping verifies that HTTP-level failures
// (non-2xx responses with no JSON-RPC envelope) map through the canonical
// HTTP status table to the matching SDK sentinel.
func TestMCPClient_HTTPStatusSentinelMapping(t *testing.T) {
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
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(tc.status)
		}))

		client := newMCPClientForTest(t, srv.URL)

		_, err := client.Get(context.Background(), "k")
		require.Error(t, err, "status %d", tc.status)
		assert.ErrorIs(t, err, tc.sentinel, "status %d", tc.status)

		client.Close()
		srv.Close()
	}

	// 5xx stays a plain server error with no sentinel attached.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	client := newMCPClientForTest(t, srv.URL)
	defer client.Close()

	_, err := client.Get(context.Background(), "k")
	require.Error(t, err)
	for _, tc := range cases {
		assert.NotErrorIs(t, err, tc.sentinel)
	}
}

// TestMCPClient_AuthHeaders verifies Token, Headers, and TenantID are forwarded.
func TestMCPClient_AuthHeaders(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "Bearer secret-token", r.Header.Get("Authorization"))
		assert.Equal(t, "tenant-abc", r.Header.Get("X-Tenant-ID"))
		assert.Equal(t, "custom-value", r.Header.Get("X-Custom-Header"))

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"result": map[string]any{
				"content": []map[string]any{
					{"type": "text", "text": `{"status":"healthy","version":"1.0"}`},
				},
			},
			"id": 1,
		})
	}))
	defer srv.Close()

	addr := strings.TrimPrefix(srv.URL, "http://")
	client, err := newMCPClient(&ClientConfig{
		Protocol: ProtocolMCP,
		Address:  addr,
		Token:    "secret-token",
		TenantID: "tenant-abc",
		Headers:  map[string]string{"X-Custom-Header": "custom-value"},
	})
	require.NoError(t, err)
	defer client.Close()

	_, err = client.Health(context.Background())
	require.NoError(t, err)
}

// TestMCPClient_Close verifies Close does not error.
func TestMCPClient_Close(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer srv.Close()

	client := newMCPClientForTest(t, srv.URL)
	assert.NoError(t, client.Close())
}

// TestNewClient_MCP verifies the factory dispatches to MCPClient.
func TestNewClient_MCP(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer srv.Close()

	addr := strings.TrimPrefix(srv.URL, "http://")
	client, err := NewClient(&ClientConfig{
		Protocol: ProtocolMCP,
		Address:  addr,
	})
	require.NoError(t, err)
	assert.NotNil(t, client)
	_, ok := client.(*MCPClient)
	assert.True(t, ok)
	client.Close()
}
