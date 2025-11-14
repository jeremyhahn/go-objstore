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

package client

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// Helper functions for tests
func newMockServer(handler http.HandlerFunc) *httptest.Server {
	return httptest.NewServer(handler)
}

func newRESTClient(baseURL string) *RESTClient {
	return &RESTClient{
		baseURL:    baseURL,
		httpClient: &http.Client{},
	}
}

// TestRESTClient_Replication tests all REST replication methods with mock server
func TestRESTClient_Replication(t *testing.T) {
	// REST client replication methods make actual HTTP calls, not stubs
	// We'll test they work with a mock server
	ctx := context.Background()

	policy := common.ReplicationPolicy{
		ID:                  "test-policy",
		SourceBackend:       "local",
		DestinationBackend:  "local",
		SourceSettings:      map[string]string{"path": "/tmp/src"},
		DestinationSettings: map[string]string{"path": "/tmp/dest"},
		CheckInterval:       time.Hour,
	}

	// Test with a server that returns not implemented for all replication endpoints
	server := newMockServer(func(w http.ResponseWriter, r *http.Request) {
		// Return 501 Not Implemented for all replication requests
		w.WriteHeader(http.StatusNotImplemented)
		w.Write([]byte("replication not supported"))
	})
	defer server.Close()

	client := newRESTClient(server.URL)

	// Test AddReplicationPolicy - should get server error
	err := client.AddReplicationPolicy(ctx, policy)
	if err == nil {
		t.Error("Expected error from AddReplicationPolicy, got nil")
	}

	// Test RemoveReplicationPolicy - should get server error
	err = client.RemoveReplicationPolicy(ctx, "test-policy")
	if err == nil {
		t.Error("Expected error from RemoveReplicationPolicy, got nil")
	}

	// Test GetReplicationPolicy - should get server error
	_, err = client.GetReplicationPolicy(ctx, "test-policy")
	if err == nil {
		t.Error("Expected error from GetReplicationPolicy, got nil")
	}

	// Test GetReplicationPolicies - should get server error
	_, err = client.GetReplicationPolicies(ctx)
	if err == nil {
		t.Error("Expected error from GetReplicationPolicies, got nil")
	}

	// Test TriggerReplication - should get server error
	_, err = client.TriggerReplication(ctx, "test-policy")
	if err == nil {
		t.Error("Expected error from TriggerReplication, got nil")
	}
}

// TestRESTClient_AddReplicationPolicySuccess tests successful policy addition
func TestRESTClient_AddReplicationPolicySuccess(t *testing.T) {
	server := newMockServer(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
	})
	defer server.Close()

	client := newRESTClient(server.URL)
	ctx := context.Background()

	policy := common.ReplicationPolicy{
		ID:                  "test-policy",
		SourceBackend:       "local",
		DestinationBackend:  "local",
		SourceSettings:      map[string]string{"path": "/tmp/src"},
		DestinationSettings: map[string]string{"path": "/tmp/dest"},
		CheckInterval:       time.Hour,
	}

	err := client.AddReplicationPolicy(ctx, policy)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
}

// TestRESTClient_RemoveReplicationPolicySuccess tests successful policy removal
func TestRESTClient_RemoveReplicationPolicySuccess(t *testing.T) {
	server := newMockServer(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	defer server.Close()

	client := newRESTClient(server.URL)
	ctx := context.Background()

	err := client.RemoveReplicationPolicy(ctx, "test-policy")
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
}

// TestRESTClient_GetReplicationPolicySuccess tests successful policy retrieval
func TestRESTClient_GetReplicationPolicySuccess(t *testing.T) {
	server := newMockServer(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"id":"test-policy","source_backend":"local","destination_backend":"local"}`))
	})
	defer server.Close()

	client := newRESTClient(server.URL)
	ctx := context.Background()

	policy, err := client.GetReplicationPolicy(ctx, "test-policy")
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if policy == nil || policy.ID != "test-policy" {
		t.Errorf("Expected policy with ID test-policy, got %v", policy)
	}
}

// TestRESTClient_GetReplicationPoliciesSuccess tests successful policies list retrieval
func TestRESTClient_GetReplicationPoliciesSuccess(t *testing.T) {
	server := newMockServer(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`[{"id":"policy1","source_backend":"local","destination_backend":"local"}]`))
	})
	defer server.Close()

	client := newRESTClient(server.URL)
	ctx := context.Background()

	policies, err := client.GetReplicationPolicies(ctx)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if len(policies) != 1 || policies[0].ID != "policy1" {
		t.Errorf("Expected 1 policy with ID policy1, got %v", policies)
	}
}

// TestRESTClient_TriggerReplicationSuccess tests successful replication trigger
func TestRESTClient_TriggerReplicationSuccess(t *testing.T) {
	server := newMockServer(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"policy_id":"test-policy","synced":10,"deleted":0,"failed":0}`))
	})
	defer server.Close()

	client := newRESTClient(server.URL)
	ctx := context.Background()

	result, err := client.TriggerReplication(ctx, "test-policy")
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if result == nil || result.PolicyID != "test-policy" || result.Synced != 10 {
		t.Errorf("Expected result with policy_id=test-policy and synced=10, got %v", result)
	}
}

// TestRESTClient_GetReplicationPolicyInvalidJSON tests JSON parsing error
func TestRESTClient_GetReplicationPolicyInvalidJSON(t *testing.T) {
	server := newMockServer(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{invalid json`))
	})
	defer server.Close()

	client := newRESTClient(server.URL)
	ctx := context.Background()

	_, err := client.GetReplicationPolicy(ctx, "test-policy")
	if err == nil {
		t.Error("Expected error from invalid JSON, got nil")
	}
}

// TestRESTClient_GetReplicationPoliciesInvalidJSON tests JSON parsing error for list
func TestRESTClient_GetReplicationPoliciesInvalidJSON(t *testing.T) {
	server := newMockServer(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`[{invalid json`))
	})
	defer server.Close()

	client := newRESTClient(server.URL)
	ctx := context.Background()

	_, err := client.GetReplicationPolicies(ctx)
	if err == nil {
		t.Error("Expected error from invalid JSON, got nil")
	}
}

// TestRESTClient_TriggerReplicationInvalidJSON tests JSON parsing error for trigger
func TestRESTClient_TriggerReplicationInvalidJSON(t *testing.T) {
	server := newMockServer(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{invalid json`))
	})
	defer server.Close()

	client := newRESTClient(server.URL)
	ctx := context.Background()

	_, err := client.TriggerReplication(ctx, "test-policy")
	if err == nil {
		t.Error("Expected error from invalid JSON, got nil")
	}
}

// TestRESTClient_UpdateMetadataSuccess tests successful metadata update
func TestRESTClient_UpdateMetadataSuccess(t *testing.T) {
	server := newMockServer(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	defer server.Close()

	client := newRESTClient(server.URL)
	ctx := context.Background()

	metadata := &common.Metadata{
		ContentType: "text/plain",
		Custom:      map[string]string{"author": "test"},
	}

	err := client.UpdateMetadata(ctx, "test-key", metadata)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
}

// TestRESTClient_UpdateMetadataError tests metadata update with server error
func TestRESTClient_UpdateMetadataError(t *testing.T) {
	server := newMockServer(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("internal error"))
	})
	defer server.Close()

	client := newRESTClient(server.URL)
	ctx := context.Background()

	metadata := &common.Metadata{
		ContentType: "text/plain",
	}

	err := client.UpdateMetadata(ctx, "test-key", metadata)
	if err == nil {
		t.Error("Expected error from server, got nil")
	}
}

// TestRESTClient_GetMetadataSuccess tests successful metadata retrieval
func TestRESTClient_GetMetadataSuccess(t *testing.T) {
	server := newMockServer(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"content_type":"text/plain","custom":{"author":"test"}}`))
	})
	defer server.Close()

	client := newRESTClient(server.URL)
	ctx := context.Background()

	metadata, err := client.GetMetadata(ctx, "test-key")
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if metadata == nil || metadata.ContentType != "text/plain" {
		t.Errorf("Expected metadata with ContentType text/plain, got %v", metadata)
	}
}

// TestRESTClient_GetMetadataError tests metadata retrieval with server error
func TestRESTClient_GetMetadataError(t *testing.T) {
	server := newMockServer(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("not found"))
	})
	defer server.Close()

	client := newRESTClient(server.URL)
	ctx := context.Background()

	_, err := client.GetMetadata(ctx, "test-key")
	if err == nil {
		t.Error("Expected error from server, got nil")
	}
}

// TestRESTClient_AddPolicySuccess tests successful policy addition
func TestRESTClient_AddPolicySuccess(t *testing.T) {
	server := newMockServer(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
	})
	defer server.Close()

	client := newRESTClient(server.URL)
	ctx := context.Background()

	policy := common.LifecyclePolicy{
		ID:        "policy1",
		Prefix:    "logs/",
		Retention: 24 * time.Hour,
		Action:    "delete",
	}

	err := client.AddPolicy(ctx, policy)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
}

// TestRESTClient_GetPoliciesSuccess tests successful policies retrieval
func TestRESTClient_GetPoliciesSuccess(t *testing.T) {
	server := newMockServer(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`[{"id":"policy1","prefix":"logs/","action":"delete"}]`))
	})
	defer server.Close()

	client := newRESTClient(server.URL)
	ctx := context.Background()

	policies, err := client.GetPolicies(ctx)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if len(policies) != 1 {
		t.Errorf("Expected 1 policy, got %d", len(policies))
	}
}

// TestRESTClient_ApplyPoliciesSuccess tests successful policy application
func TestRESTClient_ApplyPoliciesSuccess(t *testing.T) {
	server := newMockServer(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"policies_count":1,"objects_processed":10}`))
	})
	defer server.Close()

	client := newRESTClient(server.URL)
	ctx := context.Background()

	policiesCount, objectsProcessed, err := client.ApplyPolicies(ctx)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if policiesCount != 1 {
		t.Errorf("Expected 1 policy, got %d", policiesCount)
	}
	if objectsProcessed != 10 {
		t.Errorf("Expected 10 objects, got %d", objectsProcessed)
	}
}

// TestRESTClient_ArchiveSuccess tests successful archive operation
func TestRESTClient_ArchiveSuccess(t *testing.T) {
	server := newMockServer(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	defer server.Close()

	client := newRESTClient(server.URL)
	ctx := context.Background()

	err := client.Archive(ctx, "test-key", "local", map[string]string{"path": "/tmp/archive"})
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
}

// TestRESTClient_ArchiveError tests archive operation with error
func TestRESTClient_ArchiveError(t *testing.T) {
	server := newMockServer(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	defer server.Close()

	client := newRESTClient(server.URL)
	ctx := context.Background()

	err := client.Archive(ctx, "test-key", "local", map[string]string{"path": "/tmp/archive"})
	if err == nil {
		t.Error("Expected error, got nil")
	}
}

// Error path tests for better coverage

// TestRESTClient_AddReplicationPolicyError tests error handling
func TestRESTClient_AddReplicationPolicyError(t *testing.T) {
	server := newMockServer(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("internal error"))
	})
	defer server.Close()

	client := newRESTClient(server.URL)
	ctx := context.Background()

	policy := common.ReplicationPolicy{
		ID:                  "test-policy",
		SourceBackend:       "local",
		DestinationBackend:  "local",
		SourceSettings:      map[string]string{"path": "/tmp/src"},
		DestinationSettings: map[string]string{"path": "/tmp/dest"},
		CheckInterval:       time.Hour,
	}

	err := client.AddReplicationPolicy(ctx, policy)
	if err == nil {
		t.Error("Expected error, got nil")
	}
}

// TestRESTClient_RemoveReplicationPolicyError tests error handling
func TestRESTClient_RemoveReplicationPolicyError(t *testing.T) {
	server := newMockServer(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("policy not found"))
	})
	defer server.Close()

	client := newRESTClient(server.URL)
	ctx := context.Background()

	err := client.RemoveReplicationPolicy(ctx, "nonexistent")
	if err == nil {
		t.Error("Expected error, got nil")
	}
}

// TestRESTClient_GetReplicationPolicyError tests error handling
func TestRESTClient_GetReplicationPolicyError(t *testing.T) {
	server := newMockServer(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("policy not found"))
	})
	defer server.Close()

	client := newRESTClient(server.URL)
	ctx := context.Background()

	_, err := client.GetReplicationPolicy(ctx, "nonexistent")
	if err == nil {
		t.Error("Expected error, got nil")
	}
}

// TestRESTClient_GetReplicationPoliciesError tests error handling
func TestRESTClient_GetReplicationPoliciesError(t *testing.T) {
	server := newMockServer(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("internal error"))
	})
	defer server.Close()

	client := newRESTClient(server.URL)
	ctx := context.Background()

	_, err := client.GetReplicationPolicies(ctx)
	if err == nil {
		t.Error("Expected error, got nil")
	}
}

// TestRESTClient_TriggerReplicationError tests error handling
func TestRESTClient_TriggerReplicationError(t *testing.T) {
	server := newMockServer(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("invalid policy"))
	})
	defer server.Close()

	client := newRESTClient(server.URL)
	ctx := context.Background()

	_, err := client.TriggerReplication(ctx, "invalid-policy")
	if err == nil {
		t.Error("Expected error, got nil")
	}
}

// TestRESTClient_GetPoliciesError tests error handling
func TestRESTClient_GetPoliciesError(t *testing.T) {
	server := newMockServer(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("internal error"))
	})
	defer server.Close()

	client := newRESTClient(server.URL)
	ctx := context.Background()

	_, err := client.GetPolicies(ctx)
	if err == nil {
		t.Error("Expected error, got nil")
	}
}

// TestRESTClient_ApplyPoliciesError tests error handling
func TestRESTClient_ApplyPoliciesError(t *testing.T) {
	server := newMockServer(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("failed to apply policies"))
	})
	defer server.Close()

	client := newRESTClient(server.URL)
	ctx := context.Background()

	_, _, err := client.ApplyPolicies(ctx)
	if err == nil {
		t.Error("Expected error, got nil")
	}
}

// TestRESTClient_AddReplicationPolicyWithStatusOK tests with StatusOK response
func TestRESTClient_AddReplicationPolicyWithStatusOK(t *testing.T) {
	server := newMockServer(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK) // Test with OK instead of Created
	})
	defer server.Close()

	client := newRESTClient(server.URL)
	ctx := context.Background()

	policy := common.ReplicationPolicy{
		ID:                  "test-policy",
		SourceBackend:       "local",
		DestinationBackend:  "local",
		SourceSettings:      map[string]string{"path": "/tmp/src"},
		DestinationSettings: map[string]string{"path": "/tmp/dest"},
		CheckInterval:       time.Hour,
	}

	err := client.AddReplicationPolicy(ctx, policy)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
}

// TestRESTClient_RemoveReplicationPolicyWithStatusNoContent tests with StatusNoContent response
func TestRESTClient_RemoveReplicationPolicyWithStatusNoContent(t *testing.T) {
	server := newMockServer(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	defer server.Close()

	client := newRESTClient(server.URL)
	ctx := context.Background()

	err := client.RemoveReplicationPolicy(ctx, "test-policy")
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
}

// TestRESTClient_ApplyPoliciesInvalidJSON tests JSON parsing error
func TestRESTClient_ApplyPoliciesInvalidJSON(t *testing.T) {
	server := newMockServer(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{invalid`))
	})
	defer server.Close()

	client := newRESTClient(server.URL)
	ctx := context.Background()

	_, _, err := client.ApplyPolicies(ctx)
	if err == nil {
		t.Error("Expected error from invalid JSON, got nil")
	}
}

// TestRESTClient_GetPoliciesInvalidJSON tests JSON parsing error
func TestRESTClient_GetPoliciesInvalidJSON(t *testing.T) {
	server := newMockServer(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`[{invalid`))
	})
	defer server.Close()

	client := newRESTClient(server.URL)
	ctx := context.Background()

	_, err := client.GetPolicies(ctx)
	if err == nil {
		t.Error("Expected error from invalid JSON, got nil")
	}
}

// TestRESTClient_GetMetadataInvalidJSON tests JSON parsing error
func TestRESTClient_GetMetadataInvalidJSON(t *testing.T) {
	server := newMockServer(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{invalid`))
	})
	defer server.Close()

	client := newRESTClient(server.URL)
	ctx := context.Background()

	_, err := client.GetMetadata(ctx, "test-key")
	if err == nil {
		t.Error("Expected error from invalid JSON, got nil")
	}
}

// Additional REST error path tests
func TestRESTClient_Put_ErrorWithBody(t *testing.T) {
	server := newMockServer(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("put failed"))
	})
	defer server.Close()

	client := newRESTClient(server.URL)
	err := client.Put(context.Background(), "test.txt", strings.NewReader("data"), nil)
	if err == nil {
		t.Error("Expected error, got nil")
	}
}

func TestRESTClient_Get_ErrorWithBody(t *testing.T) {
	server := newMockServer(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("not found"))
	})
	defer server.Close()

	client := newRESTClient(server.URL)
	_, _, err := client.Get(context.Background(), "missing.txt")
	if err == nil {
		t.Error("Expected error, got nil")
	}
}

func TestRESTClient_Delete_ErrorWithBody(t *testing.T) {
	server := newMockServer(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("delete failed"))
	})
	defer server.Close()

	client := newRESTClient(server.URL)
	err := client.Delete(context.Background(), "test.txt")
	if err == nil {
		t.Error("Expected error, got nil")
	}
}

func TestRESTClient_List_ErrorWithBody(t *testing.T) {
	server := newMockServer(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("list failed"))
	})
	defer server.Close()

	client := newRESTClient(server.URL)
	_, err := client.List(context.Background(), &common.ListOptions{})
	if err == nil {
		t.Error("Expected error, got nil")
	}
}

func TestRESTClient_UpdateMetadata_ErrorWithBody(t *testing.T) {
	server := newMockServer(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("file not found"))
	})
	defer server.Close()

	client := newRESTClient(server.URL)
	err := client.UpdateMetadata(context.Background(), "missing.txt", &common.Metadata{})
	if err == nil {
		t.Error("Expected error, got nil")
	}
}

func TestRESTClient_Archive_ErrorWithBody(t *testing.T) {
	server := newMockServer(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("archive failed"))
	})
	defer server.Close()

	client := newRESTClient(server.URL)
	err := client.Archive(context.Background(), "test.txt", "glacier", nil)
	if err == nil {
		t.Error("Expected error, got nil")
	}
}

func TestRESTClient_AddPolicy_ErrorWithBody(t *testing.T) {
	server := newMockServer(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("invalid policy"))
	})
	defer server.Close()

	client := newRESTClient(server.URL)
	err := client.AddPolicy(context.Background(), common.LifecyclePolicy{})
	if err == nil {
		t.Error("Expected error, got nil")
	}
}

func TestRESTClient_RemovePolicy_ErrorWithBody(t *testing.T) {
	server := newMockServer(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("policy not found"))
	})
	defer server.Close()

	client := newRESTClient(server.URL)
	err := client.RemovePolicy(context.Background(), "missing")
	if err == nil {
		t.Error("Expected error, got nil")
	}
}

func TestRESTClient_Health_ErrorWithBody(t *testing.T) {
	server := newMockServer(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("service down"))
	})
	defer server.Close()

	client := newRESTClient(server.URL)
	err := client.Health(context.Background())
	if err == nil {
		t.Error("Expected error, got nil")
	}
}

func TestRESTClient_AddReplicationPolicy_ErrorWithBody(t *testing.T) {
	server := newMockServer(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("bad policy"))
	})
	defer server.Close()

	client := newRESTClient(server.URL)
	policy := common.ReplicationPolicy{ID: "test"}

	err := client.AddReplicationPolicy(context.Background(), policy)
	if err == nil {
		t.Error("Expected error, got nil")
	}
}

func TestRESTClient_RemoveReplicationPolicy_ErrorWithBody(t *testing.T) {
	server := newMockServer(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("not found"))
	})
	defer server.Close()

	client := newRESTClient(server.URL)
	err := client.RemoveReplicationPolicy(context.Background(), "missing")
	if err == nil {
		t.Error("Expected error, got nil")
	}
}

func TestRESTClient_GetReplicationPolicy_ErrorWithBody(t *testing.T) {
	server := newMockServer(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("policy not found"))
	})
	defer server.Close()

	client := newRESTClient(server.URL)
	_, err := client.GetReplicationPolicy(context.Background(), "missing")
	if err == nil {
		t.Error("Expected error, got nil")
	}
}

func TestRESTClient_GetReplicationPolicies_ErrorWithBody(t *testing.T) {
	server := newMockServer(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("server error"))
	})
	defer server.Close()

	client := newRESTClient(server.URL)
	_, err := client.GetReplicationPolicies(context.Background())
	if err == nil {
		t.Error("Expected error, got nil")
	}
}

func TestRESTClient_TriggerReplication_ErrorWithBody(t *testing.T) {
	server := newMockServer(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("trigger failed"))
	})
	defer server.Close()

	client := newRESTClient(server.URL)
	_, err := client.TriggerReplication(context.Background(), "invalid")
	if err == nil {
		t.Error("Expected error, got nil")
	}
}
