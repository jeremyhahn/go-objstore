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
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

func TestQUICClient_Put(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Errorf("expected PUT, got %s", r.Method)
		}
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	client, err := NewQUICClient(&Config{ServerURL: server.URL})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	err = client.Put(context.Background(), "test.txt", strings.NewReader("hello"), nil)
	if err != nil {
		t.Errorf("Put failed: %v", err)
	}
}

func TestQUICClient_PutWithMetadata(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if ct := r.Header.Get("Content-Type"); ct != "text/plain" {
			t.Errorf("expected Content-Type text/plain, got %s", ct)
		}
		if ce := r.Header.Get("Content-Encoding"); ce != "gzip" {
			t.Errorf("expected Content-Encoding gzip, got %s", ce)
		}
		if custom := r.Header.Get("X-Custom-Author"); custom != "test" {
			t.Errorf("expected X-Custom-Author test, got %s", custom)
		}
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	client, err := NewQUICClient(&Config{ServerURL: server.URL})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	metadata := &common.Metadata{
		ContentType:     "text/plain",
		ContentEncoding: "gzip",
		Custom:          map[string]string{"author": "test"},
	}

	err = client.Put(context.Background(), "test.txt", strings.NewReader("hello"), metadata)
	if err != nil {
		t.Errorf("Put with metadata failed: %v", err)
	}
}

func TestQUICClient_Get(t *testing.T) {
	content := "hello world"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("ETag", "abc123")
		w.Header().Set("X-Custom-Version", "1.0")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(content))
	}))
	defer server.Close()

	client, err := NewQUICClient(&Config{ServerURL: server.URL})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	reader, metadata, err := client.Get(context.Background(), "test.txt")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("failed to read: %v", err)
	}

	if string(data) != content {
		t.Errorf("expected %q, got %q", content, string(data))
	}

	if metadata.ContentType != "text/plain" {
		t.Errorf("expected text/plain, got %s", metadata.ContentType)
	}

	if metadata.ETag != "abc123" {
		t.Errorf("expected abc123, got %s", metadata.ETag)
	}

	if metadata.Custom["Version"] != "1.0" {
		t.Errorf("expected version 1.0, got %s", metadata.Custom["Version"])
	}
}

func TestQUICClient_Delete(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("expected DELETE, got %s", r.Method)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client, err := NewQUICClient(&Config{ServerURL: server.URL})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	err = client.Delete(context.Background(), "test.txt")
	if err != nil {
		t.Errorf("Delete failed: %v", err)
	}
}

func TestQUICClient_Exists(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			t.Errorf("expected HEAD, got %s", r.Method)
		}
		if strings.HasSuffix(r.URL.Path, "exists.txt") {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	client, err := NewQUICClient(&Config{ServerURL: server.URL})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	exists, err := client.Exists(context.Background(), "exists.txt")
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if !exists {
		t.Error("expected file to exist")
	}

	exists, err = client.Exists(context.Background(), "missing.txt")
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if exists {
		t.Error("expected file not to exist")
	}
}

func TestQUICClient_List(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"objects":[{"key":"file1.txt"}]}`))
	}))
	defer server.Close()

	client, err := NewQUICClient(&Config{ServerURL: server.URL})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	result, err := client.List(context.Background(), &common.ListOptions{})
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}

	if len(result.Objects) != 1 {
		t.Errorf("expected 1 object, got %d", len(result.Objects))
	}
}

func TestQUICClient_List_WithOptions(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("prefix") != "test/" {
			t.Errorf("expected prefix test/, got %s", r.URL.Query().Get("prefix"))
		}
		if r.URL.Query().Get("delimiter") != "/" {
			t.Errorf("expected delimiter /, got %s", r.URL.Query().Get("delimiter"))
		}
		if r.URL.Query().Get("max_results") != "50" {
			t.Errorf("expected max_results 50, got %s", r.URL.Query().Get("max_results"))
		}
		if r.URL.Query().Get("continue_from") != "token456" {
			t.Errorf("expected continue_from token456, got %s", r.URL.Query().Get("continue_from"))
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"Objects":[],"CommonPrefixes":["test/sub/"],"NextToken":"next","Truncated":true}`))
	}))
	defer server.Close()

	client, _ := NewQUICClient(&Config{ServerURL: server.URL})
	result, err := client.List(context.Background(), &common.ListOptions{
		Prefix:       "test/",
		Delimiter:    "/",
		MaxResults:   50,
		ContinueFrom: "token456",
	})
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}

	if len(result.CommonPrefixes) != 1 {
		t.Errorf("expected 1 common prefix, got %d", len(result.CommonPrefixes))
	}

	if !result.Truncated {
		t.Error("expected truncated to be true")
	}

	if result.NextToken != "next" {
		t.Errorf("expected next token 'next', got %s", result.NextToken)
	}
}

func TestQUICClient_GetMetadata(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("Content-Length", "50")
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Set("ETag", "xyz789")
		w.Header().Set("X-Custom-Author", "alice")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client, err := NewQUICClient(&Config{ServerURL: server.URL})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	metadata, err := client.GetMetadata(context.Background(), "test.txt")
	if err != nil {
		t.Fatalf("GetMetadata failed: %v", err)
	}

	if metadata.Size != 50 {
		t.Errorf("expected size 50, got %d", metadata.Size)
	}

	if metadata.ContentType != "text/plain" {
		t.Errorf("expected text/plain, got %s", metadata.ContentType)
	}

	if metadata.ContentEncoding != "gzip" {
		t.Errorf("expected gzip, got %s", metadata.ContentEncoding)
	}

	if metadata.ETag != "xyz789" {
		t.Errorf("expected xyz789, got %s", metadata.ETag)
	}

	if metadata.Custom["Author"] != "alice" {
		t.Errorf("expected alice, got %s", metadata.Custom["Author"])
	}
}

func TestQUICClient_UpdateMetadata(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPatch {
			t.Errorf("expected PATCH, got %s", r.Method)
		}
		if ct := r.Header.Get("Content-Type"); ct != "text/plain" {
			t.Errorf("expected Content-Type text/plain, got %s", ct)
		}
		if ce := r.Header.Get("Content-Encoding"); ce != "gzip" {
			t.Errorf("expected Content-Encoding gzip, got %s", ce)
		}
		if custom := r.Header.Get("X-Custom-Version"); custom != "2.0" {
			t.Errorf("expected X-Custom-Version 2.0, got %s", custom)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client, err := NewQUICClient(&Config{ServerURL: server.URL})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	metadata := &common.Metadata{
		ContentType:     "text/plain",
		ContentEncoding: "gzip",
		Custom:          map[string]string{"version": "2.0"},
	}
	err = client.UpdateMetadata(context.Background(), "test.txt", metadata)
	if err != nil {
		t.Errorf("UpdateMetadata failed: %v", err)
	}
}

func TestQUICClient_Archive(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if !strings.Contains(r.URL.Path, "/archive") {
			t.Errorf("expected /archive in path, got %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client, err := NewQUICClient(&Config{ServerURL: server.URL})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	err = client.Archive(context.Background(), "test.txt", "glacier", map[string]string{"vault": "test-vault", "tier": "expedited"})
	if err != nil {
		t.Errorf("Archive failed: %v", err)
	}
}

func TestQUICClient_Policies(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "apply"):
			w.Write([]byte(`{"policies_count":1,"objects_processed":3}`))
		case r.Method == http.MethodPost:
			w.WriteHeader(http.StatusCreated)
		case r.Method == http.MethodGet:
			w.Write([]byte(`{"policies":[{"id":"test","prefix":"tmp/","retention_seconds":3600,"action":"delete"}]}`))
		case r.Method == http.MethodDelete:
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	defer server.Close()

	client, err := NewQUICClient(&Config{ServerURL: server.URL})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	// Test AddPolicy
	policy := common.LifecyclePolicy{
		ID:        "test",
		Prefix:    "tmp/",
		Retention: time.Hour,
		Action:    "delete",
	}
	err = client.AddPolicy(context.Background(), policy)
	if err != nil {
		t.Errorf("AddPolicy failed: %v", err)
	}

	// Test AddPolicy with longer retention
	policy2 := common.LifecyclePolicy{
		ID:        "test2",
		Prefix:    "archive/",
		Retention: 30 * 24 * time.Hour,
		Action:    "archive",
	}
	err = client.AddPolicy(context.Background(), policy2)
	if err != nil {
		t.Errorf("AddPolicy2 failed: %v", err)
	}

	// Test GetPolicies
	policies, err := client.GetPolicies(context.Background())
	if err != nil {
		t.Fatalf("GetPolicies failed: %v", err)
	}
	if len(policies) != 1 {
		t.Errorf("expected 1 policy, got %d", len(policies))
	}

	// Test ApplyPolicies
	count, processed, err := client.ApplyPolicies(context.Background())
	if err != nil {
		t.Errorf("ApplyPolicies failed: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 policy, got %d", count)
	}
	if processed != 3 {
		t.Errorf("expected 3 processed, got %d", processed)
	}

	// Test RemovePolicy
	err = client.RemovePolicy(context.Background(), "test")
	if err != nil {
		t.Errorf("RemovePolicy failed: %v", err)
	}
}

func TestQUICClient_Health(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client, err := NewQUICClient(&Config{ServerURL: server.URL})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	err = client.Health(context.Background())
	if err != nil {
		t.Errorf("Health failed: %v", err)
	}
}

func TestQUICClient_Close(t *testing.T) {
	client, err := NewQUICClient(&Config{ServerURL: "http://localhost:4433"})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	err = client.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}
}

func TestNewQUICClient_InvalidURL(t *testing.T) {
	_, err := NewQUICClient(&Config{ServerURL: ""})
	if err == nil {
		t.Error("expected error with empty URL")
	}
}

// Additional error case tests

func TestQUICClient_Put_Error(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	client, _ := NewQUICClient(&Config{ServerURL: server.URL})
	err := client.Put(context.Background(), "test.txt", strings.NewReader("data"), nil)
	if err == nil {
		t.Error("expected error on server failure")
	}
}

func TestQUICClient_Get_Error(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	client, _ := NewQUICClient(&Config{ServerURL: server.URL})
	_, _, err := client.Get(context.Background(), "missing.txt")
	if err == nil {
		t.Error("expected error for missing file")
	}
}

func TestQUICClient_Delete_Error(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	client, _ := NewQUICClient(&Config{ServerURL: server.URL})
	err := client.Delete(context.Background(), "test.txt")
	if err == nil {
		t.Error("expected error on server failure")
	}
}

func TestQUICClient_List_Error(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	client, _ := NewQUICClient(&Config{ServerURL: server.URL})
	_, err := client.List(context.Background(), &common.ListOptions{})
	if err == nil {
		t.Error("expected error on server failure")
	}
}

func TestQUICClient_List_InvalidJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("not json"))
	}))
	defer server.Close()

	client, _ := NewQUICClient(&Config{ServerURL: server.URL})
	_, err := client.List(context.Background(), &common.ListOptions{})
	if err == nil {
		t.Error("expected error on invalid JSON")
	}
}

func TestQUICClient_GetMetadata_Error(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	client, _ := NewQUICClient(&Config{ServerURL: server.URL})
	_, err := client.GetMetadata(context.Background(), "missing.txt")
	if err == nil {
		t.Error("expected error for missing file")
	}
}

func TestQUICClient_UpdateMetadata_Error(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	client, _ := NewQUICClient(&Config{ServerURL: server.URL})
	err := client.UpdateMetadata(context.Background(), "missing.txt", &common.Metadata{})
	if err == nil {
		t.Error("expected error for missing file")
	}
}

func TestQUICClient_Archive_Error(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	client, _ := NewQUICClient(&Config{ServerURL: server.URL})
	err := client.Archive(context.Background(), "test.txt", "glacier", nil)
	if err == nil {
		t.Error("expected error on server failure")
	}
}

func TestQUICClient_AddPolicy_Error(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()

	client, _ := NewQUICClient(&Config{ServerURL: server.URL})
	err := client.AddPolicy(context.Background(), common.LifecyclePolicy{})
	if err == nil {
		t.Error("expected error on bad request")
	}
}

func TestQUICClient_RemovePolicy_Error(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	client, _ := NewQUICClient(&Config{ServerURL: server.URL})
	err := client.RemovePolicy(context.Background(), "missing")
	if err == nil {
		t.Error("expected error for missing policy")
	}
}

func TestQUICClient_GetPolicies_Error(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	client, _ := NewQUICClient(&Config{ServerURL: server.URL})
	_, err := client.GetPolicies(context.Background())
	if err == nil {
		t.Error("expected error on server failure")
	}
}

func TestQUICClient_GetPolicies_InvalidJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("not json"))
	}))
	defer server.Close()

	client, _ := NewQUICClient(&Config{ServerURL: server.URL})
	_, err := client.GetPolicies(context.Background())
	if err == nil {
		t.Error("expected error on invalid JSON")
	}
}

func TestQUICClient_ApplyPolicies_Error(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	client, _ := NewQUICClient(&Config{ServerURL: server.URL})
	_, _, err := client.ApplyPolicies(context.Background())
	if err == nil {
		t.Error("expected error on server failure")
	}
}

func TestQUICClient_ApplyPolicies_InvalidJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("bad"))
	}))
	defer server.Close()

	client, _ := NewQUICClient(&Config{ServerURL: server.URL})
	_, _, err := client.ApplyPolicies(context.Background())
	if err == nil {
		t.Error("expected error on invalid JSON")
	}
}

func TestQUICClient_Health_Error(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	client, _ := NewQUICClient(&Config{ServerURL: server.URL})
	err := client.Health(context.Background())
	if err == nil {
		t.Error("expected error on unhealthy server")
	}
}

func TestQUICClient_List_NilOptions(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify no query parameters when nil options
		if len(r.URL.Query()) > 0 {
			t.Error("expected no query parameters with nil options")
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"Objects":[]}`))
	}))
	defer server.Close()

	client, _ := NewQUICClient(&Config{ServerURL: server.URL})
	_, err := client.List(context.Background(), nil)
	if err != nil {
		t.Errorf("List with nil options failed: %v", err)
	}
}

func TestQUICClient_Delete_WithStatusOK(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK) // Test with OK instead of NoContent
	}))
	defer server.Close()

	client, _ := NewQUICClient(&Config{ServerURL: server.URL})
	err := client.Delete(context.Background(), "test.txt")
	if err != nil {
		t.Errorf("Delete with StatusOK failed: %v", err)
	}
}

func TestRESTClient_Delete_WithStatusOK(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK) // Test with OK instead of NoContent
	}))
	defer server.Close()

	client, _ := NewRESTClient(&Config{ServerURL: server.URL})
	err := client.Delete(context.Background(), "test.txt")
	if err != nil {
		t.Errorf("Delete with StatusOK failed: %v", err)
	}
}

func TestQUICClient_GetPolicies_WithMultiplePolicies(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"policies":[
			{"id":"policy1","prefix":"tmp/","retention_seconds":3600,"action":"delete"},
			{"id":"policy2","prefix":"logs/","retention_seconds":86400,"action":"archive"}
		]}`))
	}))
	defer server.Close()

	client, _ := NewQUICClient(&Config{ServerURL: server.URL})
	policies, err := client.GetPolicies(context.Background())
	if err != nil {
		t.Fatalf("GetPolicies failed: %v", err)
	}

	if len(policies) != 2 {
		t.Errorf("expected 2 policies, got %d", len(policies))
	}

	if policies[0].ID != "policy1" {
		t.Errorf("expected policy1, got %s", policies[0].ID)
	}

	if policies[1].Retention != 24*time.Hour {
		t.Errorf("expected 24h retention, got %v", policies[1].Retention)
	}
}

func TestQUICClient_ApplyPolicies_WithResults(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"policies_count":5,"objects_processed":125}`))
	}))
	defer server.Close()

	client, _ := NewQUICClient(&Config{ServerURL: server.URL})
	count, processed, err := client.ApplyPolicies(context.Background())
	if err != nil {
		t.Fatalf("ApplyPolicies failed: %v", err)
	}

	if count != 5 {
		t.Errorf("expected 5 policies, got %d", count)
	}

	if processed != 125 {
		t.Errorf("expected 125 objects, got %d", processed)
	}
}

func TestQUICClient_RemovePolicy_WithStatusOK(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/policy-to-remove") {
			t.Errorf("expected policy path, got %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client, _ := NewQUICClient(&Config{ServerURL: server.URL})
	err := client.RemovePolicy(context.Background(), "policy-to-remove")
	if err != nil {
		t.Errorf("RemovePolicy failed: %v", err)
	}
}

func TestQUICClient_AddPolicy_WithLongRetention(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	client, _ := NewQUICClient(&Config{ServerURL: server.URL})
	policy := common.LifecyclePolicy{
		ID:        "long-term",
		Prefix:    "archive/",
		Retention: 365 * 24 * time.Hour, // 1 year
		Action:    "delete",
	}
	err := client.AddPolicy(context.Background(), policy)
	if err != nil {
		t.Errorf("AddPolicy failed: %v", err)
	}
}

// QUIC Replication tests
func TestQUICClient_AddReplicationPolicy(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if !strings.Contains(r.URL.Path, "/replication/policies") {
			t.Errorf("expected /replication/policies in path, got %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	client, _ := NewQUICClient(&Config{ServerURL: server.URL})
	policy := common.ReplicationPolicy{
		ID:                  "test-policy",
		SourceBackend:       "local",
		DestinationBackend:  "s3",
		SourceSettings:      map[string]string{"path": "/tmp/src"},
		DestinationSettings: map[string]string{"bucket": "my-bucket"},
		CheckInterval:       time.Hour,
	}

	err := client.AddReplicationPolicy(context.Background(), policy)
	if err != nil {
		t.Errorf("AddReplicationPolicy failed: %v", err)
	}
}

func TestQUICClient_AddReplicationPolicy_Error(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("invalid policy"))
	}))
	defer server.Close()

	client, _ := NewQUICClient(&Config{ServerURL: server.URL})
	policy := common.ReplicationPolicy{ID: "test"}

	err := client.AddReplicationPolicy(context.Background(), policy)
	if err == nil {
		t.Error("expected error on bad request")
	}
}

func TestQUICClient_RemoveReplicationPolicy(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("expected DELETE, got %s", r.Method)
		}
		if !strings.HasSuffix(r.URL.Path, "/test-policy") {
			t.Errorf("expected policy ID in path, got %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client, _ := NewQUICClient(&Config{ServerURL: server.URL})
	err := client.RemoveReplicationPolicy(context.Background(), "test-policy")
	if err != nil {
		t.Errorf("RemoveReplicationPolicy failed: %v", err)
	}
}

func TestQUICClient_RemoveReplicationPolicy_Error(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("policy not found"))
	}))
	defer server.Close()

	client, _ := NewQUICClient(&Config{ServerURL: server.URL})
	err := client.RemoveReplicationPolicy(context.Background(), "missing")
	if err == nil {
		t.Error("expected error for missing policy")
	}
}

func TestQUICClient_GetReplicationPolicy(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"id":"test-policy","source_backend":"local","destination_backend":"s3"}`))
	}))
	defer server.Close()

	client, _ := NewQUICClient(&Config{ServerURL: server.URL})
	policy, err := client.GetReplicationPolicy(context.Background(), "test-policy")
	if err != nil {
		t.Fatalf("GetReplicationPolicy failed: %v", err)
	}
	if policy == nil || policy.ID != "test-policy" {
		t.Errorf("expected policy with ID test-policy, got %v", policy)
	}
}

func TestQUICClient_GetReplicationPolicy_Error(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("policy not found"))
	}))
	defer server.Close()

	client, _ := NewQUICClient(&Config{ServerURL: server.URL})
	_, err := client.GetReplicationPolicy(context.Background(), "missing")
	if err == nil {
		t.Error("expected error for missing policy")
	}
}

func TestQUICClient_GetReplicationPolicy_InvalidJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{invalid json`))
	}))
	defer server.Close()

	client, _ := NewQUICClient(&Config{ServerURL: server.URL})
	_, err := client.GetReplicationPolicy(context.Background(), "test")
	if err == nil {
		t.Error("expected error on invalid JSON")
	}
}

func TestQUICClient_GetReplicationPolicies(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`[
			{"id":"policy1","source_backend":"local","destination_backend":"s3"},
			{"id":"policy2","source_backend":"s3","destination_backend":"local"}
		]`))
	}))
	defer server.Close()

	client, _ := NewQUICClient(&Config{ServerURL: server.URL})
	policies, err := client.GetReplicationPolicies(context.Background())
	if err != nil {
		t.Fatalf("GetReplicationPolicies failed: %v", err)
	}
	if len(policies) != 2 {
		t.Errorf("expected 2 policies, got %d", len(policies))
	}
	if policies[0].ID != "policy1" {
		t.Errorf("expected policy1, got %s", policies[0].ID)
	}
}

func TestQUICClient_GetReplicationPolicies_Error(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("internal error"))
	}))
	defer server.Close()

	client, _ := NewQUICClient(&Config{ServerURL: server.URL})
	_, err := client.GetReplicationPolicies(context.Background())
	if err == nil {
		t.Error("expected error on server failure")
	}
}

func TestQUICClient_GetReplicationPolicies_InvalidJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`[{invalid`))
	}))
	defer server.Close()

	client, _ := NewQUICClient(&Config{ServerURL: server.URL})
	_, err := client.GetReplicationPolicies(context.Background())
	if err == nil {
		t.Error("expected error on invalid JSON")
	}
}

func TestQUICClient_TriggerReplication(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if r.URL.Query().Get("policy_id") != "test-policy" {
			t.Errorf("expected policy_id=test-policy, got %s", r.URL.Query().Get("policy_id"))
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"policy_id":"test-policy","synced":10,"deleted":0,"failed":0}`))
	}))
	defer server.Close()

	client, _ := NewQUICClient(&Config{ServerURL: server.URL})
	result, err := client.TriggerReplication(context.Background(), "test-policy")
	if err != nil {
		t.Fatalf("TriggerReplication failed: %v", err)
	}
	if result == nil || result.PolicyID != "test-policy" {
		t.Errorf("expected result with policy_id=test-policy, got %v", result)
	}
	if result.Synced != 10 {
		t.Errorf("expected 10 synced, got %d", result.Synced)
	}
}

func TestQUICClient_TriggerReplication_EmptyPolicyID(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("policy_id") != "" {
			t.Error("expected empty policy_id")
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"synced":5,"deleted":2,"failed":1}`))
	}))
	defer server.Close()

	client, _ := NewQUICClient(&Config{ServerURL: server.URL})
	result, err := client.TriggerReplication(context.Background(), "")
	if err != nil {
		t.Fatalf("TriggerReplication failed: %v", err)
	}
	if result == nil {
		t.Error("expected non-nil result")
	}
}

func TestQUICClient_TriggerReplication_Error(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("invalid policy"))
	}))
	defer server.Close()

	client, _ := NewQUICClient(&Config{ServerURL: server.URL})
	_, err := client.TriggerReplication(context.Background(), "invalid")
	if err == nil {
		t.Error("expected error on bad request")
	}
}

func TestQUICClient_TriggerReplication_InvalidJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{bad json`))
	}))
	defer server.Close()

	client, _ := NewQUICClient(&Config{ServerURL: server.URL})
	_, err := client.TriggerReplication(context.Background(), "test")
	if err == nil {
		t.Error("expected error on invalid JSON")
	}
}

func TestQUICClient_AddReplicationPolicy_WithStatusOK(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK) // Test with OK instead of Created
	}))
	defer server.Close()

	client, _ := NewQUICClient(&Config{ServerURL: server.URL})
	policy := common.ReplicationPolicy{ID: "test"}

	err := client.AddReplicationPolicy(context.Background(), policy)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestQUICClient_RemoveReplicationPolicy_WithStatusOK(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK) // Test with OK instead of NoContent
	}))
	defer server.Close()

	client, _ := NewQUICClient(&Config{ServerURL: server.URL})
	err := client.RemoveReplicationPolicy(context.Background(), "test-policy")
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

// Additional error path tests with error response bodies
func TestQUICClient_Put_ErrorWithBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("internal server error"))
	}))
	defer server.Close()

	client, _ := NewQUICClient(&Config{ServerURL: server.URL})
	err := client.Put(context.Background(), "test.txt", strings.NewReader("data"), nil)
	if err == nil || !strings.Contains(err.Error(), "internal server error") {
		t.Errorf("expected error with body, got %v", err)
	}
}

func TestQUICClient_Get_ErrorWithBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("not found"))
	}))
	defer server.Close()

	client, _ := NewQUICClient(&Config{ServerURL: server.URL})
	_, _, err := client.Get(context.Background(), "missing.txt")
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Errorf("expected error with body, got %v", err)
	}
}

func TestQUICClient_Delete_ErrorWithBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("delete failed"))
	}))
	defer server.Close()

	client, _ := NewQUICClient(&Config{ServerURL: server.URL})
	err := client.Delete(context.Background(), "test.txt")
	if err == nil || !strings.Contains(err.Error(), "delete failed") {
		t.Errorf("expected error with body, got %v", err)
	}
}

func TestQUICClient_List_ErrorWithBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("list failed"))
	}))
	defer server.Close()

	client, _ := NewQUICClient(&Config{ServerURL: server.URL})
	_, err := client.List(context.Background(), &common.ListOptions{})
	if err == nil || !strings.Contains(err.Error(), "list failed") {
		t.Errorf("expected error with body, got %v", err)
	}
}

func TestQUICClient_UpdateMetadata_ErrorWithBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("file not found"))
	}))
	defer server.Close()

	client, _ := NewQUICClient(&Config{ServerURL: server.URL})
	err := client.UpdateMetadata(context.Background(), "missing.txt", &common.Metadata{})
	if err == nil || !strings.Contains(err.Error(), "file not found") {
		t.Errorf("expected error with body, got %v", err)
	}
}

func TestQUICClient_Archive_ErrorWithBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("archive failed"))
	}))
	defer server.Close()

	client, _ := NewQUICClient(&Config{ServerURL: server.URL})
	err := client.Archive(context.Background(), "test.txt", "glacier", nil)
	if err == nil || !strings.Contains(err.Error(), "archive failed") {
		t.Errorf("expected error with body, got %v", err)
	}
}

func TestQUICClient_AddPolicy_ErrorWithBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("invalid policy"))
	}))
	defer server.Close()

	client, _ := NewQUICClient(&Config{ServerURL: server.URL})
	err := client.AddPolicy(context.Background(), common.LifecyclePolicy{})
	if err == nil || !strings.Contains(err.Error(), "invalid policy") {
		t.Errorf("expected error with body, got %v", err)
	}
}

func TestQUICClient_RemovePolicy_ErrorWithBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("policy not found"))
	}))
	defer server.Close()

	client, _ := NewQUICClient(&Config{ServerURL: server.URL})
	err := client.RemovePolicy(context.Background(), "missing")
	if err == nil || !strings.Contains(err.Error(), "policy not found") {
		t.Errorf("expected error with body, got %v", err)
	}
}

func TestQUICClient_GetPolicies_ErrorWithBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("server error"))
	}))
	defer server.Close()

	client, _ := NewQUICClient(&Config{ServerURL: server.URL})
	_, err := client.GetPolicies(context.Background())
	if err == nil || !strings.Contains(err.Error(), "server error") {
		t.Errorf("expected error with body, got %v", err)
	}
}

func TestQUICClient_ApplyPolicies_ErrorWithBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("apply failed"))
	}))
	defer server.Close()

	client, _ := NewQUICClient(&Config{ServerURL: server.URL})
	_, _, err := client.ApplyPolicies(context.Background())
	if err == nil || !strings.Contains(err.Error(), "apply failed") {
		t.Errorf("expected error with body, got %v", err)
	}
}

func TestQUICClient_Health_ErrorWithBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("service unavailable"))
	}))
	defer server.Close()

	client, _ := NewQUICClient(&Config{ServerURL: server.URL})
	err := client.Health(context.Background())
	if err == nil || !strings.Contains(err.Error(), "service unavailable") {
		t.Errorf("expected error with body, got %v", err)
	}
}

func TestQUICClient_AddReplicationPolicy_ErrorWithBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("bad policy"))
	}))
	defer server.Close()

	client, _ := NewQUICClient(&Config{ServerURL: server.URL})
	policy := common.ReplicationPolicy{ID: "test"}

	err := client.AddReplicationPolicy(context.Background(), policy)
	if err == nil || !strings.Contains(err.Error(), "bad policy") {
		t.Errorf("expected error with body, got %v", err)
	}
}

func TestQUICClient_RemoveReplicationPolicy_ErrorWithBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("not found"))
	}))
	defer server.Close()

	client, _ := NewQUICClient(&Config{ServerURL: server.URL})
	err := client.RemoveReplicationPolicy(context.Background(), "missing")
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Errorf("expected error with body, got %v", err)
	}
}

func TestQUICClient_GetReplicationPolicy_ErrorWithBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("policy not found"))
	}))
	defer server.Close()

	client, _ := NewQUICClient(&Config{ServerURL: server.URL})
	_, err := client.GetReplicationPolicy(context.Background(), "missing")
	if err == nil || !strings.Contains(err.Error(), "policy not found") {
		t.Errorf("expected error with body, got %v", err)
	}
}

func TestQUICClient_GetReplicationPolicies_ErrorWithBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("server error"))
	}))
	defer server.Close()

	client, _ := NewQUICClient(&Config{ServerURL: server.URL})
	_, err := client.GetReplicationPolicies(context.Background())
	if err == nil || !strings.Contains(err.Error(), "server error") {
		t.Errorf("expected error with body, got %v", err)
	}
}

func TestQUICClient_TriggerReplication_ErrorWithBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("trigger failed"))
	}))
	defer server.Close()

	client, _ := NewQUICClient(&Config{ServerURL: server.URL})
	_, err := client.TriggerReplication(context.Background(), "invalid")
	if err == nil || !strings.Contains(err.Error(), "trigger failed") {
		t.Errorf("expected error with body, got %v", err)
	}
}
