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
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

func TestRESTClient_Put(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Errorf("expected PUT, got %s", r.Method)
		}
		if !strings.HasSuffix(r.URL.Path, "/test.txt") {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	client, err := NewRESTClient(&Config{ServerURL: server.URL})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	err = client.Put(context.Background(), "test.txt", strings.NewReader("hello"), nil)
	if err != nil {
		t.Errorf("Put failed: %v", err)
	}
}

func TestRESTClient_PutWithMetadata(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if ct := r.Header.Get("Content-Type"); ct != "text/plain" {
			t.Errorf("expected Content-Type text/plain, got %s", ct)
		}
		if custom := r.Header.Get("X-Custom-Author"); custom != "test" {
			t.Errorf("expected X-Custom-Author test, got %s", custom)
		}
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	client, err := NewRESTClient(&Config{ServerURL: server.URL})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	metadata := &common.Metadata{
		ContentType: "text/plain",
		Custom:      map[string]string{"author": "test"},
	}

	err = client.Put(context.Background(), "test.txt", strings.NewReader("hello"), metadata)
	if err != nil {
		t.Errorf("Put with metadata failed: %v", err)
	}
}

func TestRESTClient_Get(t *testing.T) {
	content := "hello world"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("X-Custom-Version", "1.0")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(content))
	}))
	defer server.Close()

	client, err := NewRESTClient(&Config{ServerURL: server.URL})
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

	if metadata.Custom["Version"] != "1.0" {
		t.Errorf("expected version 1.0, got %s", metadata.Custom["Version"])
	}
}

func TestRESTClient_Delete(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("expected DELETE, got %s", r.Method)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client, err := NewRESTClient(&Config{ServerURL: server.URL})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	err = client.Delete(context.Background(), "test.txt")
	if err != nil {
		t.Errorf("Delete failed: %v", err)
	}
}

func TestRESTClient_Exists(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			t.Errorf("expected HEAD, got %s", r.Method)
		}
		if strings.Contains(r.URL.Path, "exists.txt") {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	client, err := NewRESTClient(&Config{ServerURL: server.URL})
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

func TestRESTClient_List(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		prefix := r.URL.Query().Get("prefix")
		if prefix != "test/" {
			t.Errorf("expected prefix test/, got %s", prefix)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"objects":[{"key":"test/file1.txt"},{"key":"test/file2.txt"}]}`))
	}))
	defer server.Close()

	client, err := NewRESTClient(&Config{ServerURL: server.URL})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	result, err := client.List(context.Background(), &common.ListOptions{Prefix: "test/"})
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}

	if len(result.Objects) != 2 {
		t.Errorf("expected 2 objects, got %d", len(result.Objects))
	}
}

func TestRESTClient_GetMetadata(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"content_type":"text/plain","size":100}`))
	}))
	defer server.Close()

	client, err := NewRESTClient(&Config{ServerURL: server.URL})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	metadata, err := client.GetMetadata(context.Background(), "test.txt")
	if err != nil {
		t.Fatalf("GetMetadata failed: %v", err)
	}

	if metadata.ContentType != "text/plain" {
		t.Errorf("expected text/plain, got %s", metadata.ContentType)
	}

	if metadata.Size != 100 {
		t.Errorf("expected size 100, got %d", metadata.Size)
	}
}

func TestRESTClient_UpdateMetadata(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Errorf("expected PUT, got %s", r.Method)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client, err := NewRESTClient(&Config{ServerURL: server.URL})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	metadata := &common.Metadata{ContentType: "text/plain"}
	err = client.UpdateMetadata(context.Background(), "test.txt", metadata)
	if err != nil {
		t.Errorf("UpdateMetadata failed: %v", err)
	}
}

func TestRESTClient_Archive(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if !strings.Contains(r.URL.Path, "/archive") {
			t.Errorf("expected /archive in path, got %s", r.URL.Path)
		}
		// Read and validate JSON body
		var payload map[string]any
		json.NewDecoder(r.Body).Decode(&payload)
		if payload["key"] != "test.txt" {
			t.Errorf("expected key test.txt, got %v", payload["key"])
		}
		if payload["destination_type"] != "glacier" {
			t.Errorf("expected destination_type glacier, got %v", payload["destination_type"])
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client, err := NewRESTClient(&Config{ServerURL: server.URL})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	err = client.Archive(context.Background(), "test.txt", "glacier", map[string]string{"vault": "test", "tier": "expedited"})
	if err != nil {
		t.Errorf("Archive failed: %v", err)
	}
}

func TestRESTClient_Policies(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/policies":
			if r.Method == http.MethodPost {
				w.WriteHeader(http.StatusCreated)
			} else if r.Method == http.MethodGet {
				w.Write([]byte(`[{"id":"test","prefix":"tmp/","retention_seconds":86400,"action":"delete"}]`))
			}
		case "/api/v1/policies/test":
			if r.Method == http.MethodDelete {
				w.WriteHeader(http.StatusNoContent)
			}
		case "/api/v1/policies/apply":
			if r.Method == http.MethodPost {
				w.Write([]byte(`{"policies_count":1,"objects_processed":5}`))
			}
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	client, err := NewRESTClient(&Config{ServerURL: server.URL})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	// Test AddPolicy
	policy := common.LifecyclePolicy{
		ID:        "test",
		Prefix:    "tmp/",
		Retention: 24 * time.Hour,
		Action:    "delete",
	}
	err = client.AddPolicy(context.Background(), policy)
	if err != nil {
		t.Errorf("AddPolicy failed: %v", err)
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
		t.Errorf("expected 1 policy applied, got %d", count)
	}
	if processed != 5 {
		t.Errorf("expected 5 objects processed, got %d", processed)
	}

	// Test RemovePolicy
	err = client.RemovePolicy(context.Background(), "test")
	if err != nil {
		t.Errorf("RemovePolicy failed: %v", err)
	}
}

func TestRESTClient_Health(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health" {
			t.Errorf("expected /health, got %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client, err := NewRESTClient(&Config{ServerURL: server.URL})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	err = client.Health(context.Background())
	if err != nil {
		t.Errorf("Health failed: %v", err)
	}
}

func TestRESTClient_Errors(t *testing.T) {
	// Test server errors
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("server error"))
	}))
	defer server.Close()

	client, err := NewRESTClient(&Config{ServerURL: server.URL})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	err = client.Put(context.Background(), "test.txt", strings.NewReader("data"), nil)
	if err == nil {
		t.Error("expected error on server failure")
	}

	_, _, err = client.Get(context.Background(), "test.txt")
	if err == nil {
		t.Error("expected error on server failure")
	}
}

func TestRESTClient_Close(t *testing.T) {
	client, err := NewRESTClient(&Config{ServerURL: "http://localhost:8080"})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	err = client.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}
}

func TestNewRESTClient_InvalidURL(t *testing.T) {
	_, err := NewRESTClient(&Config{ServerURL: ""})
	if err == nil {
		t.Error("expected error with empty URL")
	}
}

// Additional error case tests

func TestRESTClient_Put_Error(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("server error"))
	}))
	defer server.Close()

	client, _ := NewRESTClient(&Config{ServerURL: server.URL})
	err := client.Put(context.Background(), "test.txt", strings.NewReader("data"), nil)
	if err == nil {
		t.Error("expected error on server failure")
	}
}

func TestRESTClient_Get_Error(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("not found"))
	}))
	defer server.Close()

	client, _ := NewRESTClient(&Config{ServerURL: server.URL})
	_, _, err := client.Get(context.Background(), "missing.txt")
	if err == nil {
		t.Error("expected error for missing file")
	}
}

func TestRESTClient_Delete_Error(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	client, _ := NewRESTClient(&Config{ServerURL: server.URL})
	err := client.Delete(context.Background(), "test.txt")
	if err == nil {
		t.Error("expected error on server failure")
	}
}

func TestRESTClient_List_Error(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	client, _ := NewRESTClient(&Config{ServerURL: server.URL})
	_, err := client.List(context.Background(), &common.ListOptions{})
	if err == nil {
		t.Error("expected error on server failure")
	}
}

func TestRESTClient_List_InvalidJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("invalid json"))
	}))
	defer server.Close()

	client, _ := NewRESTClient(&Config{ServerURL: server.URL})
	_, err := client.List(context.Background(), &common.ListOptions{})
	if err == nil {
		t.Error("expected error on invalid JSON")
	}
}

func TestRESTClient_GetMetadata_Error(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	client, _ := NewRESTClient(&Config{ServerURL: server.URL})
	_, err := client.GetMetadata(context.Background(), "missing.txt")
	if err == nil {
		t.Error("expected error for missing file")
	}
}

func TestRESTClient_GetMetadata_InvalidJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("not json"))
	}))
	defer server.Close()

	client, _ := NewRESTClient(&Config{ServerURL: server.URL})
	_, err := client.GetMetadata(context.Background(), "test.txt")
	if err == nil {
		t.Error("expected error on invalid JSON")
	}
}

func TestRESTClient_UpdateMetadata_Error(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	client, _ := NewRESTClient(&Config{ServerURL: server.URL})
	err := client.UpdateMetadata(context.Background(), "missing.txt", &common.Metadata{})
	if err == nil {
		t.Error("expected error for missing file")
	}
}

func TestRESTClient_Archive_Error(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	client, _ := NewRESTClient(&Config{ServerURL: server.URL})
	err := client.Archive(context.Background(), "test.txt", "glacier", nil)
	if err == nil {
		t.Error("expected error on server failure")
	}
}

func TestRESTClient_AddPolicy_Error(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()

	client, _ := NewRESTClient(&Config{ServerURL: server.URL})
	err := client.AddPolicy(context.Background(), common.LifecyclePolicy{})
	if err == nil {
		t.Error("expected error on bad request")
	}
}

func TestRESTClient_RemovePolicy_Error(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	client, _ := NewRESTClient(&Config{ServerURL: server.URL})
	err := client.RemovePolicy(context.Background(), "missing")
	if err == nil {
		t.Error("expected error for missing policy")
	}
}

func TestRESTClient_GetPolicies_Error(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	client, _ := NewRESTClient(&Config{ServerURL: server.URL})
	_, err := client.GetPolicies(context.Background())
	if err == nil {
		t.Error("expected error on server failure")
	}
}

func TestRESTClient_GetPolicies_InvalidJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("bad json"))
	}))
	defer server.Close()

	client, _ := NewRESTClient(&Config{ServerURL: server.URL})
	_, err := client.GetPolicies(context.Background())
	if err == nil {
		t.Error("expected error on invalid JSON")
	}
}

func TestRESTClient_ApplyPolicies_Error(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	client, _ := NewRESTClient(&Config{ServerURL: server.URL})
	_, _, err := client.ApplyPolicies(context.Background())
	if err == nil {
		t.Error("expected error on server failure")
	}
}

func TestRESTClient_ApplyPolicies_InvalidJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("invalid"))
	}))
	defer server.Close()

	client, _ := NewRESTClient(&Config{ServerURL: server.URL})
	_, _, err := client.ApplyPolicies(context.Background())
	if err == nil {
		t.Error("expected error on invalid JSON")
	}
}

func TestRESTClient_Health_Error(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	client, _ := NewRESTClient(&Config{ServerURL: server.URL})
	err := client.Health(context.Background())
	if err == nil {
		t.Error("expected error on unhealthy server")
	}
}

func TestRESTClient_List_WithAllOptions(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Check query parameters
		if r.URL.Query().Get("prefix") != "test/" {
			t.Errorf("expected prefix test/, got %s", r.URL.Query().Get("prefix"))
		}
		if r.URL.Query().Get("delimiter") != "/" {
			t.Errorf("expected delimiter /, got %s", r.URL.Query().Get("delimiter"))
		}
		if r.URL.Query().Get("max_results") != "10" {
			t.Errorf("expected max_results 10, got %s", r.URL.Query().Get("max_results"))
		}
		if r.URL.Query().Get("continue_from") != "token123" {
			t.Errorf("expected continue_from token123, got %s", r.URL.Query().Get("continue_from"))
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"Objects":[],"CommonPrefixes":["test/a/","test/b/"],"NextToken":"abc","Truncated":true}`))
	}))
	defer server.Close()

	client, _ := NewRESTClient(&Config{ServerURL: server.URL})
	result, err := client.List(context.Background(), &common.ListOptions{
		Prefix:       "test/",
		Delimiter:    "/",
		MaxResults:   10,
		ContinueFrom: "token123",
	})
	if err != nil {
		t.Errorf("List failed: %v", err)
	}

	if len(result.CommonPrefixes) != 2 {
		t.Errorf("expected 2 common prefixes, got %d", len(result.CommonPrefixes))
	}

	if result.NextToken != "abc" {
		t.Errorf("expected next token 'abc', got %s", result.NextToken)
	}

	if !result.Truncated {
		t.Error("expected Truncated to be true")
	}
}

func TestRESTClient_Put_WithContentEncoding(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if ce := r.Header.Get("Content-Encoding"); ce != "gzip" {
			t.Errorf("expected Content-Encoding gzip, got %s", ce)
		}
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	client, _ := NewRESTClient(&Config{ServerURL: server.URL})
	metadata := &common.Metadata{
		ContentEncoding: "gzip",
	}
	err := client.Put(context.Background(), "test.txt", strings.NewReader("data"), metadata)
	if err != nil {
		t.Errorf("Put failed: %v", err)
	}
}

func TestRESTClient_Get_WithContentLength(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "5")
		w.Header().Set("ETag", "abc123")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("hello"))
	}))
	defer server.Close()

	client, _ := NewRESTClient(&Config{ServerURL: server.URL})
	reader, metadata, err := client.Get(context.Background(), "test.txt")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	defer reader.Close()

	if metadata.Size != 5 {
		t.Errorf("expected size 5, got %d", metadata.Size)
	}

	if metadata.ETag != "abc123" {
		t.Errorf("expected ETag abc123, got %s", metadata.ETag)
	}

	data, _ := io.ReadAll(reader)
	if string(data) != "hello" {
		t.Errorf("expected 'hello', got %q", string(data))
	}
}

func TestRESTClient_List_NilOptions(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify no query parameters when nil options
		if len(r.URL.Query()) > 0 {
			t.Error("expected no query parameters with nil options")
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"Objects":[]}`))
	}))
	defer server.Close()

	client, _ := NewRESTClient(&Config{ServerURL: server.URL})
	_, err := client.List(context.Background(), nil)
	if err != nil {
		t.Errorf("List with nil options failed: %v", err)
	}
}

func TestRESTClient_AddPolicy_WithDestination(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if ct := r.Header.Get("Content-Type"); ct != "application/json" {
			t.Errorf("expected Content-Type application/json, got %s", ct)
		}
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	client, _ := NewRESTClient(&Config{ServerURL: server.URL})
	policy := common.LifecyclePolicy{
		ID:        "archive-policy",
		Prefix:    "logs/",
		Retention: time.Hour,
		Action:    "archive",
	}
	err := client.AddPolicy(context.Background(), policy)
	if err != nil {
		t.Errorf("AddPolicy failed: %v", err)
	}
}

func TestRESTClient_RemovePolicy_WithStatusOK(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK) // Test with OK instead of NoContent
	}))
	defer server.Close()

	client, _ := NewRESTClient(&Config{ServerURL: server.URL})
	err := client.RemovePolicy(context.Background(), "test-policy")
	if err != nil {
		t.Errorf("RemovePolicy with StatusOK failed: %v", err)
	}
}

func TestRESTClient_ApplyPolicies_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"policies_count":3,"objects_processed":15}`))
	}))
	defer server.Close()

	client, _ := NewRESTClient(&Config{ServerURL: server.URL})
	count, processed, err := client.ApplyPolicies(context.Background())
	if err != nil {
		t.Errorf("ApplyPolicies failed: %v", err)
	}
	if count != 3 {
		t.Errorf("expected 3 policies, got %d", count)
	}
	if processed != 15 {
		t.Errorf("expected 15 objects, got %d", processed)
	}
}

func TestRESTClient_GetPolicies_WithMultiplePolicies(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`[
			{"ID":"p1","Prefix":"test/","Retention":7200000000000,"Action":"delete"},
			{"ID":"p2","Prefix":"old/","Retention":2592000000000000,"Action":"archive"},
			{"ID":"p3","Prefix":"temp/","Retention":3600000000000,"Action":"delete"}
		]`))
	}))
	defer server.Close()

	client, _ := NewRESTClient(&Config{ServerURL: server.URL})
	policies, err := client.GetPolicies(context.Background())
	if err != nil {
		t.Fatalf("GetPolicies failed: %v", err)
	}

	if len(policies) != 3 {
		t.Errorf("expected 3 policies, got %d", len(policies))
	}

	if policies[1].Action != "archive" {
		t.Errorf("expected archive action, got %s", policies[1].Action)
	}
}

func TestRESTClient_Put_StatusOK(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK) // Test with OK instead of Created
	}))
	defer server.Close()

	client, _ := NewRESTClient(&Config{ServerURL: server.URL})
	err := client.Put(context.Background(), "test.txt", strings.NewReader("data"), nil)
	if err != nil {
		t.Errorf("Put with StatusOK failed: %v", err)
	}
}

func TestQUICClient_Put_StatusOK(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK) // Test with OK instead of Created
	}))
	defer server.Close()

	client, _ := NewQUICClient(&Config{ServerURL: server.URL})
	err := client.Put(context.Background(), "test.txt", strings.NewReader("data"), nil)
	if err != nil {
		t.Errorf("Put with StatusOK failed: %v", err)
	}
}

// Additional error path tests for Exists

func TestRESTClient_Exists_RequestError(t *testing.T) {
	client, _ := NewRESTClient(&Config{ServerURL: "http://localhost:0"})

	_, err := client.Exists(context.Background(), "test.txt")
	if err == nil {
		t.Error("expected error from connection failure")
	}
}

func TestRESTClient_Exists_ContextCanceled(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client, _ := NewRESTClient(&Config{ServerURL: server.URL})

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, err := client.Exists(ctx, "test.txt")
	if err == nil {
		t.Error("expected error from canceled context")
	}
}

func TestQUICClient_Exists_RequestError(t *testing.T) {
	client, _ := NewQUICClient(&Config{ServerURL: "http://localhost:0"})

	_, err := client.Exists(context.Background(), "test.txt")
	if err == nil {
		t.Error("expected error from connection failure")
	}
}

func TestQUICClient_Exists_ContextCanceled(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client, _ := NewQUICClient(&Config{ServerURL: server.URL})

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, err := client.Exists(ctx, "test.txt")
	if err == nil {
		t.Error("expected error from canceled context")
	}
}

// Additional error path tests for other methods

func TestRESTClient_Delete_RequestError(t *testing.T) {
	client, _ := NewRESTClient(&Config{ServerURL: "http://localhost:0"})

	err := client.Delete(context.Background(), "test.txt")
	if err == nil {
		t.Error("expected error from connection failure")
	}
}

func TestQUICClient_Delete_RequestError(t *testing.T) {
	client, _ := NewQUICClient(&Config{ServerURL: "http://localhost:0"})

	err := client.Delete(context.Background(), "test.txt")
	if err == nil {
		t.Error("expected error from connection failure")
	}
}

func TestRESTClient_Health_RequestError(t *testing.T) {
	client, _ := NewRESTClient(&Config{ServerURL: "http://localhost:0"})

	err := client.Health(context.Background())
	if err == nil {
		t.Error("expected error from connection failure")
	}
}

func TestQUICClient_Health_RequestError(t *testing.T) {
	client, _ := NewQUICClient(&Config{ServerURL: "http://localhost:0"})

	err := client.Health(context.Background())
	if err == nil {
		t.Error("expected error from connection failure")
	}
}

func TestRESTClient_Archive_RequestError(t *testing.T) {
	client, _ := NewRESTClient(&Config{ServerURL: "http://localhost:0"})

	err := client.Archive(context.Background(), "test.txt", "glacier", map[string]string{})
	if err == nil {
		t.Error("expected error from connection failure")
	}
}

func TestQUICClient_Archive_RequestError(t *testing.T) {
	client, _ := NewQUICClient(&Config{ServerURL: "http://localhost:0"})

	err := client.Archive(context.Background(), "test.txt", "glacier", map[string]string{})
	if err == nil {
		t.Error("expected error from connection failure")
	}
}

func TestRESTClient_AddPolicy_RequestError(t *testing.T) {
	client, _ := NewRESTClient(&Config{ServerURL: "http://localhost:0"})

	policy := common.LifecyclePolicy{
		ID:        "test-policy",
		Prefix:    "test/",
		Retention: 24 * time.Hour,
		Action:    "delete",
	}

	err := client.AddPolicy(context.Background(), policy)
	if err == nil {
		t.Error("expected error from connection failure")
	}
}

func TestQUICClient_AddPolicy_RequestError(t *testing.T) {
	client, _ := NewQUICClient(&Config{ServerURL: "http://localhost:0"})

	policy := common.LifecyclePolicy{
		ID:        "test-policy",
		Prefix:    "test/",
		Retention: 24 * time.Hour,
		Action:    "delete",
	}

	err := client.AddPolicy(context.Background(), policy)
	if err == nil {
		t.Error("expected error from connection failure")
	}
}

func TestRESTClient_RemovePolicy_RequestError(t *testing.T) {
	client, _ := NewRESTClient(&Config{ServerURL: "http://localhost:0"})

	err := client.RemovePolicy(context.Background(), "test-policy")
	if err == nil {
		t.Error("expected error from connection failure")
	}
}

func TestQUICClient_RemovePolicy_RequestError(t *testing.T) {
	client, _ := NewQUICClient(&Config{ServerURL: "http://localhost:0"})

	err := client.RemovePolicy(context.Background(), "test-policy")
	if err == nil {
		t.Error("expected error from connection failure")
	}
}

// Additional error tests for Put methods

func TestRESTClient_Put_ReadError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	client, _ := NewRESTClient(&Config{ServerURL: server.URL})

	// Create a reader that errors
	reader := &errorReader{}
	err := client.Put(context.Background(), "test.txt", reader, nil)
	if err == nil {
		t.Error("expected error from reader failure")
	}
}

func TestQUICClient_Put_ReadError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	client, _ := NewQUICClient(&Config{ServerURL: server.URL})

	// Create a reader that errors
	reader := &errorReader{}
	err := client.Put(context.Background(), "test.txt", reader, nil)
	if err == nil {
		t.Error("expected error from reader failure")
	}
}

// REST Replication tests
func TestRESTClient_AddReplicationPolicy(t *testing.T) {
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

	client, _ := NewRESTClient(&Config{ServerURL: server.URL})
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

func TestRESTClient_AddReplicationPolicy_Error(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("invalid policy"))
	}))
	defer server.Close()

	client, _ := NewRESTClient(&Config{ServerURL: server.URL})
	policy := common.ReplicationPolicy{ID: "test"}

	err := client.AddReplicationPolicy(context.Background(), policy)
	if err == nil {
		t.Error("expected error on bad request")
	}
}

func TestRESTClient_RemoveReplicationPolicy(t *testing.T) {
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

	client, _ := NewRESTClient(&Config{ServerURL: server.URL})
	err := client.RemoveReplicationPolicy(context.Background(), "test-policy")
	if err != nil {
		t.Errorf("RemoveReplicationPolicy failed: %v", err)
	}
}

func TestRESTClient_RemoveReplicationPolicy_Error(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("policy not found"))
	}))
	defer server.Close()

	client, _ := NewRESTClient(&Config{ServerURL: server.URL})
	err := client.RemoveReplicationPolicy(context.Background(), "missing")
	if err == nil {
		t.Error("expected error for missing policy")
	}
}

func TestRESTClient_GetReplicationPolicy(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"id":"test-policy","source_backend":"local","destination_backend":"s3"}`))
	}))
	defer server.Close()

	client, _ := NewRESTClient(&Config{ServerURL: server.URL})
	policy, err := client.GetReplicationPolicy(context.Background(), "test-policy")
	if err != nil {
		t.Fatalf("GetReplicationPolicy failed: %v", err)
	}
	if policy == nil || policy.ID != "test-policy" {
		t.Errorf("expected policy with ID test-policy, got %v", policy)
	}
}

func TestRESTClient_GetReplicationPolicy_Error(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("policy not found"))
	}))
	defer server.Close()

	client, _ := NewRESTClient(&Config{ServerURL: server.URL})
	_, err := client.GetReplicationPolicy(context.Background(), "missing")
	if err == nil {
		t.Error("expected error for missing policy")
	}
}

func TestRESTClient_GetReplicationPolicy_InvalidJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{invalid json`))
	}))
	defer server.Close()

	client, _ := NewRESTClient(&Config{ServerURL: server.URL})
	_, err := client.GetReplicationPolicy(context.Background(), "test")
	if err == nil {
		t.Error("expected error on invalid JSON")
	}
}

func TestRESTClient_GetReplicationPolicies(t *testing.T) {
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

	client, _ := NewRESTClient(&Config{ServerURL: server.URL})
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

func TestRESTClient_GetReplicationPolicies_Error(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("internal error"))
	}))
	defer server.Close()

	client, _ := NewRESTClient(&Config{ServerURL: server.URL})
	_, err := client.GetReplicationPolicies(context.Background())
	if err == nil {
		t.Error("expected error on server failure")
	}
}

func TestRESTClient_GetReplicationPolicies_InvalidJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`[{invalid`))
	}))
	defer server.Close()

	client, _ := NewRESTClient(&Config{ServerURL: server.URL})
	_, err := client.GetReplicationPolicies(context.Background())
	if err == nil {
		t.Error("expected error on invalid JSON")
	}
}

func TestRESTClient_TriggerReplication(t *testing.T) {
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

	client, _ := NewRESTClient(&Config{ServerURL: server.URL})
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

func TestRESTClient_TriggerReplication_EmptyPolicyID(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("policy_id") != "" {
			t.Error("expected empty policy_id")
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"synced":5,"deleted":2,"failed":1}`))
	}))
	defer server.Close()

	client, _ := NewRESTClient(&Config{ServerURL: server.URL})
	result, err := client.TriggerReplication(context.Background(), "")
	if err != nil {
		t.Fatalf("TriggerReplication failed: %v", err)
	}
	if result == nil {
		t.Error("expected non-nil result")
	}
}

func TestRESTClient_TriggerReplication_Error(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("invalid policy"))
	}))
	defer server.Close()

	client, _ := NewRESTClient(&Config{ServerURL: server.URL})
	_, err := client.TriggerReplication(context.Background(), "invalid")
	if err == nil {
		t.Error("expected error on bad request")
	}
}

func TestRESTClient_TriggerReplication_InvalidJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{bad json`))
	}))
	defer server.Close()

	client, _ := NewRESTClient(&Config{ServerURL: server.URL})
	_, err := client.TriggerReplication(context.Background(), "test")
	if err == nil {
		t.Error("expected error on invalid JSON")
	}
}

func TestRESTClient_AddReplicationPolicy_WithStatusOK(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK) // Test with OK instead of Created
	}))
	defer server.Close()

	client, _ := NewRESTClient(&Config{ServerURL: server.URL})
	policy := common.ReplicationPolicy{ID: "test"}

	err := client.AddReplicationPolicy(context.Background(), policy)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestRESTClient_RemoveReplicationPolicy_WithStatusOK(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK) // Test with OK instead of NoContent
	}))
	defer server.Close()

	client, _ := NewRESTClient(&Config{ServerURL: server.URL})
	err := client.RemoveReplicationPolicy(context.Background(), "test-policy")
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

// Additional error tests for request creation errors

func TestRESTClient_Put_RequestError(t *testing.T) {
	client, _ := NewRESTClient(&Config{ServerURL: "http://localhost:0"})

	err := client.Put(context.Background(), "test.txt", strings.NewReader("data"), nil)
	if err == nil {
		t.Error("expected error from connection failure")
	}
}

func TestRESTClient_Get_RequestError(t *testing.T) {
	client, _ := NewRESTClient(&Config{ServerURL: "http://localhost:0"})

	_, _, err := client.Get(context.Background(), "test.txt")
	if err == nil {
		t.Error("expected error from connection failure")
	}
}

func TestRESTClient_GetMetadata_RequestError(t *testing.T) {
	client, _ := NewRESTClient(&Config{ServerURL: "http://localhost:0"})

	_, err := client.GetMetadata(context.Background(), "test.txt")
	if err == nil {
		t.Error("expected error from connection failure")
	}
}

func TestRESTClient_UpdateMetadata_RequestError(t *testing.T) {
	client, _ := NewRESTClient(&Config{ServerURL: "http://localhost:0"})

	err := client.UpdateMetadata(context.Background(), "test.txt", &common.Metadata{})
	if err == nil {
		t.Error("expected error from connection failure")
	}
}

func TestRESTClient_GetPolicies_RequestError(t *testing.T) {
	client, _ := NewRESTClient(&Config{ServerURL: "http://localhost:0"})

	_, err := client.GetPolicies(context.Background())
	if err == nil {
		t.Error("expected error from connection failure")
	}
}

func TestRESTClient_ApplyPolicies_RequestError(t *testing.T) {
	client, _ := NewRESTClient(&Config{ServerURL: "http://localhost:0"})

	_, _, err := client.ApplyPolicies(context.Background())
	if err == nil {
		t.Error("expected error from connection failure")
	}
}

func TestRESTClient_List_RequestError(t *testing.T) {
	client, _ := NewRESTClient(&Config{ServerURL: "http://localhost:0"})

	_, err := client.List(context.Background(), &common.ListOptions{})
	if err == nil {
		t.Error("expected error from connection failure")
	}
}

func TestRESTClient_GetReplicationPolicy_RequestError(t *testing.T) {
	client, _ := NewRESTClient(&Config{ServerURL: "http://localhost:0"})

	_, err := client.GetReplicationPolicy(context.Background(), "test")
	if err == nil {
		t.Error("expected error from connection failure")
	}
}

func TestRESTClient_GetReplicationPolicies_RequestError(t *testing.T) {
	client, _ := NewRESTClient(&Config{ServerURL: "http://localhost:0"})

	_, err := client.GetReplicationPolicies(context.Background())
	if err == nil {
		t.Error("expected error from connection failure")
	}
}

func TestRESTClient_TriggerReplication_RequestError(t *testing.T) {
	client, _ := NewRESTClient(&Config{ServerURL: "http://localhost:0"})

	_, err := client.TriggerReplication(context.Background(), "test")
	if err == nil {
		t.Error("expected error from connection failure")
	}
}

func TestRESTClient_AddReplicationPolicy_RequestError(t *testing.T) {
	client, _ := NewRESTClient(&Config{ServerURL: "http://localhost:0"})

	err := client.AddReplicationPolicy(context.Background(), common.ReplicationPolicy{})
	if err == nil {
		t.Error("expected error from connection failure")
	}
}

func TestRESTClient_RemoveReplicationPolicy_RequestError(t *testing.T) {
	client, _ := NewRESTClient(&Config{ServerURL: "http://localhost:0"})

	err := client.RemoveReplicationPolicy(context.Background(), "test")
	if err == nil {
		t.Error("expected error from connection failure")
	}
}

// Add similar tests for QUIC
func TestQUICClient_GetMetadata_RequestError(t *testing.T) {
	client, _ := NewQUICClient(&Config{ServerURL: "http://localhost:0"})

	_, err := client.GetMetadata(context.Background(), "test.txt")
	if err == nil {
		t.Error("expected error from connection failure")
	}
}

func TestQUICClient_UpdateMetadata_RequestError(t *testing.T) {
	client, _ := NewQUICClient(&Config{ServerURL: "http://localhost:0"})

	err := client.UpdateMetadata(context.Background(), "test.txt", &common.Metadata{})
	if err == nil {
		t.Error("expected error from connection failure")
	}
}

func TestQUICClient_GetPolicies_RequestError(t *testing.T) {
	client, _ := NewQUICClient(&Config{ServerURL: "http://localhost:0"})

	_, err := client.GetPolicies(context.Background())
	if err == nil {
		t.Error("expected error from connection failure")
	}
}

func TestQUICClient_ApplyPolicies_RequestError(t *testing.T) {
	client, _ := NewQUICClient(&Config{ServerURL: "http://localhost:0"})

	_, _, err := client.ApplyPolicies(context.Background())
	if err == nil {
		t.Error("expected error from connection failure")
	}
}

func TestQUICClient_List_RequestError(t *testing.T) {
	client, _ := NewQUICClient(&Config{ServerURL: "http://localhost:0"})

	_, err := client.List(context.Background(), &common.ListOptions{})
	if err == nil {
		t.Error("expected error from connection failure")
	}
}

func TestQUICClient_Get_RequestError(t *testing.T) {
	client, _ := NewQUICClient(&Config{ServerURL: "http://localhost:0"})

	_, _, err := client.Get(context.Background(), "test.txt")
	if err == nil {
		t.Error("expected error from connection failure")
	}
}

func TestQUICClient_Put_RequestError(t *testing.T) {
	client, _ := NewQUICClient(&Config{ServerURL: "http://localhost:0"})

	err := client.Put(context.Background(), "test.txt", strings.NewReader("data"), nil)
	if err == nil {
		t.Error("expected error from connection failure")
	}
}

func TestQUICClient_GetReplicationPolicy_RequestError(t *testing.T) {
	client, _ := NewQUICClient(&Config{ServerURL: "http://localhost:0"})

	_, err := client.GetReplicationPolicy(context.Background(), "test")
	if err == nil {
		t.Error("expected error from connection failure")
	}
}

func TestQUICClient_GetReplicationPolicies_RequestError(t *testing.T) {
	client, _ := NewQUICClient(&Config{ServerURL: "http://localhost:0"})

	_, err := client.GetReplicationPolicies(context.Background())
	if err == nil {
		t.Error("expected error from connection failure")
	}
}

func TestQUICClient_TriggerReplication_RequestError(t *testing.T) {
	client, _ := NewQUICClient(&Config{ServerURL: "http://localhost:0"})

	_, err := client.TriggerReplication(context.Background(), "test")
	if err == nil {
		t.Error("expected error from connection failure")
	}
}

func TestQUICClient_AddReplicationPolicy_RequestError(t *testing.T) {
	client, _ := NewQUICClient(&Config{ServerURL: "http://localhost:0"})

	err := client.AddReplicationPolicy(context.Background(), common.ReplicationPolicy{})
	if err == nil {
		t.Error("expected error from connection failure")
	}
}

func TestQUICClient_RemoveReplicationPolicy_RequestError(t *testing.T) {
	client, _ := NewQUICClient(&Config{ServerURL: "http://localhost:0"})

	err := client.RemoveReplicationPolicy(context.Background(), "test")
	if err == nil {
		t.Error("expected error from connection failure")
	}
}
