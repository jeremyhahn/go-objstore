// Copyright (c) 2025 Jeremy Hahn
// Copyright (c) 2025 Automate The Things, LLC
//
// This file is part of go-objstore.
//
// go-objstore is dual-licensed under AGPL-3.0 and a Commercial License.

package objstore

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRESTClient_Put_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "PUT", r.Method)
		assert.Equal(t, "/objects/test-key", r.URL.Path)
		assert.Equal(t, "text/plain", r.Header.Get("Content-Type"))

		w.Header().Set("ETag", "abc123")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"message": "Object stored successfully",
			"data": map[string]string{
				"etag": "abc123",
			},
		})
	}))
	defer server.Close()

	client := &RESTClient{
		baseURL:    server.URL,
		httpClient: server.Client(),
		config:     &ClientConfig{},
	}

	ctx := context.Background()
	result, err := client.Put(ctx, "test-key", []byte("test-data"), &Metadata{
		ContentType: "text/plain",
	})

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.True(t, result.Success)
	assert.Equal(t, "abc123", result.ETag)
}

func TestRESTClient_Put_Error(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	client := &RESTClient{
		baseURL:    server.URL,
		httpClient: server.Client(),
		config:     &ClientConfig{},
	}

	ctx := context.Background()
	_, err := client.Put(ctx, "test-key", []byte("test-data"), nil)
	assert.Error(t, err)
}

func TestRESTClient_Get_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "GET", r.Method)
		assert.Equal(t, "/objects/test-key", r.URL.Path)

		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("ETag", "abc123")
		w.Header().Set("Content-Length", "9")
		w.Header().Set("Last-Modified", time.Now().Format(http.TimeFormat))
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("test-data"))
	}))
	defer server.Close()

	client := &RESTClient{
		baseURL:    server.URL,
		httpClient: server.Client(),
		config:     &ClientConfig{},
	}

	ctx := context.Background()
	result, err := client.Get(ctx, "test-key")
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, []byte("test-data"), result.Data)
	assert.Equal(t, "text/plain", result.Metadata.ContentType)
	assert.Equal(t, "abc123", result.Metadata.ETag)
}

func TestRESTClient_Get_NotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	client := &RESTClient{
		baseURL:    server.URL,
		httpClient: server.Client(),
		config:     &ClientConfig{},
	}

	ctx := context.Background()
	_, err := client.Get(ctx, "test-key")
	assert.ErrorIs(t, err, ErrObjectNotFound)
}

func TestRESTClient_Get_WithCustomMetadata(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		customMeta := map[string]string{
			"author":  "test-user",
			"version": "1.0",
		}
		metaJSON, _ := json.Marshal(customMeta)
		w.Header().Set("X-Object-Metadata", string(metaJSON))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("{}"))
	}))
	defer server.Close()

	client := &RESTClient{
		baseURL:    server.URL,
		httpClient: server.Client(),
		config:     &ClientConfig{},
	}

	ctx := context.Background()
	result, err := client.Get(ctx, "test-key")
	assert.NoError(t, err)
	assert.NotNil(t, result.Metadata)
	assert.Equal(t, "test-user", result.Metadata.Custom["author"])
	assert.Equal(t, "1.0", result.Metadata.Custom["version"])
}

func TestRESTClient_Delete_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "DELETE", r.Method)
		assert.Equal(t, "/objects/test-key", r.URL.Path)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := &RESTClient{
		baseURL:    server.URL,
		httpClient: server.Client(),
		config:     &ClientConfig{},
	}

	ctx := context.Background()
	err := client.Delete(ctx, "test-key")
	assert.NoError(t, err)
}

func TestRESTClient_Delete_NotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	client := &RESTClient{
		baseURL:    server.URL,
		httpClient: server.Client(),
		config:     &ClientConfig{},
	}

	ctx := context.Background()
	err := client.Delete(ctx, "test-key")
	assert.ErrorIs(t, err, ErrObjectNotFound)
}

func TestRESTClient_List_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "GET", r.Method)
		assert.Equal(t, "/objects", r.URL.Path)
		assert.Equal(t, "test/", r.URL.Query().Get("prefix"))

		response := map[string]interface{}{
			"objects": []map[string]interface{}{
				{
					"key":      "test/file1",
					"size":     100,
					"modified": time.Now().Format(time.RFC3339),
					"etag":     "abc123",
				},
				{
					"key":      "test/file2",
					"size":     200,
					"modified": time.Now().Format(time.RFC3339),
					"etag":     "def456",
				},
			},
			"common_prefixes": []string{"test/subdir/"},
			"next_token":      "token123",
			"truncated":       true,
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	client := &RESTClient{
		baseURL:    server.URL,
		httpClient: server.Client(),
		config:     &ClientConfig{},
	}

	ctx := context.Background()
	result, err := client.List(ctx, &ListOptions{
		Prefix:     "test/",
		Delimiter:  "/",
		MaxResults: 100,
	})

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Len(t, result.Objects, 2)
	assert.Equal(t, "test/file1", result.Objects[0].Key)
	assert.Equal(t, int64(100), result.Objects[0].Metadata.Size)
	assert.Len(t, result.CommonPrefixes, 1)
	assert.Equal(t, "token123", result.NextToken)
	assert.True(t, result.Truncated)
}

func TestRESTClient_List_EmptyOptions(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response := map[string]interface{}{
			"objects":   []map[string]interface{}{},
			"truncated": false,
		}
		json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	client := &RESTClient{
		baseURL:    server.URL,
		httpClient: server.Client(),
		config:     &ClientConfig{},
	}

	ctx := context.Background()
	result, err := client.List(ctx, nil)
	assert.NoError(t, err)
	assert.NotNil(t, result)
}

func TestRESTClient_Exists_True(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "HEAD", r.Method)
		assert.Equal(t, "/objects/test-key", r.URL.Path)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := &RESTClient{
		baseURL:    server.URL,
		httpClient: server.Client(),
		config:     &ClientConfig{},
	}

	ctx := context.Background()
	exists, err := client.Exists(ctx, "test-key")
	assert.NoError(t, err)
	assert.True(t, exists)
}

func TestRESTClient_Exists_False(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	client := &RESTClient{
		baseURL:    server.URL,
		httpClient: server.Client(),
		config:     &ClientConfig{},
	}

	ctx := context.Background()
	exists, err := client.Exists(ctx, "test-key")
	assert.NoError(t, err)
	assert.False(t, exists)
}

func TestRESTClient_GetMetadata_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "HEAD", r.Method)
		assert.Equal(t, "/objects/test-key", r.URL.Path)

		w.Header().Set("ETag", "abc123")
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", "1024")
		w.Header().Set("Last-Modified", time.Now().Format(time.RFC1123))

		customMeta := map[string]string{"author": "test"}
		metaJSON, _ := json.Marshal(customMeta)
		w.Header().Set("X-Object-Metadata", string(metaJSON))

		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := &RESTClient{
		baseURL:    server.URL,
		httpClient: server.Client(),
		config:     &ClientConfig{},
	}

	ctx := context.Background()
	metadata, err := client.GetMetadata(ctx, "test-key")
	assert.NoError(t, err)
	assert.NotNil(t, metadata)
	assert.Equal(t, "abc123", metadata.ETag)
	assert.Equal(t, "application/json", metadata.ContentType)
	assert.Equal(t, int64(1024), metadata.Size)
	assert.Equal(t, "test", metadata.Custom["author"])
}

func TestRESTClient_GetMetadata_NotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	client := &RESTClient{
		baseURL:    server.URL,
		httpClient: server.Client(),
		config:     &ClientConfig{},
	}

	ctx := context.Background()
	_, err := client.GetMetadata(ctx, "test-key")
	assert.ErrorIs(t, err, ErrObjectNotFound)
}

func TestRESTClient_UpdateMetadata_NotSupported(t *testing.T) {
	client := &RESTClient{
		baseURL:    "http://localhost:8080",
		httpClient: &http.Client{},
		config:     &ClientConfig{},
	}

	ctx := context.Background()
	err := client.UpdateMetadata(ctx, "test-key", &Metadata{ContentType: "text/html"})
	assert.ErrorIs(t, err, ErrNotSupported)
}

func TestRESTClient_Health_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "GET", r.Method)
		assert.Equal(t, "/health", r.URL.Path)

		response := map[string]string{
			"status":  "healthy",
			"version": "1.0.0",
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	client := &RESTClient{
		baseURL:    server.URL,
		httpClient: server.Client(),
		config:     &ClientConfig{},
	}

	ctx := context.Background()
	status, err := client.Health(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, status)
	assert.Equal(t, "HEALTHY", status.Status)
	assert.Equal(t, "1.0.0", status.Message)
}

func TestRESTClient_Put_WithCustomMetadata(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "gzip", r.Header.Get("Content-Encoding"))

		metaHeader := r.Header.Get("X-Object-Metadata")
		assert.NotEmpty(t, metaHeader)

		var customMeta map[string]string
		err := json.Unmarshal([]byte(metaHeader), &customMeta)
		require.NoError(t, err)
		assert.Equal(t, "test-user", customMeta["author"])

		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"message": "Success",
		})
	}))
	defer server.Close()

	client := &RESTClient{
		baseURL:    server.URL,
		httpClient: server.Client(),
		config:     &ClientConfig{},
	}

	ctx := context.Background()
	_, err := client.Put(ctx, "test-key", []byte("data"), &Metadata{
		ContentType:     "application/json",
		ContentEncoding: "gzip",
		Custom: map[string]string{
			"author": "test-user",
		},
	})
	assert.NoError(t, err)
}

func TestRESTClient_List_WithAllOptions(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "test-prefix", r.URL.Query().Get("prefix"))
		assert.Equal(t, "/", r.URL.Query().Get("delimiter"))
		assert.Equal(t, "10", r.URL.Query().Get("limit"))
		assert.Equal(t, "continue-token", r.URL.Query().Get("token"))

		response := map[string]interface{}{
			"objects":   []map[string]interface{}{},
			"truncated": false,
		}
		json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	client := &RESTClient{
		baseURL:    server.URL,
		httpClient: server.Client(),
		config:     &ClientConfig{},
	}

	ctx := context.Background()
	_, err := client.List(ctx, &ListOptions{
		Prefix:       "test-prefix",
		Delimiter:    "/",
		MaxResults:   10,
		ContinueFrom: "continue-token",
	})
	assert.NoError(t, err)
}

func TestRESTClient_Put_StatusOK(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"message": "Updated",
		})
	}))
	defer server.Close()

	client := &RESTClient{
		baseURL:    server.URL,
		httpClient: server.Client(),
		config:     &ClientConfig{},
	}

	ctx := context.Background()
	result, err := client.Put(ctx, "test-key", []byte("data"), nil)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.True(t, result.Success)
}

func TestRESTClient_Get_ServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	client := &RESTClient{
		baseURL:    server.URL,
		httpClient: server.Client(),
		config:     &ClientConfig{},
	}

	ctx := context.Background()
	_, err := client.Get(ctx, "test-key")
	assert.Error(t, err)
}

func TestRESTClient_Delete_ServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	client := &RESTClient{
		baseURL:    server.URL,
		httpClient: server.Client(),
		config:     &ClientConfig{},
	}

	ctx := context.Background()
	err := client.Delete(ctx, "test-key")
	assert.Error(t, err)
}

func TestRESTClient_List_ServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	client := &RESTClient{
		baseURL:    server.URL,
		httpClient: server.Client(),
		config:     &ClientConfig{},
	}

	ctx := context.Background()
	_, err := client.List(ctx, nil)
	assert.Error(t, err)
}

func TestRESTClient_GetMetadata_ServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	client := &RESTClient{
		baseURL:    server.URL,
		httpClient: server.Client(),
		config:     &ClientConfig{},
	}

	ctx := context.Background()
	_, err := client.GetMetadata(ctx, "test-key")
	assert.Error(t, err)
}

func TestNewRESTClient_WithRequestTimeout(t *testing.T) {
	config := &ClientConfig{
		Protocol:       ProtocolREST,
		Address:        "localhost:8080",
		RequestTimeout: 5 * time.Second,
	}

	client, err := newRESTClient(config)
	require.NoError(t, err)
	assert.NotNil(t, client)
	assert.Equal(t, 5*time.Second, client.httpClient.Timeout)
}

func TestNewRESTClient_WithInvalidTLSConfig(t *testing.T) {
	config := &ClientConfig{
		Protocol: ProtocolREST,
		Address:  "localhost:8443",
		UseTLS:   true,
		CAFile:   "/nonexistent/ca.pem",
	}

	_, err := newRESTClient(config)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to build TLS config")
}
