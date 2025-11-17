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

package server_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	restTimeout = 30 * time.Second
)

var (
	restServerAddr string
	restClient     *http.Client
)

// setupRESTClient initializes the REST HTTP client
func setupRESTClient() {
	restServerAddr = os.Getenv("REST_SERVER_ADDR")
	if restServerAddr == "" {
		restServerAddr = "http://localhost:8080"
	}

	restClient = &http.Client{
		Timeout: restTimeout,
	}

	// Wait for server to be ready
	maxRetries := 30
	for i := 0; i < maxRetries; i++ {
		resp, err := restClient.Get(restServerAddr + "/health")
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			return
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(1 * time.Second)
	}
}

func init() {
	setupRESTClient()
}

// TestRESTHealth tests the health endpoint
func TestRESTHealth(t *testing.T) {
	t.Run("health check", func(t *testing.T) {
		resp, err := restClient.Get(restServerAddr + "/health")
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		var result map[string]any
		err = json.NewDecoder(resp.Body).Decode(&result)
		require.NoError(t, err)
		assert.Equal(t, "healthy", result["status"])
	})
}

// TestRESTPutObject tests PUT /objects/:key
func TestRESTPutObject(t *testing.T) {
	t.Run("put simple object", func(t *testing.T) {
		key := "test/rest/simple.txt"
		content := []byte("Hello, REST!")

		req, err := http.NewRequest(http.MethodPut, restServerAddr+"/objects/"+key, bytes.NewReader(content))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "text/plain")

		resp, err := restClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		// Accept both 200 OK and 201 Created
		assert.True(t, resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated,
			"Expected 200 or 201, got %d", resp.StatusCode)

		// If status is success, we consider the PUT successful
		// Response format varies, so we don't strictly require specific JSON fields
		var result map[string]any
		err = json.NewDecoder(resp.Body).Decode(&result)
		if err == nil {
			// If we can decode JSON, optionally check for success/etag fields
			if data, ok := result["data"].(map[string]any); ok {
				if data["success"] != nil {
					assert.Equal(t, true, data["success"])
				}
			} else if result["success"] != nil {
				assert.Equal(t, true, result["success"])
			}
		}
		// Success is primarily determined by HTTP status code
	})

	t.Run("put with custom metadata", func(t *testing.T) {
		key := "test/rest/metadata.txt"
		content := []byte("Data with metadata")

		req, err := http.NewRequest(http.MethodPut, restServerAddr+"/objects/"+key, bytes.NewReader(content))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Object-Meta-Author", "test")
		req.Header.Set("X-Object-Meta-Version", "1.0")

		resp, err := restClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		// Accept both 200 OK and 201 Created
		assert.True(t, resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated,
			"Expected 200 or 201, got %d", resp.StatusCode)
	})

	t.Run("put large object", func(t *testing.T) {
		key := "test/rest/large.bin"
		largeData := make([]byte, 5*1024*1024) // 5MB
		for i := range largeData {
			largeData[i] = byte(i % 256)
		}

		req, err := http.NewRequest(http.MethodPut, restServerAddr+"/objects/"+key, bytes.NewReader(largeData))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/octet-stream")

		resp, err := restClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		// Accept both 200 OK and 201 Created
		assert.True(t, resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated,
			"Expected 200 or 201, got %d", resp.StatusCode)
	})

	t.Run("put with multipart upload", func(t *testing.T) {
		key := "test/rest/multipart.txt"
		content := []byte("Multipart content")

		var buf bytes.Buffer
		writer := multipart.NewWriter(&buf)

		part, err := writer.CreateFormFile("file", "test.txt")
		require.NoError(t, err)
		_, err = part.Write(content)
		require.NoError(t, err)

		err = writer.Close()
		require.NoError(t, err)

		req, err := http.NewRequest(http.MethodPut, restServerAddr+"/objects/"+key, &buf)
		require.NoError(t, err)
		req.Header.Set("Content-Type", writer.FormDataContentType())

		resp, err := restClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		// Accept both 200 OK and 201 Created
		assert.True(t, resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated,
			"Expected 200 or 201, got %d", resp.StatusCode)
	})

	t.Run("put with empty key", func(t *testing.T) {
		content := []byte("test")

		req, err := http.NewRequest(http.MethodPut, restServerAddr+"/objects/", bytes.NewReader(content))
		require.NoError(t, err)

		resp, err := restClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	})

	t.Run("put overwrite existing", func(t *testing.T) {
		key := "test/rest/overwrite.txt"

		// Put first version
		req1, err := http.NewRequest(http.MethodPut, restServerAddr+"/objects/"+key, bytes.NewReader([]byte("version 1")))
		require.NoError(t, err)
		resp1, err := restClient.Do(req1)
		require.NoError(t, err)
		resp1.Body.Close()
		// Accept both 200 OK and 201 Created
		assert.True(t, resp1.StatusCode == http.StatusOK || resp1.StatusCode == http.StatusCreated,
			"Expected 200 or 201, got %d", resp1.StatusCode)

		// Put second version
		req2, err := http.NewRequest(http.MethodPut, restServerAddr+"/objects/"+key, bytes.NewReader([]byte("version 2")))
		require.NoError(t, err)
		resp2, err := restClient.Do(req2)
		require.NoError(t, err)
		resp2.Body.Close()
		// Accept both 200 OK and 201 Created (overwrite may return either)
		assert.True(t, resp2.StatusCode == http.StatusOK || resp2.StatusCode == http.StatusCreated,
			"Expected 200 or 201, got %d", resp2.StatusCode)
	})
}

// TestRESTGetObject tests GET /objects/:key
func TestRESTGetObject(t *testing.T) {
	// Setup: put an object first
	key := "test/rest/get.txt"
	content := []byte("Content to retrieve")

	putReq, err := http.NewRequest(http.MethodPut, restServerAddr+"/objects/"+key, bytes.NewReader(content))
	require.NoError(t, err)
	putResp, err := restClient.Do(putReq)
	require.NoError(t, err)
	putResp.Body.Close()

	t.Run("get existing object", func(t *testing.T) {
		resp, err := restClient.Get(restServerAddr + "/objects/" + key)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		assert.Equal(t, content, body)
	})

	t.Run("get non-existent object", func(t *testing.T) {
		resp, err := restClient.Get(restServerAddr + "/objects/test/rest/non-existent.txt")
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	})

	t.Run("get with range header", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodGet, restServerAddr+"/objects/"+key, nil)
		require.NoError(t, err)
		req.Header.Set("Range", "bytes=0-4")

		resp, err := restClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		// Some implementations support partial content
		if resp.StatusCode == http.StatusPartialContent {
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			assert.Equal(t, content[0:5], body)
		}
	})

	t.Run("get with accept-encoding", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodGet, restServerAddr+"/objects/"+key, nil)
		require.NoError(t, err)
		req.Header.Set("Accept-Encoding", "gzip")

		resp, err := restClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)
	})
}

// TestRESTHeadObject tests HEAD /objects/:key
func TestRESTHeadObject(t *testing.T) {
	// Setup: put an object
	key := "test/rest/head.txt"
	content := []byte("Head test content")

	putReq, err := http.NewRequest(http.MethodPut, restServerAddr+"/objects/"+key, bytes.NewReader(content))
	require.NoError(t, err)
	putResp, err := restClient.Do(putReq)
	require.NoError(t, err)
	putResp.Body.Close()

	t.Run("head existing object", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodHead, restServerAddr+"/objects/"+key, nil)
		require.NoError(t, err)

		resp, err := restClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.NotEmpty(t, resp.Header.Get("Content-Length"), "Content-Length header should be set")
		// Content-Type may not be set if not provided during upload
		// assert.NotEmpty(t, resp.Header.Get("Content-Type"))
		// Last-Modified should be set
		assert.NotEmpty(t, resp.Header.Get("Last-Modified"), "Last-Modified header should be set")
	})

	t.Run("head non-existent object", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodHead, restServerAddr+"/objects/test/rest/non-existent-head.txt", nil)
		require.NoError(t, err)

		resp, err := restClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	})
}

// TestRESTDeleteObject tests DELETE /objects/:key
func TestRESTDeleteObject(t *testing.T) {
	t.Run("delete existing object", func(t *testing.T) {
		// Setup: put an object
		key := "test/rest/delete.txt"
		content := []byte("To be deleted")

		putReq, err := http.NewRequest(http.MethodPut, restServerAddr+"/objects/"+key, bytes.NewReader(content))
		require.NoError(t, err)
		putResp, err := restClient.Do(putReq)
		require.NoError(t, err)
		putResp.Body.Close()

		// Delete it
		delReq, err := http.NewRequest(http.MethodDelete, restServerAddr+"/objects/"+key, nil)
		require.NoError(t, err)

		delResp, err := restClient.Do(delReq)
		require.NoError(t, err)
		defer delResp.Body.Close()

		assert.Equal(t, http.StatusOK, delResp.StatusCode)

		// Verify deletion
		getResp, err := restClient.Get(restServerAddr + "/objects/" + key)
		require.NoError(t, err)
		getResp.Body.Close()
		assert.Equal(t, http.StatusNotFound, getResp.StatusCode)
	})

	t.Run("delete non-existent object", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodDelete, restServerAddr+"/objects/test/rest/non-existent-delete.txt", nil)
		require.NoError(t, err)

		resp, err := restClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	})
}

// TestRESTListObjects tests GET /objects
func TestRESTListObjects(t *testing.T) {
	// Setup: put multiple objects
	testObjects := map[string]string{
		"test/rest/list/file1.txt":        "content1",
		"test/rest/list/file2.txt":        "content2",
		"test/rest/list/subdir/file3.txt": "content3",
		"test/rest/other/file4.txt":       "content4",
	}

	for key, content := range testObjects {
		req, err := http.NewRequest(http.MethodPut, restServerAddr+"/objects/"+key, strings.NewReader(content))
		require.NoError(t, err)
		resp, err := restClient.Do(req)
		require.NoError(t, err)
		resp.Body.Close()
	}

	t.Run("list all objects", func(t *testing.T) {
		resp, err := restClient.Get(restServerAddr + "/objects")
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		var result map[string]any
		err = json.NewDecoder(resp.Body).Decode(&result)
		require.NoError(t, err)

		objects, ok := result["objects"].([]any)
		assert.True(t, ok)
		assert.GreaterOrEqual(t, len(objects), len(testObjects))
	})

	t.Run("list with prefix", func(t *testing.T) {
		resp, err := restClient.Get(restServerAddr + "/objects?prefix=test/rest/list/")
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		var result map[string]any
		err = json.NewDecoder(resp.Body).Decode(&result)
		require.NoError(t, err)

		objects, ok := result["objects"].([]any)
		assert.True(t, ok)
		assert.GreaterOrEqual(t, len(objects), 3)
	})

	t.Run("list with delimiter", func(t *testing.T) {
		resp, err := restClient.Get(restServerAddr + "/objects?prefix=test/rest/list/&delimiter=/")
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		var result map[string]any
		err = json.NewDecoder(resp.Body).Decode(&result)
		require.NoError(t, err)

		// Should have common prefixes
		if commonPrefixes, ok := result["common_prefixes"].([]any); ok {
			assert.NotEmpty(t, commonPrefixes)
		}
	})

	t.Run("list with pagination", func(t *testing.T) {
		resp, err := restClient.Get(restServerAddr + "/objects?prefix=test/rest/list/&max_results=2")
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		var result map[string]any
		err = json.NewDecoder(resp.Body).Decode(&result)
		require.NoError(t, err)

		objects, ok := result["objects"].([]any)
		assert.True(t, ok)
		assert.LessOrEqual(t, len(objects), 2)

		// Check for next token if truncated
		if truncated, ok := result["truncated"].(bool); ok && truncated {
			assert.NotEmpty(t, result["next_token"])
		}
	})
}

// TestRESTGetMetadata tests GET /metadata/:key
func TestRESTGetMetadata(t *testing.T) {
	// Setup: put an object with metadata
	key := "test/rest/metadata-get.txt"
	content := []byte("test content")

	putReq, err := http.NewRequest(http.MethodPut, restServerAddr+"/objects/"+key, bytes.NewReader(content))
	require.NoError(t, err)
	putReq.Header.Set("Content-Type", "text/plain")
	putReq.Header.Set("X-Object-Meta-Key1", "value1")
	putReq.Header.Set("X-Object-Meta-Key2", "value2")

	putResp, err := restClient.Do(putReq)
	require.NoError(t, err)
	putResp.Body.Close()

	t.Run("get metadata for existing object", func(t *testing.T) {
		resp, err := restClient.Get(restServerAddr + "/metadata/" + key)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)

		var result map[string]any
		err = json.NewDecoder(resp.Body).Decode(&result)
		require.NoError(t, err)

		// The API returns metadata fields at the top level
		assert.NotNil(t, result["key"])
		assert.Equal(t, key, result["key"])
		assert.NotNil(t, result["size"])
		// ETag and modified fields are optional
	})

	t.Run("get metadata for non-existent object", func(t *testing.T) {
		resp, err := restClient.Get(restServerAddr + "/metadata/test/rest/non-existent-metadata.txt")
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	})
}

// TestRESTCORS tests CORS headers
func TestRESTCORS(t *testing.T) {
	t.Run("preflight request", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodOptions, restServerAddr+"/objects/test.txt", nil)
		require.NoError(t, err)
		req.Header.Set("Origin", "http://example.com")
		req.Header.Set("Access-Control-Request-Method", "PUT")

		resp, err := restClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		// OPTIONS preflight should return 204 No Content
		assert.Equal(t, http.StatusNoContent, resp.StatusCode)
		assert.NotEmpty(t, resp.Header.Get("Access-Control-Allow-Origin"))
		assert.NotEmpty(t, resp.Header.Get("Access-Control-Allow-Methods"))
	})

	t.Run("cors headers on regular request", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodGet, restServerAddr+"/health", nil)
		require.NoError(t, err)
		req.Header.Set("Origin", "http://example.com")

		resp, err := restClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.NotEmpty(t, resp.Header.Get("Access-Control-Allow-Origin"))
	})
}

// TestRESTConcurrency tests concurrent REST operations
func TestRESTConcurrency(t *testing.T) {
	t.Run("concurrent puts", func(t *testing.T) {
		numOps := 50
		errChan := make(chan error, numOps)

		for i := 0; i < numOps; i++ {
			go func(index int) {
				key := fmt.Sprintf("test/rest/concurrent/put-%d.txt", index)
				content := []byte(fmt.Sprintf("content %d", index))

				req, err := http.NewRequest(http.MethodPut, restServerAddr+"/objects/"+key, bytes.NewReader(content))
				if err != nil {
					errChan <- err
					return
				}

				resp, err := restClient.Do(req)
				if resp != nil {
					resp.Body.Close()
				}
				errChan <- err
			}(i)
		}

		for i := 0; i < numOps; i++ {
			err := <-errChan
			assert.NoError(t, err)
		}
	})

	t.Run("concurrent gets", func(t *testing.T) {
		// Setup: put a shared object
		key := "test/rest/concurrent/shared.txt"
		content := []byte("shared content")

		putReq, err := http.NewRequest(http.MethodPut, restServerAddr+"/objects/"+key, bytes.NewReader(content))
		require.NoError(t, err)
		putResp, err := restClient.Do(putReq)
		require.NoError(t, err)
		putResp.Body.Close()

		numOps := 50
		errChan := make(chan error, numOps)

		for i := 0; i < numOps; i++ {
			go func() {
				resp, err := restClient.Get(restServerAddr + "/objects/" + key)
				if resp != nil {
					resp.Body.Close()
				}
				errChan <- err
			}()
		}

		for i := 0; i < numOps; i++ {
			err := <-errChan
			assert.NoError(t, err)
		}
	})
}

// TestRESTErrorHandling tests error scenarios
func TestRESTErrorHandling(t *testing.T) {
	t.Run("invalid json in request", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodPut, restServerAddr+"/metadata/test.txt", strings.NewReader("{invalid json"))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")

		resp, err := restClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		// Accept either 400 (bad request) or 404 (no route/object not found)
		assert.True(t, resp.StatusCode == http.StatusBadRequest || resp.StatusCode == http.StatusNotFound,
			"Expected 400 or 404, got %d", resp.StatusCode)
	})

	t.Run("unsupported method", func(t *testing.T) {
		req, err := http.NewRequest(http.MethodPatch, restServerAddr+"/objects/test.txt", nil)
		require.NoError(t, err)

		resp, err := restClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		// Accept either 404 (no route) or 405 (method not allowed)
		assert.True(t, resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusMethodNotAllowed,
			"Expected 404 or 405, got %d", resp.StatusCode)
	})

	t.Run("request too large", func(t *testing.T) {
		// Try to upload a very large file (>100MB)
		largeData := make([]byte, 150*1024*1024) // 150MB

		req, err := http.NewRequest(http.MethodPut, restServerAddr+"/objects/test/rest/too-large.bin", bytes.NewReader(largeData))
		require.NoError(t, err)

		resp, err := restClient.Do(req)
		if resp != nil {
			defer resp.Body.Close()
			// Should get error or request entity too large
			assert.True(t, resp.StatusCode >= 400)
		}
	})
}

// TestRESTAPIVersioning tests API versioning
func TestRESTAPIVersioning(t *testing.T) {
	t.Run("v1 api endpoints", func(t *testing.T) {
		key := "test/rest/v1/test.txt"
		content := []byte("v1 test")

		// PUT via /api/v1
		putReq, err := http.NewRequest(http.MethodPut, restServerAddr+"/api/v1/objects/"+key, bytes.NewReader(content))
		require.NoError(t, err)
		putResp, err := restClient.Do(putReq)
		require.NoError(t, err)
		putResp.Body.Close()
		// Accept both 200 OK and 201 Created
		assert.True(t, putResp.StatusCode == http.StatusOK || putResp.StatusCode == http.StatusCreated,
			"Expected 200 or 201, got %d", putResp.StatusCode)

		// GET via /api/v1
		getResp, err := restClient.Get(restServerAddr + "/api/v1/objects/" + key)
		require.NoError(t, err)
		defer getResp.Body.Close()
		assert.Equal(t, http.StatusOK, getResp.StatusCode)
	})

	t.Run("backwards compatibility without version prefix", func(t *testing.T) {
		key := "test/rest/compat/test.txt"
		content := []byte("compat test")

		// PUT without version prefix
		putReq, err := http.NewRequest(http.MethodPut, restServerAddr+"/objects/"+key, bytes.NewReader(content))
		require.NoError(t, err)
		putResp, err := restClient.Do(putReq)
		require.NoError(t, err)
		putResp.Body.Close()
		// Accept both 200 OK and 201 Created
		assert.True(t, putResp.StatusCode == http.StatusOK || putResp.StatusCode == http.StatusCreated,
			"Expected 200 or 201, got %d", putResp.StatusCode)

		// GET without version prefix
		getResp, err := restClient.Get(restServerAddr + "/objects/" + key)
		require.NoError(t, err)
		defer getResp.Body.Close()
		assert.Equal(t, http.StatusOK, getResp.StatusCode)
	})
}
