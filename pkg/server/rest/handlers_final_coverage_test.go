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

package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// Test PutObject with multipart form check (checking c.Request.MultipartForm != nil path)
func TestPutObjectMultipartFormCheck(t *testing.T) {
	storage := NewMockStorage()
	handler := NewHandler(storage)

	router := gin.New()
	router.PUT("/objects/*key", handler.PutObject)

	// Create multipart request
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, _ := writer.CreateFormFile("file", "test.txt")
	part.Write([]byte("test content"))
	contentType := writer.FormDataContentType()
	writer.Close()

	req := httptest.NewRequest("PUT", "/objects/test.txt", body)
	req.Header.Set("Content-Type", contentType)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("PutObject() with multipart status = %v, want %v", w.Code, http.StatusCreated)
	}
}

// Test PutObject with empty metadata (no X-Metadata header)
func TestPutObjectNoMetadataHeader(t *testing.T) {
	storage := NewMockStorage()
	handler := NewHandler(storage)

	router := gin.New()
	router.PUT("/objects/*key", handler.PutObject)

	req := httptest.NewRequest("PUT", "/objects/test.txt", strings.NewReader("content"))
	req.Header.Set("Content-Type", "application/octet-stream")
	// No X-Metadata header
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("PutObject() without metadata header status = %v, want %v", w.Code, http.StatusCreated)
	}
}

// Test PutObject with content type from direct upload
func TestPutObjectDirectUploadContentType(t *testing.T) {
	storage := NewMockStorage()
	handler := NewHandler(storage)

	router := gin.New()
	router.PUT("/objects/*key", handler.PutObject)

	req := httptest.NewRequest("PUT", "/objects/test.json", strings.NewReader(`{"key":"value"}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("PutObject() with JSON content type status = %v, want %v", w.Code, http.StatusCreated)
	}
}

// Test GetObject with zero-value LastModified
func TestGetObjectZeroLastModified(t *testing.T) {
	storage := NewMockStorage()
	handler := NewHandler(storage)

	// Add object with zero LastModified
	storage.PutWithMetadata(context.Background(), "test.txt", strings.NewReader("content"), &common.Metadata{
		ContentType: "text/plain",
		Size:        7,
		// LastModified is zero value
	})

	router := gin.New()
	router.GET("/objects/*key", handler.GetObject)

	req := httptest.NewRequest("GET", "/objects/test.txt", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("GetObject() with zero LastModified status = %v, want %v", w.Code, http.StatusOK)
	}

	// Should not have Last-Modified header
	if w.Header().Get("Last-Modified") != "" {
		t.Log("GetObject() should not set Last-Modified for zero time")
	}
}

// Test GetObject with zero size
func TestGetObjectZeroSize(t *testing.T) {
	storage := NewMockStorage()
	handler := NewHandler(storage)

	// Add object with zero size
	storage.PutWithMetadata(context.Background(), "test.txt", strings.NewReader(""), &common.Metadata{
		ContentType: "text/plain",
		Size:        0,
	})

	router := gin.New()
	router.GET("/objects/*key", handler.GetObject)

	req := httptest.NewRequest("GET", "/objects/test.txt", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("GetObject() with zero size status = %v, want %v", w.Code, http.StatusOK)
	}
}

// Test GetObject with empty ETag
func TestGetObjectEmptyETag(t *testing.T) {
	storage := NewMockStorage()
	handler := NewHandler(storage)

	// Add object with empty ETag
	storage.PutWithMetadata(context.Background(), "test.txt", strings.NewReader("content"), &common.Metadata{
		ContentType: "text/plain",
		Size:        7,
		ETag:        "", // Empty ETag
	})

	router := gin.New()
	router.GET("/objects/*key", handler.GetObject)

	req := httptest.NewRequest("GET", "/objects/test.txt", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("GetObject() with empty ETag status = %v, want %v", w.Code, http.StatusOK)
	}
}

// Test GetObject with empty ContentEncoding
func TestGetObjectEmptyContentEncoding(t *testing.T) {
	storage := NewMockStorage()
	handler := NewHandler(storage)

	// Add object with empty ContentEncoding
	storage.PutWithMetadata(context.Background(), "test.txt", strings.NewReader("content"), &common.Metadata{
		ContentType:     "text/plain",
		Size:            7,
		ContentEncoding: "", // Empty
	})

	router := gin.New()
	router.GET("/objects/*key", handler.GetObject)

	req := httptest.NewRequest("GET", "/objects/test.txt", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("GetObject() status = %v, want %v", w.Code, http.StatusOK)
	}
}

// Test GetObjectMetadata with empty key (no param)
func TestGetObjectMetadataEmptyKey(t *testing.T) {
	storage := NewMockStorage()
	handler := NewHandler(storage)

	router := gin.New()
	// Route without :key param - will cause empty key
	router.GET("/metadata/", handler.GetObjectMetadata)

	req := httptest.NewRequest("GET", "/metadata/", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("GetObjectMetadata() with empty key status = %v, want %v", w.Code, http.StatusBadRequest)
	}
}

// Test UpdateObjectMetadata with empty key
func TestUpdateObjectMetadataEmptyKey(t *testing.T) {
	storage := NewMockStorage()
	handler := NewHandler(storage)

	router := gin.New()
	router.PUT("/metadata/", handler.UpdateObjectMetadata)

	metadata := &common.Metadata{ContentType: "text/plain"}
	body, _ := json.Marshal(metadata)

	req := httptest.NewRequest("PUT", "/metadata/", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("UpdateObjectMetadata() with empty key status = %v, want %v", w.Code, http.StatusBadRequest)
	}
}

// Test UpdateObjectMetadata with zero Custom metadata
func TestUpdateObjectMetadataNoCustom(t *testing.T) {
	storage := NewMockStorage()
	storage.PutWithContext(context.Background(), "test.txt", strings.NewReader("content"))

	handler := NewHandler(storage)

	router := gin.New()
	router.PUT("/metadata/:key", handler.UpdateObjectMetadata)

	metadata := &common.Metadata{
		ContentType: "text/plain",
		// No Custom field
	}
	body, _ := json.Marshal(metadata)

	req := httptest.NewRequest("PUT", "/metadata/test.txt", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("UpdateObjectMetadata() without custom metadata status = %v, want %v", w.Code, http.StatusOK)
	}
}

// Test RespondWithListObjects with objects that have custom metadata
func TestRespondWithListObjectsWithCustomMetadata(t *testing.T) {
	now := time.Now()
	result := &common.ListResult{
		Objects: []*common.ObjectInfo{
			{
				Key: "obj1",
				Metadata: &common.Metadata{
					Size:         100,
					ETag:         "etag1",
					LastModified: now,
					Custom: map[string]string{
						"key1": "value1",
					},
				},
			},
		},
		Truncated: false,
	}

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)

	RespondWithListObjects(c, result)

	if w.Code != http.StatusOK {
		t.Errorf("RespondWithListObjects() status = %v, want %v", w.Code, http.StatusOK)
	}

	var response ListObjectsResponse
	json.Unmarshal(w.Body.Bytes(), &response)
	if len(response.Objects) > 0 {
		if response.Objects[0].Modified == "" {
			t.Error("RespondWithListObjects() should include modified time")
		}
		if len(response.Objects[0].Metadata) == 0 {
			t.Error("RespondWithListObjects() should include custom metadata")
		}
	}
}

// Test RespondWithListObjects with empty custom metadata map
func TestRespondWithListObjectsEmptyCustomMetadata(t *testing.T) {
	result := &common.ListResult{
		Objects: []*common.ObjectInfo{
			{
				Key: "obj1",
				Metadata: &common.Metadata{
					Size:   100,
					ETag:   "etag1",
					Custom: map[string]string{}, // Empty map
				},
			},
		},
		Truncated: false,
	}

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)

	RespondWithListObjects(c, result)

	if w.Code != http.StatusOK {
		t.Errorf("RespondWithListObjects() status = %v, want %v", w.Code, http.StatusOK)
	}
}

// Test PutObject with multipart form and Content-Type header check
func TestPutObjectMultipartContentTypeCheck(t *testing.T) {
	storage := NewMockStorage()
	handler := NewHandler(storage)

	router := gin.New()
	router.PUT("/objects/*key", handler.PutObject)

	// Create a multipart form
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, _ := writer.CreateFormFile("file", "document.pdf")
	part.Write([]byte("PDF content"))
	contentType := writer.FormDataContentType()
	writer.Close()

	req := httptest.NewRequest("PUT", "/objects/document.pdf", body)
	req.Header.Set("Content-Type", contentType)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("PutObject() multipart status = %v, want %v", w.Code, http.StatusCreated)
	}
}

// Test PutObject with empty body
func TestPutObjectEmptyBodyContentType(t *testing.T) {
	storage := NewMockStorage()
	handler := NewHandler(storage)

	router := gin.New()
	router.PUT("/objects/*key", handler.PutObject)

	req := httptest.NewRequest("PUT", "/objects/empty.bin", bytes.NewReader([]byte{}))
	req.Header.Set("Content-Type", "application/octet-stream")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("PutObject() with empty body status = %v, want %v", w.Code, http.StatusCreated)
	}
}
