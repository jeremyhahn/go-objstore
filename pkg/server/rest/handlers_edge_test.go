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
	"errors"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// ErrorMockStorage is a mock storage that returns errors for testing error paths
type ErrorMockStorage struct {
	*MockStorage
	putErr      error
	getErr      error
	deleteErr   error
	existsErr   error
	metadataErr error
}

func (e *ErrorMockStorage) PutWithMetadata(ctx context.Context, key string, data io.Reader, metadata *common.Metadata) error {
	if e.putErr != nil {
		return e.putErr
	}
	return e.MockStorage.PutWithMetadata(ctx, key, data, metadata)
}

func (e *ErrorMockStorage) GetMetadata(ctx context.Context, key string) (*common.Metadata, error) {
	if e.metadataErr != nil {
		return nil, e.metadataErr
	}
	return e.MockStorage.GetMetadata(ctx, key)
}

func (e *ErrorMockStorage) GetWithContext(ctx context.Context, key string) (io.ReadCloser, error) {
	if e.getErr != nil {
		return nil, e.getErr
	}
	return e.MockStorage.GetWithContext(ctx, key)
}

func (e *ErrorMockStorage) DeleteWithContext(ctx context.Context, key string) error {
	if e.deleteErr != nil {
		return e.deleteErr
	}
	return e.MockStorage.DeleteWithContext(ctx, key)
}

func (e *ErrorMockStorage) Exists(ctx context.Context, key string) (bool, error) {
	if e.existsErr != nil {
		return false, e.existsErr
	}
	return e.MockStorage.Exists(ctx, key)
}

func (e *ErrorMockStorage) UpdateMetadata(ctx context.Context, key string, metadata *common.Metadata) error {
	if e.metadataErr != nil {
		return e.metadataErr
	}
	return e.MockStorage.UpdateMetadata(ctx, key, metadata)
}

func TestPutObjectStorageError(t *testing.T) {
	storage := &ErrorMockStorage{
		MockStorage: NewMockStorage(),
		putErr:      errors.New("storage error"),
	}
	handler := NewHandler(storage)

	router := gin.New()
	router.PUT("/objects/*key", handler.PutObject)

	req := httptest.NewRequest("PUT", "/objects/test.txt", strings.NewReader("content"))
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("PutObject() with storage error status = %v, want %v", w.Code, http.StatusInternalServerError)
	}
}

func TestGetObjectMetadataError(t *testing.T) {
	storage := &ErrorMockStorage{
		MockStorage: NewMockStorage(),
		metadataErr: errors.New("metadata error"),
	}
	handler := NewHandler(storage)

	router := gin.New()
	router.GET("/objects/*key", handler.GetObject)

	req := httptest.NewRequest("GET", "/objects/test.txt", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("GetObject() with metadata error status = %v, want %v", w.Code, http.StatusNotFound)
	}
}

func TestDeleteObjectExistsError(t *testing.T) {
	storage := &ErrorMockStorage{
		MockStorage: NewMockStorage(),
		existsErr:   errors.New("exists check error"),
	}
	handler := NewHandler(storage)

	router := gin.New()
	router.DELETE("/objects/*key", handler.DeleteObject)

	req := httptest.NewRequest("DELETE", "/objects/test.txt", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("DeleteObject() with exists error status = %v, want %v", w.Code, http.StatusInternalServerError)
	}
}

func TestDeleteObjectDeleteError(t *testing.T) {
	storage := &ErrorMockStorage{
		MockStorage: NewMockStorage(),
		deleteErr:   errors.New("delete error"),
	}
	storage.PutWithContext(context.Background(), "test.txt", strings.NewReader("content"))

	handler := NewHandler(storage)

	router := gin.New()
	router.DELETE("/objects/*key", handler.DeleteObject)

	req := httptest.NewRequest("DELETE", "/objects/test.txt", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("DeleteObject() with delete error status = %v, want %v", w.Code, http.StatusInternalServerError)
	}
}

func TestHeadObjectExistsError(t *testing.T) {
	storage := &ErrorMockStorage{
		MockStorage: NewMockStorage(),
		existsErr:   errors.New("exists error"),
	}
	handler := NewHandler(storage)

	router := gin.New()
	router.HEAD("/objects/*key", handler.HeadObject)

	req := httptest.NewRequest("HEAD", "/objects/test.txt", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("HeadObject() with exists error status = %v, want %v", w.Code, http.StatusInternalServerError)
	}
}

func TestListObjectsError(t *testing.T) {
	storage := NewMockStorage()
	storage.listFunc = func(ctx context.Context, opts *common.ListOptions) (*common.ListResult, error) {
		return nil, errors.New("list error")
	}

	handler := NewHandler(storage)

	router := gin.New()
	router.GET("/objects", handler.ListObjects)

	req := httptest.NewRequest("GET", "/objects", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("ListObjects() with list error status = %v, want %v", w.Code, http.StatusInternalServerError)
	}
}

func TestListObjectsNegativeLimit(t *testing.T) {
	storage := NewMockStorage()
	handler := NewHandler(storage)

	router := gin.New()
	router.GET("/objects", handler.ListObjects)

	req := httptest.NewRequest("GET", "/objects?limit=-1", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("ListObjects() with negative limit status = %v, want %v", w.Code, http.StatusBadRequest)
	}
}

func TestListObjectsLargeLimit(t *testing.T) {
	storage := NewMockStorage()
	// Add some test objects
	for i := 0; i < 5; i++ {
		key := string(rune('a' + i))
		storage.PutWithContext(context.Background(), key, strings.NewReader("content"))
	}

	handler := NewHandler(storage)

	router := gin.New()
	router.GET("/objects", handler.ListObjects)

	// Request with limit over 1000 (should be capped at 1000)
	req := httptest.NewRequest("GET", "/objects?limit=5000", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("ListObjects() with large limit status = %v, want %v", w.Code, http.StatusOK)
	}
}

func TestGetObjectMetadataHandler(t *testing.T) {
	storage := &ErrorMockStorage{
		MockStorage: NewMockStorage(),
		metadataErr: errors.New("metadata error"),
	}

	handler := NewHandler(storage)

	router := gin.New()
	router.GET("/metadata/:key", handler.GetObjectMetadata)

	req := httptest.NewRequest("GET", "/metadata/test.txt", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("GetObjectMetadata() with error status = %v, want %v", w.Code, http.StatusNotFound)
	}
}

func TestUpdateObjectMetadataExistsError(t *testing.T) {
	storage := &ErrorMockStorage{
		MockStorage: NewMockStorage(),
		existsErr:   errors.New("exists error"),
	}

	handler := NewHandler(storage)

	router := gin.New()
	router.PUT("/metadata/:key", handler.UpdateObjectMetadata)

	metadata := &common.Metadata{ContentType: "text/plain"}
	body, _ := json.Marshal(metadata)

	req := httptest.NewRequest("PUT", "/metadata/test.txt", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("UpdateObjectMetadata() with exists error status = %v, want %v", w.Code, http.StatusInternalServerError)
	}
}

func TestUpdateObjectMetadataUpdateError(t *testing.T) {
	storage := &ErrorMockStorage{
		MockStorage: NewMockStorage(),
		metadataErr: errors.New("update error"),
	}
	storage.PutWithContext(context.Background(), "test.txt", strings.NewReader("content"))

	handler := NewHandler(storage)

	router := gin.New()
	router.PUT("/metadata/:key", handler.UpdateObjectMetadata)

	metadata := &common.Metadata{ContentType: "text/plain"}
	body, _ := json.Marshal(metadata)

	req := httptest.NewRequest("PUT", "/metadata/test.txt", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("UpdateObjectMetadata() with update error status = %v, want %v", w.Code, http.StatusInternalServerError)
	}
}

func TestRespondWithListObjectsWithPrefixes(t *testing.T) {
	result := &common.ListResult{
		Objects: []*common.ObjectInfo{
			{
				Key: "file1.txt",
				Metadata: &common.Metadata{
					Size: 100,
				},
			},
		},
		CommonPrefixes: []string{"dir1/", "dir2/"},
		NextToken:      "next",
		Truncated:      true,
	}

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)

	RespondWithListObjects(c, result)

	if w.Code != http.StatusOK {
		t.Errorf("RespondWithListObjects() status = %v, want %v", w.Code, http.StatusOK)
	}

	body := w.Body.String()
	if !contains(body, "dir1/") || !contains(body, "dir2/") {
		t.Error("RespondWithListObjects() should contain common prefixes")
	}
}

func TestPutObjectMultipartNoMetadata(t *testing.T) {
	storage := NewMockStorage()
	handler := NewHandler(storage)

	router := gin.New()
	router.PUT("/objects/*key", handler.PutObject)

	// Create multipart form without metadata field
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
		t.Errorf("PutObject() multipart without metadata status = %v, want %v", w.Code, http.StatusCreated)
	}
}

func TestGetObjectNoContentType(t *testing.T) {
	storage := NewMockStorage()
	handler := NewHandler(storage)

	// Add test object without content type
	storage.PutWithMetadata(context.Background(), "test.txt", strings.NewReader("test content"), &common.Metadata{
		Size: 12,
	})

	router := gin.New()
	router.GET("/objects/*key", handler.GetObject)

	req := httptest.NewRequest("GET", "/objects/test.txt", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("GetObject() status = %v, want %v", w.Code, http.StatusOK)
	}

	// Should default to application/octet-stream
	if w.Header().Get("Content-Type") != "application/octet-stream" {
		t.Errorf("GetObject() Content-Type = %v, want application/octet-stream", w.Header().Get("Content-Type"))
	}
}

func TestGetObjectNoSize(t *testing.T) {
	storage := NewMockStorage()
	handler := NewHandler(storage)

	// Add test object without size
	storage.PutWithMetadata(context.Background(), "test.txt", strings.NewReader("test content"), &common.Metadata{
		ContentType: "text/plain",
		Size:        0, // No size
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

func TestGetObjectWithContentEncoding(t *testing.T) {
	storage := NewMockStorage()
	handler := NewHandler(storage)

	// Add test object with content encoding
	storage.PutWithMetadata(context.Background(), "test.txt", strings.NewReader("test content"), &common.Metadata{
		ContentType:     "text/plain",
		ContentEncoding: "gzip",
		Size:            12,
	})

	router := gin.New()
	router.GET("/objects/*key", handler.GetObject)

	req := httptest.NewRequest("GET", "/objects/test.txt", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("GetObject() status = %v, want %v", w.Code, http.StatusOK)
	}

	if w.Header().Get("Content-Encoding") != "gzip" {
		t.Errorf("GetObject() Content-Encoding = %v, want gzip", w.Header().Get("Content-Encoding"))
	}
}

func TestHeadObjectWithMetadata(t *testing.T) {
	storage := NewMockStorage()
	handler := NewHandler(storage)

	// Add test object with full metadata
	storage.PutWithMetadata(context.Background(), "test.txt", strings.NewReader("test content"), &common.Metadata{
		ContentType: "text/plain",
		Size:        12,
		ETag:        "abc123",
	})

	router := gin.New()
	router.HEAD("/objects/*key", handler.HeadObject)

	req := httptest.NewRequest("HEAD", "/objects/test.txt", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("HeadObject() status = %v, want %v", w.Code, http.StatusOK)
	}

	// Mock storage sets ETag to mock-etag
	if w.Header().Get("ETag") == "" {
		t.Error("HeadObject() should have ETag header")
	}

	if w.Header().Get("Content-Length") != "12" {
		t.Errorf("HeadObject() Content-Length = %v, want 12", w.Header().Get("Content-Length"))
	}
}

func TestPutObjectNoBody(t *testing.T) {
	storage := NewMockStorage()
	handler := NewHandler(storage)

	router := gin.New()
	router.PUT("/objects/*key", handler.PutObject)

	// Test with empty body (direct upload)
	req := httptest.NewRequest("PUT", "/objects/empty.txt", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("PutObject() with empty body status = %v, want %v", w.Code, http.StatusCreated)
	}
}

func TestGetObjectWithAllHeaders(t *testing.T) {
	storage := NewMockStorage()
	handler := NewHandler(storage)

	// Add test object with all metadata fields
	now := time.Now()
	storage.PutWithMetadata(context.Background(), "test.txt", strings.NewReader("test content"), &common.Metadata{
		ContentType:     "text/plain",
		ContentEncoding: "gzip",
		Size:            12,
		ETag:            "test-etag",
		LastModified:    now,
	})

	router := gin.New()
	router.GET("/objects/*key", handler.GetObject)

	req := httptest.NewRequest("GET", "/objects/test.txt", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("GetObject() status = %v, want %v", w.Code, http.StatusOK)
	}

	// Check all headers are set
	if w.Header().Get("Content-Type") == "" {
		t.Error("GetObject() should have Content-Type header")
	}
	if w.Header().Get("Content-Encoding") == "" {
		t.Error("GetObject() should have Content-Encoding header")
	}
	if w.Header().Get("ETag") == "" {
		t.Error("GetObject() should have ETag header")
	}
	if w.Header().Get("Last-Modified") == "" {
		t.Error("GetObject() should have Last-Modified header")
	}
	if w.Header().Get("Content-Length") == "" {
		t.Error("GetObject() should have Content-Length header")
	}
}
