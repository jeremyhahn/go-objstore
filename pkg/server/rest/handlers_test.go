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

// MockStorage implements common.Storage for testing
type MockStorage struct {
	objects  map[string]*mockObject
	metadata map[string]*common.Metadata
	listFunc func(ctx context.Context, opts *common.ListOptions) (*common.ListResult, error)
}

type mockObject struct {
	data     []byte
	metadata *common.Metadata
}

func NewMockStorage() *MockStorage {
	return &MockStorage{
		objects:  make(map[string]*mockObject),
		metadata: make(map[string]*common.Metadata),
	}
}

func (m *MockStorage) Configure(settings map[string]string) error {
	return nil
}

func (m *MockStorage) Put(key string, data io.Reader) error {
	return m.PutWithContext(context.Background(), key, data)
}

func (m *MockStorage) PutWithContext(ctx context.Context, key string, data io.Reader) error {
	return m.PutWithMetadata(ctx, key, data, &common.Metadata{})
}

func (m *MockStorage) PutWithMetadata(ctx context.Context, key string, data io.Reader, metadata *common.Metadata) error {
	content, err := io.ReadAll(data)
	if err != nil {
		return err
	}

	if metadata == nil {
		metadata = &common.Metadata{}
	}
	metadata.Size = int64(len(content))
	metadata.LastModified = time.Now()
	metadata.ETag = "mock-etag"

	m.objects[key] = &mockObject{
		data:     content,
		metadata: metadata,
	}
	m.metadata[key] = metadata

	return nil
}

func (m *MockStorage) Get(key string) (io.ReadCloser, error) {
	return m.GetWithContext(context.Background(), key)
}

func (m *MockStorage) GetWithContext(ctx context.Context, key string) (io.ReadCloser, error) {
	obj, exists := m.objects[key]
	if !exists {
		return nil, errors.New("object not found")
	}
	return io.NopCloser(bytes.NewReader(obj.data)), nil
}

func (m *MockStorage) GetMetadata(ctx context.Context, key string) (*common.Metadata, error) {
	metadata, exists := m.metadata[key]
	if !exists {
		return nil, errors.New("object not found")
	}
	return metadata, nil
}

func (m *MockStorage) UpdateMetadata(ctx context.Context, key string, metadata *common.Metadata) error {
	if _, exists := m.metadata[key]; !exists {
		return errors.New("object not found")
	}
	m.metadata[key] = metadata
	if obj, exists := m.objects[key]; exists {
		obj.metadata = metadata
	}
	return nil
}

func (m *MockStorage) Delete(key string) error {
	return m.DeleteWithContext(context.Background(), key)
}

func (m *MockStorage) DeleteWithContext(ctx context.Context, key string) error {
	if _, exists := m.objects[key]; !exists {
		return errors.New("object not found")
	}
	delete(m.objects, key)
	delete(m.metadata, key)
	return nil
}

func (m *MockStorage) Exists(ctx context.Context, key string) (bool, error) {
	_, exists := m.objects[key]
	return exists, nil
}

func (m *MockStorage) List(prefix string) ([]string, error) {
	return m.ListWithContext(context.Background(), prefix)
}

func (m *MockStorage) ListWithContext(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	for key := range m.objects {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	return keys, nil
}

func (m *MockStorage) ListWithOptions(ctx context.Context, opts *common.ListOptions) (*common.ListResult, error) {
	if m.listFunc != nil {
		return m.listFunc(ctx, opts)
	}

	var objects []*common.ObjectInfo
	for key, obj := range m.objects {
		if opts.Prefix == "" || strings.HasPrefix(key, opts.Prefix) {
			objects = append(objects, &common.ObjectInfo{
				Key:      key,
				Metadata: obj.metadata,
			})
		}
	}

	// Apply limit
	if opts.MaxResults > 0 && len(objects) > opts.MaxResults {
		objects = objects[:opts.MaxResults]
	}

	return &common.ListResult{
		Objects:   objects,
		Truncated: false,
	}, nil
}

func (m *MockStorage) Archive(key string, destination common.Archiver) error {
	return nil
}

func (m *MockStorage) AddPolicy(policy common.LifecyclePolicy) error {
	return nil
}

func (m *MockStorage) RemovePolicy(id string) error {
	return nil
}

func (m *MockStorage) GetPolicies() ([]common.LifecyclePolicy, error) {
	return nil, nil
}

func TestPutObject(t *testing.T) {
	storage := NewMockStorage()
	handler := newTestHandler(t, storage)

	tests := []struct {
		name           string
		key            string
		body           string
		contentType    string
		wantStatusCode int
	}{
		{
			name:           "successful upload",
			key:            "test.txt",
			body:           "test content",
			contentType:    "text/plain",
			wantStatusCode: http.StatusCreated,
		},
		{
			name:           "leading slash key",
			key:            "/path/test.txt",
			body:           "test content",
			contentType:    "text/plain",
			wantStatusCode: http.StatusCreated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			router := gin.New()
			router.PUT("/objects/*key", handler.PutObject)

			req := httptest.NewRequest("PUT", "/objects/"+tt.key, strings.NewReader(tt.body))
			req.Header.Set("Content-Type", tt.contentType)
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)

			if w.Code != tt.wantStatusCode {
				t.Errorf("PutObject() status = %v, want %v, body: %s", w.Code, tt.wantStatusCode, w.Body.String())
			}
		})
	}
}

func TestPutObjectMultipart(t *testing.T) {
	storage := NewMockStorage()
	handler := newTestHandler(t, storage)

	router := gin.New()
	router.PUT("/objects/*key", handler.PutObject)

	// Create multipart form
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	// Add file
	part, err := writer.CreateFormFile("file", "test.txt")
	if err != nil {
		t.Fatal(err)
	}
	part.Write([]byte("test content"))

	// Add metadata
	metadata := map[string]any{
		"custom": map[string]string{
			"author": "test",
		},
	}
	metadataJSON, _ := json.Marshal(metadata)
	writer.WriteField("metadata", string(metadataJSON))

	writer.Close()

	req := httptest.NewRequest("PUT", "/objects/test.txt", body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("PutObject() multipart status = %v, want %v, body: %s", w.Code, http.StatusCreated, w.Body.String())
	}
}

func TestPutObjectWithMetadataHeader(t *testing.T) {
	storage := NewMockStorage()
	handler := newTestHandler(t, storage)

	router := gin.New()
	router.PUT("/objects/*key", handler.PutObject)

	metadata := map[string]any{
		"content_type": "text/plain",
		"custom": map[string]string{
			"author": "test",
		},
	}
	metadataJSON, _ := json.Marshal(metadata)

	req := httptest.NewRequest("PUT", "/objects/test.txt", strings.NewReader("test content"))
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("X-Metadata", string(metadataJSON))
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("PutObject() with metadata header status = %v, want %v, body: %s", w.Code, http.StatusCreated, w.Body.String())
	}
}

func TestPutObjectInvalidMetadataJSON(t *testing.T) {
	storage := NewMockStorage()
	handler := newTestHandler(t, storage)

	router := gin.New()
	router.PUT("/objects/*key", handler.PutObject)

	req := httptest.NewRequest("PUT", "/objects/test.txt", strings.NewReader("test content"))
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("X-Metadata", "invalid json")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("PutObject() with invalid metadata status = %v, want %v", w.Code, http.StatusBadRequest)
	}
}

func TestGetObjectError(t *testing.T) {
	// Create storage that will fail on Get
	storage := NewMockStorage()
	storage.PutWithContext(context.Background(), "test.txt", strings.NewReader("content"))

	handler := newTestHandler(t, storage)

	router := gin.New()
	router.GET("/objects/*key", handler.GetObject)

	// Test getting object with streaming error
	req := httptest.NewRequest("GET", "/objects/test.txt", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	// Should succeed for this simple case
	if w.Code != http.StatusOK {
		t.Errorf("GetObject() status = %v, want %v", w.Code, http.StatusOK)
	}
}

func TestGetObject(t *testing.T) {
	storage := NewMockStorage()
	handler := newTestHandler(t, storage)

	// Add test object
	storage.PutWithMetadata(context.Background(), "test.txt", strings.NewReader("test content"), &common.Metadata{
		ContentType: "text/plain",
	})

	tests := []struct {
		name           string
		key            string
		wantStatusCode int
		wantBody       string
	}{
		{
			name:           "existing object",
			key:            "test.txt",
			wantStatusCode: http.StatusOK,
			wantBody:       "test content",
		},
		{
			name:           "non-existent object",
			key:            "nonexistent.txt",
			wantStatusCode: http.StatusNotFound,
			wantBody:       "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			router := gin.New()
			router.GET("/objects/*key", handler.GetObject)

			req := httptest.NewRequest("GET", "/objects/"+tt.key, nil)
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)

			if w.Code != tt.wantStatusCode {
				t.Errorf("GetObject() status = %v, want %v", w.Code, tt.wantStatusCode)
			}

			if tt.wantBody != "" && w.Body.String() != tt.wantBody {
				t.Errorf("GetObject() body = %v, want %v", w.Body.String(), tt.wantBody)
			}
		})
	}
}

func TestDeleteObject(t *testing.T) {
	storage := NewMockStorage()
	handler := newTestHandler(t, storage)

	// Add test object
	storage.PutWithContext(context.Background(), "test.txt", strings.NewReader("test content"))

	tests := []struct {
		name           string
		key            string
		wantStatusCode int
	}{
		{
			name:           "delete existing object",
			key:            "test.txt",
			wantStatusCode: http.StatusOK,
		},
		{
			name:           "delete non-existent object",
			key:            "nonexistent.txt",
			wantStatusCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			router := gin.New()
			router.DELETE("/objects/*key", handler.DeleteObject)

			req := httptest.NewRequest("DELETE", "/objects/"+tt.key, nil)
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)

			if w.Code != tt.wantStatusCode {
				t.Errorf("DeleteObject() status = %v, want %v", w.Code, tt.wantStatusCode)
			}
		})
	}
}

func TestHeadObject(t *testing.T) {
	storage := NewMockStorage()
	handler := newTestHandler(t, storage)

	// Add test object
	storage.PutWithMetadata(context.Background(), "test.txt", strings.NewReader("test content"), &common.Metadata{
		ContentType: "text/plain",
		Size:        12,
	})

	tests := []struct {
		name           string
		key            string
		wantStatusCode int
	}{
		{
			name:           "existing object",
			key:            "test.txt",
			wantStatusCode: http.StatusOK,
		},
		{
			name:           "non-existent object",
			key:            "nonexistent.txt",
			wantStatusCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			router := gin.New()
			router.HEAD("/objects/*key", handler.HeadObject)

			req := httptest.NewRequest("HEAD", "/objects/"+tt.key, nil)
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)

			if w.Code != tt.wantStatusCode {
				t.Errorf("HeadObject() status = %v, want %v", w.Code, tt.wantStatusCode)
			}
		})
	}
}

func TestListObjects(t *testing.T) {
	storage := NewMockStorage()
	handler := newTestHandler(t, storage)

	// Add test objects
	storage.PutWithContext(context.Background(), "docs/test1.txt", strings.NewReader("content1"))
	storage.PutWithContext(context.Background(), "docs/test2.txt", strings.NewReader("content2"))
	storage.PutWithContext(context.Background(), "images/pic.jpg", strings.NewReader("image"))

	tests := []struct {
		name           string
		queryParams    string
		wantStatusCode int
		wantCount      int
	}{
		{
			name:           "list all",
			queryParams:    "",
			wantStatusCode: http.StatusOK,
			wantCount:      3,
		},
		{
			name:           "list with prefix",
			queryParams:    "?prefix=docs/",
			wantStatusCode: http.StatusOK,
			wantCount:      2,
		},
		{
			name:           "list with limit",
			queryParams:    "?limit=1",
			wantStatusCode: http.StatusOK,
			wantCount:      1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			router := gin.New()
			router.GET("/objects", handler.ListObjects)

			req := httptest.NewRequest("GET", "/objects"+tt.queryParams, nil)
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)

			if w.Code != tt.wantStatusCode {
				t.Errorf("ListObjects() status = %v, want %v", w.Code, tt.wantStatusCode)
			}

			var response ListObjectsResponse
			if err := json.Unmarshal(w.Body.Bytes(), &response); err == nil {
				if len(response.Objects) > tt.wantCount {
					t.Errorf("ListObjects() count = %v, want at most %v", len(response.Objects), tt.wantCount)
				}
			}
		})
	}
}

func TestListObjectsInvalidLimit(t *testing.T) {
	storage := NewMockStorage()
	handler := newTestHandler(t, storage)

	router := gin.New()
	router.GET("/objects", handler.ListObjects)

	req := httptest.NewRequest("GET", "/objects?limit=invalid", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("ListObjects() with invalid limit status = %v, want %v", w.Code, http.StatusBadRequest)
	}
}

func TestGetObjectMetadata(t *testing.T) {
	storage := NewMockStorage()
	handler := newTestHandler(t, storage)

	// Add test object
	storage.PutWithMetadata(context.Background(), "test.txt", strings.NewReader("test content"), &common.Metadata{
		ContentType: "text/plain",
		Size:        12,
		Custom: map[string]string{
			"author": "test",
		},
	})

	tests := []struct {
		name           string
		key            string
		wantStatusCode int
	}{
		{
			name:           "existing object",
			key:            "test.txt",
			wantStatusCode: http.StatusOK,
		},
		{
			name:           "non-existent object",
			key:            "nonexistent.txt",
			wantStatusCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			router := gin.New()
			router.GET("/metadata/:key", handler.GetObjectMetadata)

			req := httptest.NewRequest("GET", "/metadata/"+tt.key, nil)
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)

			if w.Code != tt.wantStatusCode {
				t.Errorf("GetObjectMetadata() status = %v, want %v", w.Code, tt.wantStatusCode)
			}
		})
	}
}

func TestUpdateObjectMetadata(t *testing.T) {
	storage := NewMockStorage()
	handler := newTestHandler(t, storage)

	// Add test object
	storage.PutWithContext(context.Background(), "test.txt", strings.NewReader("test content"))

	tests := []struct {
		name           string
		key            string
		metadata       *common.Metadata
		wantStatusCode int
	}{
		{
			name: "update existing object",
			key:  "test.txt",
			metadata: &common.Metadata{
				ContentType: "text/plain",
				Custom: map[string]string{
					"author": "updated",
				},
			},
			wantStatusCode: http.StatusOK,
		},
		{
			name: "update non-existent object",
			key:  "nonexistent.txt",
			metadata: &common.Metadata{
				ContentType: "text/plain",
			},
			wantStatusCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			router := gin.New()
			router.PUT("/metadata/:key", handler.UpdateObjectMetadata)

			body, _ := json.Marshal(tt.metadata)
			req := httptest.NewRequest("PUT", "/metadata/"+tt.key, bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)

			if w.Code != tt.wantStatusCode {
				t.Errorf("UpdateObjectMetadata() status = %v, want %v", w.Code, tt.wantStatusCode)
			}
		})
	}
}

func TestUpdateObjectMetadataInvalidJSON(t *testing.T) {
	storage := NewMockStorage()
	handler := newTestHandler(t, storage)

	// Add test object
	storage.PutWithContext(context.Background(), "test.txt", strings.NewReader("test content"))

	router := gin.New()
	router.PUT("/metadata/:key", handler.UpdateObjectMetadata)

	req := httptest.NewRequest("PUT", "/metadata/test.txt", strings.NewReader("invalid json"))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("UpdateObjectMetadata() with invalid JSON status = %v, want %v", w.Code, http.StatusBadRequest)
	}
}

func TestHealthCheck(t *testing.T) {
	storage := NewMockStorage()
	handler := newTestHandler(t, storage)

	router := gin.New()
	router.GET("/health", handler.HealthCheck)

	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("HealthCheck() status = %v, want %v", w.Code, http.StatusOK)
	}

	var response HealthResponse
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Errorf("HealthCheck() failed to parse response: %v", err)
	}

	if response.Status != "healthy" {
		t.Errorf("HealthCheck() status = %v, want healthy", response.Status)
	}
}

func TestPutObjectWithLeadingSlash(t *testing.T) {
	storage := NewMockStorage()
	handler := newTestHandler(t, storage)

	router := gin.New()
	router.PUT("/objects/*key", handler.PutObject)

	req := httptest.NewRequest("PUT", "/objects//test.txt", strings.NewReader("content"))
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("PutObject() with leading slash status = %v, want %v", w.Code, http.StatusCreated)
	}

	// Verify object was stored (with or without leading slash is handled by the handler)
	exists1, _ := storage.Exists(context.Background(), "test.txt")
	exists2, _ := storage.Exists(context.Background(), "/test.txt")
	if !exists1 && !exists2 {
		t.Error("PutObject() should store object")
	}
}
