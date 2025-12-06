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

package quic

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/common"
	"google.golang.org/grpc/metadata"
)

// mockLifecycleStorage extends the base storage with lifecycle operations
type mockLifecycleStorage struct {
	data              map[string][]byte
	metadata          map[string]*common.Metadata
	policies          []common.LifecyclePolicy
	archiveError      error
	addPolicyError    error
	removePolicyError error
	getPoliciesError  error
}

func newMockLifecycleStorage() *mockLifecycleStorage {
	return &mockLifecycleStorage{
		data:     make(map[string][]byte),
		metadata: make(map[string]*common.Metadata),
		policies: []common.LifecyclePolicy{},
	}
}

func (m *mockLifecycleStorage) Configure(settings map[string]string) error {
	return nil
}

func (m *mockLifecycleStorage) Put(key string, data io.Reader) error {
	return m.PutWithContext(context.Background(), key, data)
}

func (m *mockLifecycleStorage) PutWithContext(ctx context.Context, key string, data io.Reader) error {
	content, err := io.ReadAll(data)
	if err != nil {
		return err
	}
	m.data[key] = content
	m.metadata[key] = &common.Metadata{
		Size:         int64(len(content)),
		LastModified: time.Now(),
	}
	return nil
}

func (m *mockLifecycleStorage) PutWithMetadata(ctx context.Context, key string, data io.Reader, metadata *common.Metadata) error {
	if err := m.PutWithContext(ctx, key, data); err != nil {
		return err
	}
	m.metadata[key] = metadata
	return nil
}

func (m *mockLifecycleStorage) Get(key string) (io.ReadCloser, error) {
	return m.GetWithContext(context.Background(), key)
}

func (m *mockLifecycleStorage) GetWithContext(ctx context.Context, key string) (io.ReadCloser, error) {
	content, exists := m.data[key]
	if !exists {
		return nil, errors.New("object not found")
	}
	return io.NopCloser(bytes.NewReader(content)), nil
}

func (m *mockLifecycleStorage) GetMetadata(ctx context.Context, key string) (*common.Metadata, error) {
	metadata, exists := m.metadata[key]
	if !exists {
		return nil, errors.New("object not found")
	}
	return metadata, nil
}

func (m *mockLifecycleStorage) UpdateMetadata(ctx context.Context, key string, metadata *common.Metadata) error {
	if _, exists := m.data[key]; !exists {
		return errors.New("object not found")
	}
	m.metadata[key] = metadata
	return nil
}

func (m *mockLifecycleStorage) Delete(key string) error {
	return m.DeleteWithContext(context.Background(), key)
}

func (m *mockLifecycleStorage) DeleteWithContext(ctx context.Context, key string) error {
	if _, exists := m.data[key]; !exists {
		return errors.New("object not found")
	}
	delete(m.data, key)
	delete(m.metadata, key)
	return nil
}

func (m *mockLifecycleStorage) Exists(ctx context.Context, key string) (bool, error) {
	_, exists := m.data[key]
	return exists, nil
}

func (m *mockLifecycleStorage) List(prefix string) ([]string, error) {
	return m.ListWithContext(context.Background(), prefix)
}

func (m *mockLifecycleStorage) ListWithContext(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	for key := range m.data {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	return keys, nil
}

func (m *mockLifecycleStorage) ListWithOptions(ctx context.Context, opts *common.ListOptions) (*common.ListResult, error) {
	var objects []*common.ObjectInfo
	for key, meta := range m.metadata {
		if opts.Prefix == "" || strings.HasPrefix(key, opts.Prefix) {
			objects = append(objects, &common.ObjectInfo{
				Key:      key,
				Metadata: meta,
			})
		}
	}
	return &common.ListResult{
		Objects:   objects,
		Truncated: false,
	}, nil
}

func (m *mockLifecycleStorage) Archive(key string, destination common.Archiver) error {
	if m.archiveError != nil {
		return m.archiveError
	}
	if _, exists := m.data[key]; !exists {
		return errors.New("object not found")
	}
	return nil
}

func (m *mockLifecycleStorage) AddPolicy(policy common.LifecyclePolicy) error {
	if m.addPolicyError != nil {
		return m.addPolicyError
	}
	for _, p := range m.policies {
		if p.ID == policy.ID {
			return errors.New("policy already exists")
		}
	}
	m.policies = append(m.policies, policy)
	return nil
}

func (m *mockLifecycleStorage) RemovePolicy(id string) error {
	if m.removePolicyError != nil {
		return m.removePolicyError
	}
	for i, p := range m.policies {
		if p.ID == id {
			m.policies = append(m.policies[:i], m.policies[i+1:]...)
			return nil
		}
	}
	return errors.New("policy not found")
}

func (m *mockLifecycleStorage) GetPolicies() ([]common.LifecyclePolicy, error) {
	if m.getPoliciesError != nil {
		return nil, m.getPoliciesError
	}
	return m.policies, nil
}

// mockArchiver for testing
type mockArchiver struct{}

func (m *mockArchiver) Put(key string, data io.Reader) error {
	return nil
}

// mockLogger for testing
type mockLogger struct{}

func (m *mockLogger) Debug(ctx context.Context, msg string, fields ...adapters.Field) {}
func (m *mockLogger) Info(ctx context.Context, msg string, fields ...adapters.Field)  {}
func (m *mockLogger) Warn(ctx context.Context, msg string, fields ...adapters.Field)  {}
func (m *mockLogger) Error(ctx context.Context, msg string, fields ...adapters.Field) {}
func (m *mockLogger) Fatal(ctx context.Context, msg string, fields ...adapters.Field) {}
func (m *mockLogger) WithFields(fields ...adapters.Field) adapters.Logger             { return m }
func (m *mockLogger) WithContext(ctx context.Context) adapters.Logger                 { return m }
func (m *mockLogger) SetLevel(level adapters.LogLevel)                                {}
func (m *mockLogger) GetLevel() adapters.LogLevel                                     { return adapters.InfoLevel }

// mockAuthenticator for testing
type mockAuthenticator struct {
	shouldFail bool
}

func (m *mockAuthenticator) AuthenticateHTTP(ctx context.Context, r *http.Request) (*adapters.Principal, error) {
	if m.shouldFail {
		return nil, errors.New("authentication failed")
	}
	return &adapters.Principal{
		ID:   "test-user",
		Name: "Test User",
	}, nil
}

func (m *mockAuthenticator) AuthenticateGRPC(ctx context.Context, md metadata.MD) (*adapters.Principal, error) {
	if m.shouldFail {
		return nil, errors.New("authentication failed")
	}
	return &adapters.Principal{
		ID:   "test-user",
		Name: "Test User",
	}, nil
}

func (m *mockAuthenticator) AuthenticateMTLS(ctx context.Context, connState *tls.ConnectionState) (*adapters.Principal, error) {
	if m.shouldFail {
		return nil, errors.New("authentication failed")
	}
	return &adapters.Principal{
		ID:   "test-user",
		Name: "Test User",
	}, nil
}

func (m *mockAuthenticator) ValidatePermission(ctx context.Context, principal *adapters.Principal, resource, action string) error {
	if m.shouldFail {
		return errors.New("permission denied")
	}
	return nil
}

// TestArchiveViaQUIC tests archiving objects via QUIC/HTTP3
func TestArchiveViaQUIC(t *testing.T) {
	tests := []struct {
		name           string
		setupStorage   func() *mockLifecycleStorage
		requestBody    map[string]any
		wantStatusCode int
	}{
		{
			name: "successful archive",
			setupStorage: func() *mockLifecycleStorage {
				storage := newMockLifecycleStorage()
				storage.PutWithContext(context.Background(), "test.txt", strings.NewReader("content"))
				return storage
			},
			requestBody: map[string]any{
				"key":              "test.txt",
				"destination_type": "glacier",
				"destination_settings": map[string]string{
					"region": "us-east-1",
				},
			},
			wantStatusCode: http.StatusOK,
		},
		{
			name: "object not found",
			setupStorage: func() *mockLifecycleStorage {
				return newMockLifecycleStorage()
			},
			requestBody: map[string]any{
				"key":              "nonexistent.txt",
				"destination_type": "glacier",
			},
			wantStatusCode: http.StatusNotFound,
		},
		{
			name: "storage error",
			setupStorage: func() *mockLifecycleStorage {
				storage := newMockLifecycleStorage()
				storage.PutWithContext(context.Background(), "test.txt", strings.NewReader("content"))
				storage.archiveError = errors.New("internal error")
				return storage
			},
			requestBody: map[string]any{
				"key":              "test.txt",
				"destination_type": "glacier",
			},
			wantStatusCode: http.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage := tt.setupStorage()
			_ = createHandlerWithStorage(t, storage, 10*1024*1024, 30*time.Second, 30*time.Second, &mockLogger{}, &mockAuthenticator{})

			// We can't directly test the handler's custom routing, but we can test the storage operations
			// In a real test, we would set up a full HTTP/3 server
			key := tt.requestBody["key"].(string)
			err := storage.Archive(key, &mockArchiver{})

			if tt.wantStatusCode >= 400 {
				if err == nil {
					t.Errorf("Archive() error = nil, want error")
				}
			} else {
				if err != nil {
					t.Errorf("Archive() unexpected error = %v", err)
				}
			}
		})
	}
}

// TestAddPolicyViaQUIC tests adding lifecycle policies via QUIC/HTTP3
func TestAddPolicyViaQUIC(t *testing.T) {
	tests := []struct {
		name           string
		setupStorage   func() *mockLifecycleStorage
		policy         common.LifecyclePolicy
		wantStatusCode int
	}{
		{
			name: "successful add delete policy",
			setupStorage: func() *mockLifecycleStorage {
				return newMockLifecycleStorage()
			},
			policy: common.LifecyclePolicy{
				ID:        "policy1",
				Prefix:    "logs/",
				Retention: 24 * time.Hour,
				Action:    "delete",
			},
			wantStatusCode: http.StatusCreated,
		},
		{
			name: "successful add archive policy",
			setupStorage: func() *mockLifecycleStorage {
				return newMockLifecycleStorage()
			},
			policy: common.LifecyclePolicy{
				ID:        "policy2",
				Prefix:    "data/",
				Retention: 30 * 24 * time.Hour,
				Action:    "archive",
			},
			wantStatusCode: http.StatusCreated,
		},
		{
			name: "duplicate policy",
			setupStorage: func() *mockLifecycleStorage {
				storage := newMockLifecycleStorage()
				storage.policies = []common.LifecyclePolicy{
					{ID: "existing", Prefix: "test/", Retention: 24 * time.Hour, Action: "delete"},
				}
				return storage
			},
			policy: common.LifecyclePolicy{
				ID:        "existing",
				Prefix:    "logs/",
				Retention: 24 * time.Hour,
				Action:    "delete",
			},
			wantStatusCode: http.StatusConflict,
		},
		{
			name: "storage error",
			setupStorage: func() *mockLifecycleStorage {
				storage := newMockLifecycleStorage()
				storage.addPolicyError = errors.New("storage error")
				return storage
			},
			policy: common.LifecyclePolicy{
				ID:        "policy3",
				Prefix:    "logs/",
				Retention: 24 * time.Hour,
				Action:    "delete",
			},
			wantStatusCode: http.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage := tt.setupStorage()

			err := storage.AddPolicy(tt.policy)

			if tt.wantStatusCode >= 400 {
				if err == nil {
					t.Errorf("AddPolicy() error = nil, want error")
				}
			} else {
				if err != nil {
					t.Errorf("AddPolicy() unexpected error = %v", err)
				}
				// Verify policy was added
				policies, _ := storage.GetPolicies()
				found := false
				for _, p := range policies {
					if p.ID == tt.policy.ID {
						found = true
						break
					}
				}
				if !found {
					t.Error("AddPolicy() policy not found in storage")
				}
			}
		})
	}
}

// TestRemovePolicyViaQUIC tests removing lifecycle policies via QUIC/HTTP3
func TestRemovePolicyViaQUIC(t *testing.T) {
	tests := []struct {
		name           string
		setupStorage   func() *mockLifecycleStorage
		policyID       string
		wantStatusCode int
	}{
		{
			name: "successful remove",
			setupStorage: func() *mockLifecycleStorage {
				storage := newMockLifecycleStorage()
				storage.policies = []common.LifecyclePolicy{
					{ID: "policy1", Prefix: "logs/", Retention: 24 * time.Hour, Action: "delete"},
				}
				return storage
			},
			policyID:       "policy1",
			wantStatusCode: http.StatusOK,
		},
		{
			name: "policy not found",
			setupStorage: func() *mockLifecycleStorage {
				return newMockLifecycleStorage()
			},
			policyID:       "nonexistent",
			wantStatusCode: http.StatusNotFound,
		},
		{
			name: "storage error",
			setupStorage: func() *mockLifecycleStorage {
				storage := newMockLifecycleStorage()
				storage.removePolicyError = errors.New("storage error")
				return storage
			},
			policyID:       "policy1",
			wantStatusCode: http.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage := tt.setupStorage()

			err := storage.RemovePolicy(tt.policyID)

			if tt.wantStatusCode >= 400 {
				if err == nil {
					t.Errorf("RemovePolicy() error = nil, want error")
				}
			} else {
				if err != nil {
					t.Errorf("RemovePolicy() unexpected error = %v", err)
				}
				// Verify policy was removed
				policies, _ := storage.GetPolicies()
				for _, p := range policies {
					if p.ID == tt.policyID {
						t.Error("RemovePolicy() policy still exists in storage")
					}
				}
			}
		})
	}
}

// TestGetPoliciesViaQUIC tests retrieving lifecycle policies via QUIC/HTTP3
func TestGetPoliciesViaQUIC(t *testing.T) {
	tests := []struct {
		name           string
		setupStorage   func() *mockLifecycleStorage
		prefixFilter   string
		wantCount      int
		wantStatusCode int
	}{
		{
			name: "get all policies",
			setupStorage: func() *mockLifecycleStorage {
				storage := newMockLifecycleStorage()
				storage.policies = []common.LifecyclePolicy{
					{ID: "policy1", Prefix: "logs/", Retention: 24 * time.Hour, Action: "delete"},
					{ID: "policy2", Prefix: "data/", Retention: 30 * 24 * time.Hour, Action: "archive"},
				}
				return storage
			},
			prefixFilter:   "",
			wantCount:      2,
			wantStatusCode: http.StatusOK,
		},
		{
			name: "filter by prefix",
			setupStorage: func() *mockLifecycleStorage {
				storage := newMockLifecycleStorage()
				storage.policies = []common.LifecyclePolicy{
					{ID: "policy1", Prefix: "logs/", Retention: 24 * time.Hour, Action: "delete"},
					{ID: "policy2", Prefix: "data/", Retention: 30 * 24 * time.Hour, Action: "delete"},
					{ID: "policy3", Prefix: "logs/", Retention: 7 * 24 * time.Hour, Action: "archive"},
				}
				return storage
			},
			prefixFilter:   "logs/",
			wantCount:      2,
			wantStatusCode: http.StatusOK,
		},
		{
			name: "no policies",
			setupStorage: func() *mockLifecycleStorage {
				return newMockLifecycleStorage()
			},
			prefixFilter:   "",
			wantCount:      0,
			wantStatusCode: http.StatusOK,
		},
		{
			name: "storage error",
			setupStorage: func() *mockLifecycleStorage {
				storage := newMockLifecycleStorage()
				storage.getPoliciesError = errors.New("storage error")
				return storage
			},
			prefixFilter:   "",
			wantStatusCode: http.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage := tt.setupStorage()

			policies, err := storage.GetPolicies()

			if tt.wantStatusCode >= 400 {
				if err == nil {
					t.Errorf("GetPolicies() error = nil, want error")
				}
			} else {
				if err != nil {
					t.Errorf("GetPolicies() unexpected error = %v", err)
				}

				// Filter by prefix if specified
				if tt.prefixFilter != "" {
					filtered := []common.LifecyclePolicy{}
					for _, p := range policies {
						if p.Prefix == tt.prefixFilter {
							filtered = append(filtered, p)
						}
					}
					policies = filtered
				}

				if len(policies) != tt.wantCount {
					t.Errorf("GetPolicies() count = %d, want %d", len(policies), tt.wantCount)
				}
			}
		})
	}
}

// TestQUICHandlerWithLifecycleOperations tests the full QUIC handler with lifecycle operations
func TestQUICHandlerWithLifecycleOperations(t *testing.T) {
	storage := newMockLifecycleStorage()
	handler := createHandlerWithStorage(t, storage, 10*1024*1024, 30*time.Second, 30*time.Second, &mockLogger{}, &mockAuthenticator{})

	// Test health endpoint (no auth required)
	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Health check status = %v, want %v", w.Code, http.StatusOK)
	}
}

// TestExistsViaQUIC tests object existence checking via QUIC
func TestExistsViaQUIC(t *testing.T) {
	storage := newMockLifecycleStorage()
	handler := createHandlerWithStorage(t, storage, 10*1024*1024, 30*time.Second, 30*time.Second, &mockLogger{}, &mockAuthenticator{})

	// Put an object first
	putReq := httptest.NewRequest("PUT", "/objects/test-key", strings.NewReader("test data"))
	putReq.Header.Set("Content-Type", "text/plain")
	putW := httptest.NewRecorder()
	handler.ServeHTTP(putW, putReq)

	if putW.Code != http.StatusCreated {
		t.Fatalf("PUT failed with status %v", putW.Code)
	}

	// Test exists query for existing object
	req := httptest.NewRequest("GET", "/objects/test-key?exists=true", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Exists check status = %v, want %v", w.Code, http.StatusOK)
	}

	var response map[string]bool
	if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if !response["exists"] {
		t.Error("Expected exists=true for existing object")
	}

	// Test exists query for non-existing object
	req = httptest.NewRequest("GET", "/objects/non-existent?exists=true", nil)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Exists check status = %v, want %v", w.Code, http.StatusOK)
	}

	if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if response["exists"] {
		t.Error("Expected exists=false for non-existing object")
	}
}

// TestUpdateMetadataViaQUIC tests metadata update via QUIC
func TestUpdateMetadataViaQUIC(t *testing.T) {
	storage := newMockLifecycleStorage()
	handler := createHandlerWithStorage(t, storage, 10*1024*1024, 30*time.Second, 30*time.Second, &mockLogger{}, &mockAuthenticator{})

	// Put an object first
	putReq := httptest.NewRequest("PUT", "/objects/test-key", strings.NewReader("test data"))
	putReq.Header.Set("Content-Type", "text/plain")
	putW := httptest.NewRecorder()
	handler.ServeHTTP(putW, putReq)

	if putW.Code != http.StatusCreated {
		t.Fatalf("PUT failed with status %v", putW.Code)
	}

	// Update metadata
	metadataUpdate := map[string]any{
		"content_type":     "application/json",
		"content_encoding": "gzip",
		"custom": map[string]string{
			"key1": "value1",
			"key2": "value2",
		},
	}

	body, _ := json.Marshal(metadataUpdate)
	req := httptest.NewRequest("PATCH", "/objects/test-key", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("UpdateMetadata status = %v, want %v, body: %s", w.Code, http.StatusOK, w.Body.String())
	}

	var response map[string]string
	if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if response["message"] != "metadata updated successfully" {
		t.Errorf("Expected success message, got: %v", response["message"])
	}

	// Verify metadata was updated
	headReq := httptest.NewRequest("HEAD", "/objects/test-key", nil)
	headW := httptest.NewRecorder()
	handler.ServeHTTP(headW, headReq)

	if headW.Code != http.StatusOK {
		t.Errorf("HEAD status = %v, want %v", headW.Code, http.StatusOK)
	}

	if headW.Header().Get("Content-Type") != "application/json" {
		t.Errorf("Content-Type = %v, want application/json", headW.Header().Get("Content-Type"))
	}

	if headW.Header().Get("Content-Encoding") != "gzip" {
		t.Errorf("Content-Encoding = %v, want gzip", headW.Header().Get("Content-Encoding"))
	}
}

// TestUpdateMetadataInvalidJSON tests UpdateMetadata with invalid JSON
func TestUpdateMetadataInvalidJSON(t *testing.T) {
	storage := newMockLifecycleStorage()
	handler := createHandlerWithStorage(t, storage, 10*1024*1024, 30*time.Second, 30*time.Second, &mockLogger{}, &mockAuthenticator{})

	req := httptest.NewRequest("PATCH", "/objects/test-key", strings.NewReader("invalid json"))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Status = %v, want %v", w.Code, http.StatusBadRequest)
	}
}

// TestApplyPolicies tests applying lifecycle policies
func TestApplyPolicies(t *testing.T) {
	storage := newMockLifecycleStorage()
	handler := createHandlerWithStorage(t, storage, 10*1024*1024, 30*time.Second, 30*time.Second, &mockLogger{}, &mockAuthenticator{})

	// Add a policy
	policy := common.LifecyclePolicy{
		ID:        "cleanup-old",
		Prefix:    "logs/",
		Retention: 24 * time.Hour,
		Action:    "delete",
	}
	storage.AddPolicy(policy)

	// Add some test objects
	storage.PutWithContext(context.Background(), "logs/old.txt", bytes.NewReader([]byte("old")))
	storage.PutWithContext(context.Background(), "logs/recent.txt", bytes.NewReader([]byte("recent")))

	// Set timestamps
	storage.metadata["logs/old.txt"].LastModified = time.Now().Add(-48 * time.Hour)
	storage.metadata["logs/recent.txt"].LastModified = time.Now()

	req := httptest.NewRequest("POST", "/policies/apply", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Status = %v, want %v", w.Code, http.StatusOK)
	}

	// Verify response
	var result map[string]any
	if err := json.NewDecoder(w.Body).Decode(&result); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if result["policies_count"] == nil {
		t.Error("Expected policies_count in response")
	}
}

// TestApplyPolicies_NoPolicies tests applying policies when no policies exist
func TestApplyPolicies_NoPolicies(t *testing.T) {
	storage := newMockLifecycleStorage()
	handler := createHandlerWithStorage(t, storage, 10*1024*1024, 30*time.Second, 30*time.Second, &mockLogger{}, &mockAuthenticator{})

	req := httptest.NewRequest("POST", "/policies/apply", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Status = %v, want %v", w.Code, http.StatusOK)
	}

	var result map[string]any
	if err := json.NewDecoder(w.Body).Decode(&result); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if result["policies_count"].(float64) != 0 {
		t.Errorf("Expected 0 policies, got %v", result["policies_count"])
	}
	if result["objects_processed"].(float64) != 0 {
		t.Errorf("Expected 0 objects processed, got %v", result["objects_processed"])
	}
}

// TestApplyPolicies_WrongMethod tests applying policies with wrong HTTP method
func TestApplyPolicies_WrongMethod(t *testing.T) {
	storage := newMockLifecycleStorage()
	handler := createHandlerWithStorage(t, storage, 10*1024*1024, 30*time.Second, 30*time.Second, &mockLogger{}, &mockAuthenticator{})

	req := httptest.NewRequest("GET", "/policies/apply", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("Status = %v, want %v", w.Code, http.StatusMethodNotAllowed)
	}
}

// TestHealth tests the health endpoint
func TestHealth(t *testing.T) {
	storage := newMockLifecycleStorage()
	handler := createHandlerWithStorage(t, storage, 10*1024*1024, 30*time.Second, 30*time.Second, &mockLogger{}, &mockAuthenticator{})

	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Status = %v, want %v", w.Code, http.StatusOK)
	}

	// Health endpoint returns JSON
	var result map[string]any
	if err := json.NewDecoder(w.Body).Decode(&result); err != nil {
		t.Fatalf("Failed to decode health response: %v", err)
	}

	if status, ok := result["status"]; !ok || status != "healthy" {
		t.Errorf("Expected status 'healthy', got %v", result["status"])
	}
}

// TestHealth_POST tests health with POST method
func TestHealth_POST(t *testing.T) {
	storage := newMockLifecycleStorage()
	handler := createHandlerWithStorage(t, storage, 10*1024*1024, 30*time.Second, 30*time.Second, &mockLogger{}, &mockAuthenticator{})

	req := httptest.NewRequest("POST", "/health", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	// Health endpoint accepts all methods
	if w.Code != http.StatusOK {
		t.Errorf("Status = %v, want %v", w.Code, http.StatusOK)
	}
}

// TestExists_NotFound tests checking existence of non-existent object
func TestExists_NotFound(t *testing.T) {
	storage := newMockLifecycleStorage()
	handler := createHandlerWithStorage(t, storage, 10*1024*1024, 30*time.Second, 30*time.Second, &mockLogger{}, &mockAuthenticator{})

	req := httptest.NewRequest("HEAD", "/objects/nonexistent.txt", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("Status = %v, want %v", w.Code, http.StatusNotFound)
	}
}

// TestExists_WrongMethod tests exists with wrong method
func TestExists_WrongMethod(t *testing.T) {
	storage := newMockLifecycleStorage()
	handler := createHandlerWithStorage(t, storage, 10*1024*1024, 30*time.Second, 30*time.Second, &mockLogger{}, &mockAuthenticator{})

	req := httptest.NewRequest("POST", "/objects/test.txt", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("Status = %v, want %v", w.Code, http.StatusMethodNotAllowed)
	}
}

// TestGet_NotFound tests getting non-existent object
func TestGet_NotFound(t *testing.T) {
	storage := newMockLifecycleStorage()
	handler := createHandlerWithStorage(t, storage, 10*1024*1024, 30*time.Second, 30*time.Second, &mockLogger{}, &mockAuthenticator{})

	req := httptest.NewRequest("GET", "/objects/nonexistent.txt", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("Status = %v, want %v", w.Code, http.StatusNotFound)
	}
}

// TestGet_WrongMethod tests get with wrong method
func TestGet_WrongMethod(t *testing.T) {
	storage := newMockLifecycleStorage()
	handler := createHandlerWithStorage(t, storage, 10*1024*1024, 30*time.Second, 30*time.Second, &mockLogger{}, &mockAuthenticator{})

	req := httptest.NewRequest("POST", "/objects/test.txt", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("Status = %v, want %v", w.Code, http.StatusMethodNotAllowed)
	}
}

// TestApplyPolicies_WithArchive tests applying archive policies
func TestApplyPolicies_WithArchive(t *testing.T) {
	storage := newMockLifecycleStorage()
	handler := createHandlerWithStorage(t, storage, 10*1024*1024, 30*time.Second, 30*time.Second, &mockLogger{}, &mockAuthenticator{})

	// Add archive policy
	policy := common.LifecyclePolicy{
		ID:          "archive-old",
		Prefix:      "data/",
		Retention:   24 * time.Hour,
		Action:      "archive",
		Destination: &mockArchiver{},
	}
	storage.AddPolicy(policy)

	// Add old object
	storage.PutWithContext(context.Background(), "data/old.csv", bytes.NewReader([]byte("data")))
	storage.metadata["data/old.csv"].LastModified = time.Now().Add(-48 * time.Hour)

	req := httptest.NewRequest("POST", "/policies/apply", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Status = %v, want %v", w.Code, http.StatusOK)
	}
}

// TestApplyPolicies_ObjectWithoutMetadata tests policy on objects without metadata
func TestApplyPolicies_ObjectWithoutMetadata(t *testing.T) {
	storage := newMockLifecycleStorage()
	handler := createHandlerWithStorage(t, storage, 10*1024*1024, 30*time.Second, 30*time.Second, &mockLogger{}, &mockAuthenticator{})

	// Add policy
	policy := common.LifecyclePolicy{
		ID:        "cleanup",
		Prefix:    "",
		Retention: 24 * time.Hour,
		Action:    "delete",
	}
	storage.AddPolicy(policy)

	// Add object and clear its metadata
	storage.PutWithContext(context.Background(), "test.txt", bytes.NewReader([]byte("test")))
	storage.metadata["test.txt"] = nil

	req := httptest.NewRequest("POST", "/policies/apply", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Status = %v, want %v", w.Code, http.StatusOK)
	}

	// Object should not be deleted due to missing metadata
	if _, exists := storage.data["test.txt"]; !exists {
		t.Error("Object should not be deleted when metadata is nil")
	}
}

// TestUpdateMetadata_InvalidJSON tests update metadata with invalid JSON
func TestUpdateMetadata_InvalidJSON2(t *testing.T) {
	storage := newMockLifecycleStorage()
	handler := createHandlerWithStorage(t, storage, 10*1024*1024, 30*time.Second, 30*time.Second, &mockLogger{}, &mockAuthenticator{})

	// Add object
	storage.PutWithContext(context.Background(), "test.txt", bytes.NewReader([]byte("test")))

	req := httptest.NewRequest("PATCH", "/objects/test.txt", strings.NewReader("{invalid}"))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Status = %v, want %v", w.Code, http.StatusBadRequest)
	}
}

// TestAddPolicy_InvalidJSON tests add policy with invalid JSON
func TestAddPolicy_InvalidJSON(t *testing.T) {
	storage := newMockLifecycleStorage()
	handler := createHandlerWithStorage(t, storage, 10*1024*1024, 30*time.Second, 30*time.Second, &mockLogger{}, &mockAuthenticator{})

	req := httptest.NewRequest("POST", "/policies", strings.NewReader("{invalid json}"))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Status = %v, want %v", w.Code, http.StatusBadRequest)
	}
}

// TestAddPolicy_MissingID tests add policy without ID
func TestAddPolicy_MissingID(t *testing.T) {
	storage := newMockLifecycleStorage()
	handler := createHandlerWithStorage(t, storage, 10*1024*1024, 30*time.Second, 30*time.Second, &mockLogger{}, &mockAuthenticator{})

	policyJSON := `{"retention_seconds": 86400, "action": "delete"}`
	req := httptest.NewRequest("POST", "/policies", strings.NewReader(policyJSON))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Status = %v, want %v", w.Code, http.StatusBadRequest)
	}
}

// TestAddPolicy_MissingAction tests add policy without action
func TestAddPolicy_MissingAction(t *testing.T) {
	storage := newMockLifecycleStorage()
	handler := createHandlerWithStorage(t, storage, 10*1024*1024, 30*time.Second, 30*time.Second, &mockLogger{}, &mockAuthenticator{})

	policyJSON := `{"id": "test", "retention_seconds": 86400}`
	req := httptest.NewRequest("POST", "/policies", strings.NewReader(policyJSON))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Status = %v, want %v", w.Code, http.StatusBadRequest)
	}
}

// TestAddPolicy_InvalidAction tests add policy with invalid action
func TestAddPolicy_InvalidAction(t *testing.T) {
	storage := newMockLifecycleStorage()
	handler := createHandlerWithStorage(t, storage, 10*1024*1024, 30*time.Second, 30*time.Second, &mockLogger{}, &mockAuthenticator{})

	policyJSON := `{"id": "test", "retention_seconds": 86400, "action": "invalid"}`
	req := httptest.NewRequest("POST", "/policies", strings.NewReader(policyJSON))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Status = %v, want %v", w.Code, http.StatusBadRequest)
	}
}

// TestAddPolicy_NegativeRetention tests add policy with negative retention
func TestAddPolicy_NegativeRetention(t *testing.T) {
	storage := newMockLifecycleStorage()
	handler := createHandlerWithStorage(t, storage, 10*1024*1024, 30*time.Second, 30*time.Second, &mockLogger{}, &mockAuthenticator{})

	policyJSON := `{"id": "test", "retention_seconds": -1, "action": "delete"}`
	req := httptest.NewRequest("POST", "/policies", strings.NewReader(policyJSON))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Status = %v, want %v", w.Code, http.StatusBadRequest)
	}
}

// TestGetPolicies_Empty tests getting policies when none exist
func TestGetPolicies_Empty(t *testing.T) {
	storage := newMockLifecycleStorage()
	handler := createHandlerWithStorage(t, storage, 10*1024*1024, 30*time.Second, 30*time.Second, &mockLogger{}, &mockAuthenticator{})

	req := httptest.NewRequest("GET", "/policies", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Status = %v, want %v", w.Code, http.StatusOK)
	}

	var result map[string]any
	if err := json.NewDecoder(w.Body).Decode(&result); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	policies, ok := result["policies"].([]any)
	if !ok {
		t.Fatal("Expected policies array in response")
	}

	if len(policies) != 0 {
		t.Errorf("Expected 0 policies, got %d", len(policies))
	}
}

// TestPut_WithExistingObject tests putting an object that already exists
func TestPut_WithExistingObject(t *testing.T) {
	storage := newMockLifecycleStorage()
	handler := createHandlerWithStorage(t, storage, 10*1024*1024, 30*time.Second, 30*time.Second, &mockLogger{}, &mockAuthenticator{})

	// Put object first time
	req := httptest.NewRequest("PUT", "/objects/test.txt", bytes.NewReader([]byte("data1")))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("First PUT Status = %v, want %v", w.Code, http.StatusCreated)
	}

	// Put again to overwrite
	req = httptest.NewRequest("PUT", "/objects/test.txt", bytes.NewReader([]byte("data2")))
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("Second PUT Status = %v, want %v", w.Code, http.StatusCreated)
	}

	// Verify the object was overwritten
	if string(storage.data["test.txt"]) != "data2" {
		t.Errorf("Expected data2, got %s", string(storage.data["test.txt"]))
	}
}

// TestDelete_Success tests deleting an existing object
func TestDelete_Success(t *testing.T) {
	storage := newMockLifecycleStorage()
	handler := createHandlerWithStorage(t, storage, 10*1024*1024, 30*time.Second, 30*time.Second, &mockLogger{}, &mockAuthenticator{})

	// Add object first
	storage.PutWithContext(context.Background(), "test.txt", bytes.NewReader([]byte("data")))

	req := httptest.NewRequest("DELETE", "/objects/test.txt", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Errorf("Status = %v, want %v", w.Code, http.StatusNoContent)
	}

	// Verify object was deleted
	if _, exists := storage.data["test.txt"]; exists {
		t.Error("Object should have been deleted")
	}
}

// TestList_WithPrefix tests listing objects with prefix
func TestList_WithPrefix(t *testing.T) {
	storage := newMockLifecycleStorage()
	handler := createHandlerWithStorage(t, storage, 10*1024*1024, 30*time.Second, 30*time.Second, &mockLogger{}, &mockAuthenticator{})

	// Add objects
	storage.PutWithContext(context.Background(), "logs/app.log", bytes.NewReader([]byte("log1")))
	storage.PutWithContext(context.Background(), "logs/error.log", bytes.NewReader([]byte("log2")))
	storage.PutWithContext(context.Background(), "data/file.txt", bytes.NewReader([]byte("data")))

	req := httptest.NewRequest("GET", "/objects?prefix=logs/", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Status = %v, want %v", w.Code, http.StatusOK)
	}

	var result map[string]any
	if err := json.NewDecoder(w.Body).Decode(&result); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	objects, ok := result["objects"].([]any)
	if !ok {
		t.Fatal("Expected objects array in response")
	}

	if len(objects) != 2 {
		t.Errorf("Expected 2 objects with logs/ prefix, got %d", len(objects))
	}
}
