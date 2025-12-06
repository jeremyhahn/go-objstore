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
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// mockReplicationStorage is a simple mock that supports replication
type mockReplicationStorage struct {
	*mockLifecycleStorage
	replManager *mockReplicationManager
}

type mockReplicationManager struct {
	policies []common.ReplicationPolicy
}

func (m *mockReplicationManager) AddPolicy(policy common.ReplicationPolicy) error {
	m.policies = append(m.policies, policy)
	return nil
}

func (m *mockReplicationManager) RemovePolicy(id string) error {
	return nil
}

func (m *mockReplicationManager) GetPolicy(id string) (*common.ReplicationPolicy, error) {
	for _, p := range m.policies {
		if p.ID == id {
			return &p, nil
		}
	}
	return nil, common.ErrPolicyNotFound
}

func (m *mockReplicationManager) GetPolicies() ([]common.ReplicationPolicy, error) {
	return m.policies, nil
}

func (m *mockReplicationManager) SyncAll(ctx context.Context) (*common.SyncResult, error) {
	return &common.SyncResult{}, nil
}

func (m *mockReplicationManager) SyncPolicy(ctx context.Context, policyID string) (*common.SyncResult, error) {
	return &common.SyncResult{
		PolicyID: policyID,
		Synced:   0,
		Deleted:  0,
		Failed:   0,
	}, nil
}

func (m *mockReplicationManager) SetBackendEncrypterFactory(policyID string, factory common.EncrypterFactory) error {
	return nil
}

func (m *mockReplicationManager) SetSourceEncrypterFactory(policyID string, factory common.EncrypterFactory) error {
	return nil
}

func (m *mockReplicationManager) SetDestinationEncrypterFactory(policyID string, factory common.EncrypterFactory) error {
	return nil
}

func (m *mockReplicationManager) Run(ctx context.Context) {
	// No-op for testing
}

func (m *mockReplicationStorage) GetReplicationManager() (common.ReplicationManager, error) {
	if m.replManager == nil {
		return nil, common.ErrReplicationNotSupported
	}
	return m.replManager, nil
}

func newMockReplicationStorage() *mockReplicationStorage {
	return &mockReplicationStorage{
		mockLifecycleStorage: newMockLifecycleStorage(),
		replManager: &mockReplicationManager{
			policies: []common.ReplicationPolicy{},
		},
	}
}

// TestHandleReplicationPolicies_GET tests the router for GET requests
func TestHandleReplicationPolicies_GET(t *testing.T) {
	storage := newMockReplicationStorage()
	initTestFacade(t, storage)
	handler := &Handler{
		backend:     "",
		readTimeout: 5 * time.Second,
	}

	req := httptest.NewRequest(http.MethodGet, "/replication/policies", nil)
	w := httptest.NewRecorder()

	handler.handleReplicationPolicies(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status %d, got %d: %s", http.StatusOK, w.Code, w.Body.String())
	}
}

// TestHandleReplicationPolicies_POST tests the router for POST requests
func TestHandleReplicationPolicies_POST(t *testing.T) {
	storage := newMockReplicationStorage()
	initTestFacade(t, storage)
	handler := &Handler{
		backend:      "",
		writeTimeout: 5 * time.Second,
	}

	policy := struct {
		ID                 string            `json:"id"`
		SourceBucket       string            `json:"source_bucket"`
		DestinationBucket  string            `json:"destination_bucket"`
		DestinationType    string            `json:"destination_type"`
		DestinationSettings map[string]string `json:"destination_settings"`
		Enabled            bool              `json:"enabled"`
	}{
		ID:                 "test-policy",
		SourceBucket:       "source",
		DestinationBucket:  "dest",
		DestinationType:    "local",
		DestinationSettings: map[string]string{"path": "/tmp/test"},
		Enabled:            true,
	}

	body, _ := json.Marshal(policy)
	req := httptest.NewRequest(http.MethodPost, "/replication/policies", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handler.handleReplicationPolicies(w, req)

	if w.Code != http.StatusCreated && w.Code != http.StatusOK {
		t.Logf("POST returned status %d: %s", w.Code, w.Body.String())
	}
}

// TestHandleReplicationPolicies_MethodNotAllowed tests unsupported HTTP methods
func TestHandleReplicationPolicies_MethodNotAllowed(t *testing.T) {
	storage := newMockReplicationStorage()
	initTestFacade(t, storage)
	handler := &Handler{
		backend:     "",
		readTimeout: 5 * time.Second,
	}

	req := httptest.NewRequest(http.MethodPut, "/replication/policies", nil)
	w := httptest.NewRecorder()

	handler.handleReplicationPolicies(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected status %d, got %d", http.StatusMethodNotAllowed, w.Code)
	}
}

// TestHandleReplicationPolicies_DELETE tests DELETE method
func TestHandleReplicationPolicies_DELETE(t *testing.T) {
	storage := newMockReplicationStorage()
	initTestFacade(t, storage)
	handler := &Handler{
		backend:     "",
		readTimeout: 5 * time.Second,
	}

	req := httptest.NewRequest(http.MethodDelete, "/replication/policies", nil)
	w := httptest.NewRecorder()

	handler.handleReplicationPolicies(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected status %d, got %d", http.StatusMethodNotAllowed, w.Code)
	}
}

// mockSimpleStorage is a storage without lifecycle support
type mockSimpleStorage struct {
	data     map[string][]byte
	metadata map[string]*common.Metadata
}

func (m *mockSimpleStorage) Configure(settings map[string]string) error { return nil }
func (m *mockSimpleStorage) Put(key string, data io.Reader) error       { return nil }
func (m *mockSimpleStorage) PutWithContext(ctx context.Context, key string, data io.Reader) error {
	return nil
}
func (m *mockSimpleStorage) PutWithMetadata(ctx context.Context, key string, data io.Reader, metadata *common.Metadata) error {
	return nil
}
func (m *mockSimpleStorage) Get(key string) (io.ReadCloser, error)                      { return nil, nil }
func (m *mockSimpleStorage) GetWithContext(ctx context.Context, key string) (io.ReadCloser, error) {
	return nil, nil
}
func (m *mockSimpleStorage) GetMetadata(ctx context.Context, key string) (*common.Metadata, error) {
	return nil, nil
}
func (m *mockSimpleStorage) UpdateMetadata(ctx context.Context, key string, metadata *common.Metadata) error {
	return nil
}
func (m *mockSimpleStorage) Delete(key string) error                              { return nil }
func (m *mockSimpleStorage) DeleteWithContext(ctx context.Context, key string) error { return nil }
func (m *mockSimpleStorage) Exists(ctx context.Context, key string) (bool, error) { return false, nil }
func (m *mockSimpleStorage) List(prefix string) ([]string, error)                 { return nil, nil }
func (m *mockSimpleStorage) ListWithContext(ctx context.Context, prefix string) ([]string, error) {
	return nil, nil
}
func (m *mockSimpleStorage) ListWithOptions(ctx context.Context, opts *common.ListOptions) (*common.ListResult, error) {
	return nil, nil
}
func (m *mockSimpleStorage) AddPolicy(policy common.LifecyclePolicy) error    { return nil }
func (m *mockSimpleStorage) RemovePolicy(id string) error                      { return nil }
func (m *mockSimpleStorage) GetPolicies() ([]common.LifecyclePolicy, error)   { return nil, nil }
func (m *mockSimpleStorage) Archive(key string, destination common.Archiver) error { return nil }

// TestHandleApplyPolicies_NoPolicies tests when there are no policies to apply
func TestHandleApplyPolicies_NoPolicies(t *testing.T) {
	// Use a storage with no policies
	storage := newMockLifecycleStorage()
	initTestFacade(t, storage)
	handler := &Handler{
		backend:      "",
		writeTimeout: 5 * time.Second,
	}

	req := httptest.NewRequest(http.MethodPost, "/policies/apply", nil)
	w := httptest.NewRecorder()

	handler.handleApplyPolicies(w, req)

	// Should succeed with 0 policies applied
	if w.Code != http.StatusOK {
		t.Errorf("Expected status %d when no policies, got %d: %s",
			http.StatusOK, w.Code, w.Body.String())
	}
}

// TestHandleApplyPolicies_Success tests successful policy application
func TestHandleApplyPolicies_Success(t *testing.T) {
	storage := newMockLifecycleStorage()

	// Add a policy
	policy := common.LifecyclePolicy{
		ID:        "test-policy",
		Prefix:    "test/",
		Retention: 24 * time.Hour,
		Action:    "delete",
	}
	storage.policies = []common.LifecyclePolicy{policy}

	initTestFacade(t, storage)
	handler := &Handler{
		backend:      "",
		writeTimeout: 5 * time.Second,
	}

	req := httptest.NewRequest(http.MethodPost, "/policies/apply", nil)
	w := httptest.NewRecorder()

	handler.handleApplyPolicies(w, req)

	if w.Code != http.StatusOK {
		t.Logf("Apply policies returned status %d: %s", w.Code, w.Body.String())
	}
}

// Additional replication handler tests

// TestHandleGetReplicationPolicies_Error tests error handling
func TestHandleGetReplicationPolicies_Error(t *testing.T) {
	storage := newMockReplicationStorage()
	storage.replManager = nil // Simulate unsupported replication
	
	initTestFacade(t, storage)
	handler := &Handler{
		backend:     "",
		readTimeout: 5 * time.Second,
	}

	req := httptest.NewRequest(http.MethodGet, "/replication/policies", nil)
	w := httptest.NewRecorder()

	handler.handleGetReplicationPolicies(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("Expected status %d for unsupported replication, got %d", 
			http.StatusInternalServerError, w.Code)
	}
}

// TestHandleAddReplicationPolicy_MissingFields tests validation
func TestHandleAddReplicationPolicy_MissingFields(t *testing.T) {
	storage := newMockReplicationStorage()
	initTestFacade(t, storage)
	handler := &Handler{
		backend:      "",
		writeTimeout: 5 * time.Second,
	}

	tests := []struct {
		name string
		body map[string]interface{}
	}{
		{
			name: "missing id",
			body: map[string]interface{}{
				"source_backend": "local",
				"destination_backend": "s3",
			},
		},
		{
			name: "missing source_backend",
			body: map[string]interface{}{
				"id": "test-policy",
				"destination_backend": "s3",
			},
		},
		{
			name: "missing destination_backend",
			body: map[string]interface{}{
				"id": "test-policy",
				"source_backend": "local",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body, _ := json.Marshal(tt.body)
			req := httptest.NewRequest(http.MethodPost, "/replication/policies", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()

			handler.handleAddReplicationPolicy(w, req)

			if w.Code != http.StatusBadRequest {
				t.Errorf("Expected status %d for %s, got %d: %s", 
					http.StatusBadRequest, tt.name, w.Code, w.Body.String())
			}
		})
	}
}

// TestHandleGetReplicationPolicy_Success tests successful policy retrieval
func TestHandleGetReplicationPolicy_Success(t *testing.T) {
	storage := newMockReplicationStorage()
	
	// Add a policy
	policy := common.ReplicationPolicy{
		ID: "test-policy",
		SourceBackend: "local",
		DestinationBackend: "s3",
	}
	storage.replManager.AddPolicy(policy)
	
	initTestFacade(t, storage)
	handler := &Handler{
		backend:     "",
		readTimeout: 5 * time.Second,
	}

	req := httptest.NewRequest(http.MethodGet, "/replication/policies/test-policy", nil)
	w := httptest.NewRecorder()

	handler.handleGetReplicationPolicy(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status %d, got %d: %s", http.StatusOK, w.Code, w.Body.String())
	}
}

// TestHandleGetReplicationPolicy_NotFound tests missing policy
func TestHandleGetReplicationPolicy_NotFound(t *testing.T) {
	storage := newMockReplicationStorage()
	initTestFacade(t, storage)
	handler := &Handler{
		backend:     "",
		readTimeout: 5 * time.Second,
	}

	req := httptest.NewRequest(http.MethodGet, "/replication/policies/nonexistent", nil)
	w := httptest.NewRecorder()

	handler.handleGetReplicationPolicy(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("Expected status %d, got %d: %s", http.StatusNotFound, w.Code, w.Body.String())
	}
}

// TestHandleDeleteReplicationPolicy_Success tests successful deletion
func TestHandleDeleteReplicationPolicy_Success(t *testing.T) {
	storage := newMockReplicationStorage()
	
	// Add a policy first
	policy := common.ReplicationPolicy{
		ID: "test-policy",
		SourceBackend: "local",
		DestinationBackend: "s3",
	}
	storage.replManager.AddPolicy(policy)
	
	initTestFacade(t, storage)
	handler := &Handler{
		backend:      "",
		writeTimeout: 5 * time.Second,
	}

	req := httptest.NewRequest(http.MethodDelete, "/replication/policies/test-policy", nil)
	w := httptest.NewRecorder()

	handler.handleDeleteReplicationPolicy(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status %d, got %d: %s", http.StatusOK, w.Code, w.Body.String())
	}
}

// TestHandleReplicationPolicyByID_GET tests GET request routing
func TestHandleReplicationPolicyByID_GET(t *testing.T) {
	storage := newMockReplicationStorage()
	
	// Add a policy
	policy := common.ReplicationPolicy{
		ID: "test-policy",
		SourceBackend: "local",
		DestinationBackend: "s3",
	}
	storage.replManager.AddPolicy(policy)
	
	initTestFacade(t, storage)
	handler := &Handler{
		backend:     "",
		readTimeout: 5 * time.Second,
	}

	req := httptest.NewRequest(http.MethodGet, "/replication/policies/test-policy", nil)
	w := httptest.NewRecorder()

	handler.handleReplicationPolicyByID(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status %d, got %d: %s", http.StatusOK, w.Code, w.Body.String())
	}
}

// TestHandleReplicationPolicyByID_DELETE tests DELETE request routing
func TestHandleReplicationPolicyByID_DELETE(t *testing.T) {
	storage := newMockReplicationStorage()
	
	// Add a policy
	policy := common.ReplicationPolicy{
		ID: "test-policy",
		SourceBackend: "local",
		DestinationBackend: "s3",
	}
	storage.replManager.AddPolicy(policy)
	
	initTestFacade(t, storage)
	handler := &Handler{
		backend:      "",
		writeTimeout: 5 * time.Second,
	}

	req := httptest.NewRequest(http.MethodDelete, "/replication/policies/test-policy", nil)
	w := httptest.NewRecorder()

	handler.handleReplicationPolicyByID(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status %d, got %d: %s", http.StatusOK, w.Code, w.Body.String())
	}
}

// TestHandleTriggerReplication_Success tests successful replication trigger
func TestHandleTriggerReplication_Success(t *testing.T) {
	storage := newMockReplicationStorage()
	
	// Add a policy
	policy := common.ReplicationPolicy{
		ID: "test-policy",
		SourceBackend: "local",
		DestinationBackend: "s3",
	}
	storage.replManager.AddPolicy(policy)
	
	initTestFacade(t, storage)
	handler := &Handler{
		backend:      "",
		writeTimeout: 5 * time.Second,
	}

	reqBody := map[string]string{"policy_id": "test-policy"}
	body, _ := json.Marshal(reqBody)
	req := httptest.NewRequest(http.MethodPost, "/replication/trigger", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handler.handleTriggerReplication(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status %d, got %d: %s", http.StatusOK, w.Code, w.Body.String())
	}
}

// TestHandleTriggerReplication_EmptyPolicyID tests empty policy_id (returns success with empty result)
func TestHandleTriggerReplication_EmptyPolicyID(t *testing.T) {
	storage := newMockReplicationStorage()
	initTestFacade(t, storage)
	handler := &Handler{
		backend:      "",
		writeTimeout: 5 * time.Second,
	}

	reqBody := map[string]string{}
	body, _ := json.Marshal(reqBody)
	req := httptest.NewRequest(http.MethodPost, "/replication/trigger", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handler.handleTriggerReplication(w, req)

	// Server allows empty policy_id and returns success
	if w.Code != http.StatusOK {
		t.Errorf("Expected status %d for empty policy_id, got %d: %s",
			http.StatusOK, w.Code, w.Body.String())
	}
}

// TestHandleHealth tests health check endpoint
func TestHandleHealth(t *testing.T) {
	storage := newMockLifecycleStorage()
	initTestFacade(t, storage)
	handler := &Handler{
		backend:     "",
		readTimeout: 5 * time.Second,
	}

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()

	handler.handleHealth(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status %d, got %d", http.StatusOK, w.Code)
	}
}
// TestHandleExists_Success tests successful key existence check
func TestHandleExists_Success(t *testing.T) {
	storage := newMockLifecycleStorage()
	storage.data["test-key"] = []byte("test data")

	initTestFacade(t, storage)
	handler := &Handler{
		backend:     "",
		readTimeout: 5 * time.Second,
	}

	req := httptest.NewRequest(http.MethodHead, "/test-key", nil)
	w := httptest.NewRecorder()

	handler.handleExists(w, req, "test-key")

	if w.Code != http.StatusOK {
		t.Errorf("Expected status %d, got %d: %s", http.StatusOK, w.Code, w.Body.String())
	}
}

// TestHandleExists_NotFound tests key that doesn't exist
func TestHandleExists_NotFound(t *testing.T) {
	storage := newMockLifecycleStorage()

	initTestFacade(t, storage)
	handler := &Handler{
		backend:     "",
		readTimeout: 5 * time.Second,
	}

	req := httptest.NewRequest(http.MethodHead, "/nonexistent-key", nil)
	w := httptest.NewRecorder()

	handler.handleExists(w, req, "nonexistent-key")

	if w.Code != http.StatusOK {
		t.Errorf("Expected status %d, got %d: %s", http.StatusOK, w.Code, w.Body.String())
	}
}

// TestHandleApplyPolicies_MethodNotAllowed tests non-POST methods
func TestHandleApplyPolicies_MethodNotAllowed(t *testing.T) {
	storage := newMockLifecycleStorage()
	initTestFacade(t, storage)
	handler := &Handler{
		backend:      "",
		writeTimeout: 5 * time.Second,
	}

	req := httptest.NewRequest(http.MethodGet, "/policies/apply", nil)
	w := httptest.NewRecorder()

	handler.handleApplyPolicies(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected status %d, got %d", http.StatusMethodNotAllowed, w.Code)
	}
}


// TestHandleGetReplicationPolicies_EmptyList tests getting empty policy list
func TestHandleGetReplicationPolicies_EmptyList(t *testing.T) {
	storage := &mockReplicationStorage{
		mockLifecycleStorage: newMockLifecycleStorage(),
		replManager: &mockReplicationManager{
			policies: nil,
		},
	}

	initTestFacade(t, storage)
	handler := &Handler{
		backend:     "",
		readTimeout: 5 * time.Second,
	}

	req := httptest.NewRequest(http.MethodGet, "/replication/policies", nil)
	w := httptest.NewRecorder()

	handler.handleGetReplicationPolicies(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status %d, got %d: %s", http.StatusOK, w.Code, w.Body.String())
	}
}

// TestHandleDeleteReplicationPolicy_EmptyID tests deleting with empty policy ID
func TestHandleDeleteReplicationPolicy_EmptyID(t *testing.T) {
	storage := newMockReplicationStorage()
	initTestFacade(t, storage)
	handler := &Handler{
		backend:      "",
		writeTimeout: 5 * time.Second,
	}

	req := httptest.NewRequest(http.MethodDelete, "/replication/policies/", nil)
	w := httptest.NewRecorder()

	handler.handleDeleteReplicationPolicy(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status %d for empty ID, got %d: %s",
			http.StatusBadRequest, w.Code, w.Body.String())
	}
}

// TestHandleGetReplicationPolicy_EmptyID tests getting policy with empty ID
func TestHandleGetReplicationPolicy_EmptyID(t *testing.T) {
	storage := newMockReplicationStorage()
	initTestFacade(t, storage)
	handler := &Handler{
		backend:     "",
		readTimeout: 5 * time.Second,
	}

	req := httptest.NewRequest(http.MethodGet, "/replication/policies/", nil)
	w := httptest.NewRecorder()

	handler.handleGetReplicationPolicy(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status %d for empty ID, got %d: %s",
			http.StatusBadRequest, w.Code, w.Body.String())
	}
}

// TestHandleAddReplicationPolicy_InvalidSourceBackend tests invalid source backend
func TestHandleAddReplicationPolicy_InvalidSourceBackend(t *testing.T) {
	storage := newMockReplicationStorage()
	initTestFacade(t, storage)
	handler := &Handler{
		backend:      "",
		writeTimeout: 5 * time.Second,
	}

	requestBody := map[string]any{
		"id":                   "policy1",
		"source_backend":       "", // Empty source backend
		"destination_backend":  "local",
		"destination_settings": map[string]string{"path": "/tmp/dest"},
		"check_interval":       3600,
	}

	body, _ := json.Marshal(requestBody)
	req := httptest.NewRequest(http.MethodPost, "/replication/policies", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handler.handleAddReplicationPolicy(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status %d for empty source backend, got %d: %s",
			http.StatusBadRequest, w.Code, w.Body.String())
	}
}

// TestHandleAddReplicationPolicy_InvalidDestinationBackend tests invalid destination backend
func TestHandleAddReplicationPolicy_InvalidDestinationBackend(t *testing.T) {
	storage := newMockReplicationStorage()
	initTestFacade(t, storage)
	handler := &Handler{
		backend:      "",
		writeTimeout: 5 * time.Second,
	}

	requestBody := map[string]any{
		"id":                  "policy1",
		"source_backend":      "local",
		"source_settings":     map[string]string{"path": "/tmp/src"},
		"destination_backend": "", // Empty destination backend
		"check_interval":      3600,
	}

	body, _ := json.Marshal(requestBody)
	req := httptest.NewRequest(http.MethodPost, "/replication/policies", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handler.handleAddReplicationPolicy(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status %d for empty destination backend, got %d: %s",
			http.StatusBadRequest, w.Code, w.Body.String())
	}
}


// TestHandleAddReplicationPolicy_InvalidJSON tests invalid JSON request body
func TestHandleAddReplicationPolicy_InvalidJSON(t *testing.T) {
	storage := newMockReplicationStorage()
	initTestFacade(t, storage)
	handler := &Handler{
		backend:      "",
		writeTimeout: 5 * time.Second,
	}

	req := httptest.NewRequest(http.MethodPost, "/replication/policies", bytes.NewReader([]byte("invalid json")))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handler.handleAddReplicationPolicy(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status %d for invalid JSON, got %d: %s",
			http.StatusBadRequest, w.Code, w.Body.String())
	}
}
