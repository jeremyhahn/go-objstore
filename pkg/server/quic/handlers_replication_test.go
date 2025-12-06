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

//go:build local

package quic

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/common"
	replicationPkg "github.com/jeremyhahn/go-objstore/pkg/replication"
)

// MockReplicationManager implements common.ReplicationManager for testing
type MockReplicationManager struct {
	policies            map[string]common.ReplicationPolicy
	replicationStatuses map[string]*replicationPkg.ReplicationStatus
	syncCalled          bool
	syncPolicyID        string
	syncAllCalled       bool
	addError            error
	removeError         error
	getError            error
	syncError           error
	getStatusError      error
}

func NewMockReplicationManager() *MockReplicationManager {
	return &MockReplicationManager{
		policies:            make(map[string]common.ReplicationPolicy),
		replicationStatuses: make(map[string]*replicationPkg.ReplicationStatus),
	}
}

// GetReplicationStatus implements the optional status provider interface
func (m *MockReplicationManager) GetReplicationStatus(id string) (*replicationPkg.ReplicationStatus, error) {
	if m.getStatusError != nil {
		return nil, m.getStatusError
	}
	status, exists := m.replicationStatuses[id]
	if !exists {
		return nil, common.ErrPolicyNotFound
	}
	return status, nil
}

func (m *MockReplicationManager) AddPolicy(policy common.ReplicationPolicy) error {
	if m.addError != nil {
		return m.addError
	}
	m.policies[policy.ID] = policy
	return nil
}

func (m *MockReplicationManager) RemovePolicy(id string) error {
	if m.removeError != nil {
		return m.removeError
	}
	if _, exists := m.policies[id]; !exists {
		return common.ErrPolicyNotFound
	}
	delete(m.policies, id)
	return nil
}

func (m *MockReplicationManager) GetPolicy(id string) (*common.ReplicationPolicy, error) {
	if m.getError != nil {
		return nil, m.getError
	}
	policy, exists := m.policies[id]
	if !exists {
		return nil, common.ErrPolicyNotFound
	}
	return &policy, nil
}

func (m *MockReplicationManager) GetPolicies() ([]common.ReplicationPolicy, error) {
	if m.getError != nil {
		return nil, m.getError
	}
	policies := make([]common.ReplicationPolicy, 0, len(m.policies))
	for _, p := range m.policies {
		policies = append(policies, p)
	}
	return policies, nil
}

func (m *MockReplicationManager) SyncAll(ctx context.Context) (*common.SyncResult, error) {
	if m.syncError != nil {
		return nil, m.syncError
	}
	m.syncAllCalled = true
	return &common.SyncResult{
		PolicyID:   "all",
		Synced:     10,
		Deleted:    2,
		Failed:     1,
		BytesTotal: 1024,
		Duration:   5 * time.Second,
	}, nil
}

func (m *MockReplicationManager) SyncPolicy(ctx context.Context, policyID string) (*common.SyncResult, error) {
	if m.syncError != nil {
		return nil, m.syncError
	}
	if _, exists := m.policies[policyID]; !exists {
		return nil, common.ErrPolicyNotFound
	}
	m.syncCalled = true
	m.syncPolicyID = policyID
	return &common.SyncResult{
		PolicyID:   policyID,
		Synced:     5,
		Deleted:    1,
		Failed:     0,
		BytesTotal: 512,
		Duration:   2 * time.Second,
	}, nil
}

func (m *MockReplicationManager) SetBackendEncrypterFactory(policyID string, factory common.EncrypterFactory) error {
	return nil
}

func (m *MockReplicationManager) SetSourceEncrypterFactory(policyID string, factory common.EncrypterFactory) error {
	return nil
}

func (m *MockReplicationManager) SetDestinationEncrypterFactory(policyID string, factory common.EncrypterFactory) error {
	return nil
}

func (m *MockReplicationManager) Run(ctx context.Context) {
	// No-op for testing
}

// MockStorageWithReplication is a test storage that supports replication
type MockStorageWithReplication struct {
	repMgr *MockReplicationManager
	repErr error
}

func NewMockStorageWithReplication() *MockStorageWithReplication {
	return &MockStorageWithReplication{
		repMgr: NewMockReplicationManager(),
	}
}

func (m *MockStorageWithReplication) GetReplicationManager() (common.ReplicationManager, error) {
	if m.repErr != nil {
		return nil, m.repErr
	}
	return m.repMgr, nil
}

// Implement required Storage interface methods (minimal implementation for testing)
func (m *MockStorageWithReplication) Configure(settings map[string]string) error { return nil }
func (m *MockStorageWithReplication) Put(key string, data io.Reader) error       { return nil }
func (m *MockStorageWithReplication) PutWithContext(ctx context.Context, key string, data io.Reader) error {
	return nil
}
func (m *MockStorageWithReplication) PutWithMetadata(ctx context.Context, key string, data io.Reader, metadata *common.Metadata) error {
	return nil
}
func (m *MockStorageWithReplication) Get(key string) (io.ReadCloser, error) { return nil, nil }
func (m *MockStorageWithReplication) GetWithContext(ctx context.Context, key string) (io.ReadCloser, error) {
	return nil, nil
}
func (m *MockStorageWithReplication) GetMetadata(ctx context.Context, key string) (*common.Metadata, error) {
	return nil, nil
}
func (m *MockStorageWithReplication) UpdateMetadata(ctx context.Context, key string, metadata *common.Metadata) error {
	return nil
}
func (m *MockStorageWithReplication) Delete(key string) error { return nil }
func (m *MockStorageWithReplication) DeleteWithContext(ctx context.Context, key string) error {
	return nil
}
func (m *MockStorageWithReplication) Exists(ctx context.Context, key string) (bool, error) {
	return false, nil
}
func (m *MockStorageWithReplication) List(prefix string) ([]string, error) { return nil, nil }
func (m *MockStorageWithReplication) ListWithContext(ctx context.Context, prefix string) ([]string, error) {
	return nil, nil
}
func (m *MockStorageWithReplication) ListWithOptions(ctx context.Context, opts *common.ListOptions) (*common.ListResult, error) {
	return nil, nil
}
func (m *MockStorageWithReplication) Archive(key string, destination common.Archiver) error {
	return nil
}
func (m *MockStorageWithReplication) SetLifecyclePolicy(policy *common.LifecyclePolicy) error {
	return nil
}
func (m *MockStorageWithReplication) GetLifecyclePolicy() *common.LifecyclePolicy    { return nil }
func (m *MockStorageWithReplication) RunLifecycle(ctx context.Context) error         { return nil }
func (m *MockStorageWithReplication) AddPolicy(policy common.LifecyclePolicy) error  { return nil }
func (m *MockStorageWithReplication) RemovePolicy(id string) error                   { return nil }
func (m *MockStorageWithReplication) GetPolicies() ([]common.LifecyclePolicy, error) { return nil, nil }

// MockStorageNoReplication is a test storage without replication support
type MockStorageNoReplication struct {
	MockStorageWithReplication
}

func NewMockStorageNoReplication() *MockStorageNoReplication {
	return &MockStorageNoReplication{}
}

// MockStorageNoReplication doesn't implement GetReplicationManager

func setupReplicationTestHandler(t *testing.T, storage common.Storage) *Handler {
	t.Helper()
	logger := adapters.NewNoOpLogger()
	auth := adapters.NewNoOpAuthenticator()
	return createHandlerWithStorage(t, storage, 10*1024*1024, 30*time.Second, 30*time.Second, logger, auth)
}

func TestHandleAddReplicationPolicy(t *testing.T) {
	storage := NewMockStorageWithReplication()
	handler := setupReplicationTestHandler(t, storage)

	tests := []struct {
		name           string
		requestBody    map[string]any
		expectedStatus int
		expectInMgr    bool
	}{
		{
			name: "valid policy",
			requestBody: map[string]any{
				"id":                  "test-policy-1",
				"source_backend":      "local",
				"destination_backend": "s3",
				"check_interval":      300,
				"enabled":             true,
				"replication_mode":    "transparent",
			},
			expectedStatus: http.StatusCreated,
			expectInMgr:    true,
		},
		{
			name: "with source settings",
			requestBody: map[string]any{
				"id":                   "test-policy-2",
				"source_backend":       "local",
				"source_settings":      map[string]any{"path": "/data"},
				"destination_backend":  "s3",
				"destination_settings": map[string]any{"bucket": "backup"},
				"check_interval":       600,
				"enabled":              true,
			},
			expectedStatus: http.StatusCreated,
			expectInMgr:    true,
		},
		{
			name: "missing id",
			requestBody: map[string]any{
				"source_backend":      "local",
				"destination_backend": "s3",
				"check_interval":      300,
			},
			expectedStatus: http.StatusBadRequest,
			expectInMgr:    false,
		},
		{
			name: "missing source_backend",
			requestBody: map[string]any{
				"id":                  "test-policy-3",
				"destination_backend": "s3",
				"check_interval":      300,
			},
			expectedStatus: http.StatusBadRequest,
			expectInMgr:    false,
		},
		{
			name: "missing destination_backend",
			requestBody: map[string]any{
				"id":             "test-policy-4",
				"source_backend": "local",
				"check_interval": 300,
			},
			expectedStatus: http.StatusBadRequest,
			expectInMgr:    false,
		},
		{
			name: "invalid check_interval",
			requestBody: map[string]any{
				"id":                  "test-policy-5",
				"source_backend":      "local",
				"destination_backend": "s3",
				"check_interval":      0,
			},
			expectedStatus: http.StatusBadRequest,
			expectInMgr:    false,
		},
		{
			name: "invalid replication_mode",
			requestBody: map[string]any{
				"id":                  "test-policy-6",
				"source_backend":      "local",
				"destination_backend": "s3",
				"check_interval":      300,
				"replication_mode":    "invalid",
			},
			expectedStatus: http.StatusBadRequest,
			expectInMgr:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body, _ := json.Marshal(tt.requestBody)
			req := httptest.NewRequest(http.MethodPost, "/replication/policies", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()

			handler.handleAddReplicationPolicy(w, req)

			if w.Code != tt.expectedStatus {
				t.Errorf("expected status %d, got %d: %s", tt.expectedStatus, w.Code, w.Body.String())
			}

			if tt.expectInMgr {
				id := tt.requestBody["id"].(string)
				policy, err := storage.repMgr.GetPolicy(id)
				if err != nil {
					t.Errorf("expected policy to be added, got error: %v", err)
				}
				if policy.ID != id {
					t.Errorf("expected policy ID %s, got %s", id, policy.ID)
				}
			}
		})
	}
}

func TestHandleGetReplicationPolicies(t *testing.T) {
	storage := NewMockStorageWithReplication()
	handler := setupReplicationTestHandler(t, storage)

	// Add some policies
	storage.repMgr.AddPolicy(common.ReplicationPolicy{
		ID:                 "policy-1",
		SourceBackend:      "local",
		DestinationBackend: "s3",
		CheckInterval:      5 * time.Minute,
		Enabled:            true,
		ReplicationMode:    common.ReplicationModeTransparent,
	})
	storage.repMgr.AddPolicy(common.ReplicationPolicy{
		ID:                 "policy-2",
		SourceBackend:      "s3",
		DestinationBackend: "gcs",
		CheckInterval:      10 * time.Minute,
		Enabled:            false,
		ReplicationMode:    common.ReplicationModeOpaque,
	})

	req := httptest.NewRequest(http.MethodGet, "/replication/policies", nil)
	w := httptest.NewRecorder()

	handler.handleGetReplicationPolicies(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	var response map[string]any
	if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if !response["success"].(bool) {
		t.Error("expected success to be true")
	}

	count := int(response["count"].(float64))
	if count != 2 {
		t.Errorf("expected 2 policies, got %d", count)
	}
}

func TestHandleGetReplicationPolicy(t *testing.T) {
	storage := NewMockStorageWithReplication()
	handler := setupReplicationTestHandler(t, storage)

	// Add a policy
	storage.repMgr.AddPolicy(common.ReplicationPolicy{
		ID:                 "test-policy",
		SourceBackend:      "local",
		DestinationBackend: "s3",
		CheckInterval:      5 * time.Minute,
		Enabled:            true,
		ReplicationMode:    common.ReplicationModeTransparent,
	})

	tests := []struct {
		name           string
		policyID       string
		expectedStatus int
	}{
		{
			name:           "existing policy",
			policyID:       "test-policy",
			expectedStatus: http.StatusOK,
		},
		{
			name:           "non-existent policy",
			policyID:       "nonexistent",
			expectedStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/replication/policies/"+tt.policyID, nil)
			w := httptest.NewRecorder()

			handler.handleGetReplicationPolicy(w, req)

			if w.Code != tt.expectedStatus {
				t.Errorf("expected status %d, got %d", tt.expectedStatus, w.Code)
			}

			if tt.expectedStatus == http.StatusOK {
				var response map[string]any
				if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
					t.Fatalf("failed to decode response: %v", err)
				}

				if !response["success"].(bool) {
					t.Error("expected success to be true")
				}

				if response["id"].(string) != tt.policyID {
					t.Errorf("expected policy ID %s, got %s", tt.policyID, response["id"])
				}
			}
		})
	}
}

func TestHandleDeleteReplicationPolicy(t *testing.T) {
	storage := NewMockStorageWithReplication()
	handler := setupReplicationTestHandler(t, storage)

	// Add a policy
	storage.repMgr.AddPolicy(common.ReplicationPolicy{
		ID:                 "test-policy",
		SourceBackend:      "local",
		DestinationBackend: "s3",
		CheckInterval:      5 * time.Minute,
		Enabled:            true,
		ReplicationMode:    common.ReplicationModeTransparent,
	})

	tests := []struct {
		name           string
		policyID       string
		expectedStatus int
	}{
		{
			name:           "delete existing policy",
			policyID:       "test-policy",
			expectedStatus: http.StatusOK,
		},
		{
			name:           "delete non-existent policy",
			policyID:       "nonexistent",
			expectedStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodDelete, "/replication/policies/"+tt.policyID, nil)
			w := httptest.NewRecorder()

			handler.handleDeleteReplicationPolicy(w, req)

			if w.Code != tt.expectedStatus {
				t.Errorf("expected status %d, got %d", tt.expectedStatus, w.Code)
			}
		})
	}
}

func TestHandleTriggerReplication(t *testing.T) {
	storage := NewMockStorageWithReplication()
	handler := setupReplicationTestHandler(t, storage)

	// Add a policy
	storage.repMgr.AddPolicy(common.ReplicationPolicy{
		ID:                 "test-policy",
		SourceBackend:      "local",
		DestinationBackend: "s3",
		CheckInterval:      5 * time.Minute,
		Enabled:            true,
		ReplicationMode:    common.ReplicationModeTransparent,
	})

	tests := []struct {
		name           string
		policyID       string
		expectedStatus int
		checkSyncAll   bool
		checkSyncID    bool
	}{
		{
			name:           "sync specific policy",
			policyID:       "test-policy",
			expectedStatus: http.StatusOK,
			checkSyncID:    true,
		},
		{
			name:           "sync all policies",
			policyID:       "",
			expectedStatus: http.StatusOK,
			checkSyncAll:   true,
		},
		{
			name:           "sync non-existent policy",
			policyID:       "nonexistent",
			expectedStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset flags
			storage.repMgr.syncCalled = false
			storage.repMgr.syncAllCalled = false
			storage.repMgr.syncPolicyID = ""

			url := "/replication/trigger"
			if tt.policyID != "" {
				url += "?policy_id=" + tt.policyID
			}

			req := httptest.NewRequest(http.MethodPost, url, nil)
			w := httptest.NewRecorder()

			handler.handleTriggerReplication(w, req)

			if w.Code != tt.expectedStatus {
				t.Errorf("expected status %d, got %d", tt.expectedStatus, w.Code)
			}

			if tt.expectedStatus == http.StatusOK {
				var response map[string]any
				if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
					t.Fatalf("failed to decode response: %v", err)
				}

				if !response["success"].(bool) {
					t.Error("expected success to be true")
				}

				if tt.checkSyncAll && !storage.repMgr.syncAllCalled {
					t.Error("expected SyncAll to be called")
				}

				if tt.checkSyncID {
					if !storage.repMgr.syncCalled {
						t.Error("expected SyncPolicy to be called")
					}
					if storage.repMgr.syncPolicyID != tt.policyID {
						t.Errorf("expected sync policy ID %s, got %s", tt.policyID, storage.repMgr.syncPolicyID)
					}
				}
			}
		})
	}
}

func TestHandleReplicationPolicyByID_InvalidMethod(t *testing.T) {
	storage := NewMockStorageWithReplication()
	handler := setupReplicationTestHandler(t, storage)

	req := httptest.NewRequest(http.MethodPost, "/replication/policies/test", nil)
	w := httptest.NewRecorder()

	handler.handleReplicationPolicyByID(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected status 405, got %d", w.Code)
	}
}

func TestHandleTriggerReplication_InvalidMethod(t *testing.T) {
	storage := NewMockStorageWithReplication()
	handler := setupReplicationTestHandler(t, storage)

	req := httptest.NewRequest(http.MethodGet, "/replication/trigger", nil)
	w := httptest.NewRecorder()

	handler.handleTriggerReplication(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected status 405, got %d", w.Code)
	}
}

func TestReplicationWithoutSupport(t *testing.T) {
	// Create storage without replication support
	storage := &struct {
		common.Storage
	}{}

	handler := setupReplicationTestHandler(t, storage)

	tests := []struct {
		name    string
		handler func(w http.ResponseWriter, r *http.Request)
		method  string
		path    string
	}{
		{
			name:    "add policy",
			handler: handler.handleAddReplicationPolicy,
			method:  http.MethodPost,
			path:    "/replication/policies",
		},
		{
			name:    "get policies",
			handler: handler.handleGetReplicationPolicies,
			method:  http.MethodGet,
			path:    "/replication/policies",
		},
		{
			name:    "get policy",
			handler: handler.handleGetReplicationPolicy,
			method:  http.MethodGet,
			path:    "/replication/policies/test",
		},
		{
			name:    "delete policy",
			handler: handler.handleDeleteReplicationPolicy,
			method:  http.MethodDelete,
			path:    "/replication/policies/test",
		},
		{
			name:    "trigger replication",
			handler: handler.handleTriggerReplication,
			method:  http.MethodPost,
			path:    "/replication/trigger",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var body *bytes.Reader
			if tt.method == http.MethodPost {
				body = bytes.NewReader([]byte(`{"id":"test","source_backend":"local","destination_backend":"s3","check_interval":300}`))
			} else {
				body = bytes.NewReader([]byte{})
			}

			req := httptest.NewRequest(tt.method, tt.path, body)
			if tt.method == http.MethodPost {
				req.Header.Set("Content-Type", "application/json")
			}
			w := httptest.NewRecorder()

			tt.handler(w, req)

			if w.Code != http.StatusInternalServerError {
				t.Errorf("expected status 500, got %d", w.Code)
			}
		})
	}
}

func TestReplicationErrorHandling(t *testing.T) {
	storage := NewMockStorageWithReplication()
	handler := setupReplicationTestHandler(t, storage)

	// Add a policy first
	storage.repMgr.AddPolicy(common.ReplicationPolicy{
		ID:                 "test-policy",
		SourceBackend:      "local",
		DestinationBackend: "s3",
		CheckInterval:      5 * time.Minute,
		Enabled:            true,
	})

	t.Run("add duplicate policy", func(t *testing.T) {
		storage.repMgr.addError = errors.New("policy already exists")
		defer func() { storage.repMgr.addError = nil }()

		body, _ := json.Marshal(map[string]any{
			"id":                  "dup-policy",
			"source_backend":      "local",
			"destination_backend": "s3",
			"check_interval":      300,
		})
		req := httptest.NewRequest(http.MethodPost, "/replication/policies", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		handler.handleAddReplicationPolicy(w, req)

		if w.Code != http.StatusConflict {
			t.Errorf("expected status 409, got %d", w.Code)
		}
	})

	t.Run("sync error", func(t *testing.T) {
		storage.repMgr.syncError = errors.New("sync failed")
		defer func() { storage.repMgr.syncError = nil }()

		req := httptest.NewRequest(http.MethodPost, "/replication/trigger?policy_id=test-policy", nil)
		w := httptest.NewRecorder()

		handler.handleTriggerReplication(w, req)

		if w.Code != http.StatusInternalServerError {
			t.Errorf("expected status 500, got %d", w.Code)
		}
	})
}

// Tests for GetReplicationStatus

func TestHandleGetReplicationStatus_Success(t *testing.T) {
	storage := NewMockStorageWithReplication()
	handler := setupReplicationTestHandler(t, storage)

	// Set up a replication status
	storage.repMgr.replicationStatuses["test-policy"] = &replicationPkg.ReplicationStatus{
		PolicyID:           "test-policy",
		SourceBackend:      "local",
		DestinationBackend: "s3",
		Enabled:            true,
		TotalObjectsSynced: 100,
		TotalBytesSynced:   1024 * 1024,
	}

	req := httptest.NewRequest(http.MethodGet, "/replication/status/test-policy", nil)
	w := httptest.NewRecorder()

	handler.handleGetReplicationStatus(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	var response map[string]any
	if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if !response["success"].(bool) {
		t.Error("expected success to be true")
	}

	if response["policy_id"].(string) != "test-policy" {
		t.Errorf("expected policy_id 'test-policy', got %s", response["policy_id"])
	}
}

func TestHandleGetReplicationStatus_EmptyID(t *testing.T) {
	storage := NewMockStorageWithReplication()
	handler := setupReplicationTestHandler(t, storage)

	req := httptest.NewRequest(http.MethodGet, "/replication/status/", nil)
	w := httptest.NewRecorder()

	handler.handleGetReplicationStatus(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", w.Code)
	}
}

func TestHandleGetReplicationStatus_NotFound(t *testing.T) {
	storage := NewMockStorageWithReplication()
	handler := setupReplicationTestHandler(t, storage)

	req := httptest.NewRequest(http.MethodGet, "/replication/status/nonexistent", nil)
	w := httptest.NewRecorder()

	handler.handleGetReplicationStatus(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected status 404, got %d", w.Code)
	}
}

func TestHandleGetReplicationStatus_ReplicationNotSupported(t *testing.T) {
	// Create storage without replication support
	storage := &struct {
		common.Storage
	}{}

	handler := setupReplicationTestHandler(t, storage)

	req := httptest.NewRequest(http.MethodGet, "/replication/status/test-policy", nil)
	w := httptest.NewRecorder()

	handler.handleGetReplicationStatus(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected status 500, got %d", w.Code)
	}
}

func TestHandleGetReplicationStatus_GetReplicationManagerError(t *testing.T) {
	storage := NewMockStorageWithReplication()
	storage.repErr = errors.New("manager unavailable")
	handler := setupReplicationTestHandler(t, storage)

	req := httptest.NewRequest(http.MethodGet, "/replication/status/test-policy", nil)
	w := httptest.NewRecorder()

	handler.handleGetReplicationStatus(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected status 500, got %d", w.Code)
	}
}

func TestHandleGetReplicationStatus_GenericError(t *testing.T) {
	storage := NewMockStorageWithReplication()
	storage.repMgr.getStatusError = errors.New("internal error")
	handler := setupReplicationTestHandler(t, storage)

	req := httptest.NewRequest(http.MethodGet, "/replication/status/test-policy", nil)
	w := httptest.NewRecorder()

	handler.handleGetReplicationStatus(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected status 500, got %d", w.Code)
	}
}
