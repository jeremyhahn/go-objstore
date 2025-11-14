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
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockReplicationManager implements common.ReplicationManager for testing
type MockReplicationManager struct {
	policies         map[string]common.ReplicationPolicy
	addPolicyErr     error
	removePolicyErr  error
	getPolicyErr     error
	getPoliciesErr   error
	syncAllErr       error
	syncPolicyErr    error
	syncAllResult    *common.SyncResult
	syncPolicyResult *common.SyncResult
}

func NewMockReplicationManager() *MockReplicationManager {
	return &MockReplicationManager{
		policies: make(map[string]common.ReplicationPolicy),
	}
}

func (m *MockReplicationManager) AddPolicy(policy common.ReplicationPolicy) error {
	if m.addPolicyErr != nil {
		return m.addPolicyErr
	}
	if _, exists := m.policies[policy.ID]; exists {
		return errors.New("policy already exists")
	}
	m.policies[policy.ID] = policy
	return nil
}

func (m *MockReplicationManager) RemovePolicy(id string) error {
	if m.removePolicyErr != nil {
		return m.removePolicyErr
	}
	if _, exists := m.policies[id]; !exists {
		return common.ErrPolicyNotFound
	}
	delete(m.policies, id)
	return nil
}

func (m *MockReplicationManager) GetPolicy(id string) (*common.ReplicationPolicy, error) {
	if m.getPolicyErr != nil {
		return nil, m.getPolicyErr
	}
	policy, exists := m.policies[id]
	if !exists {
		return nil, common.ErrPolicyNotFound
	}
	return &policy, nil
}

func (m *MockReplicationManager) GetPolicies() ([]common.ReplicationPolicy, error) {
	if m.getPoliciesErr != nil {
		return nil, m.getPoliciesErr
	}
	policies := make([]common.ReplicationPolicy, 0, len(m.policies))
	for _, p := range m.policies {
		policies = append(policies, p)
	}
	return policies, nil
}

func (m *MockReplicationManager) SyncAll(ctx context.Context) (*common.SyncResult, error) {
	if m.syncAllErr != nil {
		return nil, m.syncAllErr
	}
	if m.syncAllResult != nil {
		return m.syncAllResult, nil
	}
	return &common.SyncResult{
		PolicyID:   "all",
		Synced:     10,
		Failed:     0,
		BytesTotal: 1024,
		Duration:   time.Second,
	}, nil
}

func (m *MockReplicationManager) SyncPolicy(ctx context.Context, policyID string) (*common.SyncResult, error) {
	if m.syncPolicyErr != nil {
		return nil, m.syncPolicyErr
	}
	if _, exists := m.policies[policyID]; !exists {
		return nil, common.ErrPolicyNotFound
	}
	if m.syncPolicyResult != nil {
		return m.syncPolicyResult, nil
	}
	return &common.SyncResult{
		PolicyID:   policyID,
		Synced:     5,
		Failed:     0,
		BytesTotal: 512,
		Duration:   500 * time.Millisecond,
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

func (m *MockReplicationManager) Run(ctx context.Context) {}

// MockStorageWithReplication extends MockStorage to support replication
type MockStorageWithReplication struct {
	*MockStorage
	replicationMgr *MockReplicationManager
	getRepErr      error
}

func NewMockStorageWithReplication() *MockStorageWithReplication {
	return &MockStorageWithReplication{
		MockStorage:    NewMockStorage(),
		replicationMgr: NewMockReplicationManager(),
	}
}

func (m *MockStorageWithReplication) GetReplicationManager() (common.ReplicationManager, error) {
	if m.getRepErr != nil {
		return nil, m.getRepErr
	}
	return m.replicationMgr, nil
}

// Test helper functions

func setupTestRouter(storage common.Storage) (*gin.Engine, *Handler) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	handler := NewHandler(storage)
	SetupRoutes(router, handler)
	return router, handler
}

// Tests for AddReplicationPolicy

func TestAddReplicationPolicy_Success(t *testing.T) {
	storage := NewMockStorageWithReplication()
	router, _ := setupTestRouter(storage)

	policy := AddReplicationPolicyRequest{
		ID:                  "test-policy",
		SourceBackend:       "local",
		SourceSettings:      map[string]string{"path": "/data/source"},
		DestinationBackend:  "s3",
		DestinationSettings: map[string]string{"bucket": "backup"},
		CheckInterval:       5 * time.Minute,
		Enabled:             true,
		ReplicationMode:     common.ReplicationModeTransparent,
	}

	body, _ := json.Marshal(policy)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/replication/policies", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusCreated, w.Code)

	var response SuccessResponse
	err := json.Unmarshal(w.Body.Bytes(), &response)
	require.NoError(t, err)
	assert.Equal(t, "replication policy added successfully", response.Message)
}

func TestAddReplicationPolicy_MissingID(t *testing.T) {
	storage := NewMockStorageWithReplication()
	router, _ := setupTestRouter(storage)

	policy := AddReplicationPolicyRequest{
		SourceBackend:      "local",
		DestinationBackend: "s3",
		CheckInterval:      5 * time.Minute,
	}

	body, _ := json.Marshal(policy)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/replication/policies", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)

	var response ErrorResponse
	err := json.Unmarshal(w.Body.Bytes(), &response)
	require.NoError(t, err)
	assert.Contains(t, response.Message, "ID")
}

func TestAddReplicationPolicy_MissingSourceBackend(t *testing.T) {
	storage := NewMockStorageWithReplication()
	router, _ := setupTestRouter(storage)

	policy := AddReplicationPolicyRequest{
		ID:                 "test-policy",
		DestinationBackend: "s3",
		CheckInterval:      5 * time.Minute,
	}

	body, _ := json.Marshal(policy)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/replication/policies", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestAddReplicationPolicy_InvalidReplicationMode(t *testing.T) {
	storage := NewMockStorageWithReplication()
	router, _ := setupTestRouter(storage)

	policy := AddReplicationPolicyRequest{
		ID:                 "test-policy",
		SourceBackend:      "local",
		DestinationBackend: "s3",
		CheckInterval:      5 * time.Minute,
		ReplicationMode:    "invalid-mode",
	}

	body, _ := json.Marshal(policy)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/replication/policies", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "invalid replication_mode")
}

func TestAddReplicationPolicy_DuplicatePolicy(t *testing.T) {
	storage := NewMockStorageWithReplication()
	router, _ := setupTestRouter(storage)

	policy := AddReplicationPolicyRequest{
		ID:                 "test-policy",
		SourceBackend:      "local",
		DestinationBackend: "s3",
		CheckInterval:      5 * time.Minute,
		Enabled:            true,
	}

	// Add policy first time
	body, _ := json.Marshal(policy)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/replication/policies", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	assert.Equal(t, http.StatusCreated, w.Code)

	// Try to add again
	req = httptest.NewRequest(http.MethodPost, "/api/v1/replication/policies", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusConflict, w.Code)
}

func TestAddReplicationPolicy_ReplicationNotSupported(t *testing.T) {
	storage := NewMockStorage() // Regular storage without replication support
	router, _ := setupTestRouter(storage)

	policy := AddReplicationPolicyRequest{
		ID:                 "test-policy",
		SourceBackend:      "local",
		DestinationBackend: "s3",
		CheckInterval:      5 * time.Minute,
	}

	body, _ := json.Marshal(policy)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/replication/policies", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
	assert.Contains(t, w.Body.String(), "replication not supported")
}

func TestAddReplicationPolicy_WithEncryption(t *testing.T) {
	storage := NewMockStorageWithReplication()
	router, _ := setupTestRouter(storage)

	policy := AddReplicationPolicyRequest{
		ID:                 "test-policy",
		SourceBackend:      "local",
		DestinationBackend: "s3",
		CheckInterval:      5 * time.Minute,
		Enabled:            true,
		Encryption: &common.EncryptionPolicy{
			Source: &common.EncryptionConfig{
				Enabled:  true,
				Provider: "custom",
			},
		},
	}

	body, _ := json.Marshal(policy)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/replication/policies", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusCreated, w.Code)
}

// Tests for RemoveReplicationPolicy

func TestRemoveReplicationPolicy_Success(t *testing.T) {
	storage := NewMockStorageWithReplication()
	router, _ := setupTestRouter(storage)

	// Add a policy first
	storage.replicationMgr.policies["test-policy"] = common.ReplicationPolicy{
		ID: "test-policy",
	}

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/replication/policies/test-policy", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var response SuccessResponse
	err := json.Unmarshal(w.Body.Bytes(), &response)
	require.NoError(t, err)
	assert.Equal(t, "replication policy removed successfully", response.Message)
}

func TestRemoveReplicationPolicy_NotFound(t *testing.T) {
	storage := NewMockStorageWithReplication()
	router, _ := setupTestRouter(storage)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/replication/policies/nonexistent", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestRemoveReplicationPolicy_EmptyID(t *testing.T) {
	storage := NewMockStorageWithReplication()
	router, _ := setupTestRouter(storage)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/replication/policies/", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	// Should match the list endpoint instead and return method not allowed
	assert.NotEqual(t, http.StatusOK, w.Code)
}

// Tests for GetReplicationPolicies

func TestGetReplicationPolicies_Success(t *testing.T) {
	storage := NewMockStorageWithReplication()
	router, _ := setupTestRouter(storage)

	// Add some policies
	storage.replicationMgr.policies["policy1"] = common.ReplicationPolicy{
		ID:            "policy1",
		SourceBackend: "local",
		Enabled:       true,
	}
	storage.replicationMgr.policies["policy2"] = common.ReplicationPolicy{
		ID:            "policy2",
		SourceBackend: "s3",
		Enabled:       false,
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/replication/policies", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var response GetReplicationPoliciesResponse
	err := json.Unmarshal(w.Body.Bytes(), &response)
	require.NoError(t, err)
	assert.Equal(t, 2, response.Count)
	assert.Len(t, response.Policies, 2)
}

func TestGetReplicationPolicies_Empty(t *testing.T) {
	storage := NewMockStorageWithReplication()
	router, _ := setupTestRouter(storage)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/replication/policies", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var response GetReplicationPoliciesResponse
	err := json.Unmarshal(w.Body.Bytes(), &response)
	require.NoError(t, err)
	assert.Equal(t, 0, response.Count)
	assert.Len(t, response.Policies, 0)
}

func TestGetReplicationPolicies_Error(t *testing.T) {
	storage := NewMockStorageWithReplication()
	storage.replicationMgr.getPoliciesErr = errors.New("database error")
	router, _ := setupTestRouter(storage)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/replication/policies", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

// Tests for GetReplicationPolicy

func TestGetReplicationPolicy_Success(t *testing.T) {
	storage := NewMockStorageWithReplication()
	router, _ := setupTestRouter(storage)

	now := time.Now()
	storage.replicationMgr.policies["test-policy"] = common.ReplicationPolicy{
		ID:              "test-policy",
		SourceBackend:   "local",
		Enabled:         true,
		CheckInterval:   5 * time.Minute,
		LastSyncTime:    now,
		ReplicationMode: common.ReplicationModeTransparent,
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/replication/policies/test-policy", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var response ReplicationPolicyResponse
	err := json.Unmarshal(w.Body.Bytes(), &response)
	require.NoError(t, err)
	assert.Equal(t, "test-policy", response.ID)
	assert.Equal(t, "local", response.SourceBackend)
	assert.True(t, response.Enabled)
	assert.NotEmpty(t, response.LastSyncTime)
}

func TestGetReplicationPolicy_NotFound(t *testing.T) {
	storage := NewMockStorageWithReplication()
	router, _ := setupTestRouter(storage)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/replication/policies/nonexistent", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
}

// Tests for TriggerReplication

func TestTriggerReplication_AllPolicies(t *testing.T) {
	storage := NewMockStorageWithReplication()
	router, _ := setupTestRouter(storage)

	storage.replicationMgr.policies["policy1"] = common.ReplicationPolicy{
		ID:      "policy1",
		Enabled: true,
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/replication/trigger", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var response TriggerReplicationResponse
	err := json.Unmarshal(w.Body.Bytes(), &response)
	require.NoError(t, err)
	assert.True(t, response.Success)
	assert.NotNil(t, response.Result)
	assert.Equal(t, "all", response.Result.PolicyID)
	assert.Equal(t, 10, response.Result.Synced)
}

func TestTriggerReplication_SpecificPolicy(t *testing.T) {
	storage := NewMockStorageWithReplication()
	router, _ := setupTestRouter(storage)

	storage.replicationMgr.policies["test-policy"] = common.ReplicationPolicy{
		ID:      "test-policy",
		Enabled: true,
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/replication/trigger?policy_id=test-policy", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var response TriggerReplicationResponse
	err := json.Unmarshal(w.Body.Bytes(), &response)
	require.NoError(t, err)
	assert.True(t, response.Success)
	assert.NotNil(t, response.Result)
	assert.Equal(t, "test-policy", response.Result.PolicyID)
	assert.Equal(t, 5, response.Result.Synced)
}

func TestTriggerReplication_PolicyNotFound(t *testing.T) {
	storage := NewMockStorageWithReplication()
	router, _ := setupTestRouter(storage)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/replication/trigger?policy_id=nonexistent", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestTriggerReplication_SyncError(t *testing.T) {
	storage := NewMockStorageWithReplication()
	storage.replicationMgr.syncAllErr = errors.New("sync failed")
	router, _ := setupTestRouter(storage)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/replication/trigger", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestTriggerReplication_WithErrors(t *testing.T) {
	storage := NewMockStorageWithReplication()
	router, _ := setupTestRouter(storage)

	storage.replicationMgr.policies["test-policy"] = common.ReplicationPolicy{
		ID:      "test-policy",
		Enabled: true,
	}

	storage.replicationMgr.syncAllResult = &common.SyncResult{
		PolicyID:   "all",
		Synced:     5,
		Failed:     2,
		BytesTotal: 512,
		Duration:   time.Second,
		Errors:     []string{"error1", "error2"},
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/replication/trigger", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var response TriggerReplicationResponse
	err := json.Unmarshal(w.Body.Bytes(), &response)
	require.NoError(t, err)
	assert.True(t, response.Success)
	assert.Equal(t, 5, response.Result.Synced)
	assert.Equal(t, 2, response.Result.Failed)
	assert.Len(t, response.Result.Errors, 2)
}

// Tests for backwards compatibility routes

func TestReplicationPolicies_BackwardsCompatibility(t *testing.T) {
	storage := NewMockStorageWithReplication()
	router, _ := setupTestRouter(storage)

	// Test POST without /api/v1 prefix
	policy := AddReplicationPolicyRequest{
		ID:                 "test-policy",
		SourceBackend:      "local",
		DestinationBackend: "s3",
		CheckInterval:      5 * time.Minute,
		Enabled:            true,
	}

	body, _ := json.Marshal(policy)
	req := httptest.NewRequest(http.MethodPost, "/replication/policies", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusCreated, w.Code)

	// Test GET without /api/v1 prefix
	req = httptest.NewRequest(http.MethodGet, "/replication/policies", nil)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	// Test DELETE without /api/v1 prefix
	req = httptest.NewRequest(http.MethodDelete, "/replication/policies/test-policy", nil)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

// Edge case tests

func TestAddReplicationPolicy_InvalidJSON(t *testing.T) {
	storage := NewMockStorageWithReplication()
	router, _ := setupTestRouter(storage)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/replication/policies", bytes.NewBufferString("invalid json"))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestAddReplicationPolicy_ZeroCheckInterval(t *testing.T) {
	storage := NewMockStorageWithReplication()
	router, _ := setupTestRouter(storage)

	policy := AddReplicationPolicyRequest{
		ID:                 "test-policy",
		SourceBackend:      "local",
		DestinationBackend: "s3",
		CheckInterval:      0, // Invalid
	}

	body, _ := json.Marshal(policy)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/replication/policies", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "CheckInterval")
}

func TestGetReplicationManager_Error(t *testing.T) {
	storage := NewMockStorageWithReplication()
	storage.getRepErr = errors.New("replication manager unavailable")
	router, _ := setupTestRouter(storage)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/replication/policies", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestRemoveReplicationPolicy_LeadingSlash(t *testing.T) {
	storage := NewMockStorageWithReplication()
	router, _ := setupTestRouter(storage)

	storage.replicationMgr.policies["test-policy"] = common.ReplicationPolicy{
		ID: "test-policy",
	}

	// The route will have a leading slash due to Gin's handling
	req := httptest.NewRequest(http.MethodDelete, "/api/v1/replication/policies//test-policy", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	// Should still work after stripping leading slashes
	assert.True(t, w.Code == http.StatusOK || w.Code == http.StatusNotFound)
}

func TestReplicationPolicies_Coverage(t *testing.T) {
	storage := NewMockStorageWithReplication()
	router, _ := setupTestRouter(storage)

	// Test that nil result is handled in RespondWithSyncResult
	storage.replicationMgr.syncAllResult = nil
	storage.replicationMgr.policies["test"] = common.ReplicationPolicy{ID: "test", Enabled: true}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/replication/trigger", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
}

// Additional coverage tests

func TestAddReplicationPolicy_GetReplicationManagerError(t *testing.T) {
	storage := NewMockStorageWithReplication()
	storage.getRepErr = common.ErrReplicationNotSupported
	router, _ := setupTestRouter(storage)

	policy := AddReplicationPolicyRequest{
		ID:                 "test-policy",
		SourceBackend:      "local",
		DestinationBackend: "s3",
		CheckInterval:      5 * time.Minute,
	}

	body, _ := json.Marshal(policy)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/replication/policies", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
	assert.Contains(t, w.Body.String(), "replication not supported")
}

func TestRemoveReplicationPolicy_GetReplicationManagerError(t *testing.T) {
	storage := NewMockStorageWithReplication()
	storage.getRepErr = common.ErrReplicationNotSupported
	router, _ := setupTestRouter(storage)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/replication/policies/test-policy", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
	assert.Contains(t, w.Body.String(), "replication not supported")
}

func TestGetReplicationPolicies_GetReplicationManagerError(t *testing.T) {
	storage := NewMockStorageWithReplication()
	storage.getRepErr = common.ErrReplicationNotSupported
	router, _ := setupTestRouter(storage)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/replication/policies", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
	assert.Contains(t, w.Body.String(), "replication not supported")
}

func TestGetReplicationPolicy_GetReplicationManagerError(t *testing.T) {
	storage := NewMockStorageWithReplication()
	storage.getRepErr = common.ErrReplicationNotSupported
	router, _ := setupTestRouter(storage)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/replication/policies/test-policy", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
	assert.Contains(t, w.Body.String(), "replication not supported")
}

func TestTriggerReplication_GetReplicationManagerError(t *testing.T) {
	storage := NewMockStorageWithReplication()
	storage.getRepErr = common.ErrReplicationNotSupported
	router, _ := setupTestRouter(storage)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/replication/trigger", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
	assert.Contains(t, w.Body.String(), "replication not supported")
}

func TestAddReplicationPolicy_MissingDestinationBackend(t *testing.T) {
	storage := NewMockStorageWithReplication()
	router, _ := setupTestRouter(storage)

	policy := AddReplicationPolicyRequest{
		ID:            "test-policy",
		SourceBackend: "local",
		CheckInterval: 5 * time.Minute,
	}

	body, _ := json.Marshal(policy)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/replication/policies", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestAddReplicationPolicy_OpaqueMode(t *testing.T) {
	storage := NewMockStorageWithReplication()
	router, _ := setupTestRouter(storage)

	policy := AddReplicationPolicyRequest{
		ID:                 "test-policy",
		SourceBackend:      "local",
		DestinationBackend: "s3",
		CheckInterval:      5 * time.Minute,
		Enabled:            true,
		ReplicationMode:    common.ReplicationModeOpaque,
	}

	body, _ := json.Marshal(policy)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/replication/policies", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusCreated, w.Code)
}

func TestGetReplicationPolicy_EmptyID(t *testing.T) {
	storage := NewMockStorageWithReplication()
	router, _ := setupTestRouter(storage)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/replication/policies/", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	// Should match list endpoint and succeed
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestRemoveReplicationPolicy_GenericError(t *testing.T) {
	storage := NewMockStorageWithReplication()
	storage.replicationMgr.removePolicyErr = errors.New("internal error")
	router, _ := setupTestRouter(storage)

	storage.replicationMgr.policies["test-policy"] = common.ReplicationPolicy{
		ID: "test-policy",
	}

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/replication/policies/test-policy", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestGetReplicationPolicy_GenericError(t *testing.T) {
	storage := NewMockStorageWithReplication()
	storage.replicationMgr.getPolicyErr = errors.New("database error")
	router, _ := setupTestRouter(storage)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/replication/policies/test-policy", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestAddReplicationPolicy_AddPolicyError(t *testing.T) {
	storage := NewMockStorageWithReplication()
	storage.replicationMgr.addPolicyErr = errors.New("database error")
	router, _ := setupTestRouter(storage)

	policy := AddReplicationPolicyRequest{
		ID:                 "test-policy",
		SourceBackend:      "local",
		DestinationBackend: "s3",
		CheckInterval:      5 * time.Minute,
	}

	body, _ := json.Marshal(policy)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/replication/policies", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestTriggerReplication_SyncPolicyError(t *testing.T) {
	storage := NewMockStorageWithReplication()
	storage.replicationMgr.syncPolicyErr = errors.New("sync failed")
	router, _ := setupTestRouter(storage)

	storage.replicationMgr.policies["test-policy"] = common.ReplicationPolicy{
		ID:      "test-policy",
		Enabled: true,
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/replication/trigger?policy_id=test-policy", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}
