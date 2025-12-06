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
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jeremyhahn/go-objstore/pkg/common"
	_ "github.com/jeremyhahn/go-objstore/pkg/factory" // Register archivers
)

// mockLifecycleStorage extends MockStorage with lifecycle functionality
type mockLifecycleStorage struct {
	*MockStorage
	policies          []common.LifecyclePolicy
	archiveError      error
	addPolicyError    error
	removePolicyError error
	getPoliciesError  error
	existsError       error
	listErr           error
}

func newMockLifecycleStorage() *mockLifecycleStorage {
	return &mockLifecycleStorage{
		MockStorage: NewMockStorage(),
		policies:    []common.LifecyclePolicy{},
	}
}

func (m *mockLifecycleStorage) Archive(key string, destination common.Archiver) error {
	if m.archiveError != nil {
		return m.archiveError
	}
	if _, exists := m.objects[key]; !exists {
		return errors.New("object not found")
	}
	return nil
}

func (m *mockLifecycleStorage) AddPolicy(policy common.LifecyclePolicy) error {
	if m.addPolicyError != nil {
		return m.addPolicyError
	}
	// Check for duplicate ID
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

func (m *mockLifecycleStorage) Exists(ctx context.Context, key string) (bool, error) {
	if m.existsError != nil {
		return false, m.existsError
	}
	return m.MockStorage.Exists(ctx, key)
}

func (m *mockLifecycleStorage) ListWithOptions(ctx context.Context, opts *common.ListOptions) (*common.ListResult, error) {
	if m.listErr != nil {
		return nil, m.listErr
	}
	return m.MockStorage.ListWithOptions(ctx, opts)
}

// mockArchiver for testing
type mockArchiver struct{}

func (m *mockArchiver) Put(key string, data io.Reader) error {
	return nil
}

// TestArchiveObject tests the archive REST endpoint
func TestArchiveObject(t *testing.T) {
	tests := []struct {
		name           string
		setupStorage   func() *mockLifecycleStorage
		requestBody    any
		wantStatusCode int
	}{
		{
			name: "successful archive",
			setupStorage: func() *mockLifecycleStorage {
				storage := newMockLifecycleStorage()
				// Add an object to archive
				storage.objects["test-key"] = &mockObject{
					data:     []byte("test data"),
					metadata: &common.Metadata{Size: 9},
				}
				return storage
			},
			requestBody: ArchiveRequest{
				Key:                 "test-key",
				DestinationType:     "local",
				DestinationSettings: map[string]string{"path": "/tmp/archive-test"},
			},
			wantStatusCode: http.StatusOK,
		},
		{
			name: "missing key",
			setupStorage: func() *mockLifecycleStorage {
				return newMockLifecycleStorage()
			},
			requestBody: map[string]any{
				"destination_type": "local",
			},
			wantStatusCode: http.StatusBadRequest,
		},
		{
			name: "object not found",
			setupStorage: func() *mockLifecycleStorage {
				return newMockLifecycleStorage()
			},
			requestBody: ArchiveRequest{
				Key:                 "nonexistent",
				DestinationType:     "local",
				DestinationSettings: map[string]string{"path": "/tmp/archive-test"},
			},
			wantStatusCode: http.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage := tt.setupStorage()
			handler := newTestHandler(t, storage)
			router := gin.New()
			router.POST("/archive", handler.Archive)

			body, _ := json.Marshal(tt.requestBody)
			req := httptest.NewRequest(http.MethodPost, "/archive", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)

			if w.Code != tt.wantStatusCode {
				t.Errorf("Status = %v, want %v, body: %s", w.Code, tt.wantStatusCode, w.Body.String())
			}
		})
	}
}

// TestAddPolicyEndpoint tests the add policy REST endpoint
func TestAddPolicyEndpoint(t *testing.T) {
	tests := []struct {
		name           string
		setupStorage   func() *mockLifecycleStorage
		requestBody    any
		wantStatusCode int
	}{
		{
			name: "successful add delete policy",
			setupStorage: func() *mockLifecycleStorage {
				return newMockLifecycleStorage()
			},
			requestBody: AddPolicyRequest{
				ID:               "policy1",
				Prefix:           "logs/",
				Retention: 24 * time.Hour,
				Action:           "delete",
			},
			wantStatusCode: http.StatusCreated,
		},
		// Note: archive policy test skipped due to factory.NewArchiver dependency
		{
			name: "missing policy ID",
			setupStorage: func() *mockLifecycleStorage {
				return newMockLifecycleStorage()
			},
			requestBody: map[string]any{
				"prefix":    "logs/",
				"retention": 86400,
				"action":    "delete",
			},
			wantStatusCode: http.StatusBadRequest,
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
			requestBody: AddPolicyRequest{
				ID:               "existing",
				Prefix:           "logs/",
				Retention: 24 * time.Hour,
				Action:           "delete",
			},
			wantStatusCode: http.StatusConflict,
		},
		{
			name: "invalid JSON",
			setupStorage: func() *mockLifecycleStorage {
				return newMockLifecycleStorage()
			},
			requestBody:    "invalid json",
			wantStatusCode: http.StatusBadRequest,
		},
		{
			name: "storage error",
			setupStorage: func() *mockLifecycleStorage {
				storage := newMockLifecycleStorage()
				storage.addPolicyError = errors.New("storage error")
				return storage
			},
			requestBody: AddPolicyRequest{
				ID:               "policy3",
				Prefix:           "logs/",
				Retention: 24 * time.Hour,
				Action:           "delete",
			},
			wantStatusCode: http.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage := tt.setupStorage()
			handler := newTestHandler(t, storage)

			router := gin.New()
			router.POST("/policies", handler.AddPolicy)

			var body []byte
			var err error
			if str, ok := tt.requestBody.(string); ok {
				body = []byte(str)
			} else {
				body, err = json.Marshal(tt.requestBody)
				if err != nil {
					t.Fatalf("Failed to marshal request: %v", err)
				}
			}

			req := httptest.NewRequest("POST", "/policies", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)

			if w.Code != tt.wantStatusCode {
				t.Errorf("AddPolicy() status = %v, want %v, body: %s", w.Code, tt.wantStatusCode, w.Body.String())
			}
		})
	}
}

// TestRemovePolicyEndpoint tests the remove policy REST endpoint
func TestRemovePolicyEndpoint(t *testing.T) {
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
				storage := newMockLifecycleStorage()
				storage.removePolicyError = common.ErrPolicyNotFound
				return storage
			},
			policyID:       "nonexistent",
			wantStatusCode: http.StatusNotFound,
		},
		{
			name: "empty policy ID",
			setupStorage: func() *mockLifecycleStorage {
				return newMockLifecycleStorage()
			},
			policyID:       "",
			wantStatusCode: http.StatusNotFound, // Gin returns 404 for non-matching routes
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
			handler := newTestHandler(t, storage)

			router := gin.New()
			router.DELETE("/policies/:id", handler.RemovePolicy)

			req := httptest.NewRequest("DELETE", "/policies/"+tt.policyID, nil)
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)

			if w.Code != tt.wantStatusCode {
				t.Errorf("RemovePolicy() status = %v, want %v, body: %s", w.Code, tt.wantStatusCode, w.Body.String())
			}
		})
	}
}

// TestGetPoliciesEndpoint tests the get policies REST endpoint
func TestGetPoliciesEndpoint(t *testing.T) {
	tests := []struct {
		name           string
		setupStorage   func() *mockLifecycleStorage
		queryParams    string
		wantStatusCode int
		wantCount      int
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
			queryParams:    "",
			wantStatusCode: http.StatusOK,
			wantCount:      2,
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
			queryParams:    "?prefix=logs/",
			wantStatusCode: http.StatusOK,
			wantCount:      2,
		},
		{
			name: "no policies",
			setupStorage: func() *mockLifecycleStorage {
				return newMockLifecycleStorage()
			},
			queryParams:    "",
			wantStatusCode: http.StatusOK,
			wantCount:      0,
		},
		{
			name: "storage error",
			setupStorage: func() *mockLifecycleStorage {
				storage := newMockLifecycleStorage()
				storage.getPoliciesError = errors.New("storage error")
				return storage
			},
			queryParams:    "",
			wantStatusCode: http.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage := tt.setupStorage()
			handler := newTestHandler(t, storage)

			router := gin.New()
			router.GET("/policies", handler.GetPolicies)

			req := httptest.NewRequest("GET", "/policies"+tt.queryParams, nil)
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)

			if w.Code != tt.wantStatusCode {
				t.Errorf("GetPolicies() status = %v, want %v, body: %s", w.Code, tt.wantStatusCode, w.Body.String())
			}

			if w.Code == http.StatusOK {
				var response map[string]any
				if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
					t.Fatalf("Failed to unmarshal response: %v", err)
				}

				count, ok := response["count"].(float64)
				if !ok {
					t.Fatal("Response missing count field")
				}

				if int(count) != tt.wantCount {
					t.Errorf("GetPolicies() count = %d, want %d", int(count), tt.wantCount)
				}
			}
		})
	}
}

func TestHandler_ApplyPolicies(t *testing.T) {
	tests := []struct {
		name           string
		policies       []common.LifecyclePolicy
		getPoliciesErr error
		wantStatusCode int
		wantMessage    string
	}{
		{
			name: "success with policies",
			policies: []common.LifecyclePolicy{
				{
					ID:        "policy1",
					Prefix:    "old/",
					Retention: 30 * 24 * time.Hour,
					Action:    "delete",
				},
			},
			wantStatusCode: http.StatusOK,
			wantMessage:    "Lifecycle policies applied successfully",
		},
		{
			name:           "no policies to apply",
			policies:       []common.LifecyclePolicy{},
			wantStatusCode: http.StatusOK,
			wantMessage:    "No lifecycle policies to apply",
		},
		{
			name:           "error getting policies",
			getPoliciesErr: errors.New("database error"),
			wantStatusCode: http.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage := newMockLifecycleStorage()
			storage.policies = tt.policies
			storage.getPoliciesError = tt.getPoliciesErr

			handler := newTestHandler(t, storage)
			router := gin.New()
			router.POST("/policies/apply", handler.ApplyPolicies)

			req := httptest.NewRequest("POST", "/policies/apply", nil)
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)

			if w.Code != tt.wantStatusCode {
				t.Errorf("ApplyPolicies() status = %v, want %v, body: %s", w.Code, tt.wantStatusCode, w.Body.String())
			}

			if w.Code == http.StatusOK {
				var response map[string]any
				if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
					t.Fatalf("Failed to unmarshal response: %v", err)
				}

				message, ok := response["message"].(string)
				if !ok {
					t.Fatal("Response missing message field")
				}

				if message != tt.wantMessage {
					t.Errorf("ApplyPolicies() message = %q, want %q", message, tt.wantMessage)
				}
			}
		})
	}
}

func TestHandler_ApplyPolicies_WithObjects(t *testing.T) {
	tests := []struct {
		name            string
		policies        []common.LifecyclePolicy
		objects         map[string]*mockObject
		listErr         error
		wantStatusCode  int
		wantProcessed   int
		wantMessage     string
	}{
		{
			name: "delete old objects matching prefix",
			policies: []common.LifecyclePolicy{
				{
					ID:        "delete-old",
					Prefix:    "logs/",
					Retention: 1 * time.Hour,
					Action:    "delete",
				},
			},
			objects: map[string]*mockObject{
				"logs/old.log": {
					data: []byte("old log"),
					metadata: &common.Metadata{
						LastModified: time.Now().Add(-2 * time.Hour), // 2 hours old
					},
				},
				"logs/new.log": {
					data: []byte("new log"),
					metadata: &common.Metadata{
						LastModified: time.Now(), // fresh
					},
				},
				"data/file.txt": {
					data: []byte("data"),
					metadata: &common.Metadata{
						LastModified: time.Now().Add(-2 * time.Hour), // old but wrong prefix
					},
				},
			},
			wantStatusCode: http.StatusOK,
			wantProcessed:  1, // only logs/old.log
			wantMessage:    "Lifecycle policies applied successfully",
		},
		{
			name: "archive old objects",
			policies: []common.LifecyclePolicy{
				{
					ID:        "archive-old",
					Prefix:    "data/",
					Retention: 1 * time.Hour,
					Action:    "archive",
					Destination: &mockArchiver{},
				},
			},
			objects: map[string]*mockObject{
				"data/old-file.txt": {
					data: []byte("old data"),
					metadata: &common.Metadata{
						LastModified: time.Now().Add(-2 * time.Hour),
					},
				},
			},
			wantStatusCode: http.StatusOK,
			wantProcessed:  1,
			wantMessage:    "Lifecycle policies applied successfully",
		},
		{
			name: "skip objects without metadata",
			policies: []common.LifecyclePolicy{
				{
					ID:        "delete-no-meta",
					Prefix:    "",
					Retention: 1 * time.Hour,
					Action:    "delete",
				},
			},
			objects: map[string]*mockObject{
				"no-meta.txt": {
					data:     []byte("no metadata"),
					metadata: nil, // no metadata
				},
			},
			wantStatusCode: http.StatusOK,
			wantProcessed:  0,
			wantMessage:    "Lifecycle policies applied successfully",
		},
		{
			name: "skip archive without destination",
			policies: []common.LifecyclePolicy{
				{
					ID:          "archive-no-dest",
					Prefix:      "",
					Retention:   1 * time.Hour,
					Action:      "archive",
					Destination: nil, // no destination
				},
			},
			objects: map[string]*mockObject{
				"file.txt": {
					data: []byte("data"),
					metadata: &common.Metadata{
						LastModified: time.Now().Add(-2 * time.Hour),
					},
				},
			},
			wantStatusCode: http.StatusOK,
			wantProcessed:  0,
			wantMessage:    "Lifecycle policies applied successfully",
		},
		{
			name: "error listing objects",
			policies: []common.LifecyclePolicy{
				{
					ID:        "policy1",
					Prefix:    "",
					Retention: 1 * time.Hour,
					Action:    "delete",
				},
			},
			listErr:        errors.New("list failed"),
			wantStatusCode: http.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage := newMockLifecycleStorage()
			storage.policies = tt.policies
			if tt.objects != nil {
				storage.objects = tt.objects
			}
			if tt.listErr != nil {
				storage.listErr = tt.listErr
			}

			handler := newTestHandler(t, storage)
			router := gin.New()
			router.POST("/policies/apply", handler.ApplyPolicies)

			req := httptest.NewRequest("POST", "/policies/apply", nil)
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)

			if w.Code != tt.wantStatusCode {
				t.Errorf("ApplyPolicies() status = %v, want %v, body: %s", w.Code, tt.wantStatusCode, w.Body.String())
			}

			if w.Code == http.StatusOK {
				var response map[string]any
				if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
					t.Fatalf("Failed to unmarshal response: %v", err)
				}

				message, ok := response["message"].(string)
				if !ok {
					t.Fatal("Response missing message field")
				}

				if message != tt.wantMessage {
					t.Errorf("ApplyPolicies() message = %q, want %q", message, tt.wantMessage)
				}

				processed, _ := response["objects_processed"].(float64)
				if int(processed) != tt.wantProcessed {
					t.Errorf("ApplyPolicies() objects_processed = %d, want %d", int(processed), tt.wantProcessed)
				}
			}
		})
	}
}

func TestHandler_ExistsObject(t *testing.T) {
	tests := []struct {
		name           string
		key            string
		objectExists   bool
		existsError    error
		wantStatusCode int
		wantExists     bool
	}{
		{
			name:           "object exists",
			key:            "existing-file.txt",
			objectExists:   true,
			wantStatusCode: http.StatusOK,
			wantExists:     true,
		},
		{
			name:           "object does not exist",
			key:            "missing-file.txt",
			objectExists:   false,
			wantStatusCode: http.StatusOK,
			wantExists:     false,
		},
		{
			name:           "empty key",
			key:            "",
			wantStatusCode: http.StatusBadRequest,
		},
		{
			name:           "error checking existence",
			key:            "file.txt",
			existsError:    errors.New("storage error"),
			wantStatusCode: http.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage := newMockLifecycleStorage()

			if tt.objectExists && tt.existsError == nil {
				storage.objects["existing-file.txt"] = &mockObject{
					data:     []byte("test"),
					metadata: &common.Metadata{},
				}
			}

			if tt.existsError != nil {
				// Create a mock that returns the error
				storage.existsError = tt.existsError
			}

			handler := newTestHandler(t, storage)
			router := gin.New()
			router.GET("/exists/*key", handler.ExistsObject)

			req := httptest.NewRequest("GET", "/exists/"+tt.key, nil)
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)

			if w.Code != tt.wantStatusCode {
				t.Errorf("ExistsObject() status = %v, want %v, body: %s", w.Code, tt.wantStatusCode, w.Body.String())
			}

			if w.Code == http.StatusOK {
				var response map[string]any
				if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
					t.Fatalf("Failed to unmarshal response: %v", err)
				}

				exists, ok := response["exists"].(bool)
				if !ok {
					t.Fatal("Response missing exists field")
				}

				if exists != tt.wantExists {
					t.Errorf("ExistsObject() exists = %v, want %v", exists, tt.wantExists)
				}
			}
		})
	}
}
