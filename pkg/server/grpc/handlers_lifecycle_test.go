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

package grpc

import (
	"context"
	"errors"
	"testing"
	"time"

	objstorepb "github.com/jeremyhahn/go-objstore/api/proto"
	"github.com/jeremyhahn/go-objstore/pkg/common"
	_ "github.com/jeremyhahn/go-objstore/pkg/factory" // Import factory to register archivers
)

// mockLifecycleStorage extends mockStorage with lifecycle policy management
type mockLifecycleStorage struct {
	*mockStorage
	policies          []common.LifecyclePolicy
	archiveError      error
	addPolicyError    error
	removePolicyError error
	getPoliciesError  error
}

func newMockLifecycleStorage() *mockLifecycleStorage {
	return &mockLifecycleStorage{
		mockStorage: newMockStorage(),
		policies:    []common.LifecyclePolicy{},
	}
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

func (m *mockLifecycleStorage) Archive(key string, destination common.Archiver) error {
	if m.archiveError != nil {
		return m.archiveError
	}
	if _, ok := m.data[key]; !ok {
		return &notFoundError{}
	}
	return nil
}

// mockArchiver implements the Archiver interface for testing
type mockArchiver struct {
	putError error
}

func (m *mockArchiver) Put(key string, data any) error {
	if m.putError != nil {
		return m.putError
	}
	return nil
}

// TestArchive tests the Archive gRPC handler
func TestArchive(t *testing.T) {
	tests := []struct {
		name           string
		setupStorage   func() *mockLifecycleStorage
		request        *objstorepb.ArchiveRequest
		wantError      bool
		wantStatusCode string
	}{
		{
			name: "successful archive to local",
			setupStorage: func() *mockLifecycleStorage {
				storage := newMockLifecycleStorage()
				storage.data["test-key"] = []byte("test-data")
				storage.metadata["test-key"] = &common.Metadata{Size: 9}
				return storage
			},
			request: &objstorepb.ArchiveRequest{
				Key:             "test-key",
				DestinationType: "local",
				DestinationSettings: map[string]string{
					"path": "/tmp/archive-test",
				},
			},
			wantError: false,
		},
		{
			name: "missing key",
			setupStorage: func() *mockLifecycleStorage {
				return newMockLifecycleStorage()
			},
			request: &objstorepb.ArchiveRequest{
				Key:             "",
				DestinationType: "local",
			},
			wantError:      true,
			wantStatusCode: "InvalidArgument",
		},
		{
			name: "missing destination type",
			setupStorage: func() *mockLifecycleStorage {
				storage := newMockLifecycleStorage()
				storage.data["test-key"] = []byte("test-data")
				return storage
			},
			request: &objstorepb.ArchiveRequest{
				Key:             "test-key",
				DestinationType: "",
			},
			wantError:      true,
			wantStatusCode: "InvalidArgument",
		},
		{
			name: "object not found",
			setupStorage: func() *mockLifecycleStorage {
				storage := newMockLifecycleStorage()
				storage.archiveError = &notFoundError{}
				return storage
			},
			request: &objstorepb.ArchiveRequest{
				Key:             "non-existent",
				DestinationType: "local",
				DestinationSettings: map[string]string{
					"path": "/tmp/archive",
				},
			},
			wantError: true,
		},
		{
			name: "invalid destination type",
			setupStorage: func() *mockLifecycleStorage {
				storage := newMockLifecycleStorage()
				storage.data["test-key"] = []byte("test-data")
				return storage
			},
			request: &objstorepb.ArchiveRequest{
				Key:             "test-key",
				DestinationType: "invalid-backend",
			},
			wantError:      true,
			wantStatusCode: "InvalidArgument",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage := tt.setupStorage()
			server, err := NewServer(storage)
			if err != nil {
				t.Fatalf("Failed to create server: %v", err)
			}

			ctx := context.Background()
			resp, err := server.Archive(ctx, tt.request)

			if tt.wantError {
				if err == nil {
					t.Errorf("Archive() error = nil, wantError %v", tt.wantError)
				}
			} else {
				if err != nil {
					t.Errorf("Archive() unexpected error = %v", err)
				}
				if resp == nil {
					t.Fatal("Archive() response is nil")
				}
				if !resp.Success {
					t.Errorf("Archive() success = %v, want true", resp.Success)
				}
			}
		})
	}
}

// TestAddPolicy tests the AddPolicy gRPC handler
func TestAddPolicy(t *testing.T) {
	tests := []struct {
		name         string
		setupStorage func() *mockLifecycleStorage
		request      *objstorepb.AddPolicyRequest
		wantError    bool
	}{
		{
			name: "successful add delete policy",
			setupStorage: func() *mockLifecycleStorage {
				return newMockLifecycleStorage()
			},
			request: &objstorepb.AddPolicyRequest{
				Policy: &objstorepb.LifecyclePolicy{
					Id:               "policy1",
					Prefix:           "logs/",
					RetentionSeconds: 86400, // 1 day
					Action:           "delete",
				},
			},
			wantError: false,
		},
		{
			name: "successful add archive policy",
			setupStorage: func() *mockLifecycleStorage {
				return newMockLifecycleStorage()
			},
			request: &objstorepb.AddPolicyRequest{
				Policy: &objstorepb.LifecyclePolicy{
					Id:               "policy2",
					Prefix:           "data/",
					RetentionSeconds: 2592000, // 30 days
					Action:           "archive",
					DestinationType:  "local",
					DestinationSettings: map[string]string{
						"path": "/tmp/archive-test",
					},
				},
			},
			wantError: false,
		},
		{
			name: "nil policy",
			setupStorage: func() *mockLifecycleStorage {
				return newMockLifecycleStorage()
			},
			request: &objstorepb.AddPolicyRequest{
				Policy: nil,
			},
			wantError: true,
		},
		{
			name: "empty policy ID",
			setupStorage: func() *mockLifecycleStorage {
				return newMockLifecycleStorage()
			},
			request: &objstorepb.AddPolicyRequest{
				Policy: &objstorepb.LifecyclePolicy{
					Id:               "",
					Prefix:           "logs/",
					RetentionSeconds: 86400,
					Action:           "delete",
				},
			},
			wantError: true,
		},
		{
			name: "archive action without destination type",
			setupStorage: func() *mockLifecycleStorage {
				return newMockLifecycleStorage()
			},
			request: &objstorepb.AddPolicyRequest{
				Policy: &objstorepb.LifecyclePolicy{
					Id:               "policy3",
					Prefix:           "data/",
					RetentionSeconds: 86400,
					Action:           "archive",
					DestinationType:  "",
				},
			},
			wantError: true,
		},
		{
			name: "duplicate policy ID",
			setupStorage: func() *mockLifecycleStorage {
				storage := newMockLifecycleStorage()
				storage.policies = []common.LifecyclePolicy{
					{
						ID:        "existing",
						Prefix:    "test/",
						Retention: 24 * time.Hour,
						Action:    "delete",
					},
				}
				return storage
			},
			request: &objstorepb.AddPolicyRequest{
				Policy: &objstorepb.LifecyclePolicy{
					Id:               "existing",
					Prefix:           "logs/",
					RetentionSeconds: 86400,
					Action:           "delete",
				},
			},
			wantError: true,
		},
		{
			name: "storage error",
			setupStorage: func() *mockLifecycleStorage {
				storage := newMockLifecycleStorage()
				storage.addPolicyError = errors.New("storage error")
				return storage
			},
			request: &objstorepb.AddPolicyRequest{
				Policy: &objstorepb.LifecyclePolicy{
					Id:               "policy4",
					Prefix:           "logs/",
					RetentionSeconds: 86400,
					Action:           "delete",
				},
			},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage := tt.setupStorage()
			server, err := NewServer(storage)
			if err != nil {
				t.Fatalf("Failed to create server: %v", err)
			}

			ctx := context.Background()
			resp, err := server.AddPolicy(ctx, tt.request)

			if tt.wantError {
				if err == nil {
					t.Errorf("AddPolicy() error = nil, wantError %v", tt.wantError)
				}
			} else {
				if err != nil {
					t.Errorf("AddPolicy() unexpected error = %v", err)
				}
				if resp == nil {
					t.Fatal("AddPolicy() response is nil")
				}
				if !resp.Success {
					t.Errorf("AddPolicy() success = %v, want true", resp.Success)
				}
				if resp.Message == "" {
					t.Error("AddPolicy() message is empty")
				}
			}
		})
	}
}

// TestRemovePolicy tests the RemovePolicy gRPC handler
func TestRemovePolicy(t *testing.T) {
	tests := []struct {
		name         string
		setupStorage func() *mockLifecycleStorage
		request      *objstorepb.RemovePolicyRequest
		wantError    bool
	}{
		{
			name: "successful remove",
			setupStorage: func() *mockLifecycleStorage {
				storage := newMockLifecycleStorage()
				storage.policies = []common.LifecyclePolicy{
					{
						ID:        "policy1",
						Prefix:    "logs/",
						Retention: 24 * time.Hour,
						Action:    "delete",
					},
				}
				return storage
			},
			request: &objstorepb.RemovePolicyRequest{
				Id: "policy1",
			},
			wantError: false,
		},
		{
			name: "empty policy ID",
			setupStorage: func() *mockLifecycleStorage {
				return newMockLifecycleStorage()
			},
			request: &objstorepb.RemovePolicyRequest{
				Id: "",
			},
			wantError: true,
		},
		{
			name: "policy not found",
			setupStorage: func() *mockLifecycleStorage {
				return newMockLifecycleStorage()
			},
			request: &objstorepb.RemovePolicyRequest{
				Id: "non-existent",
			},
			wantError: true,
		},
		{
			name: "storage error",
			setupStorage: func() *mockLifecycleStorage {
				storage := newMockLifecycleStorage()
				storage.removePolicyError = errors.New("storage error")
				return storage
			},
			request: &objstorepb.RemovePolicyRequest{
				Id: "policy1",
			},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage := tt.setupStorage()
			server, err := NewServer(storage)
			if err != nil {
				t.Fatalf("Failed to create server: %v", err)
			}

			ctx := context.Background()
			resp, err := server.RemovePolicy(ctx, tt.request)

			if tt.wantError {
				if err == nil {
					t.Errorf("RemovePolicy() error = nil, wantError %v", tt.wantError)
				}
			} else {
				if err != nil {
					t.Errorf("RemovePolicy() unexpected error = %v", err)
				}
				if resp == nil {
					t.Fatal("RemovePolicy() response is nil")
				}
				if !resp.Success {
					t.Errorf("RemovePolicy() success = %v, want true", resp.Success)
				}
				if resp.Message == "" {
					t.Error("RemovePolicy() message is empty")
				}
			}
		})
	}
}

// TestGetPolicies tests the GetPolicies gRPC handler
func TestGetPolicies(t *testing.T) {
	tests := []struct {
		name         string
		setupStorage func() *mockLifecycleStorage
		request      *objstorepb.GetPoliciesRequest
		wantCount    int
		wantError    bool
	}{
		{
			name: "get all policies",
			setupStorage: func() *mockLifecycleStorage {
				storage := newMockLifecycleStorage()
				storage.policies = []common.LifecyclePolicy{
					{
						ID:        "policy1",
						Prefix:    "logs/",
						Retention: 24 * time.Hour,
						Action:    "delete",
					},
					{
						ID:        "policy2",
						Prefix:    "data/",
						Retention: 30 * 24 * time.Hour,
						Action:    "archive",
					},
				}
				return storage
			},
			request: &objstorepb.GetPoliciesRequest{
				Prefix: "",
			},
			wantCount: 2,
			wantError: false,
		},
		{
			name: "filter by prefix",
			setupStorage: func() *mockLifecycleStorage {
				storage := newMockLifecycleStorage()
				storage.policies = []common.LifecyclePolicy{
					{
						ID:        "policy1",
						Prefix:    "logs/",
						Retention: 24 * time.Hour,
						Action:    "delete",
					},
					{
						ID:        "policy2",
						Prefix:    "data/",
						Retention: 30 * 24 * time.Hour,
						Action:    "delete",
					},
					{
						ID:        "policy3",
						Prefix:    "logs/",
						Retention: 7 * 24 * time.Hour,
						Action:    "archive",
					},
				}
				return storage
			},
			request: &objstorepb.GetPoliciesRequest{
				Prefix: "logs/",
			},
			wantCount: 2,
			wantError: false,
		},
		{
			name: "no policies",
			setupStorage: func() *mockLifecycleStorage {
				return newMockLifecycleStorage()
			},
			request: &objstorepb.GetPoliciesRequest{
				Prefix: "",
			},
			wantCount: 0,
			wantError: false,
		},
		{
			name: "storage error",
			setupStorage: func() *mockLifecycleStorage {
				storage := newMockLifecycleStorage()
				storage.getPoliciesError = errors.New("storage error")
				return storage
			},
			request: &objstorepb.GetPoliciesRequest{
				Prefix: "",
			},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage := tt.setupStorage()
			server, err := NewServer(storage)
			if err != nil {
				t.Fatalf("Failed to create server: %v", err)
			}

			ctx := context.Background()
			resp, err := server.GetPolicies(ctx, tt.request)

			if tt.wantError {
				if err == nil {
					t.Errorf("GetPolicies() error = nil, wantError %v", tt.wantError)
				}
			} else {
				if err != nil {
					t.Errorf("GetPolicies() unexpected error = %v", err)
				}
				if resp == nil {
					t.Fatal("GetPolicies() response is nil")
				}
				if !resp.Success {
					t.Errorf("GetPolicies() success = %v, want true", resp.Success)
				}
				if len(resp.Policies) != tt.wantCount {
					t.Errorf("GetPolicies() count = %d, want %d", len(resp.Policies), tt.wantCount)
				}
			}
		})
	}
}

// TestProtoToLifecyclePolicy tests the conversion from proto to common LifecyclePolicy
func TestProtoToLifecyclePolicy(t *testing.T) {
	tests := []struct {
		name      string
		proto     *objstorepb.LifecyclePolicy
		wantError bool
	}{
		{
			name: "valid delete policy",
			proto: &objstorepb.LifecyclePolicy{
				Id:               "test1",
				Prefix:           "logs/",
				RetentionSeconds: 86400,
				Action:           "delete",
			},
			wantError: false,
		},
		{
			name: "valid archive policy",
			proto: &objstorepb.LifecyclePolicy{
				Id:               "test2",
				Prefix:           "data/",
				RetentionSeconds: 2592000,
				Action:           "archive",
				DestinationType:  "local",
				DestinationSettings: map[string]string{
					"path": "/tmp/archive-test",
				},
			},
			wantError: false,
		},
		{
			name:      "nil policy",
			proto:     nil,
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			policy, err := protoToLifecyclePolicy(tt.proto)

			if tt.wantError {
				if err == nil {
					t.Errorf("protoToLifecyclePolicy() error = nil, wantError %v", tt.wantError)
				}
			} else {
				if err != nil {
					t.Errorf("protoToLifecyclePolicy() unexpected error = %v", err)
				}
				if policy == nil {
					t.Fatal("protoToLifecyclePolicy() policy is nil")
				}
				if policy.ID != tt.proto.Id {
					t.Errorf("protoToLifecyclePolicy() ID = %v, want %v", policy.ID, tt.proto.Id)
				}
				if policy.Prefix != tt.proto.Prefix {
					t.Errorf("protoToLifecyclePolicy() Prefix = %v, want %v", policy.Prefix, tt.proto.Prefix)
				}
				expectedRetention := time.Duration(tt.proto.RetentionSeconds) * time.Second
				if policy.Retention != expectedRetention {
					t.Errorf("protoToLifecyclePolicy() Retention = %v, want %v", policy.Retention, expectedRetention)
				}
			}
		})
	}
}

// TestLifecyclePolicyToProto tests the conversion from common to proto LifecyclePolicy
func TestLifecyclePolicyToProto(t *testing.T) {
	tests := []struct {
		name   string
		policy *common.LifecyclePolicy
		want   *objstorepb.LifecyclePolicy
	}{
		{
			name: "valid policy",
			policy: &common.LifecyclePolicy{
				ID:        "test1",
				Prefix:    "logs/",
				Retention: 24 * time.Hour,
				Action:    "delete",
			},
			want: &objstorepb.LifecyclePolicy{
				Id:               "test1",
				Prefix:           "logs/",
				RetentionSeconds: 86400,
				Action:           "delete",
			},
		},
		{
			name:   "nil policy",
			policy: nil,
			want:   nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := lifecyclePolicyToProto(tt.policy)

			if tt.want == nil {
				if got != nil {
					t.Errorf("lifecyclePolicyToProto() = %v, want nil", got)
				}
				return
			}

			if got == nil {
				t.Fatal("lifecyclePolicyToProto() returned nil")
			}

			if got.Id != tt.want.Id {
				t.Errorf("lifecyclePolicyToProto() ID = %v, want %v", got.Id, tt.want.Id)
			}
			if got.Prefix != tt.want.Prefix {
				t.Errorf("lifecyclePolicyToProto() Prefix = %v, want %v", got.Prefix, tt.want.Prefix)
			}
			if got.RetentionSeconds != tt.want.RetentionSeconds {
				t.Errorf("lifecyclePolicyToProto() RetentionSeconds = %v, want %v", got.RetentionSeconds, tt.want.RetentionSeconds)
			}
			if got.Action != tt.want.Action {
				t.Errorf("lifecyclePolicyToProto() Action = %v, want %v", got.Action, tt.want.Action)
			}
		})
	}
}
