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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// mockReplicationManager implements common.ReplicationManager for testing
type mockReplicationManager struct {
	policies           map[string]common.ReplicationPolicy
	addPolicyError     error
	removePolicyError  error
	getPolicyError     error
	getPoliciesError   error
	syncAllError       error
	syncPolicyError    error
	syncAllResult      *common.SyncResult
	syncPolicyResult   *common.SyncResult
}

func newMockReplicationManager() *mockReplicationManager {
	return &mockReplicationManager{
		policies: make(map[string]common.ReplicationPolicy),
		syncAllResult: &common.SyncResult{
			PolicyID:   "all",
			Synced:     5,
			Deleted:    0,
			Failed:     0,
			BytesTotal: 1024,
			Duration:   100 * time.Millisecond,
		},
		syncPolicyResult: &common.SyncResult{
			PolicyID:   "test-policy",
			Synced:     3,
			Deleted:    0,
			Failed:     0,
			BytesTotal: 512,
			Duration:   50 * time.Millisecond,
		},
	}
}

func (m *mockReplicationManager) AddPolicy(policy common.ReplicationPolicy) error {
	if m.addPolicyError != nil {
		return m.addPolicyError
	}
	if _, exists := m.policies[policy.ID]; exists {
		return errors.New("policy already exists")
	}
	m.policies[policy.ID] = policy
	return nil
}

func (m *mockReplicationManager) RemovePolicy(id string) error {
	if m.removePolicyError != nil {
		return m.removePolicyError
	}
	if _, exists := m.policies[id]; !exists {
		return common.ErrPolicyNotFound
	}
	delete(m.policies, id)
	return nil
}

func (m *mockReplicationManager) GetPolicy(id string) (*common.ReplicationPolicy, error) {
	if m.getPolicyError != nil {
		return nil, m.getPolicyError
	}
	policy, exists := m.policies[id]
	if !exists {
		return nil, common.ErrPolicyNotFound
	}
	return &policy, nil
}

func (m *mockReplicationManager) GetPolicies() ([]common.ReplicationPolicy, error) {
	if m.getPoliciesError != nil {
		return nil, m.getPoliciesError
	}
	policies := make([]common.ReplicationPolicy, 0, len(m.policies))
	for _, p := range m.policies {
		policies = append(policies, p)
	}
	return policies, nil
}

func (m *mockReplicationManager) SyncAll(ctx context.Context) (*common.SyncResult, error) {
	if m.syncAllError != nil {
		return nil, m.syncAllError
	}
	return m.syncAllResult, nil
}

func (m *mockReplicationManager) SyncPolicy(ctx context.Context, policyID string) (*common.SyncResult, error) {
	if m.syncPolicyError != nil {
		return nil, m.syncPolicyError
	}
	return m.syncPolicyResult, nil
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

// mockReplicationCapableStorage extends mockStorage with replication capabilities
type mockReplicationCapableStorage struct {
	*mockStorage
	replicationMgr     *mockReplicationManager
	replicationMgrErr  error
}

func newMockReplicationCapableStorage() *mockReplicationCapableStorage {
	return &mockReplicationCapableStorage{
		mockStorage:     newMockStorage(),
		replicationMgr:  newMockReplicationManager(),
	}
}

func (m *mockReplicationCapableStorage) GetReplicationManager() (common.ReplicationManager, error) {
	if m.replicationMgrErr != nil {
		return nil, m.replicationMgrErr
	}
	return m.replicationMgr, nil
}

// TestAddReplicationPolicy tests the AddReplicationPolicy gRPC handler
func TestAddReplicationPolicy(t *testing.T) {
	tests := []struct {
		name           string
		setupStorage   func() common.Storage
		request        *objstorepb.AddReplicationPolicyRequest
		wantError      bool
		wantStatusCode codes.Code
	}{
		{
			name: "successful add policy",
			setupStorage: func() common.Storage {
				return newMockReplicationCapableStorage()
			},
			request: &objstorepb.AddReplicationPolicyRequest{
				Policy: &objstorepb.ReplicationPolicy{
					Id:                    "test-policy",
					SourceBackend:         "local",
					SourceSettings:        map[string]string{"path": "/source"},
					DestinationBackend:    "s3",
					DestinationSettings:   map[string]string{"bucket": "dest"},
					CheckIntervalSeconds:  300,
					Enabled:               true,
					ReplicationMode:       objstorepb.ReplicationMode_TRANSPARENT,
				},
			},
			wantError:      false,
			wantStatusCode: codes.OK,
		},
		{
			name: "policy is nil",
			setupStorage: func() common.Storage {
				return newMockReplicationCapableStorage()
			},
			request: &objstorepb.AddReplicationPolicyRequest{
				Policy: nil,
			},
			wantError:      true,
			wantStatusCode: codes.InvalidArgument,
		},
		{
			name: "policy ID is empty",
			setupStorage: func() common.Storage {
				return newMockReplicationCapableStorage()
			},
			request: &objstorepb.AddReplicationPolicyRequest{
				Policy: &objstorepb.ReplicationPolicy{
					Id:                    "",
					SourceBackend:         "local",
					DestinationBackend:    "s3",
				},
			},
			wantError:      true,
			wantStatusCode: codes.InvalidArgument,
		},
		{
			name: "replication not supported",
			setupStorage: func() common.Storage {
				return newMockStorage()
			},
			request: &objstorepb.AddReplicationPolicyRequest{
				Policy: &objstorepb.ReplicationPolicy{
					Id:                    "test-policy",
					SourceBackend:         "local",
					DestinationBackend:    "s3",
				},
			},
			wantError:      true,
			wantStatusCode: codes.Unimplemented,
		},
		{
			name: "add policy error",
			setupStorage: func() common.Storage {
				storage := newMockReplicationCapableStorage()
				storage.replicationMgr.addPolicyError = errors.New("add failed")
				return storage
			},
			request: &objstorepb.AddReplicationPolicyRequest{
				Policy: &objstorepb.ReplicationPolicy{
					Id:                    "test-policy",
					SourceBackend:         "local",
					DestinationBackend:    "s3",
				},
			},
			wantError:      true,
			wantStatusCode: codes.Internal,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage := tt.setupStorage()
			server, err := newTestServer(t, storage)
			if err != nil {
				t.Fatalf("Failed to create server: %v", err)
			}

			resp, err := server.AddReplicationPolicy(context.Background(), tt.request)

			if tt.wantError {
				if err == nil {
					t.Error("expected error but got none")
				}
				if st, ok := status.FromError(err); ok {
					if st.Code() != tt.wantStatusCode {
						t.Errorf("expected status code %v, got %v", tt.wantStatusCode, st.Code())
					}
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if resp == nil || !resp.Success {
					t.Error("expected successful response")
				}
			}
		})
	}
}

// TestRemoveReplicationPolicy tests the RemoveReplicationPolicy gRPC handler
func TestRemoveReplicationPolicy(t *testing.T) {
	tests := []struct {
		name           string
		setupStorage   func() common.Storage
		request        *objstorepb.RemoveReplicationPolicyRequest
		wantError      bool
		wantStatusCode codes.Code
	}{
		{
			name: "successful remove policy",
			setupStorage: func() common.Storage {
				storage := newMockReplicationCapableStorage()
				storage.replicationMgr.policies["test-policy"] = common.ReplicationPolicy{
					ID: "test-policy",
				}
				return storage
			},
			request: &objstorepb.RemoveReplicationPolicyRequest{
				Id: "test-policy",
			},
			wantError:      false,
			wantStatusCode: codes.OK,
		},
		{
			name: "policy ID is empty",
			setupStorage: func() common.Storage {
				return newMockReplicationCapableStorage()
			},
			request: &objstorepb.RemoveReplicationPolicyRequest{
				Id: "",
			},
			wantError:      true,
			wantStatusCode: codes.InvalidArgument,
		},
		{
			name: "policy not found",
			setupStorage: func() common.Storage {
				return newMockReplicationCapableStorage()
			},
			request: &objstorepb.RemoveReplicationPolicyRequest{
				Id: "nonexistent",
			},
			wantError:      true,
			wantStatusCode: codes.Internal,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage := tt.setupStorage()
			server, err := newTestServer(t, storage)
			if err != nil {
				t.Fatalf("Failed to create server: %v", err)
			}

			resp, err := server.RemoveReplicationPolicy(context.Background(), tt.request)

			if tt.wantError {
				if err == nil {
					t.Error("expected error but got none")
				}
				if st, ok := status.FromError(err); ok {
					if st.Code() != tt.wantStatusCode {
						t.Errorf("expected status code %v, got %v", tt.wantStatusCode, st.Code())
					}
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if resp == nil || !resp.Success {
					t.Error("expected successful response")
				}
			}
		})
	}
}

// TestGetReplicationPolicies tests the GetReplicationPolicies gRPC handler
func TestGetReplicationPolicies(t *testing.T) {
	tests := []struct {
		name           string
		setupStorage   func() common.Storage
		request        *objstorepb.GetReplicationPoliciesRequest
		wantError      bool
		wantCount      int
		wantStatusCode codes.Code
	}{
		{
			name: "get all policies",
			setupStorage: func() common.Storage {
				storage := newMockReplicationCapableStorage()
				storage.replicationMgr.policies["policy1"] = common.ReplicationPolicy{
					ID:                  "policy1",
					SourceBackend:       "local",
					DestinationBackend:  "s3",
					Enabled:             true,
					ReplicationMode:     common.ReplicationModeTransparent,
				}
				storage.replicationMgr.policies["policy2"] = common.ReplicationPolicy{
					ID:                  "policy2",
					SourceBackend:       "s3",
					DestinationBackend:  "gcs",
					Enabled:             false,
					ReplicationMode:     common.ReplicationModeOpaque,
				}
				return storage
			},
			request:        &objstorepb.GetReplicationPoliciesRequest{},
			wantError:      false,
			wantCount:      2,
			wantStatusCode: codes.OK,
		},
		{
			name: "no policies",
			setupStorage: func() common.Storage {
				return newMockReplicationCapableStorage()
			},
			request:        &objstorepb.GetReplicationPoliciesRequest{},
			wantError:      false,
			wantCount:      0,
			wantStatusCode: codes.OK,
		},
		{
			name: "get policies error",
			setupStorage: func() common.Storage {
				storage := newMockReplicationCapableStorage()
				storage.replicationMgr.getPoliciesError = errors.New("failed to get policies")
				return storage
			},
			request:        &objstorepb.GetReplicationPoliciesRequest{},
			wantError:      true,
			wantStatusCode: codes.Internal,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage := tt.setupStorage()
			server, err := newTestServer(t, storage)
			if err != nil {
				t.Fatalf("Failed to create server: %v", err)
			}

			resp, err := server.GetReplicationPolicies(context.Background(), tt.request)

			if tt.wantError {
				if err == nil {
					t.Error("expected error but got none")
				}
				if st, ok := status.FromError(err); ok {
					if st.Code() != tt.wantStatusCode {
						t.Errorf("expected status code %v, got %v", tt.wantStatusCode, st.Code())
					}
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if resp == nil {
					t.Fatal("expected response but got nil")
				}
				if len(resp.Policies) != tt.wantCount {
					t.Errorf("expected %d policies, got %d", tt.wantCount, len(resp.Policies))
				}
			}
		})
	}
}

// TestGetReplicationPolicy tests the GetReplicationPolicy gRPC handler
func TestGetReplicationPolicy(t *testing.T) {
	tests := []struct {
		name           string
		setupStorage   func() common.Storage
		request        *objstorepb.GetReplicationPolicyRequest
		wantError      bool
		wantStatusCode codes.Code
	}{
		{
			name: "get existing policy",
			setupStorage: func() common.Storage {
				storage := newMockReplicationCapableStorage()
				storage.replicationMgr.policies["test-policy"] = common.ReplicationPolicy{
					ID:                  "test-policy",
					SourceBackend:       "local",
					DestinationBackend:  "s3",
					CheckInterval:       5 * time.Minute,
					Enabled:             true,
					ReplicationMode:     common.ReplicationModeTransparent,
				}
				return storage
			},
			request: &objstorepb.GetReplicationPolicyRequest{
				Id: "test-policy",
			},
			wantError:      false,
			wantStatusCode: codes.OK,
		},
		{
			name: "policy ID is empty",
			setupStorage: func() common.Storage {
				return newMockReplicationCapableStorage()
			},
			request: &objstorepb.GetReplicationPolicyRequest{
				Id: "",
			},
			wantError:      true,
			wantStatusCode: codes.InvalidArgument,
		},
		{
			name: "policy not found",
			setupStorage: func() common.Storage {
				return newMockReplicationCapableStorage()
			},
			request: &objstorepb.GetReplicationPolicyRequest{
				Id: "nonexistent",
			},
			wantError:      true,
			wantStatusCode: codes.Internal,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage := tt.setupStorage()
			server, err := newTestServer(t, storage)
			if err != nil {
				t.Fatalf("Failed to create server: %v", err)
			}

			resp, err := server.GetReplicationPolicy(context.Background(), tt.request)

			if tt.wantError {
				if err == nil {
					t.Error("expected error but got none")
				}
				if st, ok := status.FromError(err); ok {
					if st.Code() != tt.wantStatusCode {
						t.Errorf("expected status code %v, got %v", tt.wantStatusCode, st.Code())
					}
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if resp == nil || resp.Policy == nil {
					t.Error("expected policy in response")
				}
			}
		})
	}
}

// TestTriggerReplication tests the TriggerReplication gRPC handler
func TestTriggerReplication(t *testing.T) {
	tests := []struct {
		name           string
		setupStorage   func() common.Storage
		request        *objstorepb.TriggerReplicationRequest
		wantError      bool
		wantStatusCode codes.Code
		wantSynced     int32
	}{
		{
			name: "sync specific policy",
			setupStorage: func() common.Storage {
				return newMockReplicationCapableStorage()
			},
			request: &objstorepb.TriggerReplicationRequest{
				PolicyId: "test-policy",
			},
			wantError:      false,
			wantStatusCode: codes.OK,
			wantSynced:     3,
		},
		{
			name: "sync all policies",
			setupStorage: func() common.Storage {
				return newMockReplicationCapableStorage()
			},
			request: &objstorepb.TriggerReplicationRequest{
				PolicyId: "",
			},
			wantError:      false,
			wantStatusCode: codes.OK,
			wantSynced:     5,
		},
		{
			name: "sync policy error",
			setupStorage: func() common.Storage {
				storage := newMockReplicationCapableStorage()
				storage.replicationMgr.syncPolicyError = errors.New("sync failed")
				return storage
			},
			request: &objstorepb.TriggerReplicationRequest{
				PolicyId: "test-policy",
			},
			wantError:      true,
			wantStatusCode: codes.Internal,
		},
		{
			name: "sync all error",
			setupStorage: func() common.Storage {
				storage := newMockReplicationCapableStorage()
				storage.replicationMgr.syncAllError = errors.New("sync all failed")
				return storage
			},
			request: &objstorepb.TriggerReplicationRequest{
				PolicyId: "",
			},
			wantError:      true,
			wantStatusCode: codes.Internal,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			storage := tt.setupStorage()
			server, err := newTestServer(t, storage)
			if err != nil {
				t.Fatalf("Failed to create server: %v", err)
			}

			resp, err := server.TriggerReplication(context.Background(), tt.request)

			if tt.wantError {
				if err == nil {
					t.Error("expected error but got none")
				}
				if st, ok := status.FromError(err); ok {
					if st.Code() != tt.wantStatusCode {
						t.Errorf("expected status code %v, got %v", tt.wantStatusCode, st.Code())
					}
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if resp == nil || !resp.Success {
					t.Error("expected successful response")
				}
				if resp.Result == nil {
					t.Fatal("expected result in response")
				}
				if resp.Result.Synced != tt.wantSynced {
					t.Errorf("expected %d synced objects, got %d", tt.wantSynced, resp.Result.Synced)
				}
			}
		})
	}
}

// TestProtoConversions tests the protobuf conversion functions
func TestProtoConversions(t *testing.T) {
	t.Run("protoToReplicationPolicy", func(t *testing.T) {
		now := time.Now()
		proto := &objstorepb.ReplicationPolicy{
			Id:                    "test-policy",
			SourceBackend:         "local",
			SourceSettings:        map[string]string{"path": "/source"},
			SourcePrefix:          "prefix/",
			DestinationBackend:    "s3",
			DestinationSettings:   map[string]string{"bucket": "dest"},
			CheckIntervalSeconds:  300,
			LastSyncTime:          timestamppb.New(now),
			Enabled:               true,
			ReplicationMode:       objstorepb.ReplicationMode_TRANSPARENT,
			Encryption: &objstorepb.EncryptionPolicy{
				Backend: &objstorepb.EncryptionConfig{
					Enabled:    true,
					Provider:   "custom",
					DefaultKey: "key1",
				},
			},
		}

		domain, err := protoToReplicationPolicy(proto)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if domain.ID != proto.Id {
			t.Errorf("expected ID %s, got %s", proto.Id, domain.ID)
		}
		if domain.SourceBackend != proto.SourceBackend {
			t.Errorf("expected SourceBackend %s, got %s", proto.SourceBackend, domain.SourceBackend)
		}
		if domain.CheckInterval != 300*time.Second {
			t.Errorf("expected CheckInterval 300s, got %v", domain.CheckInterval)
		}
		if domain.ReplicationMode != common.ReplicationModeTransparent {
			t.Errorf("expected transparent mode, got %v", domain.ReplicationMode)
		}
		if domain.Encryption == nil || domain.Encryption.Backend == nil {
			t.Error("expected encryption config")
		}
	})

	t.Run("replicationPolicyToProto", func(t *testing.T) {
		now := time.Now()
		domain := &common.ReplicationPolicy{
			ID:                  "test-policy",
			SourceBackend:       "local",
			SourceSettings:      map[string]string{"path": "/source"},
			DestinationBackend:  "s3",
			DestinationSettings: map[string]string{"bucket": "dest"},
			CheckInterval:       5 * time.Minute,
			LastSyncTime:        now,
			Enabled:             true,
			ReplicationMode:     common.ReplicationModeOpaque,
			Encryption: &common.EncryptionPolicy{
				Source: &common.EncryptionConfig{
					Enabled:    true,
					Provider:   "custom",
					DefaultKey: "key2",
				},
			},
		}

		proto := replicationPolicyToProto(domain)
		if proto == nil {
			t.Fatal("expected proto policy")
		}

		if proto.Id != domain.ID {
			t.Errorf("expected ID %s, got %s", domain.ID, proto.Id)
		}
		if proto.CheckIntervalSeconds != 300 {
			t.Errorf("expected CheckIntervalSeconds 300, got %d", proto.CheckIntervalSeconds)
		}
		if proto.ReplicationMode != objstorepb.ReplicationMode_OPAQUE {
			t.Errorf("expected opaque mode, got %v", proto.ReplicationMode)
		}
		if proto.Encryption == nil || proto.Encryption.Source == nil {
			t.Error("expected source encryption config")
		}
	})

	t.Run("syncResultToProto", func(t *testing.T) {
		domain := &common.SyncResult{
			PolicyID:   "test-policy",
			Synced:     10,
			Deleted:    2,
			Failed:     1,
			BytesTotal: 2048,
			Duration:   200 * time.Millisecond,
			Errors:     []string{"error1", "error2"},
		}

		proto := syncResultToProto(domain)
		if proto == nil {
			t.Fatal("expected proto sync result")
		}

		if proto.PolicyId != domain.PolicyID {
			t.Errorf("expected PolicyID %s, got %s", domain.PolicyID, proto.PolicyId)
		}
		if proto.Synced != 10 {
			t.Errorf("expected Synced 10, got %d", proto.Synced)
		}
		if proto.DurationMs != 200 {
			t.Errorf("expected DurationMs 200, got %d", proto.DurationMs)
		}
		if len(proto.Errors) != 2 {
			t.Errorf("expected 2 errors, got %d", len(proto.Errors))
		}
	})

	t.Run("nil conversions", func(t *testing.T) {
		if result := replicationPolicyToProto(nil); result != nil {
			t.Error("expected nil for nil input")
		}
		if result := syncResultToProto(nil); result != nil {
			t.Error("expected nil for nil input")
		}
		if result := encryptionPolicyToProto(nil); result != nil {
			t.Error("expected nil for nil input")
		}
		if result := protoToEncryptionPolicy(nil); result != nil {
			t.Error("expected nil for nil input")
		}
	})
}
