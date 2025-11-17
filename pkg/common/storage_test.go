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

package common_test

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// MockArchiver is a mock implementation of common.Archiver for testing.
type MockArchiver struct {
	PutFunc func(key string, data io.Reader) error
}

func (m *MockArchiver) Put(key string, data io.Reader) error {
	if m.PutFunc != nil {
		return m.PutFunc(key, data)
	}
	return nil
}

// MockLifecycleManager is a mock implementation of common.LifecycleManager for testing.
type MockLifecycleManager struct {
	AddPolicyFunc    func(policy common.LifecyclePolicy) error
	RemovePolicyFunc func(id string) error
	GetPoliciesFunc  func() ([]common.LifecyclePolicy, error)
}

func (m *MockLifecycleManager) AddPolicy(policy common.LifecyclePolicy) error {
	if m.AddPolicyFunc != nil {
		return m.AddPolicyFunc(policy)
	}
	return nil
}

func (m *MockLifecycleManager) RemovePolicy(id string) error {
	if m.RemovePolicyFunc != nil {
		return m.RemovePolicyFunc(id)
	}
	return nil
}

func (m *MockLifecycleManager) GetPolicies() ([]common.LifecyclePolicy, error) {
	if m.GetPoliciesFunc != nil {
		return m.GetPoliciesFunc()
	}
	return nil, nil
}

// MockStorage is a mock implementation of common.Storage for testing.
type MockStorage struct {
	MockLifecycleManager
	ConfigureFunc         func(settings map[string]string) error
	PutFunc               func(key string, data io.Reader) error
	PutWithContextFunc    func(ctx context.Context, key string, data io.Reader) error
	PutWithMetadataFunc   func(ctx context.Context, key string, data io.Reader, metadata *common.Metadata) error
	GetFunc               func(key string) (io.ReadCloser, error)
	GetWithContextFunc    func(ctx context.Context, key string) (io.ReadCloser, error)
	GetMetadataFunc       func(ctx context.Context, key string) (*common.Metadata, error)
	UpdateMetadataFunc    func(ctx context.Context, key string, metadata *common.Metadata) error
	DeleteFunc            func(key string) error
	DeleteWithContextFunc func(ctx context.Context, key string) error
	ExistsFunc            func(ctx context.Context, key string) (bool, error)
	ArchiveFunc           func(key string, destination common.Archiver) error
	ListFunc              func(prefix string) ([]string, error)
	ListWithContextFunc   func(ctx context.Context, prefix string) ([]string, error)
	ListWithOptionsFunc   func(ctx context.Context, opts *common.ListOptions) (*common.ListResult, error)
}

func (m *MockStorage) Configure(settings map[string]string) error {
	if m.ConfigureFunc != nil {
		return m.ConfigureFunc(settings)
	}
	return nil
}

func (m *MockStorage) Put(key string, data io.Reader) error {
	if m.PutFunc != nil {
		return m.PutFunc(key, data)
	}
	return nil
}

func (m *MockStorage) PutWithContext(ctx context.Context, key string, data io.Reader) error {
	if m.PutWithContextFunc != nil {
		return m.PutWithContextFunc(ctx, key, data)
	}
	return nil
}

func (m *MockStorage) PutWithMetadata(ctx context.Context, key string, data io.Reader, metadata *common.Metadata) error {
	if m.PutWithMetadataFunc != nil {
		return m.PutWithMetadataFunc(ctx, key, data, metadata)
	}
	return nil
}

func (m *MockStorage) Get(key string) (io.ReadCloser, error) {
	if m.GetFunc != nil {
		return m.GetFunc(key)
	}
	return io.NopCloser(strings.NewReader("")), nil
}

func (m *MockStorage) GetWithContext(ctx context.Context, key string) (io.ReadCloser, error) {
	if m.GetWithContextFunc != nil {
		return m.GetWithContextFunc(ctx, key)
	}
	return io.NopCloser(strings.NewReader("")), nil
}

func (m *MockStorage) GetMetadata(ctx context.Context, key string) (*common.Metadata, error) {
	if m.GetMetadataFunc != nil {
		return m.GetMetadataFunc(ctx, key)
	}
	return nil, nil
}

func (m *MockStorage) UpdateMetadata(ctx context.Context, key string, metadata *common.Metadata) error {
	if m.UpdateMetadataFunc != nil {
		return m.UpdateMetadataFunc(ctx, key, metadata)
	}
	return nil
}

func (m *MockStorage) Delete(key string) error {
	if m.DeleteFunc != nil {
		return m.DeleteFunc(key)
	}
	return nil
}

func (m *MockStorage) DeleteWithContext(ctx context.Context, key string) error {
	if m.DeleteWithContextFunc != nil {
		return m.DeleteWithContextFunc(ctx, key)
	}
	return nil
}

func (m *MockStorage) Exists(ctx context.Context, key string) (bool, error) {
	if m.ExistsFunc != nil {
		return m.ExistsFunc(ctx, key)
	}
	return false, nil
}

func (m *MockStorage) Archive(key string, destination common.Archiver) error {
	if m.ArchiveFunc != nil {
		return m.ArchiveFunc(key, destination)
	}
	return nil
}

func (m *MockStorage) List(prefix string) ([]string, error) {
	if m.ListFunc != nil {
		return m.ListFunc(prefix)
	}
	return nil, nil
}

func (m *MockStorage) ListWithContext(ctx context.Context, prefix string) ([]string, error) {
	if m.ListWithContextFunc != nil {
		return m.ListWithContextFunc(ctx, prefix)
	}
	return nil, nil
}

func (m *MockStorage) ListWithOptions(ctx context.Context, opts *common.ListOptions) (*common.ListResult, error) {
	if m.ListWithOptionsFunc != nil {
		return m.ListWithOptionsFunc(ctx, opts)
	}
	return &common.ListResult{}, nil
}

// TestStorageInterface ensures that MockStorage satisfies the common.Storage interface.
func TestStorageInterface(t *testing.T) {
	var _ common.Storage = (*MockStorage)(nil)
}

// TestArchiverInterface ensures that MockArchiver satisfies the common.Archiver interface.
func TestArchiverInterface(t *testing.T) {
	var _ common.Archiver = (*MockArchiver)(nil)
}

// TestLifecycleManagerInterface ensures that MockLifecycleManager satisfies the common.LifecycleManager interface.
func TestLifecycleManagerInterface(t *testing.T) {
	var _ common.LifecycleManager = (*MockLifecycleManager)(nil)
}

func TestMockStorage_LifecycleMethods(t *testing.T) {
	mockStorage := &MockStorage{}

	policy := common.LifecyclePolicy{
		ID:        "test-policy",
		Prefix:    "test/",
		Retention: 24 * time.Hour,
		Action:    "delete",
	}

	// Test AddPolicy
	mockStorage.AddPolicy(policy)
	policies, _ := mockStorage.GetPolicies()
	if len(policies) != 0 { // Mock implementation returns empty slice by default
		t.Errorf("Expected 0 policies, got %d", len(policies))
	}

	// Test with custom AddPolicyFunc and GetPoliciesFunc
	addedPolicy := common.LifecyclePolicy{}
	mockStorage.AddPolicyFunc = func(p common.LifecyclePolicy) error {
		addedPolicy = p
		return nil
	}
	mockStorage.GetPoliciesFunc = func() ([]common.LifecyclePolicy, error) {
		return []common.LifecyclePolicy{addedPolicy}, nil
	}

	mockStorage.AddPolicy(policy)
	policies, _ = mockStorage.GetPolicies()
	if len(policies) != 1 || policies[0].ID != policy.ID {
		t.Errorf("AddPolicy or GetPolicies failed. Expected policy ID %s, got %v", policy.ID, policies)
	}

	// Test RemovePolicy
	removedPolicyID := ""
	mockStorage.RemovePolicyFunc = func(id string) error {
		removedPolicyID = id
		mockStorage.GetPoliciesFunc = func() ([]common.LifecyclePolicy, error) { return nil, nil } // Simulate removal
		return nil
	}
	mockStorage.RemovePolicy(policy.ID)
	if removedPolicyID != policy.ID {
		t.Errorf("RemovePolicy failed. Expected to remove policy ID %s, got %s", policy.ID, removedPolicyID)
	}
	policies, _ = mockStorage.GetPolicies()
	if len(policies) != 0 {
		t.Errorf("Expected 0 policies after removal, got %d", len(policies))
	}
}
