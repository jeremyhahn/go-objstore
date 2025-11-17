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
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

func TestLifecycleManager_AddPolicy(t *testing.T) {
	// Test case 1: Successful AddPolicy
	called := false
	mockLifecycleManager := &MockLifecycleManager{
		AddPolicyFunc: func(policy common.LifecyclePolicy) error {
			called = true
			if policy.ID != "test-policy" {
				t.Errorf("Expected policy ID 'test-policy', got '%s'", policy.ID)
			}
			return nil
		},
	}

	policy := common.LifecyclePolicy{
		ID:        "test-policy",
		Prefix:    "test/",
		Retention: 24 * time.Hour,
		Action:    "delete",
	}

	err := mockLifecycleManager.AddPolicy(policy)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if !called {
		t.Error("Expected AddPolicyFunc to be called, but it wasn't")
	}

	// Test case 2: Error during AddPolicy (mocking an error)
	mockLifecycleManagerWithError := &MockLifecycleManager{
		AddPolicyFunc: func(policy common.LifecyclePolicy) error {
			return common.ErrInvalidPolicy // Simulate an error
		},
	}

	err = mockLifecycleManagerWithError.AddPolicy(policy)
	if err == nil || err.Error() != common.ErrInvalidPolicy.Error() {
		t.Errorf("Expected error '%v', got '%v'", common.ErrInvalidPolicy, err)
	}
}

func TestLifecycleManager_RemovePolicy(t *testing.T) {
	// Test case 1: Successful RemovePolicy
	called := false
	mockLifecycleManager := &MockLifecycleManager{
		RemovePolicyFunc: func(id string) error {
			called = true
			if id != "test-policy" {
				t.Errorf("Expected policy ID 'test-policy', got '%s'", id)
			}
			return nil
		},
	}

	err := mockLifecycleManager.RemovePolicy("test-policy")
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if !called {
		t.Error("Expected RemovePolicyFunc to be called, but it wasn't")
	}

	// Test case 2: Error during RemovePolicy (mocking an error)
	mockLifecycleManagerWithError := &MockLifecycleManager{
		RemovePolicyFunc: func(id string) error {
			return common.ErrPolicyNotFound // Simulate an error
		},
	}

	err = mockLifecycleManagerWithError.RemovePolicy("non-existent-policy")
	if err == nil || err.Error() != common.ErrPolicyNotFound.Error() {
		t.Errorf("Expected error '%v', got '%v'", common.ErrPolicyNotFound, err)
	}
}

func TestLifecycleManager_GetPolicies(t *testing.T) {
	// Test case 1: Successful GetPolicies
	called := false
	policy := common.LifecyclePolicy{
		ID:        "test-policy",
		Prefix:    "test/",
		Retention: 24 * time.Hour,
		Action:    "delete",
	}
	mockLifecycleManager := &MockLifecycleManager{
		GetPoliciesFunc: func() ([]common.LifecyclePolicy, error) {
			called = true
			return []common.LifecyclePolicy{policy}, nil
		},
	}

	policies, err := mockLifecycleManager.GetPolicies()
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if !called {
		t.Error("Expected GetPoliciesFunc to be called, but it wasn't")
	}
	if len(policies) != 1 || policies[0].ID != policy.ID {
		t.Errorf("Expected 1 policy with ID '%s', got %v", policy.ID, policies)
	}

	// Test case 2: Error during GetPolicies (mocking an error)
	mockLifecycleManagerWithError := &MockLifecycleManager{
		GetPoliciesFunc: func() ([]common.LifecyclePolicy, error) {
			return nil, common.ErrInternal // Simulate an error
		},
	}

	policies, err = mockLifecycleManagerWithError.GetPolicies()
	if err == nil || err.Error() != common.ErrInternal.Error() {
		t.Errorf("Expected error '%v', got '%v'", common.ErrInternal, err)
	}
	if policies != nil {
		t.Errorf("Expected nil policies, got %v", policies)
	}
}
