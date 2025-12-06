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

package local

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// TestLocalStorage_PersistentLifecycleManager tests that the local storage
// backend can use the persistent lifecycle manager
func TestLocalStorage_PersistentLifecycleManager(t *testing.T) {
	dir := t.TempDir()

	// Create local storage with persistent lifecycle manager
	s1 := New()
	err := s1.Configure(map[string]string{
		"path":                 dir,
		"lifecycleManagerType": "persistent",
		"lifecyclePolicyFile":  "test-policies.json",
	})
	if err != nil {
		t.Fatalf("Failed to configure storage: %v", err)
	}

	// Verify it's using persistent manager
	ll1 := s1.(*Local)
	if _, ok := ll1.lifecycleManager.(*common.PersistentLifecycleManager); !ok {
		t.Fatal("Expected persistent lifecycle manager")
	}

	// Add some policies
	policy1 := common.LifecyclePolicy{
		ID:        "policy-1",
		Prefix:    "logs/",
		Retention: 24 * 3600 * 1000000000, // 24 hours in nanoseconds
		Action:    "delete",
	}
	policy2 := common.LifecyclePolicy{
		ID:        "policy-2",
		Prefix:    "temp/",
		Retention: 48 * 3600 * 1000000000, // 48 hours in nanoseconds
		Action:    "delete",
	}

	if err := s1.AddPolicy(policy1); err != nil {
		t.Fatalf("Failed to add policy 1: %v", err)
	}
	if err := s1.AddPolicy(policy2); err != nil {
		t.Fatalf("Failed to add policy 2: %v", err)
	}

	// Verify policies were added
	policies, err := s1.GetPolicies()
	if err != nil {
		t.Fatalf("Failed to get policies: %v", err)
	}
	if len(policies) != 2 {
		t.Errorf("Expected 2 policies, got %d", len(policies))
	}

	// Verify policy file was created
	policyFilePath := filepath.Join(dir, "test-policies.json")
	if _, err := os.Stat(policyFilePath); os.IsNotExist(err) {
		t.Fatal("Policy file was not created")
	}

	// Create a new storage instance (simulating process restart)
	s2 := New()
	err = s2.Configure(map[string]string{
		"path":                 dir,
		"lifecycleManagerType": "persistent",
		"lifecyclePolicyFile":  "test-policies.json",
	})
	if err != nil {
		t.Fatalf("Failed to configure second storage: %v", err)
	}

	// Verify policies were loaded
	policies2, err := s2.GetPolicies()
	if err != nil {
		t.Fatalf("Failed to get policies from second storage: %v", err)
	}
	if len(policies2) != 2 {
		t.Errorf("Expected 2 policies after restart, got %d", len(policies2))
	}

	// Verify policy contents
	foundPolicy1 := false
	foundPolicy2 := false
	for _, p := range policies2 {
		if p.ID == "policy-1" {
			foundPolicy1 = true
			if p.Prefix != "logs/" || p.Action != "delete" {
				t.Error("Policy 1 contents mismatch after restart")
			}
		}
		if p.ID == "policy-2" {
			foundPolicy2 = true
			if p.Prefix != "temp/" || p.Action != "delete" {
				t.Error("Policy 2 contents mismatch after restart")
			}
		}
	}

	if !foundPolicy1 {
		t.Error("Policy 1 not found after restart")
	}
	if !foundPolicy2 {
		t.Error("Policy 2 not found after restart")
	}

	// Test removing a policy
	if err := s2.RemovePolicy("policy-1"); err != nil {
		t.Fatalf("Failed to remove policy: %v", err)
	}

	// Verify removal persisted
	policies3, err := s2.GetPolicies()
	if err != nil {
		t.Fatalf("Failed to get policies after removal: %v", err)
	}
	if len(policies3) != 1 {
		t.Errorf("Expected 1 policy after removal, got %d", len(policies3))
	}
	if policies3[0].ID != "policy-2" {
		t.Errorf("Expected remaining policy to be policy-2, got %s", policies3[0].ID)
	}

	// Create a third instance to verify the removal persisted
	s3 := New()
	err = s3.Configure(map[string]string{
		"path":                 dir,
		"lifecycleManagerType": "persistent",
		"lifecyclePolicyFile":  "test-policies.json",
	})
	if err != nil {
		t.Fatalf("Failed to configure third storage: %v", err)
	}

	policies4, err := s3.GetPolicies()
	if err != nil {
		t.Fatalf("Failed to get policies from third storage: %v", err)
	}
	if len(policies4) != 1 {
		t.Errorf("Expected 1 policy in third instance, got %d", len(policies4))
	}
}

// TestLocalStorage_InMemoryLifecycleManager tests that the default behavior
// is still in-memory for backwards compatibility
func TestLocalStorage_InMemoryLifecycleManager(t *testing.T) {
	dir := t.TempDir()

	// Create local storage without specifying lifecycle manager type
	s := New()
	err := s.Configure(map[string]string{
		"path": dir,
	})
	if err != nil {
		t.Fatalf("Failed to configure storage: %v", err)
	}

	// Verify it's using in-memory manager by default
	ll := s.(*Local)
	if _, ok := ll.lifecycleManager.(*LifecycleManager); !ok {
		t.Fatal("Expected in-memory lifecycle manager by default")
	}

	// Add a policy
	policy := common.LifecyclePolicy{
		ID:        "test-policy",
		Prefix:    "logs/",
		Retention: 24 * 3600 * 1000000000,
		Action:    "delete",
	}

	if err := s.AddPolicy(policy); err != nil {
		t.Fatalf("Failed to add policy: %v", err)
	}

	// Create a new storage instance (simulating process restart)
	s2 := New()
	err = s2.Configure(map[string]string{
		"path": dir,
	})
	if err != nil {
		t.Fatalf("Failed to configure second storage: %v", err)
	}

	// Verify policies were NOT persisted (in-memory only)
	policies, err := s2.GetPolicies()
	if err != nil {
		t.Fatalf("Failed to get policies: %v", err)
	}
	if len(policies) != 0 {
		t.Errorf("Expected 0 policies after restart with in-memory manager, got %d", len(policies))
	}
}

// TestLocalStorage_PersistentLifecycleManager_DefaultFile tests that the default
// policy file name is used when not specified
func TestLocalStorage_PersistentLifecycleManager_DefaultFile(t *testing.T) {
	dir := t.TempDir()

	// Create local storage with persistent lifecycle manager but no file specified
	s := New()
	err := s.Configure(map[string]string{
		"path":                 dir,
		"lifecycleManagerType": "persistent",
	})
	if err != nil {
		t.Fatalf("Failed to configure storage: %v", err)
	}

	// Add a policy
	policy := common.LifecyclePolicy{
		ID:        "test-policy",
		Prefix:    "data/",
		Retention: 24 * 3600 * 1000000000,
		Action:    "delete",
	}

	if err := s.AddPolicy(policy); err != nil {
		t.Fatalf("Failed to add policy: %v", err)
	}

	// Verify default policy file was created
	defaultPolicyFile := filepath.Join(dir, ".lifecycle-policies.json")
	if _, err := os.Stat(defaultPolicyFile); os.IsNotExist(err) {
		t.Fatal("Default policy file was not created")
	}
}

// TestLocalStorage_InvalidLifecycleManagerType tests error handling for invalid manager type
func TestLocalStorage_InvalidLifecycleManagerType(t *testing.T) {
	dir := t.TempDir()

	s := New()
	err := s.Configure(map[string]string{
		"path":                 dir,
		"lifecycleManagerType": "invalid",
	})
	if err == nil {
		t.Fatal("Expected error for invalid lifecycle manager type")
	}
}

// TestLocalStorage_PersistentLifecycleWithData tests that persistent lifecycle
// manager works correctly with actual data operations
func TestLocalStorage_PersistentLifecycleWithData(t *testing.T) {
	dir := t.TempDir()

	// Create storage with persistent lifecycle
	s := New()
	err := s.Configure(map[string]string{
		"path":                 dir,
		"lifecycleManagerType": "persistent",
	})
	if err != nil {
		t.Fatalf("Failed to configure storage: %v", err)
	}

	// Store some data
	testData := []byte("test data")
	if err := s.Put("data/test.txt", bytes.NewReader(testData)); err != nil {
		t.Fatalf("Failed to put data: %v", err)
	}

	// Add a policy
	policy := common.LifecyclePolicy{
		ID:        "data-policy",
		Prefix:    "data/",
		Retention: 1 * 3600 * 1000000000, // 1 hour
		Action:    "delete",
	}
	if err := s.AddPolicy(policy); err != nil {
		t.Fatalf("Failed to add policy: %v", err)
	}

	// Verify data still exists
	reader, err := s.Get("data/test.txt")
	if err != nil {
		t.Fatalf("Failed to get data: %v", err)
	}
	reader.Close()

	// Restart storage
	s2 := New()
	err = s2.Configure(map[string]string{
		"path":                 dir,
		"lifecycleManagerType": "persistent",
	})
	if err != nil {
		t.Fatalf("Failed to configure second storage: %v", err)
	}

	// Verify policy persisted
	policies, err := s2.GetPolicies()
	if err != nil {
		t.Fatalf("Failed to get policies: %v", err)
	}
	if len(policies) != 1 {
		t.Errorf("Expected 1 policy after restart, got %d", len(policies))
	}

	// Verify data still exists
	reader2, err := s2.Get("data/test.txt")
	if err != nil {
		t.Fatalf("Failed to get data after restart: %v", err)
	}
	reader2.Close()
}
