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

//go:build integration

package local

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/factory"
	testcommon "github.com/jeremyhahn/go-objstore/test/integration/common"
)

func TestLocal_BasicOps(t *testing.T) {
	tmpDir := filepath.Join("/tmp", fmt.Sprintf("objstore-local-test-%d", time.Now().UnixNano()))
	defer os.RemoveAll(tmpDir)

	st, err := factory.NewStorage("local", map[string]string{
		"path": tmpDir,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Test Put
	key := "test/file.txt"
	data := []byte("hello local storage")
	if err := st.Put(key, bytes.NewReader(data)); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Test Get
	rc, err := st.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if string(got) != string(data) {
		t.Fatalf("Get mismatch: got %s, want %s", got, data)
	}

	// Test Delete
	if err := st.Delete(key); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify deletion
	_, err = st.Get(key)
	if err == nil {
		t.Fatal("Expected error getting deleted file")
	}
}

func TestLocal_EmptyFile(t *testing.T) {
	tmpDir := filepath.Join("/tmp", fmt.Sprintf("objstore-local-empty-%d", time.Now().UnixNano()))
	defer os.RemoveAll(tmpDir)

	st, err := factory.NewStorage("local", map[string]string{
		"path": tmpDir,
	})
	if err != nil {
		t.Fatal(err)
	}

	key := "empty.txt"
	if err := st.Put(key, bytes.NewReader([]byte{})); err != nil {
		t.Fatalf("Put empty file failed: %v", err)
	}

	rc, err := st.Get(key)
	if err != nil {
		t.Fatalf("Get empty file failed: %v", err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if len(got) != 0 {
		t.Fatalf("Expected empty file, got %d bytes", len(got))
	}
}

func TestLocal_SpecialCharacters(t *testing.T) {
	tmpDir := filepath.Join("/tmp", fmt.Sprintf("objstore-local-special-%d", time.Now().UnixNano()))
	defer os.RemoveAll(tmpDir)

	st, err := factory.NewStorage("local", map[string]string{
		"path": tmpDir,
	})
	if err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		name string
		key  string
	}{
		{"spaces", "file with spaces.txt"},
		{"unicode", "файл-文件-ファイル.txt"},
		{"dots", "file.with.many.dots.txt"},
		{"nested", "deep/nested/path/file.txt"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			data := []byte("test data for " + tc.name)
			if err := st.Put(tc.key, bytes.NewReader(data)); err != nil {
				t.Fatalf("Put failed for %s: %v", tc.key, err)
			}
			rc, err := st.Get(tc.key)
			if err != nil {
				t.Fatalf("Get failed for %s: %v", tc.key, err)
			}
			got, _ := io.ReadAll(rc)
			rc.Close()
			if string(got) != string(data) {
				t.Fatalf("Data mismatch for %s", tc.key)
			}
		})
	}
}

func TestLocal_LargeFile(t *testing.T) {
	tmpDir := filepath.Join("/tmp", fmt.Sprintf("objstore-local-large-%d", time.Now().UnixNano()))
	defer os.RemoveAll(tmpDir)

	st, err := factory.NewStorage("local", map[string]string{
		"path": tmpDir,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Create 1MB file
	size := 1024 * 1024
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 256)
	}

	key := "large/file.bin"
	if err := st.Put(key, bytes.NewReader(data)); err != nil {
		t.Fatalf("Put large file failed: %v", err)
	}

	rc, err := st.Get(key)
	if err != nil {
		t.Fatalf("Get large file failed: %v", err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()

	if len(got) != size {
		t.Fatalf("Size mismatch: got %d, want %d", len(got), size)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("Data mismatch for large file")
	}
}

func TestLocal_Lifecycle(t *testing.T) {
	tmpDir := filepath.Join("/tmp", fmt.Sprintf("objstore-lifecycle-%d", time.Now().UnixNano()))
	defer os.RemoveAll(tmpDir)

	st, err := factory.NewStorage("local", map[string]string{
		"path": tmpDir,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Note: Lifecycle tests are basic validation only
	// Full lifecycle functionality should be tested in pkg/local unit tests

	// Get initial policies (should be empty)
	policies, err := st.GetPolicies()
	if err != nil {
		t.Fatalf("GetPolicies failed: %v", err)
	}
	initialCount := len(policies)

	t.Logf("Local storage lifecycle methods available, initial policy count: %d", initialCount)
}

// TestLocal_PersistentLifecycleManager tests persistent lifecycle manager with process restart
func TestLocal_PersistentLifecycleManager(t *testing.T) {
	tmpDir := filepath.Join("/tmp", fmt.Sprintf("objstore-persistent-lifecycle-%d", time.Now().UnixNano()))
	defer os.RemoveAll(tmpDir)

	// Create first instance with persistent lifecycle manager
	st1, err := factory.NewStorage("local", map[string]string{
		"path":                 tmpDir,
		"lifecycleManagerType": "persistent",
		"lifecyclePolicyFile":  "test-policies.json",
	})
	if err != nil {
		t.Fatalf("Failed to create first storage instance: %v", err)
	}

	// Add some test policies
	policy1 := common.LifecyclePolicy{
		ID:        "test-policy-1",
		Prefix:    "logs/",
		Retention: 24 * time.Hour,
		Action:    "delete",
	}
	policy2 := common.LifecyclePolicy{
		ID:        "test-policy-2",
		Prefix:    "temp/",
		Retention: 48 * time.Hour,
		Action:    "delete",
	}

	if err := st1.AddPolicy(policy1); err != nil {
		t.Fatalf("Failed to add policy 1: %v", err)
	}
	if err := st1.AddPolicy(policy2); err != nil {
		t.Fatalf("Failed to add policy 2: %v", err)
	}

	// Verify policies were added
	policies, err := st1.GetPolicies()
	if err != nil {
		t.Fatalf("Failed to get policies: %v", err)
	}
	if len(policies) != 2 {
		t.Fatalf("Expected 2 policies, got %d", len(policies))
	}

	// Verify policy file exists on disk
	policyFilePath := filepath.Join(tmpDir, "test-policies.json")
	if _, err := os.Stat(policyFilePath); os.IsNotExist(err) {
		t.Fatal("Policy file was not created on disk")
	}

	// Read and verify policy file content
	policyFileContent, err := os.ReadFile(policyFilePath)
	if err != nil {
		t.Fatalf("Failed to read policy file: %v", err)
	}
	t.Logf("Policy file content: %s", string(policyFileContent))

	// Create second instance (simulating process restart)
	st2, err := factory.NewStorage("local", map[string]string{
		"path":                 tmpDir,
		"lifecycleManagerType": "persistent",
		"lifecyclePolicyFile":  "test-policies.json",
	})
	if err != nil {
		t.Fatalf("Failed to create second storage instance: %v", err)
	}

	// Verify policies were loaded from disk
	policies2, err := st2.GetPolicies()
	if err != nil {
		t.Fatalf("Failed to get policies from second instance: %v", err)
	}
	if len(policies2) != 2 {
		t.Fatalf("Expected 2 policies after restart, got %d", len(policies2))
	}

	// Verify policy contents
	foundPolicy1 := false
	foundPolicy2 := false
	for _, p := range policies2 {
		if p.ID == "test-policy-1" {
			foundPolicy1 = true
			if p.Prefix != "logs/" || p.Action != "delete" {
				t.Error("Policy 1 contents mismatch after restart")
			}
		}
		if p.ID == "test-policy-2" {
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
	if err := st2.RemovePolicy("test-policy-1"); err != nil {
		t.Fatalf("Failed to remove policy: %v", err)
	}

	// Verify removal persisted
	policies3, err := st2.GetPolicies()
	if err != nil {
		t.Fatalf("Failed to get policies after removal: %v", err)
	}
	if len(policies3) != 1 {
		t.Fatalf("Expected 1 policy after removal, got %d", len(policies3))
	}

	// Create third instance to verify removal persisted
	st3, err := factory.NewStorage("local", map[string]string{
		"path":                 tmpDir,
		"lifecycleManagerType": "persistent",
		"lifecyclePolicyFile":  "test-policies.json",
	})
	if err != nil {
		t.Fatalf("Failed to create third storage instance: %v", err)
	}

	policies4, err := st3.GetPolicies()
	if err != nil {
		t.Fatalf("Failed to get policies from third instance: %v", err)
	}
	if len(policies4) != 1 {
		t.Fatalf("Expected 1 policy in third instance, got %d", len(policies4))
	}
	if policies4[0].ID != "test-policy-2" {
		t.Fatalf("Expected remaining policy to be test-policy-2, got %s", policies4[0].ID)
	}

	t.Log("Persistent lifecycle manager successfully tested with process restart scenarios")
}

// TestLocal_InMemoryLifecycleManager tests default in-memory lifecycle manager behavior
func TestLocal_InMemoryLifecycleManager(t *testing.T) {
	tmpDir := filepath.Join("/tmp", fmt.Sprintf("objstore-inmemory-lifecycle-%d", time.Now().UnixNano()))
	defer os.RemoveAll(tmpDir)

	// Create first instance without specifying lifecycle manager type (defaults to in-memory)
	st1, err := factory.NewStorage("local", map[string]string{
		"path": tmpDir,
	})
	if err != nil {
		t.Fatalf("Failed to create first storage instance: %v", err)
	}

	// Add a test policy
	policy := common.LifecyclePolicy{
		ID:        "test-policy",
		Prefix:    "logs/",
		Retention: 24 * time.Hour,
		Action:    "delete",
	}

	if err := st1.AddPolicy(policy); err != nil {
		t.Fatalf("Failed to add policy: %v", err)
	}

	// Verify policy was added
	policies, err := st1.GetPolicies()
	if err != nil {
		t.Fatalf("Failed to get policies: %v", err)
	}
	if len(policies) != 1 {
		t.Fatalf("Expected 1 policy, got %d", len(policies))
	}

	// Create second instance (simulating process restart)
	st2, err := factory.NewStorage("local", map[string]string{
		"path": tmpDir,
	})
	if err != nil {
		t.Fatalf("Failed to create second storage instance: %v", err)
	}

	// Verify policies were NOT persisted (in-memory only)
	policies2, err := st2.GetPolicies()
	if err != nil {
		t.Fatalf("Failed to get policies from second instance: %v", err)
	}
	if len(policies2) != 0 {
		t.Fatalf("Expected 0 policies after restart with in-memory manager, got %d", len(policies2))
	}

	t.Log("In-memory lifecycle manager correctly does not persist policies")
}

// TestLocal_PersistentLifecycleManager_DefaultFile tests default policy file name
func TestLocal_PersistentLifecycleManager_DefaultFile(t *testing.T) {
	tmpDir := filepath.Join("/tmp", fmt.Sprintf("objstore-default-file-%d", time.Now().UnixNano()))
	defer os.RemoveAll(tmpDir)

	// Create storage with persistent manager but no file specified
	st, err := factory.NewStorage("local", map[string]string{
		"path":                 tmpDir,
		"lifecycleManagerType": "persistent",
	})
	if err != nil {
		t.Fatalf("Failed to create storage instance: %v", err)
	}

	// Add a test policy
	policy := common.LifecyclePolicy{
		ID:        "test-policy",
		Prefix:    "data/",
		Retention: 24 * time.Hour,
		Action:    "delete",
	}

	if err := st.AddPolicy(policy); err != nil {
		t.Fatalf("Failed to add policy: %v", err)
	}

	// Verify default policy file was created
	defaultPolicyFile := filepath.Join(tmpDir, ".lifecycle-policies.json")
	if _, err := os.Stat(defaultPolicyFile); os.IsNotExist(err) {
		t.Fatal("Default policy file was not created")
	}

	t.Log("Default policy file name (.lifecycle-policies.json) works correctly")
}

// TestLocal_InvalidLifecycleManagerType tests error handling for invalid manager type
func TestLocal_InvalidLifecycleManagerType(t *testing.T) {
	tmpDir := filepath.Join("/tmp", fmt.Sprintf("objstore-invalid-type-%d", time.Now().UnixNano()))
	defer os.RemoveAll(tmpDir)

	_, err := factory.NewStorage("local", map[string]string{
		"path":                 tmpDir,
		"lifecycleManagerType": "invalid-type",
	})
	if err == nil {
		t.Fatal("Expected error for invalid lifecycle manager type")
	}

	t.Logf("Correctly rejected invalid lifecycle manager type: %v", err)
}

// TestLocal_PersistentLifecycleWithData tests persistent lifecycle with actual data operations
func TestLocal_PersistentLifecycleWithData(t *testing.T) {
	tmpDir := filepath.Join("/tmp", fmt.Sprintf("objstore-lifecycle-with-data-%d", time.Now().UnixNano()))
	defer os.RemoveAll(tmpDir)

	// Create storage with persistent lifecycle
	st1, err := factory.NewStorage("local", map[string]string{
		"path":                 tmpDir,
		"lifecycleManagerType": "persistent",
	})
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}

	// Store some test data
	testData := []byte("test data for lifecycle")
	if err := st1.Put("data/test.txt", bytes.NewReader(testData)); err != nil {
		t.Fatalf("Failed to put data: %v", err)
	}

	// Add lifecycle policy
	policy := common.LifecyclePolicy{
		ID:        "data-policy",
		Prefix:    "data/",
		Retention: 1 * time.Hour,
		Action:    "delete",
	}
	if err := st1.AddPolicy(policy); err != nil {
		t.Fatalf("Failed to add policy: %v", err)
	}

	// Verify data exists
	rc, err := st1.Get("data/test.txt")
	if err != nil {
		t.Fatalf("Failed to get data: %v", err)
	}
	rc.Close()

	// Simulate restart
	st2, err := factory.NewStorage("local", map[string]string{
		"path":                 tmpDir,
		"lifecycleManagerType": "persistent",
	})
	if err != nil {
		t.Fatalf("Failed to create second storage: %v", err)
	}

	// Verify policy persisted
	policies, err := st2.GetPolicies()
	if err != nil {
		t.Fatalf("Failed to get policies: %v", err)
	}
	if len(policies) != 1 {
		t.Fatalf("Expected 1 policy after restart, got %d", len(policies))
	}

	// Verify data still exists
	rc2, err := st2.Get("data/test.txt")
	if err != nil {
		t.Fatalf("Failed to get data after restart: %v", err)
	}
	got, _ := io.ReadAll(rc2)
	rc2.Close()

	if !bytes.Equal(got, testData) {
		t.Fatal("Data mismatch after restart")
	}

	t.Log("Persistent lifecycle manager works correctly with data operations")
}

// TestLocal_MultiplePersistentInstances tests multiple storage instances with same policy file
func TestLocal_MultiplePersistentInstances(t *testing.T) {
	tmpDir := filepath.Join("/tmp", fmt.Sprintf("objstore-multi-instances-%d", time.Now().UnixNano()))
	defer os.RemoveAll(tmpDir)

	// Create first instance
	st1, err := factory.NewStorage("local", map[string]string{
		"path":                 tmpDir,
		"lifecycleManagerType": "persistent",
		"lifecyclePolicyFile":  "shared-policies.json",
	})
	if err != nil {
		t.Fatalf("Failed to create first instance: %v", err)
	}

	// Add policy from first instance
	policy1 := common.LifecyclePolicy{
		ID:        "policy-from-instance-1",
		Prefix:    "logs/",
		Retention: 24 * time.Hour,
		Action:    "delete",
	}
	if err := st1.AddPolicy(policy1); err != nil {
		t.Fatalf("Failed to add policy from instance 1: %v", err)
	}

	// Create second instance using same policy file
	st2, err := factory.NewStorage("local", map[string]string{
		"path":                 tmpDir,
		"lifecycleManagerType": "persistent",
		"lifecyclePolicyFile":  "shared-policies.json",
	})
	if err != nil {
		t.Fatalf("Failed to create second instance: %v", err)
	}

	// Verify second instance can see the policy
	policies, err := st2.GetPolicies()
	if err != nil {
		t.Fatalf("Failed to get policies from second instance: %v", err)
	}
	if len(policies) != 1 {
		t.Fatalf("Expected 1 policy in second instance, got %d", len(policies))
	}

	// Add policy from second instance
	policy2 := common.LifecyclePolicy{
		ID:        "policy-from-instance-2",
		Prefix:    "temp/",
		Retention: 48 * time.Hour,
		Action:    "delete",
	}
	if err := st2.AddPolicy(policy2); err != nil {
		t.Fatalf("Failed to add policy from instance 2: %v", err)
	}

	// Create third instance to verify both policies persist
	st3, err := factory.NewStorage("local", map[string]string{
		"path":                 tmpDir,
		"lifecycleManagerType": "persistent",
		"lifecyclePolicyFile":  "shared-policies.json",
	})
	if err != nil {
		t.Fatalf("Failed to create third instance: %v", err)
	}

	policies3, err := st3.GetPolicies()
	if err != nil {
		t.Fatalf("Failed to get policies from third instance: %v", err)
	}
	if len(policies3) != 2 {
		t.Fatalf("Expected 2 policies in third instance, got %d", len(policies3))
	}

	t.Log("Multiple instances can share the same persistent policy file")
}

// TestLocal_ComprehensiveSuite runs the complete test suite for all interface methods
func TestLocal_ComprehensiveSuite(t *testing.T) {
	tmpDir := filepath.Join("/tmp", fmt.Sprintf("objstore-comprehensive-%d", time.Now().UnixNano()))
	defer os.RemoveAll(tmpDir)

	st, err := factory.NewStorage("local", map[string]string{
		"path": tmpDir,
	})
	if err != nil {
		t.Fatal(err)
	}

	suite := &testcommon.ComprehensiveTestSuite{
		Storage: st,
		T:       t,
	}

	suite.RunAllTests()
}
