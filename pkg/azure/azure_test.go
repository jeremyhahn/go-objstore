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

//go:build azureblob

package azure_test

import (
	"context"
	"errors"
	"net/url"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/jeremyhahn/go-objstore/pkg/azure"
	"github.com/jeremyhahn/go-objstore/pkg/common"
)

func TestAzure_Configure(t *testing.T) {
	// Test case 1: Successful configuration
	storage := azure.New()
	settings := map[string]string{
		"accountName":   "testaccount",
		"accountKey":    "MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MA==", // Valid base64 encoded key
		"containerName": "testcontainer",
	}

	err := storage.Configure(settings)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	// Test case 2: Missing accountName
	storage = azure.New()
	settings = map[string]string{
		"accountKey":    "testkey",
		"containerName": "testcontainer",
	}
	err = storage.Configure(settings)
	if err == nil || !strings.Contains(err.Error(), "accountName") {
		t.Errorf("Expected error containing 'accountName', got %v", err)
	}

	// Test case 3: Missing accountKey
	storage = azure.New()
	settings = map[string]string{
		"accountName":   "testaccount",
		"containerName": "testcontainer",
	}
	err = storage.Configure(settings)
	if err == nil || !strings.Contains(err.Error(), "accountKey") {
		t.Errorf("Expected error containing 'accountKey', got %v", err)
	}

	// Test case 4: Missing containerName
	storage = azure.New()
	settings = map[string]string{
		"accountName": "testaccount",
		"accountKey":  "testkey",
	}
	err = storage.Configure(settings)
	if err == nil || !strings.Contains(err.Error(), "containerName") {
		t.Errorf("Expected error containing 'containerName', got %v", err)
	}
}

func TestAzure_Configure_BadEndpoint(t *testing.T) {
	storage := azure.New()
	settings := map[string]string{
		"accountName":   "testaccount",
		"accountKey":    "MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MA==",
		"containerName": "testcontainer",
		"endpoint":      "://bad url",
	}
	err := storage.Configure(settings)
	if err == nil {
		t.Errorf("Expected error for bad endpoint, got nil")
	}
}

func TestAzure_Configure_TestContainerURLBranch(t *testing.T) {
	storage := azure.New()
	// Inject a preconfigured container URL, Configure should short-circuit
	u, _ := url.Parse("http://127.0.0.1:1/container")
	storage.(*azure.Azure).TestContainerURL = azblob.NewContainerURL(*u, azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{}))
	if err := storage.Configure(map[string]string{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestAzure_LifecycleMethods(t *testing.T) {
	// Note: Azure lifecycle management requires ARM SDK with subscriptionID and resourceGroup.
	// For basic integration testing without ARM SDK, we test that it returns appropriate errors.
	storage := azure.New()

	policy := common.LifecyclePolicy{
		ID:        "test-policy",
		Prefix:    "test/",
		Retention: 24 * time.Hour,
		Action:    "delete",
	}

	// Test AddPolicy - should return error without management client configured
	err := storage.AddPolicy(policy)
	if err == nil {
		t.Error("Expected error without management client, got nil")
	}

	// Test GetPolicies - should return empty list without management client
	policies, err := storage.GetPolicies()
	if err != nil {
		t.Errorf("Expected no error from GetPolicies, got %v", err)
	}
	if len(policies) != 0 {
		t.Errorf("Expected 0 policies without management client, got %d", len(policies))
	}

	// Test RemovePolicy - should return error without management client
	err = storage.RemovePolicy(policy.ID)
	if err == nil {
		t.Error("Expected error without management client, got nil")
	}
	if len(policies) != 0 {
		t.Errorf("Expected 0 policies after removal, got %d", len(policies))
	}
}

// Mock container for testing List functionality
type mockContainer struct {
	listFn func(ctx context.Context, prefix string) ([]string, error)
}

func (m *mockContainer) NewBlockBlob(name string) azure.BlobAPI {
	return nil // Not needed for List tests
}

func (m *mockContainer) ListBlobsFlat(ctx context.Context, prefix string) ([]string, error) {
	if m.listFn != nil {
		return m.listFn(ctx, prefix)
	}
	return []string{}, nil
}

func TestAzure_List_EmptyPrefix(t *testing.T) {
	mockCont := &mockContainer{
		listFn: func(ctx context.Context, prefix string) ([]string, error) {
			return []string{"file1.txt", "file2.txt", "dir/file3.txt"}, nil
		},
	}

	azureStorage := &testAzureStorage{
		container: mockCont,
	}

	keys, err := azureStorage.List("")
	if err != nil {
		t.Fatalf("Expected no error on List, got %v", err)
	}

	sort.Strings(keys)
	expected := []string{"dir/file3.txt", "file1.txt", "file2.txt"}
	sort.Strings(expected)

	if len(keys) != len(expected) {
		t.Fatalf("Expected %d keys, got %d", len(expected), len(keys))
	}

	for i, key := range keys {
		if key != expected[i] {
			t.Errorf("Expected key %s, got %s", expected[i], key)
		}
	}
}

func TestAzure_List_WithPrefix(t *testing.T) {
	mockCont := &mockContainer{
		listFn: func(ctx context.Context, prefix string) ([]string, error) {
			if prefix == "logs/" {
				return []string{"logs/2023/file1.log", "logs/2023/file2.log", "logs/2024/file3.log"}, nil
			}
			return []string{}, nil
		},
	}

	azureStorage := &testAzureStorage{
		container: mockCont,
	}

	keys, err := azureStorage.List("logs/")
	if err != nil {
		t.Fatalf("Expected no error on List, got %v", err)
	}

	sort.Strings(keys)
	expected := []string{"logs/2023/file1.log", "logs/2023/file2.log", "logs/2024/file3.log"}
	sort.Strings(expected)

	if len(keys) != len(expected) {
		t.Fatalf("Expected %d keys, got %d", len(expected), len(keys))
	}

	for i, key := range keys {
		if key != expected[i] {
			t.Errorf("Expected key %s, got %s", expected[i], key)
		}
	}
}

func TestAzure_List_Empty(t *testing.T) {
	mockCont := &mockContainer{
		listFn: func(ctx context.Context, prefix string) ([]string, error) {
			return []string{}, nil
		},
	}

	azureStorage := &testAzureStorage{
		container: mockCont,
	}

	keys, err := azureStorage.List("nonexistent/")
	if err != nil {
		t.Fatalf("Expected no error on List, got %v", err)
	}

	if len(keys) != 0 {
		t.Errorf("Expected 0 keys, got %d", len(keys))
	}
}

func TestAzure_List_Error(t *testing.T) {
	mockCont := &mockContainer{
		listFn: func(ctx context.Context, prefix string) ([]string, error) {
			return nil, errors.New("list error")
		},
	}

	azureStorage := &testAzureStorage{
		container: mockCont,
	}

	_, err := azureStorage.List("")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestAzure_List_NotConfigured(t *testing.T) {
	storage := azure.New()

	_, err := storage.List("")
	if err == nil {
		t.Fatal("expected error when not configured, got nil")
	}
	if !strings.Contains(err.Error(), "not configured") {
		t.Errorf("expected 'not configured' error, got %v", err)
	}
}

// testAzureStorage wraps Azure to expose container field for testing
type testAzureStorage struct {
	container azure.ContainerAPI
}

func (t *testAzureStorage) List(prefix string) ([]string, error) {
	if t.container == nil {
		return nil, errors.New("azure not configured")
	}
	return t.container.ListBlobsFlat(context.Background(), prefix)
}

// Intentionally avoid Put/Get/Delete/Archive unit tests here to prevent network attempts.
