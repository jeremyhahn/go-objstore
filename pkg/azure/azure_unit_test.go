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

package azure

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/storage/armstorage"
	"github.com/Azure/azure-storage-blob-go/azblob"
)

type memBlob struct {
	data                          []byte
	upErr, rdErr, delErr, propErr error
}

func (m *memBlob) UploadFromReader(_ context.Context, r io.Reader) error {
	if m.upErr != nil {
		return m.upErr
	}
	b, _ := io.ReadAll(r)
	m.data = b
	return nil
}

func (m *memBlob) NewReader(_ context.Context) (io.ReadCloser, error) {
	if m.rdErr != nil {
		return nil, m.rdErr
	}
	return io.NopCloser(bytes.NewReader(m.data)), nil
}

func (m *memBlob) Delete(_ context.Context) error {
	if m.delErr != nil {
		return m.delErr
	}
	m.data = nil
	return nil
}

func (m *memBlob) GetProperties(_ context.Context) error {
	if m.propErr != nil {
		return m.propErr
	}
	if m.data == nil {
		return errors.New("blob not found")
	}
	return nil
}

type memContainer struct{ blobs map[string]*memBlob }

func (c memContainer) NewBlockBlob(name string) BlobAPI {
	if c.blobs[name] == nil {
		c.blobs[name] = &memBlob{}
	}
	return c.blobs[name]
}

func (c memContainer) ListBlobsFlat(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	for name := range c.blobs {
		if strings.HasPrefix(name, prefix) {
			keys = append(keys, name)
		}
	}
	return keys, nil
}

// mockManagementPoliciesClient mocks the ARM SDK ManagementPoliciesClient for testing
type mockManagementPoliciesClient struct {
	policy    *armstorage.ManagementPolicy
	getErr    error
	createErr error
	deleteErr error
}

func (m *mockManagementPoliciesClient) Get(ctx context.Context, resourceGroupName string, accountName string, managementPolicyName armstorage.ManagementPolicyName, options *armstorage.ManagementPoliciesClientGetOptions) (armstorage.ManagementPoliciesClientGetResponse, error) {
	if m.getErr != nil {
		return armstorage.ManagementPoliciesClientGetResponse{}, m.getErr
	}
	if m.policy == nil {
		return armstorage.ManagementPoliciesClientGetResponse{}, errors.New("policy not found")
	}
	return armstorage.ManagementPoliciesClientGetResponse{
		ManagementPolicy: *m.policy,
	}, nil
}

func (m *mockManagementPoliciesClient) CreateOrUpdate(ctx context.Context, resourceGroupName string, accountName string, managementPolicyName armstorage.ManagementPolicyName, properties armstorage.ManagementPolicy, options *armstorage.ManagementPoliciesClientCreateOrUpdateOptions) (armstorage.ManagementPoliciesClientCreateOrUpdateResponse, error) {
	if m.createErr != nil {
		return armstorage.ManagementPoliciesClientCreateOrUpdateResponse{}, m.createErr
	}
	m.policy = &properties
	return armstorage.ManagementPoliciesClientCreateOrUpdateResponse{
		ManagementPolicy: properties,
	}, nil
}

func (m *mockManagementPoliciesClient) Delete(ctx context.Context, resourceGroupName string, accountName string, managementPolicyName armstorage.ManagementPolicyName, options *armstorage.ManagementPoliciesClientDeleteOptions) (armstorage.ManagementPoliciesClientDeleteResponse, error) {
	if m.deleteErr != nil {
		return armstorage.ManagementPoliciesClientDeleteResponse{}, m.deleteErr
	}
	m.policy = nil
	return armstorage.ManagementPoliciesClientDeleteResponse{}, nil
}

func TestAzure_New(t *testing.T) {
	s := New()
	if s == nil {
		t.Fatal("New() returned nil")
	}
	if _, ok := s.(*Azure); !ok {
		t.Fatal("New() did not return *Azure type")
	}
}

func TestAzure_Configure_Success(t *testing.T) {
	tests := []struct {
		name     string
		settings map[string]string
	}{
		{
			name: "standard configuration",
			settings: map[string]string{
				"accountName":   "testaccount",
				"accountKey":    "MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MA==",
				"containerName": "testcontainer",
			},
		},
		{
			name: "configuration with custom endpoint",
			settings: map[string]string{
				"accountName":   "testaccount",
				"accountKey":    "MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MA==",
				"containerName": "testcontainer",
				"endpoint":      "http://127.0.0.1:10000/devstoreaccount1",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &Azure{}
			err := a.Configure(tt.settings)
			if err != nil {
				t.Fatalf("Configure() error = %v", err)
			}
			if a.container == nil {
				t.Fatal("container not set after Configure()")
			}
		})
	}
}

func TestAzure_Configure_Errors(t *testing.T) {
	tests := []struct {
		name        string
		settings    map[string]string
		expectedErr string
	}{
		{
			name:        "missing accountName",
			settings:    map[string]string{"accountKey": "key", "containerName": "container"},
			expectedErr: "accountName",
		},
		{
			name:        "missing accountKey",
			settings:    map[string]string{"accountName": "account", "containerName": "container"},
			expectedErr: "accountKey",
		},
		{
			name:        "missing containerName",
			settings:    map[string]string{"accountName": "account", "accountKey": "MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MA=="},
			expectedErr: "containerName",
		},
		{
			name:        "empty settings",
			settings:    map[string]string{},
			expectedErr: "accountName",
		},
		{
			name: "invalid accountKey",
			settings: map[string]string{
				"accountName":   "account",
				"accountKey":    "invalid-key",
				"containerName": "container",
			},
			expectedErr: "illegal base64",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &Azure{}
			err := a.Configure(tt.settings)
			if err == nil {
				t.Fatal("Configure() expected error, got nil")
			}
			if !strings.Contains(err.Error(), tt.expectedErr) {
				t.Fatalf("Configure() error = %v, want error containing %q", err, tt.expectedErr)
			}
		})
	}
}

func TestAzure_Configure_WithTestContainer(t *testing.T) {
	// Test the path where TestContainerURL is already set
	u, _ := url.Parse("http://127.0.0.1:10000/devstoreaccount1/testcontainer")
	testURL := azblob.NewContainerURL(*u, azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{}))

	a := &Azure{TestContainerURL: testURL}
	err := a.Configure(map[string]string{})
	if err != nil {
		t.Fatalf("Configure with TestContainerURL should not error, got: %v", err)
	}
	if a.container == nil {
		t.Error("container should be set when using TestContainerURL")
	}
}

func TestAzure_Configure_WithCustomEndpoint(t *testing.T) {
	// Test the path where a custom endpoint is provided
	settings := map[string]string{
		"accountName":   "testaccount",
		"accountKey":    "dGVzdGtleQ==",
		"containerName": "testcontainer",
		"endpoint":      "http://127.0.0.1:10000/devstoreaccount1",
	}

	a := &Azure{}
	err := a.Configure(settings)
	if err != nil {
		t.Fatalf("Configure with custom endpoint should not error, got: %v", err)
	}
	if a.container == nil {
		t.Error("container should be set")
	}
	if a.accountName != "testaccount" {
		t.Errorf("accountName not set correctly, got: %s", a.accountName)
	}
}

func TestAzure_Configure_SubscriptionIDAndResourceGroup(t *testing.T) {
	// Test that Configure handles subscriptionID and resourceGroup
	// Note: This will fail to create management client in test environment,
	// but the function should handle that gracefully and still succeed
	settings := map[string]string{
		"accountName":    "testaccount",
		"accountKey":     "dGVzdGtleQ==",
		"containerName":  "testcontainer",
		"subscriptionID": "test-sub-id",
		"resourceGroup":  "test-rg",
	}

	a := &Azure{}
	err := a.Configure(settings)
	// Should not error even if management client setup fails
	if err != nil {
		t.Fatalf("Configure should handle management client failure gracefully, got: %v", err)
	}
	if a.subscriptionID != "test-sub-id" {
		t.Errorf("subscriptionID not set, got: %s", a.subscriptionID)
	}
	if a.resourceGroup != "test-rg" {
		t.Errorf("resourceGroup not set, got: %s", a.resourceGroup)
	}
}

func TestAzure_Put_Success(t *testing.T) {
	c := memContainer{blobs: map[string]*memBlob{}}
	a := &Azure{container: c}

	tests := []struct {
		name string
		key  string
		data string
	}{
		{"simple put", "test.txt", "hello world"},
		{"empty data", "empty.txt", ""},
		{"large data", "large.txt", strings.Repeat("x", 10000)},
		{"key with path", "dir/subdir/file.txt", "nested file"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := a.Put(tt.key, bytes.NewBufferString(tt.data))
			if err != nil {
				t.Fatalf("Put() error = %v", err)
			}

			// Verify the data was stored
			blob := c.blobs[tt.key]
			if blob == nil {
				t.Fatal("blob not created")
			}
			if string(blob.data) != tt.data {
				t.Fatalf("Put() stored data = %q, want %q", string(blob.data), tt.data)
			}
		})
	}
}

func TestAzure_Put_Errors(t *testing.T) {
	tests := []struct {
		name        string
		setup       func() *Azure
		key         string
		expectedErr string
	}{
		{
			name: "not configured",
			setup: func() *Azure {
				return &Azure{}
			},
			key:         "test.txt",
			expectedErr: "not configured",
		},
		{
			name: "upload error",
			setup: func() *Azure {
				c := memContainer{blobs: map[string]*memBlob{
					"error.txt": {upErr: errors.New("upload failed")},
				}}
				return &Azure{container: c}
			},
			key:         "error.txt",
			expectedErr: "upload failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := tt.setup()
			err := a.Put(tt.key, bytes.NewBufferString("data"))
			if err == nil {
				t.Fatal("Put() expected error, got nil")
			}
			if !strings.Contains(err.Error(), tt.expectedErr) {
				t.Fatalf("Put() error = %v, want error containing %q", err, tt.expectedErr)
			}
		})
	}
}

func TestAzure_Get_Success(t *testing.T) {
	c := memContainer{blobs: map[string]*memBlob{
		"test.txt": {data: []byte("hello world")},
	}}
	a := &Azure{container: c}

	rc, err := a.Get("test.txt")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}

	if string(data) != "hello world" {
		t.Fatalf("Get() returned %q, want %q", string(data), "hello world")
	}
}

func TestAzure_Get_Errors(t *testing.T) {
	tests := []struct {
		name        string
		setup       func() *Azure
		key         string
		expectedErr string
	}{
		{
			name: "not configured",
			setup: func() *Azure {
				return &Azure{}
			},
			key:         "test.txt",
			expectedErr: "not configured",
		},
		{
			name: "read error",
			setup: func() *Azure {
				c := memContainer{blobs: map[string]*memBlob{
					"error.txt": {rdErr: errors.New("read failed")},
				}}
				return &Azure{container: c}
			},
			key:         "error.txt",
			expectedErr: "read failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := tt.setup()
			_, err := a.Get(tt.key)
			if err == nil {
				t.Fatal("Get() expected error, got nil")
			}
			if !strings.Contains(err.Error(), tt.expectedErr) {
				t.Fatalf("Get() error = %v, want error containing %q", err, tt.expectedErr)
			}
		})
	}
}

func TestAzure_Delete_Success(t *testing.T) {
	c := memContainer{blobs: map[string]*memBlob{
		"test.txt": {data: []byte("hello world")},
	}}
	a := &Azure{container: c}

	err := a.Delete("test.txt")
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	// Verify blob data was cleared
	if c.blobs["test.txt"].data != nil {
		t.Fatal("Delete() did not clear blob data")
	}
}

func TestAzure_Delete_Errors(t *testing.T) {
	tests := []struct {
		name        string
		setup       func() *Azure
		key         string
		expectedErr string
	}{
		{
			name: "not configured",
			setup: func() *Azure {
				return &Azure{}
			},
			key:         "test.txt",
			expectedErr: "not configured",
		},
		{
			name: "delete error",
			setup: func() *Azure {
				c := memContainer{blobs: map[string]*memBlob{
					"error.txt": {delErr: errors.New("delete failed")},
				}}
				return &Azure{container: c}
			},
			key:         "error.txt",
			expectedErr: "delete failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := tt.setup()
			err := a.Delete(tt.key)
			if err == nil {
				t.Fatal("Delete() expected error, got nil")
			}
			if !strings.Contains(err.Error(), tt.expectedErr) {
				t.Fatalf("Delete() error = %v, want error containing %q", err, tt.expectedErr)
			}
		})
	}
}

func TestAzure_Archive_Success(t *testing.T) {
	c := memContainer{blobs: map[string]*memBlob{
		"test.txt": {data: []byte("archive me")},
	}}
	a := &Azure{container: c}

	var archived []byte
	archiver := archiverFunc(func(k string, r io.Reader) error {
		archived, _ = io.ReadAll(r)
		return nil
	})

	err := a.Archive("test.txt", archiver)
	if err != nil {
		t.Fatalf("Archive() error = %v", err)
	}

	if string(archived) != "archive me" {
		t.Fatalf("Archive() archived data = %q, want %q", string(archived), "archive me")
	}
}

func TestAzure_Archive_Errors(t *testing.T) {
	tests := []struct {
		name        string
		setup       func() *Azure
		key         string
		archiver    common.Archiver
		expectedErr string
	}{
		{
			name: "get error",
			setup: func() *Azure {
				c := memContainer{blobs: map[string]*memBlob{
					"error.txt": {rdErr: errors.New("get failed")},
				}}
				return &Azure{container: c}
			},
			key:         "error.txt",
			archiver:    archiverFunc(func(k string, r io.Reader) error { return nil }),
			expectedErr: "get failed",
		},
		{
			name: "archiver put error",
			setup: func() *Azure {
				c := memContainer{blobs: map[string]*memBlob{
					"test.txt": {data: []byte("data")},
				}}
				return &Azure{container: c}
			},
			key:         "test.txt",
			archiver:    archiverFunc(func(k string, r io.Reader) error { return errors.New("put failed") }),
			expectedErr: "put failed",
		},
		{
			name: "not configured",
			setup: func() *Azure {
				return &Azure{}
			},
			key:         "test.txt",
			archiver:    archiverFunc(func(k string, r io.Reader) error { return nil }),
			expectedErr: "not configured",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := tt.setup()
			err := a.Archive(tt.key, tt.archiver)
			if err == nil {
				t.Fatal("Archive() expected error, got nil")
			}
			if !strings.Contains(err.Error(), tt.expectedErr) {
				t.Fatalf("Archive() error = %v, want error containing %q", err, tt.expectedErr)
			}
		})
	}
}

func TestAzure_AddPolicy_Success(t *testing.T) {
	mockMgmt := &mockManagementPoliciesClient{}
	a := &Azure{
		mgmtClient:    mockMgmt,
		subscriptionID: "test-sub",
		resourceGroup:  "test-rg",
		accountName:    "testaccount",
		containerName:  "testcontainer",
	}

	policy := common.LifecyclePolicy{
		ID:        "policy1",
		Prefix:    "logs/",
		Retention: 24 * 7 * time.Hour,
		Action:    "delete",
	}
	err := a.AddPolicy(policy)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Verify the management policy was created correctly
	if mockMgmt.policy == nil {
		t.Fatal("expected management policy to be set")
	}
	if mockMgmt.policy.Properties == nil || mockMgmt.policy.Properties.Policy == nil {
		t.Fatal("expected policy properties to be set")
	}
	if mockMgmt.policy.Properties.Policy.Rules == nil || len(mockMgmt.policy.Properties.Policy.Rules) != 1 {
		t.Fatalf("expected 1 policy rule, got %v", mockMgmt.policy.Properties.Policy.Rules)
	}

	rule := mockMgmt.policy.Properties.Policy.Rules[0]

	// Verify rule name (ID)
	if rule.Name == nil || *rule.Name != "policy1" {
		t.Fatalf("expected rule name 'policy1', got %v", rule.Name)
	}

	// Verify rule is enabled
	if rule.Enabled == nil || !*rule.Enabled {
		t.Fatalf("expected rule to be enabled, got %v", rule.Enabled)
	}

	// Verify rule type
	if rule.Type == nil || *rule.Type != "Lifecycle" {
		t.Fatalf("expected rule type 'Lifecycle', got %v", rule.Type)
	}

	// Verify definition exists
	if rule.Definition == nil {
		t.Fatal("expected rule definition to be set")
	}

	// Verify filters
	if rule.Definition.Filters == nil {
		t.Fatal("expected rule filters to be set")
	}
	if rule.Definition.Filters.BlobTypes == nil || len(rule.Definition.Filters.BlobTypes) != 1 {
		t.Fatal("expected 1 blob type in filters")
	}
	if *rule.Definition.Filters.BlobTypes[0] != "blockBlob" {
		t.Fatalf("expected blob type 'blockBlob', got %v", *rule.Definition.Filters.BlobTypes[0])
	}
	if rule.Definition.Filters.PrefixMatch == nil || len(rule.Definition.Filters.PrefixMatch) != 1 {
		t.Fatal("expected 1 prefix match in filters")
	}
	expectedPrefix := "testcontainer/logs/"
	if *rule.Definition.Filters.PrefixMatch[0] != expectedPrefix {
		t.Fatalf("expected prefix '%s', got %v", expectedPrefix, *rule.Definition.Filters.PrefixMatch[0])
	}

	// Verify actions for delete
	if rule.Definition.Actions == nil || rule.Definition.Actions.BaseBlob == nil {
		t.Fatal("expected base blob actions to be set")
	}
	if rule.Definition.Actions.BaseBlob.Delete == nil {
		t.Fatal("expected Delete action to be set for delete policy")
	}
	if rule.Definition.Actions.BaseBlob.Delete.DaysAfterModificationGreaterThan == nil {
		t.Fatal("expected DaysAfterModificationGreaterThan to be set")
	}
	expectedDays := float32(7)
	if *rule.Definition.Actions.BaseBlob.Delete.DaysAfterModificationGreaterThan != expectedDays {
		t.Fatalf("expected days %f, got %v", expectedDays, *rule.Definition.Actions.BaseBlob.Delete.DaysAfterModificationGreaterThan)
	}

	// Verify no archive action is set for delete policy
	if rule.Definition.Actions.BaseBlob.TierToArchive != nil {
		t.Fatal("expected no TierToArchive action for delete policy")
	}

	policies, err := a.GetPolicies()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(policies) != 1 {
		t.Fatalf("expected 1 policy, got %d", len(policies))
	}
	if policies[0].ID != "policy1" {
		t.Fatalf("expected policy1, got %s", policies[0].ID)
	}
}

func TestAzure_AddPolicy_Archive(t *testing.T) {
	mockMgmt := &mockManagementPoliciesClient{}
	a := &Azure{
		mgmtClient:    mockMgmt,
		subscriptionID: "test-sub",
		resourceGroup:  "test-rg",
		accountName:    "testaccount",
		containerName:  "testcontainer",
	}

	policy := common.LifecyclePolicy{
		ID:        "archive-policy",
		Prefix:    "old-data/",
		Retention: 30 * 24 * time.Hour,
		Action:    "archive",
	}
	err := a.AddPolicy(policy)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Verify the management policy was created correctly
	if mockMgmt.policy == nil {
		t.Fatal("expected management policy to be set")
	}
	if mockMgmt.policy.Properties == nil || mockMgmt.policy.Properties.Policy == nil {
		t.Fatal("expected policy properties to be set")
	}
	if mockMgmt.policy.Properties.Policy.Rules == nil || len(mockMgmt.policy.Properties.Policy.Rules) != 1 {
		t.Fatalf("expected 1 policy rule, got %v", mockMgmt.policy.Properties.Policy.Rules)
	}

	rule := mockMgmt.policy.Properties.Policy.Rules[0]

	// Verify rule name (ID)
	if rule.Name == nil || *rule.Name != "archive-policy" {
		t.Fatalf("expected rule name 'archive-policy', got %v", rule.Name)
	}

	// Verify rule is enabled
	if rule.Enabled == nil || !*rule.Enabled {
		t.Fatalf("expected rule to be enabled, got %v", rule.Enabled)
	}

	// Verify prefix filter
	if rule.Definition == nil || rule.Definition.Filters == nil {
		t.Fatal("expected rule definition and filters to be set")
	}
	expectedPrefix := "testcontainer/old-data/"
	if rule.Definition.Filters.PrefixMatch == nil || len(rule.Definition.Filters.PrefixMatch) != 1 {
		t.Fatal("expected 1 prefix match in filters")
	}
	if *rule.Definition.Filters.PrefixMatch[0] != expectedPrefix {
		t.Fatalf("expected prefix '%s', got %v", expectedPrefix, *rule.Definition.Filters.PrefixMatch[0])
	}

	// Verify actions for archive
	if rule.Definition.Actions == nil || rule.Definition.Actions.BaseBlob == nil {
		t.Fatal("expected base blob actions to be set")
	}
	if rule.Definition.Actions.BaseBlob.TierToArchive == nil {
		t.Fatal("expected TierToArchive action to be set for archive policy")
	}
	if rule.Definition.Actions.BaseBlob.TierToArchive.DaysAfterModificationGreaterThan == nil {
		t.Fatal("expected DaysAfterModificationGreaterThan to be set")
	}
	expectedDays := float32(30)
	if *rule.Definition.Actions.BaseBlob.TierToArchive.DaysAfterModificationGreaterThan != expectedDays {
		t.Fatalf("expected days %f, got %v", expectedDays, *rule.Definition.Actions.BaseBlob.TierToArchive.DaysAfterModificationGreaterThan)
	}

	// Verify no delete action is set for archive policy
	if rule.Definition.Actions.BaseBlob.Delete != nil {
		t.Fatal("expected no Delete action for archive policy")
	}

	policies, err := a.GetPolicies()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(policies) != 1 {
		t.Fatalf("expected 1 policy, got %d", len(policies))
	}
	if policies[0].ID != "archive-policy" {
		t.Fatalf("expected archive-policy, got %s", policies[0].ID)
	}
	if policies[0].Action != "archive" {
		t.Fatalf("expected action 'archive', got %s", policies[0].Action)
	}
}

func TestAzure_AddPolicy_InvalidID(t *testing.T) {
	mockMgmt := &mockManagementPoliciesClient{}
	a := &Azure{
		mgmtClient:    mockMgmt,
		subscriptionID: "test-sub",
		resourceGroup:  "test-rg",
		accountName:    "testaccount",
		containerName:  "testcontainer",
	}

	policy := common.LifecyclePolicy{
		ID:        "",
		Prefix:    "logs/",
		Retention: 24 * time.Hour,
		Action:    "delete",
	}
	err := a.AddPolicy(policy)
	if err != common.ErrInvalidPolicy {
		t.Fatalf("expected ErrInvalidPolicy, got %v", err)
	}
}

func TestAzure_AddPolicy_InvalidAction(t *testing.T) {
	mockMgmt := &mockManagementPoliciesClient{}
	a := &Azure{
		mgmtClient:    mockMgmt,
		subscriptionID: "test-sub",
		resourceGroup:  "test-rg",
		accountName:    "testaccount",
		containerName:  "testcontainer",
	}

	policy := common.LifecyclePolicy{
		ID:        "policy1",
		Prefix:    "logs/",
		Retention: 24 * time.Hour,
		Action:    "invalid",
	}
	err := a.AddPolicy(policy)
	if err != common.ErrInvalidPolicy {
		t.Fatalf("expected ErrInvalidPolicy, got %v", err)
	}
}

func TestAzure_AddPolicy_NoManagementClient(t *testing.T) {
	// Test that lifecycle operations fail gracefully without management client
	a := New().(*Azure)
	policy := common.LifecyclePolicy{
		ID:        "policy1",
		Prefix:    "logs/",
		Retention: 24 * time.Hour,
		Action:    "delete",
	}
	err := a.AddPolicy(policy)
	if err == nil {
		t.Fatal("expected error when management client not available, got nil")
	}
}

func TestAzure_RemovePolicy(t *testing.T) {
	mockMgmt := &mockManagementPoliciesClient{}
	a := &Azure{
		mgmtClient:    mockMgmt,
		subscriptionID: "test-sub",
		resourceGroup:  "test-rg",
		accountName:    "testaccount",
		containerName:  "testcontainer",
	}

	policy := common.LifecyclePolicy{
		ID:        "policy1",
		Prefix:    "logs/",
		Retention: 24 * time.Hour,
		Action:    "delete",
	}
	err := a.AddPolicy(policy)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	err = a.RemovePolicy("policy1")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	policies, _ := a.GetPolicies()
	if len(policies) != 0 {
		t.Fatalf("expected 0 policies, got %d", len(policies))
	}
}

func TestAzure_RemovePolicy_NoManagementClient(t *testing.T) {
	a := New().(*Azure)
	err := a.RemovePolicy("policy1")
	if err == nil {
		t.Fatal("expected error when management client not available, got nil")
	}
}

func TestAzure_RemovePolicy_NonExistent(t *testing.T) {
	mockMgmt := &mockManagementPoliciesClient{}
	a := &Azure{
		mgmtClient:    mockMgmt,
		subscriptionID: "test-sub",
		resourceGroup:  "test-rg",
		accountName:    "testaccount",
		containerName:  "testcontainer",
	}

	err := a.RemovePolicy("nonexistent")
	if err != nil {
		t.Fatalf("expected no error for removing non-existent policy, got %v", err)
	}
}

func TestAzure_RemovePolicy_MultipleRules(t *testing.T) {
	mockMgmt := &mockManagementPoliciesClient{}
	a := &Azure{
		mgmtClient:    mockMgmt,
		subscriptionID: "test-sub",
		resourceGroup:  "test-rg",
		accountName:    "testaccount",
		containerName:  "testcontainer",
	}

	// Add multiple policies
	policy1 := common.LifecyclePolicy{
		ID:        "policy1",
		Prefix:    "logs/",
		Retention: 24 * time.Hour,
		Action:    "delete",
	}
	policy2 := common.LifecyclePolicy{
		ID:        "policy2",
		Prefix:    "temp/",
		Retention: 48 * time.Hour,
		Action:    "delete",
	}

	if err := a.AddPolicy(policy1); err != nil {
		t.Fatalf("AddPolicy failed: %v", err)
	}
	if err := a.AddPolicy(policy2); err != nil {
		t.Fatalf("AddPolicy failed: %v", err)
	}

	// Remove one policy - should leave the other
	err := a.RemovePolicy("policy1")
	if err != nil {
		t.Fatalf("RemovePolicy failed: %v", err)
	}

	// Verify one policy remains
	policies, _ := a.GetPolicies()
	if len(policies) != 1 {
		t.Fatalf("expected 1 policy remaining, got %d", len(policies))
	}
	if policies[0].ID != "policy2" {
		t.Errorf("expected remaining policy to be policy2, got %s", policies[0].ID)
	}
}

func TestAzure_GetPolicies_NoManagementClient(t *testing.T) {
	a := New().(*Azure)
	policies, err := a.GetPolicies()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(policies) != 0 {
		t.Fatalf("expected 0 policies when no management client, got %d", len(policies))
	}
}

func TestAzure_GetPolicies_Empty(t *testing.T) {
	mockMgmt := &mockManagementPoliciesClient{}
	a := &Azure{
		mgmtClient:    mockMgmt,
		subscriptionID: "test-sub",
		resourceGroup:  "test-rg",
		accountName:    "testaccount",
		containerName:  "testcontainer",
	}

	policies, err := a.GetPolicies()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(policies) != 0 {
		t.Fatalf("expected 0 policies, got %d", len(policies))
	}
}

func TestAzure_GetPolicies_Multiple(t *testing.T) {
	mockMgmt := &mockManagementPoliciesClient{}
	a := &Azure{
		mgmtClient:    mockMgmt,
		subscriptionID: "test-sub",
		resourceGroup:  "test-rg",
		accountName:    "testaccount",
		containerName:  "testcontainer",
	}
	policy1 := common.LifecyclePolicy{
		ID:        "policy1",
		Prefix:    "logs/",
		Retention: 24 * time.Hour,
		Action:    "delete",
	}
	policy2 := common.LifecyclePolicy{
		ID:        "policy2",
		Prefix:    "archive/",
		Retention: 48 * time.Hour,
		Action:    "archive",
	}

	a.AddPolicy(policy1)
	a.AddPolicy(policy2)

	policies, err := a.GetPolicies()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(policies) != 2 {
		t.Fatalf("expected 2 policies, got %d", len(policies))
	}
}

type archiverFunc func(string, io.Reader) error

func (f archiverFunc) Put(k string, r io.Reader) error { return f(k, r) }

func TestAzure_List_Success(t *testing.T) {
	c := memContainer{blobs: map[string]*memBlob{
		"file1.txt":        {},
		"file2.txt":        {},
		"prefix/file3.txt": {},
	}}
	a := &Azure{container: c}

	keys, err := a.List("")
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}

	if len(keys) != 3 {
		t.Fatalf("List() returned %d keys, want 3", len(keys))
	}
}

func TestAzure_List_WithPrefix(t *testing.T) {
	c := memContainer{blobs: map[string]*memBlob{
		"file1.txt":        {},
		"file2.txt":        {},
		"prefix/file3.txt": {},
	}}
	a := &Azure{container: c}

	keys, err := a.List("prefix/")
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}

	if len(keys) != 1 {
		t.Fatalf("List() returned %d keys, want 1", len(keys))
	}
}

func TestAzure_List_NotConfigured(t *testing.T) {
	a := &Azure{}

	_, err := a.List("")
	if err == nil {
		t.Fatal("List() expected error, got nil")
	}
	if !strings.Contains(err.Error(), "not configured") {
		t.Fatalf("List() error = %v, want error containing 'not configured'", err)
	}
}
