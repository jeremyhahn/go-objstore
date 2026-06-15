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
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/storage/armstorage"
	"github.com/Azure/azure-storage-blob-go/azblob"
)

// ---------------------------------------------------------------------------
// ReplicationManager stubs
// ---------------------------------------------------------------------------

// stubAzureReplicationManager satisfies common.ReplicationManager for testing.
type stubAzureReplicationManager struct{}

func (s *stubAzureReplicationManager) AddPolicy(_ common.ReplicationPolicy) error { return nil }
func (s *stubAzureReplicationManager) RemovePolicy(_ string) error                { return nil }
func (s *stubAzureReplicationManager) GetPolicy(_ string) (*common.ReplicationPolicy, error) {
	return nil, nil
}
func (s *stubAzureReplicationManager) GetPolicies() ([]common.ReplicationPolicy, error) {
	return nil, nil
}
func (s *stubAzureReplicationManager) SyncAll(_ context.Context) (*common.SyncResult, error) {
	return nil, nil
}
func (s *stubAzureReplicationManager) SyncPolicy(_ context.Context, _ string) (*common.SyncResult, error) {
	return nil, nil
}
func (s *stubAzureReplicationManager) SyncAllParallel(_ context.Context, _ int) (*common.SyncResult, error) {
	return nil, nil
}
func (s *stubAzureReplicationManager) SyncPolicyParallel(_ context.Context, _ string, _ int) (*common.SyncResult, error) {
	return nil, nil
}
func (s *stubAzureReplicationManager) SetBackendEncrypterFactory(_ string, _ common.EncrypterFactory) error {
	return nil
}
func (s *stubAzureReplicationManager) SetSourceEncrypterFactory(_ string, _ common.EncrypterFactory) error {
	return nil
}
func (s *stubAzureReplicationManager) SetDestinationEncrypterFactory(_ string, _ common.EncrypterFactory) error {
	return nil
}
func (s *stubAzureReplicationManager) Run(_ context.Context) {}

// ---------------------------------------------------------------------------
// GetReplicationManager / SetReplicationManager
// ---------------------------------------------------------------------------

// TestAzure_GetReplicationManager_Nil covers the nil replicationManager branch.
func TestAzure_GetReplicationManager_Nil(t *testing.T) {
	a := &Azure{}
	_, err := a.GetReplicationManager()
	if !errors.Is(err, common.ErrReplicationNotSupported) {
		t.Fatalf("expected ErrReplicationNotSupported, got %v", err)
	}
}

// TestAzure_GetReplicationManager_Set covers GetReplicationManager when set.
func TestAzure_GetReplicationManager_Set(t *testing.T) {
	rm := &stubAzureReplicationManager{}
	a := &Azure{replicationManager: rm}
	got, err := a.GetReplicationManager()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if got != rm {
		t.Fatal("returned manager does not match set manager")
	}
}

// TestAzure_SetReplicationManager covers SetReplicationManager.
func TestAzure_SetReplicationManager(t *testing.T) {
	a := &Azure{}
	rm := &stubAzureReplicationManager{}
	a.SetReplicationManager(rm)
	if a.replicationManager != rm {
		t.Fatal("SetReplicationManager did not store the manager")
	}
}

// ---------------------------------------------------------------------------
// ValidateKey paths (Put/Get/Delete/PutWithMetadata/GetWithContext/DeleteWithContext/Exists)
// ---------------------------------------------------------------------------

// TestAzure_Put_ValidateKeyError covers the ValidateKey guard in Put.
func TestAzure_Put_ValidateKeyError(t *testing.T) {
	c := memContainer{blobs: map[string]*memBlob{}}
	a := &Azure{container: c}
	if err := a.Put("", bytes.NewBufferString("d")); err == nil {
		t.Fatal("expected error for empty key")
	}
}

// TestAzure_Get_ValidateKeyError covers the ValidateKey guard in Get.
func TestAzure_Get_ValidateKeyError(t *testing.T) {
	c := memContainer{blobs: map[string]*memBlob{}}
	a := &Azure{container: c}
	if _, err := a.Get(""); err == nil {
		t.Fatal("expected error for empty key")
	}
}

// TestAzure_Delete_ValidateKeyError covers the ValidateKey guard in Delete.
func TestAzure_Delete_ValidateKeyError(t *testing.T) {
	c := memContainer{blobs: map[string]*memBlob{}}
	a := &Azure{container: c}
	if err := a.Delete(""); err == nil {
		t.Fatal("expected error for empty key")
	}
}

// TestAzure_PutWithMetadata_ValidateKeyError covers the ValidateKey guard in PutWithMetadata.
func TestAzure_PutWithMetadata_ValidateKeyError(t *testing.T) {
	c := memContainer{blobs: map[string]*memBlob{}}
	a := &Azure{container: c}
	if err := a.PutWithMetadata(context.Background(), "", bytes.NewBufferString("d"), nil); err == nil {
		t.Fatal("expected error for empty key")
	}
}

// TestAzure_GetWithContext_ValidateKeyError covers the ValidateKey guard in GetWithContext.
func TestAzure_GetWithContext_ValidateKeyError(t *testing.T) {
	c := memContainer{blobs: map[string]*memBlob{}}
	a := &Azure{container: c}
	if _, err := a.GetWithContext(context.Background(), ""); err == nil {
		t.Fatal("expected error for empty key")
	}
}

// TestAzure_DeleteWithContext_ValidateKeyError covers the ValidateKey guard in DeleteWithContext.
func TestAzure_DeleteWithContext_ValidateKeyError(t *testing.T) {
	c := memContainer{blobs: map[string]*memBlob{}}
	a := &Azure{container: c}
	if err := a.DeleteWithContext(context.Background(), ""); err == nil {
		t.Fatal("expected error for empty key")
	}
}

// TestAzure_Exists_ValidateKeyError covers the ValidateKey guard in Exists.
func TestAzure_Exists_ValidateKeyError(t *testing.T) {
	c := memContainer{blobs: map[string]*memBlob{}}
	a := &Azure{container: c}
	if _, err := a.Exists(context.Background(), ""); err == nil {
		t.Fatal("expected error for empty key")
	}
}

// TestAzure_UpdateMetadata_ValidateKeyError covers the ValidateKey guard in UpdateMetadata.
func TestAzure_UpdateMetadata_ValidateKeyError(t *testing.T) {
	c := memContainer{blobs: map[string]*memBlob{}}
	a := &Azure{container: c}
	if err := a.UpdateMetadata(context.Background(), "", nil); err == nil {
		t.Fatal("expected error for empty key")
	}
}

// TestAzure_GetMetadata_ValidateKeyError covers the ValidateKey guard in GetMetadata.
func TestAzure_GetMetadata_ValidateKeyError(t *testing.T) {
	c := memContainer{blobs: map[string]*memBlob{}}
	a := &Azure{container: c}
	if _, err := a.GetMetadata(context.Background(), ""); err == nil {
		t.Fatal("expected error for empty key")
	}
}

// TestAzure_GetMetadata_NoCustomMetadata covers the path where GetProperties
// returns BlobProperties with an empty Metadata map (no custom metadata copy).
func TestAzure_GetMetadata_NoCustomMetadata(t *testing.T) {
	mockCont := &mockContainerEnhanced{
		newBlockBlobFn: func(name string) BlobAPI {
			return &mockBlob{
				getPropertiesFn: func(ctx context.Context) (*BlobProperties, error) {
					return &BlobProperties{
						Size:        512,
						ContentType: "text/plain",
						// Metadata is nil / empty - no custom metadata
					}, nil
				},
			}
		},
	}
	a := &Azure{container: mockCont}
	meta, err := a.GetMetadata(context.Background(), "key")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if meta == nil {
		t.Fatal("expected non-nil metadata")
	}
	if meta.Custom != nil {
		t.Fatalf("expected nil custom metadata, got %v", meta.Custom)
	}
}

// ---------------------------------------------------------------------------
// AddPolicy edge cases
// ---------------------------------------------------------------------------

// TestAzure_AddPolicy_ShortRetention covers the "minimum 1 day" branch.
func TestAzure_AddPolicy_ShortRetention(t *testing.T) {
	mockMgmt := &mockManagementPoliciesClient{}
	a := &Azure{
		mgmtClient:     mockMgmt,
		subscriptionID: "sub",
		resourceGroup:  "rg",
		accountName:    "acct",
		containerName:  "container",
	}
	policy := common.LifecyclePolicy{
		ID:        "short",
		Prefix:    "tmp/",
		Retention: 0, // zero -> rounded up to 1 day
		Action:    "delete",
	}
	if err := a.AddPolicy(policy); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	// Confirm the rule was stored with days >= 1
	if mockMgmt.policy == nil {
		t.Fatal("expected policy to be set")
	}
	rule := mockMgmt.policy.Properties.Policy.Rules[0]
	if rule.Definition.Actions.BaseBlob.Delete.DaysAfterModificationGreaterThan == nil {
		t.Fatal("expected DaysAfterModificationGreaterThan to be set")
	}
	if *rule.Definition.Actions.BaseBlob.Delete.DaysAfterModificationGreaterThan < 1 {
		t.Fatal("expected days >= 1")
	}
}

// TestAzure_AddPolicy_ExistingRuleReplaced covers the branch that removes a
// rule with the same ID before appending the new one.
func TestAzure_AddPolicy_ExistingRuleReplaced(t *testing.T) {
	mockMgmt := &mockManagementPoliciesClient{}
	a := &Azure{
		mgmtClient:     mockMgmt,
		subscriptionID: "sub",
		resourceGroup:  "rg",
		accountName:    "acct",
		containerName:  "container",
	}

	policy := common.LifecyclePolicy{
		ID:        "p1",
		Prefix:    "logs/",
		Retention: 7 * 24 * time.Hour,
		Action:    "delete",
	}
	if err := a.AddPolicy(policy); err != nil {
		t.Fatalf("first AddPolicy: %v", err)
	}
	// Re-adding the same ID should replace it.
	policy.Retention = 14 * 24 * time.Hour
	if err := a.AddPolicy(policy); err != nil {
		t.Fatalf("second AddPolicy: %v", err)
	}

	policies, err := a.GetPolicies()
	if err != nil {
		t.Fatalf("GetPolicies: %v", err)
	}
	if len(policies) != 1 {
		t.Fatalf("expected 1 policy after upsert, got %d", len(policies))
	}
	if policies[0].Retention != 14*24*time.Hour {
		t.Fatalf("expected updated retention, got %v", policies[0].Retention)
	}
}

// TestAzure_AddPolicy_CreateOrUpdateError covers the CreateOrUpdate failure branch.
func TestAzure_AddPolicy_CreateOrUpdateError(t *testing.T) {
	errCreate := errors.New("create error")
	mockMgmt := &mockManagementPoliciesClient{createErr: errCreate}
	a := &Azure{
		mgmtClient:     mockMgmt,
		subscriptionID: "sub",
		resourceGroup:  "rg",
		accountName:    "acct",
		containerName:  "container",
	}
	policy := common.LifecyclePolicy{
		ID:        "p1",
		Prefix:    "x/",
		Retention: 24 * time.Hour,
		Action:    "delete",
	}
	if err := a.AddPolicy(policy); !errors.Is(err, errCreate) {
		t.Fatalf("expected createErr, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// RemovePolicy edge cases
// ---------------------------------------------------------------------------

// TestAzure_RemovePolicy_DeleteError covers the Delete call failure when no
// rules remain after removing a policy.
func TestAzure_RemovePolicy_DeleteError(t *testing.T) {
	errDelete := errors.New("delete err")
	mockMgmt := &mockManagementPoliciesClient{deleteErr: errDelete}
	a := &Azure{
		mgmtClient:     mockMgmt,
		subscriptionID: "sub",
		resourceGroup:  "rg",
		accountName:    "acct",
		containerName:  "container",
	}

	// Add one policy so there is one rule to delete.
	if err := a.AddPolicy(common.LifecyclePolicy{
		ID: "p1", Prefix: "x/", Retention: 24 * time.Hour, Action: "delete",
	}); err != nil {
		t.Fatalf("AddPolicy: %v", err)
	}
	// Reset deleteErr after the CreateOrUpdate in AddPolicy (which succeeded).
	mockMgmt.deleteErr = errDelete

	// Removing the last rule should call Delete, which fails.
	err := a.RemovePolicy("p1")
	if !errors.Is(err, errDelete) {
		t.Fatalf("expected deleteErr, got %v", err)
	}
}

// TestAzure_RemovePolicy_UpdateAfterRemoveError covers the CreateOrUpdate
// failure when rules remain after removal.
func TestAzure_RemovePolicy_UpdateAfterRemoveError(t *testing.T) {
	mockMgmt := &mockManagementPoliciesClient{}
	a := &Azure{
		mgmtClient:     mockMgmt,
		subscriptionID: "sub",
		resourceGroup:  "rg",
		accountName:    "acct",
		containerName:  "container",
	}

	// Add two policies.
	if err := a.AddPolicy(common.LifecyclePolicy{
		ID: "p1", Prefix: "a/", Retention: 24 * time.Hour, Action: "delete",
	}); err != nil {
		t.Fatalf("AddPolicy p1: %v", err)
	}
	if err := a.AddPolicy(common.LifecyclePolicy{
		ID: "p2", Prefix: "b/", Retention: 48 * time.Hour, Action: "delete",
	}); err != nil {
		t.Fatalf("AddPolicy p2: %v", err)
	}

	// Now make CreateOrUpdate fail for the next call (the update after remove).
	errUpdate := errors.New("update err")
	mockMgmt.createErr = errUpdate

	err := a.RemovePolicy("p1")
	if !errors.Is(err, errUpdate) {
		t.Fatalf("expected updateErr, got %v", err)
	}
}

// TestAzure_RemovePolicy_NilPolicyProperties covers the early-return when the
// management policy has nil Properties/Policy/Rules.
func TestAzure_RemovePolicy_NilPolicyProperties(t *testing.T) {
	// mockManagementPoliciesClient.Get returns nil policy when policy == nil.
	// The Get method returns errTestPolicyNotFound when policy is nil, so
	// RemovePolicy returns nil (nothing to remove).
	mockMgmt := &mockManagementPoliciesClient{policy: nil}
	a := &Azure{
		mgmtClient:     mockMgmt,
		subscriptionID: "sub",
		resourceGroup:  "rg",
		accountName:    "acct",
		containerName:  "container",
	}
	// mockManagementPoliciesClient.Get returns an error when policy is nil,
	// which causes RemovePolicy to return nil immediately.
	if err := a.RemovePolicy("any"); err != nil {
		t.Fatalf("expected nil for non-existent policy, got %v", err)
	}
}

// TestAzure_RemovePolicy_NilRules covers the nil Rules early-return in RemovePolicy.
func TestAzure_RemovePolicy_NilRules(t *testing.T) {
	// Policy with Properties set but Rules == nil.
	mgmtPolicy := &armstorage.ManagementPolicy{
		Properties: &armstorage.ManagementPolicyProperties{
			Policy: &armstorage.ManagementPolicySchema{
				Rules: nil,
			},
		},
	}
	mockMgmt := &mockManagementPoliciesClient{policy: mgmtPolicy}
	a := &Azure{
		mgmtClient:     mockMgmt,
		subscriptionID: "sub",
		resourceGroup:  "rg",
		accountName:    "acct",
		containerName:  "container",
	}
	if err := a.RemovePolicy("any"); err != nil {
		t.Fatalf("expected nil for nil rules, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// GetPolicies edge cases
// ---------------------------------------------------------------------------

// TestAzure_GetPolicies_NilNameSkipped covers the branch that skips rules
// with a nil Name pointer.
func TestAzure_GetPolicies_NilNameSkipped(t *testing.T) {
	// A rule with nil Name should be skipped.
	prefixStr := "container/logs/"
	ruleType := armstorage.RuleTypeLifecycle
	enabled := true
	blobType := "blockBlob"
	days := float32(7)
	mgmtPolicy := &armstorage.ManagementPolicy{
		Properties: &armstorage.ManagementPolicyProperties{
			Policy: &armstorage.ManagementPolicySchema{
				Rules: []*armstorage.ManagementPolicyRule{
					{
						// Name is nil — should be skipped
						Name:    nil,
						Enabled: &enabled,
						Type:    &ruleType,
						Definition: &armstorage.ManagementPolicyDefinition{
							Filters: &armstorage.ManagementPolicyFilter{
								BlobTypes:   []*string{&blobType},
								PrefixMatch: []*string{&prefixStr},
							},
							Actions: &armstorage.ManagementPolicyAction{
								BaseBlob: &armstorage.ManagementPolicyBaseBlob{
									Delete: &armstorage.DateAfterModification{
										DaysAfterModificationGreaterThan: &days,
									},
								},
							},
						},
					},
				},
			},
		},
	}
	mockMgmt := &mockManagementPoliciesClient{policy: mgmtPolicy}
	a := &Azure{
		mgmtClient:     mockMgmt,
		containerName:  "container",
		subscriptionID: "sub",
		resourceGroup:  "rg",
		accountName:    "acct",
	}
	policies, err := a.GetPolicies()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(policies) != 0 {
		t.Fatalf("expected 0 policies (nil name skipped), got %d", len(policies))
	}
}

// TestAzure_GetPolicies_NilDefinitionSkipped covers the branch that skips
// rules with a nil Definition pointer.
func TestAzure_GetPolicies_NilDefinitionSkipped(t *testing.T) {
	ruleName := "p1"
	mgmtPolicy := &armstorage.ManagementPolicy{
		Properties: &armstorage.ManagementPolicyProperties{
			Policy: &armstorage.ManagementPolicySchema{
				Rules: []*armstorage.ManagementPolicyRule{
					{
						Name:       &ruleName,
						Definition: nil, // skip
					},
				},
			},
		},
	}
	mockMgmt := &mockManagementPoliciesClient{policy: mgmtPolicy}
	a := &Azure{
		mgmtClient:     mockMgmt,
		containerName:  "container",
		subscriptionID: "sub",
		resourceGroup:  "rg",
		accountName:    "acct",
	}
	policies, err := a.GetPolicies()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(policies) != 0 {
		t.Fatalf("expected 0 policies (nil definition skipped), got %d", len(policies))
	}
}

// TestAzure_GetPolicies_DifferentContainerSkipped covers the prefix filter
// that skips rules not matching our container.
func TestAzure_GetPolicies_DifferentContainerSkipped(t *testing.T) {
	ruleName := "other-container-rule"
	otherPrefix := "othercontainer/logs/"
	ruleType := armstorage.RuleTypeLifecycle
	blobType := "blockBlob"
	days := float32(7)
	enabled := true
	mgmtPolicy := &armstorage.ManagementPolicy{
		Properties: &armstorage.ManagementPolicyProperties{
			Policy: &armstorage.ManagementPolicySchema{
				Rules: []*armstorage.ManagementPolicyRule{
					{
						Name:    &ruleName,
						Enabled: &enabled,
						Type:    &ruleType,
						Definition: &armstorage.ManagementPolicyDefinition{
							Filters: &armstorage.ManagementPolicyFilter{
								BlobTypes:   []*string{&blobType},
								PrefixMatch: []*string{&otherPrefix},
							},
							Actions: &armstorage.ManagementPolicyAction{
								BaseBlob: &armstorage.ManagementPolicyBaseBlob{
									Delete: &armstorage.DateAfterModification{
										DaysAfterModificationGreaterThan: &days,
									},
								},
							},
						},
					},
				},
			},
		},
	}
	mockMgmt := &mockManagementPoliciesClient{policy: mgmtPolicy}
	a := &Azure{
		mgmtClient:     mockMgmt,
		containerName:  "mycontainer", // doesn't match "othercontainer/"
		subscriptionID: "sub",
		resourceGroup:  "rg",
		accountName:    "acct",
	}
	policies, err := a.GetPolicies()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(policies) != 0 {
		t.Fatalf("expected 0 policies (wrong container skipped), got %d", len(policies))
	}
}

// TestAzure_GetPolicies_NoActionsSkipped covers the "continue" branch in
// GetPolicies when a rule has nil Actions or nil BaseBlob.
func TestAzure_GetPolicies_NoActionsSkipped(t *testing.T) {
	ruleName := "no-actions"
	containerName := "mycontainer"
	prefix := containerName + "/logs/"
	ruleType := armstorage.RuleTypeLifecycle
	blobType := "blockBlob"
	enabled := true
	mgmtPolicy := &armstorage.ManagementPolicy{
		Properties: &armstorage.ManagementPolicyProperties{
			Policy: &armstorage.ManagementPolicySchema{
				Rules: []*armstorage.ManagementPolicyRule{
					{
						Name:    &ruleName,
						Enabled: &enabled,
						Type:    &ruleType,
						Definition: &armstorage.ManagementPolicyDefinition{
							Filters: &armstorage.ManagementPolicyFilter{
								BlobTypes:   []*string{&blobType},
								PrefixMatch: []*string{&prefix},
							},
							Actions: nil, // no actions — skip
						},
					},
				},
			},
		},
	}
	mockMgmt := &mockManagementPoliciesClient{policy: mgmtPolicy}
	a := &Azure{
		mgmtClient:     mockMgmt,
		containerName:  containerName,
		subscriptionID: "sub",
		resourceGroup:  "rg",
		accountName:    "acct",
	}
	policies, err := a.GetPolicies()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(policies) != 0 {
		t.Fatalf("expected 0 policies (no actions skipped), got %d", len(policies))
	}
}

// TestAzure_GetPolicies_UnknownActionSkipped covers the final else/continue
// branch when the action is neither delete nor archive.
func TestAzure_GetPolicies_UnknownActionSkipped(t *testing.T) {
	// A rule whose BaseBlob has neither Delete nor TierToArchive set.
	ruleName := "unknown-action"
	containerName := "mycontainer"
	prefix := containerName + "/logs/"
	ruleType := armstorage.RuleTypeLifecycle
	blobType := "blockBlob"
	enabled := true
	mgmtPolicy := &armstorage.ManagementPolicy{
		Properties: &armstorage.ManagementPolicyProperties{
			Policy: &armstorage.ManagementPolicySchema{
				Rules: []*armstorage.ManagementPolicyRule{
					{
						Name:    &ruleName,
						Enabled: &enabled,
						Type:    &ruleType,
						Definition: &armstorage.ManagementPolicyDefinition{
							Filters: &armstorage.ManagementPolicyFilter{
								BlobTypes:   []*string{&blobType},
								PrefixMatch: []*string{&prefix},
							},
							Actions: &armstorage.ManagementPolicyAction{
								BaseBlob: &armstorage.ManagementPolicyBaseBlob{
									// Neither Delete nor TierToArchive is set
								},
							},
						},
					},
				},
			},
		},
	}
	mockMgmt := &mockManagementPoliciesClient{policy: mgmtPolicy}
	a := &Azure{
		mgmtClient:     mockMgmt,
		containerName:  containerName,
		subscriptionID: "sub",
		resourceGroup:  "rg",
		accountName:    "acct",
	}
	policies, err := a.GetPolicies()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(policies) != 0 {
		t.Fatalf("expected 0 policies (unknown action skipped), got %d", len(policies))
	}
}

// TestAzure_GetPolicies_NilPolicyProperties covers the nil Properties check.
func TestAzure_GetPolicies_NilPolicyProperties(t *testing.T) {
	// mockManagementPoliciesClient.Get returns an error when policy is nil,
	// so GetPolicies returns an empty list.
	mockMgmt := &mockManagementPoliciesClient{policy: nil}
	a := &Azure{
		mgmtClient:     mockMgmt,
		containerName:  "mycontainer",
		subscriptionID: "sub",
		resourceGroup:  "rg",
		accountName:    "acct",
	}
	policies, err := a.GetPolicies()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(policies) != 0 {
		t.Fatalf("expected 0 policies, got %d", len(policies))
	}
}

// TestAzure_GetPolicies_NilPolicySchema covers the nil Policy check in GetPolicies.
func TestAzure_GetPolicies_NilPolicySchema(t *testing.T) {
	mgmtPolicy := &armstorage.ManagementPolicy{
		Properties: &armstorage.ManagementPolicyProperties{
			Policy: nil,
		},
	}
	mockMgmt := &mockManagementPoliciesClient{policy: mgmtPolicy}
	a := &Azure{
		mgmtClient:     mockMgmt,
		containerName:  "mycontainer",
		subscriptionID: "sub",
		resourceGroup:  "rg",
		accountName:    "acct",
	}
	policies, err := a.GetPolicies()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(policies) != 0 {
		t.Fatalf("expected 0 policies for nil policy schema, got %d", len(policies))
	}
}

// TestAzure_GetPolicies_NilRulesInSchema covers the nil Rules in schema check.
func TestAzure_GetPolicies_NilRulesInSchema(t *testing.T) {
	mgmtPolicy := &armstorage.ManagementPolicy{
		Properties: &armstorage.ManagementPolicyProperties{
			Policy: &armstorage.ManagementPolicySchema{
				Rules: nil,
			},
		},
	}
	mockMgmt := &mockManagementPoliciesClient{policy: mgmtPolicy}
	a := &Azure{
		mgmtClient:     mockMgmt,
		containerName:  "mycontainer",
		subscriptionID: "sub",
		resourceGroup:  "rg",
		accountName:    "acct",
	}
	policies, err := a.GetPolicies()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(policies) != 0 {
		t.Fatalf("expected 0 policies, got %d", len(policies))
	}
}

// TestAzure_GetPolicies_NilPrefixMatch covers the path where PrefixMatch is nil
// or empty — the rule is included with an empty prefix since no container
// filtering is applied when there is no prefix.
func TestAzure_GetPolicies_NilPrefixMatch(t *testing.T) {
	ruleName := "no-prefix"
	containerName := "mycontainer"
	ruleType := armstorage.RuleTypeLifecycle
	blobType := "blockBlob"
	enabled := true
	days := float32(7)
	mgmtPolicy := &armstorage.ManagementPolicy{
		Properties: &armstorage.ManagementPolicyProperties{
			Policy: &armstorage.ManagementPolicySchema{
				Rules: []*armstorage.ManagementPolicyRule{
					{
						Name:    &ruleName,
						Enabled: &enabled,
						Type:    &ruleType,
						Definition: &armstorage.ManagementPolicyDefinition{
							Filters: &armstorage.ManagementPolicyFilter{
								BlobTypes:   []*string{&blobType},
								PrefixMatch: nil, // nil prefix — policy.Prefix stays ""
							},
							Actions: &armstorage.ManagementPolicyAction{
								BaseBlob: &armstorage.ManagementPolicyBaseBlob{
									Delete: &armstorage.DateAfterModification{
										DaysAfterModificationGreaterThan: &days,
									},
								},
							},
						},
					},
				},
			},
		},
	}
	mockMgmt := &mockManagementPoliciesClient{policy: mgmtPolicy}
	a := &Azure{
		mgmtClient:     mockMgmt,
		containerName:  containerName,
		subscriptionID: "sub",
		resourceGroup:  "rg",
		accountName:    "acct",
	}
	policies, err := a.GetPolicies()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Rule is included with empty prefix because the nil PrefixMatch branch is
	// skipped and no container filtering is applied.
	if len(policies) != 1 {
		t.Fatalf("expected 1 policy (nil prefix included), got %d", len(policies))
	}
	if policies[0].Prefix != "" {
		t.Fatalf("expected empty prefix, got %q", policies[0].Prefix)
	}
}

// ---------------------------------------------------------------------------
// mapNotFound helper
// ---------------------------------------------------------------------------

// TestAzure_MapNotFound_NonStorageError covers the pass-through branch where
// the error is not an azblob.StorageError.
func TestAzure_MapNotFound_NonStorageError(t *testing.T) {
	plainErr := errors.New("plain error")
	got := mapNotFound(plainErr, "key")
	if got != plainErr {
		t.Fatalf("expected same error, got %v", got)
	}
}

// TestAzure_MapNotFound_BlobNotFound covers mapping BlobNotFound to ErrKeyNotFound.
func TestAzure_MapNotFound_BlobNotFound(t *testing.T) {
	stgErr := &fakeStorageError{code: azblob.ServiceCodeBlobNotFound}
	got := mapNotFound(stgErr, "mykey")
	if !errors.Is(got, common.ErrKeyNotFound) {
		t.Fatalf("expected ErrKeyNotFound, got %v", got)
	}
}

// TestAzure_MapNotFound_OtherStorageError covers a StorageError that is NOT
// BlobNotFound — should be returned unchanged.
func TestAzure_MapNotFound_OtherStorageError(t *testing.T) {
	stgErr := &fakeStorageError{code: azblob.ServiceCodeContainerAlreadyExists}
	got := mapNotFound(stgErr, "key")
	if errors.Is(got, common.ErrKeyNotFound) {
		t.Fatal("non-BlobNotFound StorageError must not map to ErrKeyNotFound")
	}
	if got != stgErr {
		t.Fatalf("expected unchanged error, got %v", got)
	}
}

// ---------------------------------------------------------------------------
// UpdateMetadata error paths
// ---------------------------------------------------------------------------

// TestAzure_UpdateMetadata_SetHTTPHeadersError covers a non-not-found error
// from SetHTTPHeaders propagating unchanged.
func TestAzure_UpdateMetadata_SetHTTPHeadersError(t *testing.T) {
	errHeaders := errors.New("headers error")
	mockCont := &mockContainerEnhanced{
		newBlockBlobFn: func(name string) BlobAPI {
			return &mockBlob{
				setHTTPHeadersFn: func(ctx context.Context, headers azblob.BlobHTTPHeaders) error {
					return errHeaders
				},
			}
		},
	}
	a := &Azure{container: mockCont}
	err := a.UpdateMetadata(context.Background(), "key", &common.Metadata{ContentType: "text/plain"})
	if !errors.Is(err, errHeaders) {
		t.Fatalf("expected headers error to propagate, got %v", err)
	}
	if errors.Is(err, common.ErrKeyNotFound) {
		t.Fatal("plain error must not map to ErrKeyNotFound")
	}
}

// TestAzure_UpdateMetadata_SetMetadataError_NonStorage covers a non-StorageError
// from SetMetadata propagating unchanged.
func TestAzure_UpdateMetadata_SetMetadataError_NonStorage(t *testing.T) {
	errMeta := errors.New("metadata error")
	mockCont := &mockContainerEnhanced{
		newBlockBlobFn: func(name string) BlobAPI {
			return &mockBlob{
				setMetadataFn: func(ctx context.Context, metadata map[string]string) error {
					return errMeta
				},
			}
		},
	}
	a := &Azure{container: mockCont}
	err := a.UpdateMetadata(context.Background(), "key", &common.Metadata{})
	if !errors.Is(err, errMeta) {
		t.Fatalf("expected metadata error to propagate, got %v", err)
	}
	if errors.Is(err, common.ErrKeyNotFound) {
		t.Fatal("plain error must not map to ErrKeyNotFound")
	}
}

// ---------------------------------------------------------------------------
// GetWithContext / PutWithContext / DeleteWithContext error paths
// ---------------------------------------------------------------------------

// TestAzure_GetWithContext_Error tests context-aware get with an error.
func TestAzure_GetWithContext_Error(t *testing.T) {
	errRead := errors.New("read error")
	mockCont := &mockContainerEnhanced{
		newBlockBlobFn: func(name string) BlobAPI {
			return &mockBlob{
				readFn: func(ctx context.Context) (io.ReadCloser, error) {
					return nil, errRead
				},
			}
		},
	}
	a := &Azure{container: mockCont}
	_, err := a.GetWithContext(context.Background(), "key")
	if !errors.Is(err, errRead) {
		t.Fatalf("expected read error, got %v", err)
	}
}

// TestAzure_DeleteWithContext_Error tests context-aware delete with an error.
func TestAzure_DeleteWithContext_Error(t *testing.T) {
	errDel := errors.New("del error")
	mockCont := &mockContainerEnhanced{
		newBlockBlobFn: func(name string) BlobAPI {
			return &mockBlob{
				deleteFn: func(ctx context.Context) error {
					return errDel
				},
			}
		},
	}
	a := &Azure{container: mockCont}
	err := a.DeleteWithContext(context.Background(), "key")
	if !errors.Is(err, errDel) {
		t.Fatalf("expected del error, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Configure remaining branches
// ---------------------------------------------------------------------------

// TestAzure_Configure_MissingAccountName covers the error when accountName is missing.
func TestAzure_Configure_MissingAccountName(t *testing.T) {
	a := &Azure{}
	err := a.Configure(map[string]string{
		"accountKey":    "key",
		"containerName": "c",
	})
	if err == nil {
		t.Fatal("expected error for missing accountName")
	}
}

// TestAzure_Configure_MissingAccountKey covers the error when accountKey is missing.
func TestAzure_Configure_MissingAccountKey(t *testing.T) {
	a := &Azure{}
	err := a.Configure(map[string]string{
		"accountName":   "acct",
		"containerName": "c",
	})
	if err == nil {
		t.Fatal("expected error for missing accountKey")
	}
}

// TestAzure_Configure_MissingContainerName covers the error when containerName is missing.
func TestAzure_Configure_MissingContainerName(t *testing.T) {
	a := &Azure{}
	err := a.Configure(map[string]string{
		"accountName": "acct",
		"accountKey":  "key",
	})
	if err == nil {
		t.Fatal("expected error for missing containerName")
	}
}
