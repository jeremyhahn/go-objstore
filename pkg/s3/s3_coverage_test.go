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

//go:build awss3

package s3

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

// TestS3_New verifies that New returns a non-nil Storage value.
func TestS3_New(t *testing.T) {
	s := New()
	if s == nil {
		t.Fatal("expected non-nil Storage from New()")
	}
}

// --- key-validation branches (invalid key -> error before the cloud call) ---

func TestS3_Put_InvalidKey(t *testing.T) {
	mockS3 := &mockS3Client{}
	s := &S3{svc: mockS3, bucket: "test-bucket"}
	err := s.Put("", bytes.NewReader([]byte("data")))
	if err == nil {
		t.Fatal("expected error for empty key")
	}
}

func TestS3_Get_InvalidKey(t *testing.T) {
	mockS3 := &mockS3Client{}
	s := &S3{svc: mockS3, bucket: "test-bucket"}
	_, err := s.Get("")
	if err == nil {
		t.Fatal("expected error for empty key")
	}
}

func TestS3_Delete_InvalidKey(t *testing.T) {
	mockS3 := &mockS3Client{}
	s := &S3{svc: mockS3, bucket: "test-bucket"}
	err := s.Delete("")
	if err == nil {
		t.Fatal("expected error for empty key")
	}
}

func TestS3_PutWithMetadata_InvalidKey(t *testing.T) {
	mockS3 := &mockS3Client{}
	s := &S3{svc: mockS3, bucket: "test-bucket"}
	err := s.PutWithMetadata(context.Background(), "", bytes.NewReader([]byte("data")), nil)
	if err == nil {
		t.Fatal("expected error for empty key")
	}
}

func TestS3_GetWithContext_InvalidKey(t *testing.T) {
	mockS3 := &mockS3Client{}
	s := &S3{svc: mockS3, bucket: "test-bucket"}
	_, err := s.GetWithContext(context.Background(), "")
	if err == nil {
		t.Fatal("expected error for empty key")
	}
}

func TestS3_GetMetadata_InvalidKey(t *testing.T) {
	mockS3 := &mockS3Client{}
	s := &S3{svc: mockS3, bucket: "test-bucket"}
	_, err := s.GetMetadata(context.Background(), "")
	if err == nil {
		t.Fatal("expected error for empty key")
	}
}

func TestS3_DeleteWithContext_InvalidKey(t *testing.T) {
	mockS3 := &mockS3Client{}
	s := &S3{svc: mockS3, bucket: "test-bucket"}
	err := s.DeleteWithContext(context.Background(), "")
	if err == nil {
		t.Fatal("expected error for empty key")
	}
}

func TestS3_Exists_InvalidKey(t *testing.T) {
	mockS3 := &mockS3Client{}
	s := &S3{svc: mockS3, bucket: "test-bucket"}
	_, err := s.Exists(context.Background(), "")
	if err == nil {
		t.Fatal("expected error for empty key")
	}
}

// --- isNoSuchLifecycleConfiguration ---

func TestIsNoSuchLifecycleConfiguration_Nil(t *testing.T) {
	if isNoSuchLifecycleConfiguration(nil) {
		t.Fatal("expected false for nil error")
	}
}

func TestIsNoSuchLifecycleConfiguration_Match(t *testing.T) {
	if !isNoSuchLifecycleConfiguration(errors.New("NoSuchLifecycleConfiguration: bucket has no lifecycle")) {
		t.Fatal("expected true for NoSuchLifecycleConfiguration prefix")
	}
}

func TestIsNoSuchLifecycleConfiguration_NoMatch(t *testing.T) {
	if isNoSuchLifecycleConfiguration(errors.New("some other error")) {
		t.Fatal("expected false for unrelated error")
	}
}

// --- AddPolicy error paths ---

func TestS3_AddPolicy_GetLifecycleNonNoSuchError(t *testing.T) {
	mockS3 := &mockS3Client{
		getBucketLifecycleConfigurationError: errors.New("InternalError: something went wrong"),
	}
	s := &S3{svc: mockS3, bucket: "test-bucket"}
	policy := common.LifecyclePolicy{
		ID:        "policy1",
		Prefix:    "logs/",
		Retention: 24 * time.Hour,
		Action:    "delete",
	}
	err := s.AddPolicy(policy)
	if err == nil {
		t.Fatal("expected error when GetBucketLifecycleConfiguration returns non-NoSuch error")
	}
}

func TestS3_AddPolicy_PutLifecycleError(t *testing.T) {
	mockS3 := &mockS3Client{
		getBucketLifecycleConfigurationError:  errors.New("NoSuchLifecycleConfiguration"),
		putBucketLifecycleConfigurationError:  errors.New("PutLifecycle failed"),
		putBucketLifecycleConfigurationOutput: nil,
	}
	s := &S3{svc: mockS3, bucket: "test-bucket"}
	policy := common.LifecyclePolicy{
		ID:        "policy1",
		Prefix:    "logs/",
		Retention: 24 * time.Hour,
		Action:    "delete",
	}
	err := s.AddPolicy(policy)
	if err == nil {
		t.Fatal("expected error when PutBucketLifecycleConfiguration fails")
	}
}

// Retention < 24h should clamp to 1 day.
func TestS3_AddPolicy_SubDayRetentionClampedToOne(t *testing.T) {
	mockS3 := &mockS3Client{
		getBucketLifecycleConfigurationError:  errors.New("NoSuchLifecycleConfiguration"),
		putBucketLifecycleConfigurationOutput: &s3.PutBucketLifecycleConfigurationOutput{},
	}
	s := &S3{svc: mockS3, bucket: "test-bucket"}
	policy := common.LifecyclePolicy{
		ID:        "short-policy",
		Prefix:    "tmp/",
		Retention: 1 * time.Hour, // less than 1 day
		Action:    "delete",
	}
	if err := s.AddPolicy(policy); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if mockS3.lifecycleConfig == nil || len(mockS3.lifecycleConfig.Rules) != 1 {
		t.Fatal("expected 1 lifecycle rule")
	}
	rule := mockS3.lifecycleConfig.Rules[0]
	if rule.Expiration == nil || rule.Expiration.Days == nil || *rule.Expiration.Days != 1 {
		t.Fatalf("expected retention clamped to 1 day, got %v", rule.Expiration)
	}
}

// AddPolicy with an existing config replaces the rule with the same ID.
func TestS3_AddPolicy_ReplacesExistingRuleWithSameID(t *testing.T) {
	mockS3 := &mockS3Client{
		putBucketLifecycleConfigurationOutput: &s3.PutBucketLifecycleConfigurationOutput{},
		getBucketLifecycleConfigurationError:  errors.New("NoSuchLifecycleConfiguration"),
	}
	s := &S3{svc: mockS3, bucket: "test-bucket"}

	// Add the initial rule.
	first := common.LifecyclePolicy{
		ID:        "p1",
		Prefix:    "old/",
		Retention: 7 * 24 * time.Hour,
		Action:    "delete",
	}
	if err := s.AddPolicy(first); err != nil {
		t.Fatalf("expected no error adding first policy: %v", err)
	}

	// Re-add with same ID but different prefix; should replace, not append.
	second := common.LifecyclePolicy{
		ID:        "p1",
		Prefix:    "new/",
		Retention: 14 * 24 * time.Hour,
		Action:    "delete",
	}
	if err := s.AddPolicy(second); err != nil {
		t.Fatalf("expected no error adding replacement policy: %v", err)
	}

	policies, err := s.GetPolicies()
	if err != nil {
		t.Fatalf("expected no error getting policies: %v", err)
	}
	if len(policies) != 1 {
		t.Fatalf("expected 1 policy after replacement, got %d", len(policies))
	}
	if policies[0].Prefix != "new/" {
		t.Fatalf("expected prefix 'new/', got %s", policies[0].Prefix)
	}
}

// --- RemovePolicy error paths ---

func TestS3_RemovePolicy_GetLifecycleNonNoSuchError(t *testing.T) {
	mockS3 := &mockS3Client{
		getBucketLifecycleConfigurationError: errors.New("InternalError: something went wrong"),
	}
	s := &S3{svc: mockS3, bucket: "test-bucket"}
	err := s.RemovePolicy("policy1")
	if err == nil {
		t.Fatal("expected error when GetBucketLifecycleConfiguration returns non-NoSuch error")
	}
}

func TestS3_RemovePolicy_DeleteBucketLifecycleError(t *testing.T) {
	mockS3 := &mockS3Client{
		putBucketLifecycleConfigurationOutput: &s3.PutBucketLifecycleConfigurationOutput{},
		getBucketLifecycleConfigurationError:  errors.New("NoSuchLifecycleConfiguration"),
		deleteBucketLifecycleError:            errors.New("delete lifecycle failed"),
	}
	s := &S3{svc: mockS3, bucket: "test-bucket"}

	// Add a single rule first.
	policy := common.LifecyclePolicy{
		ID:        "p1",
		Prefix:    "logs/",
		Retention: 7 * 24 * time.Hour,
		Action:    "delete",
	}
	if err := s.AddPolicy(policy); err != nil {
		t.Fatalf("setup: expected no error adding policy: %v", err)
	}

	// Now delete will fail on DeleteBucketLifecycle.
	err := s.RemovePolicy("p1")
	if err == nil {
		t.Fatal("expected error when DeleteBucketLifecycle fails")
	}
}

func TestS3_RemovePolicy_PutLifecycleErrorWithRemainingRules(t *testing.T) {
	mockS3 := &mockS3Client{
		putBucketLifecycleConfigurationOutput: &s3.PutBucketLifecycleConfigurationOutput{},
		getBucketLifecycleConfigurationError:  errors.New("NoSuchLifecycleConfiguration"),
	}
	s := &S3{svc: mockS3, bucket: "test-bucket"}

	// Add two rules so that removing one leaves a non-empty set.
	p1 := common.LifecyclePolicy{ID: "p1", Prefix: "a/", Retention: 7 * 24 * time.Hour, Action: "delete"}
	p2 := common.LifecyclePolicy{ID: "p2", Prefix: "b/", Retention: 7 * 24 * time.Hour, Action: "delete"}
	if err := s.AddPolicy(p1); err != nil {
		t.Fatalf("setup p1: %v", err)
	}
	if err := s.AddPolicy(p2); err != nil {
		t.Fatalf("setup p2: %v", err)
	}

	// Now inject a PutBucketLifecycleConfiguration error.
	mockS3.putBucketLifecycleConfigurationError = errors.New("put lifecycle failed")

	err := s.RemovePolicy("p1")
	if err == nil {
		t.Fatal("expected error when PutBucketLifecycleConfiguration fails on remove")
	}
}

// --- GetPolicies error paths ---

func TestS3_GetPolicies_GetLifecycleNonNoSuchError(t *testing.T) {
	mockS3 := &mockS3Client{
		getBucketLifecycleConfigurationError: errors.New("InternalError: something went wrong"),
	}
	s := &S3{svc: mockS3, bucket: "test-bucket"}
	_, err := s.GetPolicies()
	if err == nil {
		t.Fatal("expected error when GetBucketLifecycleConfiguration returns non-NoSuch error")
	}
}

// GetPolicies skips rules that have neither Expiration nor Transitions.
func TestS3_GetPolicies_SkipsUnknownRuleType(t *testing.T) {
	status := "Enabled"
	id := "weird-rule"
	mockS3 := &mockS3Client{
		lifecycleConfig: &s3.BucketLifecycleConfiguration{
			Rules: []*s3.LifecycleRule{
				{
					ID:     &id,
					Status: &status,
					// No Expiration, no Transitions — should be skipped.
				},
			},
		},
	}
	s := &S3{svc: mockS3, bucket: "test-bucket"}
	policies, err := s.GetPolicies()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(policies) != 0 {
		t.Fatalf("expected 0 policies (unknown rule skipped), got %d", len(policies))
	}
}

// GetPolicies skips rules where ID or Status is nil, or Status != "Enabled".
func TestS3_GetPolicies_SkipsDisabledRule(t *testing.T) {
	idEnabled := "enabled-rule"
	statusEnabled := "Enabled"
	idDisabled := "disabled-rule"
	statusDisabled := "Disabled"
	days := int64(7)
	mockS3 := &mockS3Client{
		lifecycleConfig: &s3.BucketLifecycleConfiguration{
			Rules: []*s3.LifecycleRule{
				{
					ID:     &idDisabled,
					Status: &statusDisabled,
					Expiration: &s3.LifecycleExpiration{
						Days: aws.Int64(days),
					},
				},
				{
					ID:     &idEnabled,
					Status: &statusEnabled,
					Expiration: &s3.LifecycleExpiration{
						Days: aws.Int64(days),
					},
				},
			},
		},
	}
	s := &S3{svc: mockS3, bucket: "test-bucket"}
	policies, err := s.GetPolicies()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(policies) != 1 {
		t.Fatalf("expected 1 policy (disabled rule skipped), got %d", len(policies))
	}
	if policies[0].ID != "enabled-rule" {
		t.Fatalf("expected enabled-rule, got %s", policies[0].ID)
	}
}

// --- GetReplicationManager / SetReplicationManager ---

func TestS3_GetReplicationManager_NotSet(t *testing.T) {
	s := &S3{}
	_, err := s.GetReplicationManager()
	if err != common.ErrReplicationNotSupported {
		t.Fatalf("expected ErrReplicationNotSupported, got %v", err)
	}
}

func TestS3_SetAndGetReplicationManager(t *testing.T) {
	s := &S3{}
	rm := &mockReplicationManager{}
	s.SetReplicationManager(rm)
	got, err := s.GetReplicationManager()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if got != rm {
		t.Fatal("expected the set replication manager to be returned")
	}
}

// mockReplicationManager is a minimal stub satisfying common.ReplicationManager.
type mockReplicationManager struct{}

func (m *mockReplicationManager) AddPolicy(policy common.ReplicationPolicy) error { return nil }
func (m *mockReplicationManager) RemovePolicy(id string) error                    { return nil }
func (m *mockReplicationManager) GetPolicy(id string) (*common.ReplicationPolicy, error) {
	return nil, nil
}
func (m *mockReplicationManager) GetPolicies() ([]common.ReplicationPolicy, error) { return nil, nil }
func (m *mockReplicationManager) SyncAll(ctx context.Context) (*common.SyncResult, error) {
	return nil, nil
}
func (m *mockReplicationManager) SyncPolicy(ctx context.Context, policyID string) (*common.SyncResult, error) {
	return nil, nil
}
func (m *mockReplicationManager) SyncAllParallel(ctx context.Context, workerCount int) (*common.SyncResult, error) {
	return nil, nil
}
func (m *mockReplicationManager) SyncPolicyParallel(ctx context.Context, policyID string, workerCount int) (*common.SyncResult, error) {
	return nil, nil
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
func (m *mockReplicationManager) Run(ctx context.Context) {}
