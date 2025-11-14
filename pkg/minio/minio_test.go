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

//go:build minio

package minio

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"sort"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

type mockS3Client struct {
	s3iface.S3API
	putObjectOutput                       *s3.PutObjectOutput
	getObjectOutput                       *s3.GetObjectOutput
	deleteObjectOutput                    *s3.DeleteObjectOutput
	listObjectsV2Output                   *s3.ListObjectsV2Output
	headObjectOutput                      *s3.HeadObjectOutput
	copyObjectOutput                      *s3.CopyObjectOutput
	getBucketLifecycleConfigurationOutput *s3.GetBucketLifecycleConfigurationOutput
	putBucketLifecycleConfigurationOutput *s3.PutBucketLifecycleConfigurationOutput
	deleteBucketLifecycleOutput           *s3.DeleteBucketLifecycleOutput
	putObjectError                        error
	getObjectError                        error
	deleteObjectError                     error
	listObjectsV2Error                    error
	headObjectError                       error
	copyObjectError                       error
	getBucketLifecycleConfigurationError  error
	putBucketLifecycleConfigurationError  error
	deleteBucketLifecycleError            error
	listObjectsV2Calls                    int
	listObjectsV2Outputs                  []*s3.ListObjectsV2Output
	lifecycleConfig                       *s3.BucketLifecycleConfiguration
}

func (m *mockS3Client) PutObject(input *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
	if m.putObjectError != nil {
		return nil, m.putObjectError
	}
	return m.putObjectOutput, nil
}

func (m *mockS3Client) GetObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	if m.getObjectError != nil {
		return nil, m.getObjectError
	}
	return m.getObjectOutput, nil
}

func (m *mockS3Client) DeleteObject(input *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
	if m.deleteObjectError != nil {
		return nil, m.deleteObjectError
	}
	return m.deleteObjectOutput, nil
}

func (m *mockS3Client) ListObjectsV2(input *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
	if m.listObjectsV2Error != nil {
		return nil, m.listObjectsV2Error
	}
	if len(m.listObjectsV2Outputs) > 0 {
		output := m.listObjectsV2Outputs[m.listObjectsV2Calls]
		m.listObjectsV2Calls++
		return output, nil
	}
	return m.listObjectsV2Output, nil
}

func (m *mockS3Client) GetBucketLifecycleConfiguration(input *s3.GetBucketLifecycleConfigurationInput) (*s3.GetBucketLifecycleConfigurationOutput, error) {
	// If lifecycle config exists, return it (ignoring the error)
	if m.lifecycleConfig != nil {
		return &s3.GetBucketLifecycleConfigurationOutput{
			Rules: m.lifecycleConfig.Rules,
		}, nil
	}
	// Otherwise, return the configured error or output
	if m.getBucketLifecycleConfigurationError != nil {
		return nil, m.getBucketLifecycleConfigurationError
	}
	return m.getBucketLifecycleConfigurationOutput, nil
}

func (m *mockS3Client) PutBucketLifecycleConfiguration(input *s3.PutBucketLifecycleConfigurationInput) (*s3.PutBucketLifecycleConfigurationOutput, error) {
	if m.putBucketLifecycleConfigurationError != nil {
		return nil, m.putBucketLifecycleConfigurationError
	}
	// Store the lifecycle configuration in the mock
	if input.LifecycleConfiguration != nil {
		m.lifecycleConfig = input.LifecycleConfiguration
	}
	return m.putBucketLifecycleConfigurationOutput, nil
}

func (m *mockS3Client) DeleteBucketLifecycle(input *s3.DeleteBucketLifecycleInput) (*s3.DeleteBucketLifecycleOutput, error) {
	if m.deleteBucketLifecycleError != nil {
		return nil, m.deleteBucketLifecycleError
	}
	// Clear the lifecycle configuration in the mock
	m.lifecycleConfig = nil
	return m.deleteBucketLifecycleOutput, nil
}

func TestMinIO_Put(t *testing.T) {
	mockS3 := &mockS3Client{
		putObjectOutput: &s3.PutObjectOutput{},
	}

	m := &MinIO{svc: mockS3, bucket: "test-bucket"}

	err := m.Put("key", bytes.NewReader([]byte("data")))
	if err != nil {
		t.Fatal(err)
	}
}

func TestMinIO_Put_Error(t *testing.T) {
	mockS3 := &mockS3Client{
		putObjectError: errors.New("upload error"),
	}

	m := &MinIO{svc: mockS3, bucket: "test-bucket"}

	err := m.Put("key", bytes.NewReader([]byte("data")))
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestMinIO_Get(t *testing.T) {
	mockS3 := &mockS3Client{
		getObjectOutput: &s3.GetObjectOutput{
			Body: ioutil.NopCloser(bytes.NewReader([]byte("data"))),
		},
	}

	m := &MinIO{svc: mockS3, bucket: "test-bucket"}

	r, err := m.Get("key")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	data, err := ioutil.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}

	if string(data) != "data" {
		t.Fatalf("expected %s, got %s", "data", string(data))
	}
}

func TestMinIO_Get_Error(t *testing.T) {
	mockS3 := &mockS3Client{
		getObjectError: errors.New("get error"),
	}

	m := &MinIO{svc: mockS3, bucket: "test-bucket"}

	_, err := m.Get("key")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestMinIO_Delete(t *testing.T) {
	mockS3 := &mockS3Client{
		deleteObjectOutput: &s3.DeleteObjectOutput{},
	}

	m := &MinIO{svc: mockS3, bucket: "test-bucket"}

	err := m.Delete("key")
	if err != nil {
		t.Fatal(err)
	}
}

func TestMinIO_Delete_Error(t *testing.T) {
	mockS3 := &mockS3Client{
		deleteObjectError: errors.New("delete error"),
	}

	m := &MinIO{svc: mockS3, bucket: "test-bucket"}

	err := m.Delete("key")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

type mockArchiver struct {
	putError error
}

func (m *mockArchiver) Put(key string, data io.Reader) error {
	if m.putError != nil {
		return m.putError
	}
	return nil
}

func TestMinIO_Archive(t *testing.T) {
	mockS3 := &mockS3Client{
		getObjectOutput: &s3.GetObjectOutput{
			Body: io.NopCloser(bytes.NewReader([]byte("data"))),
		},
	}

	m := &MinIO{svc: mockS3, bucket: "test-bucket"}

	err := m.Archive("key", &mockArchiver{})
	if err != nil {
		t.Fatal(err)
	}
}

func TestMinIO_Archive_GetError(t *testing.T) {
	mockS3 := &mockS3Client{
		getObjectError: errors.New("get error"),
	}

	m := &MinIO{svc: mockS3, bucket: "test-bucket"}

	err := m.Archive("key", &mockArchiver{})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestMinIO_Archive_PutError(t *testing.T) {
	mockS3 := &mockS3Client{
		getObjectOutput: &s3.GetObjectOutput{
			Body: io.NopCloser(bytes.NewReader([]byte("data"))),
		},
	}

	m := &MinIO{svc: mockS3, bucket: "test-bucket"}

	err := m.Archive("key", &mockArchiver{putError: errors.New("put error")})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestMinIO_AddPolicy_Success(t *testing.T) {
	mockS3 := &mockS3Client{
		putBucketLifecycleConfigurationOutput: &s3.PutBucketLifecycleConfigurationOutput{},
		getBucketLifecycleConfigurationError:  errors.New("NoSuchLifecycleConfiguration"),
	}

	m := &MinIO{svc: mockS3, bucket: "test-bucket"}
	policy := common.LifecyclePolicy{
		ID:        "policy1",
		Prefix:    "logs/",
		Retention: 24 * 7 * time.Hour,
		Action:    "delete",
	}
	err := m.AddPolicy(policy)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Verify the lifecycle configuration was created correctly
	if mockS3.lifecycleConfig == nil {
		t.Fatal("expected lifecycle config to be set")
	}
	if len(mockS3.lifecycleConfig.Rules) != 1 {
		t.Fatalf("expected 1 lifecycle rule, got %d", len(mockS3.lifecycleConfig.Rules))
	}

	rule := mockS3.lifecycleConfig.Rules[0]

	// Verify rule ID
	if rule.ID == nil || *rule.ID != "policy1" {
		t.Fatalf("expected rule ID 'policy1', got %v", rule.ID)
	}

	// Verify rule status
	if rule.Status == nil || *rule.Status != "Enabled" {
		t.Fatalf("expected rule status 'Enabled', got %v", rule.Status)
	}

	// Verify prefix filter
	if rule.Filter == nil || rule.Filter.Prefix == nil || *rule.Filter.Prefix != "logs/" {
		t.Fatalf("expected prefix 'logs/', got %v", rule.Filter)
	}

	// Verify expiration is set for delete action
	if rule.Expiration == nil {
		t.Fatal("expected Expiration to be set for delete action")
	}
	if rule.Expiration.Days == nil || *rule.Expiration.Days != 7 {
		t.Fatalf("expected expiration days 7, got %v", rule.Expiration.Days)
	}

	// Verify no transitions are set for delete action
	if rule.Transitions != nil && len(rule.Transitions) > 0 {
		t.Fatal("expected no transitions for delete action")
	}

	policies, err := m.GetPolicies()
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

func TestMinIO_AddPolicy_Archive(t *testing.T) {
	mockS3 := &mockS3Client{
		putBucketLifecycleConfigurationOutput: &s3.PutBucketLifecycleConfigurationOutput{},
		getBucketLifecycleConfigurationError:  errors.New("NoSuchLifecycleConfiguration"),
	}

	m := &MinIO{svc: mockS3, bucket: "test-bucket"}
	policy := common.LifecyclePolicy{
		ID:        "archive-policy",
		Prefix:    "old-data/",
		Retention: 30 * 24 * time.Hour,
		Action:    "archive",
	}
	err := m.AddPolicy(policy)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Verify the lifecycle configuration was created correctly
	if mockS3.lifecycleConfig == nil {
		t.Fatal("expected lifecycle config to be set")
	}
	if len(mockS3.lifecycleConfig.Rules) != 1 {
		t.Fatalf("expected 1 lifecycle rule, got %d", len(mockS3.lifecycleConfig.Rules))
	}

	rule := mockS3.lifecycleConfig.Rules[0]

	// Verify rule ID
	if rule.ID == nil || *rule.ID != "archive-policy" {
		t.Fatalf("expected rule ID 'archive-policy', got %v", rule.ID)
	}

	// Verify rule status
	if rule.Status == nil || *rule.Status != "Enabled" {
		t.Fatalf("expected rule status 'Enabled', got %v", rule.Status)
	}

	// Verify prefix filter
	if rule.Filter == nil || rule.Filter.Prefix == nil || *rule.Filter.Prefix != "old-data/" {
		t.Fatalf("expected prefix 'old-data/', got %v", rule.Filter)
	}

	// Verify transitions are set for archive action
	if rule.Transitions == nil || len(rule.Transitions) != 1 {
		t.Fatalf("expected 1 transition for archive action, got %v", rule.Transitions)
	}

	transition := rule.Transitions[0]

	// Verify transition days
	if transition.Days == nil || *transition.Days != 30 {
		t.Fatalf("expected transition days 30, got %v", transition.Days)
	}

	// Verify storage class is GLACIER
	if transition.StorageClass == nil || *transition.StorageClass != s3.TransitionStorageClassGlacier {
		t.Fatalf("expected storage class GLACIER, got %v", transition.StorageClass)
	}

	// Verify no expiration is set for archive action
	if rule.Expiration != nil {
		t.Fatal("expected no Expiration for archive action")
	}

	policies, err := m.GetPolicies()
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

func TestMinIO_AddPolicy_InvalidID(t *testing.T) {
	mockS3 := &mockS3Client{}
	m := &MinIO{svc: mockS3, bucket: "test-bucket"}
	policy := common.LifecyclePolicy{
		ID:        "",
		Prefix:    "logs/",
		Retention: 24 * time.Hour,
		Action:    "delete",
	}
	err := m.AddPolicy(policy)
	if err != common.ErrInvalidPolicy {
		t.Fatalf("expected ErrInvalidPolicy, got %v", err)
	}
}

func TestMinIO_AddPolicy_InvalidAction(t *testing.T) {
	mockS3 := &mockS3Client{}
	m := &MinIO{svc: mockS3, bucket: "test-bucket"}
	policy := common.LifecyclePolicy{
		ID:        "policy1",
		Prefix:    "logs/",
		Retention: 24 * time.Hour,
		Action:    "invalid",
	}
	err := m.AddPolicy(policy)
	if err != common.ErrInvalidPolicy {
		t.Fatalf("expected ErrInvalidPolicy, got %v", err)
	}
}

func TestMinIO_RemovePolicy(t *testing.T) {
	mockS3 := &mockS3Client{
		putBucketLifecycleConfigurationOutput: &s3.PutBucketLifecycleConfigurationOutput{},
		deleteBucketLifecycleOutput:           &s3.DeleteBucketLifecycleOutput{},
		getBucketLifecycleConfigurationError:  errors.New("NoSuchLifecycleConfiguration"),
	}
	m := &MinIO{svc: mockS3, bucket: "test-bucket"}
	policy := common.LifecyclePolicy{
		ID:        "policy1",
		Prefix:    "logs/",
		Retention: 24 * time.Hour,
		Action:    "delete",
	}
	err := m.AddPolicy(policy)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	err = m.RemovePolicy("policy1")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	policies, _ := m.GetPolicies()
	if len(policies) != 0 {
		t.Fatalf("expected 0 policies, got %d", len(policies))
	}
}

func TestMinIO_RemovePolicy_NonExistent(t *testing.T) {
	mockS3 := &mockS3Client{
		getBucketLifecycleConfigurationError: errors.New("NoSuchLifecycleConfiguration"),
	}
	m := &MinIO{svc: mockS3, bucket: "test-bucket"}
	err := m.RemovePolicy("nonexistent")
	if err != nil {
		t.Fatalf("expected no error for removing non-existent policy, got %v", err)
	}
}

func TestMinIO_GetPolicies_Empty(t *testing.T) {
	mockS3 := &mockS3Client{
		getBucketLifecycleConfigurationError: errors.New("NoSuchLifecycleConfiguration"),
	}
	m := &MinIO{svc: mockS3, bucket: "test-bucket"}
	policies, err := m.GetPolicies()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(policies) != 0 {
		t.Fatalf("expected 0 policies, got %d", len(policies))
	}
}

func TestMinIO_GetPolicies_Multiple(t *testing.T) {
	mockS3 := &mockS3Client{
		putBucketLifecycleConfigurationOutput: &s3.PutBucketLifecycleConfigurationOutput{},
		getBucketLifecycleConfigurationError:  errors.New("NoSuchLifecycleConfiguration"),
	}
	m := &MinIO{svc: mockS3, bucket: "test-bucket"}
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

	m.AddPolicy(policy1)
	m.AddPolicy(policy2)

	policies, err := m.GetPolicies()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(policies) != 2 {
		t.Fatalf("expected 2 policies, got %d", len(policies))
	}
}

func TestMinIO_List_EmptyPrefix(t *testing.T) {
	mockS3 := &mockS3Client{
		listObjectsV2Output: &s3.ListObjectsV2Output{
			Contents: []*s3.Object{
				{Key: aws.String("file1.txt")},
				{Key: aws.String("file2.txt")},
				{Key: aws.String("dir/file3.txt")},
			},
			IsTruncated: aws.Bool(false),
		},
	}

	m := &MinIO{svc: mockS3, bucket: "test-bucket"}

	keys, err := m.List("")
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

func TestMinIO_List_WithPrefix(t *testing.T) {
	mockS3 := &mockS3Client{
		listObjectsV2Output: &s3.ListObjectsV2Output{
			Contents: []*s3.Object{
				{Key: aws.String("logs/2023/file1.log")},
				{Key: aws.String("logs/2023/file2.log")},
				{Key: aws.String("logs/2024/file3.log")},
			},
			IsTruncated: aws.Bool(false),
		},
	}

	m := &MinIO{svc: mockS3, bucket: "test-bucket"}

	keys, err := m.List("logs/")
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

func TestMinIO_List_WithPagination(t *testing.T) {
	mockS3 := &mockS3Client{
		listObjectsV2Outputs: []*s3.ListObjectsV2Output{
			{
				Contents: []*s3.Object{
					{Key: aws.String("file1.txt")},
					{Key: aws.String("file2.txt")},
				},
				IsTruncated:           aws.Bool(true),
				NextContinuationToken: aws.String("token1"),
			},
			{
				Contents: []*s3.Object{
					{Key: aws.String("file3.txt")},
					{Key: aws.String("file4.txt")},
				},
				IsTruncated: aws.Bool(false),
			},
		},
	}

	m := &MinIO{svc: mockS3, bucket: "test-bucket"}

	keys, err := m.List("")
	if err != nil {
		t.Fatalf("Expected no error on List, got %v", err)
	}

	expected := []string{"file1.txt", "file2.txt", "file3.txt", "file4.txt"}
	if len(keys) != len(expected) {
		t.Fatalf("Expected %d keys, got %d", len(expected), len(keys))
	}

	sort.Strings(keys)
	sort.Strings(expected)

	for i, key := range keys {
		if key != expected[i] {
			t.Errorf("Expected key %s, got %s", expected[i], key)
		}
	}
}

func TestMinIO_List_Empty(t *testing.T) {
	mockS3 := &mockS3Client{
		listObjectsV2Output: &s3.ListObjectsV2Output{
			Contents:    []*s3.Object{},
			IsTruncated: aws.Bool(false),
		},
	}

	m := &MinIO{svc: mockS3, bucket: "test-bucket"}

	keys, err := m.List("nonexistent/")
	if err != nil {
		t.Fatalf("Expected no error on List, got %v", err)
	}

	if len(keys) != 0 {
		t.Errorf("Expected 0 keys, got %d", len(keys))
	}
}

func TestMinIO_List_Error(t *testing.T) {
	mockS3 := &mockS3Client{
		listObjectsV2Error: errors.New("list error"),
	}

	m := &MinIO{svc: mockS3, bucket: "test-bucket"}

	_, err := m.List("")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}
