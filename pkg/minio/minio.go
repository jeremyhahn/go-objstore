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

//nolint:gocritic,staticcheck // Style suggestions not critical for MinIO storage implementation

package minio

import (
	"io"
	"sync"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"

	"github.com/aws/aws-sdk-go/aws"                 //nolint:staticcheck // Using v1 SDK, migration to v2 planned
	"github.com/aws/aws-sdk-go/aws/credentials"    //nolint:staticcheck // Using v1 SDK, migration to v2 planned
	"github.com/aws/aws-sdk-go/aws/session"        //nolint:staticcheck // Using v1 SDK, migration to v2 planned
	"github.com/aws/aws-sdk-go/service/s3"         //nolint:staticcheck // Using v1 SDK, migration to v2 planned
	"github.com/aws/aws-sdk-go/service/s3/s3iface" //nolint:staticcheck // Using v1 SDK, migration to v2 planned
)

// Constants
const (
	actionDelete  = "delete"
	actionArchive = "archive"
)

// MinIO is a storage backend that stores files in MinIO object storage.
// MinIO is S3-compatible, so this implementation uses the AWS S3 SDK.
type MinIO struct {
	svc           s3iface.S3API
	bucket        string
	policiesMutex sync.RWMutex
}

// New creates a new MinIO storage backend.
func New() common.Storage {
	return &MinIO{}
}

// Configure sets up the backend with the necessary settings.
// Required settings:
//   - bucket: the MinIO bucket name
//   - endpoint: MinIO server endpoint (e.g., "http://localhost:9000")
//   - accessKey: MinIO access key
//   - secretKey: MinIO secret key
//
// Optional settings:
//   - region: AWS region (defaults to "us-east-1")
//   - useSSL: whether to use SSL (defaults to "false")
func (m *MinIO) Configure(settings map[string]string) error {
	m.bucket = settings["bucket"]
	if m.bucket == "" {
		return common.ErrBucketNotSet
	}

	endpoint := settings["endpoint"]
	if endpoint == "" {
		return common.ErrEndpointNotSet
	}

	accessKey := settings["accessKey"]
	if accessKey == "" {
		return common.ErrAccessKeyNotSet
	}

	secretKey := settings["secretKey"]
	if secretKey == "" {
		return common.ErrSecretKeyNotSet
	}

	region := settings["region"]
	if region == "" {
		region = "us-east-1"
	}

	cfg := &aws.Config{
		Region:           aws.String(region),
		Endpoint:         aws.String(endpoint),
		S3ForcePathStyle: aws.Bool(true), // MinIO requires path-style addressing
		Credentials:      credentials.NewStaticCredentials(accessKey, secretKey, ""),
	}

	sess, err := session.NewSession(cfg)
	if err != nil {
		return err
	}

	m.svc = s3.New(sess)
	return nil
}

// Put stores an object in the backend.
func (m *MinIO) Put(key string, data io.Reader) error {
	_, err := m.svc.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(m.bucket),
		Key:    aws.String(key),
		Body:   aws.ReadSeekCloser(data),
	})
	return err
}

// Get retrieves an object from the backend.
func (m *MinIO) Get(key string) (io.ReadCloser, error) {
	result, err := m.svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(m.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	return result.Body, nil
}

// Delete removes an object from the backend.
func (m *MinIO) Delete(key string) error {
	_, err := m.svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(m.bucket),
		Key:    aws.String(key),
	})
	return err
}

// List returns a list of keys that start with the given prefix.
func (m *MinIO) List(prefix string) ([]string, error) {
	// Pre-allocate with reasonable capacity to reduce allocations
	keys := make([]string, 0, 100)
	var continuationToken *string

	for {
		input := &s3.ListObjectsV2Input{
			Bucket: aws.String(m.bucket),
			Prefix: aws.String(prefix),
		}

		if continuationToken != nil {
			input.ContinuationToken = continuationToken
		}

		result, err := m.svc.ListObjectsV2(input)
		if err != nil {
			return nil, err
		}

		for _, obj := range result.Contents {
			if obj.Key != nil {
				keys = append(keys, *obj.Key)
			}
		}

		if !aws.BoolValue(result.IsTruncated) {
			break
		}

		continuationToken = result.NextContinuationToken
	}

	return keys, nil
}

// Archive copies an object to another backend for archival.
func (m *MinIO) Archive(key string, destination common.Archiver) error {
	rc, err := m.Get(key)
	if err != nil {
		return err
	}
	defer func() { _ = rc.Close() }()

	return destination.Put(key, rc)
}

// AddPolicy adds a new lifecycle policy by configuring MinIO bucket lifecycle rules.
// MinIO supports S3-compatible lifecycle configuration.
func (m *MinIO) AddPolicy(policy common.LifecyclePolicy) error {
	if policy.ID == "" {
		return common.ErrInvalidPolicy
	}
	if policy.Action != actionDelete && policy.Action != actionArchive {
		return common.ErrInvalidPolicy
	}

	m.policiesMutex.Lock()
	defer m.policiesMutex.Unlock()

	// Get existing lifecycle configuration
	existingConfig, err := m.svc.GetBucketLifecycleConfiguration(&s3.GetBucketLifecycleConfigurationInput{
		Bucket: aws.String(m.bucket),
	})

	var rules []*s3.LifecycleRule
	if err != nil {
		// If no lifecycle configuration exists, start with empty rules
		if !isNoSuchLifecycleConfiguration(err) {
			return err
		}
		rules = []*s3.LifecycleRule{}
	} else {
		// Remove existing rule with same ID if it exists
		for _, rule := range existingConfig.Rules {
			if rule.ID != nil && *rule.ID != policy.ID {
				rules = append(rules, rule)
			}
		}
	}

	// Convert retention duration to days (minimum 1 day)
	days := int64(policy.Retention.Hours() / 24)
	if days < 1 {
		days = 1
	}

	// Create new lifecycle rule based on action
	rule := &s3.LifecycleRule{
		ID:     aws.String(policy.ID),
		Status: aws.String("Enabled"),
		Filter: &s3.LifecycleRuleFilter{
			Prefix: aws.String(policy.Prefix),
		},
	}

	if policy.Action == "delete" {
		rule.Expiration = &s3.LifecycleExpiration{
			Days: aws.Int64(days),
		}
	} else if policy.Action == "archive" {
		// MinIO supports transitions to different storage classes
		rule.Transitions = []*s3.Transition{
			{
				Days:         aws.Int64(days),
				StorageClass: aws.String(s3.TransitionStorageClassGlacier),
			},
		}
	}

	rules = append(rules, rule)

	// Put the updated lifecycle configuration
	_, err = m.svc.PutBucketLifecycleConfiguration(&s3.PutBucketLifecycleConfigurationInput{
		Bucket: aws.String(m.bucket),
		LifecycleConfiguration: &s3.BucketLifecycleConfiguration{
			Rules: rules,
		},
	})

	return err
}

// RemovePolicy removes a lifecycle policy by updating MinIO bucket lifecycle rules.
func (m *MinIO) RemovePolicy(id string) error {
	m.policiesMutex.Lock()
	defer m.policiesMutex.Unlock()

	// Get existing lifecycle configuration
	existingConfig, err := m.svc.GetBucketLifecycleConfiguration(&s3.GetBucketLifecycleConfigurationInput{
		Bucket: aws.String(m.bucket),
	})

	if err != nil {
		// If no lifecycle configuration exists, nothing to remove
		if isNoSuchLifecycleConfiguration(err) {
			return nil
		}
		return err
	}

	// Filter out the rule with the given ID
	var rules []*s3.LifecycleRule
	for _, rule := range existingConfig.Rules {
		if rule.ID != nil && *rule.ID != id {
			rules = append(rules, rule)
		}
	}

	// If no rules left, delete the lifecycle configuration entirely
	if len(rules) == 0 {
		_, err = m.svc.DeleteBucketLifecycle(&s3.DeleteBucketLifecycleInput{
			Bucket: aws.String(m.bucket),
		})
		return err
	}

	// Otherwise, put the updated configuration
	_, err = m.svc.PutBucketLifecycleConfiguration(&s3.PutBucketLifecycleConfigurationInput{
		Bucket: aws.String(m.bucket),
		LifecycleConfiguration: &s3.BucketLifecycleConfiguration{
			Rules: rules,
		},
	})

	return err
}

// GetPolicies returns all lifecycle policies by fetching MinIO bucket lifecycle rules.
func (m *MinIO) GetPolicies() ([]common.LifecyclePolicy, error) {
	m.policiesMutex.RLock()
	defer m.policiesMutex.RUnlock()

	// Get lifecycle configuration from MinIO
	lifecycleConfig, err := m.svc.GetBucketLifecycleConfiguration(&s3.GetBucketLifecycleConfigurationInput{
		Bucket: aws.String(m.bucket),
	})

	if err != nil {
		// If no lifecycle configuration exists, return empty list
		if isNoSuchLifecycleConfiguration(err) {
			return []common.LifecyclePolicy{}, nil
		}
		return nil, err
	}

	// Convert S3-compatible lifecycle rules to common.LifecyclePolicy
	policies := make([]common.LifecyclePolicy, 0, len(lifecycleConfig.Rules))
	for _, rule := range lifecycleConfig.Rules {
		if rule.ID == nil || rule.Status == nil || *rule.Status != "Enabled" {
			continue
		}

		policy := common.LifecyclePolicy{
			ID: *rule.ID,
		}

		// Extract prefix from filter
		if rule.Filter != nil && rule.Filter.Prefix != nil {
			policy.Prefix = *rule.Filter.Prefix
		}

		// Determine action and retention based on rule configuration
		if rule.Expiration != nil && rule.Expiration.Days != nil {
			policy.Action = "delete"
			policy.Retention = time.Duration(*rule.Expiration.Days) * 24 * time.Hour
		} else if len(rule.Transitions) > 0 && rule.Transitions[0].Days != nil {
			// Use the first transition for archive action
			policy.Action = "archive"
			policy.Retention = time.Duration(*rule.Transitions[0].Days) * 24 * time.Hour
		} else {
			// Skip rules we don't understand
			continue
		}

		policies = append(policies, policy)
	}

	return policies, nil
}

// isNoSuchLifecycleConfiguration checks if the error indicates no lifecycle configuration exists.
func isNoSuchLifecycleConfiguration(err error) bool {
	if err == nil {
		return false
	}
	// AWS SDK returns "NoSuchLifecycleConfiguration" error code
	return err.Error() == "NoSuchLifecycleConfiguration" ||
		   (len(err.Error()) > 0 && err.Error()[:28] == "NoSuchLifecycleConfiguration")
}
