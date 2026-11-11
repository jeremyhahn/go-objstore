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

//nolint:gocritic,staticcheck // Style suggestions not critical for S3 storage implementation

package s3

import (
	"context"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/jeremyhahn/go-objstore/internal/multipart"
	"github.com/jeremyhahn/go-objstore/pkg/common"

	"github.com/aws/aws-sdk-go/aws"                  //nolint:staticcheck // Using v1 SDK, migration to v2 planned
	"github.com/aws/aws-sdk-go/aws/credentials"      //nolint:staticcheck // Using v1 SDK, migration to v2 planned
	"github.com/aws/aws-sdk-go/aws/session"          //nolint:staticcheck // Using v1 SDK, migration to v2 planned
	"github.com/aws/aws-sdk-go/service/s3"           //nolint:staticcheck // Using v1 SDK, migration to v2 planned
	"github.com/aws/aws-sdk-go/service/s3/s3iface"   //nolint:staticcheck // Using v1 SDK, migration to v2 planned
	"github.com/aws/aws-sdk-go/service/s3/s3manager" //nolint:staticcheck // Using v1 SDK, migration to v2 planned
)

// Constants
const (
	actionDelete  = "delete"
	actionArchive = "archive"
)

type s3Uploader interface {
	Upload(input *s3manager.UploadInput, options ...func(*s3manager.Uploader)) (*s3manager.UploadOutput, error)
}

var s3managerNewUploaderWithClient = func(c s3iface.S3API, options ...func(*s3manager.Uploader)) s3Uploader {
	return s3manager.NewUploaderWithClient(c, options...)
}

// S3 is a storage backend that stores files in Amazon S3.
type S3 struct {
	svc                s3iface.S3API
	bucket             string
	region             string
	multipartExpiry    time.Duration // zero means multipart.DefaultExpiry
	policiesMutex      sync.RWMutex
	replicationManager common.ReplicationManager
}

// New creates a new S3 storage backend.
func New() common.Storage {
	return &S3{}
}

// Configure sets up the backend with the necessary settings. The access key
// and secret key come from creds, never from settings; with a zero creds the
// SDK's default credential chain applies.
func (s *S3) Configure(settings map[string]string, creds common.Credentials) error {
	if err := common.RejectCredentialSettings(settings); err != nil {
		return err
	}
	s.bucket = settings["bucket"]
	if s.bucket == "" {
		return common.ErrBucketNotSet
	}
	return s.configureClient(settings, creds)
}

// NewManager returns an S3 that manages buckets and has no bucket of its own,
// so the bucket setting is not required. Bucket returns the Storage of a
// bucket that reuses the manager's client and credentials.
func NewManager(settings map[string]string, creds common.Credentials) (*S3, error) {
	if err := common.RejectCredentialSettings(settings); err != nil {
		return nil, err
	}
	s := &S3{}
	if err := s.configureClient(settings, creds); err != nil {
		return nil, err
	}
	return s, nil
}

// configureClient builds the client from the region and endpoint settings and
// the credential handles, and reads the multipartExpiry setting. The handles
// are opened only to build the SDK credential and are not retained.
func (s *S3) configureClient(settings map[string]string, creds common.Credentials) error {
	expiry, err := multipart.ParseExpiry(settings)
	if err != nil {
		return err
	}
	s.region = settings["region"]
	s.multipartExpiry = expiry

	cfg := &aws.Config{
		Region: aws.String(settings["region"]),
	}
	if ep := settings["endpoint"]; ep != "" {
		cfg.Endpoint = aws.String(ep)
		cfg.S3ForcePathStyle = aws.Bool(settings["forcePathStyle"] == "true")
	}
	if !creds.IsZero() {
		if creds.Key == nil {
			return common.ErrAccessKeyNotSet
		}
		if creds.Secret == nil {
			return common.ErrSecretKeyNotSet
		}
		err := creds.Open(func(key, secret []byte) error {
			cfg.Credentials = credentials.NewStaticCredentials(string(key), string(secret), "")
			return nil
		})
		if err != nil {
			return err
		}
	}

	sess, err := session.NewSession(cfg)
	if err != nil {
		return err
	}

	s.svc = s3.New(sess)
	return nil
}

// Put stores an object in the backend.
func (s *S3) Put(key string, data io.Reader) error {
	if err := common.ValidateKey(key); err != nil {
		return err
	}
	_, err := s.svc.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   aws.ReadSeekCloser(data),
	})
	return err
}

// Get retrieves an object from the backend. A missing key returns an error
// that wraps common.ErrKeyNotFound.
func (s *S3) Get(key string) (io.ReadCloser, error) {
	if err := common.ValidateKey(key); err != nil {
		return nil, err
	}
	result, err := s.svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, objectError(err, key)
	}
	return result.Body, nil
}

// Delete removes an object from the backend.
func (s *S3) Delete(key string) error {
	if err := common.ValidateKey(key); err != nil {
		return err
	}
	_, err := s.svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	return err
}

// List returns a list of keys that start with the given prefix.
func (s *S3) List(prefix string) ([]string, error) {
	// Pre-allocate with reasonable capacity to reduce allocations
	keys := make([]string, 0, 100)
	var continuationToken *string

	for {
		input := &s3.ListObjectsV2Input{
			Bucket: aws.String(s.bucket),
			Prefix: aws.String(prefix),
		}

		if continuationToken != nil {
			input.ContinuationToken = continuationToken
		}

		result, err := s.svc.ListObjectsV2(input)
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
func (s *S3) Archive(key string, destination common.Archiver) error {
	if destination == nil {
		return common.ErrArchiveDestinationNil
	}
	// The archived copy keeps the object's client-side encryption metadata
	// (or the archive is refused when the destination cannot store it).
	ctx := context.TODO()
	rc, err := s.Get(key)
	if err != nil {
		return err
	}
	defer func() { _ = rc.Close() }()
	md, err := s.GetMetadata(ctx, key)
	if err != nil {
		return err
	}

	return common.ArchiveCopy(ctx, key, rc, md, destination)
}

// AddPolicy adds a new lifecycle policy by configuring S3 bucket lifecycle rules.
func (s *S3) AddPolicy(policy common.LifecyclePolicy) error {
	if policy.ID == "" {
		return common.ErrInvalidPolicy
	}
	if policy.Action != actionDelete && policy.Action != actionArchive {
		return common.ErrInvalidPolicy
	}

	s.policiesMutex.Lock()
	defer s.policiesMutex.Unlock()

	// Get existing lifecycle configuration
	existingConfig, err := s.svc.GetBucketLifecycleConfiguration(&s3.GetBucketLifecycleConfigurationInput{
		Bucket: aws.String(s.bucket),
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
		rule.Transitions = []*s3.Transition{
			{
				Days:         aws.Int64(days),
				StorageClass: aws.String(s3.TransitionStorageClassGlacier),
			},
		}
	}

	rules = append(rules, rule)

	// Put the updated lifecycle configuration
	_, err = s.svc.PutBucketLifecycleConfiguration(&s3.PutBucketLifecycleConfigurationInput{
		Bucket: aws.String(s.bucket),
		LifecycleConfiguration: &s3.BucketLifecycleConfiguration{
			Rules: rules,
		},
	})

	return err
}

// RemovePolicy removes a lifecycle policy by updating S3 bucket lifecycle rules.
func (s *S3) RemovePolicy(id string) error {
	s.policiesMutex.Lock()
	defer s.policiesMutex.Unlock()

	// Get existing lifecycle configuration
	existingConfig, err := s.svc.GetBucketLifecycleConfiguration(&s3.GetBucketLifecycleConfigurationInput{
		Bucket: aws.String(s.bucket),
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
		_, err = s.svc.DeleteBucketLifecycle(&s3.DeleteBucketLifecycleInput{
			Bucket: aws.String(s.bucket),
		})
		return err
	}

	// Otherwise, put the updated configuration
	_, err = s.svc.PutBucketLifecycleConfiguration(&s3.PutBucketLifecycleConfigurationInput{
		Bucket: aws.String(s.bucket),
		LifecycleConfiguration: &s3.BucketLifecycleConfiguration{
			Rules: rules,
		},
	})

	return err
}

// GetPolicies returns all lifecycle policies by fetching S3 bucket lifecycle rules.
func (s *S3) GetPolicies() ([]common.LifecyclePolicy, error) {
	s.policiesMutex.RLock()
	defer s.policiesMutex.RUnlock()

	// Get lifecycle configuration from S3
	lifecycleConfig, err := s.svc.GetBucketLifecycleConfiguration(&s3.GetBucketLifecycleConfigurationInput{
		Bucket: aws.String(s.bucket),
	})

	if err != nil {
		// If no lifecycle configuration exists, return empty list
		if isNoSuchLifecycleConfiguration(err) {
			return []common.LifecyclePolicy{}, nil
		}
		return nil, err
	}

	// Convert S3 lifecycle rules to common.LifecyclePolicy
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
	// AWS SDK returns "NoSuchLifecycleConfiguration" as the error code or as a prefix.
	return strings.HasPrefix(err.Error(), "NoSuchLifecycleConfiguration")
}

// GetReplicationManager returns the replication manager for this backend.
// This method implements the common.ReplicationCapable interface.
func (s *S3) GetReplicationManager() (common.ReplicationManager, error) {
	if s.replicationManager == nil {
		return nil, common.ErrReplicationNotSupported
	}
	return s.replicationManager, nil
}

// SetReplicationManager allows manually setting a replication manager.
// This is useful for testing or when you want to share a replication manager
// across multiple backends.
func (s *S3) SetReplicationManager(rm common.ReplicationManager) {
	s.replicationManager = rm
}
