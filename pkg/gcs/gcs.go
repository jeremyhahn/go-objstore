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

//go:build gcpstorage

package gcs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

// Constants
const (
	actionDelete = "delete"
)

const (
	lifecycleActionArchive = "archive"
)

// Small internal interfaces to enable unit tests without real GCS.
type gcsObject interface {
	NewWriter(ctx context.Context) io.WriteCloser
	NewReader(ctx context.Context) (io.ReadCloser, error)
	Delete(ctx context.Context) error
	Attrs(ctx context.Context) (*storage.ObjectAttrs, error)
	Update(ctx context.Context, uattrs storage.ObjectAttrsToUpdate) (*storage.ObjectAttrs, error)
}

type gcsBucket interface {
	Object(name string) gcsObject
	Objects(ctx context.Context, query *storage.Query) gcsIterator
	Attrs(ctx context.Context) (*storage.BucketAttrs, error)
	Update(ctx context.Context, uattrs storage.BucketAttrsToUpdate) (*storage.BucketAttrs, error)
}

type gcsIterator interface {
	Next() (*storage.ObjectAttrs, error)
}

type gcsClient interface {
	Bucket(name string) gcsBucket
}

type clientWrapper struct{ *storage.Client }
type bucketWrapper struct{ *storage.BucketHandle }
type objectWrapper struct{ *storage.ObjectHandle }
type iteratorWrapper struct{ *storage.ObjectIterator }

func (c clientWrapper) Bucket(name string) gcsBucket { return bucketWrapper{c.Client.Bucket(name)} }
func (b bucketWrapper) Object(name string) gcsObject {
	return objectWrapper{b.BucketHandle.Object(name)}
}
func (b bucketWrapper) Objects(ctx context.Context, query *storage.Query) gcsIterator {
	return iteratorWrapper{b.BucketHandle.Objects(ctx, query)}
}
func (b bucketWrapper) Attrs(ctx context.Context) (*storage.BucketAttrs, error) {
	return gcsGetBucketAttrsFn(ctx, b.BucketHandle)
}
func (b bucketWrapper) Update(ctx context.Context, uattrs storage.BucketAttrsToUpdate) (*storage.BucketAttrs, error) {
	return gcsUpdateBucketFn(ctx, b.BucketHandle, uattrs)
}
func (i iteratorWrapper) Next() (*storage.ObjectAttrs, error) {
	return i.ObjectIterator.Next()
}

// Function variables to enable unit testing without real network I/O.
var (
	gcsNewWriterFn    = func(o *storage.ObjectHandle, ctx context.Context) io.WriteCloser { w := o.NewWriter(ctx); return w }
	gcsNewReaderFn    = func(o *storage.ObjectHandle, ctx context.Context) (io.ReadCloser, error) { return o.NewReader(ctx) }
	gcsDeleteFn       = func(o *storage.ObjectHandle, ctx context.Context) error { return o.Delete(ctx) }
	gcsAttrsFn        = func(o *storage.ObjectHandle, ctx context.Context) (*storage.ObjectAttrs, error) { return o.Attrs(ctx) }
	gcsUpdateObjectFn = func(o *storage.ObjectHandle, ctx context.Context, uattrs storage.ObjectAttrsToUpdate) (*storage.ObjectAttrs, error) {
		return o.Update(ctx, uattrs)
	}
	gcsGetBucketAttrsFn = func(ctx context.Context, b *storage.BucketHandle) (*storage.BucketAttrs, error) { return b.Attrs(ctx) }
	gcsUpdateBucketFn   = func(ctx context.Context, b *storage.BucketHandle, uattrs storage.BucketAttrsToUpdate) (*storage.BucketAttrs, error) {
		return b.Update(ctx, uattrs)
	}
)

func (o objectWrapper) NewWriter(ctx context.Context) io.WriteCloser {
	return gcsNewWriterFn(o.ObjectHandle, ctx)
}
func (o objectWrapper) NewReader(ctx context.Context) (io.ReadCloser, error) {
	return gcsNewReaderFn(o.ObjectHandle, ctx)
}
func (o objectWrapper) Delete(ctx context.Context) error { return gcsDeleteFn(o.ObjectHandle, ctx) }
func (o objectWrapper) Attrs(ctx context.Context) (*storage.ObjectAttrs, error) {
	return gcsAttrsFn(o.ObjectHandle, ctx)
}
func (o objectWrapper) Update(ctx context.Context, uattrs storage.ObjectAttrsToUpdate) (*storage.ObjectAttrs, error) {
	return gcsUpdateObjectFn(o.ObjectHandle, ctx, uattrs)
}

// GCS is a storage backend that stores files in Google Cloud Storage.
type GCS struct {
	client             gcsClient
	bucket             string
	policiesMutex      sync.RWMutex
	replicationManager common.ReplicationManager
}

var gcsNewClient = func(ctx context.Context) (*storage.Client, error) { return storage.NewClient(ctx) }

// New creates a new GCS storage backend.
func New() common.Storage {
	return &GCS{}
}

// Configure sets up the backend with the necessary settings.
func (g *GCS) Configure(settings map[string]string) error {
	g.bucket = settings["bucket"]
	if g.bucket == "" {
		return common.ErrBucketNotSet
	}
	if g.client != nil {
		return nil
	}
	// Allow skipping client creation for testing
	if settings["skip_client"] == "true" {
		return nil
	}
	ctx := context.Background()
	client, err := gcsNewClient(ctx)
	if err != nil {
		return err
	}
	g.client = clientWrapper{client}
	return nil
}

// Put stores an object in the backend.
func (g *GCS) Put(key string, data io.Reader) error {
	if err := common.ValidateKey(key); err != nil {
		return err
	}
	w := g.client.Bucket(g.bucket).Object(key).NewWriter(context.Background())
	if _, err := io.Copy(w, data); err != nil {
		// Close the writer to release resources; ignore the close error since
		// the copy error is the primary failure.
		_ = w.Close()
		return err
	}
	return w.Close()
}

// Get retrieves an object from the backend.
func (g *GCS) Get(key string) (io.ReadCloser, error) {
	if err := common.ValidateKey(key); err != nil {
		return nil, err
	}
	return g.client.Bucket(g.bucket).Object(key).NewReader(context.Background())
}

// Delete removes an object from the backend.
func (g *GCS) Delete(key string) error {
	if err := common.ValidateKey(key); err != nil {
		return err
	}
	return g.client.Bucket(g.bucket).Object(key).Delete(context.Background())
}

// List returns a list of keys that start with the given prefix.
func (g *GCS) List(prefix string) ([]string, error) {
	// Pre-allocate with reasonable capacity to reduce allocations
	keys := make([]string, 0, 100)
	ctx := context.Background()

	query := &storage.Query{
		Prefix: prefix,
	}

	it := g.client.Bucket(g.bucket).Objects(ctx, query)

	for {
		attrs, err := it.Next()
		if err == iterator.Done { //nolint:err113 // iterator.Done is the standard sentinel error for GCS iterators
			break
		}
		if err != nil {
			return nil, err
		}

		keys = append(keys, attrs.Name)
	}

	return keys, nil
}

func (g *GCS) Archive(key string, destination common.Archiver) error {
	rc, err := g.Get(key)
	if err != nil {
		return err
	}
	defer func() { _ = rc.Close() }()

	// Buffer the data to ensure compatibility with destinations that require Content-Length
	data, err := io.ReadAll(rc)
	if err != nil {
		return err
	}

	return destination.Put(key, bytes.NewReader(data))
}

// AddPolicy adds a new lifecycle policy by configuring GCS bucket lifecycle rules.
func (g *GCS) AddPolicy(policy common.LifecyclePolicy) error {
	if policy.ID == "" {
		return common.ErrInvalidPolicy
	}
	if policy.Action != actionDelete && policy.Action != lifecycleActionArchive {
		return common.ErrInvalidPolicy
	}

	g.policiesMutex.Lock()
	defer g.policiesMutex.Unlock()

	ctx := context.Background()
	bucket := g.client.Bucket(g.bucket)

	// Get current bucket attributes
	attrs, err := bucket.Attrs(ctx)
	if err != nil {
		return err
	}

	// Copy existing lifecycle rules, removing any with the same ID
	var rules []storage.LifecycleRule
	if attrs.Lifecycle.Rules != nil {
		for i := range attrs.Lifecycle.Rules {
			rule := &attrs.Lifecycle.Rules[i]
			// Skip rules with the same name/condition prefix (our policy ID goes in condition)
			if len(rule.Condition.MatchesPrefix) > 0 &&
				rule.Condition.MatchesPrefix[0] == policy.Prefix &&
				rule.Condition.AgeInDays == int64(policy.Retention.Hours()/24) {
				continue
			}
			rules = append(rules, *rule)
		}
	}

	// Convert retention duration to days (minimum 1 day)
	days := int64(policy.Retention.Hours() / 24)
	if days < 1 {
		days = 1
	}

	// Create new lifecycle rule based on action
	newRule := storage.LifecycleRule{
		Condition: storage.LifecycleCondition{
			AgeInDays:     days,
			MatchesPrefix: []string{policy.Prefix},
		},
	}

	switch policy.Action {
	case "delete":
		newRule.Action = storage.LifecycleAction{
			Type: storage.DeleteAction,
		}
	case lifecycleActionArchive:
		// Transition to Archive storage class
		newRule.Action = storage.LifecycleAction{
			Type:         storage.SetStorageClassAction,
			StorageClass: "ARCHIVE",
		}
	}

	rules = append(rules, newRule)

	// Update the bucket with the new lifecycle configuration
	_, err = bucket.Update(ctx, storage.BucketAttrsToUpdate{
		Lifecycle: &storage.Lifecycle{
			Rules: rules,
		},
	})

	return err
}

// RemovePolicy removes a lifecycle policy by updating GCS bucket lifecycle rules.
// GCS lifecycle rules carry no identifiers, so the id must follow the
// "rule-<index>" scheme that GetPolicies uses to synthesize policy IDs. The
// identified rule's translated condition (prefix + age, the same condition
// AddPolicy writes) determines which rule(s) are removed. An id that does not
// resolve to an existing rule yields an error wrapping common.ErrPolicyNotFound.
func (g *GCS) RemovePolicy(id string) error {
	g.policiesMutex.Lock()
	defer g.policiesMutex.Unlock()

	ctx := context.Background()
	bucket := g.client.Bucket(g.bucket)

	// Get current bucket attributes
	attrs, err := bucket.Attrs(ctx)
	if err != nil {
		return err
	}

	// Resolve the id to a rule using the same "rule-<index>" scheme as GetPolicies.
	index := -1
	for i := range attrs.Lifecycle.Rules {
		if fmt.Sprintf("rule-%d", i) == id {
			index = i
			break
		}
	}
	if index < 0 {
		return fmt.Errorf("%w: %s", common.ErrPolicyNotFound, id)
	}

	// Remove every rule matching the resolved rule's condition (prefix + age),
	// mirroring the condition AddPolicy uses to translate a policy into a rule.
	target := &attrs.Lifecycle.Rules[index]
	prefix := rulePrefix(target)
	age := target.Condition.AgeInDays

	rules := make([]storage.LifecycleRule, 0, len(attrs.Lifecycle.Rules))
	for i := range attrs.Lifecycle.Rules {
		rule := &attrs.Lifecycle.Rules[i]
		if rulePrefix(rule) == prefix && rule.Condition.AgeInDays == age {
			continue
		}
		rules = append(rules, *rule)
	}

	_, err = bucket.Update(ctx, storage.BucketAttrsToUpdate{
		Lifecycle: &storage.Lifecycle{
			Rules: rules,
		},
	})

	return err
}

// rulePrefix returns the first prefix of a lifecycle rule condition, or the
// empty string when the rule has no prefix condition.
func rulePrefix(rule *storage.LifecycleRule) string {
	if len(rule.Condition.MatchesPrefix) > 0 {
		return rule.Condition.MatchesPrefix[0]
	}
	return ""
}

// GetPolicies returns all lifecycle policies by fetching GCS bucket lifecycle rules.
func (g *GCS) GetPolicies() ([]common.LifecyclePolicy, error) {
	g.policiesMutex.RLock()
	defer g.policiesMutex.RUnlock()

	ctx := context.Background()
	bucket := g.client.Bucket(g.bucket)

	// Get bucket attributes including lifecycle rules
	attrs, err := bucket.Attrs(ctx)
	if err != nil {
		return nil, err
	}

	// If no lifecycle rules exist, return empty list
	if attrs.Lifecycle.Rules == nil {
		return []common.LifecyclePolicy{}, nil
	}

	// Convert GCS lifecycle rules to common.LifecyclePolicy
	policies := make([]common.LifecyclePolicy, 0, len(attrs.Lifecycle.Rules))
	for i := range attrs.Lifecycle.Rules {
		rule := &attrs.Lifecycle.Rules[i]
		policy := common.LifecyclePolicy{
			// GCS doesn't have rule IDs, so we use index as ID
			ID: fmt.Sprintf("rule-%d", i),
		}

		// Extract prefix from condition
		if len(rule.Condition.MatchesPrefix) > 0 {
			policy.Prefix = rule.Condition.MatchesPrefix[0]
		}

		// Extract retention from age condition
		if rule.Condition.AgeInDays > 0 {
			policy.Retention = time.Duration(rule.Condition.AgeInDays) * 24 * time.Hour
		}

		// Determine action
		switch rule.Action.Type {
		case storage.DeleteAction:
			policy.Action = "delete"
		case storage.SetStorageClassAction:
			policy.Action = lifecycleActionArchive
		default:
			// Skip rules we don't understand
			continue
		}

		policies = append(policies, policy)
	}

	return policies, nil
}

// GetReplicationManager returns the replication manager for this backend.
// This method implements the common.ReplicationCapable interface.
func (g *GCS) GetReplicationManager() (common.ReplicationManager, error) {
	if g.replicationManager == nil {
		return nil, common.ErrReplicationNotSupported
	}
	return g.replicationManager, nil
}

// SetReplicationManager allows manually setting a replication manager.
// This is useful for testing or when you want to share a replication manager
// across multiple backends.
func (g *GCS) SetReplicationManager(rm common.ReplicationManager) {
	g.replicationManager = rm
}
