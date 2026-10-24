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

package memory

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// Compile-time check that Memory satisfies the bucket capability.
var _ common.BucketManager = (*Memory)(nil)

// bucket is one emulated bucket: a child Memory that is reached through
// Bucket and owned by the instance that created it.
type bucket struct {
	store   *Memory
	created time.Time
}

// CreateBucket creates an empty bucket owned by this instance.
func (m *Memory) CreateBucket(ctx context.Context, name string) error {
	if err := common.ValidateBucketName(name); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.buckets[name]; ok {
		return fmt.Errorf("%w: %s", common.ErrBucketAlreadyExists, name)
	}
	if m.buckets == nil {
		m.buckets = make(map[string]*bucket)
	}
	child := &Memory{
		objects:          make(map[string]*object),
		lifecycleManager: NewLifecycleManager(),
	}
	child.multipartExpiry = m.multipartExpiry
	child.now = m.now
	m.buckets[name] = &bucket{store: child, created: m.clock()}
	return nil
}

// DeleteBucket removes an empty bucket. A bucket that holds objects or
// uploads in progress is not empty.
func (m *Memory) DeleteBucket(ctx context.Context, name string) error {
	if err := common.ValidateBucketName(name); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.buckets[name]
	if !ok {
		return fmt.Errorf("%w: %s", common.ErrBucketNotFound, name)
	}
	b.store.mu.RLock()
	empty := len(b.store.objects) == 0 && len(b.store.uploads) == 0 && len(b.store.buckets) == 0
	b.store.mu.RUnlock()
	if !empty {
		return fmt.Errorf("%w: %s", common.ErrBucketNotEmpty, name)
	}
	delete(m.buckets, name)
	return nil
}

// ListBuckets returns the buckets this instance created, sorted by name.
func (m *Memory) ListBuckets(ctx context.Context) ([]common.BucketInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]common.BucketInfo, 0, len(m.buckets))
	for name, b := range m.buckets {
		out = append(out, common.BucketInfo{Name: name, Created: b.created})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// Bucket returns the Storage bound to the named bucket.
func (m *Memory) Bucket(ctx context.Context, name string) (common.Storage, error) {
	if err := common.ValidateBucketName(name); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	m.mu.RLock()
	defer m.mu.RUnlock()
	b, ok := m.buckets[name]
	if !ok {
		return nil, fmt.Errorf("%w: %s", common.ErrBucketNotFound, name)
	}
	return b.store, nil
}
