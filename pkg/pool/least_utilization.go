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

package pool

import (
	"context"
	"encoding/binary"
	"sync"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// leastUtilizationStrategyName is the canonical name returned by Name.
const leastUtilizationStrategyName = "least_utilization"

// loadKeySuffix is appended to the pool namespace to form the StateStore
// key holding the per-candidate load tallies. The full key is
// "pool/<poolName>/least_utilization/loads".
const loadKeySuffix = "/" + leastUtilizationStrategyName + "/loads"

// LeastUtilizationStrategy picks the candidate carrying the lowest
// cumulative assigned load. Load is tracked per candidate index in the
// caller-supplied StateStore: every Pick adds the caller's
// Hint.SizeHint (or 1 when no size hint is given) to the chosen
// candidate's tally, so placement converges on the backend that has
// been assigned the fewest bytes. Ties break toward the lowest
// candidate index, making selection deterministic.
//
// Like the round-robin cursor, the tallies are namespaced by pool name
// and keyed by candidate index, so candidate order must stay stable
// across restarts for the persisted state to remain meaningful. If the
// candidate count changes, the tallies reset to zero.
//
// A single LeastUtilizationStrategy value can serve multiple pools; the
// pool name is read from the context the Manager threads in via Pick.
type LeastUtilizationStrategy struct {
	// State is the persistent load store. Required — Pick returns a
	// MissingStateStoreError if it is nil.
	State StateStore

	// mu serializes concurrent Pick calls so read-modify-write of the
	// load tallies is atomic with respect to the StateStore round trip.
	mu sync.Mutex
}

// Pick implements Strategy.
//
// Algorithm:
//  1. Read the per-candidate load tallies from the StateStore (all zero
//     if missing or if the candidate count changed).
//  2. Select the candidate with the lowest tally; ties go to the lowest
//     index.
//  3. Add max(hint.SizeHint, 1) to the selected candidate's tally and
//     persist.
func (s *LeastUtilizationStrategy) Pick(ctx context.Context, candidates []common.Storage, hint Hint) (common.Storage, error) {
	if s.State == nil {
		return nil, &MissingStateStoreError{Strategy: leastUtilizationStrategyName}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(candidates) == 0 {
		return nil, &EmptyPoolError{Name: poolNameFromContext(ctx)}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	key := loadKey(poolNameFromContext(ctx))

	loads, err := s.loadTallies(ctx, key, len(candidates))
	if err != nil {
		return nil, err
	}

	idx := 0
	for i := 1; i < len(loads); i++ {
		if loads[i] < loads[idx] {
			idx = i
		}
	}

	weight := uint64(1)
	if hint.SizeHint > 0 {
		weight = uint64(hint.SizeHint)
	}
	loads[idx] += weight

	if err := s.storeTallies(ctx, key, loads); err != nil {
		return nil, err
	}

	return candidates[idx], nil
}

// Name implements Strategy.
func (s *LeastUtilizationStrategy) Name() string {
	return leastUtilizationStrategyName
}

// loadTallies reads the per-candidate load tallies from the state store.
// A missing key, or a value whose length does not match the current
// candidate count (the pool was resized, or the value is corrupt),
// yields all-zero tallies that are overwritten on the next store.
func (s *LeastUtilizationStrategy) loadTallies(ctx context.Context, key string, n int) ([]uint64, error) {
	loads := make([]uint64, n)

	value, found, err := s.State.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if !found || len(value) != 8*n {
		return loads, nil
	}

	for i := range loads {
		loads[i] = binary.BigEndian.Uint64(value[8*i:])
	}
	return loads, nil
}

// storeTallies encodes and persists the per-candidate load tallies.
func (s *LeastUtilizationStrategy) storeTallies(ctx context.Context, key string, loads []uint64) error {
	buf := make([]byte, 8*len(loads))
	for i, load := range loads {
		binary.BigEndian.PutUint64(buf[8*i:], load)
	}
	return s.State.Put(ctx, key, buf)
}

// loadKey returns the StateStore key for a given pool's load tallies.
// The pool name is included so multiple pools sharing the same
// StateStore don't trample each other.
func loadKey(poolName string) string {
	if poolName == "" {
		// Fallback when called outside a Manager (e.g. direct unit
		// tests on the strategy). Real Manager-driven calls always
		// supply a name.
		poolName = "_unbound_"
	}
	return cursorKeyPrefix + poolName + loadKeySuffix
}
