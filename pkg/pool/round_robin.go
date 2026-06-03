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

// roundRobinStrategyName is the canonical name returned by Name and used
// in the cursor key.
const roundRobinStrategyName = "round_robin"

// cursorKeyPrefix namespaces the round-robin cursor inside the
// caller-supplied StateStore. The full key is
// "pool/<poolName>/round_robin/cursor". Keeping it stable across
// versions is important for cursor compatibility on upgrade.
const cursorKeyPrefix = "pool/"

// RoundRobinStrategy returns candidates in rotation. The cursor is
// namespaced by pool name and persisted via the supplied StateStore so
// consecutive picks across daemon restarts continue advancing instead of
// resetting.
//
// A single RoundRobinStrategy value can serve multiple pools; the pool
// name is read from the context the Manager threads in via Pick.
type RoundRobinStrategy struct {
	// State is the persistent cursor store. Required — Pick returns a
	// MissingStateStoreError if it is nil. Callers can use a tiny
	// in-memory map for tests; production callers (qrdb) plug in a
	// system-tenant-backed KV store.
	State StateStore

	// mu serializes concurrent Pick calls so increment-and-store is
	// atomic with respect to the StateStore round trip. The lock is
	// per-strategy (not per-pool) — per-pool locks would require keying
	// off the pool name and add complexity that the round-robin
	// throughput does not warrant.
	mu sync.Mutex
}

// Pick implements Strategy.
//
// Algorithm:
//  1. Read cursor c from StateStore (zero if missing).
//  2. Select candidate at c % len(candidates).
//  3. Persist cursor (c+1) — modulo applied at read time so it never
//     overflows a uint64 in any realistic deployment.
//
// Pick is serialized by an internal mutex so two concurrent callers on
// the same strategy can't both see cursor=N.
func (s *RoundRobinStrategy) Pick(ctx context.Context, candidates []common.Storage, hint Hint) (common.Storage, error) {
	if s.State == nil {
		return nil, &MissingStateStoreError{}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Guard against an empty candidate set. The Manager rejects empty
	// pools upstream, but a direct caller can still pass nil/empty here;
	// without this check the cursor % len(candidates) below would panic
	// with an integer divide-by-zero.
	if len(candidates) == 0 {
		return nil, &EmptyPoolError{Name: poolNameFromContext(ctx)}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	key := cursorKey(poolNameFromContext(ctx))

	cursor, err := s.loadCursor(ctx, key)
	if err != nil {
		return nil, err
	}

	idx := cursor % uint64(len(candidates))
	picked := candidates[idx]

	if err := s.storeCursor(ctx, key, cursor+1); err != nil {
		return nil, err
	}

	return picked, nil
}

// Name implements Strategy.
func (s *RoundRobinStrategy) Name() string {
	return roundRobinStrategyName
}

// loadCursor reads the cursor value from the state store. A missing key
// returns 0 (the cursor starts at zero). A malformed value (wrong byte
// length) is treated as zero and overwritten on the next store — robust
// against corrupted state from older versions or partial writes.
func (s *RoundRobinStrategy) loadCursor(ctx context.Context, key string) (uint64, error) {
	value, found, err := s.State.Get(ctx, key)
	if err != nil {
		return 0, err
	}
	if !found || len(value) != 8 {
		return 0, nil
	}
	return binary.BigEndian.Uint64(value), nil
}

// storeCursor encodes and persists the cursor value.
func (s *RoundRobinStrategy) storeCursor(ctx context.Context, key string, cursor uint64) error {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, cursor)
	return s.State.Put(ctx, key, buf)
}

// cursorKey returns the StateStore key for a given pool's cursor. The
// pool name is included so multiple pools using the same StateStore
// don't trample each other.
func cursorKey(poolName string) string {
	if poolName == "" {
		// Fallback when called outside a Manager (e.g. direct unit
		// tests on the strategy). Real Manager-driven calls always
		// supply a name.
		poolName = "_unbound_"
	}
	return cursorKeyPrefix + poolName + "/" + roundRobinStrategyName + "/cursor"
}
