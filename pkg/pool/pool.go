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

// Package pool selects one Storage from registered candidates with a pluggable Strategy; persistent state lives in a caller-supplied StateStore.
package pool

import (
	"context"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// Hint carries optional placement information; strategies may ignore any field.
type Hint struct {
	// TenantID is a sticky-placement signal. A future strategy could use
	// it to keep the same tenant on the same backend across restarts.
	TenantID string

	// SizeHint is the expected payload size in bytes. Capacity-aware
	// strategies use it to filter candidates whose remaining space would
	// fall below thresholds.
	SizeHint int64

	// Region is a preferred region tag (e.g. "us-east-1"). Strategies
	// that understand region tags filter candidates accordingly.
	Region string
}

// Strategy picks a Storage from a pool's candidates (RoundRobinStrategy, LeastUtilizationStrategy); it must be safe for concurrent use across pools.
type Strategy interface {
	// Pick selects one of candidates, consulting ctx and the StateStore.
	Pick(ctx context.Context, candidates []common.Storage, hint Hint) (common.Storage, error)

	// Name returns the canonical strategy name (e.g. "round_robin").
	// Used for logging and diagnostic output.
	Name() string
}

// StateStore is a caller-supplied key/value store for strategy state; a missing key is found=false with a nil error.
type StateStore interface {
	// Get returns the value for key. If the key is not present found
	// will be false and err nil. err is reserved for true I/O failures.
	Get(ctx context.Context, key string) (value []byte, found bool, err error)

	// Put stores value under key, overwriting any previous value.
	Put(ctx context.Context, key string, value []byte) error
}

// Pool is one named pool of registered storage candidates. Pool values
// are immutable snapshots returned by Manager.List; consumers must not
// mutate the slice.
type Pool struct {
	// Name is the pool's unique identifier within a Manager.
	Name string

	// Candidates are the Storage instances eligible for selection.
	Candidates []common.Storage

	// Strategy is the placement strategy used by Pick.
	Strategy Strategy
}

// Manager is the registry of named pools. Implementations must be safe
// for concurrent use.
type Manager interface {
	// Register adds a pool; DuplicatePoolError, InvalidPoolNameError or NilStrategyError on bad input. Empty candidates fail at Pick.
	Register(name string, candidates []common.Storage, strategy Strategy) error

	// Pick selects one Storage from the named pool using its strategy.
	// Returns PoolNotFoundError if the pool is not registered and
	// EmptyPoolError if the pool has no candidates.
	Pick(ctx context.Context, poolName string, hint Hint) (common.Storage, error)

	// List returns an independent snapshot of all pools, candidates copied.
	List(ctx context.Context) ([]Pool, error)
}

// poolNameContextKey threads the active pool name to the strategy; unexported so it cannot be spoofed.
type poolNameContextKey struct{}

// withPoolName returns a derived context carrying the supplied pool
// name. Used by Manager.Pick when invoking the strategy.
func withPoolName(ctx context.Context, name string) context.Context {
	return context.WithValue(ctx, poolNameContextKey{}, name)
}

// poolNameFromContext extracts the pool name set by withPoolName. If
// the context does not carry one, an empty string is returned and the
// caller falls back to a non-namespaced key.
func poolNameFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(poolNameContextKey{}).(string); ok {
		return v
	}
	return ""
}
