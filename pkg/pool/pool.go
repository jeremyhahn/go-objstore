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

// Package pool provides a Volume Pool primitive that selects one Storage
// from a registered set of candidates using a pluggable Strategy. It is
// intentionally Layer 0 — storage-agnostic, no external dependencies
// beyond pkg/common — so qrdb and future consumers can share the
// same placement primitive without taking on any of each other's deps.
//
// Cursor / metric state is supplied by the caller through the small
// StateStore interface. go-objstore tests use an in-memory stub; qrdb
// supplies an implementation backed by its system-tenant key/value
// store so cursors survive daemon restarts.
package pool

import (
	"context"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// Hint carries optional placement information passed from the caller to
// the strategy. Every field is optional; strategies may ignore any field
// they do not consume. Adding new fields is non-breaking — strategies
// that don't read them are unaffected.
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

// Strategy decides which Storage to return from a pool's candidates.
// Implementations: RoundRobinStrategy (production), LeastUtilizationStrategy
// (stub returning ErrStrategyNotImplemented for now).
//
// Strategies must be safe for concurrent use across multiple goroutines
// and across multiple pools sharing the same instance.
type Strategy interface {
	// Pick selects one Storage from candidates. Implementations may
	// consult ctx for cancellation and the StateStore for persistent
	// cursor / metric state. The returned Storage is one of the values
	// in candidates; callers receive a typed error otherwise.
	Pick(ctx context.Context, candidates []common.Storage, hint Hint) (common.Storage, error)

	// Name returns the canonical strategy name (e.g. "round_robin").
	// Used for logging and diagnostic output.
	Name() string
}

// StateStore is a tiny key/value abstraction the round-robin strategy
// uses to persist its cursor across restarts. It is caller-supplied so
// the pool package stays storage-agnostic and Layer 0.
//
// Get returns the stored value plus a found flag. A missing key is not
// an error condition — callers signal it with found=false and a nil
// error so the caller can apply a zero default.
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
	// Register adds a new pool. Returns DuplicatePoolError if the name
	// is already taken, InvalidPoolNameError if name is empty, and
	// NilStrategyError if strategy is nil. An empty candidates slice
	// is accepted at Register time — Pick surfaces EmptyPoolError.
	Register(name string, candidates []common.Storage, strategy Strategy) error

	// Pick selects one Storage from the named pool using its strategy.
	// Returns PoolNotFoundError if the pool is not registered and
	// EmptyPoolError if the pool has no candidates.
	Pick(ctx context.Context, poolName string, hint Hint) (common.Storage, error)

	// List returns a snapshot of all registered pools. The returned
	// slice is independent of the manager's internal state — callers
	// may iterate freely. The Candidates slice inside each Pool is
	// also copied to keep the snapshot stable.
	List(ctx context.Context) ([]Pool, error)
}

// poolNameContextKey is the private context key that the Manager uses
// to thread the active pool name down to the strategy. Strategies that
// need to namespace persistent state by pool name (round-robin) read
// it via poolNameFromContext. Keeping the key unexported prevents
// outside packages from spoofing it.
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
