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

// PoolNotFoundError is returned by Manager.Pick when the requested pool
// name has not been registered.
type PoolNotFoundError struct {
	Name string
}

// Error implements the error interface.
func (e *PoolNotFoundError) Error() string {
	return "pool/" + e.Name + ": pool not found"
}

// EmptyPoolError is returned by Manager.Pick when the requested pool has
// no registered candidates.
type EmptyPoolError struct {
	Name string
}

// Error implements the error interface.
func (e *EmptyPoolError) Error() string {
	return "pool/" + e.Name + ": pool has no candidates"
}

// DuplicatePoolError is returned by Manager.Register when a pool with the
// same name has already been registered.
type DuplicatePoolError struct {
	Name string
}

// Error implements the error interface.
func (e *DuplicatePoolError) Error() string {
	return "pool/" + e.Name + ": pool already registered"
}

// StrategyNotImplementedError is returned by strategies whose concrete
// implementation has been deferred. Callers should fail loudly rather
// than silently fall back to a different strategy.
type StrategyNotImplementedError struct {
	Strategy string
}

// Error implements the error interface.
func (e *StrategyNotImplementedError) Error() string {
	return "pool: strategy " + e.Strategy + " not implemented"
}

// ErrStrategyNotImplemented was the sentinel returned by
// LeastUtilizationStrategy.Pick before its concrete implementation
// landed. No strategy in this package returns it anymore; it is kept so
// existing callers that match on it keep compiling.
var ErrStrategyNotImplemented = &StrategyNotImplementedError{Strategy: leastUtilizationStrategyName}

// NonLocalCandidateError is returned by LocalOnly.Pick when the wrapped
// Manager surfaces a candidate that does not satisfy common.PathAccessor.
// Cloud-only backends (S3, GCS, Azure, Glacier) trigger this — their
// presence in a LocalOnly pool is a configuration mistake.
type NonLocalCandidateError struct {
	Pool string
}

// Error implements the error interface.
func (e *NonLocalCandidateError) Error() string {
	return "pool/" + e.Pool + ": LocalOnly wrapper: candidate is not a PathAccessor"
}

// InvalidPoolNameError is returned by Manager.Register when the pool name
// is empty.
type InvalidPoolNameError struct{}

// Error implements the error interface.
func (e *InvalidPoolNameError) Error() string {
	return "pool: pool name must not be empty"
}

// NilStrategyError is returned by Manager.Register when no strategy was
// supplied.
type NilStrategyError struct {
	Pool string
}

// Error implements the error interface.
func (e *NilStrategyError) Error() string {
	return "pool/" + e.Pool + ": strategy must not be nil"
}

// MissingStateStoreError is returned by a strategy's Pick when the
// strategy was constructed without a StateStore. Its persistent state
// (round-robin cursor, least-utilization load tallies) cannot be kept
// without one.
type MissingStateStoreError struct {
	// Strategy is the canonical name of the strategy that needs the
	// StateStore (e.g. "round_robin").
	Strategy string
}

// Error implements the error interface.
func (e *MissingStateStoreError) Error() string {
	return "pool: " + e.Strategy + " strategy requires a non-nil StateStore"
}
