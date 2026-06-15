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
	"sort"
	"sync"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// memoryManager is the default in-memory Manager implementation. It is
// safe for concurrent use; reads share a sync.RWMutex with writes.
type memoryManager struct {
	mu    sync.RWMutex
	pools map[string]*Pool
}

// NewManager constructs an in-memory Manager. Pools live for the
// lifetime of the Manager value; persistence (if any) is a property of
// the StateStore the caller wires into each Strategy.
func NewManager() Manager {
	return &memoryManager{
		pools: make(map[string]*Pool),
	}
}

// Register implements Manager.
func (m *memoryManager) Register(name string, candidates []common.Storage, strategy Strategy) error {
	if name == "" {
		return &InvalidPoolNameError{}
	}
	if strategy == nil {
		return &NilStrategyError{Pool: name}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.pools[name]; exists {
		return &DuplicatePoolError{Name: name}
	}

	// Defensive copy of candidates so the caller cannot mutate the pool
	// after registration.
	storedCandidates := make([]common.Storage, len(candidates))
	copy(storedCandidates, candidates)

	m.pools[name] = &Pool{
		Name:       name,
		Candidates: storedCandidates,
		Strategy:   strategy,
	}
	return nil
}

// Pick implements Manager.
func (m *memoryManager) Pick(ctx context.Context, poolName string, hint Hint) (common.Storage, error) {
	m.mu.RLock()
	p, ok := m.pools[poolName]
	m.mu.RUnlock()

	if !ok {
		return nil, &PoolNotFoundError{Name: poolName}
	}
	if len(p.Candidates) == 0 {
		return nil, &EmptyPoolError{Name: poolName}
	}

	// Thread the pool name to the strategy via a private context key so
	// strategies that namespace persistent state (round-robin cursor)
	// can do so without leaking the pool name into the public Hint.
	ctx = withPoolName(ctx, poolName)
	return p.Strategy.Pick(ctx, p.Candidates, hint)
}

// List implements Manager.
func (m *memoryManager) List(ctx context.Context) ([]Pool, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make([]Pool, 0, len(m.pools))
	for _, p := range m.pools {
		candidates := make([]common.Storage, len(p.Candidates))
		copy(candidates, p.Candidates)
		out = append(out, Pool{
			Name:       p.Name,
			Candidates: candidates,
			Strategy:   p.Strategy,
		})
	}

	// Sort by name so List output is deterministic across calls — useful
	// for snapshot tests and operator diagnostics.
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}
