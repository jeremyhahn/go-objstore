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

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// localOnlyManager rejects at Pick time any candidate that is not a common.PathAccessor, so one pool can serve filesystem-only and cloud-tolerant consumers.
type localOnlyManager struct {
	inner Manager
}

// LocalOnly wraps a Manager so Pick rejects any candidate that isn't a
// common.PathAccessor. Register and List delegate verbatim to the
// inner Manager.
//
// The wrapper does not deep-copy the inner Manager — registering or
// listing on either the wrapper or the inner is equivalent. Pool
// ownership stays with the caller that created the inner Manager.
func LocalOnly(m Manager) Manager {
	return &localOnlyManager{inner: m}
}

// Register delegates to the inner Manager.
func (m *localOnlyManager) Register(name string, candidates []common.Storage, strategy Strategy) error {
	return m.inner.Register(name, candidates, strategy)
}

// Pick delegates to the inner Manager and then verifies that the
// returned Storage satisfies common.PathAccessor. Cloud-only backends
// fail here with NonLocalCandidateError.
func (m *localOnlyManager) Pick(ctx context.Context, poolName string, hint Hint) (common.Storage, error) {
	picked, err := m.inner.Pick(ctx, poolName, hint)
	if err != nil {
		return nil, err
	}
	if _, ok := picked.(common.PathAccessor); !ok {
		return nil, &NonLocalCandidateError{Pool: poolName}
	}
	return picked, nil
}

// List delegates to the inner Manager.
func (m *localOnlyManager) List(ctx context.Context) ([]Pool, error) {
	return m.inner.List(ctx)
}
