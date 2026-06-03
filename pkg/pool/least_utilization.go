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

// leastUtilizationStrategyName is the canonical name returned by Name.
const leastUtilizationStrategyName = "least_utilization"

// LeastUtilizationStrategy is a placeholder for capacity-aware placement.
// The concrete implementation (consult per-candidate Capacity()/Used()
// metrics, pick the lowest-utilized backend) is deferred post-MVP per
// the parsed-jingling-crane plan.
//
// Until then, Pick returns ErrStrategyNotImplemented so callers fail
// loudly instead of silently getting round-robin behavior they didn't
// ask for.
type LeastUtilizationStrategy struct{}

// Pick implements Strategy. Always returns ErrStrategyNotImplemented.
func (s *LeastUtilizationStrategy) Pick(ctx context.Context, candidates []common.Storage, hint Hint) (common.Storage, error) {
	return nil, ErrStrategyNotImplemented
}

// Name implements Strategy.
func (s *LeastUtilizationStrategy) Name() string {
	return leastUtilizationStrategyName
}
