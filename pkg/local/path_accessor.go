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

package local

import "github.com/jeremyhahn/go-objstore/pkg/common"

// LocalPath returns the absolute path backing this storage (common.PathAccessor); empty before Configure.
func (l *Local) LocalPath() string {
	return l.path
}

// Compile-time assertion that *Local satisfies common.PathAccessor. This
// guarantees the local backend remains a valid candidate for any pool
// wrapped by pool.LocalOnly.
var _ common.PathAccessor = (*Local)(nil)
