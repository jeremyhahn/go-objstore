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

package uploadtest_test

import (
	"testing"

	"github.com/jeremyhahn/go-objstore/internal/uploadtest"
	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/memory"
)

// The suite is exercised against the memory provider so that its own
// statements are covered where it is defined.
func TestSuiteAgainstMemory(t *testing.T) {
	uploadtest.RunMultipart(t, func(t *testing.T) common.Storage { return memory.New() })
	uploadtest.RunBuckets(t, func(t *testing.T) common.BucketManager { return memory.New().(common.BucketManager) })
}
