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

package local_test

import (
	"testing"

	"github.com/jeremyhahn/go-objstore/internal/storagetest"
	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/local"
)

func TestStorageContract(t *testing.T) {
	storagetest.Run(t, func(t *testing.T) common.Storage {
		s := local.New()
		if err := s.Configure(map[string]string{"path": t.TempDir()}, common.Credentials{}); err != nil {
			t.Fatalf("Configure: %v", err)
		}
		if c, ok := s.(interface{ Close() }); ok {
			t.Cleanup(c.Close)
		}
		return s
	})
}
