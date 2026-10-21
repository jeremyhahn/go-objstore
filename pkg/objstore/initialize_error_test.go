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

package objstore

import (
	"errors"
	"testing"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

func TestInitialize_RetainsFirstError(t *testing.T) {
	Reset()
	t.Cleanup(Reset)

	first := Initialize(nil)
	if !errors.Is(first, ErrInitializationFailed) {
		t.Fatalf("first call: want ErrInitializationFailed, got %v", first)
	}

	// A second call with a valid config must still report the first failure.
	second := Initialize(&FacadeConfig{
		Backends:       map[string]common.Storage{"m": newMockStorage("m")},
		DefaultBackend: "m",
	})
	if !errors.Is(second, ErrInitializationFailed) {
		t.Fatalf("second call: want ErrInitializationFailed, got %v", second)
	}
	if second.Error() != first.Error() {
		t.Fatalf("second call returned %q, want first error %q", second, first)
	}
	if IsInitialized() {
		t.Fatal("facade must stay uninitialized after a failed first call")
	}
}
