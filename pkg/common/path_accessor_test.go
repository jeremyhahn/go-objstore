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

package common_test

import (
	"testing"

	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/local"
	"github.com/jeremyhahn/go-objstore/pkg/memory"
)

// TestPathAccessor_LocalSatisfies verifies the local backend satisfies
// the PathAccessor interface and returns the configured path.
func TestPathAccessor_LocalSatisfies(t *testing.T) {
	tempDir := t.TempDir()

	storage := local.New()
	if err := storage.Configure(map[string]string{"path": tempDir}); err != nil {
		t.Fatalf("Configure failed: %v", err)
	}

	accessor, ok := storage.(common.PathAccessor)
	if !ok {
		t.Fatalf("local.Local does not satisfy common.PathAccessor")
	}

	got := accessor.LocalPath()
	if got != tempDir {
		t.Errorf("LocalPath() = %q, want %q", got, tempDir)
	}
}

// TestPathAccessor_LocalBeforeConfigure verifies LocalPath returns the
// empty string when called before Configure has been invoked. This is
// the documented zero-value behaviour and lets a callers-can-detect
// pattern (e.g. "nil path means unconfigured") work without a nil check.
func TestPathAccessor_LocalBeforeConfigure(t *testing.T) {
	storage := local.New()

	accessor, ok := storage.(common.PathAccessor)
	if !ok {
		t.Fatalf("local.Local does not satisfy common.PathAccessor")
	}

	if got := accessor.LocalPath(); got != "" {
		t.Errorf("LocalPath() before Configure = %q, want empty string", got)
	}
}

// TestPathAccessor_MemoryDoesNotSatisfy verifies that a non-local
// backend (the in-memory backend stands in for cloud backends here —
// they all share the property of having no on-disk path) does NOT
// satisfy PathAccessor. This guards the contract that LocalOnly's
// type assertion is the right discriminator.
func TestPathAccessor_MemoryDoesNotSatisfy(t *testing.T) {
	storage := memory.New()

	if _, ok := storage.(common.PathAccessor); ok {
		t.Errorf("memory.Memory unexpectedly satisfies common.PathAccessor; "+
			"the LocalOnly wrapper would treat it as a local backend. got: %T", storage)
	}
}

// pathAccessorCheck is a compile-time-style test helper that asserts
// the runtime type does NOT satisfy PathAccessor. It exists so the
// "cloud backends must not be PathAccessor" rule is enforced at the
// package boundary by a single function call instead of repeated ad-hoc
// type assertions.
func pathAccessorCheck(t *testing.T, label string, storage common.Storage, wantSatisfies bool) {
	t.Helper()
	_, ok := storage.(common.PathAccessor)
	if ok != wantSatisfies {
		t.Errorf("%s: PathAccessor satisfaction = %v, want %v (concrete type %T)",
			label, ok, wantSatisfies, storage)
	}
}

// TestPathAccessor_HelperVerifiesBothPaths exercises the helper above
// against both a local backend (must satisfy) and a memory backend
// (must not satisfy). It pins down the helper's behaviour and catches
// future regressions where a backend silently grows a LocalPath method.
func TestPathAccessor_HelperVerifiesBothPaths(t *testing.T) {
	tempDir := t.TempDir()
	localStorage := local.New()
	if err := localStorage.Configure(map[string]string{"path": tempDir}); err != nil {
		t.Fatalf("Configure failed: %v", err)
	}

	pathAccessorCheck(t, "local", localStorage, true)
	pathAccessorCheck(t, "memory", memory.New(), false)
}
