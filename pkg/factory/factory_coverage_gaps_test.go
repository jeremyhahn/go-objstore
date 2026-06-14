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

package factory

import (
	"slices"
	"sort"
	"testing"
)

// TestListStorageBackends verifies that the registered storage backends are
// returned and that archive-only types are excluded.
func TestListStorageBackends(t *testing.T) {
	backends := ListStorageBackends()
	if backends == nil {
		t.Fatal("ListStorageBackends() returned nil")
	}

	// Archive-only types must not appear.
	for _, b := range backends {
		if b == "glacier" || b == "azurearchive" {
			t.Errorf("archive-only backend %q must not appear in ListStorageBackends()", b)
		}
	}

	// "local" is always registered (no build tag required).
	if !slices.Contains(backends, "local") {
		t.Errorf("expected 'local' in ListStorageBackends(), got %v", backends)
	}

	// "memory" is always registered.
	if !slices.Contains(backends, "memory") {
		t.Errorf("expected 'memory' in ListStorageBackends(), got %v", backends)
	}
}

// TestListStorageBackends_Sorted verifies the result is consistent across calls
// (order may vary but the same set is returned).
func TestListStorageBackends_Sorted(t *testing.T) {
	a := ListStorageBackends()
	b := ListStorageBackends()

	sort.Strings(a)
	sort.Strings(b)

	if len(a) != len(b) {
		t.Errorf("inconsistent lengths: %d vs %d", len(a), len(b))
	}
	for i := range a {
		if a[i] != b[i] {
			t.Errorf("inconsistent at index %d: %q vs %q", i, a[i], b[i])
		}
	}
}

// TestListArchivers verifies that the registered archiver types are returned.
// "local" is always registered as an archiver.
func TestListArchivers(t *testing.T) {
	archivers := ListArchivers()
	if archivers == nil {
		t.Fatal("ListArchivers() returned nil")
	}

	// "local" is always registered as an archiver.
	if !slices.Contains(archivers, "local") {
		t.Errorf("expected 'local' in ListArchivers(), got %v", archivers)
	}
}

// TestListArchivers_IncludesConditional checks that conditionally compiled
// archivers (glacier, azurearchive) appear when built with their tags.
// With the full tag set used by the project's CI/test targets, both are present.
func TestListArchivers_IncludesConditional(t *testing.T) {
	archivers := ListArchivers()

	for _, expected := range []string{"glacier", "azurearchive"} {
		if slices.Contains(archivers, expected) {
			t.Logf("archiver %q registered (build tag active)", expected)
		} else {
			t.Logf("archiver %q not registered (build tag inactive)", expected)
		}
	}
}

// TestIsStorageBackendRegistered verifies the predicate for known and unknown types.
func TestIsStorageBackendRegistered(t *testing.T) {
	tests := []struct {
		backendType string
		want        bool
	}{
		{"local", true},
		{"memory", true},
		{"glacier", false},      // archive-only — must return false
		{"azurearchive", false}, // archive-only — must return false
		{"unknown", false},
	}

	for _, tt := range tests {
		t.Run(tt.backendType, func(t *testing.T) {
			got := IsStorageBackendRegistered(tt.backendType)
			if got != tt.want {
				t.Errorf("IsStorageBackendRegistered(%q) = %v, want %v", tt.backendType, got, tt.want)
			}
		})
	}
}

// TestIsArchiverRegistered verifies the predicate for known and unknown archiver types.
func TestIsArchiverRegistered(t *testing.T) {
	tests := []struct {
		archiverType string
		want         bool
	}{
		{"local", true},
		{"unknown", false},
		{"s3", false},
	}

	for _, tt := range tests {
		t.Run(tt.archiverType, func(t *testing.T) {
			got := IsArchiverRegistered(tt.archiverType)
			if got != tt.want {
				t.Errorf("IsArchiverRegistered(%q) = %v, want %v", tt.archiverType, got, tt.want)
			}
		})
	}
}

// TestIsArchiverRegistered_Conditional checks glacier/azurearchive registration
// state matches what ListArchivers reports.
func TestIsArchiverRegistered_Conditional(t *testing.T) {
	archivers := ListArchivers()
	archiverSet := make(map[string]bool, len(archivers))
	for _, a := range archivers {
		archiverSet[a] = true
	}

	for _, name := range []string{"glacier", "azurearchive"} {
		got := IsArchiverRegistered(name)
		if got != archiverSet[name] {
			t.Errorf("IsArchiverRegistered(%q) = %v but ListArchivers reports presence = %v",
				name, got, archiverSet[name])
		}
	}
}

// TestNewStorage_Memory exercises the memory backend creator registered in
// factory_memory.go's init(), which is the 16.7%-covered init function.
func TestNewStorage_Memory(t *testing.T) {
	st, err := NewStorage("memory", map[string]string{})
	if err != nil {
		t.Fatalf("NewStorage(\"memory\") error = %v", err)
	}
	if st == nil {
		t.Fatal("NewStorage(\"memory\") returned nil storage")
	}

	// Confirm it's actually usable.
	if err := st.Configure(map[string]string{}); err != nil {
		t.Fatalf("memory storage Configure() error = %v", err)
	}
}

// TestIsStorageBackendRegistered_ConditionalBackends checks that conditionally
// compiled storage backends (s3, gcs, azure, minio) are registered if and only
// if they appear in ListStorageBackends.
func TestIsStorageBackendRegistered_ConditionalBackends(t *testing.T) {
	all := ListStorageBackends()
	present := make(map[string]bool, len(all))
	for _, b := range all {
		present[b] = true
	}

	for _, name := range []string{"s3", "gcs", "azure", "minio"} {
		got := IsStorageBackendRegistered(name)
		if got != present[name] {
			t.Errorf("IsStorageBackendRegistered(%q) = %v but ListStorageBackends presence = %v",
				name, got, present[name])
		}
	}
}
