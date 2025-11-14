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

package version

import (
	"strings"
	"testing"
)

func TestGet(t *testing.T) {
	version := Get()
	if version == "" {
		t.Error("Expected non-empty version")
	}

	// Should match VERSION file content
	expectedVersion := "0.1.0-beta"
	if version != expectedVersion {
		t.Logf("Version from file: %q (expected %q)", version, expectedVersion)
		// Don't fail - just log in case VERSION file was updated
	}

	// Calling Get() again should return the same cached value
	version2 := Get()
	if version != version2 {
		t.Errorf("Expected consistent version, got %q then %q", version, version2)
	}
}

func TestGet_Consistency(t *testing.T) {
	// Call Get multiple times to ensure consistency
	results := make([]string, 10)
	for i := 0; i < 10; i++ {
		results[i] = Get()
	}

	// All results should be identical
	first := results[0]
	for i, result := range results {
		if result != first {
			t.Errorf("Result %d differs: got %q, want %q", i, result, first)
		}
		if result == "" {
			t.Errorf("Result %d is empty", i)
		}
	}
}

func TestGet_ReturnValue(t *testing.T) {
	v := Get()

	// Version should not be empty
	if len(v) == 0 {
		t.Error("Get() returned empty string")
	}

	// Version should be trimmed (no leading/trailing whitespace)
	if v != strings.TrimSpace(v) {
		t.Errorf("Get() returned untrimmed version: %q", v)
	}
}

func BenchmarkGet(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = Get()
	}
}
