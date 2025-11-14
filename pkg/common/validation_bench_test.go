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

package common

import (
	"testing"
)

func BenchmarkValidateKey(b *testing.B) {
	keys := []string{
		"simple-key",
		"path/to/object",
		"deeply/nested/path/to/object.txt",
		"key-with-many-dashes-and-underscores_123",
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := keys[i%len(keys)]
		if err := ValidateKey(key); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkValidateMetadata(b *testing.B) {
	metadata := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := ValidateMetadata(metadata); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkValidateKey_Invalid(b *testing.B) {
	invalidKeys := []string{
		"../../../etc/passwd",
		"path/../../../secret",
		"\\..\\..\\windows\\system32",
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := invalidKeys[i%len(invalidKeys)]
		_ = ValidateKey(key)
	}
}
