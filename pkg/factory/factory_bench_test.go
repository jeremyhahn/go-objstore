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
	"testing"
)

func BenchmarkNewStorage_Local(b *testing.B) {
	settings := map[string]string{
		"path": "/tmp/bench-test",
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := NewStorage("local", settings)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkNewStorage_S3(b *testing.B) {
	settings := map[string]string{
		"bucket": "test-bucket",
		"region": "us-east-1",
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := NewStorage("s3", settings)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkNewStorage_MinIO(b *testing.B) {
	settings := map[string]string{
		"bucket":    "test-bucket",
		"endpoint":  "http://localhost:9000",
		"accessKey": "minioadmin",
		"secretKey": "minioadmin",
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := NewStorage("minio", settings)
		if err != nil {
			b.Fatal(err)
		}
	}
}
