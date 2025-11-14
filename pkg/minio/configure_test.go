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

//go:build minio

package minio

import "testing"

func TestMinIO_Configure_Errors(t *testing.T) {
	m := &MinIO{}

	// Test missing bucket
	if err := m.Configure(map[string]string{}); err == nil {
		t.Fatal("expected error for missing bucket")
	}

	// Test missing endpoint
	if err := m.Configure(map[string]string{
		"bucket": "test-bucket",
	}); err == nil {
		t.Fatal("expected error for missing endpoint")
	}

	// Test missing accessKey
	if err := m.Configure(map[string]string{
		"bucket":   "test-bucket",
		"endpoint": "http://localhost:9000",
	}); err == nil {
		t.Fatal("expected error for missing accessKey")
	}

	// Test missing secretKey
	if err := m.Configure(map[string]string{
		"bucket":    "test-bucket",
		"endpoint":  "http://localhost:9000",
		"accessKey": "minioadmin",
	}); err == nil {
		t.Fatal("expected error for missing secretKey")
	}
}

func TestMinIO_Configure_Success(t *testing.T) {
	m := &MinIO{}
	err := m.Configure(map[string]string{
		"bucket":    "test-bucket",
		"endpoint":  "http://localhost:9000",
		"accessKey": "minioadmin",
		"secretKey": "minioadmin",
	})
	if err != nil {
		t.Fatalf("unexpected configure error: %v", err)
	}
	if m.svc == nil {
		t.Fatal("expected svc initialized")
	}
	if m.bucket != "test-bucket" {
		t.Fatalf("expected bucket test-bucket, got %s", m.bucket)
	}
}

func TestMinIO_Configure_WithRegion(t *testing.T) {
	m := &MinIO{}
	err := m.Configure(map[string]string{
		"bucket":    "test-bucket",
		"endpoint":  "http://localhost:9000",
		"region":    "us-west-2",
		"accessKey": "minioadmin",
		"secretKey": "minioadmin",
	})
	if err != nil {
		t.Fatalf("unexpected configure error: %v", err)
	}
	if m.svc == nil {
		t.Fatal("expected svc initialized")
	}
}

func TestMinIO_Configure_DefaultRegion(t *testing.T) {
	m := &MinIO{}
	err := m.Configure(map[string]string{
		"bucket":    "test-bucket",
		"endpoint":  "http://localhost:9000",
		"accessKey": "minioadmin",
		"secretKey": "minioadmin",
	})
	if err != nil {
		t.Fatalf("unexpected configure error: %v", err)
	}
	// Region should default to us-east-1, but we can't easily verify that
	// without exposing internal state or making invasive changes
	if m.svc == nil {
		t.Fatal("expected svc initialized")
	}
}
