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

//go:build awss3

package s3

import "testing"

func TestS3_Configure_Errors(t *testing.T) {
	s := &S3{}
	if err := s.Configure(map[string]string{}); err == nil {
		t.Fatalf("expected error for missing bucket")
	}
}

func TestS3_Configure_WithEndpointAndCreds(t *testing.T) {
	s := &S3{}
	err := s.Configure(map[string]string{
		"bucket":         "b",
		"region":         "us-east-1",
		"endpoint":       "http://localhost:9000",
		"forcePathStyle": "true",
		"accessKey":      "ak",
		"secretKey":      "sk",
	})
	if err != nil {
		t.Fatalf("unexpected configure error: %v", err)
	}
	if s.svc == nil {
		t.Fatalf("expected svc initialized")
	}
}
