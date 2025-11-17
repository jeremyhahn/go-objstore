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

//go:build integration

package s3

import (
	"bytes"
	"io"
	"testing"

	"github.com/jeremyhahn/go-objstore/pkg/factory"
	testcommon "github.com/jeremyhahn/go-objstore/test/integration/common"
)

func TestS3_BasicOps(t *testing.T) {
	bucket := "objstore-test"
	testcommon.CreateMinIOBucket(t, bucket)

	st, err := factory.NewStorage("s3", map[string]string{
		"bucket":         bucket,
		"region":         "us-east-1",
		"endpoint":       "http://minio:9000",
		"forcePathStyle": "true",
		"accessKey":      "minioadmin",
		"secretKey":      "minioadmin",
	})
	if err != nil {
		t.Fatal(err)
	}

	key := "it/s3.txt"
	data := []byte("hello minio")
	if err := st.Put(key, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	rc, err := st.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if string(got) != string(data) {
		t.Fatalf("s3 get mismatch: %s != %s", got, data)
	}
	if err := st.Delete(key); err != nil {
		t.Fatal(err)
	}
}

func TestS3_EmptyFile(t *testing.T) {
	bucket := "objstore-s3-empty"
	testcommon.CreateMinIOBucket(t, bucket)

	st, err := factory.NewStorage("s3", map[string]string{
		"bucket":         bucket,
		"region":         "us-east-1",
		"endpoint":       "http://minio:9000",
		"forcePathStyle": "true",
		"accessKey":      "minioadmin",
		"secretKey":      "minioadmin",
	})
	if err != nil {
		t.Fatal(err)
	}

	key := "empty.txt"
	if err := st.Put(key, bytes.NewReader([]byte{})); err != nil {
		t.Fatalf("Put empty failed: %v", err)
	}

	rc, err := st.Get(key)
	if err != nil {
		t.Fatalf("Get empty failed: %v", err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if len(got) != 0 {
		t.Fatalf("Expected empty, got %d bytes", len(got))
	}
}

func TestS3_SpecialCharacters(t *testing.T) {
	bucket := "objstore-s3-special"
	testcommon.CreateMinIOBucket(t, bucket)

	st, err := factory.NewStorage("s3", map[string]string{
		"bucket":         bucket,
		"region":         "us-east-1",
		"endpoint":       "http://minio:9000",
		"forcePathStyle": "true",
		"accessKey":      "minioadmin",
		"secretKey":      "minioadmin",
	})
	if err != nil {
		t.Fatal(err)
	}

	keys := []string{
		"file with spaces.txt",
		"file+plus.txt",
		"deep/nested/path/file.txt",
	}

	for _, key := range keys {
		data := []byte("data for " + key)
		if err := st.Put(key, bytes.NewReader(data)); err != nil {
			t.Fatalf("Put %s failed: %v", key, err)
		}
		rc, err := st.Get(key)
		if err != nil {
			t.Fatalf("Get %s failed: %v", key, err)
		}
		got, _ := io.ReadAll(rc)
		rc.Close()
		if string(got) != string(data) {
			t.Fatalf("Mismatch for %s", key)
		}
	}
}

func TestS3_ErrorHandling(t *testing.T) {
	bucket := "objstore-s3-errors"
	testcommon.CreateMinIOBucket(t, bucket)

	st, err := factory.NewStorage("s3", map[string]string{
		"bucket":         bucket,
		"region":         "us-east-1",
		"endpoint":       "http://minio:9000",
		"forcePathStyle": "true",
		"accessKey":      "minioadmin",
		"secretKey":      "minioadmin",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Test get non-existent key
	_, err = st.Get("nonexistent.txt")
	if err == nil {
		t.Fatal("Expected error for non-existent key")
	}

	// Test delete non-existent key (S3 doesn't error on this)
	if err := st.Delete("nonexistent.txt"); err != nil {
		t.Logf("Delete non-existent returned: %v", err)
	}
}

func TestS3_Lifecycle(t *testing.T) {
	bucket := "objstore-s3-lifecycle"
	testcommon.CreateMinIOBucket(t, bucket)

	st, err := factory.NewStorage("s3", map[string]string{
		"bucket":         bucket,
		"region":         "us-east-1",
		"endpoint":       "http://minio:9000",
		"forcePathStyle": "true",
		"accessKey":      "minioadmin",
		"secretKey":      "minioadmin",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Get initial policies
	policies, err := st.GetPolicies()
	if err != nil {
		t.Fatalf("GetPolicies failed: %v", err)
	}

	t.Logf("S3 storage lifecycle methods available, initial policy count: %d", len(policies))
}

// TestS3_ComprehensiveSuite runs the complete test suite for all interface methods
func TestS3_ComprehensiveSuite(t *testing.T) {
	bucket := "objstore-s3-comprehensive"
	testcommon.CreateMinIOBucket(t, bucket)

	st, err := factory.NewStorage("s3", map[string]string{
		"bucket":         bucket,
		"region":         "us-east-1",
		"endpoint":       "http://minio:9000",
		"forcePathStyle": "true",
		"accessKey":      "minioadmin",
		"secretKey":      "minioadmin",
	})
	if err != nil {
		t.Fatal(err)
	}

	suite := &testcommon.ComprehensiveTestSuite{
		Storage: st,
		T:       t,
	}

	suite.RunAllTests()
}
