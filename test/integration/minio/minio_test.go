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

package minio

import (
	"bytes"
	"io"
	"testing"

	"github.com/jeremyhahn/go-objstore/pkg/factory"
	testcommon "github.com/jeremyhahn/go-objstore/test/integration/common"
)

func TestMinIO_BasicOps(t *testing.T) {
	bucket := "objstore-minio-test"
	testcommon.CreateMinIOBucket(t, bucket)

	st, err := factory.NewStorage("minio", map[string]string{
		"bucket":    bucket,
		"endpoint":  "http://minio:9000",
		"accessKey": "minioadmin",
		"secretKey": "minioadmin",
	})
	if err != nil {
		t.Fatal(err)
	}

	key := "it/minio.txt"
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
		t.Fatalf("minio get mismatch: %s != %s", got, data)
	}
	if err := st.Delete(key); err != nil {
		t.Fatal(err)
	}
}

func TestMinIO_EmptyFile(t *testing.T) {
	bucket := "objstore-minio-empty"
	testcommon.CreateMinIOBucket(t, bucket)

	st, err := factory.NewStorage("minio", map[string]string{
		"bucket":    bucket,
		"endpoint":  "http://minio:9000",
		"accessKey": "minioadmin",
		"secretKey": "minioadmin",
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

func TestMinIO_SpecialCharacters(t *testing.T) {
	bucket := "objstore-minio-special"
	testcommon.CreateMinIOBucket(t, bucket)

	st, err := factory.NewStorage("minio", map[string]string{
		"bucket":    bucket,
		"endpoint":  "http://minio:9000",
		"accessKey": "minioadmin",
		"secretKey": "minioadmin",
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

func TestMinIO_ErrorHandling(t *testing.T) {
	bucket := "objstore-minio-errors"
	testcommon.CreateMinIOBucket(t, bucket)

	st, err := factory.NewStorage("minio", map[string]string{
		"bucket":    bucket,
		"endpoint":  "http://minio:9000",
		"accessKey": "minioadmin",
		"secretKey": "minioadmin",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Test get non-existent key
	_, err = st.Get("nonexistent.txt")
	if err == nil {
		t.Fatal("Expected error for non-existent key")
	}

	// Test delete non-existent key (MinIO doesn't error on this)
	if err := st.Delete("nonexistent.txt"); err != nil {
		t.Logf("Delete non-existent returned: %v", err)
	}
}

func TestMinIO_Lifecycle(t *testing.T) {
	bucket := "objstore-minio-lifecycle"
	testcommon.CreateMinIOBucket(t, bucket)

	st, err := factory.NewStorage("minio", map[string]string{
		"bucket":    bucket,
		"endpoint":  "http://minio:9000",
		"accessKey": "minioadmin",
		"secretKey": "minioadmin",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Get initial policies
	policies, err := st.GetPolicies()
	if err != nil {
		t.Fatalf("GetPolicies failed: %v", err)
	}

	t.Logf("MinIO storage lifecycle methods available, initial policy count: %d", len(policies))
}

// TestMinIO_ComprehensiveSuite runs the complete test suite for all interface methods
func TestMinIO_ComprehensiveSuite(t *testing.T) {
	bucket := "objstore-minio-comprehensive"
	testcommon.CreateMinIOBucket(t, bucket)

	st, err := factory.NewStorage("minio", map[string]string{
		"bucket":    bucket,
		"endpoint":  "http://minio:9000",
		"accessKey": "minioadmin",
		"secretKey": "minioadmin",
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
