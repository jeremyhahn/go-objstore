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

package gcs

import (
	"bytes"
	"io"
	"testing"

	"github.com/jeremyhahn/go-objstore/pkg/factory"
	testcommon "github.com/jeremyhahn/go-objstore/test/integration/common"
)

func TestGCS_BasicOps(t *testing.T) {
	bucket := "objstore-gcs"
	testcommon.CreateGCSBucket(t, bucket)

	st, err := factory.NewStorage("gcs", map[string]string{"bucket": bucket})
	if err != nil {
		t.Fatal(err)
	}
	key := "it/gcs.txt"
	want := []byte("hello gcs")
	if err := st.Put(key, bytes.NewReader(want)); err != nil {
		t.Fatal(err)
	}
	r, err := st.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(r)
	r.Close()
	if string(got) != string(want) {
		t.Fatalf("gcs mismatch: %s != %s", got, want)
	}
	if err := st.Delete(key); err != nil {
		t.Fatal(err)
	}
}

func TestGCS_EmptyFile(t *testing.T) {
	bucket := "objstore-gcs-empty"
	testcommon.CreateGCSBucket(t, bucket)

	st, err := factory.NewStorage("gcs", map[string]string{"bucket": bucket})
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

func TestGCS_ErrorHandling(t *testing.T) {
	bucket := "objstore-gcs-errors"
	testcommon.CreateGCSBucket(t, bucket)

	st, err := factory.NewStorage("gcs", map[string]string{"bucket": bucket})
	if err != nil {
		t.Fatal(err)
	}

	// Test get non-existent object
	_, err = st.Get("nonexistent.txt")
	if err == nil {
		t.Fatal("Expected error for non-existent object")
	}
}

func TestGCS_Lifecycle(t *testing.T) {
	bucket := "objstore-gcs-lifecycle"
	testcommon.CreateGCSBucket(t, bucket)

	st, err := factory.NewStorage("gcs", map[string]string{"bucket": bucket})
	if err != nil {
		t.Fatal(err)
	}

	// Get initial policies
	policies, err := st.GetPolicies()
	if err != nil {
		t.Fatalf("GetPolicies failed: %v", err)
	}

	t.Logf("GCS storage lifecycle methods available, initial policy count: %d", len(policies))
}

// TestGCS_ComprehensiveSuite runs the complete test suite for all interface methods
func TestGCS_ComprehensiveSuite(t *testing.T) {
	bucket := "objstore-gcs-comprehensive"
	testcommon.CreateGCSBucket(t, bucket)

	st, err := factory.NewStorage("gcs", map[string]string{"bucket": bucket})
	if err != nil {
		t.Fatal(err)
	}

	suite := &testcommon.ComprehensiveTestSuite{
		Storage: st,
		T:       t,
	}

	suite.RunAllTests()
}
