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

package azure

import (
	"bytes"
	"io"
	"testing"

	"github.com/jeremyhahn/go-objstore/pkg/factory"
	testcommon "github.com/jeremyhahn/go-objstore/test/integration/common"
)

func TestAzure_BasicOps(t *testing.T) {
	container := "objstoretest"
	testcommon.CreateAzuriteContainer(t, container)

	st, err := factory.NewStorage("azure", map[string]string{
		"accountName":   "devstoreaccount1",
		"accountKey":    "bXlrZXk=",
		"containerName": container,
		"endpoint":      "http://azurite:10000/devstoreaccount1",
	})
	if err != nil {
		t.Fatal(err)
	}

	obj := "it/az.txt"
	if err := st.Put(obj, bytes.NewReader([]byte("hello azurite"))); err != nil {
		t.Fatal(err)
	}
	r, err := st.Get(obj)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(r)
	r.Close()
	if string(b) == "" {
		t.Fatalf("empty read from azurite")
	}
	if err := st.Delete(obj); err != nil {
		t.Fatal(err)
	}
}

func TestAzure_EmptyFile(t *testing.T) {
	container := "objstore-azure-empty"
	testcommon.CreateAzuriteContainer(t, container)

	st, err := factory.NewStorage("azure", map[string]string{
		"accountName":   "devstoreaccount1",
		"accountKey":    "bXlrZXk=",
		"containerName": container,
		"endpoint":      "http://azurite:10000/devstoreaccount1",
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

func TestAzure_ErrorHandling(t *testing.T) {
	container := "objstore-azure-errors"
	testcommon.CreateAzuriteContainer(t, container)

	st, err := factory.NewStorage("azure", map[string]string{
		"accountName":   "devstoreaccount1",
		"accountKey":    "bXlrZXk=",
		"containerName": container,
		"endpoint":      "http://azurite:10000/devstoreaccount1",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Test get non-existent blob
	_, err = st.Get("nonexistent.txt")
	if err == nil {
		t.Fatal("Expected error for non-existent blob")
	}
}

func TestAzure_Lifecycle(t *testing.T) {
	container := "objstore-azure-lifecycle"
	testcommon.CreateAzuriteContainer(t, container)

	st, err := factory.NewStorage("azure", map[string]string{
		"accountName":   "devstoreaccount1",
		"accountKey":    "bXlrZXk=",
		"containerName": container,
		"endpoint":      "http://azurite:10000/devstoreaccount1",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Get initial policies
	policies, err := st.GetPolicies()
	if err != nil {
		t.Fatalf("GetPolicies failed: %v", err)
	}

	t.Logf("Azure storage lifecycle methods available, initial policy count: %d", len(policies))
}

// TestAzure_ComprehensiveSuite runs the complete test suite for all interface methods
func TestAzure_ComprehensiveSuite(t *testing.T) {
	container := "objstore-azure-comprehensive"
	testcommon.CreateAzuriteContainer(t, container)

	st, err := factory.NewStorage("azure", map[string]string{
		"accountName":   "devstoreaccount1",
		"accountKey":    "bXlrZXk=",
		"containerName": container,
		"endpoint":      "http://azurite:10000/devstoreaccount1",
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
