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

package factory

import (
	"testing"

	"github.com/jeremyhahn/go-objstore/pkg/factory"
)

func TestFactory_InvalidBackend(t *testing.T) {
	_, err := factory.NewStorage("nonexistent", nil)
	if err == nil {
		t.Fatal("Expected error for invalid backend")
	}
}

func TestFactory_ArchiveOnlyAsStorage(t *testing.T) {
	// Glacier should not be allowed as primary storage
	_, err := factory.NewStorage("glacier", map[string]string{
		"vault":     "test",
		"region":    "us-east-1",
		"accessKey": "test",
		"secretKey": "test",
	})
	if err == nil {
		t.Fatal("Expected error when using archive-only backend as storage")
	}
}

func TestFactory_InvalidArchiver(t *testing.T) {
	// Test creating invalid archiver
	_, err := factory.NewArchiver("nonexistent", nil)
	if err == nil {
		t.Fatal("Expected error for invalid archiver type")
	}
}

func TestFactory_MissingConfig(t *testing.T) {
	// Test S3 without required bucket
	_, err := factory.NewStorage("s3", map[string]string{
		"region": "us-east-1",
		// missing bucket
	})
	if err == nil {
		t.Fatal("Expected error for missing bucket config")
	}
}

func TestFactory_LocalStorageCreation(t *testing.T) {
	// Test successful local storage creation
	st, err := factory.NewStorage("local", map[string]string{
		"path": "/tmp/test-factory-local",
	})
	if err != nil {
		t.Fatalf("Failed to create local storage: %v", err)
	}
	if st == nil {
		t.Fatal("Expected non-nil storage")
	}
}
