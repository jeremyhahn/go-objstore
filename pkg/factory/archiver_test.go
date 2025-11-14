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
	"bytes"
	"errors"
	"os"
	"testing"
)

func TestNewArchiver(t *testing.T) {
	// Test Glacier archiver - either succeeds or returns ErrUnknownArchiver
	_, err := NewArchiver("glacier", map[string]string{"vaultName": "test-vault", "region": "us-east-1"})
	if err != nil {
		if !errors.Is(err, ErrUnknownArchiver) {
			t.Fatalf("expected ErrUnknownArchiver or success, got: %v", err)
		}
	}

	// Test Azure Archive archiver - either succeeds or returns ErrUnknownArchiver
	_, err = NewArchiver("azurearchive", map[string]string{"accountName": "test-account", "accountKey": "dGVzdC1rZXk=", "containerName": "test-container"})
	if err != nil {
		if !errors.Is(err, ErrUnknownArchiver) {
			t.Fatalf("expected ErrUnknownArchiver or success, got: %v", err)
		}
	}

	// Test unknown archiver - should always fail
	_, err = NewArchiver("unknown", nil)
	if err == nil {
		t.Fatal("expected error for unknown archiver, got nil")
	}
}

func TestArchiveOnlyBackend(t *testing.T) {
	_, err := NewStorage("glacier", nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	_, err = NewStorage("azurearchive", nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestGlacier(t *testing.T) {
	vaultName := os.Getenv("AWS_GLACIER_VAULT_NAME")
	region := os.Getenv("AWS_REGION")

	archiver, err := NewArchiver("glacier", map[string]string{
		"vaultName": vaultName,
		"region":    region,
	})

	if err != nil {
		if errors.Is(err, ErrUnknownArchiver) {
			return
		}
		if vaultName == "" || region == "" {
			return
		}
		t.Fatalf("unexpected error: %v", err)
	}

	if vaultName == "" || region == "" {
		t.Log("Glacier backend available but no credentials configured")
		return
	}

	key := "test-key"
	data := []byte("test-data")

	err = archiver.Put(key, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
}

func TestAzureArchive(t *testing.T) {
	accountName := os.Getenv("AZURE_ACCOUNT_NAME")
	accountKey := os.Getenv("AZURE_ACCOUNT_KEY")
	containerName := os.Getenv("AZURE_CONTAINER_NAME")

	archiver, err := NewArchiver("azurearchive", map[string]string{
		"accountName":   accountName,
		"accountKey":    accountKey,
		"containerName": containerName,
	})

	if err != nil {
		if errors.Is(err, ErrUnknownArchiver) {
			return
		}
		if accountName == "" || accountKey == "" || containerName == "" {
			return
		}
		t.Fatalf("unexpected error: %v", err)
	}

	if accountName == "" || accountKey == "" || containerName == "" {
		t.Log("Azure Archive backend available but no credentials configured")
		return
	}

	key := "test-key"
	data := []byte("test-data")

	err = archiver.Put(key, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
}
