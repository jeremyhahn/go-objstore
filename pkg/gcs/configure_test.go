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

//go:build gcpstorage

package gcs

import (
	"context"
	"errors"
	"testing"

	"cloud.google.com/go/storage"
)

// Test error variable
var errBoom = errors.New("boom")

func TestGCS_Configure_Success_WithStubClient(t *testing.T) {
	old := gcsNewClient
	gcsNewClient = func(_ context.Context) (*storage.Client, error) { return &storage.Client{}, nil }
	defer func() { gcsNewClient = old }()

	g := &GCS{}
	if err := g.Configure(map[string]string{"bucket": "b"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if g.client == nil {
		t.Fatalf("expected client set")
	}
}

func TestGCS_Configure_NewClientError(t *testing.T) {
	old := gcsNewClient
	gcsNewClient = func(_ context.Context) (*storage.Client, error) { return nil, errBoom }
	defer func() { gcsNewClient = old }()

	g := &GCS{}
	if err := g.Configure(map[string]string{"bucket": "b"}); err == nil {
		t.Fatalf("expected error from new client")
	}
}

func TestGCS_Configure_SkipClient(t *testing.T) {
	g := &GCS{}
	err := g.Configure(map[string]string{"bucket": "test-bucket", "skip_client": "true"})
	if err != nil {
		t.Fatalf("unexpected error with skip_client: %v", err)
	}
	if g.client != nil {
		t.Error("expected client to be nil with skip_client=true")
	}
}

func TestGCS_Configure_ReuseExistingClient(t *testing.T) {
	old := gcsNewClient
	callCount := 0
	gcsNewClient = func(_ context.Context) (*storage.Client, error) {
		callCount++
		return &storage.Client{}, nil
	}
	defer func() { gcsNewClient = old }()

	g := &GCS{}
	// First configure - should create client
	if err := g.Configure(map[string]string{"bucket": "b"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Second configure - should not create new client
	if err := g.Configure(map[string]string{"bucket": "b"}); err != nil {
		t.Fatalf("unexpected error on second configure: %v", err)
	}

	if callCount != 1 {
		t.Errorf("expected gcsNewClient called once, got %d", callCount)
	}
}
