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


package local

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

type mockArchiver struct{ keys []string }

func (m *mockArchiver) Put(key string, r io.Reader) error {
	m.keys = append(m.keys, key)
	_, _ = io.Copy(io.Discard, r)
	return nil
}

func TestLifecycle_Process_Delete(t *testing.T) {
	dir := t.TempDir()
	s := New()
	if err := s.Configure(map[string]string{"path": dir}); err != nil {
		t.Fatal(err)
	}
	ll := s.(*Local)

	// Ensure we're using the in-memory lifecycle manager for this test
	memManager, ok := ll.lifecycleManager.(*LifecycleManager)
	if !ok {
		t.Fatal("expected in-memory lifecycle manager")
	}

	// create old file matching prefix
	key := "logs/old.txt"
	if err := s.Put(key, bytes.NewBufferString("data")); err != nil {
		t.Fatal(err)
	}
	// set modtime to past
	path := filepath.Join(dir, key)
	past := time.Now().Add(-48 * time.Hour)
	if err := os.Chtimes(path, past, past); err != nil {
		t.Fatal(err)
	}

	// add policy to delete anything under logs/ older than 1h
	if err := s.AddPolicy(common.LifecyclePolicy{ID: "p1", Prefix: "logs/", Retention: time.Hour, Action: "delete"}); err != nil {
		t.Fatal(err)
	}

	memManager.Process(ll)

	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("expected file deleted by lifecycle, got err=%v", err)
	}
}

func TestLifecycle_Process_Archive(t *testing.T) {
	dir := t.TempDir()
	s := New()
	if err := s.Configure(map[string]string{"path": dir}); err != nil {
		t.Fatal(err)
	}
	ll := s.(*Local)

	// Ensure we're using the in-memory lifecycle manager for this test
	memManager, ok := ll.lifecycleManager.(*LifecycleManager)
	if !ok {
		t.Fatal("expected in-memory lifecycle manager")
	}

	key := "docs/old.txt"
	if err := s.Put(key, bytes.NewBufferString("data")); err != nil {
		t.Fatal(err)
	}
	pth := filepath.Join(dir, key)
	past := time.Now().Add(-72 * time.Hour)
	if err := os.Chtimes(pth, past, past); err != nil {
		t.Fatal(err)
	}

	ma := &mockArchiver{}
	if err := s.AddPolicy(common.LifecyclePolicy{ID: "p2", Prefix: "docs/", Retention: time.Hour, Action: "archive", Destination: ma}); err != nil {
		t.Fatal(err)
	}

	memManager.Process(ll)

	if len(ma.keys) != 1 || ma.keys[0] != key {
		t.Fatalf("expected archived key %s, got %v", key, ma.keys)
	}
}
