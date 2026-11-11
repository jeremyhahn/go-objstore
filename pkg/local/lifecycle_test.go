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
	if err := s.Configure(map[string]string{"path": dir}, noCreds); err != nil {
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
	ageObject(t, ll, dir, key, time.Now().Add(-48*time.Hour))

	// add policy to delete anything under logs/ older than 1h
	if err := s.AddPolicy(common.LifecyclePolicy{ID: "p1", Prefix: "logs/", Retention: time.Hour, Action: "delete"}); err != nil {
		t.Fatal(err)
	}

	memManager.Process(ll)

	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("expected file deleted by lifecycle, got err=%v", err)
	}
}

// An object replaced between lifecycle expiry's decision and its delete
// survives: the delete re-reads the age under the key's write lock (#224).
func TestLifecycle_Process_ReplacedAfterDecisionSurvives(t *testing.T) {
	dir := t.TempDir()
	s := New()
	if err := s.Configure(map[string]string{"path": dir}, noCreds); err != nil {
		t.Fatal(err)
	}
	ll := s.(*Local)
	memManager, ok := ll.lifecycleManager.(*LifecycleManager)
	if !ok {
		t.Fatal("expected in-memory lifecycle manager")
	}

	key := "logs/old.txt"
	if err := s.Put(key, bytes.NewBufferString("old")); err != nil {
		t.Fatal(err)
	}
	ageObject(t, ll, dir, key, time.Now().Add(-48*time.Hour))
	if err := s.AddPolicy(common.LifecyclePolicy{ID: "p1", Prefix: "logs/", Retention: time.Hour, Action: "delete"}); err != nil {
		t.Fatal(err)
	}

	replaced := false
	ll.expiryHook = func(k string) {
		if k != key || replaced {
			return
		}
		replaced = true
		if err := s.Put(key, bytes.NewBufferString("new")); err != nil {
			t.Errorf("replacing %s: %v", key, err)
		}
	}
	memManager.Process(ll)

	if !replaced {
		t.Fatal("lifecycle never decided to expire the old object")
	}
	rc, err := s.Get(key)
	if err != nil {
		t.Fatalf("replacement written after the expiry decision was deleted: %v", err)
	}
	defer func() { _ = rc.Close() }()
	got, err := io.ReadAll(rc)
	if err != nil || string(got) != "new" {
		t.Fatalf("Get(%s) = %q, %v; want the replacement %q", key, got, err, "new")
	}
}

func TestLifecycle_Process_Archive(t *testing.T) {
	dir := t.TempDir()
	s := New()
	if err := s.Configure(map[string]string{"path": dir}, noCreds); err != nil {
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
	ageObject(t, ll, dir, key, time.Now().Add(-72*time.Hour))

	ma := &mockArchiver{}
	if err := s.AddPolicy(common.LifecyclePolicy{ID: "p2", Prefix: "docs/", Retention: time.Hour, Action: "archive", Destination: ma}); err != nil {
		t.Fatal(err)
	}

	memManager.Process(ll)

	if len(ma.keys) != 1 || ma.keys[0] != key {
		t.Fatalf("expected archived key %s, got %v", key, ma.keys)
	}
}
