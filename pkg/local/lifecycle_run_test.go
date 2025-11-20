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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

func TestLifecycle_Run_DeletesOldFiles(t *testing.T) {
	dir := t.TempDir()
	s := New()
	// Use in-memory lifecycle manager (default)
	if err := s.Configure(map[string]string{"path": dir}); err != nil {
		t.Fatal(err)
	}
	ll := s.(*Local)

	// Ensure we're using the in-memory lifecycle manager for this test
	memManager, ok := ll.lifecycleManager.(*LifecycleManager)
	if !ok {
		t.Fatal("expected in-memory lifecycle manager")
	}

	// create old file
	key := "logs/old.txt"
	if err := s.Put(key, bytes.NewBufferString("data")); err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(dir, key)
	old := time.Now().Add(-48 * time.Hour)
	if err := os.Chtimes(p, old, old); err != nil {
		t.Fatal(err)
	}

	// policy to delete under logs older than 1h
	if err := s.AddPolicy(common.LifecyclePolicy{ID: "run", Prefix: "logs/", Retention: time.Hour, Action: "delete"}); err != nil {
		t.Fatal(err)
	}

	// speed up interval and run
	memManager.interval = 10 * time.Millisecond
	done := make(chan struct{})
	go func() { memManager.Run(ll); close(done) }()

	// wait a bit and verify deletion
	time.Sleep(50 * time.Millisecond)
	if _, err := os.Stat(p); !os.IsNotExist(err) {
		t.Fatalf("expected file deleted, got err=%v", err)
	}
}
