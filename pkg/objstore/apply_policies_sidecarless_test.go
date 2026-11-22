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

package objstore

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/local"
)

// recordingArchiver records the keys archived to it and fails the keys in
// fail.
type recordingArchiver struct {
	mu   sync.Mutex
	keys []string
	fail map[string]error
}

func (r *recordingArchiver) Put(key string, data io.Reader) error {
	if _, err := io.Copy(io.Discard, data); err != nil {
		return err
	}
	if err := r.fail[key]; err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.keys = append(r.keys, key)
	return nil
}

// rewritingLister rewrites key, without a sidecar and at the same size,
// once the listing has finished: between ApplyPolicies' listing and its
// actions.
type rewritingLister struct {
	*local.Local
	t   *testing.T
	dir string
	key string
}

func (r *rewritingLister) ListWithOptions(ctx context.Context, opts *common.ListOptions) (*common.ListResult, error) {
	page, err := r.Local.ListWithOptions(ctx, opts)
	if err == nil && !page.Truncated {
		if werr := os.WriteFile(filepath.Join(r.dir, r.key), []byte("new"), 0o644); werr != nil {
			r.t.Fatal(werr)
		}
	}
	return page, err
}

// TestApplyPolicies_ArchiveRechecksSidecarlessObjects: an archive policy
// rechecks an object stored without a metadata sidecar against its size and
// modification time, so it archives the unchanged ones, leaves one rewritten
// after the listing alone, and reports an action that fails.
func TestApplyPolicies_ArchiveRechecksSidecarlessObjects(t *testing.T) {
	dir := t.TempDir()
	store, ok := local.New().(*local.Local)
	if !ok {
		t.Fatal("local.New() is not a *local.Local")
	}
	if err := store.Configure(map[string]string{"path": dir}, common.Credentials{}); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "logs"), 0o755); err != nil {
		t.Fatal(err)
	}
	past := time.Now().Add(-48 * time.Hour)
	for _, key := range []string{"logs/a.txt", "logs/b.txt", "logs/c.txt"} {
		path := filepath.Join(dir, key)
		if err := os.WriteFile(path, []byte("old"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(path, past, past); err != nil {
			t.Fatal(err)
		}
	}
	errArchive := errors.New("archive unavailable")
	archiver := &recordingArchiver{fail: map[string]error{"logs/c.txt": errArchive}}
	if err := store.AddPolicy(common.LifecyclePolicy{
		ID: "logs", Prefix: "logs/", Retention: time.Hour, Action: "archive", Destination: archiver,
	}); err != nil {
		t.Fatal(err)
	}

	Reset()
	t.Cleanup(Reset)
	if err := Initialize(&FacadeConfig{
		Backends:       map[string]common.Storage{"loc": &rewritingLister{Local: store, t: t, dir: dir, key: "logs/a.txt"}},
		DefaultBackend: "loc",
	}); err != nil {
		t.Fatalf("Initialize: %v", err)
	}

	failures := map[string]error{}
	result, err := ApplyPolicies(context.Background(), "loc", func(key, _ string, err error) {
		failures[key] = err
	})
	if err != nil {
		t.Fatalf("ApplyPolicies: %v", err)
	}
	if len(failures) != 1 || !errors.Is(failures["logs/c.txt"], errArchive) {
		t.Fatalf("action failures = %v, want only logs/c.txt: %v", failures, errArchive)
	}
	sort.Strings(archiver.keys)
	if len(archiver.keys) != 1 || archiver.keys[0] != "logs/b.txt" {
		t.Errorf("archived %v, want [logs/b.txt]", archiver.keys)
	}
	if result.ObjectsProcessed != 1 {
		t.Errorf("ObjectsProcessed = %d, want 1", result.ObjectsProcessed)
	}
}
