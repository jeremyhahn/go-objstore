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
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/storagefs"
)

// storageFSAdaptable gives storagefs.StorageFS the OpenFile signature
// common.NewFileSystemAdapter takes.
type storageFSAdaptable struct{ fs *storagefs.StorageFS }

func (s storageFSAdaptable) OpenFile(name string, flag int, perm os.FileMode) (any, error) {
	return s.fs.OpenFile(name, flag, perm)
}
func (s storageFSAdaptable) Remove(name string) error     { return s.fs.Remove(name) }
func (s storageFSAdaptable) Rename(src, dst string) error { return s.fs.Rename(src, dst) }

// TestPersistentLifecycle_ThroughStorageFS: a PersistentLifecycleManager
// backed by a StorageFS over a provider that validates keys (the local
// backend refuses ".tmp-" keys) can add and remove policies, and they
// persist across a new manager.
func TestPersistentLifecycle_ThroughStorageFS(t *testing.T) {
	store := new(Local)
	if err := store.Configure(map[string]string{"path": t.TempDir()}, noCreds); err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	// An existing policy document, as a manager that ran before left it.
	if err := store.Put("policies.json", strings.NewReader(`{"policies":[]}`)); err != nil {
		t.Fatal(err)
	}
	fsys := common.NewFileSystemAdapter(storageFSAdaptable{fs: storagefs.New(store)})

	lm, err := common.NewPersistentLifecycleManager(fsys, "policies.json")
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"a", "b"} {
		if err := lm.AddPolicy(common.LifecyclePolicy{ID: id, Prefix: id + "/", Retention: time.Hour, Action: "delete"}); err != nil {
			t.Fatalf("AddPolicy(%s): %v", id, err)
		}
	}
	if err := lm.RemovePolicy("a"); err != nil {
		t.Fatalf("RemovePolicy(a): %v", err)
	}

	reloaded, err := common.NewPersistentLifecycleManager(fsys, "policies.json")
	if err != nil {
		t.Fatal(err)
	}
	policies, err := reloaded.GetPolicies()
	if err != nil {
		t.Fatal(err)
	}
	if len(policies) != 1 || policies[0].ID != "b" {
		t.Fatalf("policies after reload = %+v, want only b", policies)
	}
}
