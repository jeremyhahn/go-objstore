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

package common

import (
	"os"
	"testing"
	"time"
)

// recordingFS records every name save opens, so a test can check that the
// staging file is one no object key can reach.
type recordingFS struct {
	*mockFileSystem
	opened  []string
	renamed [][2]string
}

// ReservesStagingNames makes save take the staging path the test checks.
func (r *recordingFS) ReservesStagingNames() bool { return true }

func (r *recordingFS) Rename(src, dst string) error {
	r.renamed = append(r.renamed, [2]string{src, dst})
	return r.mockFileSystem.Rename(src, dst)
}

func (r *recordingFS) OpenFile(name string, flag int, perm os.FileMode) (LifecycleFile, error) {
	r.opened = append(r.opened, name)
	return r.mockFileSystem.OpenFile(name, flag, perm)
}

// TestPersistentLifecycleManager_StagingNameIsReserved: the file save writes
// before renaming it over the policy file must be a name ValidateKey refuses,
// for the default policy file and for custom ones; otherwise a client with
// object write access could PUT that key and have it renamed over the policies.
func TestPersistentLifecycleManager_StagingNameIsReserved(t *testing.T) {
	for _, policyFile := range []string{"", "rules.json", "conf/lifecycle.json"} {
		fs := &recordingFS{mockFileSystem: newMockFileSystem()}
		lm, err := NewPersistentLifecycleManager(fs, policyFile)
		if err != nil {
			t.Fatal(err)
		}
		fs.opened, fs.renamed = nil, nil
		if err := lm.AddPolicy(LifecyclePolicy{ID: "p", Action: "delete", Retention: time.Hour}); err != nil {
			t.Fatal(err)
		}
		staging := PolicyStagingName(lm.policyFile)
		if ValidateKey(staging) == nil {
			t.Errorf("policy file %q: staging file %q is a valid object key", policyFile, staging)
		}
		if len(fs.opened) != 1 || fs.opened[0] != staging {
			t.Errorf("policy file %q: save opened %q, want only the staging file %q", policyFile, fs.opened, staging)
		}
		if len(fs.renamed) != 1 || fs.renamed[0] != [2]string{staging, lm.policyFile} {
			t.Errorf("policy file %q: save renamed %q, want %q onto %q", policyFile, fs.renamed, staging, lm.policyFile)
		}
		if fs.getData(lm.policyFile) == nil {
			t.Errorf("policy file %q: not written", policyFile)
		}
	}
}
