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
	"errors"
	"testing"
	"time"
)

// TestPersistentLifecycleManager_FailedSaveLeavesPoliciesUnchanged: a policy
// change that fails to persist leaves the live policies as they were, so the
// scheduled pass never runs a rule that is not on disk and never drops one
// that still is.
func TestPersistentLifecycleManager_FailedSaveLeavesPoliciesUnchanged(t *testing.T) {
	inner := newMockFileSystem()
	lm, err := NewPersistentLifecycleManager(inner, "policies.json")
	if err != nil {
		t.Fatal(err)
	}
	orig := LifecyclePolicy{ID: "p1", Prefix: "logs/", Action: "delete", Retention: time.Hour}
	if err := lm.AddPolicy(orig); err != nil {
		t.Fatal(err)
	}

	lm.fs = &renameErrFS{inner: inner}

	// A new policy that fails to persist is not live.
	if err := lm.AddPolicy(LifecyclePolicy{ID: "p2", Action: "delete", Retention: time.Second}); !errors.Is(err, errFS) {
		t.Fatalf("AddPolicy(p2) error = %v, want errFS", err)
	}
	// A replacement that fails to persist keeps the previous version.
	if err := lm.AddPolicy(LifecyclePolicy{ID: "p1", Prefix: "", Action: "delete", Retention: time.Second}); !errors.Is(err, errFS) {
		t.Fatalf("AddPolicy(p1 replacement) error = %v, want errFS", err)
	}
	// A removal that fails to persist keeps the policy.
	if err := lm.RemovePolicy("p1"); !errors.Is(err, errFS) {
		t.Fatalf("RemovePolicy(p1) error = %v, want errFS", err)
	}

	policies, err := lm.GetPolicies()
	if err != nil {
		t.Fatal(err)
	}
	if len(policies) != 1 || policies[0] != orig {
		t.Fatalf("policies after failed saves = %+v, want only %+v", policies, orig)
	}
}
