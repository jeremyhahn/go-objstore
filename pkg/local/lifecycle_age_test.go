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
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// TestLocal_ScheduledPassAgesLikeManualPass: after UpdateMetadata on an old
// object, the scheduled pass keeps it, exactly as a manual ApplyPolicies does
// (which ages objects by the listing's Metadata.LastModified); an old object
// with no update is expired by both.
func TestLocal_ScheduledPassAgesLikeManualPass(t *testing.T) {
	dir := t.TempDir()
	l := new(Local)
	if err := l.Configure(map[string]string{"path": dir}, noCreds); err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	policy := common.LifecyclePolicy{ID: "expire-logs", Prefix: "logs/", Retention: time.Hour, Action: "delete"}
	if err := l.AddPolicy(policy); err != nil {
		t.Fatal(err)
	}

	updated, stale := "logs/updated.txt", "logs/stale.txt"
	old := time.Now().Add(-48 * time.Hour)
	for _, key := range []string{updated, stale} {
		if err := l.Put(key, bytes.NewBufferString("data")); err != nil {
			t.Fatal(err)
		}
		ageObject(t, l, dir, key, old)
	}
	// The data file keeps its old mtime; the metadata LastModified is now.
	if err := l.UpdateMetadata(context.Background(), updated, &common.Metadata{ContentType: "text/plain"}); err != nil {
		t.Fatal(err)
	}

	// What a manual pass sees: the listing's LastModified.
	listed, err := l.ListWithOptions(context.Background(), &common.ListOptions{Prefix: "logs/"})
	if err != nil {
		t.Fatal(err)
	}
	manualExpires := map[string]bool{}
	for _, obj := range listed.Objects {
		manualExpires[obj.Key] = time.Since(obj.Metadata.LastModified) > policy.Retention
	}

	l.processLifecycle()

	for _, key := range []string{updated, stale} {
		_, statErr := os.Stat(filepath.Join(dir, key))
		scheduledExpired := os.IsNotExist(statErr)
		if scheduledExpired != manualExpires[key] {
			t.Errorf("%s: scheduled pass expired=%v, manual pass expired=%v", key, scheduledExpired, manualExpires[key])
		}
	}
	if manualExpires[updated] || !manualExpires[stale] {
		t.Fatalf("manual pass expiry = %v, want %s kept and %s expired", manualExpires, updated, stale)
	}
}
