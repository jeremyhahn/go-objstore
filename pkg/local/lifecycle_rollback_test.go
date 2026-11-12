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

// readOnlyPolicyFS has no policy file and refuses every write, so each save
// of the persistent lifecycle manager fails.
type readOnlyPolicyFS struct{}

func (readOnlyPolicyFS) OpenFile(_ string, flag int, _ os.FileMode) (common.LifecycleFile, error) {
	if flag&(os.O_WRONLY|os.O_RDWR|os.O_CREATE) != 0 {
		return nil, os.ErrPermission
	}
	return nil, os.ErrNotExist
}
func (readOnlyPolicyFS) Remove(string) error         { return os.ErrPermission }
func (readOnlyPolicyFS) Rename(string, string) error { return os.ErrPermission }

// ageObject backdates key both on disk and in its metadata sidecar.
func ageObject(t *testing.T, l *Local, dir, key string, when time.Time) {
	t.Helper()
	if err := os.Chtimes(filepath.Join(dir, key), when, when); err != nil {
		t.Fatal(err)
	}
	md, err := l.loadMetadata(key)
	if err != nil {
		md = &common.Metadata{}
	}
	md.LastModified = when
	if err := l.saveMetadata(key, md); err != nil {
		t.Fatal(err)
	}
}

// TestLocal_ScheduledPassSkipsPolicyThatFailedToPersist: AddPolicy reports
// the failed save, and the next scheduled pass does not apply the rejected
// rule.
func TestLocal_ScheduledPassSkipsPolicyThatFailedToPersist(t *testing.T) {
	dir := t.TempDir()
	l := new(Local)
	if err := l.Configure(map[string]string{"path": dir}, noCreds); err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	mgr, err := common.NewPersistentLifecycleManager(readOnlyPolicyFS{}, "")
	if err != nil {
		t.Fatal(err)
	}
	l.lifecycleManager = mgr

	key := "logs/old.txt"
	if err := l.Put(key, bytes.NewBufferString("data")); err != nil {
		t.Fatal(err)
	}
	ageObject(t, l, dir, key, time.Now().Add(-48*time.Hour))

	if err := l.AddPolicy(common.LifecyclePolicy{ID: "expire-logs", Prefix: "logs/", Retention: time.Hour, Action: "delete"}); err == nil {
		t.Fatal("AddPolicy succeeded although the policy file cannot be written")
	}

	l.processLifecycle()

	if _, err := os.Stat(filepath.Join(dir, key)); err != nil {
		t.Fatalf("the scheduled pass applied a policy that failed to persist: %s err=%v", key, err)
	}
}
