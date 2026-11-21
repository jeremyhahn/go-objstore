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
	"errors"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

var _ common.ConditionalDeleter = (*Local)(nil)

// TestLocal_DeleteIfUnmodified deletes only the revision the caller saw.
func TestLocal_DeleteIfUnmodified(t *testing.T) {
	l := new(Local)
	if err := l.Configure(map[string]string{"path": t.TempDir()}, noCreds); err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	ctx := context.Background()
	if err := l.Put("a", bytes.NewBufferString("x")); err != nil {
		t.Fatal(err)
	}
	if err := l.UpdateMetadata(ctx, "a", &common.Metadata{}); err != nil {
		t.Fatal(err)
	}
	seen, err := l.GetMetadata(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}

	if err := l.DeleteIfUnmodified(ctx, "a", seen.LastModified.Add(-time.Second)); !errors.Is(err, common.ErrObjectChanged) {
		t.Fatalf("stale revision: err = %v, want ErrObjectChanged", err)
	}
	if ok, _ := l.Exists(ctx, "a"); !ok {
		t.Fatal("a stale-revision delete removed the object")
	}
	if err := l.DeleteIfUnmodified(ctx, "a", seen.LastModified); err != nil {
		t.Fatalf("current revision: %v", err)
	}
	if err := l.DeleteIfUnmodified(ctx, "a", seen.LastModified); !errors.Is(err, common.ErrKeyNotFound) {
		t.Fatalf("gone: err = %v, want ErrKeyNotFound", err)
	}
	if err := l.DeleteIfUnmodified(ctx, "", time.Time{}); err == nil {
		t.Fatal("empty key accepted")
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := l.DeleteIfUnmodified(cancelled, "b", time.Time{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled: err = %v", err)
	}
}
