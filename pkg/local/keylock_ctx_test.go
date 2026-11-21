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
	"io"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// TestKeyLocks_LockCtx: lockCtx returns the context's error when the context
// ends while it waits, leaves the lock to its holder, and drops its own
// reference so the entry goes once the holder unlocks.
func TestKeyLocks_LockCtx(t *testing.T) {
	var k keyLocks
	unlock := k.lock("a")

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if u, err := k.lockCtx(ctx, "a"); !errors.Is(err, context.DeadlineExceeded) || u != nil {
		t.Fatalf("lockCtx while held = (%v, %v), want (nil, DeadlineExceeded)", u != nil, err)
	}

	done, stop := context.WithCancel(t.Context())
	stop()
	unlock()
	if u, err := k.lockCtx(done, "a"); !errors.Is(err, context.Canceled) || u != nil {
		t.Fatalf("lockCtx with an ended context = (%v, %v), want (nil, Canceled)", u != nil, err)
	}
	if n := len(k.locks); n != 0 {
		t.Fatalf("%d lock entries left after every caller let go, want 0", n)
	}

	u, err := k.lockCtx(t.Context(), "A")
	if err != nil {
		t.Fatalf("lockCtx on a free key: %v", err)
	}
	u()
}

// TestLocal_WritersHonourCancelWhileLocked: a writer waiting on the key lock
// returns its context's error when the context is cancelled, and the object
// and its metadata are left as they were.
func TestLocal_WritersHonourCancelWhileLocked(t *testing.T) {
	l := new(Local)
	if err := l.Configure(map[string]string{"path": t.TempDir()}, noCreds); err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	const key = "logs/a"
	if err := l.PutWithMetadata(t.Context(), key, bytes.NewBufferString("payload"),
		&common.Metadata{ContentType: "text/original"}); err != nil {
		t.Fatal(err)
	}
	before, err := l.GetMetadata(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}

	writers := map[string]func(ctx context.Context) error{
		"PutWithMetadata": func(ctx context.Context) error {
			return l.PutWithMetadata(ctx, key, bytes.NewBufferString("changed"), &common.Metadata{ContentType: "text/changed"})
		},
		"UpdateMetadata": func(ctx context.Context) error {
			return l.UpdateMetadata(ctx, key, &common.Metadata{ContentType: "text/changed"})
		},
		"DeleteWithContext": func(ctx context.Context) error {
			return l.DeleteWithContext(ctx, key)
		},
		"DeleteIfUnmodified": func(ctx context.Context) error {
			return l.DeleteIfUnmodified(ctx, key, before.LastModified)
		},
	}
	for name, write := range writers {
		t.Run(name, func(t *testing.T) {
			unlock := l.keyMu.lock(key)
			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan error, 1)
			go func() { done <- write(ctx) }()
			time.Sleep(50 * time.Millisecond)
			cancel()
			select {
			case err := <-done:
				unlock()
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("%s = %v, want context.Canceled", name, err)
				}
			case <-time.After(5 * time.Second):
				unlock()
				t.Fatalf("%s did not return after its context was cancelled", name)
			}

			rc, err := l.GetWithContext(t.Context(), key)
			if err != nil {
				t.Fatalf("object gone after a cancelled %s: %v", name, err)
			}
			got, _ := io.ReadAll(rc)
			_ = rc.Close()
			if string(got) != "payload" {
				t.Fatalf("object = %q after a cancelled %s, want %q", got, name, "payload")
			}
			after, err := l.GetMetadata(t.Context(), key)
			if err != nil {
				t.Fatal(err)
			}
			if after.ContentType != before.ContentType || !after.LastModified.Equal(before.LastModified) {
				t.Fatalf("metadata changed by a cancelled %s: %+v, want %+v", name, after, before)
			}
		})
	}
}
