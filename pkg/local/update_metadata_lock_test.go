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
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// TestLocal_UpdateMetadataWaitsForExpiry: UpdateMetadata takes the key lock
// the expiry pass holds from its re-read of the object's age through Delete,
// so a metadata update racing an expiry cannot publish a fresh LastModified
// in that window (where expiry would then delete the refreshed object).
func TestLocal_UpdateMetadataWaitsForExpiry(t *testing.T) {
	l := new(Local)
	if err := l.Configure(map[string]string{"path": t.TempDir()}, noCreds); err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if err := l.Put("logs/a", bytes.NewBufferString("payload")); err != nil {
		t.Fatal(err)
	}

	// Stand in for the expiry pass between its re-read and its Delete.
	unlock := l.keyMu.lock("logs/a")
	done := make(chan error, 1)
	go func() {
		done <- l.UpdateMetadata(t.Context(), "logs/a", &common.Metadata{ContentType: "text/plain"})
	}()
	select {
	case err := <-done:
		unlock()
		t.Fatalf("UpdateMetadata returned (%v) while the expiry pass held the key lock", err)
	case <-time.After(200 * time.Millisecond):
	}
	unlock()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("UpdateMetadata after the lock was released: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("UpdateMetadata did not finish after the key lock was released")
	}
}
