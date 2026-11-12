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
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// pausingEncrypter is a keyedEncrypter whose first KeyID call, which a put
// makes after writing the data and before saving the metadata, waits until
// the test releases it.
type pausingEncrypter struct {
	keyedEncrypter
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (p *pausingEncrypter) KeyID() string {
	p.once.Do(func() {
		close(p.entered)
		<-p.release
	})
	return p.keyedEncrypter.KeyID()
}

// sequenceFactory hands out its encrypters in order for new writes and
// looks them up by key ID for reads.
type sequenceFactory struct {
	mu   sync.Mutex
	next []common.Encrypter
	byID map[string]common.Encrypter
}

func (f *sequenceFactory) GetEncrypter(id string) (common.Encrypter, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if id != "" {
		return f.byID[id], nil
	}
	e := f.next[0]
	f.next = f.next[1:]
	return e, nil
}
func (f *sequenceFactory) DefaultKeyID() string { return "" }
func (f *sequenceFactory) Close() error         { return nil }

// Two overlapping puts of one key must not leave the bytes of one and the
// at-rest key ID of the other (#243).
func TestAtRestOverlappingPutsKeepKeyIDWithData(t *testing.T) {
	a := &pausingEncrypter{keyedEncrypter: keyedEncrypter{id: "A"}, entered: make(chan struct{}), release: make(chan struct{})}
	b := &keyedEncrypter{id: "B"}
	f := &sequenceFactory{next: []common.Encrypter{a, b}, byID: map[string]common.Encrypter{"A": a, "B": b}}
	l := newAtRestLocal(t, f)
	ctx := context.Background()

	errs := make(chan error, 2)
	go func() { errs <- l.PutWithMetadata(ctx, "obj", bytes.NewReader([]byte("one")), nil) }()
	<-a.entered // the first put has written its data, not yet its metadata

	second := make(chan struct{})
	go func() { errs <- l.PutWithMetadata(ctx, "obj", bytes.NewReader([]byte("two")), nil); close(second) }()
	select {
	case <-second: // no per-key lock: the second put overtook the first
	case <-time.After(200 * time.Millisecond): // the second put waits for the first
	}
	close(a.release)
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatalf("put: %v", err)
		}
	}

	md, err := l.GetMetadata(ctx, "obj")
	if err != nil {
		t.Fatalf("GetMetadata: %v", err)
	}
	got, err := readAll(t, l, "obj")
	if err != nil {
		t.Fatalf("read with recorded key %q: %v", md.Custom["at_rest_encryption_key_id"], err)
	}
	if got != "two" {
		t.Fatalf("read %q, want the later put %q", got, "two")
	}
}

func TestKeyLocksReleaseEntries(t *testing.T) {
	var k keyLocks
	unlock := k.lock("a")
	done := make(chan struct{})
	go func() { k.lock("a")(); close(done) }()
	select {
	case <-done:
		t.Fatal("second lock of the same key did not wait")
	case <-time.After(50 * time.Millisecond):
	}
	k.lock("b")() // another key does not wait
	unlock()
	<-done
	if len(k.m) != 0 {
		t.Fatalf("entries left after release: %d", len(k.m))
	}
}
