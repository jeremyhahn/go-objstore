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
	"context"
	"strings"
	"sync"
)

// keyLocks is a set of mutexes, one per object key, created on first use
// and dropped when no caller holds or waits for it. Keys that name the same
// file (the store compares keys without case) share one lock. The zero
// value is ready to use.
type keyLocks struct {
	mu    sync.Mutex
	locks map[string]*keyLock
}

// keyLock is held while its one-slot channel is full: a channel rather than
// a mutex, so lockCtx can stop waiting when its context ends.
type keyLock struct {
	held chan struct{}
	refs int
}

func lockName(key string) string {
	return strings.ToLower(rootName(key))
}

// acquire returns the entry for key with a reference taken.
func (k *keyLocks) acquire(name string) *keyLock {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.locks == nil {
		k.locks = make(map[string]*keyLock)
	}
	e := k.locks[name]
	if e == nil {
		e = &keyLock{held: make(chan struct{}, 1)}
		k.locks[name] = e
	}
	e.refs++
	return e
}

// release drops a reference to the entry for name, and the entry with it
// when it was the last.
func (k *keyLocks) release(name string, e *keyLock) {
	k.mu.Lock()
	defer k.mu.Unlock()
	e.refs--
	if e.refs == 0 {
		delete(k.locks, name)
	}
}

// lock locks key and returns the function that unlocks it.
func (k *keyLocks) lock(key string) (unlock func()) {
	name := lockName(key)
	e := k.acquire(name)
	e.held <- struct{}{}
	return k.unlocker(name, e)
}

// lockCtx locks key as lock does, but stops waiting and returns ctx.Err()
// when ctx ends first. It checks ctx again once the lock is held, so a
// caller whose context ended while it waited does not go on to write; in
// both cases the lock is not held and unlock is nil.
func (k *keyLocks) lockCtx(ctx context.Context, key string) (unlock func(), err error) {
	name := lockName(key)
	e := k.acquire(name)
	select {
	case e.held <- struct{}{}:
	case <-ctx.Done():
		k.release(name, e)
		return nil, ctx.Err()
	}
	unlock = k.unlocker(name, e)
	if err := ctx.Err(); err != nil {
		unlock()
		return nil, err
	}
	return unlock, nil
}

// unlocker returns the function that unlocks the held entry e for name.
func (k *keyLocks) unlocker(name string, e *keyLock) func() {
	return func() {
		<-e.held
		k.release(name, e)
	}
}
