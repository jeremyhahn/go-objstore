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

type keyLock struct {
	mu   sync.Mutex
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
		e = &keyLock{}
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
	e.mu.Lock()
	return func() {
		e.mu.Unlock()
		k.release(name, e)
	}
}
