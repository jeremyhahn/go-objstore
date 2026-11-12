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

import "sync"

// keyLocks serializes the writers of one key. A put holds the key's lock
// from the data write until its metadata is saved, and UpdateMetadata holds
// it from reading the stored at-rest fields until it saves; so the stored
// at-rest key ID always belongs to the bytes on disk (#243). The zero value
// is ready to use; an entry lives only while someone holds or waits for it.
type keyLocks struct {
	mu sync.Mutex
	m  map[string]*keyLock
}

type keyLock struct {
	mu   sync.Mutex
	refs int
}

// lock takes the lock of key and returns the function that releases it.
func (k *keyLocks) lock(key string) func() {
	k.mu.Lock()
	if k.m == nil {
		k.m = make(map[string]*keyLock)
	}
	e := k.m[key]
	if e == nil {
		e = &keyLock{}
		k.m[key] = e
	}
	e.refs++
	k.mu.Unlock()

	e.mu.Lock()
	return func() {
		e.mu.Unlock()
		k.mu.Lock()
		e.refs--
		if e.refs == 0 {
			delete(k.m, key)
		}
		k.mu.Unlock()
	}
}
