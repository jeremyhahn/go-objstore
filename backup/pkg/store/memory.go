package store

import (
	"context"
	"sort"
	"strings"
	"sync"
)

// Memory is a Store held in memory, for tests.
type Memory struct {
	mu      sync.RWMutex
	objects map[string][]byte
}

// NewMemory returns an empty Memory store.
func NewMemory() *Memory {
	return &Memory{objects: map[string][]byte{}}
}

// Put stores a copy of data under name, once.
func (m *Memory) Put(ctx context.Context, name string, data []byte) error {
	if err := check(ctx, "put", name); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.objects[name]; ok {
		return newError("put", name, ErrExists, nil)
	}
	m.objects[name] = append([]byte{}, data...)
	return nil
}

// Get returns a copy of the object's bytes.
func (m *Memory) Get(ctx context.Context, name string) ([]byte, error) {
	if err := check(ctx, "get", name); err != nil {
		return nil, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	data, ok := m.objects[name]
	if !ok {
		return nil, newError("get", name, ErrNotFound, nil)
	}
	return append([]byte{}, data...), nil
}

// List returns the names that begin with prefix, sorted.
func (m *Memory) List(ctx context.Context, prefix string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, newError("list", prefix, ErrBackend, err)
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	var names []string
	for name := range m.objects {
		if strings.HasPrefix(name, prefix) {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names, nil
}

// Exists reports whether name exists.
func (m *Memory) Exists(ctx context.Context, name string) (bool, error) {
	if err := check(ctx, "exists", name); err != nil {
		return false, err
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.objects[name]
	return ok, nil
}

// Remove deletes name, or fails with ErrNotFound.
func (m *Memory) Remove(ctx context.Context, name string) error {
	if err := check(ctx, "remove", name); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.objects[name]; !ok {
		return newError("remove", name, ErrNotFound, nil)
	}
	delete(m.objects, name)
	return nil
}

var _ PruneStore = (*Memory)(nil)

// Replace overwrites or creates an object, bypassing put-once. It exists so
// tests can play an attacker who controls the bucket.
func (m *Memory) Replace(name string, data []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objects[name] = append([]byte{}, data...)
}

// Delete removes an object, for tests that play an attacker or a lost write.
func (m *Memory) Delete(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objects, name)
}

// Each calls fn with every object's name and bytes, in name order. fn must
// not modify data.
func (m *Memory) Each(fn func(name string, data []byte)) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	names := make([]string, 0, len(m.objects))
	for name := range m.objects {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		fn(name, m.objects[name])
	}
}

// check rejects an empty name or a done context.
func check(ctx context.Context, op, name string) error {
	if name == "" {
		return newError(op, name, ErrInvalidName, nil)
	}
	if err := ctx.Err(); err != nil {
		return newError(op, name, ErrBackend, err)
	}
	return nil
}
