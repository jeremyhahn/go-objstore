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

package storagefs

import (
	"bytes"
	"context"
	"io"
	"io/fs"
	"strings"
	"sync"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// mockStorage is an in-memory storage implementation for testing
type mockStorage struct {
	mu          sync.RWMutex
	data        map[string][]byte
	putError    error
	getError    error
	deleteError error
}

// newMockStorage creates a new mock storage backend
func newMockStorage() *mockStorage {
	return &mockStorage{
		data: make(map[string][]byte),
	}
}

// Configure is a no-op for mock storage
func (m *mockStorage) Configure(settings map[string]string) error {
	return nil
}

// Put stores data in memory
func (m *mockStorage) Put(key string, data io.Reader) error {
	if m.putError != nil {
		return m.putError
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	content, err := io.ReadAll(data)
	if err != nil {
		return err
	}

	m.data[key] = content
	return nil
}

// PutWithContext stores data in memory with context support
func (m *mockStorage) PutWithContext(ctx context.Context, key string, data io.Reader) error {
	return m.Put(key, data)
}

// PutWithMetadata stores data with metadata (metadata ignored in mock)
func (m *mockStorage) PutWithMetadata(ctx context.Context, key string, data io.Reader, metadata *common.Metadata) error {
	return m.Put(key, data)
}

// Get retrieves data from memory
func (m *mockStorage) Get(key string) (io.ReadCloser, error) {
	if m.getError != nil {
		return nil, m.getError
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	content, exists := m.data[key]
	if !exists {
		return nil, fs.ErrNotExist
	}

	return io.NopCloser(bytes.NewReader(content)), nil
}

// GetWithContext retrieves data with context support
func (m *mockStorage) GetWithContext(ctx context.Context, key string) (io.ReadCloser, error) {
	return m.Get(key)
}

// GetMetadata retrieves metadata (returns nil for mock)
func (m *mockStorage) GetMetadata(ctx context.Context, key string) (*common.Metadata, error) {
	return nil, nil
}

// UpdateMetadata updates metadata (no-op for mock)
func (m *mockStorage) UpdateMetadata(ctx context.Context, key string, metadata *common.Metadata) error {
	return nil
}

// Delete removes data from memory
func (m *mockStorage) Delete(key string) error {
	if m.deleteError != nil {
		return m.deleteError
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.data[key]; !exists {
		return fs.ErrNotExist
	}

	delete(m.data, key)
	return nil
}

// DeleteWithContext removes data with context support
func (m *mockStorage) DeleteWithContext(ctx context.Context, key string) error {
	return m.Delete(key)
}

// Exists checks if a key exists in storage
func (m *mockStorage) Exists(ctx context.Context, key string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	_, exists := m.data[key]
	return exists, nil
}

// List returns all keys with a given prefix
func (m *mockStorage) List(prefix string) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	keys := m.listKeys(prefix)
	return keys, nil
}

// ListWithContext returns all keys with context support
func (m *mockStorage) ListWithContext(ctx context.Context, prefix string) ([]string, error) {
	return m.List(prefix)
}

// ListWithOptions returns paginated list results with delimiter support
func (m *mockStorage) ListWithOptions(ctx context.Context, opts *common.ListOptions) (*common.ListResult, error) {
	if opts == nil {
		opts = &common.ListOptions{}
	}

	keys, err := m.List(opts.Prefix)
	if err != nil {
		return nil, err
	}

	result := &common.ListResult{
		Objects:        make([]*common.ObjectInfo, 0),
		CommonPrefixes: make([]string, 0),
	}

	// If no delimiter, return all keys as objects
	if opts.Delimiter == "" {
		for _, key := range keys {
			// Get metadata for the object
			m.mu.RLock()
			data, exists := m.data[key]
			m.mu.RUnlock()

			metadata := &common.Metadata{
				Size: int64(len(data)),
			}

			if exists {
				result.Objects = append(result.Objects, &common.ObjectInfo{
					Key:      key,
					Metadata: metadata,
				})
			}
		}
		return result, nil
	}

	// With delimiter, we need to separate directories (CommonPrefixes) from files
	prefixMap := make(map[string]bool)
	for _, key := range keys {
		// Remove the prefix to get the relative path
		relPath := key
		if opts.Prefix != "" {
			relPath = strings.TrimPrefix(key, opts.Prefix)
		}

		// Check if this key contains the delimiter (indicating a subdirectory)
		delimIndex := strings.Index(relPath, opts.Delimiter)
		if delimIndex >= 0 {
			// This is a subdirectory - extract the prefix up to and including delimiter
			commonPrefix := opts.Prefix + relPath[:delimIndex+len(opts.Delimiter)]
			prefixMap[commonPrefix] = true
		} else if relPath != "" {
			// This is a direct file (no delimiter in relative path)
			m.mu.RLock()
			data, exists := m.data[key]
			m.mu.RUnlock()

			metadata := &common.Metadata{
				Size: int64(len(data)),
			}

			if exists {
				result.Objects = append(result.Objects, &common.ObjectInfo{
					Key:      key,
					Metadata: metadata,
				})
			}
		}
	}

	// Convert prefix map to sorted slice
	for prefix := range prefixMap {
		result.CommonPrefixes = append(result.CommonPrefixes, prefix)
	}

	return result, nil
}

// Archive copies data to another storage backend
func (m *mockStorage) Archive(key string, archiver common.Archiver) error {
	data, err := m.Get(key)
	if err != nil {
		return err
	}
	defer func() { _ = data.Close() }()

	return archiver.Put(key, data)
}

// AddPolicy adds a lifecycle policy (no-op for mock)
func (m *mockStorage) AddPolicy(policy common.LifecyclePolicy) error {
	return nil
}

// RemovePolicy removes a lifecycle policy (no-op for mock)
func (m *mockStorage) RemovePolicy(id string) error {
	return nil
}

// GetPolicies returns all lifecycle policies (empty for mock)
func (m *mockStorage) GetPolicies() ([]common.LifecyclePolicy, error) {
	return []common.LifecyclePolicy{}, nil
}

// RunPolicies executes lifecycle policies (no-op for mock)
func (m *mockStorage) RunPolicies(ctx context.Context) error {
	return nil
}

// Helper methods
func (m *mockStorage) exists(key string) bool {
	_, ok := m.data[key]
	return ok
}

func (m *mockStorage) listKeys(prefix string) []string {
	var keys []string
	for key := range m.data {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	return keys
}

// clear removes all data
func (m *mockStorage) clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data = make(map[string][]byte)
}
