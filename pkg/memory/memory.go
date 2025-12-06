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

// Package memory provides an in-memory implementation of the storage interface.
// This is useful for testing, development, and scenarios where persistence is not required.
package memory

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// object represents a stored object with its data and metadata.
type object struct {
	data     []byte
	metadata *common.Metadata
}

// Memory is a storage backend that stores objects in memory.
type Memory struct {
	mu               sync.RWMutex
	objects          map[string]*object
	lifecycleManager common.LifecycleManager
}

// New creates a new Memory storage backend.
func New() common.Storage {
	return &Memory{
		objects:          make(map[string]*object),
		lifecycleManager: NewLifecycleManager(),
	}
}

// Configure sets up the backend with the necessary settings.
// The memory backend has no required settings.
func (m *Memory) Configure(settings map[string]string) error {
	return nil
}

// Put stores an object in the backend.
func (m *Memory) Put(key string, data io.Reader) error {
	return m.PutWithContext(context.Background(), key, data)
}

// PutWithContext stores an object in the backend with context support.
func (m *Memory) PutWithContext(ctx context.Context, key string, data io.Reader) error {
	return m.PutWithMetadata(ctx, key, data, nil)
}

// validateKey checks if a key is safe to use.
func (m *Memory) validateKey(key string) error {
	return common.ValidateKey(key)
}

// PutWithMetadata stores an object with associated metadata.
func (m *Memory) PutWithMetadata(ctx context.Context, key string, data io.Reader, metadata *common.Metadata) error {
	if err := m.validateKey(key); err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Read all data from the reader
	dataBytes, err := io.ReadAll(data)
	if err != nil {
		return err
	}

	// Create or update metadata
	if metadata == nil {
		metadata = &common.Metadata{}
	}
	metadata.Size = int64(len(dataBytes))
	metadata.LastModified = time.Now()
	metadata.ETag = fmt.Sprintf("%d-%d", metadata.LastModified.Unix(), metadata.Size)

	m.mu.Lock()
	m.objects[key] = &object{
		data:     dataBytes,
		metadata: metadata,
	}
	m.mu.Unlock()

	return nil
}

// Get retrieves an object from the backend.
func (m *Memory) Get(key string) (io.ReadCloser, error) {
	return m.GetWithContext(context.Background(), key)
}

// GetWithContext retrieves an object from the backend with context support.
func (m *Memory) GetWithContext(ctx context.Context, key string) (io.ReadCloser, error) {
	if err := m.validateKey(key); err != nil {
		return nil, err
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	m.mu.RLock()
	obj, exists := m.objects[key]
	m.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("%w: %s", common.ErrKeyNotFound, key)
	}

	// Return a copy of the data to prevent mutation
	dataCopy := make([]byte, len(obj.data))
	copy(dataCopy, obj.data)

	return io.NopCloser(bytes.NewReader(dataCopy)), nil
}

// GetMetadata retrieves only the metadata for an object.
func (m *Memory) GetMetadata(ctx context.Context, key string) (*common.Metadata, error) {
	if err := m.validateKey(key); err != nil {
		return nil, err
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	m.mu.RLock()
	obj, exists := m.objects[key]
	m.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("%w: %s", common.ErrMetadataNotFound, key)
	}

	// Return a copy of the metadata
	metadataCopy := *obj.metadata
	if obj.metadata.Custom != nil {
		metadataCopy.Custom = make(map[string]string)
		for k, v := range obj.metadata.Custom {
			metadataCopy.Custom[k] = v
		}
	}

	return &metadataCopy, nil
}

// UpdateMetadata updates the metadata for an existing object.
func (m *Memory) UpdateMetadata(ctx context.Context, key string, metadata *common.Metadata) error {
	if err := m.validateKey(key); err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	obj, exists := m.objects[key]
	if !exists {
		return fmt.Errorf("%w: %s", common.ErrKeyNotFound, key)
	}

	// Update metadata while preserving size
	if metadata == nil {
		metadata = &common.Metadata{}
	}
	metadata.Size = int64(len(obj.data))
	metadata.LastModified = time.Now()
	metadata.ETag = fmt.Sprintf("%d-%d", metadata.LastModified.Unix(), metadata.Size)

	obj.metadata = metadata
	return nil
}

// Delete removes an object from the backend.
func (m *Memory) Delete(key string) error {
	return m.DeleteWithContext(context.Background(), key)
}

// DeleteWithContext removes an object from the backend with context support.
func (m *Memory) DeleteWithContext(ctx context.Context, key string) error {
	if err := m.validateKey(key); err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.objects[key]; !exists {
		return fmt.Errorf("%w: %s", common.ErrKeyNotFound, key)
	}

	delete(m.objects, key)
	return nil
}

// Exists checks if an object exists in the backend.
func (m *Memory) Exists(ctx context.Context, key string) (bool, error) {
	if err := m.validateKey(key); err != nil {
		return false, err
	}

	select {
	case <-ctx.Done():
		return false, ctx.Err()
	default:
	}

	m.mu.RLock()
	_, exists := m.objects[key]
	m.mu.RUnlock()

	return exists, nil
}

// List returns a list of keys that start with the given prefix.
func (m *Memory) List(prefix string) ([]string, error) {
	return m.ListWithContext(context.Background(), prefix)
}

// ListWithContext returns a list of keys with context support.
func (m *Memory) ListWithContext(ctx context.Context, prefix string) ([]string, error) {
	// Validate prefix if not empty
	if prefix != "" {
		if err := m.validateKey(prefix); err != nil {
			return nil, err
		}
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	var keys []string
	for key := range m.objects {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}

	// Sort for consistent ordering
	sort.Strings(keys)
	return keys, nil
}

// ListWithOptions returns a paginated list of objects with full metadata.
func (m *Memory) ListWithOptions(ctx context.Context, opts *common.ListOptions) (*common.ListResult, error) {
	if opts == nil {
		opts = &common.ListOptions{}
	}

	// Validate prefix if not empty
	if opts.Prefix != "" {
		if err := m.validateKey(opts.Prefix); err != nil {
			return nil, err
		}
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	result := &common.ListResult{
		Objects:        []*common.ObjectInfo{},
		CommonPrefixes: []string{},
	}

	prefixMap := make(map[string]bool)
	var allObjects []*common.ObjectInfo

	// Get all matching keys and sort them
	var matchingKeys []string
	for key := range m.objects {
		if strings.HasPrefix(key, opts.Prefix) {
			matchingKeys = append(matchingKeys, key)
		}
	}
	sort.Strings(matchingKeys)

	for _, key := range matchingKeys {
		obj := m.objects[key]

		// Handle delimiter
		if opts.Delimiter != "" {
			remainder := strings.TrimPrefix(key, opts.Prefix)
			if idx := strings.Index(remainder, opts.Delimiter); idx >= 0 {
				commonPrefix := opts.Prefix + remainder[:idx+len(opts.Delimiter)]
				if !prefixMap[commonPrefix] {
					prefixMap[commonPrefix] = true
					result.CommonPrefixes = append(result.CommonPrefixes, commonPrefix)
				}
				continue
			}
		}

		// Copy metadata
		metadataCopy := *obj.metadata
		if obj.metadata.Custom != nil {
			metadataCopy.Custom = make(map[string]string)
			for k, v := range obj.metadata.Custom {
				metadataCopy.Custom[k] = v
			}
		}

		allObjects = append(allObjects, &common.ObjectInfo{
			Key:      key,
			Metadata: &metadataCopy,
		})
	}

	// Handle pagination
	startIdx := 0
	if opts.ContinueFrom != "" {
		for i, obj := range allObjects {
			if obj.Key == opts.ContinueFrom {
				startIdx = i + 1
				break
			}
		}
	}

	maxResults := opts.MaxResults
	if maxResults <= 0 {
		maxResults = 1000
	}

	endIdx := startIdx + maxResults
	if endIdx > len(allObjects) {
		endIdx = len(allObjects)
	}

	result.Objects = allObjects[startIdx:endIdx]

	if endIdx < len(allObjects) {
		result.Truncated = true
		result.NextToken = allObjects[endIdx-1].Key
	}

	return result, nil
}

// Archive copies an object to another backend for archival.
func (m *Memory) Archive(key string, destination common.Archiver) error {
	if err := m.validateKey(key); err != nil {
		return err
	}
	if destination == nil {
		return common.ErrArchiveDestinationNil
	}

	reader, err := m.Get(key)
	if err != nil {
		return err
	}
	defer func() { _ = reader.Close() }()

	return destination.Put(key, reader)
}

// AddPolicy adds a new lifecycle policy.
func (m *Memory) AddPolicy(policy common.LifecyclePolicy) error {
	return m.lifecycleManager.AddPolicy(policy)
}

// RemovePolicy removes a lifecycle policy.
func (m *Memory) RemovePolicy(id string) error {
	return m.lifecycleManager.RemovePolicy(id)
}

// GetPolicies returns all the lifecycle policies.
func (m *Memory) GetPolicies() ([]common.LifecyclePolicy, error) {
	return m.lifecycleManager.GetPolicies()
}

// Clear removes all objects from the storage. This is useful for testing.
func (m *Memory) Clear() {
	m.mu.Lock()
	m.objects = make(map[string]*object)
	m.mu.Unlock()
}

// Count returns the number of objects in storage. This is useful for testing.
func (m *Memory) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.objects)
}
