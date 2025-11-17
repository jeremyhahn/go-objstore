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

package replication

import (
	"bytes"
	"context"
	"io"
	"sort"
	"sync"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/audit"
	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// mockLogger implements adapters.Logger for testing
type mockLogger struct{}

func (m *mockLogger) Debug(ctx context.Context, msg string, fields ...adapters.Field) {}
func (m *mockLogger) Info(ctx context.Context, msg string, fields ...adapters.Field)  {}
func (m *mockLogger) Warn(ctx context.Context, msg string, fields ...adapters.Field)  {}
func (m *mockLogger) Error(ctx context.Context, msg string, fields ...adapters.Field) {}
func (m *mockLogger) WithFields(fields ...adapters.Field) adapters.Logger              { return m }
func (m *mockLogger) WithContext(ctx context.Context) adapters.Logger                  { return m }
func (m *mockLogger) SetLevel(level adapters.LogLevel)                                 {}
func (m *mockLogger) GetLevel() adapters.LogLevel                                      { return adapters.InfoLevel }

// mockAuditLogger implements audit.AuditLogger for testing
type mockAuditLogger struct{}

func (m *mockAuditLogger) LogEvent(ctx context.Context, event *audit.AuditEvent) error {
	return nil
}

func (m *mockAuditLogger) LogAuthFailure(ctx context.Context, userID, principal, ipAddress, requestID, reason string) error {
	return nil
}

func (m *mockAuditLogger) LogAuthSuccess(ctx context.Context, userID, principal, ipAddress, requestID string) error {
	return nil
}

func (m *mockAuditLogger) LogObjectAccess(ctx context.Context, userID, principal, bucket, key, ipAddress, requestID string, result audit.Result, err error) error {
	return nil
}

func (m *mockAuditLogger) LogObjectMutation(ctx context.Context, eventType audit.EventType, userID, principal, bucket, key, ipAddress, requestID string, bytesTransferred int64, result audit.Result, err error) error {
	return nil
}

func (m *mockAuditLogger) LogPolicyChange(ctx context.Context, userID, principal, bucket, policyID, ipAddress, requestID string, result audit.Result, err error) error {
	return nil
}

func (m *mockAuditLogger) SetLevel(level adapters.LogLevel) {}

func (m *mockAuditLogger) GetLevel() adapters.LogLevel { return adapters.InfoLevel }

// mockStorage is a mock implementation of common.Storage for testing
type mockStorage struct {
	mu             sync.RWMutex
	objects        map[string]*common.Metadata
	listError      error
	getMetaError   error
	listCallCount  int
	maxResults     int
	shouldTruncate bool
}

func newMockStorage() *mockStorage {
	return &mockStorage{
		objects:    make(map[string]*common.Metadata),
		maxResults: 1000,
	}
}

func (m *mockStorage) Configure(settings map[string]string) error { return nil }
func (m *mockStorage) Put(key string, data io.Reader) error       { return nil }
func (m *mockStorage) PutWithContext(ctx context.Context, key string, data io.Reader) error {
	return nil
}
func (m *mockStorage) PutWithMetadata(ctx context.Context, key string, data io.Reader, metadata *common.Metadata) error {
	return nil
}
func (m *mockStorage) Get(key string) (io.ReadCloser, error) { return nil, nil }
func (m *mockStorage) GetWithContext(ctx context.Context, key string) (io.ReadCloser, error) {
	return nil, nil
}
func (m *mockStorage) GetMetadata(ctx context.Context, key string) (*common.Metadata, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.getMetaError != nil {
		return nil, m.getMetaError
	}
	meta, exists := m.objects[key]
	if !exists {
		return nil, common.ErrKeyNotFound
	}
	return meta, nil
}
func (m *mockStorage) UpdateMetadata(ctx context.Context, key string, metadata *common.Metadata) error {
	return nil
}
func (m *mockStorage) Delete(key string) error                                { return nil }
func (m *mockStorage) DeleteWithContext(ctx context.Context, key string) error { return nil }
func (m *mockStorage) Exists(ctx context.Context, key string) (bool, error)    { return false, nil }
func (m *mockStorage) List(prefix string) ([]string, error)                    { return nil, nil }
func (m *mockStorage) ListWithContext(ctx context.Context, prefix string) ([]string, error) {
	return nil, nil
}
func (m *mockStorage) ListWithOptions(ctx context.Context, opts *common.ListOptions) (*common.ListResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.listCallCount++

	if m.listError != nil {
		return nil, m.listError
	}

	// Collect and sort all matching keys for consistent pagination
	var allKeys []string
	for key := range m.objects {
		// Filter by prefix
		if opts.Prefix == "" || len(key) >= len(opts.Prefix) && key[:len(opts.Prefix)] == opts.Prefix {
			allKeys = append(allKeys, key)
		}
	}
	sort.Strings(allKeys)

	// Filter based on continuation token
	var matchedObjects []*common.ObjectInfo
	for _, key := range allKeys {
		// Skip objects up to and including the continuation token
		if opts.ContinueFrom != "" && key <= opts.ContinueFrom {
			continue
		}
		meta := m.objects[key]
		matchedObjects = append(matchedObjects, &common.ObjectInfo{
			Key:      key,
			Metadata: meta,
		})
	}

	result := &common.ListResult{
		Objects: matchedObjects,
	}

	// Simulate pagination - return only first object when shouldTruncate is true
	if m.shouldTruncate && len(matchedObjects) > 1 {
		result.Objects = matchedObjects[:1]
		result.Truncated = true
		// Use the first object's key as the next token
		// Next call will skip this key and continue with the rest
		result.NextToken = matchedObjects[0].Key
	} else if m.shouldTruncate && len(matchedObjects) == 1 {
		// Last page with one item
		result.Objects = matchedObjects
		result.Truncated = false
	}

	return result, nil
}
func (m *mockStorage) Archive(key string, destination common.Archiver) error { return nil }
func (m *mockStorage) AddPolicy(policy common.LifecyclePolicy) error         { return nil }
func (m *mockStorage) RemovePolicy(id string) error                          { return nil }
func (m *mockStorage) GetPolicies() ([]common.LifecyclePolicy, error)        { return nil, nil }


// extendedMockStorage extends mockStorage with Put/Get capabilities for syncing
type extendedMockStorage struct {
	*mockStorage
	data           map[string][]byte
	putError       error
	getError       error
	putCalled      bool
	putWithMetaFn  func(ctx context.Context, key string, data io.Reader, metadata *common.Metadata) error
}

func newExtendedMockStorage() *extendedMockStorage {
	ems := &extendedMockStorage{
		mockStorage: newMockStorage(),
		data:        make(map[string][]byte),
	}
	// Set default implementation
	ems.putWithMetaFn = ems.defaultPutWithMetadata
	return ems
}

func (e *extendedMockStorage) GetWithContext(ctx context.Context, key string) (io.ReadCloser, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.getError != nil {
		return nil, e.getError
	}
	data, exists := e.data[key]
	if !exists {
		return nil, common.ErrKeyNotFound
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (e *extendedMockStorage) defaultPutWithMetadata(ctx context.Context, key string, data io.Reader, metadata *common.Metadata) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.putError != nil {
		return e.putError
	}

	bytes, err := io.ReadAll(data)
	if err != nil {
		return err
	}

	e.data[key] = bytes
	e.mockStorage.objects[key] = metadata
	e.putCalled = true
	return nil
}

func (e *extendedMockStorage) PutWithMetadata(ctx context.Context, key string, data io.Reader, metadata *common.Metadata) error {
	return e.putWithMetaFn(ctx, key, data, metadata)
}

func (e *extendedMockStorage) DeleteWithContext(ctx context.Context, key string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	delete(e.data, key)
	delete(e.mockStorage.objects, key)
	return nil
}
