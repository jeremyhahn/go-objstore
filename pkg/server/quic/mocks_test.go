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

package quic

import (
	"context"
	"errors"
	"io"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// mockErrorStorage is a storage implementation that returns specific errors
type mockErrorStorage struct {
	common.Storage

	// Error controls
	existsError         error
	getError            error
	putError            error
	deleteError         error
	listError           error
	getMetadataError    error
	updateMetadataError error
	archiveError        error
	getPoliciesError    error
	addPolicyError      error
	removePolicyError   error

	// Data
	objects  map[string][]byte
	metadata map[string]*common.Metadata
	policies []common.LifecyclePolicy
}

func newMockErrorStorage() *mockErrorStorage {
	return &mockErrorStorage{
		objects:  make(map[string][]byte),
		metadata: make(map[string]*common.Metadata),
		policies: make([]common.LifecyclePolicy, 0),
	}
}

func (m *mockErrorStorage) Configure(config map[string]string) error {
	return nil
}

func (m *mockErrorStorage) Put(key string, data io.Reader) error {
	if m.putError != nil {
		return m.putError
	}
	buf := make([]byte, 1024*1024)
	n, _ := data.Read(buf)
	m.objects[key] = buf[:n]
	m.metadata[key] = &common.Metadata{Size: int64(n)}
	return nil
}

func (m *mockErrorStorage) PutWithContext(ctx context.Context, key string, data io.Reader) error {
	return m.Put(key, data)
}

func (m *mockErrorStorage) PutWithMetadata(ctx context.Context, key string, data io.Reader, metadata *common.Metadata) error {
	if m.putError != nil {
		return m.putError
	}
	buf := make([]byte, 1024*1024)
	n, _ := data.Read(buf)
	m.objects[key] = buf[:n]
	m.metadata[key] = metadata
	return nil
}

func (m *mockErrorStorage) Get(key string) (io.ReadCloser, error) {
	if m.getError != nil {
		return nil, m.getError
	}
	data, exists := m.objects[key]
	if !exists {
		return nil, errors.New("object not found")
	}
	return io.NopCloser(io.Reader(newBytesReader(data))), nil
}

func (m *mockErrorStorage) GetWithContext(ctx context.Context, key string) (io.ReadCloser, error) {
	return m.Get(key)
}

func (m *mockErrorStorage) Exists(ctx context.Context, key string) (bool, error) {
	if m.existsError != nil {
		return false, m.existsError
	}
	_, exists := m.objects[key]
	return exists, nil
}

func (m *mockErrorStorage) Delete(key string) error {
	if m.deleteError != nil {
		return m.deleteError
	}
	delete(m.objects, key)
	delete(m.metadata, key)
	return nil
}

func (m *mockErrorStorage) DeleteWithContext(ctx context.Context, key string) error {
	return m.Delete(key)
}

func (m *mockErrorStorage) List(prefix string) ([]string, error) {
	if m.listError != nil {
		return nil, m.listError
	}

	result := make([]string, 0)
	for key := range m.metadata {
		if prefix == "" || len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			result = append(result, key)
		}
	}

	return result, nil
}

func (m *mockErrorStorage) ListWithContext(ctx context.Context, prefix string) ([]string, error) {
	return m.List(prefix)
}

func (m *mockErrorStorage) ListWithOptions(ctx context.Context, opts *common.ListOptions) (*common.ListResult, error) {
	if m.listError != nil {
		return nil, m.listError
	}

	result := &common.ListResult{
		Objects: make([]*common.ObjectInfo, 0),
	}

	prefix := ""
	if opts != nil && opts.Prefix != "" {
		prefix = opts.Prefix
	}

	// Collect matching keys
	for key, meta := range m.metadata {
		if prefix == "" || len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			result.Objects = append(result.Objects, &common.ObjectInfo{
				Key:      key,
				Metadata: meta,
			})
		}
	}

	// Simulate pagination if there are many results
	if opts != nil && opts.MaxResults > 0 && len(result.Objects) > opts.MaxResults {
		result.Objects = result.Objects[:opts.MaxResults]
		result.Truncated = true
		result.NextToken = "mock-next-token"
	}

	// Simulate delimiter-based prefix grouping
	if opts != nil && opts.Delimiter != "" {
		result.CommonPrefixes = []string{"common/prefix/"}
	}

	return result, nil
}

func (m *mockErrorStorage) GetMetadata(ctx context.Context, key string) (*common.Metadata, error) {
	if m.getMetadataError != nil {
		return nil, m.getMetadataError
	}
	meta, exists := m.metadata[key]
	if !exists {
		return nil, errors.New("object not found")
	}
	return meta, nil
}

func (m *mockErrorStorage) UpdateMetadata(ctx context.Context, key string, metadata *common.Metadata) error {
	if m.updateMetadataError != nil {
		return m.updateMetadataError
	}
	_, exists := m.metadata[key]
	if !exists {
		return errors.New("object not found")
	}
	m.metadata[key] = metadata
	return nil
}

func (m *mockErrorStorage) Archive(key string, archiver common.Archiver) error {
	if m.archiveError != nil {
		return m.archiveError
	}
	return nil
}

func (m *mockErrorStorage) GetPolicies() ([]common.LifecyclePolicy, error) {
	if m.getPoliciesError != nil {
		return nil, m.getPoliciesError
	}
	return m.policies, nil
}

func (m *mockErrorStorage) AddPolicy(policy common.LifecyclePolicy) error {
	if m.addPolicyError != nil {
		return m.addPolicyError
	}

	// Check for duplicate
	for _, p := range m.policies {
		if p.ID == policy.ID {
			return errors.New("policy already exists")
		}
	}

	m.policies = append(m.policies, policy)
	return nil
}

func (m *mockErrorStorage) RemovePolicy(id string) error {
	if m.removePolicyError != nil {
		return m.removePolicyError
	}

	for i, p := range m.policies {
		if p.ID == id {
			m.policies = append(m.policies[:i], m.policies[i+1:]...)
			return nil
		}
	}

	return common.ErrPolicyNotFound
}

func (m *mockErrorStorage) Close() error {
	return nil
}

// Helper to create a bytes reader
type bytesReader struct {
	data []byte
	pos  int
}

func newBytesReader(data []byte) *bytesReader {
	return &bytesReader{data: data}
}

func (r *bytesReader) Read(p []byte) (n int, err error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n = copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}
