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

//go:build azureblob

package azure

import (
	"context"
	"io"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// PutWithContext stores an object in the backend with context support.
func (a *Azure) PutWithContext(ctx context.Context, key string, data io.Reader) error {
	return a.PutWithMetadata(ctx, key, data, nil)
}

// PutWithMetadata stores an object with associated metadata.
// Note: Azure metadata support requires extending the ContainerAPI interface.
// For now, this delegates to the standard Put method.
func (a *Azure) PutWithMetadata(ctx context.Context, key string, data io.Reader, metadata *common.Metadata) error {
	blob := a.container.NewBlockBlob(key)
	return blob.UploadFromReader(ctx, data)
}

// GetWithContext retrieves an object from the backend with context support.
func (a *Azure) GetWithContext(ctx context.Context, key string) (io.ReadCloser, error) {
	blob := a.container.NewBlockBlob(key)
	return blob.NewReader(ctx)
}

// GetMetadata retrieves only the metadata for an object.
// Note: This is a stub implementation. Full implementation requires Azure SDK extensions.
func (a *Azure) GetMetadata(ctx context.Context, key string) (*common.Metadata, error) {
	// Stub: return nil to indicate metadata not implemented for Azure wrapper
	return nil, nil
}

// UpdateMetadata updates the metadata for an existing object.
// Note: This is a stub implementation. Full implementation requires Azure SDK extensions.
func (a *Azure) UpdateMetadata(ctx context.Context, key string, metadata *common.Metadata) error {
	// Stub: no-op for now
	return nil
}

// DeleteWithContext removes an object from the backend with context support.
func (a *Azure) DeleteWithContext(ctx context.Context, key string) error {
	blob := a.container.NewBlockBlob(key)
	return blob.Delete(ctx)
}

// Exists checks if an object exists in the backend.
func (a *Azure) Exists(ctx context.Context, key string) (bool, error) {
	blob := a.container.NewBlockBlob(key)
	err := blob.GetProperties(ctx)
	if err != nil {
		return false, nil
	}
	return true, nil
}

// ListWithContext returns a list of keys with context support.
func (a *Azure) ListWithContext(ctx context.Context, prefix string) ([]string, error) {
	return a.container.ListBlobsFlat(ctx, prefix)
}

// ListWithOptions returns a paginated list of objects with full metadata.
// Note: This is a simplified implementation that uses the existing List method.
func (a *Azure) ListWithOptions(ctx context.Context, opts *common.ListOptions) (*common.ListResult, error) {
	if opts == nil {
		opts = &common.ListOptions{}
	}

	// Use the existing list method
	keys, err := a.container.ListBlobsFlat(ctx, opts.Prefix)
	if err != nil {
		return nil, err
	}

	result := &common.ListResult{
		Objects:        make([]*common.ObjectInfo, 0, len(keys)),
		CommonPrefixes: []string{},
	}

	// Handle pagination manually
	startIdx := 0
	if opts.ContinueFrom != "" {
		for i, key := range keys {
			if key == opts.ContinueFrom {
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
	if endIdx > len(keys) {
		endIdx = len(keys)
	}

	selectedKeys := keys[startIdx:endIdx]

	// Convert to ObjectInfo
	for _, key := range selectedKeys {
		objInfo := &common.ObjectInfo{
			Key:      key,
			Metadata: nil, // Metadata retrieval not implemented in wrapper
		}
		result.Objects = append(result.Objects, objInfo)
	}

	// Set pagination info
	if endIdx < len(keys) {
		result.Truncated = true
		result.NextToken = keys[endIdx-1]
	}

	return result, nil
}
