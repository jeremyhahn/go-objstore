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

//go:build gcpstorage

package gcs

import (
	"context"
	"io"

	"github.com/jeremyhahn/go-objstore/pkg/common"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

// PutWithContext stores an object in the backend with context support.
func (g *GCS) PutWithContext(ctx context.Context, key string, data io.Reader) error {
	return g.PutWithMetadata(ctx, key, data, nil)
}

// PutWithMetadata stores an object with associated metadata.
// Note: GCS metadata support requires accessing the underlying storage.Writer.
// For now, this delegates to the standard Put method.
func (g *GCS) PutWithMetadata(ctx context.Context, key string, data io.Reader, metadata *common.Metadata) error {
	w := g.client.Bucket(g.bucket).Object(key).NewWriter(ctx)
	defer w.Close()
	_, err := io.Copy(w, data)
	return err
}

// GetWithContext retrieves an object from the backend with context support.
func (g *GCS) GetWithContext(ctx context.Context, key string) (io.ReadCloser, error) {
	obj := g.client.Bucket(g.bucket).Object(key)
	return obj.NewReader(ctx)
}

// GetMetadata retrieves only the metadata for an object.
// Note: This is a stub implementation. Full implementation requires GCS SDK extensions.
func (g *GCS) GetMetadata(ctx context.Context, key string) (*common.Metadata, error) {
	// Stub: return nil to indicate metadata not implemented for GCS wrapper
	return nil, nil
}

// UpdateMetadata updates the metadata for an existing object.
// Note: This is a stub implementation. Full implementation requires GCS SDK extensions.
func (g *GCS) UpdateMetadata(ctx context.Context, key string, metadata *common.Metadata) error {
	// Stub: no-op for now
	return nil
}

// DeleteWithContext removes an object from the backend with context support.
func (g *GCS) DeleteWithContext(ctx context.Context, key string) error {
	obj := g.client.Bucket(g.bucket).Object(key)
	return obj.Delete(ctx)
}

// Exists checks if an object exists in the backend.
func (g *GCS) Exists(ctx context.Context, key string) (bool, error) {
	obj := g.client.Bucket(g.bucket).Object(key)
	_, err := obj.Attrs(ctx)
	if err != nil {
		return false, nil
	}
	return true, nil
}

// ListWithContext returns a list of keys with context support.
func (g *GCS) ListWithContext(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	query := &storage.Query{Prefix: prefix}
	it := g.client.Bucket(g.bucket).Objects(ctx, query)

	for {
		attrs, err := it.Next()
		if err == iterator.Done { //nolint:err113 // iterator.Done is the standard sentinel error for GCS iterators
			break
		}
		if err != nil {
			return nil, err
		}
		keys = append(keys, attrs.Name)
	}

	return keys, nil
}

// ListWithOptions returns a paginated list of objects with full metadata.
// Note: This is a simplified implementation using the existing List method.
func (g *GCS) ListWithOptions(ctx context.Context, opts *common.ListOptions) (*common.ListResult, error) {
	if opts == nil {
		opts = &common.ListOptions{}
	}

	query := &storage.Query{
		Prefix: opts.Prefix,
	}

	if opts.Delimiter != "" {
		query.Delimiter = opts.Delimiter
	}

	it := g.client.Bucket(g.bucket).Objects(ctx, query)

	result := &common.ListResult{
		Objects:        []*common.ObjectInfo{},
		CommonPrefixes: []string{},
	}

	count := 0
	maxResults := opts.MaxResults
	if maxResults <= 0 {
		maxResults = 1000
	}

	// Skip to continuation point if provided
	if opts.ContinueFrom != "" {
		for {
			attrs, err := it.Next()
			if err == iterator.Done { //nolint:err113 // iterator.Done is the standard sentinel error for GCS iterators
				break
			}
			if err != nil {
				return nil, err
			}
			if attrs.Name == opts.ContinueFrom {
				break
			}
		}
	}

	for {
		attrs, err := it.Next()
		if err == iterator.Done { //nolint:err113 // iterator.Done is the standard sentinel error for GCS iterators
			break
		}
		if err != nil {
			return nil, err
		}

		// Check if this is a common prefix
		if attrs.Prefix != "" {
			if !contains(result.CommonPrefixes, attrs.Prefix) {
				result.CommonPrefixes = append(result.CommonPrefixes, attrs.Prefix)
			}
			continue
		}

		objInfo := &common.ObjectInfo{
			Key:      attrs.Name,
			Metadata: nil, // Metadata retrieval not fully implemented
		}
		result.Objects = append(result.Objects, objInfo)

		count++
		if count >= maxResults {
			result.Truncated = true
			result.NextToken = attrs.Name
			break
		}
	}

	return result, nil
}

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}
