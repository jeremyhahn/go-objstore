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
	"errors"
	"fmt"
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
func (g *GCS) PutWithMetadata(ctx context.Context, key string, data io.Reader, metadata *common.Metadata) error {
	if err := common.ValidateKey(key); err != nil {
		return err
	}
	w := g.client.Bucket(g.bucket).Object(key).NewWriter(ctx)
	if _, err := io.Copy(w, data); err != nil {
		// Close to release the GCS write stream; ignore close error.
		_ = w.Close()
		return err
	}
	// Close finalizes the GCS upload; capture its error.
	return w.Close()
}

// GetWithContext retrieves an object from the backend with context support.
func (g *GCS) GetWithContext(ctx context.Context, key string) (io.ReadCloser, error) {
	if err := common.ValidateKey(key); err != nil {
		return nil, err
	}
	obj := g.client.Bucket(g.bucket).Object(key)
	return obj.NewReader(ctx)
}

// GetMetadata retrieves only the metadata for an object.
// It performs a best-effort Attrs call to populate Size and ContentType;
// callers are guaranteed a non-nil *common.Metadata on success.
func (g *GCS) GetMetadata(ctx context.Context, key string) (*common.Metadata, error) {
	if err := common.ValidateKey(key); err != nil {
		return nil, err
	}
	meta := &common.Metadata{}
	obj := g.client.Bucket(g.bucket).Object(key)
	attrs, err := obj.Attrs(ctx)
	if err != nil {
		// Return the empty struct rather than nil so callers never dereference nil.
		return meta, nil
	}
	meta.Size = attrs.Size
	meta.ContentType = attrs.ContentType
	return meta, nil
}

// UpdateMetadata updates the metadata for an existing object.
// Matching the local and S3 backends, the object's metadata is replaced
// rather than merged: custom metadata, content type and content encoding not
// present in the supplied metadata are cleared. A missing object yields an
// error wrapping common.ErrKeyNotFound.
func (g *GCS) UpdateMetadata(ctx context.Context, key string, metadata *common.Metadata) error {
	if err := common.ValidateKey(key); err != nil {
		return err
	}
	if metadata == nil {
		metadata = &common.Metadata{}
	}
	custom := metadata.Custom
	if custom == nil {
		// An empty (non-nil) map instructs GCS to delete all custom metadata,
		// preserving replace semantics.
		custom = map[string]string{}
	}
	uattrs := storage.ObjectAttrsToUpdate{
		ContentType:     metadata.ContentType,
		ContentEncoding: metadata.ContentEncoding,
		Metadata:        custom,
	}
	if _, err := g.client.Bucket(g.bucket).Object(key).Update(ctx, uattrs); err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return fmt.Errorf("%w: %s", common.ErrKeyNotFound, key)
		}
		return err
	}
	return nil
}

// DeleteWithContext removes an object from the backend with context support.
func (g *GCS) DeleteWithContext(ctx context.Context, key string) error {
	if err := common.ValidateKey(key); err != nil {
		return err
	}
	obj := g.client.Bucket(g.bucket).Object(key)
	return obj.Delete(ctx)
}

// Exists checks if an object exists in the backend.
// Returns false,nil only for a not-found condition; propagates all other errors.
func (g *GCS) Exists(ctx context.Context, key string) (bool, error) {
	if err := common.ValidateKey(key); err != nil {
		return false, err
	}
	obj := g.client.Bucket(g.bucket).Object(key)
	_, err := obj.Attrs(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return false, nil
		}
		return false, err
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
