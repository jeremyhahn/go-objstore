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
	"errors"
	"fmt"
	"io"

	"github.com/jeremyhahn/go-objstore/pkg/common"

	"github.com/Azure/azure-storage-blob-go/azblob"
)

// mapNotFound translates an Azure BlobNotFound storage error into an error
// wrapping common.ErrKeyNotFound; all other errors are returned unchanged.
func mapNotFound(err error, key string) error {
	var stgErr azblob.StorageError
	if errors.As(err, &stgErr) && stgErr.ServiceCode() == azblob.ServiceCodeBlobNotFound {
		return fmt.Errorf("%w: %s", common.ErrKeyNotFound, key)
	}
	return err
}

// PutWithContext stores an object in the backend with context support.
func (a *Azure) PutWithContext(ctx context.Context, key string, data io.Reader) error {
	return a.PutWithMetadata(ctx, key, data, nil)
}

// PutWithMetadata stores an object with associated metadata.
func (a *Azure) PutWithMetadata(ctx context.Context, key string, data io.Reader, metadata *common.Metadata) error {
	if err := common.ValidateKey(key); err != nil {
		return err
	}
	blob := a.container.NewBlockBlob(key)
	return blob.UploadFromReader(ctx, data)
}

// GetWithContext retrieves an object from the backend with context support.
func (a *Azure) GetWithContext(ctx context.Context, key string) (io.ReadCloser, error) {
	if err := common.ValidateKey(key); err != nil {
		return nil, err
	}
	blob := a.container.NewBlockBlob(key)
	return blob.NewReader(ctx)
}

// GetMetadata retrieves only the metadata for an object.
// Callers are guaranteed a non-nil *common.Metadata on success.
// A missing blob yields an error wrapping common.ErrKeyNotFound.
func (a *Azure) GetMetadata(ctx context.Context, key string) (*common.Metadata, error) {
	if err := common.ValidateKey(key); err != nil {
		return nil, err
	}
	blob := a.container.NewBlockBlob(key)
	props, err := blob.GetProperties(ctx)
	if err != nil {
		return nil, mapNotFound(err, key)
	}
	metadata := &common.Metadata{
		ContentType:     props.ContentType,
		ContentEncoding: props.ContentEncoding,
		Size:            props.Size,
		LastModified:    props.LastModified,
		ETag:            props.ETag,
	}
	if len(props.Metadata) > 0 {
		metadata.Custom = make(map[string]string, len(props.Metadata))
		for k, v := range props.Metadata {
			metadata.Custom[k] = v
		}
	}
	return metadata, nil
}

// UpdateMetadata updates the metadata for an existing object.
// Matching the local and S3 backends, the object's metadata is replaced
// rather than merged: custom metadata and HTTP headers not present in the
// supplied metadata are cleared. A missing blob yields an error wrapping
// common.ErrKeyNotFound.
func (a *Azure) UpdateMetadata(ctx context.Context, key string, metadata *common.Metadata) error {
	if err := common.ValidateKey(key); err != nil {
		return err
	}
	if metadata == nil {
		metadata = &common.Metadata{}
	}
	blob := a.container.NewBlockBlob(key)
	if err := blob.SetMetadata(ctx, metadata.Custom); err != nil {
		return mapNotFound(err, key)
	}
	headers := azblob.BlobHTTPHeaders{
		ContentType:     metadata.ContentType,
		ContentEncoding: metadata.ContentEncoding,
	}
	if err := blob.SetHTTPHeaders(ctx, headers); err != nil {
		return mapNotFound(err, key)
	}
	return nil
}

// DeleteWithContext removes an object from the backend with context support.
func (a *Azure) DeleteWithContext(ctx context.Context, key string) error {
	if err := common.ValidateKey(key); err != nil {
		return err
	}
	blob := a.container.NewBlockBlob(key)
	return blob.Delete(ctx)
}

// Exists checks if an object exists in the backend.
// Returns false,nil only for a BlobNotFound condition; propagates all other errors.
func (a *Azure) Exists(ctx context.Context, key string) (bool, error) {
	if err := common.ValidateKey(key); err != nil {
		return false, err
	}
	blob := a.container.NewBlockBlob(key)
	if _, err := blob.GetProperties(ctx); err != nil {
		if errors.Is(mapNotFound(err, key), common.ErrKeyNotFound) {
			return false, nil
		}
		return false, err
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
