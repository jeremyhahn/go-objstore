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

//go:build minio

package minio

import (
	"context"
	"io"
	"strings"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"

	"github.com/aws/aws-sdk-go/aws"        //nolint:staticcheck // Using v1 SDK, migration to v2 planned
	"github.com/aws/aws-sdk-go/service/s3" //nolint:staticcheck // Using v1 SDK, migration to v2 planned
)

// PutWithContext stores an object in the backend with context support.
func (m *MinIO) PutWithContext(ctx context.Context, key string, data io.Reader) error {
	return m.PutWithMetadata(ctx, key, data, nil)
}

// PutWithMetadata stores an object with associated metadata.
func (m *MinIO) PutWithMetadata(ctx context.Context, key string, data io.Reader, metadata *common.Metadata) error {
	input := &s3.PutObjectInput{
		Bucket: aws.String(m.bucket),
		Key:    aws.String(key),
		Body:   aws.ReadSeekCloser(data),
	}

	// Add metadata if provided
	if metadata != nil {
		if metadata.ContentType != "" {
			input.ContentType = aws.String(metadata.ContentType)
		}
		if metadata.ContentEncoding != "" {
			input.ContentEncoding = aws.String(metadata.ContentEncoding)
		}
		if len(metadata.Custom) > 0 {
			input.Metadata = make(map[string]*string)
			for k, v := range metadata.Custom {
				input.Metadata[k] = aws.String(v)
			}
		}
	}

	_, err := m.svc.PutObjectWithContext(ctx, input)
	return err
}

// GetWithContext retrieves an object from the backend with context support.
func (m *MinIO) GetWithContext(ctx context.Context, key string) (io.ReadCloser, error) {
	result, err := m.svc.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: aws.String(m.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	return result.Body, nil
}

// GetMetadata retrieves only the metadata for an object.
func (m *MinIO) GetMetadata(ctx context.Context, key string) (*common.Metadata, error) {
	result, err := m.svc.HeadObjectWithContext(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(m.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}

	metadata := &common.Metadata{
		Size:         aws.Int64Value(result.ContentLength),
		LastModified: aws.TimeValue(result.LastModified),
		ETag:         aws.StringValue(result.ETag),
	}

	if result.ContentType != nil {
		metadata.ContentType = aws.StringValue(result.ContentType)
	}
	if result.ContentEncoding != nil {
		metadata.ContentEncoding = aws.StringValue(result.ContentEncoding)
	}

	// Convert MinIO metadata to custom metadata
	if len(result.Metadata) > 0 {
		metadata.Custom = make(map[string]string)
		for k, v := range result.Metadata {
			if v != nil {
				metadata.Custom[k] = *v
			}
		}
	}

	return metadata, nil
}

// UpdateMetadata updates the metadata for an existing object.
func (m *MinIO) UpdateMetadata(ctx context.Context, key string, metadata *common.Metadata) error {
	input := &s3.CopyObjectInput{
		Bucket:            aws.String(m.bucket),
		CopySource:        aws.String(m.bucket + "/" + key),
		Key:               aws.String(key),
		MetadataDirective: aws.String("REPLACE"),
	}

	if metadata != nil {
		if metadata.ContentType != "" {
			input.ContentType = aws.String(metadata.ContentType)
		}
		if metadata.ContentEncoding != "" {
			input.ContentEncoding = aws.String(metadata.ContentEncoding)
		}
		if len(metadata.Custom) > 0 {
			input.Metadata = make(map[string]*string)
			for k, v := range metadata.Custom {
				input.Metadata[k] = aws.String(v)
			}
		}
	}

	_, err := m.svc.CopyObjectWithContext(ctx, input)
	return err
}

// DeleteWithContext removes an object from the backend with context support.
func (m *MinIO) DeleteWithContext(ctx context.Context, key string) error {
	_, err := m.svc.DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(m.bucket),
		Key:    aws.String(key),
	})
	return err
}

// Exists checks if an object exists in the backend.
func (m *MinIO) Exists(ctx context.Context, key string) (bool, error) {
	_, err := m.svc.HeadObjectWithContext(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(m.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if strings.Contains(err.Error(), "NotFound") || strings.Contains(err.Error(), "404") {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// ListWithContext returns a list of keys with context support.
func (m *MinIO) ListWithContext(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	var continuationToken *string

	for {
		input := &s3.ListObjectsV2Input{
			Bucket: aws.String(m.bucket),
			Prefix: aws.String(prefix),
		}

		if continuationToken != nil {
			input.ContinuationToken = continuationToken
		}

		result, err := m.svc.ListObjectsV2WithContext(ctx, input)
		if err != nil {
			return nil, err
		}

		for _, obj := range result.Contents {
			if obj.Key != nil {
				keys = append(keys, *obj.Key)
			}
		}

		if !aws.BoolValue(result.IsTruncated) {
			break
		}

		continuationToken = result.NextContinuationToken
	}

	return keys, nil
}

// ListWithOptions returns a paginated list of objects with full metadata.
func (m *MinIO) ListWithOptions(ctx context.Context, opts *common.ListOptions) (*common.ListResult, error) {
	if opts == nil {
		opts = &common.ListOptions{}
	}

	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(m.bucket),
	}

	if opts.Prefix != "" {
		input.Prefix = aws.String(opts.Prefix)
	}
	if opts.Delimiter != "" {
		input.Delimiter = aws.String(opts.Delimiter)
	}
	if opts.MaxResults > 0 {
		input.MaxKeys = aws.Int64(int64(opts.MaxResults))
	}
	if opts.ContinueFrom != "" {
		input.ContinuationToken = aws.String(opts.ContinueFrom)
	}

	result, err := m.svc.ListObjectsV2WithContext(ctx, input)
	if err != nil {
		return nil, err
	}

	listResult := &common.ListResult{
		Objects:        make([]*common.ObjectInfo, 0, len(result.Contents)),
		CommonPrefixes: make([]string, 0, len(result.CommonPrefixes)),
		Truncated:      aws.BoolValue(result.IsTruncated),
	}

	// Convert MinIO objects to ObjectInfo
	for _, obj := range result.Contents {
		if obj.Key == nil {
			continue
		}

		metadata := &common.Metadata{
			Size: aws.Int64Value(obj.Size),
			ETag: aws.StringValue(obj.ETag),
		}
		if obj.LastModified != nil {
			metadata.LastModified = *obj.LastModified
		} else {
			metadata.LastModified = time.Now()
		}

		objInfo := &common.ObjectInfo{
			Key:      *obj.Key,
			Metadata: metadata,
		}
		listResult.Objects = append(listResult.Objects, objInfo)
	}

	// Convert common prefixes
	for _, prefix := range result.CommonPrefixes {
		if prefix.Prefix != nil {
			listResult.CommonPrefixes = append(listResult.CommonPrefixes, *prefix.Prefix)
		}
	}

	// Set next token
	if result.NextContinuationToken != nil {
		listResult.NextToken = *result.NextContinuationToken
	}

	return listResult, nil
}
