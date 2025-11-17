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

//go:build awss3

package s3

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
)

// Extended mock to support context methods
func (m *mockS3Client) PutObjectWithContext(ctx aws.Context, input *s3.PutObjectInput, opts ...request.Option) (*s3.PutObjectOutput, error) {
	if m.putObjectError != nil {
		return nil, m.putObjectError
	}
	return m.putObjectOutput, nil
}

func (m *mockS3Client) GetObjectWithContext(ctx aws.Context, input *s3.GetObjectInput, opts ...request.Option) (*s3.GetObjectOutput, error) {
	if m.getObjectError != nil {
		return nil, m.getObjectError
	}
	return m.getObjectOutput, nil
}

func (m *mockS3Client) DeleteObjectWithContext(ctx aws.Context, input *s3.DeleteObjectInput, opts ...request.Option) (*s3.DeleteObjectOutput, error) {
	if m.deleteObjectError != nil {
		return nil, m.deleteObjectError
	}
	return m.deleteObjectOutput, nil
}

func (m *mockS3Client) HeadObjectWithContext(ctx aws.Context, input *s3.HeadObjectInput, opts ...request.Option) (*s3.HeadObjectOutput, error) {
	if m.headObjectError != nil {
		return nil, m.headObjectError
	}
	return m.headObjectOutput, nil
}

func (m *mockS3Client) CopyObjectWithContext(ctx aws.Context, input *s3.CopyObjectInput, opts ...request.Option) (*s3.CopyObjectOutput, error) {
	if m.copyObjectError != nil {
		return nil, m.copyObjectError
	}
	return m.copyObjectOutput, nil
}

func (m *mockS3Client) ListObjectsV2WithContext(ctx aws.Context, input *s3.ListObjectsV2Input, opts ...request.Option) (*s3.ListObjectsV2Output, error) {
	if m.listObjectsV2Error != nil {
		return nil, m.listObjectsV2Error
	}
	if len(m.listObjectsV2Outputs) > 0 {
		output := m.listObjectsV2Outputs[m.listObjectsV2Calls]
		m.listObjectsV2Calls++
		return output, nil
	}
	return m.listObjectsV2Output, nil
}

// TestS3_PutWithContext tests the context-aware Put method
func TestS3_PutWithContext(t *testing.T) {
	mockS3 := &mockS3Client{
		putObjectOutput: &s3.PutObjectOutput{},
	}

	s := &S3{svc: mockS3, bucket: "test-bucket"}
	ctx := context.Background()

	err := s.PutWithContext(ctx, "key", bytes.NewReader([]byte("data")))
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestS3_PutWithContext_Error(t *testing.T) {
	mockS3 := &mockS3Client{
		putObjectError: errors.New("upload error"),
	}

	s := &S3{svc: mockS3, bucket: "test-bucket"}
	ctx := context.Background()

	err := s.PutWithContext(ctx, "key", bytes.NewReader([]byte("data")))
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// TestS3_PutWithMetadata tests storing objects with metadata
func TestS3_PutWithMetadata(t *testing.T) {
	mockS3 := &mockS3Client{
		putObjectOutput: &s3.PutObjectOutput{},
	}

	s := &S3{svc: mockS3, bucket: "test-bucket"}
	ctx := context.Background()

	metadata := &common.Metadata{
		ContentType:     "application/json",
		ContentEncoding: "gzip",
		Custom: map[string]string{
			"author":  "test",
			"version": "1.0",
		},
	}

	err := s.PutWithMetadata(ctx, "key", bytes.NewReader([]byte("data")), metadata)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestS3_PutWithMetadata_NilMetadata(t *testing.T) {
	mockS3 := &mockS3Client{
		putObjectOutput: &s3.PutObjectOutput{},
	}

	s := &S3{svc: mockS3, bucket: "test-bucket"}
	ctx := context.Background()

	err := s.PutWithMetadata(ctx, "key", bytes.NewReader([]byte("data")), nil)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestS3_PutWithMetadata_Error(t *testing.T) {
	mockS3 := &mockS3Client{
		putObjectError: errors.New("upload error"),
	}

	s := &S3{svc: mockS3, bucket: "test-bucket"}
	ctx := context.Background()

	metadata := &common.Metadata{
		ContentType: "text/plain",
	}

	err := s.PutWithMetadata(ctx, "key", bytes.NewReader([]byte("data")), metadata)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// TestS3_GetWithContext tests context-aware Get
func TestS3_GetWithContext(t *testing.T) {
	mockS3 := &mockS3Client{
		getObjectOutput: &s3.GetObjectOutput{
			Body: io.NopCloser(bytes.NewReader([]byte("data"))),
		},
	}

	s := &S3{svc: mockS3, bucket: "test-bucket"}
	ctx := context.Background()

	r, err := s.GetWithContext(ctx, "key")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	defer r.Close()

	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("error reading data: %v", err)
	}

	if string(data) != "data" {
		t.Fatalf("expected %s, got %s", "data", string(data))
	}
}

func TestS3_GetWithContext_Error(t *testing.T) {
	mockS3 := &mockS3Client{
		getObjectError: errors.New("get error"),
	}

	s := &S3{svc: mockS3, bucket: "test-bucket"}
	ctx := context.Background()

	_, err := s.GetWithContext(ctx, "key")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// TestS3_GetMetadata tests metadata retrieval
func TestS3_GetMetadata(t *testing.T) {
	now := time.Now()
	mockS3 := &mockS3Client{
		headObjectOutput: &s3.HeadObjectOutput{
			ContentLength:   aws.Int64(1024),
			LastModified:    &now,
			ETag:            aws.String("abc123"),
			ContentType:     aws.String("application/json"),
			ContentEncoding: aws.String("gzip"),
			Metadata: map[string]*string{
				"author":  aws.String("test"),
				"version": aws.String("1.0"),
			},
		},
	}

	s := &S3{svc: mockS3, bucket: "test-bucket"}
	ctx := context.Background()

	metadata, err := s.GetMetadata(ctx, "key")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if metadata.Size != 1024 {
		t.Errorf("expected size 1024, got %d", metadata.Size)
	}
	if metadata.ETag != "abc123" {
		t.Errorf("expected ETag abc123, got %s", metadata.ETag)
	}
	if metadata.ContentType != "application/json" {
		t.Errorf("expected ContentType application/json, got %s", metadata.ContentType)
	}
	if metadata.ContentEncoding != "gzip" {
		t.Errorf("expected ContentEncoding gzip, got %s", metadata.ContentEncoding)
	}
	if metadata.Custom["author"] != "test" {
		t.Errorf("expected author test, got %s", metadata.Custom["author"])
	}
}

func TestS3_GetMetadata_MinimalFields(t *testing.T) {
	now := time.Now()
	mockS3 := &mockS3Client{
		headObjectOutput: &s3.HeadObjectOutput{
			ContentLength: aws.Int64(512),
			LastModified:  &now,
			ETag:          aws.String("def456"),
		},
	}

	s := &S3{svc: mockS3, bucket: "test-bucket"}
	ctx := context.Background()

	metadata, err := s.GetMetadata(ctx, "key")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if metadata.Size != 512 {
		t.Errorf("expected size 512, got %d", metadata.Size)
	}
	if metadata.ContentType != "" {
		t.Errorf("expected empty ContentType, got %s", metadata.ContentType)
	}
}

func TestS3_GetMetadata_Error(t *testing.T) {
	mockS3 := &mockS3Client{
		headObjectError: errors.New("head error"),
	}

	s := &S3{svc: mockS3, bucket: "test-bucket"}
	ctx := context.Background()

	_, err := s.GetMetadata(ctx, "key")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// TestS3_UpdateMetadata tests metadata update
func TestS3_UpdateMetadata(t *testing.T) {
	mockS3 := &mockS3Client{
		copyObjectOutput: &s3.CopyObjectOutput{},
	}

	s := &S3{svc: mockS3, bucket: "test-bucket"}
	ctx := context.Background()

	metadata := &common.Metadata{
		ContentType:     "text/plain",
		ContentEncoding: "none",
		Custom: map[string]string{
			"updated": "true",
		},
	}

	err := s.UpdateMetadata(ctx, "key", metadata)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestS3_UpdateMetadata_NilMetadata(t *testing.T) {
	mockS3 := &mockS3Client{
		copyObjectOutput: &s3.CopyObjectOutput{},
	}

	s := &S3{svc: mockS3, bucket: "test-bucket"}
	ctx := context.Background()

	err := s.UpdateMetadata(ctx, "key", nil)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestS3_UpdateMetadata_Error(t *testing.T) {
	mockS3 := &mockS3Client{
		copyObjectError: errors.New("copy error"),
	}

	s := &S3{svc: mockS3, bucket: "test-bucket"}
	ctx := context.Background()

	metadata := &common.Metadata{
		ContentType: "application/xml",
	}

	err := s.UpdateMetadata(ctx, "key", metadata)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// TestS3_DeleteWithContext tests context-aware Delete
func TestS3_DeleteWithContext(t *testing.T) {
	mockS3 := &mockS3Client{
		deleteObjectOutput: &s3.DeleteObjectOutput{},
	}

	s := &S3{svc: mockS3, bucket: "test-bucket"}
	ctx := context.Background()

	err := s.DeleteWithContext(ctx, "key")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestS3_DeleteWithContext_Error(t *testing.T) {
	mockS3 := &mockS3Client{
		deleteObjectError: errors.New("delete error"),
	}

	s := &S3{svc: mockS3, bucket: "test-bucket"}
	ctx := context.Background()

	err := s.DeleteWithContext(ctx, "key")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// TestS3_Exists tests object existence check
func TestS3_Exists(t *testing.T) {
	mockS3 := &mockS3Client{
		headObjectOutput: &s3.HeadObjectOutput{
			ContentLength: aws.Int64(100),
		},
	}

	s := &S3{svc: mockS3, bucket: "test-bucket"}
	ctx := context.Background()

	exists, err := s.Exists(ctx, "key")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !exists {
		t.Fatal("expected object to exist")
	}
}

func TestS3_Exists_NotFound(t *testing.T) {
	mockS3 := &mockS3Client{
		headObjectError: errors.New("NotFound"),
	}

	s := &S3{svc: mockS3, bucket: "test-bucket"}
	ctx := context.Background()

	exists, err := s.Exists(ctx, "key")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if exists {
		t.Fatal("expected object to not exist")
	}
}

func TestS3_Exists_OtherError(t *testing.T) {
	mockS3 := &mockS3Client{
		headObjectError: errors.New("access denied"),
	}

	s := &S3{svc: mockS3, bucket: "test-bucket"}
	ctx := context.Background()

	_, err := s.Exists(ctx, "key")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// TestS3_ListWithContext tests context-aware list
func TestS3_ListWithContext(t *testing.T) {
	mockS3 := &mockS3Client{
		listObjectsV2Output: &s3.ListObjectsV2Output{
			Contents: []*s3.Object{
				{Key: aws.String("file1.txt")},
				{Key: aws.String("file2.txt")},
			},
			IsTruncated: aws.Bool(false),
		},
	}

	s := &S3{svc: mockS3, bucket: "test-bucket"}
	ctx := context.Background()

	keys, err := s.ListWithContext(ctx, "")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if len(keys) != 2 {
		t.Fatalf("expected 2 keys, got %d", len(keys))
	}
}

func TestS3_ListWithContext_WithPagination(t *testing.T) {
	mockS3 := &mockS3Client{
		listObjectsV2Outputs: []*s3.ListObjectsV2Output{
			{
				Contents: []*s3.Object{
					{Key: aws.String("file1.txt")},
				},
				IsTruncated:           aws.Bool(true),
				NextContinuationToken: aws.String("token1"),
			},
			{
				Contents: []*s3.Object{
					{Key: aws.String("file2.txt")},
				},
				IsTruncated: aws.Bool(false),
			},
		},
	}

	s := &S3{svc: mockS3, bucket: "test-bucket"}
	ctx := context.Background()

	keys, err := s.ListWithContext(ctx, "prefix/")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if len(keys) != 2 {
		t.Fatalf("expected 2 keys, got %d", len(keys))
	}
}

func TestS3_ListWithContext_Error(t *testing.T) {
	mockS3 := &mockS3Client{
		listObjectsV2Error: errors.New("list error"),
	}

	s := &S3{svc: mockS3, bucket: "test-bucket"}
	ctx := context.Background()

	_, err := s.ListWithContext(ctx, "")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// TestS3_ListWithOptions tests paginated list with options
func TestS3_ListWithOptions(t *testing.T) {
	now := time.Now()
	mockS3 := &mockS3Client{
		listObjectsV2Output: &s3.ListObjectsV2Output{
			Contents: []*s3.Object{
				{
					Key:          aws.String("file1.txt"),
					Size:         aws.Int64(100),
					ETag:         aws.String("etag1"),
					LastModified: &now,
				},
				{
					Key:          aws.String("file2.txt"),
					Size:         aws.Int64(200),
					ETag:         aws.String("etag2"),
					LastModified: &now,
				},
			},
			CommonPrefixes: []*s3.CommonPrefix{
				{Prefix: aws.String("dir/")},
			},
			IsTruncated:           aws.Bool(true),
			NextContinuationToken: aws.String("next-token"),
		},
	}

	s := &S3{svc: mockS3, bucket: "test-bucket"}
	ctx := context.Background()

	opts := &common.ListOptions{
		Prefix:       "test/",
		Delimiter:    "/",
		MaxResults:   10,
		ContinueFrom: "start",
	}

	result, err := s.ListWithOptions(ctx, opts)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if len(result.Objects) != 2 {
		t.Fatalf("expected 2 objects, got %d", len(result.Objects))
	}
	if len(result.CommonPrefixes) != 1 {
		t.Fatalf("expected 1 common prefix, got %d", len(result.CommonPrefixes))
	}
	if !result.Truncated {
		t.Fatal("expected truncated to be true")
	}
	if result.NextToken != "next-token" {
		t.Fatalf("expected next-token, got %s", result.NextToken)
	}
}

func TestS3_ListWithOptions_NilOptions(t *testing.T) {
	mockS3 := &mockS3Client{
		listObjectsV2Output: &s3.ListObjectsV2Output{
			Contents:    []*s3.Object{},
			IsTruncated: aws.Bool(false),
		},
	}

	s := &S3{svc: mockS3, bucket: "test-bucket"}
	ctx := context.Background()

	result, err := s.ListWithOptions(ctx, nil)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if result == nil {
		t.Fatal("expected result, got nil")
	}
}

func TestS3_ListWithOptions_NilKey(t *testing.T) {
	mockS3 := &mockS3Client{
		listObjectsV2Output: &s3.ListObjectsV2Output{
			Contents: []*s3.Object{
				{Key: nil}, // Nil key should be skipped
				{Key: aws.String("valid.txt")},
			},
			IsTruncated: aws.Bool(false),
		},
	}

	s := &S3{svc: mockS3, bucket: "test-bucket"}
	ctx := context.Background()

	result, err := s.ListWithOptions(ctx, &common.ListOptions{})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if len(result.Objects) != 1 {
		t.Fatalf("expected 1 object (nil key skipped), got %d", len(result.Objects))
	}
}

func TestS3_ListWithOptions_NilLastModified(t *testing.T) {
	mockS3 := &mockS3Client{
		listObjectsV2Output: &s3.ListObjectsV2Output{
			Contents: []*s3.Object{
				{
					Key:          aws.String("file.txt"),
					Size:         aws.Int64(100),
					LastModified: nil, // Should default to time.Now()
				},
			},
			IsTruncated: aws.Bool(false),
		},
	}

	s := &S3{svc: mockS3, bucket: "test-bucket"}
	ctx := context.Background()

	result, err := s.ListWithOptions(ctx, &common.ListOptions{})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if len(result.Objects) != 1 {
		t.Fatalf("expected 1 object, got %d", len(result.Objects))
	}
	if result.Objects[0].Metadata.LastModified.IsZero() {
		t.Fatal("expected LastModified to be set")
	}
}

func TestS3_ListWithOptions_Error(t *testing.T) {
	mockS3 := &mockS3Client{
		listObjectsV2Error: errors.New("list error"),
	}

	s := &S3{svc: mockS3, bucket: "test-bucket"}
	ctx := context.Background()

	_, err := s.ListWithOptions(ctx, &common.ListOptions{})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}
