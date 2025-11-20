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
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/jeremyhahn/go-objstore/pkg/common"

	"cloud.google.com/go/storage"
)

// Test error variable
var errListError = errors.New("list error")

// TestGCS_PutWithContext tests the context-aware Put method
func TestGCS_PutWithContext(t *testing.T) {
	fc := fakeClient{b: fakeBucket{objs: map[string]*fakeObj{}}}
	g := &GCS{client: fc, bucket: "test-bucket"}
	ctx := context.Background()

	err := g.PutWithContext(ctx, "key", bytes.NewReader([]byte("data")))
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestGCS_PutWithContext_Error(t *testing.T) {
	objs := map[string]*fakeObj{
		"test-key": {writeErr: true},
	}
	fc := fakeClient{b: fakeBucket{objs: objs}}
	g := &GCS{client: fc, bucket: "test-bucket"}
	ctx := context.Background()

	err := g.PutWithContext(ctx, "test-key", bytes.NewReader([]byte("data")))
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// TestGCS_PutWithMetadata tests storing objects with metadata
func TestGCS_PutWithMetadata(t *testing.T) {
	fc := fakeClient{b: fakeBucket{objs: map[string]*fakeObj{}}}
	g := &GCS{client: fc, bucket: "test-bucket"}
	ctx := context.Background()

	metadata := &common.Metadata{
		ContentType:     "application/json",
		ContentEncoding: "gzip",
		Custom: map[string]string{
			"author":  "test",
			"version": "1.0",
		},
	}

	err := g.PutWithMetadata(ctx, "key", bytes.NewReader([]byte("data")), metadata)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestGCS_PutWithMetadata_NilMetadata(t *testing.T) {
	fc := fakeClient{b: fakeBucket{objs: map[string]*fakeObj{}}}
	g := &GCS{client: fc, bucket: "test-bucket"}
	ctx := context.Background()

	err := g.PutWithMetadata(ctx, "key", bytes.NewReader([]byte("data")), nil)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestGCS_PutWithMetadata_Error(t *testing.T) {
	objs := map[string]*fakeObj{
		"test-key": {writeErr: true},
	}
	fc := fakeClient{b: fakeBucket{objs: objs}}
	g := &GCS{client: fc, bucket: "test-bucket"}
	ctx := context.Background()

	metadata := &common.Metadata{
		ContentType: "text/plain",
	}

	err := g.PutWithMetadata(ctx, "test-key", bytes.NewReader([]byte("data")), metadata)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// TestGCS_GetWithContext tests context-aware Get
func TestGCS_GetWithContext(t *testing.T) {
	objs := map[string]*fakeObj{
		"test-key": {data: []byte("data")},
	}
	fc := fakeClient{b: fakeBucket{objs: objs}}
	g := &GCS{client: fc, bucket: "test-bucket"}
	ctx := context.Background()

	r, err := g.GetWithContext(ctx, "test-key")
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

func TestGCS_GetWithContext_Error(t *testing.T) {
	objs := map[string]*fakeObj{
		"test-key": {err: true},
	}
	fc := fakeClient{b: fakeBucket{objs: objs}}
	g := &GCS{client: fc, bucket: "test-bucket"}
	ctx := context.Background()

	_, err := g.GetWithContext(ctx, "test-key")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// TestGCS_GetMetadata tests metadata retrieval (stub implementation)
func TestGCS_GetMetadata(t *testing.T) {
	fc := fakeClient{b: fakeBucket{objs: map[string]*fakeObj{}}}
	g := &GCS{client: fc, bucket: "test-bucket"}
	ctx := context.Background()

	metadata, err := g.GetMetadata(ctx, "key")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Stub implementation returns nil
	if metadata != nil {
		t.Fatalf("expected nil metadata (stub), got %v", metadata)
	}
}

// TestGCS_UpdateMetadata tests metadata update (stub implementation)
func TestGCS_UpdateMetadata(t *testing.T) {
	fc := fakeClient{b: fakeBucket{objs: map[string]*fakeObj{}}}
	g := &GCS{client: fc, bucket: "test-bucket"}
	ctx := context.Background()

	metadata := &common.Metadata{
		ContentType:     "text/plain",
		ContentEncoding: "none",
		Custom: map[string]string{
			"updated": "true",
		},
	}

	err := g.UpdateMetadata(ctx, "key", metadata)
	if err != nil {
		t.Fatalf("expected no error (stub), got %v", err)
	}
}

func TestGCS_UpdateMetadata_NilMetadata(t *testing.T) {
	fc := fakeClient{b: fakeBucket{objs: map[string]*fakeObj{}}}
	g := &GCS{client: fc, bucket: "test-bucket"}
	ctx := context.Background()

	err := g.UpdateMetadata(ctx, "key", nil)
	if err != nil {
		t.Fatalf("expected no error (stub), got %v", err)
	}
}

// TestGCS_DeleteWithContext tests context-aware Delete
func TestGCS_DeleteWithContext(t *testing.T) {
	objs := map[string]*fakeObj{
		"test-key": {data: []byte("data")},
	}
	fc := fakeClient{b: fakeBucket{objs: objs}}
	g := &GCS{client: fc, bucket: "test-bucket"}
	ctx := context.Background()

	err := g.DeleteWithContext(ctx, "test-key")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestGCS_DeleteWithContext_Error(t *testing.T) {
	objs := map[string]*fakeObj{
		"test-key": {deleteErr: true},
	}
	fc := fakeClient{b: fakeBucket{objs: objs}}
	g := &GCS{client: fc, bucket: "test-bucket"}
	ctx := context.Background()

	err := g.DeleteWithContext(ctx, "test-key")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// TestGCS_Exists tests object existence check
func TestGCS_Exists(t *testing.T) {
	// Test with an object that exists
	fc := fakeClient{b: fakeBucket{objs: map[string]*fakeObj{
		"existing": {data: []byte("test data")},
	}}}
	g := &GCS{client: fc, bucket: "test-bucket"}
	ctx := context.Background()

	exists, err := g.Exists(ctx, "existing")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !exists {
		t.Fatal("expected existing object to exist")
	}

	// Test with an object that doesn't exist
	exists, err = g.Exists(ctx, "nonexistent")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if exists {
		t.Fatal("expected nonexistent object to not exist")
	}
}

// TestGCS_ListWithContext tests context-aware list
func TestGCS_ListWithContext(t *testing.T) {
	iter := &fakeIterator{
		objects: []*storage.ObjectAttrs{
			{Name: "file1.txt"},
			{Name: "file2.txt"},
		},
	}

	fc := fakeClient{
		b: fakeBucket{
			objs:     map[string]*fakeObj{},
			iterator: iter,
		},
	}

	g := &GCS{client: fc, bucket: "test-bucket"}
	ctx := context.Background()

	keys, err := g.ListWithContext(ctx, "")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if len(keys) != 2 {
		t.Fatalf("expected 2 keys, got %d", len(keys))
	}
}

func TestGCS_ListWithContext_WithPrefix(t *testing.T) {
	iter := &fakeIterator{
		objects: []*storage.ObjectAttrs{
			{Name: "prefix/file1.txt"},
			{Name: "prefix/file2.txt"},
		},
	}

	fc := fakeClient{
		b: fakeBucket{
			objs:     map[string]*fakeObj{},
			iterator: iter,
		},
	}

	g := &GCS{client: fc, bucket: "test-bucket"}
	ctx := context.Background()

	keys, err := g.ListWithContext(ctx, "prefix/")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if len(keys) != 2 {
		t.Fatalf("expected 2 keys, got %d", len(keys))
	}
}

func TestGCS_ListWithContext_Error(t *testing.T) {
	iter := &fakeIterator{
		err: errListError,
	}

	fc := fakeClient{
		b: fakeBucket{
			objs:     map[string]*fakeObj{},
			iterator: iter,
		},
	}

	g := &GCS{client: fc, bucket: "test-bucket"}
	ctx := context.Background()

	_, err := g.ListWithContext(ctx, "")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// TestGCS_ListWithOptions tests paginated list with options
func TestGCS_ListWithOptions(t *testing.T) {
	iter := &fakeIterator{
		objects: []*storage.ObjectAttrs{
			{Name: "file1.txt"},
			{Name: "file2.txt"},
			{Name: "file3.txt"},
		},
	}

	fc := fakeClient{
		b: fakeBucket{
			objs:     map[string]*fakeObj{},
			iterator: iter,
		},
	}

	g := &GCS{client: fc, bucket: "test-bucket"}
	ctx := context.Background()

	opts := &common.ListOptions{
		Prefix:     "test/",
		Delimiter:  "/",
		MaxResults: 2,
	}

	result, err := g.ListWithOptions(ctx, opts)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if len(result.Objects) != 2 {
		t.Fatalf("expected 2 objects (limited by MaxResults), got %d", len(result.Objects))
	}
	if !result.Truncated {
		t.Fatal("expected truncated to be true")
	}
	if result.NextToken != "file2.txt" {
		t.Fatalf("expected file2.txt as NextToken, got %s", result.NextToken)
	}
}

func TestGCS_ListWithOptions_NilOptions(t *testing.T) {
	iter := &fakeIterator{
		objects: []*storage.ObjectAttrs{},
	}

	fc := fakeClient{
		b: fakeBucket{
			objs:     map[string]*fakeObj{},
			iterator: iter,
		},
	}

	g := &GCS{client: fc, bucket: "test-bucket"}
	ctx := context.Background()

	result, err := g.ListWithOptions(ctx, nil)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if result == nil {
		t.Fatal("expected result, got nil")
	}
}

func TestGCS_ListWithOptions_WithDelimiter(t *testing.T) {
	iter := &fakeIterator{
		objects: []*storage.ObjectAttrs{
			{Name: "file1.txt"},
			{Prefix: "dir/"}, // Common prefix
			{Name: "file2.txt"},
		},
	}

	fc := fakeClient{
		b: fakeBucket{
			objs:     map[string]*fakeObj{},
			iterator: iter,
		},
	}

	g := &GCS{client: fc, bucket: "test-bucket"}
	ctx := context.Background()

	opts := &common.ListOptions{
		Delimiter: "/",
	}

	result, err := g.ListWithOptions(ctx, opts)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if len(result.Objects) != 2 {
		t.Fatalf("expected 2 objects, got %d", len(result.Objects))
	}
	if len(result.CommonPrefixes) != 1 {
		t.Fatalf("expected 1 common prefix, got %d", len(result.CommonPrefixes))
	}
}

func TestGCS_ListWithOptions_ContinueFrom(t *testing.T) {
	iter := &fakeIterator{
		objects: []*storage.ObjectAttrs{
			{Name: "file1.txt"},
			{Name: "file2.txt"},
			{Name: "file3.txt"},
			{Name: "file4.txt"},
		},
	}

	fc := fakeClient{
		b: fakeBucket{
			objs:     map[string]*fakeObj{},
			iterator: iter,
		},
	}

	g := &GCS{client: fc, bucket: "test-bucket"}
	ctx := context.Background()

	opts := &common.ListOptions{
		ContinueFrom: "file2.txt",
		MaxResults:   1,
	}

	result, err := g.ListWithOptions(ctx, opts)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Should skip to file2.txt, then return file3.txt (1 result)
	if len(result.Objects) != 1 {
		t.Fatalf("expected 1 object, got %d", len(result.Objects))
	}
	if result.Objects[0].Key != "file3.txt" {
		t.Fatalf("expected file3.txt, got %s", result.Objects[0].Key)
	}
}

func TestGCS_ListWithOptions_IteratorDone(t *testing.T) {
	iter := &fakeIterator{
		objects: []*storage.ObjectAttrs{
			{Name: "file1.txt"},
		},
	}

	fc := fakeClient{
		b: fakeBucket{
			objs:     map[string]*fakeObj{},
			iterator: iter,
		},
	}

	g := &GCS{client: fc, bucket: "test-bucket"}
	ctx := context.Background()

	opts := &common.ListOptions{
		ContinueFrom: "file1.txt",
	}

	result, err := g.ListWithOptions(ctx, opts)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Iterator exhausted during skip, should return empty result
	if len(result.Objects) != 0 {
		t.Fatalf("expected 0 objects, got %d", len(result.Objects))
	}
}

func TestGCS_ListWithOptions_Error(t *testing.T) {
	iter := &fakeIterator{
		err: errListError,
	}

	fc := fakeClient{
		b: fakeBucket{
			objs:     map[string]*fakeObj{},
			iterator: iter,
		},
	}

	g := &GCS{client: fc, bucket: "test-bucket"}
	ctx := context.Background()

	_, err := g.ListWithOptions(ctx, &common.ListOptions{})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestGCS_ListWithOptions_ErrorDuringSkip(t *testing.T) {
	iter := &fakeIterator{
		objects: []*storage.ObjectAttrs{
			{Name: "file1.txt"},
		},
		// Will return error after first object
		err: nil,
	}

	fc := fakeClient{
		b: fakeBucket{
			objs:     map[string]*fakeObj{},
			iterator: iter,
		},
	}

	g := &GCS{client: fc, bucket: "test-bucket"}
	ctx := context.Background()

	opts := &common.ListOptions{
		ContinueFrom: "file999.txt", // Non-existent, will exhaust iterator
	}

	result, err := g.ListWithOptions(ctx, opts)
	if err != nil {
		t.Fatalf("expected no error (iterator.Done is not an error), got %v", err)
	}

	if len(result.Objects) != 0 {
		t.Fatalf("expected 0 objects, got %d", len(result.Objects))
	}
}

func TestGCS_Contains(t *testing.T) {
	slice := []string{"a", "b", "c"}

	if !contains(slice, "b") {
		t.Fatal("expected contains to return true for 'b'")
	}

	if contains(slice, "d") {
		t.Fatal("expected contains to return false for 'd'")
	}

	if contains([]string{}, "a") {
		t.Fatal("expected contains to return false for empty slice")
	}
}
