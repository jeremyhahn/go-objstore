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

package common

import (
	"testing"
	"time"
)

func TestMetadata(t *testing.T) {
	t.Run("create metadata with all fields", func(t *testing.T) {
		now := time.Now()
		meta := &Metadata{
			ContentType:     "application/json",
			ContentEncoding: "gzip",
			Size:            1024,
			LastModified:    now,
			ETag:            "abc123",
			Custom: map[string]string{
				"author":  "test",
				"version": "1.0",
			},
		}

		if meta.ContentType != "application/json" {
			t.Errorf("expected ContentType 'application/json', got '%s'", meta.ContentType)
		}
		if meta.ContentEncoding != "gzip" {
			t.Errorf("expected ContentEncoding 'gzip', got '%s'", meta.ContentEncoding)
		}
		if meta.Size != 1024 {
			t.Errorf("expected Size 1024, got %d", meta.Size)
		}
		if !meta.LastModified.Equal(now) {
			t.Errorf("expected LastModified %v, got %v", now, meta.LastModified)
		}
		if meta.ETag != "abc123" {
			t.Errorf("expected ETag 'abc123', got '%s'", meta.ETag)
		}
		if meta.Custom["author"] != "test" {
			t.Errorf("expected Custom author 'test', got '%s'", meta.Custom["author"])
		}
	})

	t.Run("create minimal metadata", func(t *testing.T) {
		now := time.Now()
		meta := &Metadata{
			Size:         512,
			LastModified: now,
		}

		if meta.Size != 512 {
			t.Errorf("expected Size 512, got %d", meta.Size)
		}
		if meta.ContentType != "" {
			t.Errorf("expected empty ContentType, got '%s'", meta.ContentType)
		}
		if meta.Custom != nil {
			t.Errorf("expected nil Custom map, got %v", meta.Custom)
		}
	})
}

func TestObjectInfo(t *testing.T) {
	t.Run("create object info with metadata", func(t *testing.T) {
		now := time.Now()
		obj := &ObjectInfo{
			Key: "test/key",
			Metadata: &Metadata{
				Size:         100,
				LastModified: now,
				ContentType:  "text/plain",
			},
		}

		if obj.Key != "test/key" {
			t.Errorf("expected Key 'test/key', got '%s'", obj.Key)
		}
		if obj.Metadata == nil {
			t.Fatal("expected non-nil Metadata")
		}
		if obj.Metadata.Size != 100 {
			t.Errorf("expected Size 100, got %d", obj.Metadata.Size)
		}
	})

	t.Run("create object info without metadata", func(t *testing.T) {
		obj := &ObjectInfo{
			Key: "test/key2",
		}

		if obj.Key != "test/key2" {
			t.Errorf("expected Key 'test/key2', got '%s'", obj.Key)
		}
		if obj.Metadata != nil {
			t.Errorf("expected nil Metadata, got %v", obj.Metadata)
		}
	})
}

func TestListOptions(t *testing.T) {
	t.Run("create list options with all fields", func(t *testing.T) {
		opts := &ListOptions{
			Prefix:       "test/",
			Delimiter:    "/",
			MaxResults:   100,
			ContinueFrom: "token123",
		}

		if opts.Prefix != "test/" {
			t.Errorf("expected Prefix 'test/', got '%s'", opts.Prefix)
		}
		if opts.Delimiter != "/" {
			t.Errorf("expected Delimiter '/', got '%s'", opts.Delimiter)
		}
		if opts.MaxResults != 100 {
			t.Errorf("expected MaxResults 100, got %d", opts.MaxResults)
		}
		if opts.ContinueFrom != "token123" {
			t.Errorf("expected ContinueFrom 'token123', got '%s'", opts.ContinueFrom)
		}
	})

	t.Run("create minimal list options", func(t *testing.T) {
		opts := &ListOptions{
			Prefix: "data/",
		}

		if opts.Prefix != "data/" {
			t.Errorf("expected Prefix 'data/', got '%s'", opts.Prefix)
		}
		if opts.Delimiter != "" {
			t.Errorf("expected empty Delimiter, got '%s'", opts.Delimiter)
		}
		if opts.MaxResults != 0 {
			t.Errorf("expected MaxResults 0, got %d", opts.MaxResults)
		}
	})
}

func TestListResult(t *testing.T) {
	t.Run("create list result with objects", func(t *testing.T) {
		now := time.Now()
		result := &ListResult{
			Objects: []*ObjectInfo{
				{
					Key: "obj1",
					Metadata: &Metadata{
						Size:         100,
						LastModified: now,
					},
				},
				{
					Key: "obj2",
					Metadata: &Metadata{
						Size:         200,
						LastModified: now,
					},
				},
			},
			NextToken: "next123",
			Truncated: true,
		}

		if len(result.Objects) != 2 {
			t.Errorf("expected 2 objects, got %d", len(result.Objects))
		}
		if result.Objects[0].Key != "obj1" {
			t.Errorf("expected first object key 'obj1', got '%s'", result.Objects[0].Key)
		}
		if result.NextToken != "next123" {
			t.Errorf("expected NextToken 'next123', got '%s'", result.NextToken)
		}
		if !result.Truncated {
			t.Error("expected Truncated to be true")
		}
	})

	t.Run("create list result with common prefixes", func(t *testing.T) {
		result := &ListResult{
			Objects: []*ObjectInfo{},
			CommonPrefixes: []string{
				"a/b/",
				"a/c/",
			},
			Truncated: false,
		}

		if len(result.CommonPrefixes) != 2 {
			t.Errorf("expected 2 common prefixes, got %d", len(result.CommonPrefixes))
		}
		if result.CommonPrefixes[0] != "a/b/" {
			t.Errorf("expected first prefix 'a/b/', got '%s'", result.CommonPrefixes[0])
		}
		if result.Truncated {
			t.Error("expected Truncated to be false")
		}
	})

	t.Run("create empty list result", func(t *testing.T) {
		result := &ListResult{
			Objects:   []*ObjectInfo{},
			Truncated: false,
		}

		if len(result.Objects) != 0 {
			t.Errorf("expected 0 objects, got %d", len(result.Objects))
		}
		if result.NextToken != "" {
			t.Errorf("expected empty NextToken, got '%s'", result.NextToken)
		}
		if result.Truncated {
			t.Error("expected Truncated to be false")
		}
	})
}
