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

// Package storagetest is the contract suite for common.Storage: the
// guarantees a caller of the provider seam relies on (see
// docs/architecture/storage-layer.md). Every backend that runs it behaves
// the same way on each of them.
package storagetest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// StoreFactory returns a fresh, empty, configured Storage.
type StoreFactory func(t *testing.T) common.Storage

// noErr fails the test when err is not nil.
func noErr(t *testing.T, err error, what string) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %v", what, err)
	}
}

// isErr fails the test when err does not wrap target.
func isErr(t *testing.T, err, target error, what string) {
	t.Helper()
	if !errors.Is(err, target) {
		t.Errorf("%s: err = %v, want %v", what, err, target)
	}
}

// equal fails the test when got and want differ.
func equal(t *testing.T, got, want any, what string) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("%s = %v, want %v", what, got, want)
	}
}

// read returns the content of key.
func read(t *testing.T, r io.ReadCloser, err error) string {
	t.Helper()
	noErr(t, err, "get")
	defer func() { _ = r.Close() }()
	b, err := io.ReadAll(r)
	noErr(t, err, "read")
	return string(b)
}

// put stores each key with its own name as content.
func put(t *testing.T, s common.Storage, keys ...string) {
	t.Helper()
	for _, k := range keys {
		noErr(t, s.Put(k, strings.NewReader(k)), "put "+k)
	}
}

// sorted returns a sorted copy of keys, with nil as an empty slice.
func sorted(keys []string) []string {
	out := append([]string{}, keys...)
	sort.Strings(out)
	return out
}

// Run runs the Storage contract tests, each on a fresh store.
func Run(t *testing.T, newStorage StoreFactory) {
	ctx := context.Background()

	t.Run("put get round trip and overwrite", func(t *testing.T) {
		s := newStorage(t)
		noErr(t, s.Put("a/b.txt", strings.NewReader("first")), "put")
		r, err := s.Get("a/b.txt")
		equal(t, read(t, r, err), "first", "content")
		noErr(t, s.Put("a/b.txt", strings.NewReader("second")), "overwrite")
		r, err = s.Get("a/b.txt")
		equal(t, read(t, r, err), "second", "content after overwrite")
	})

	t.Run("put get with context", func(t *testing.T) {
		s := newStorage(t)
		noErr(t, s.PutWithContext(ctx, "k", strings.NewReader("ctx")), "put")
		r, err := s.GetWithContext(ctx, "k")
		equal(t, read(t, r, err), "ctx", "content")
	})

	t.Run("missing key is not found", func(t *testing.T) {
		s := newStorage(t)
		_, err := s.Get("missing")
		isErr(t, err, common.ErrKeyNotFound, "Get")
		_, err = s.GetWithContext(ctx, "missing")
		isErr(t, err, common.ErrKeyNotFound, "GetWithContext")
		err = s.UpdateMetadata(ctx, "missing", &common.Metadata{ContentType: "text/plain"})
		isErr(t, err, common.ErrKeyNotFound, "UpdateMetadata")
		isErr(t, s.Delete("missing"), common.ErrKeyNotFound, "Delete")
	})

	t.Run("exists", func(t *testing.T) {
		s := newStorage(t)
		put(t, s, "here")
		ok, err := s.Exists(ctx, "here")
		noErr(t, err, "exists")
		equal(t, ok, true, "Exists(here)")
		ok, err = s.Exists(ctx, "absent")
		noErr(t, err, "exists")
		equal(t, ok, false, "Exists(absent)")
	})

	t.Run("delete then get is not found", func(t *testing.T) {
		s := newStorage(t)
		put(t, s, "gone")
		noErr(t, s.Delete("gone"), "delete")
		_, err := s.Get("gone")
		isErr(t, err, common.ErrKeyNotFound, "Get after Delete")
		ok, err := s.Exists(ctx, "gone")
		noErr(t, err, "exists")
		equal(t, ok, false, "Exists after Delete")
	})

	t.Run("list by prefix", func(t *testing.T) {
		s := newStorage(t)
		put(t, s, "p/one", "p/two", "p/sub/three", "q/four", "pz")
		want := []string{"p/one", "p/sub/three", "p/two"}
		keys, err := s.List("p/")
		noErr(t, err, "List")
		equal(t, sorted(keys), want, "List(p/)")
		keys, err = s.ListWithContext(ctx, "p/")
		noErr(t, err, "ListWithContext")
		equal(t, sorted(keys), want, "ListWithContext(p/)")
		keys, err = s.List("")
		noErr(t, err, "List all")
		equal(t, len(keys), 5, "len(List(\"\"))")
		keys, err = s.List("none/")
		noErr(t, err, "List none")
		equal(t, len(keys), 0, "len(List(none/))")
	})

	t.Run("list with options pages every key once", func(t *testing.T) {
		s := newStorage(t)
		var want []string
		for i := range 7 {
			want = append(want, fmt.Sprintf("page/k%02d", i))
		}
		put(t, s, want...)
		put(t, s, "other")
		var got []string
		opts := &common.ListOptions{Prefix: "page/", MaxResults: 3}
		for pages := 0; ; pages++ {
			if pages > len(want) {
				t.Fatal("pagination does not terminate")
			}
			res, err := s.ListWithOptions(ctx, opts)
			noErr(t, err, "ListWithOptions")
			if len(res.Objects) > opts.MaxResults {
				t.Fatalf("page of %d objects exceeds MaxResults %d", len(res.Objects), opts.MaxResults)
			}
			for _, o := range res.Objects {
				got = append(got, o.Key)
			}
			if !res.Truncated {
				break
			}
			opts.ContinueFrom = res.NextToken
		}
		equal(t, sorted(got), want, "paged keys")
	})

	t.Run("metadata round trip", func(t *testing.T) {
		s := newStorage(t)
		md := &common.Metadata{ContentType: "application/json", Custom: map[string]string{"owner": "alice"}}
		noErr(t, s.PutWithMetadata(ctx, "m", strings.NewReader(`{"a":1}`), md), "PutWithMetadata")
		got, err := s.GetMetadata(ctx, "m")
		noErr(t, err, "GetMetadata")
		equal(t, got.ContentType, "application/json", "ContentType")
		equal(t, got.Custom, map[string]string{"owner": "alice"}, "Custom")
		equal(t, got.Size, int64(len(`{"a":1}`)), "Size")
	})

	t.Run("update metadata replaces custom", func(t *testing.T) {
		s := newStorage(t)
		md := &common.Metadata{ContentType: "text/plain", Custom: map[string]string{"old": "1"}}
		noErr(t, s.PutWithMetadata(ctx, "u", strings.NewReader("body"), md), "PutWithMetadata")
		upd := &common.Metadata{ContentType: "text/plain", Custom: map[string]string{"new": "2"}}
		noErr(t, s.UpdateMetadata(ctx, "u", upd), "UpdateMetadata")
		got, err := s.GetMetadata(ctx, "u")
		noErr(t, err, "GetMetadata")
		equal(t, got.Custom, map[string]string{"new": "2"}, "Custom after update")
		r, err := s.Get("u")
		equal(t, read(t, r, err), "body", "content after update")
	})

	t.Run("cancelled context fails", func(t *testing.T) {
		s := newStorage(t)
		put(t, s, "c")
		cctx, cancel := context.WithCancel(ctx)
		cancel()
		isErr(t, s.PutWithContext(cctx, "c2", strings.NewReader("x")), context.Canceled, "PutWithContext")
		_, err := s.GetWithContext(cctx, "c")
		isErr(t, err, context.Canceled, "GetWithContext")
		_, err = s.ListWithContext(cctx, "")
		isErr(t, err, context.Canceled, "ListWithContext")
	})

	t.Run("invalid keys are refused", func(t *testing.T) {
		s := newStorage(t)
		for _, key := range []string{"../x", ""} {
			if err := s.Put(key, strings.NewReader("x")); err == nil {
				t.Errorf("Put(%q) succeeded", key)
			}
			if _, err := s.Get(key); err == nil {
				t.Errorf("Get(%q) succeeded", key)
			}
		}
	})
}
