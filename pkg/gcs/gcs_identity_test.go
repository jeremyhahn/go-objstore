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
	"testing"

	"cloud.google.com/go/storage"
)

// GetMetadata names the object's content: a rewrite of the same length is
// a new ETag, so replication sees the source change.
func TestGCS_GetMetadata_ReportsGenerationAndUpdated(t *testing.T) {
	st := newSvcStore("b")
	g := &GCS{client: st.client(), bucket: "b"}
	ctx := context.Background()

	if err := g.PutWithMetadata(ctx, "k", bytes.NewReader([]byte("aaaa")), nil); err != nil {
		t.Fatal(err)
	}
	first, err := g.GetMetadata(ctx, "k")
	if err != nil {
		t.Fatal(err)
	}
	if first.ETag == "" || first.LastModified.IsZero() {
		t.Fatalf("no identity: etag %q modified %v", first.ETag, first.LastModified)
	}

	if err := g.PutWithMetadata(ctx, "k", bytes.NewReader([]byte("bbbb")), nil); err != nil {
		t.Fatal(err)
	}
	second, err := g.GetMetadata(ctx, "k")
	if err != nil {
		t.Fatal(err)
	}
	if second.Size != first.Size {
		t.Fatalf("sizes differ: %d %d", first.Size, second.Size)
	}
	if second.ETag == first.ETag {
		t.Errorf("a same-length rewrite kept ETag %q", first.ETag)
	}
}

func TestObjectETag_Fallbacks(t *testing.T) {
	cases := []struct {
		attrs storage.ObjectAttrs
		want  string
	}{
		{storage.ObjectAttrs{Generation: 42, Etag: "e", MD5: []byte{1}}, "42"},
		{storage.ObjectAttrs{Etag: "e", MD5: []byte{1}}, "e"},
		{storage.ObjectAttrs{MD5: []byte{0xab}}, "ab"},
		{storage.ObjectAttrs{}, ""},
	}
	for _, c := range cases {
		if got := objectETag(&c.attrs); got != c.want {
			t.Errorf("objectETag(%+v) = %q, want %q", c.attrs, got, c.want)
		}
	}
}
