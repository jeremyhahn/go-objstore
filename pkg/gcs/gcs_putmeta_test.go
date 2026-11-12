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

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// TestGCS_PutWithMetadata_PersistsMetadata: the custom metadata, content
// type and content encoding given to PutWithMetadata are stored, so a
// replication fingerprint survives and an unchanged object is not uploaded
// again on each sync (#222).
func TestGCS_PutWithMetadata_PersistsMetadata(t *testing.T) {
	objs := map[string]*fakeObj{}
	g := &GCS{client: fakeClient{b: fakeBucket{objs: objs}}, bucket: "b"}
	ctx := context.Background()

	err := g.PutWithMetadata(ctx, "key", bytes.NewReader([]byte("data")), &common.Metadata{
		ContentType:     "text/plain",
		ContentEncoding: "gzip",
		Custom:          map[string]string{"replication_source_etag": "e", "owner": "alice"},
	})
	if err != nil {
		t.Fatal(err)
	}
	o := objs["key"]
	if o.updated != nil {
		t.Fatal("a separate attribute update was made")
	}
	a := o.attrs
	if a == nil || string(o.data) != "data" {
		t.Fatalf("not uploaded with its attributes: %v %q", a, o.data)
	}
	if a.Metadata["replication_source_etag"] != "e" || a.Metadata["owner"] != "alice" {
		t.Errorf("custom metadata not stored: %v", a.Metadata)
	}
	if a.ContentType != "text/plain" || a.ContentEncoding != "gzip" {
		t.Errorf("attributes not stored: %v %v", a.ContentType, a.ContentEncoding)
	}

	// Nil metadata stores the data alone.
	if err := g.PutWithMetadata(ctx, "plain", bytes.NewReader([]byte("d")), nil); err != nil {
		t.Fatal(err)
	}
	if objs["plain"].updated != nil {
		t.Error("nil metadata: attributes updated")
	}
}

// The upload result is the object the writer created, read from its
// attributes after Close, so each rewrite reports its own generation.
func TestGCS_PutWithMetadataResult_ReportsUploadedObject(t *testing.T) {
	var _ common.IdentityPutter = (*GCS)(nil)
	ctx := context.Background()
	g := &GCS{client: newSvcStore("b").client(), bucket: "b"}

	first, err := g.PutWithMetadataResult(ctx, "k", bytes.NewReader([]byte("data")), &common.Metadata{
		Custom: map[string]string{"owner": "alice"}})
	if err != nil {
		t.Fatal(err)
	}
	if first == nil || first.ETag == "" || first.Size != 4 || first.LastModified.IsZero() {
		t.Fatalf("result = %+v, want the uploaded object's generation, size and time", first)
	}
	md, err := g.GetMetadata(ctx, "k")
	if err != nil {
		t.Fatal(err)
	}
	if md.ETag != first.ETag {
		t.Fatalf("result ETag %q, stored object %q", first.ETag, md.ETag)
	}

	second, err := g.PutWithMetadataResult(ctx, "k", bytes.NewReader([]byte("DATA")), nil)
	if err != nil {
		t.Fatal(err)
	}
	if second == nil || second.ETag == first.ETag {
		t.Fatalf("same-size rewrite: result = %+v, first = %+v", second, first)
	}

	// A writer that reports no attributes gives no identity; a failed
	// upload or an invalid key gives no result.
	objs := map[string]*fakeObj{}
	plain := &GCS{client: fakeClient{b: fakeBucket{objs: objs}}, bucket: "b"}
	if got, err := plain.PutWithMetadataResult(ctx, "k", bytes.NewReader([]byte("d")), nil); err != nil || got != nil {
		t.Fatalf("no attributes: result = %+v, err = %v", got, err)
	}
	objs["bad"] = &fakeObj{closeErr: true}
	if got, err := plain.PutWithMetadataResult(ctx, "bad", bytes.NewReader([]byte("d")), nil); err == nil || got != nil {
		t.Fatalf("failed close: result = %+v, err = %v", got, err)
	}
	if _, err := plain.PutWithMetadataResult(ctx, "", bytes.NewReader(nil), nil); err == nil {
		t.Fatal("expected an invalid key error")
	}
}

func TestGCS_PutWithMetadata_FailedUploadLeavesNoFingerprint(t *testing.T) {
	objs := map[string]*fakeObj{"key": {closeErr: true}}
	g := &GCS{client: fakeClient{b: fakeBucket{objs: objs}}, bucket: "b"}
	err := g.PutWithMetadata(context.Background(), "key", bytes.NewReader([]byte("d")),
		&common.Metadata{ContentType: "bad\n", Custom: map[string]string{"replication_source_etag": "e"}})
	if err == nil {
		t.Fatal("a rejected upload was not reported")
	}
	if o := objs["key"]; o.attrs != nil || o.updated != nil {
		t.Fatalf("attributes stored after a rejected upload: %v %v", o.attrs, o.updated)
	}
}
