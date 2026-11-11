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
	u := objs["key"].updated
	if u == nil {
		t.Fatal("metadata not stored")
	}
	if u.Metadata["replication_source_etag"] != "e" || u.Metadata["owner"] != "alice" {
		t.Errorf("custom metadata not stored: %v", u.Metadata)
	}
	if u.ContentType != "text/plain" || u.ContentEncoding != "gzip" {
		t.Errorf("attributes not stored: %v %v", u.ContentType, u.ContentEncoding)
	}

	// Nil metadata stores the data alone.
	if err := g.PutWithMetadata(ctx, "plain", bytes.NewReader([]byte("d")), nil); err != nil {
		t.Fatal(err)
	}
	if objs["plain"].updated != nil {
		t.Error("nil metadata: attributes updated")
	}
}

func TestGCS_PutWithMetadata_UpdateError(t *testing.T) {
	objs := map[string]*fakeObj{"key": {updateErr: true}}
	g := &GCS{client: fakeClient{b: fakeBucket{objs: objs}}, bucket: "b"}
	err := g.PutWithMetadata(context.Background(), "key", bytes.NewReader([]byte("d")),
		&common.Metadata{Custom: map[string]string{"k": "v"}})
	if err == nil {
		t.Fatal("an attribute update error was not returned")
	}
}
