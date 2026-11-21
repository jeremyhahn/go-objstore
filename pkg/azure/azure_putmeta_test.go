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
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// TestAzure_PutWithMetadata_PersistsMetadata: the custom metadata, content
// type and content encoding given to PutWithMetadata are stored, so a
// replication fingerprint survives and an unchanged object is not uploaded
// again on each sync (#222).
func TestAzure_PutWithMetadata_PersistsMetadata(t *testing.T) {
	c := memContainer{blobs: map[string]*memBlob{}}
	a := &Azure{container: c}
	ctx := context.Background()

	err := a.PutWithMetadata(ctx, "key", bytes.NewBufferString("data"), &common.Metadata{
		ContentType:     "text/plain",
		ContentEncoding: "gzip",
		Custom:          map[string]string{"replication_source_etag": "e", "owner": "alice"},
	})
	if err != nil {
		t.Fatal(err)
	}
	md, err := a.GetMetadata(ctx, "key")
	if err != nil {
		t.Fatal(err)
	}
	if md.Custom["replication_source_etag"] != "e" || md.Custom["owner"] != "alice" {
		t.Errorf("custom metadata not stored: %v", md.Custom)
	}
	if md.ContentType != "text/plain" || md.ContentEncoding != "gzip" {
		t.Errorf("headers not stored: %q %q", md.ContentType, md.ContentEncoding)
	}

	// Nil metadata stores the data alone.
	if err := a.PutWithMetadata(ctx, "plain", bytes.NewBufferString("d"), nil); err != nil {
		t.Fatal(err)
	}
	if md, err := a.GetMetadata(ctx, "plain"); err != nil || len(md.Custom) != 0 {
		t.Errorf("nil metadata: %v %v", md, err)
	}
}

// Payload, custom metadata and headers go up in one request: there is no
// window where the blob holds the new bytes without its metadata, and no
// separate metadata request that could land on another writer's blob.
func TestAzure_PutWithMetadata_OneUploadCarriesAll(t *testing.T) {
	b := &memBlob{}
	a := &Azure{container: memContainer{blobs: map[string]*memBlob{"key": b}}}
	err := a.PutWithMetadata(context.Background(), "key", bytes.NewBufferString("data"), &common.Metadata{
		ContentType:     "text/plain",
		ContentEncoding: "gzip",
		Custom:          map[string]string{"replication_source_etag": "e"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(b.calls) != 1 || b.calls[0] != "uploadWithMetadata" {
		t.Fatalf("calls = %v, want one uploadWithMetadata", b.calls)
	}
	if string(b.data) != "data" || b.meta["replication_source_etag"] != "e" ||
		b.headers.ContentType != "text/plain" || b.headers.ContentEncoding != "gzip" {
		t.Fatalf("stored %q %v %+v", b.data, b.meta, b.headers)
	}
}

// A rejected upload (bad headers included) stores nothing, so no
// fingerprint names bytes that are not there.
func TestAzure_PutWithMetadata_FailedUploadLeavesNoFingerprint(t *testing.T) {
	errUp := errors.New("invalid header value")
	a := &Azure{container: memContainer{blobs: map[string]*memBlob{"key": {upErr: errUp}}}}
	err := a.PutWithMetadata(context.Background(), "key", bytes.NewBufferString("d"),
		&common.Metadata{ContentType: "bad\n", Custom: map[string]string{"replication_source_etag": "e"}})
	if !errors.Is(err, errUp) {
		t.Fatalf("got %v, want the upload error", err)
	}
	if md, err := a.GetMetadata(context.Background(), "key"); err == nil && md.Custom["replication_source_etag"] != "" {
		t.Fatalf("fingerprint stored after a failed upload: %v", md.Custom)
	}
}
