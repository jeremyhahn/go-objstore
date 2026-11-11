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

func TestAzure_PutWithMetadata_SetMetadataError(t *testing.T) {
	errSet := errors.New("set metadata failed")
	c := memContainer{blobs: map[string]*memBlob{"key": {setMetaErr: errSet}}}
	a := &Azure{container: c}
	err := a.PutWithMetadata(context.Background(), "key", bytes.NewBufferString("d"),
		&common.Metadata{Custom: map[string]string{"k": "v"}})
	if !errors.Is(err, errSet) {
		t.Fatalf("got %v, want the SetMetadata error", err)
	}
}
