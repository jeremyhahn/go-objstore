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
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// blobServer answers a streamed block blob upload (stage block, commit block
// list) with the given status and ETag, recording the staged bytes and the
// headers of the commit.
func blobServer(t *testing.T, status int, etag string) (*Azure, *[]byte, *http.Header) {
	t.Helper()
	var body []byte
	var hdr http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		if r.URL.Query().Get("comp") == "blocklist" {
			hdr = r.Header.Clone() // the commit carries metadata and headers
		} else {
			body = append(body, b...)
		}
		if etag != "" {
			w.Header().Set("ETag", etag)
		}
		w.Header().Set("Last-Modified", time.Unix(1_700_000_000, 0).UTC().Format(http.TimeFormat))
		w.WriteHeader(status)
	}))
	t.Cleanup(srv.Close)
	u, err := url.Parse(srv.URL + "/container")
	if err != nil {
		t.Fatal(err)
	}
	p := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{
		Retry: azblob.RetryOptions{MaxTries: 1},
	})
	return &Azure{container: containerWrapper{azblob.NewContainerURL(*u, p)}}, &body, &hdr
}

// The upload result is the ETag and time of the upload response and the
// bytes uploaded, from the one request that carries payload and metadata.
func TestAzure_PutWithMetadataResult_ReportsUpload(t *testing.T) {
	var _ common.IdentityPutter = (*Azure)(nil)
	ctx := context.Background()

	a, body, hdr := blobServer(t, http.StatusCreated, `"0x8D1"`)
	got, err := a.PutWithMetadataResult(ctx, "k", bytes.NewBufferString("data"), &common.Metadata{
		ContentType: "text/plain",
		Custom:      map[string]string{"owner": "alice"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.ETag != `"0x8D1"` || got.Size != 4 || got.LastModified.Unix() != 1_700_000_000 {
		t.Fatalf("result = %+v, want the response ETag and time and 4 bytes", got)
	}
	if string(*body) != "data" || hdr.Get("x-ms-meta-owner") != "alice" || hdr.Get("x-ms-blob-content-type") != "text/plain" {
		t.Fatalf("upload carried %q %v", *body, *hdr)
	}

	// Nil metadata uploads the data alone and still reports the upload.
	if got, err := a.PutWithMetadataResult(ctx, "k", bytes.NewBufferString("d"), nil); err != nil || got == nil || got.Size != 1 {
		t.Fatalf("nil metadata: result = %+v, err = %v", got, err)
	}

	// A response without an ETag reports no identity.
	a, _, _ = blobServer(t, http.StatusCreated, "")
	if got, err := a.PutWithMetadataResult(ctx, "k", bytes.NewBufferString("d"), nil); err != nil || got != nil {
		t.Fatalf("no ETag: result = %+v, err = %v", got, err)
	}

	// A rejected upload is an error with no result.
	a, _, _ = blobServer(t, http.StatusForbidden, "")
	if got, err := a.PutWithMetadataResult(ctx, "k", bytes.NewBufferString("d"), nil); err == nil || got != nil {
		t.Fatalf("rejected: result = %+v, err = %v", got, err)
	}

	if _, err := a.PutWithMetadataResult(ctx, "", bytes.NewBufferString("d"), nil); err == nil {
		t.Fatal("expected an invalid key error")
	}
}

// A blob that cannot report its upload's identity still stores the object,
// with no result.
func TestAzure_PutWithMetadataResult_BlobWithoutIdentity(t *testing.T) {
	b := &memBlob{}
	a := &Azure{container: memContainer{blobs: map[string]*memBlob{"key": b}}}
	got, err := a.PutWithMetadataResult(context.Background(), "key", bytes.NewBufferString("data"),
		&common.Metadata{Custom: map[string]string{"owner": "alice"}})
	if err != nil || got != nil {
		t.Fatalf("result = %+v, err = %v", got, err)
	}
	if string(b.data) != "data" || b.meta["owner"] != "alice" {
		t.Fatalf("stored %q %v", b.data, b.meta)
	}
}
