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
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
)

// TestObjectWrapper_NewReader_KeepsGzipEncoding: an object stored with
// Content-Encoding gzip is read as its stored bytes, not decompressed, so a
// replicated copy that keeps the gzip encoding holds gzip data (#222).
func TestObjectWrapper_NewReader_KeepsGzipEncoding(t *testing.T) {
	plain := []byte("replicated payload, replicated payload, replicated payload")
	var gz bytes.Buffer
	zw := gzip.NewWriter(&gz)
	if _, err := zw.Write(plain); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	stored := gz.Bytes()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "unexpected "+r.Method, http.StatusMethodNotAllowed)
			return
		}
		// GCS serves the stored gzip bytes when the client accepts gzip.
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Set("X-Goog-Stored-Content-Encoding", "gzip")
		w.Header().Set("X-Goog-Generation", "1")
		w.Header().Set("X-Goog-Metageneration", "1")
		_, _ = w.Write(stored)
	}))
	defer srv.Close()

	ctx := context.Background()
	client, err := storage.NewClient(ctx,
		option.WithEndpoint(srv.URL+"/storage/v1/"),
		option.WithoutAuthentication(),
		option.WithHTTPClient(srv.Client()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	r, err := objectWrapper{client.Bucket("b").Object("k")}.NewReader(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(r)
	_ = r.Close()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, stored) {
		t.Fatalf("read %d bytes %q, want the %d stored gzip bytes", len(got), got, len(stored))
	}
	zr, err := gzip.NewReader(bytes.NewReader(got))
	if err != nil {
		t.Fatal(err)
	}
	back, err := io.ReadAll(zr)
	if err != nil || !bytes.Equal(back, plain) {
		t.Fatalf("round trip = %q, %v; want %q", back, err, plain)
	}
}
