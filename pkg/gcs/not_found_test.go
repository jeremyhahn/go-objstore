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
	"context"
	"errors"
	"io"
	"testing"

	"cloud.google.com/go/storage"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// missingObj answers every read the way the SDK does for a missing object.
type missingObj struct{ fakeObj }

func (missingObj) NewReader(context.Context) (io.ReadCloser, error) {
	return nil, storage.ErrObjectNotExist
}

func (missingObj) Attrs(context.Context) (*storage.ObjectAttrs, error) {
	return nil, storage.ErrObjectNotExist
}

type missingBucket struct{ fakeBucket }

func (missingBucket) Object(string) gcsObject { return &missingObj{} }

type missingClient struct{}

func (missingClient) Bucket(string) gcsBucket {
	return missingBucket{fakeBucket{objs: map[string]*fakeObj{}}}
}

func assertGCSKeyNotFound(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, common.ErrKeyNotFound) {
		t.Fatalf("expected common.ErrKeyNotFound, got %v", err)
	}
	if !errors.Is(err, storage.ErrObjectNotExist) {
		t.Fatalf("expected storage.ErrObjectNotExist in the chain, got %v", err)
	}
}

func TestGCS_Get_MissingKeyIsErrKeyNotFound(t *testing.T) {
	g := &GCS{client: missingClient{}, bucket: "b"}
	_, err := g.Get("missing")
	assertGCSKeyNotFound(t, err)
}

func TestGCS_GetWithContext_MissingKeyIsErrKeyNotFound(t *testing.T) {
	g := &GCS{client: missingClient{}, bucket: "b"}
	_, err := g.GetWithContext(context.Background(), "missing")
	assertGCSKeyNotFound(t, err)
}

func TestGCS_GetMetadata_MissingKeyIsErrKeyNotFound(t *testing.T) {
	g := &GCS{client: missingClient{}, bucket: "b"}
	meta, err := g.GetMetadata(context.Background(), "missing")
	assertGCSKeyNotFound(t, err)
	if meta != nil {
		t.Fatalf("expected nil metadata for a missing key, got %+v", meta)
	}
}
