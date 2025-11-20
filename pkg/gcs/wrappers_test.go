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
	"io"
	"testing"

	"cloud.google.com/go/storage"
)

type nopCloser struct{ io.Writer }

func (n nopCloser) Close() error { return nil }

func TestGCS_Wrappers_Coverage(t *testing.T) {
	// Stub SDK calls to avoid network
	oldW, oldR, oldD := gcsNewWriterFn, gcsNewReaderFn, gcsDeleteFn
	gcsNewWriterFn = func(_ *storage.ObjectHandle, _ context.Context) io.WriteCloser {
		return nopCloser{bytes.NewBuffer(nil)}
	}
	gcsNewReaderFn = func(_ *storage.ObjectHandle, _ context.Context) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewBufferString("ok")), nil
	}
	gcsDeleteFn = func(_ *storage.ObjectHandle, _ context.Context) error { return nil }
	defer func() { gcsNewWriterFn, gcsNewReaderFn, gcsDeleteFn = oldW, oldR, oldD }()

	ow := objectWrapper{&storage.ObjectHandle{}} // zero value; stubs ignore it
	w := ow.NewWriter(context.Background())
	_, _ = w.Write([]byte("x"))
	_ = w.Close()
	rc, err := ow.NewReader(context.Background())
	if err != nil {
		t.Fatalf("stubbed reader err: %v", err)
	}
	rc.Close()
	if err := ow.Delete(context.Background()); err != nil {
		t.Fatalf("stubbed delete err: %v", err)
	}
}

func TestGCS_Wrappers_Additional(t *testing.T) {
	// Test bucketWrapper.Attrs with stub
	oldGetAttrs := gcsGetBucketAttrsFn
	gcsGetBucketAttrsFn = func(_ context.Context, _ *storage.BucketHandle) (*storage.BucketAttrs, error) {
		return &storage.BucketAttrs{Name: "test"}, nil
	}
	defer func() { gcsGetBucketAttrsFn = oldGetAttrs }()

	bh := &storage.BucketHandle{}
	bw := bucketWrapper{bh}

	attrs, err := bw.Attrs(context.Background())
	if err != nil {
		t.Errorf("Attrs failed: %v", err)
	}
	if attrs == nil || attrs.Name != "test" {
		t.Error("Attrs returned unexpected result")
	}

	// Test bucketWrapper.Update with stub
	oldUpdate := gcsUpdateBucketFn
	gcsUpdateBucketFn = func(_ context.Context, _ *storage.BucketHandle, _ storage.BucketAttrsToUpdate) (*storage.BucketAttrs, error) {
		return &storage.BucketAttrs{Name: "updated"}, nil
	}
	defer func() { gcsUpdateBucketFn = oldUpdate }()

	updated, err := bw.Update(context.Background(), storage.BucketAttrsToUpdate{})
	if err != nil {
		t.Errorf("Update failed: %v", err)
	}
	if updated == nil || updated.Name != "updated" {
		t.Error("Update returned unexpected result")
	}
}

func TestGCS_ObjectAttrs(t *testing.T) {
	oldAttrs := gcsAttrsFn
	gcsAttrsFn = func(_ *storage.ObjectHandle, _ context.Context) (*storage.ObjectAttrs, error) {
		return &storage.ObjectAttrs{Name: "test-obj"}, nil
	}
	defer func() { gcsAttrsFn = oldAttrs }()

	ow := objectWrapper{&storage.ObjectHandle{}}
	attrs, err := ow.Attrs(context.Background())
	if err != nil {
		t.Errorf("Attrs failed: %v", err)
	}
	if attrs == nil || attrs.Name != "test-obj" {
		t.Error("Attrs returned unexpected result")
	}
}

func TestGCS_SimpleWrappers(t *testing.T) {
	// These are very simple wrapper tests just to cover the trivial wrapper methods
	// They don't test functionality, just that the wrappers exist and can be called

	// Test clientWrapper.Bucket
	cw := clientWrapper{&storage.Client{}}
	bw := cw.Bucket("test-bucket")
	if bw == nil {
		t.Error("Bucket wrapper returned nil")
	}

	// Test bucketWrapper.Object
	bhWrapper := bucketWrapper{&storage.BucketHandle{}}
	ow := bhWrapper.Object("test-object")
	if ow == nil {
		t.Error("Object wrapper returned nil")
	}
}
