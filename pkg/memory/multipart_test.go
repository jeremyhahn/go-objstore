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

package memory

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/internal/uploadtest"
	"github.com/jeremyhahn/go-objstore/pkg/common"
)

func TestMemory_MultipartConformance(t *testing.T) {
	uploadtest.RunMultipart(t, func(t *testing.T) common.Storage { return New() })
}

func TestMemory_BucketConformance(t *testing.T) {
	uploadtest.RunBuckets(t, func(t *testing.T) common.BucketManager {
		bm, ok := New().(common.BucketManager)
		if !ok {
			t.Fatal("Memory does not implement common.BucketManager")
		}
		return bm
	})
}

func TestMemory_CapabilityDiscovery(t *testing.T) {
	var s common.Storage = New()
	if _, ok := s.(common.MultipartUploader); !ok {
		t.Error("Memory is not a MultipartUploader")
	}
	if _, ok := s.(common.BucketManager); !ok {
		t.Error("Memory is not a BucketManager")
	}
}

// clock is a settable time source.
type clock struct{ t time.Time }

func (c *clock) now() time.Time { return c.t }

func newClocked(t *testing.T, settings map[string]string) (*Memory, *clock) {
	t.Helper()
	m := New().(*Memory)
	if err := m.Configure(settings, noCreds); err != nil {
		t.Fatal(err)
	}
	c := &clock{t: time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)}
	m.now = c.now
	return m, c
}

func TestMemory_Configure_MultipartExpiry(t *testing.T) {
	tests := []struct {
		name     string
		settings map[string]string
		want     time.Duration
		wantErr  bool
	}{
		{"default", nil, 168 * time.Hour, false},
		{"explicit", map[string]string{"multipartExpiry": "2h"}, 2 * time.Hour, false},
		{"invalid", map[string]string{"multipartExpiry": "never"}, 0, true},
		{"non-positive", map[string]string{"multipartExpiry": "0s"}, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := New().(*Memory)
			err := m.Configure(tt.settings, noCreds)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil && m.expiry() != tt.want {
				t.Errorf("expiry = %v, want %v", m.expiry(), tt.want)
			}
			if err != nil && !errors.Is(err, common.ErrInvalidArgument) {
				t.Errorf("error %v does not match ErrInvalidArgument", err)
			}
		})
	}
}

func TestMemory_ExpiredUploadIsTreatedAsAborted(t *testing.T) {
	ctx := context.Background()
	m, c := newClocked(t, map[string]string{"multipartExpiry": "1h"})
	id, err := m.InitiateMultipartUpload(ctx, "k", nil)
	if err != nil {
		t.Fatal(err)
	}
	info, err := m.UploadPart(ctx, "k", id, 1, strings.NewReader("data"), 4)
	if err != nil {
		t.Fatal(err)
	}

	c.t = c.t.Add(time.Hour) // exactly at the limit: still open
	if _, err := m.UploadPart(ctx, "k", id, 2, strings.NewReader("more"), 4); err != nil {
		t.Fatalf("upload at the limit: %v", err)
	}

	c.t = c.t.Add(time.Second) // past the limit
	ups, err := m.ListMultipartUploads(ctx, "")
	if err != nil || len(ups) != 1 {
		t.Fatalf("expired upload must stay listable for cleanup: %v, %v", ups, err)
	}
	if _, err := m.UploadPart(ctx, "k", id, 3, strings.NewReader("x"), 1); !errors.Is(err, common.ErrUploadNotFound) {
		t.Errorf("UploadPart after expiry = %v", err)
	}
	// The failed call dropped it.
	if ups, _ := m.ListMultipartUploads(ctx, ""); len(ups) != 0 {
		t.Errorf("expired upload not dropped: %v", ups)
	}
	err = m.CompleteMultipartUpload(ctx, "k", id, []common.CompletedPart{{PartNumber: 1, ETag: info.ETag}})
	if !errors.Is(err, common.ErrUploadNotFound) {
		t.Errorf("Complete after expiry = %v", err)
	}
	if m.Count() != 0 {
		t.Error("expired upload produced an object")
	}
}

func TestMemory_CompleteExpiredUploadFails(t *testing.T) {
	ctx := context.Background()
	m, c := newClocked(t, map[string]string{"multipartExpiry": "1h"})
	id, _ := m.InitiateMultipartUpload(ctx, "k", nil)
	info, _ := m.UploadPart(ctx, "k", id, 1, strings.NewReader("data"), 4)
	c.t = c.t.Add(2 * time.Hour)
	err := m.CompleteMultipartUpload(ctx, "k", id, []common.CompletedPart{{PartNumber: 1, ETag: info.ETag}})
	if !errors.Is(err, common.ErrUploadNotFound) {
		t.Errorf("Complete after expiry = %v", err)
	}
}

func TestMemory_AbortRemovesExpiredUpload(t *testing.T) {
	ctx := context.Background()
	m, c := newClocked(t, map[string]string{"multipartExpiry": "1h"})
	id, _ := m.InitiateMultipartUpload(ctx, "k", nil)
	c.t = c.t.Add(2 * time.Hour)
	if err := m.AbortMultipartUpload(ctx, "k", id); err != nil {
		t.Fatalf("abort of an expired upload: %v", err)
	}
	if ups, _ := m.ListMultipartUploads(ctx, ""); len(ups) != 0 {
		t.Errorf("still listed: %v", ups)
	}
}

func TestMemory_ZeroValueIsUsable(t *testing.T) {
	ctx := context.Background()
	m := &Memory{}
	id, err := m.InitiateMultipartUpload(ctx, "k", nil)
	if err != nil {
		t.Fatal(err)
	}
	info, err := m.UploadPart(ctx, "k", id, 1, strings.NewReader("abc"), 3)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.CompleteMultipartUpload(ctx, "k", id, []common.CompletedPart{{PartNumber: 1, ETag: info.ETag}}); err != nil {
		t.Fatal(err)
	}
	if m.Count() != 1 {
		t.Errorf("Count = %d", m.Count())
	}
	if err := (&Memory{}).CreateBucket(ctx, "abc"); err != nil {
		t.Errorf("CreateBucket on zero value: %v", err)
	}
}

func TestMemory_UploadDoesNotAliasCallerMetadata(t *testing.T) {
	ctx := context.Background()
	m := New().(*Memory)
	md := &common.Metadata{Custom: map[string]string{"a": "1"}}
	id, _ := m.InitiateMultipartUpload(ctx, "k", md)
	md.Custom["a"] = "changed"
	info, _ := m.UploadPart(ctx, "k", id, 1, strings.NewReader("abc"), 3)
	if err := m.CompleteMultipartUpload(ctx, "k", id, []common.CompletedPart{{PartNumber: 1, ETag: info.ETag}}); err != nil {
		t.Fatal(err)
	}
	got, err := m.GetMetadata(ctx, "k")
	if err != nil {
		t.Fatal(err)
	}
	if got.Custom["a"] != "1" {
		t.Errorf("metadata followed the caller's later change: %v", got.Custom)
	}
}

func TestMemory_BucketsInheritExpiryAndClock(t *testing.T) {
	ctx := context.Background()
	m, c := newClocked(t, map[string]string{"multipartExpiry": "1h"})
	if err := m.CreateBucket(ctx, "bkt"); err != nil {
		t.Fatal(err)
	}
	infos, _ := m.ListBuckets(ctx)
	if len(infos) != 1 || !infos[0].Created.Equal(c.t) {
		t.Fatalf("ListBuckets = %+v", infos)
	}
	s, err := m.Bucket(ctx, "bkt")
	if err != nil {
		t.Fatal(err)
	}
	mu := s.(common.MultipartUploader)
	id, _ := mu.InitiateMultipartUpload(ctx, "k", nil)
	c.t = c.t.Add(2 * time.Hour)
	if _, err := mu.UploadPart(ctx, "k", id, 1, strings.NewReader("a"), 1); !errors.Is(err, common.ErrUploadNotFound) {
		t.Errorf("bucket did not inherit the expiry: %v", err)
	}
}

func TestMemory_BucketsAreNotShared(t *testing.T) {
	ctx := context.Background()
	a, b := New().(*Memory), New().(*Memory)
	if err := a.CreateBucket(ctx, "shared-name"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Bucket(ctx, "shared-name"); !errors.Is(err, common.ErrBucketNotFound) {
		t.Errorf("a bucket leaked between instances: %v", err)
	}
}
