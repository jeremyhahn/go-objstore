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

// Package uploadtest is the conformance suite for the optional BucketManager
// and MultipartUploader capabilities. Every provider that implements either
// runs it, so a caller sees the same behavior on all of them.
package uploadtest

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"testing/iotest"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// StoreFactory returns a fresh, empty Storage that implements
// common.MultipartUploader.
type StoreFactory func(t *testing.T) common.Storage

// ManagerFactory returns a fresh BucketManager with no buckets.
type ManagerFactory func(t *testing.T) common.BucketManager

// payload returns n bytes whose content depends on seed.
var (
	// errUnread marks a reader that the code under test must not read.
	errUnread = errors.New("must not be read")
	// errReader is the failure a part's reader reports.
	errReader = errors.New("boom")
)

func payload(seed byte, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = seed + byte(i%251)
	}
	return b
}

func uploader(t *testing.T, newStore StoreFactory) (common.Storage, common.MultipartUploader) {
	t.Helper()
	s := newStore(t)
	mu, ok := s.(common.MultipartUploader)
	if !ok {
		t.Fatalf("%T does not implement common.MultipartUploader", s)
	}
	return s, mu
}

func upload(t *testing.T, mu common.MultipartUploader, key, id string, n int, data []byte) common.PartInfo {
	t.Helper()
	info, err := mu.UploadPart(context.Background(), key, id, n, bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("UploadPart %d: %v", n, err)
	}
	return info
}

func readAll(t *testing.T, s common.Storage, key string) []byte {
	t.Helper()
	r, err := s.Get(key)
	if err != nil {
		t.Fatalf("Get %q: %v", key, err)
	}
	defer func() { _ = r.Close() }()
	b, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// RunMultipart runs the MultipartUploader conformance tests.
func RunMultipart(t *testing.T, newStore StoreFactory) {
	ctx := context.Background()
	big := payload(1, common.MinPartSize)
	tail := payload(7, 1234)

	t.Run("discovery by type assertion", func(t *testing.T) {
		_, _ = uploader(t, newStore)
	})

	t.Run("complete assembles parts in order and applies metadata", func(t *testing.T) {
		s, mu := uploader(t, newStore)
		md := &common.Metadata{ContentType: "application/x-test", Custom: map[string]string{"owner": "alice"}}
		id, err := mu.InitiateMultipartUpload(ctx, "dir/object.bin", md)
		if err != nil || id == "" {
			t.Fatalf("Initiate = %q, %v", id, err)
		}
		// Upload out of order: the part number decides the position.
		p2 := upload(t, mu, "dir/object.bin", id, 2, tail)
		p1 := upload(t, mu, "dir/object.bin", id, 1, big)
		if p1.PartNumber != 1 || p1.Size != int64(len(big)) || p1.ETag == "" {
			t.Errorf("part info = %+v", p1)
		}
		if exists, _ := s.Exists(ctx, "dir/object.bin"); exists {
			t.Error("object visible before Complete")
		}
		parts := []common.CompletedPart{{PartNumber: 1, ETag: p1.ETag}, {PartNumber: 2, ETag: p2.ETag}}
		if err := mu.CompleteMultipartUpload(ctx, "dir/object.bin", id, parts); err != nil {
			t.Fatalf("Complete: %v", err)
		}
		if got, want := readAll(t, s, "dir/object.bin"), append(append([]byte{}, big...), tail...); !bytes.Equal(got, want) {
			t.Errorf("object content differs: got %d bytes, want %d", len(got), len(want))
		}
		got, err := s.GetMetadata(ctx, "dir/object.bin")
		if err != nil {
			t.Fatal(err)
		}
		if got.ContentType != "application/x-test" || got.Custom["owner"] != "alice" || got.Size != int64(len(big)+len(tail)) {
			t.Errorf("metadata = %+v", got)
		}
		if md.Size != 0 {
			t.Error("caller's metadata was modified")
		}
		ups, err := mu.ListMultipartUploads(ctx, "")
		if err != nil || len(ups) != 0 {
			t.Errorf("uploads after complete = %v, %v", ups, err)
		}
		if err := mu.CompleteMultipartUpload(ctx, "dir/object.bin", id, parts); !errors.Is(err, common.ErrUploadNotFound) {
			t.Errorf("second Complete = %v, want ErrUploadNotFound", err)
		}
	})

	t.Run("single small part and nil metadata", func(t *testing.T) {
		s, mu := uploader(t, newStore)
		id, err := mu.InitiateMultipartUpload(ctx, "small", nil)
		if err != nil {
			t.Fatal(err)
		}
		p := upload(t, mu, "small", id, 1, tail)
		if err := mu.CompleteMultipartUpload(ctx, "small", id, []common.CompletedPart{{PartNumber: 1, ETag: p.ETag}}); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(readAll(t, s, "small"), tail) {
			t.Error("content differs")
		}
	})

	t.Run("re-uploading a part number replaces the part", func(t *testing.T) {
		s, mu := uploader(t, newStore)
		id, _ := mu.InitiateMultipartUpload(ctx, "k", nil)
		old := upload(t, mu, "k", id, 1, []byte("old"))
		fresh := upload(t, mu, "k", id, 1, []byte("fresh"))
		if old.ETag == fresh.ETag {
			t.Fatal("different content produced the same ETag")
		}
		err := mu.CompleteMultipartUpload(ctx, "k", id, []common.CompletedPart{{PartNumber: 1, ETag: old.ETag}})
		if !errors.Is(err, common.ErrPartMismatch) {
			t.Fatalf("stale ETag = %v, want ErrPartMismatch", err)
		}
		if err := mu.CompleteMultipartUpload(ctx, "k", id, []common.CompletedPart{{PartNumber: 1, ETag: fresh.ETag}}); err != nil {
			t.Fatal(err)
		}
		if string(readAll(t, s, "k")) != "fresh" {
			t.Error("replacement part was not used")
		}
	})

	t.Run("staged state is never an object", func(t *testing.T) {
		s, mu := uploader(t, newStore)
		id, _ := mu.InitiateMultipartUpload(ctx, "staged", nil)
		upload(t, mu, "staged", id, 1, tail)
		keys, err := s.List("")
		if err != nil || len(keys) != 0 {
			t.Errorf("List = %v, %v", keys, err)
		}
		res, err := s.ListWithOptions(ctx, &common.ListOptions{})
		if err != nil || len(res.Objects) != 0 {
			t.Errorf("ListWithOptions = %+v, %v", res, err)
		}
		if _, err := s.Get(common.MultipartPrefix + id + "/1"); err == nil {
			t.Error("a staged part is readable as an object")
		}
	})

	t.Run("initiate validation", func(t *testing.T) {
		_, mu := uploader(t, newStore)
		tests := []struct {
			name string
			key  string
			md   *common.Metadata
		}{
			{"empty key", "", nil},
			{"traversal", "../escape", nil},
			{"reserved prefix", common.MultipartPrefix + "x", nil},
			{"bad metadata", "k", &common.Metadata{Custom: map[string]string{"a": "b\nc"}}},
		}
		for _, tt := range tests {
			if _, err := mu.InitiateMultipartUpload(ctx, tt.key, tt.md); !errors.Is(err, common.ErrInvalidArgument) {
				t.Errorf("%s: err = %v, want ErrInvalidArgument", tt.name, err)
			}
		}
	})

	t.Run("upload part validation", func(t *testing.T) {
		_, mu := uploader(t, newStore)
		id, _ := mu.InitiateMultipartUpload(ctx, "k", nil)
		other, _ := mu.InitiateMultipartUpload(ctx, "other", nil)
		unknown := strings.Repeat("0", 32)
		tests := []struct {
			name string
			key  string
			id   string
			num  int
			data io.Reader
			size int64
			want error
		}{
			{"part number zero", "k", id, 0, strings.NewReader("a"), 1, common.ErrInvalidPartNumber},
			{"part number negative", "k", id, -3, strings.NewReader("a"), 1, common.ErrInvalidPartNumber},
			{"part number too high", "k", id, common.MaxParts + 1, strings.NewReader("a"), 1, common.ErrInvalidPartNumber},
			{"short body", "k", id, 1, strings.NewReader("abc"), 5, common.ErrInvalidArgument},
			{"long body", "k", id, 1, strings.NewReader("abcdef"), 5, common.ErrInvalidArgument},
			{"negative size", "k", id, 1, strings.NewReader("a"), -1, common.ErrInvalidArgument},
			{"part too large", "k", id, 1, iotest.ErrReader(errUnread), common.MaxPartSize + 1, common.ErrPartTooLarge},
			{"unknown upload", "k", unknown, 1, strings.NewReader("a"), 1, common.ErrUploadNotFound},
			{"malformed upload id", "k", "../../etc", 1, strings.NewReader("a"), 1, common.ErrUploadNotFound},
			{"upload of another key", "k", other, 1, strings.NewReader("a"), 1, common.ErrUploadNotFound},
			{"invalid key", "../k", id, 1, strings.NewReader("a"), 1, common.ErrInvalidArgument},
		}
		for _, tt := range tests {
			_, err := mu.UploadPart(ctx, tt.key, tt.id, tt.num, tt.data, tt.size)
			if !errors.Is(err, tt.want) {
				t.Errorf("%s: err = %v, want %v", tt.name, err, tt.want)
			}
		}
		boom := errReader
		if _, err := mu.UploadPart(ctx, "k", id, 1, iotest.ErrReader(boom), 5); !errors.Is(err, boom) {
			t.Errorf("reader failure = %v, want the reader's error", err)
		}
	})

	t.Run("complete validation is all-or-nothing", func(t *testing.T) {
		s, mu := uploader(t, newStore)
		id, _ := mu.InitiateMultipartUpload(ctx, "k", nil)
		p1 := upload(t, mu, "k", id, 1, tail) // below the minimum
		p2 := upload(t, mu, "k", id, 2, big)
		p3 := upload(t, mu, "k", id, 3, tail)
		cp := func(p common.PartInfo) common.CompletedPart {
			return common.CompletedPart{PartNumber: p.PartNumber, ETag: p.ETag}
		}
		tests := []struct {
			name  string
			parts []common.CompletedPart
			want  error
		}{
			{"no parts", nil, common.ErrInvalidPartOrder},
			{"descending", []common.CompletedPart{cp(p3), cp(p2)}, common.ErrInvalidPartOrder},
			{"duplicate", []common.CompletedPart{cp(p2), cp(p2)}, common.ErrInvalidPartOrder},
			{"part number out of range", []common.CompletedPart{{PartNumber: 0, ETag: "x"}}, common.ErrInvalidPartNumber},
			{"never uploaded", []common.CompletedPart{cp(p2), {PartNumber: 9, ETag: "x"}}, common.ErrPartMismatch},
			{"wrong etag", []common.CompletedPart{{PartNumber: 2, ETag: "wrong"}}, common.ErrPartMismatch},
			{"small non-final part", []common.CompletedPart{cp(p1), cp(p2)}, common.ErrPartTooSmall},
		}
		for _, tt := range tests {
			if err := mu.CompleteMultipartUpload(ctx, "k", id, tt.parts); !errors.Is(err, tt.want) {
				t.Errorf("%s: err = %v, want %v", tt.name, err, tt.want)
			}
		}
		if exists, _ := s.Exists(ctx, "k"); exists {
			t.Error("a failed Complete left an object behind")
		}
		// The upload survives failed attempts and can still finish.
		if err := mu.CompleteMultipartUpload(ctx, "k", id, []common.CompletedPart{cp(p2), cp(p3)}); err != nil {
			t.Fatalf("Complete after failures: %v", err)
		}
		want := append(append([]byte{}, big...), tail...)
		if !bytes.Equal(readAll(t, s, "k"), want) {
			t.Error("content differs")
		}
	})

	t.Run("complete overwrites an existing object", func(t *testing.T) {
		s, mu := uploader(t, newStore)
		if err := s.Put("k", strings.NewReader("before")); err != nil {
			t.Fatal(err)
		}
		id, _ := mu.InitiateMultipartUpload(ctx, "k", nil)
		p := upload(t, mu, "k", id, 1, []byte("after"))
		if err := mu.CompleteMultipartUpload(ctx, "k", id, []common.CompletedPart{{PartNumber: 1, ETag: p.ETag}}); err != nil {
			t.Fatal(err)
		}
		if string(readAll(t, s, "k")) != "after" {
			t.Error("object not replaced")
		}
	})

	t.Run("complete with wrong key or unknown upload", func(t *testing.T) {
		_, mu := uploader(t, newStore)
		id, _ := mu.InitiateMultipartUpload(ctx, "k", nil)
		p := upload(t, mu, "k", id, 1, tail)
		parts := []common.CompletedPart{{PartNumber: 1, ETag: p.ETag}}
		if err := mu.CompleteMultipartUpload(ctx, "elsewhere", id, parts); !errors.Is(err, common.ErrUploadNotFound) {
			t.Errorf("wrong key = %v", err)
		}
		if err := mu.CompleteMultipartUpload(ctx, "k", strings.Repeat("1", 32), parts); !errors.Is(err, common.ErrUploadNotFound) {
			t.Errorf("unknown id = %v", err)
		}
		if err := mu.CompleteMultipartUpload(ctx, "../k", id, parts); !errors.Is(err, common.ErrInvalidArgument) {
			t.Errorf("invalid key = %v", err)
		}
	})

	t.Run("abort discards the upload", func(t *testing.T) {
		s, mu := uploader(t, newStore)
		id, _ := mu.InitiateMultipartUpload(ctx, "k", nil)
		p := upload(t, mu, "k", id, 1, tail)
		if err := mu.AbortMultipartUpload(ctx, "k", id); err != nil {
			t.Fatal(err)
		}
		if err := mu.AbortMultipartUpload(ctx, "k", id); !errors.Is(err, common.ErrUploadNotFound) {
			t.Errorf("second Abort = %v", err)
		}
		if _, err := mu.UploadPart(ctx, "k", id, 2, strings.NewReader("a"), 1); !errors.Is(err, common.ErrUploadNotFound) {
			t.Errorf("UploadPart after Abort = %v", err)
		}
		err := mu.CompleteMultipartUpload(ctx, "k", id, []common.CompletedPart{{PartNumber: 1, ETag: p.ETag}})
		if !errors.Is(err, common.ErrUploadNotFound) {
			t.Errorf("Complete after Abort = %v", err)
		}
		if exists, _ := s.Exists(ctx, "k"); exists {
			t.Error("aborted upload produced an object")
		}
		if err := mu.AbortMultipartUpload(ctx, "k", "not-an-id"); !errors.Is(err, common.ErrUploadNotFound) {
			t.Errorf("malformed id = %v", err)
		}
		if err := mu.AbortMultipartUpload(ctx, "../k", id); !errors.Is(err, common.ErrInvalidArgument) {
			t.Errorf("invalid key = %v", err)
		}
	})

	t.Run("list uploads filters by prefix and sorts", func(t *testing.T) {
		_, mu := uploader(t, newStore)
		ids := map[string]string{}
		for _, k := range []string{"b/two", "a/one", "b/one"} {
			id, err := mu.InitiateMultipartUpload(ctx, k, nil)
			if err != nil {
				t.Fatal(err)
			}
			ids[k] = id
		}
		all, err := mu.ListMultipartUploads(ctx, "")
		if err != nil || len(all) != 3 {
			t.Fatalf("all = %v, %v", all, err)
		}
		for i, want := range []string{"a/one", "b/one", "b/two"} {
			if all[i].Key != want || all[i].UploadID != ids[want] || all[i].Initiated.IsZero() {
				t.Errorf("entry %d = %+v", i, all[i])
			}
		}
		sub, err := mu.ListMultipartUploads(ctx, "b/")
		if err != nil || len(sub) != 2 {
			t.Errorf("prefix b/ = %v, %v", sub, err)
		}
		if _, err := mu.ListMultipartUploads(ctx, "../x"); !errors.Is(err, common.ErrInvalidArgument) {
			t.Errorf("invalid prefix = %v", err)
		}
		// Two uploads of the same key are independent.
		a, _ := mu.InitiateMultipartUpload(ctx, "dup", nil)
		b, _ := mu.InitiateMultipartUpload(ctx, "dup", nil)
		if a == b {
			t.Error("upload IDs collide")
		}
	})

	t.Run("canceled context", func(t *testing.T) {
		_, mu := uploader(t, newStore)
		id, _ := mu.InitiateMultipartUpload(ctx, "k", nil)
		cctx, cancel := context.WithCancel(ctx)
		cancel()
		if _, err := mu.InitiateMultipartUpload(cctx, "k2", nil); !errors.Is(err, context.Canceled) {
			t.Errorf("Initiate = %v", err)
		}
		if _, err := mu.UploadPart(cctx, "k", id, 1, strings.NewReader("a"), 1); !errors.Is(err, context.Canceled) {
			t.Errorf("UploadPart = %v", err)
		}
		if err := mu.CompleteMultipartUpload(cctx, "k", id, nil); !errors.Is(err, context.Canceled) {
			t.Errorf("Complete = %v", err)
		}
		if err := mu.AbortMultipartUpload(cctx, "k", id); !errors.Is(err, context.Canceled) {
			t.Errorf("Abort = %v", err)
		}
		if _, err := mu.ListMultipartUploads(cctx, ""); !errors.Is(err, context.Canceled) {
			t.Errorf("List = %v", err)
		}
	})

	t.Run("concurrent part uploads", func(t *testing.T) {
		s, mu := uploader(t, newStore)
		id, _ := mu.InitiateMultipartUpload(ctx, "k", nil)
		const n = 8
		infos := make([]common.PartInfo, n)
		var wg sync.WaitGroup
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				data := []byte(strings.Repeat(string("abcdefghij"[i%10]), 10))
				info, err := mu.UploadPart(ctx, "k", id, i+1, bytes.NewReader(data), int64(len(data)))
				if err != nil {
					t.Errorf("part %d: %v", i+1, err)
				}
				infos[i] = info
			}(i)
		}
		wg.Wait()
		// Only the last part may be smaller than the minimum.
		if err := mu.CompleteMultipartUpload(ctx, "k", id, []common.CompletedPart{{PartNumber: 1, ETag: infos[0].ETag}, {PartNumber: 2, ETag: infos[1].ETag}}); !errors.Is(err, common.ErrPartTooSmall) {
			t.Fatalf("Complete = %v, want ErrPartTooSmall", err)
		}
		if err := mu.CompleteMultipartUpload(ctx, "k", id, []common.CompletedPart{{PartNumber: n, ETag: infos[n-1].ETag}}); err != nil {
			t.Fatal(err)
		}
		if got := string(readAll(t, s, "k")); got != strings.Repeat("h", 10) {
			t.Errorf("content = %q", got)
		}
	})
}

// RunBuckets runs the BucketManager conformance tests.
func RunBuckets(t *testing.T, newManager ManagerFactory) {
	ctx := context.Background()

	t.Run("create and list", func(t *testing.T) {
		bm := newManager(t)
		if got, err := bm.ListBuckets(ctx); err != nil || len(got) != 0 {
			t.Fatalf("empty ListBuckets = %v, %v", got, err)
		}
		for _, n := range []string{"zeta", "alpha", "mid-1"} {
			if err := bm.CreateBucket(ctx, n); err != nil {
				t.Fatalf("CreateBucket %q: %v", n, err)
			}
		}
		got, err := bm.ListBuckets(ctx)
		if err != nil || len(got) != 3 {
			t.Fatalf("ListBuckets = %v, %v", got, err)
		}
		for i, want := range []string{"alpha", "mid-1", "zeta"} {
			if got[i].Name != want {
				t.Errorf("bucket %d = %q, want %q", i, got[i].Name, want)
			}
		}
	})

	t.Run("create rejects duplicates and bad names", func(t *testing.T) {
		bm := newManager(t)
		if err := bm.CreateBucket(ctx, "photos"); err != nil {
			t.Fatal(err)
		}
		err := bm.CreateBucket(ctx, "photos")
		if !errors.Is(err, common.ErrBucketAlreadyExists) || !errors.Is(err, common.ErrAlreadyExists) {
			t.Errorf("duplicate = %v", err)
		}
		for _, bad := range []string{"", "ab", "Upper", "has_underscore", "../up", "a/b", "-lead", "trail-", strings.Repeat("a", 64)} {
			if err := bm.CreateBucket(ctx, bad); !errors.Is(err, common.ErrInvalidBucketName) {
				t.Errorf("CreateBucket(%q) = %v, want ErrInvalidBucketName", bad, err)
			}
			if err := bm.DeleteBucket(ctx, bad); !errors.Is(err, common.ErrInvalidBucketName) {
				t.Errorf("DeleteBucket(%q) = %v, want ErrInvalidBucketName", bad, err)
			}
			if _, err := bm.Bucket(ctx, bad); !errors.Is(err, common.ErrInvalidBucketName) {
				t.Errorf("Bucket(%q) = %v, want ErrInvalidBucketName", bad, err)
			}
		}
	})

	t.Run("bucket storage is isolated and usable", func(t *testing.T) {
		bm := newManager(t)
		for _, n := range []string{"one", "two"} {
			if err := bm.CreateBucket(ctx, n); err != nil {
				t.Fatal(err)
			}
		}
		one, err := bm.Bucket(ctx, "one")
		if err != nil {
			t.Fatal(err)
		}
		two, err := bm.Bucket(ctx, "two")
		if err != nil {
			t.Fatal(err)
		}
		if err := one.Put("k", strings.NewReader("in one")); err != nil {
			t.Fatal(err)
		}
		if string(readAll(t, one, "k")) != "in one" {
			t.Error("bucket one lost its object")
		}
		if exists, _ := two.Exists(ctx, "k"); exists {
			t.Error("object leaked into another bucket")
		}
		again, err := bm.Bucket(ctx, "one")
		if err != nil {
			t.Fatal(err)
		}
		if string(readAll(t, again, "k")) != "in one" {
			t.Error("a second handle does not see the object")
		}
		if _, err := bm.Bucket(ctx, "missing"); !errors.Is(err, common.ErrBucketNotFound) {
			t.Errorf("missing bucket = %v", err)
		}
	})

	t.Run("delete", func(t *testing.T) {
		bm := newManager(t)
		if err := bm.DeleteBucket(ctx, "missing"); !errors.Is(err, common.ErrBucketNotFound) {
			t.Errorf("delete missing = %v", err)
		}
		if err := bm.CreateBucket(ctx, "tmp"); err != nil {
			t.Fatal(err)
		}
		s, err := bm.Bucket(ctx, "tmp")
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Put("k", strings.NewReader("x")); err != nil {
			t.Fatal(err)
		}
		if err := bm.DeleteBucket(ctx, "tmp"); !errors.Is(err, common.ErrBucketNotEmpty) {
			t.Errorf("delete non-empty = %v", err)
		}
		if err := s.Delete("k"); err != nil {
			t.Fatal(err)
		}
		if mu, ok := s.(common.MultipartUploader); ok {
			id, err := mu.InitiateMultipartUpload(ctx, "pending", nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := bm.DeleteBucket(ctx, "tmp"); !errors.Is(err, common.ErrBucketNotEmpty) {
				t.Errorf("delete with open upload = %v", err)
			}
			if err := mu.AbortMultipartUpload(ctx, "pending", id); err != nil {
				t.Fatal(err)
			}
		}
		if err := bm.DeleteBucket(ctx, "tmp"); err != nil {
			t.Fatalf("delete empty: %v", err)
		}
		if got, _ := bm.ListBuckets(ctx); len(got) != 0 {
			t.Errorf("bucket still listed: %v", got)
		}
		if _, err := bm.Bucket(ctx, "tmp"); !errors.Is(err, common.ErrBucketNotFound) {
			t.Errorf("Bucket after delete = %v", err)
		}
		// The name can be reused.
		if err := bm.CreateBucket(ctx, "tmp"); err != nil {
			t.Errorf("recreate: %v", err)
		}
	})

	t.Run("canceled context", func(t *testing.T) {
		bm := newManager(t)
		cctx, cancel := context.WithCancel(ctx)
		cancel()
		if err := bm.CreateBucket(cctx, "abc"); !errors.Is(err, context.Canceled) {
			t.Errorf("Create = %v", err)
		}
		if err := bm.DeleteBucket(cctx, "abc"); !errors.Is(err, context.Canceled) {
			t.Errorf("Delete = %v", err)
		}
		if _, err := bm.ListBuckets(cctx); !errors.Is(err, context.Canceled) {
			t.Errorf("List = %v", err)
		}
		if _, err := bm.Bucket(cctx, "abc"); !errors.Is(err, context.Canceled) {
			t.Errorf("Bucket = %v", err)
		}
	})
}
