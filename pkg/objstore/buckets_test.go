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

package objstore

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/memory"
)

// faultyUploads is a memory backend whose listing and abort can be made to
// fail.
type faultyUploads struct {
	*memory.Memory
	listErr  error
	abortErr map[string]error // by key
	aborted  []string
}

func (f *faultyUploads) ListMultipartUploads(ctx context.Context, prefix string) ([]common.MultipartUploadInfo, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	return f.Memory.ListMultipartUploads(ctx, prefix)
}

func (f *faultyUploads) AbortMultipartUpload(ctx context.Context, key, uploadID string) error {
	if err := f.abortErr[key]; err != nil {
		return err
	}
	f.aborted = append(f.aborted, key)
	return f.Memory.AbortMultipartUpload(ctx, key, uploadID)
}

// initBucketFacade initializes the facade with a memory backend "mem" as the
// default and a backend "plain" that offers neither capability.
func initBucketFacade(t *testing.T) *memory.Memory {
	t.Helper()
	Reset()
	t.Cleanup(Reset)
	mem := memory.New().(*memory.Memory)
	err := Initialize(&FacadeConfig{
		Backends: map[string]common.Storage{
			"mem":   mem,
			"plain": newMockStorage("plain"),
		},
		DefaultBackend: "mem",
	})
	if err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	return mem
}

func TestFacade_Buckets(t *testing.T) {
	initBucketFacade(t)
	ctx := context.Background()

	for _, backend := range []string{"mem", ""} {
		name := "bucket-" + map[string]string{"mem": "named", "": "default"}[backend]
		if err := CreateBucket(ctx, backend, name); err != nil {
			t.Fatalf("CreateBucket(%q): %v", backend, err)
		}
	}
	infos, err := ListBuckets(ctx, "")
	if err != nil || len(infos) != 2 {
		t.Fatalf("ListBuckets = %+v, %v", infos, err)
	}
	if err := CreateBucket(ctx, "mem", "bucket-named"); !errors.Is(err, common.ErrBucketAlreadyExists) {
		t.Errorf("duplicate = %v", err)
	}
	if err := DeleteBucket(ctx, "mem", "bucket-named"); err != nil {
		t.Errorf("DeleteBucket: %v", err)
	}
	if err := DeleteBucket(ctx, "mem", "bucket-named"); !errors.Is(err, common.ErrBucketNotFound) {
		t.Errorf("second delete = %v", err)
	}
}

func TestFacade_Buckets_Errors(t *testing.T) {
	initBucketFacade(t)
	ctx := context.Background()

	tests := []struct {
		name    string
		call    func() error
		wantErr error
	}{
		{"create: bad name", func() error { return CreateBucket(ctx, "mem", "Bad_Name") }, common.ErrInvalidBucketName},
		{"create: empty name", func() error { return CreateBucket(ctx, "mem", "") }, common.ErrInvalidBucketName},
		{"delete: bad name", func() error { return DeleteBucket(ctx, "mem", "../x") }, common.ErrInvalidBucketName},
		{"create: unsupported backend", func() error { return CreateBucket(ctx, "plain", "bucket-a") }, common.ErrNotSupported},
		{"delete: unsupported backend", func() error { return DeleteBucket(ctx, "plain", "bucket-a") }, common.ErrNotSupported},
		{"list: unsupported backend", func() error { _, err := ListBuckets(ctx, "plain"); return err }, common.ErrNotSupported},
		{"create: unknown backend", func() error { return CreateBucket(ctx, "nope", "bucket-a") }, ErrBackendNotFound},
		{"list: unknown backend", func() error { _, err := ListBuckets(ctx, "nope"); return err }, ErrBackendNotFound},
		{"create: invalid backend name", func() error { return CreateBucket(ctx, "BAD NAME", "bucket-a") }, common.ErrInvalidArgument},
		{"delete: unknown backend", func() error { return DeleteBucket(ctx, "nope", "bucket-a") }, ErrBackendNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.call(); !errors.Is(err, tt.wantErr) {
				t.Errorf("err = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestFacade_Buckets_NotInitialized(t *testing.T) {
	Reset()
	ctx := context.Background()
	if err := CreateBucket(ctx, "", "bucket-a"); !errors.Is(err, ErrNotInitialized) {
		t.Errorf("CreateBucket = %v", err)
	}
	if err := DeleteBucket(ctx, "", "bucket-a"); !errors.Is(err, ErrNotInitialized) {
		t.Errorf("DeleteBucket = %v", err)
	}
	if _, err := ListBuckets(ctx, ""); !errors.Is(err, ErrNotInitialized) {
		t.Errorf("ListBuckets = %v", err)
	}
	if _, err := InitiateMultipartUpload(ctx, "k", nil); !errors.Is(err, ErrNotInitialized) {
		t.Errorf("InitiateMultipartUpload = %v", err)
	}
	if _, err := ListMultipartUploads(ctx, "", ""); !errors.Is(err, ErrNotInitialized) {
		t.Errorf("ListMultipartUploads = %v", err)
	}
}

func TestFacade_Multipart(t *testing.T) {
	initBucketFacade(t)
	ctx := context.Background()

	for _, ref := range []string{"mem:dir/obj.bin", "dir/default.bin"} {
		t.Run(ref, func(t *testing.T) {
			id, err := InitiateMultipartUpload(ctx, ref, &common.Metadata{ContentType: "application/x-test"})
			if err != nil {
				t.Fatal(err)
			}
			first := strings.Repeat("a", common.MinPartSize)
			p1, err := UploadPart(ctx, ref, id, 1, strings.NewReader(first), int64(len(first)))
			if err != nil {
				t.Fatal(err)
			}
			p2, err := UploadPart(ctx, ref, id, 2, strings.NewReader("tail"), 4)
			if err != nil {
				t.Fatal(err)
			}
			ups, err := ListMultipartUploads(ctx, "", "dir/")
			if err != nil || len(ups) != 1 || ups[0].UploadID != id {
				t.Fatalf("ListMultipartUploads = %+v, %v", ups, err)
			}
			err = CompleteMultipartUpload(ctx, ref, id, []common.CompletedPart{
				{PartNumber: 1, ETag: p1.ETag}, {PartNumber: 2, ETag: p2.ETag},
			})
			if err != nil {
				t.Fatal(err)
			}
			rc, err := GetWithContext(ctx, ref)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = rc.Close() }()
			got, _ := io.ReadAll(rc)
			if string(got) != first+"tail" {
				t.Errorf("object has %d bytes", len(got))
			}
			if ups, _ := ListMultipartUploads(ctx, "mem", ""); len(ups) != 0 {
				t.Errorf("upload still listed after complete: %+v", ups)
			}
		})
	}
}

func TestFacade_Multipart_Abort(t *testing.T) {
	initBucketFacade(t)
	ctx := context.Background()
	id, err := InitiateMultipartUpload(ctx, "mem:k", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := AbortMultipartUpload(ctx, "mem:k", id); err != nil {
		t.Fatal(err)
	}
	if err := AbortMultipartUpload(ctx, "mem:k", id); !errors.Is(err, common.ErrUploadNotFound) {
		t.Errorf("second abort = %v", err)
	}
}

func TestFacade_Multipart_Errors(t *testing.T) {
	initBucketFacade(t)
	ctx := context.Background()
	part := []common.CompletedPart{{PartNumber: 1, ETag: "e"}}
	body := strings.NewReader("x")

	tests := []struct {
		name    string
		call    func() error
		wantErr error
	}{
		{"initiate: empty ref", func() error { _, err := InitiateMultipartUpload(ctx, "", nil); return err }, common.ErrInvalidArgument},
		{"initiate: traversal", func() error { _, err := InitiateMultipartUpload(ctx, "mem:../x", nil); return err }, common.ErrInvalidArgument},
		{"initiate: reserved prefix", func() error { _, err := InitiateMultipartUpload(ctx, "mem:.objstore-multipart/x", nil); return err }, common.ErrInvalidArgument},
		{"initiate: unsupported", func() error { _, err := InitiateMultipartUpload(ctx, "plain:k", nil); return err }, common.ErrNotSupported},
		{"initiate: unknown backend", func() error { _, err := InitiateMultipartUpload(ctx, "nope:k", nil); return err }, ErrBackendNotFound},
		{"upload: empty ref", func() error { _, err := UploadPart(ctx, "", "id", 1, body, 1); return err }, common.ErrInvalidArgument},
		{"upload: unsupported", func() error { _, err := UploadPart(ctx, "plain:k", "id", 1, body, 1); return err }, common.ErrNotSupported},
		{"upload: unknown upload", func() error { _, err := UploadPart(ctx, "mem:k", "missing", 1, body, 1); return err }, common.ErrUploadNotFound},
		{"complete: empty ref", func() error { return CompleteMultipartUpload(ctx, "", "id", part) }, common.ErrInvalidArgument},
		{"complete: unsupported", func() error { return CompleteMultipartUpload(ctx, "plain:k", "id", part) }, common.ErrNotSupported},
		{"complete: unknown upload", func() error { return CompleteMultipartUpload(ctx, "mem:k", "missing", part) }, common.ErrUploadNotFound},
		{"abort: empty ref", func() error { return AbortMultipartUpload(ctx, "", "id") }, common.ErrInvalidArgument},
		{"abort: unsupported", func() error { return AbortMultipartUpload(ctx, "plain:k", "id") }, common.ErrNotSupported},
		{"abort: unknown upload", func() error { return AbortMultipartUpload(ctx, "mem:k", "missing") }, common.ErrUploadNotFound},
		{"list: unsupported", func() error { _, err := ListMultipartUploads(ctx, "plain", ""); return err }, common.ErrNotSupported},
		{"list: unknown backend", func() error { _, err := ListMultipartUploads(ctx, "nope", ""); return err }, ErrBackendNotFound},
		{"list: bad prefix", func() error { _, err := ListMultipartUploads(ctx, "mem", "../x"); return err }, common.ErrInvalidArgument},
		{"list: canceled", func() error {
			c, cancel := context.WithCancel(ctx)
			cancel()
			_, err := ListMultipartUploads(c, "mem", "")
			return err
		}, context.Canceled},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.call(); !errors.Is(err, tt.wantErr) {
				t.Errorf("err = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

// stubEncrypterFactory is enough to wrap a backend; nothing is encrypted.
type stubEncrypterFactory struct{}

func (stubEncrypterFactory) DefaultKeyID() string { return "key" }
func (stubEncrypterFactory) GetEncrypter(string) (common.Encrypter, error) {
	return nil, errors.New("not used")
}
func (stubEncrypterFactory) Close() error { return nil }

func TestFacade_EncryptedBackendRefusesMultipartAndBuckets(t *testing.T) {
	Reset()
	t.Cleanup(Reset)
	ctx := context.Background()
	wrapped := common.NewEncryptedStorage(memory.New(), stubEncrypterFactory{})
	if err := Initialize(&FacadeConfig{Backends: map[string]common.Storage{"enc": wrapped}, DefaultBackend: "enc"}); err != nil {
		t.Fatal(err)
	}

	if _, err := InitiateMultipartUpload(ctx, "enc:k", nil); !errors.Is(err, common.ErrNotSupported) {
		t.Errorf("InitiateMultipartUpload = %v", err)
	}
	if _, err := UploadPart(ctx, "k", "id", 1, strings.NewReader("x"), 1); !errors.Is(err, common.ErrNotSupported) {
		t.Errorf("UploadPart = %v", err)
	}
	if _, err := ListMultipartUploads(ctx, "", ""); !errors.Is(err, common.ErrNotSupported) {
		t.Errorf("ListMultipartUploads = %v", err)
	}
	if err := CreateBucket(ctx, "enc", "bucket-a"); !errors.Is(err, common.ErrNotSupported) {
		t.Errorf("CreateBucket = %v", err)
	}
}

func TestFacade_AbortExpiredMultipartUploads(t *testing.T) {
	mem := initBucketFacade(t)
	ctx := context.Background()
	for _, k := range []string{"one", "two"} {
		if _, err := mem.InitiateMultipartUpload(ctx, k, nil); err != nil {
			t.Fatal(err)
		}
	}

	n, err := AbortExpiredMultipartUploads(ctx, "mem", time.Hour)
	if err != nil || n != 0 {
		t.Fatalf("young uploads: aborted %d, %v", n, err)
	}
	if ups, _ := ListMultipartUploads(ctx, "mem", ""); len(ups) != 2 {
		t.Fatalf("young uploads were removed: %+v", ups)
	}

	time.Sleep(10 * time.Millisecond)
	n, err = AbortExpiredMultipartUploads(ctx, "", 5*time.Millisecond)
	if err != nil || n != 2 {
		t.Fatalf("aborted %d, %v", n, err)
	}
	if ups, _ := ListMultipartUploads(ctx, "mem", ""); len(ups) != 0 {
		t.Errorf("uploads remain: %+v", ups)
	}
}

func TestFacade_AbortExpiredMultipartUploads_Errors(t *testing.T) {
	initBucketFacade(t)
	ctx := context.Background()

	for _, age := range []time.Duration{0, -time.Second} {
		if _, err := AbortExpiredMultipartUploads(ctx, "mem", age); !errors.Is(err, common.ErrInvalidArgument) {
			t.Errorf("age %v: err = %v", age, err)
		}
	}
	if _, err := AbortExpiredMultipartUploads(ctx, "plain", time.Hour); !errors.Is(err, common.ErrNotSupported) {
		t.Errorf("unsupported = %v", err)
	}
	if _, err := AbortExpiredMultipartUploads(ctx, "nope", time.Hour); !errors.Is(err, ErrBackendNotFound) {
		t.Errorf("unknown backend = %v", err)
	}
}

func TestFacade_AbortExpiredMultipartUploads_FaultInjection(t *testing.T) {
	Reset()
	t.Cleanup(Reset)
	ctx := context.Background()
	f := &faultyUploads{Memory: memory.New().(*memory.Memory), abortErr: map[string]error{}}
	if err := Initialize(&FacadeConfig{Backends: map[string]common.Storage{"f": f}, DefaultBackend: "f"}); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"a", "b", "c", "d"} {
		if _, err := f.InitiateMultipartUpload(ctx, k, nil); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(10 * time.Millisecond)

	boom := errors.New("backend unavailable")
	f.abortErr["b"] = boom
	f.abortErr["c"] = common.ErrUploadNotFound // removed by someone else meanwhile

	n, err := AbortExpiredMultipartUploads(ctx, "f", time.Millisecond)
	if !errors.Is(err, boom) {
		t.Errorf("err = %v, want the abort failure", err)
	}
	if n != 2 {
		t.Errorf("aborted %d, want 2 (a and d; b failed, c was already gone)", n)
	}
	if len(f.aborted) != 2 {
		t.Errorf("aborted keys = %v", f.aborted)
	}

	f.listErr = errors.New("list failed")
	if _, err := AbortExpiredMultipartUploads(ctx, "f", time.Millisecond); !errors.Is(err, f.listErr) {
		t.Errorf("list failure = %v", err)
	}
}
