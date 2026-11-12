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

//go:build awss3

package s3

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"

	"github.com/aws/aws-sdk-go/aws"         //nolint:staticcheck // Using v1 SDK, migration to v2 planned
	"github.com/aws/aws-sdk-go/aws/request" //nolint:staticcheck // Using v1 SDK, migration to v2 planned
	"github.com/aws/aws-sdk-go/service/s3"  //nolint:staticcheck // Using v1 SDK, migration to v2 planned
)

// classRecorder is the in-memory fake, recording the storage class of each
// write, the lifecycle rules put, and the calls made.
type classRecorder struct {
	*fakeS3
	putClasses    []string
	createClasses []string
	puts, aborts  int
	lifecycle     *s3.BucketLifecycleConfiguration

	// onUploadPart runs before each UploadPart is answered.
	onUploadPart func(ctx context.Context)
	// onAbort, when set, answers AbortMultipartUpload in place of the fake.
	onAbort func(ctx context.Context) error
	// abortCtx is the context the last AbortMultipartUpload was sent with.
	abortCtx context.Context
}

func newClassRecorder() *classRecorder { return &classRecorder{fakeS3: newFakeS3("bucket-one")} }

func (r *classRecorder) PutObject(in *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
	r.puts++
	r.putClasses = append(r.putClasses, aws.ToString(in.StorageClass))
	return r.fakeS3.PutObject(in)
}

func (r *classRecorder) PutObjectWithContext(ctx context.Context, in *s3.PutObjectInput, o ...request.Option) (*s3.PutObjectOutput, error) {
	r.puts++
	r.putClasses = append(r.putClasses, aws.ToString(in.StorageClass))
	return r.fakeS3.PutObjectWithContext(ctx, in, o...)
}

func (r *classRecorder) CreateMultipartUploadWithContext(ctx context.Context, in *s3.CreateMultipartUploadInput, o ...request.Option) (*s3.CreateMultipartUploadOutput, error) {
	r.createClasses = append(r.createClasses, aws.ToString(in.StorageClass))
	return r.fakeS3.CreateMultipartUploadWithContext(ctx, in, o...)
}

func (r *classRecorder) AbortMultipartUploadWithContext(ctx context.Context, in *s3.AbortMultipartUploadInput, o ...request.Option) (*s3.AbortMultipartUploadOutput, error) {
	r.aborts++
	r.abortCtx = ctx
	if r.onAbort != nil {
		if err := r.onAbort(ctx); err != nil {
			return nil, err
		}
	}
	return r.fakeS3.AbortMultipartUploadWithContext(ctx, in, o...)
}

func (r *classRecorder) UploadPartWithContext(ctx context.Context, in *s3.UploadPartInput, o ...request.Option) (*s3.UploadPartOutput, error) {
	if r.onUploadPart != nil {
		r.onUploadPart(ctx)
	}
	return r.fakeS3.UploadPartWithContext(ctx, in, o...)
}

func (r *classRecorder) GetBucketLifecycleConfiguration(*s3.GetBucketLifecycleConfigurationInput) (*s3.GetBucketLifecycleConfigurationOutput, error) {
	return nil, awsErr("NoSuchLifecycleConfiguration")
}

func (r *classRecorder) PutBucketLifecycleConfiguration(in *s3.PutBucketLifecycleConfigurationInput) (*s3.PutBucketLifecycleConfigurationOutput, error) {
	r.lifecycle = in.LifecycleConfiguration
	return &s3.PutBucketLifecycleConfigurationOutput{}, nil
}

func (r *classRecorder) object(key string) []byte {
	if o, ok := r.buckets["bucket-one"].objects[key]; ok {
		return o.data
	}
	return nil
}

func TestS3_Configure_StorageClass(t *testing.T) {
	s := &S3{}
	err := s.Configure(map[string]string{
		"bucket": "b", "region": "us-east-1",
		"storage_class": "standard_ia", "transition_storage_class": " Deep_Archive ",
	}, noCreds)
	if err != nil {
		t.Fatal(err)
	}
	if s.storageClass != s3.StorageClassStandardIa || s.transitionClass != s3.TransitionStorageClassDeepArchive {
		t.Errorf("classes = %q, %q", s.storageClass, s.transitionClass)
	}

	s = &S3{}
	if err := s.Configure(map[string]string{"bucket": "b", "region": "us-east-1"}, noCreds); err != nil {
		t.Fatal(err)
	}
	if s.storageClass != "" || s.transitionStorageClass() != s3.TransitionStorageClassGlacier {
		t.Errorf("defaults = %q, %q", s.storageClass, s.transitionStorageClass())
	}

	for _, tt := range []struct{ setting, class string }{
		{"storage_class", "PLATINUM"},
		{"transition_storage_class", "PLATINUM"},
		// A class objects can be written with but not transitioned to.
		{"transition_storage_class", s3.StorageClassReducedRedundancy},
		{"transition_storage_class", s3.StorageClassStandard},
	} {
		err := (&S3{}).Configure(map[string]string{"bucket": "b", tt.setting: tt.class}, noCreds)
		var sce *StorageClassError
		if !errors.As(err, &sce) || sce.Setting != tt.setting || sce.Class != tt.class {
			t.Errorf("%s=%s: err = %v", tt.setting, tt.class, err)
		}
		if !errors.Is(err, ErrUnknownStorageClass) || !errors.Is(err, common.ErrInvalidArgument) {
			t.Errorf("%s=%s: err = %v, want ErrUnknownStorageClass", tt.setting, tt.class, err)
		}
	}
}

func TestS3_StorageClass_OnWrites(t *testing.T) {
	ctx := context.Background()
	r := newClassRecorder()
	s := &S3{svc: r, bucket: "bucket-one", storageClass: s3.StorageClassOnezoneIa}
	if err := s.Put("a", strings.NewReader("x")); err != nil {
		t.Fatal(err)
	}
	if err := s.PutWithMetadata(ctx, "b", strings.NewReader("y"), &common.Metadata{ContentType: "text/plain"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.InitiateMultipartUpload(ctx, "c", nil); err != nil {
		t.Fatal(err)
	}
	if strings.Join(r.putClasses, ",") != "ONEZONE_IA,ONEZONE_IA" {
		t.Errorf("put classes = %v", r.putClasses)
	}
	if strings.Join(r.createClasses, ",") != "ONEZONE_IA" {
		t.Errorf("create classes = %v", r.createClasses)
	}
	if string(r.object("a")) != "x" || string(r.object("b")) != "y" {
		t.Errorf("objects = %q, %q", r.object("a"), r.object("b"))
	}

	// No class configured leaves the bucket default.
	r = newClassRecorder()
	s = &S3{svc: r, bucket: "bucket-one"}
	if err := s.Put("a", strings.NewReader("x")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.InitiateMultipartUpload(ctx, "c", nil); err != nil {
		t.Fatal(err)
	}
	if r.putClasses[0] != "" || r.createClasses[0] != "" {
		t.Errorf("classes without a setting = %v, %v", r.putClasses, r.createClasses)
	}
}

func TestS3_Bucket_KeepsStorageSettings(t *testing.T) {
	r := newClassRecorder()
	parent := &S3{svc: r, storageClass: "GLACIER_IR", transitionClass: "DEEP_ARCHIVE", multipartThreshold: 7, partSize: 9}
	m, err := parent.Bucket(context.Background(), "bucket-one")
	if err != nil {
		t.Fatal(err)
	}
	got := m.(*S3)
	if got.storageClass != "GLACIER_IR" || got.transitionClass != "DEEP_ARCHIVE" || got.multipartThreshold != 7 || got.partSize != 9 {
		t.Errorf("bucket storage = %+v", got)
	}
}

func TestS3_AddPolicy_TransitionClass(t *testing.T) {
	for _, tt := range []struct{ configured, want string }{
		{"", s3.TransitionStorageClassGlacier},
		{s3.TransitionStorageClassDeepArchive, s3.TransitionStorageClassDeepArchive},
		{s3.TransitionStorageClassStandardIa, s3.TransitionStorageClassStandardIa},
		{s3.TransitionStorageClassGlacierIr, s3.TransitionStorageClassGlacierIr},
	} {
		r := newClassRecorder()
		s := &S3{svc: r, bucket: "bucket-one", transitionClass: tt.configured}
		err := s.AddPolicy(common.LifecyclePolicy{ID: "p", Prefix: "logs/", Retention: 48 * time.Hour, Action: "archive"})
		if err != nil {
			t.Fatal(err)
		}
		tr := r.lifecycle.Rules[0].Transitions
		if len(tr) != 1 || aws.ToString(tr[0].StorageClass) != tt.want || aws.ToInt64(tr[0].Days) != 2 {
			t.Errorf("configured %q: transitions = %v", tt.configured, tr)
		}
	}
}

func TestAutoPartSize(t *testing.T) {
	const gib = int64(1 << 30)
	for _, tt := range []struct {
		name  string
		size  int64
		floor int64
		want  int64
	}{
		{"6 GiB, default floor", 6 * gib, 0, DefaultPartSize},
		{"6 GiB, minimum floor", 6 * gib, common.MinPartSize, common.MinPartSize},
		{"floor under the minimum", 6 * gib, 1, common.MinPartSize},
		{"1 TiB grows the part", 1024 * gib, 0, 105 << 20},
		{"5 TiB", MaxObjectSize, 0, 525 << 20},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := autoPartSize(tt.size, tt.floor)
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Errorf("part size = %d, want %d", got, tt.want)
			}
			if parts := (tt.size + got - 1) / got; parts > common.MaxParts {
				t.Errorf("%d parts, maximum is %d", parts, common.MaxParts)
			}
			if got < common.MinPartSize || got > common.MaxPartSize {
				t.Errorf("part size %d outside the S3 limits", got)
			}
		})
	}
	if _, err := autoPartSize(MaxObjectSize+1, 0); !errors.Is(err, ErrObjectTooLarge) || !errors.Is(err, common.ErrInvalidArgument) {
		t.Errorf("over 5 TiB = %v", err)
	}
}

// patterned is size bytes of data whose every byte depends on its offset.
func patterned(size int) []byte {
	b := make([]byte, size)
	for i := range b {
		b[i] = byte(i*7 + i/251)
	}
	return b
}

func TestS3_Put_AutoMultipart(t *testing.T) {
	ctx := context.Background()
	data := patterned(2*common.MinPartSize + 3)

	for _, tt := range []struct {
		name string
		put  func(s *S3, key string, r io.Reader) error
	}{
		{"Put", func(s *S3, key string, r io.Reader) error { return s.Put(key, r) }},
		{"PutWithMetadata", func(s *S3, key string, r io.Reader) error {
			return s.PutWithMetadata(ctx, key, r, &common.Metadata{ContentType: "application/x-big"})
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r := newClassRecorder()
			s := &S3{svc: r, bucket: "bucket-one", storageClass: "STANDARD_IA", multipartThreshold: 1 << 20, partSize: common.MinPartSize}

			// The body starts where the reader stands, not at offset zero.
			src := bytes.NewReader(append([]byte("skip"), data...))
			if _, err := src.Seek(4, io.SeekStart); err != nil {
				t.Fatal(err)
			}
			if err := tt.put(s, "big", src); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(r.object("big"), data) {
				t.Fatalf("stored %d bytes, want the %d written", len(r.object("big")), len(data))
			}
			if r.puts != 0 || len(r.bodyTypes) != 3 {
				t.Errorf("PutObject calls = %d, parts = %d; want 0 and 3", r.puts, len(r.bodyTypes))
			}
			if strings.Join(r.createClasses, ",") != "STANDARD_IA" {
				t.Errorf("create classes = %v", r.createClasses)
			}
			if n := len(r.buckets["bucket-one"].uploads); n != 0 {
				t.Errorf("%d uploads left open", n)
			}

			// At the threshold a single PutObject is still used.
			if err := tt.put(s, "small", bytes.NewReader(data[:1<<20])); err != nil {
				t.Fatal(err)
			}
			if r.puts != 1 || len(r.createClasses) != 1 {
				t.Errorf("at the threshold: puts = %d, creates = %d", r.puts, len(r.createClasses))
			}
		})
	}
}

func TestS3_Put_AutoMultipart_Failures(t *testing.T) {
	ctx := context.Background()
	data := patterned(2*common.MinPartSize + 1)
	boom := errors.New("boom")
	for _, tt := range []struct {
		op      string
		inject  error
		wantErr error
		aborts  int
	}{
		{"CreateMultipartUpload", awsErr(s3.ErrCodeNoSuchBucket), common.ErrBucketNotFound, 0},
		{"UploadPart", boom, boom, 1},
		{"UploadPart", awsErr("EntityTooLarge"), common.ErrPartTooLarge, 1},
		{"CompleteMultipartUpload", awsErr("EntityTooSmall"), common.ErrPartTooSmall, 1},
	} {
		t.Run(tt.op, func(t *testing.T) {
			r := newClassRecorder()
			r.errs[tt.op] = tt.inject
			s := &S3{svc: r, bucket: "bucket-one", multipartThreshold: 1, partSize: common.MinPartSize}
			if err := s.PutWithMetadata(ctx, "big", bytes.NewReader(data), nil); !errors.Is(err, tt.wantErr) {
				t.Errorf("err = %v, want %v", err, tt.wantErr)
			}
			if r.aborts != tt.aborts {
				t.Errorf("aborts = %d, want %d", r.aborts, tt.aborts)
			}
			if n := len(r.buckets["bucket-one"].uploads); n != 0 {
				t.Errorf("%d uploads left open", n)
			}
		})
	}

	// A context canceled mid-upload still aborts the upload it made, on a
	// context of its own that is live and bounded.
	r := newClassRecorder()
	s := &S3{svc: r, bucket: "bucket-one", multipartThreshold: 1, partSize: common.MinPartSize}
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	r.onUploadPart = func(context.Context) { cancel() }
	var abortErr error
	var abortHasDeadline bool
	r.onAbort = func(actx context.Context) error {
		abortErr = actx.Err()
		_, abortHasDeadline = actx.Deadline()
		return nil
	}
	if err := s.PutWithMetadata(cctx, "big", bytes.NewReader(data), nil); !errors.Is(err, context.Canceled) {
		t.Errorf("canceled = %v", err)
	}
	if r.aborts != 1 {
		t.Errorf("aborts = %d, want 1", r.aborts)
	}
	if abortErr != nil || !abortHasDeadline {
		t.Errorf("abort context: err = %v, deadline = %v; want live and bounded", abortErr, abortHasDeadline)
	}
	if n := len(r.buckets["bucket-one"].uploads); n != 0 {
		t.Errorf("%d uploads left open after cancel", n)
	}

	// An object over the S3 maximum is refused before anything is sent.
	r = newClassRecorder()
	s = &S3{svc: r, bucket: "bucket-one"}
	if err := s.Put("huge", &endlessSeeker{size: MaxObjectSize + 1}); !errors.Is(err, ErrObjectTooLarge) {
		t.Errorf("over 5 TiB = %v", err)
	}
	if r.puts != 0 || len(r.createClasses) != 0 {
		t.Errorf("requests sent for an object over the maximum")
	}
}

// The cleanup abort of a failed automatic multipart Put is bounded: an
// endpoint that never answers it does not hold the Put forever.
func TestS3_Put_AutoMultipart_AbortIsBounded(t *testing.T) {
	boom := errors.New("boom")
	r := newClassRecorder()
	r.errs["UploadPart"] = boom
	r.onAbort = func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Second):
			return errors.New("abort was not bounded")
		}
	}
	s := &S3{svc: r, bucket: "bucket-one", multipartThreshold: 1, partSize: common.MinPartSize, abortTimeout: 20 * time.Millisecond}

	start := time.Now()
	err := s.PutWithMetadata(context.Background(), "big", bytes.NewReader(patterned(common.MinPartSize+1)), nil)
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("Put returned after %v, want about the 20ms abort bound", elapsed)
	}
	if !errors.Is(err, boom) || !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want the part failure joined with the abort's deadline", err)
	}
	if r.aborts != 1 {
		t.Errorf("aborts = %d, want 1", r.aborts)
	}
	if got := (&S3{}).abortTimeoutOrDefault(); got != DefaultAbortTimeout {
		t.Errorf("default abort timeout = %v", got)
	}
}

// endlessSeeker reports size bytes without holding them.
type endlessSeeker struct{ size, off int64 }

func (e *endlessSeeker) Read(p []byte) (int, error) {
	if e.off >= e.size {
		return 0, io.EOF
	}
	n := int(min(int64(len(p)), e.size-e.off))
	clear(p[:n])
	e.off += int64(n)
	return n, nil
}

func (e *endlessSeeker) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		e.off = offset
	case io.SeekCurrent:
		e.off += offset
	case io.SeekEnd:
		e.off = e.size + offset
	}
	return e.off, nil
}

func TestPartWindow(t *testing.T) {
	src := strings.NewReader("0123456789")
	w := &partWindow{rs: src, start: 3, size: 4}
	if _, err := w.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(w)
	if err != nil || string(got) != "3456" {
		t.Fatalf("read %q, %v", got, err)
	}
	// The SDK rewinds a body to retry it.
	if pos, err := w.Seek(-2, io.SeekEnd); err != nil || pos != 2 {
		t.Fatalf("seek = %d, %v", pos, err)
	}
	if got, _ := io.ReadAll(w); string(got) != "56" {
		t.Errorf("after seek read %q", got)
	}
	if pos, _ := w.Seek(-1, io.SeekCurrent); pos != 3 {
		t.Errorf("seek current = %d", pos)
	}
	for _, bad := range []struct {
		off    int64
		whence int
	}{{5, io.SeekStart}, {-1, io.SeekStart}, {0, 7}} {
		if _, err := w.Seek(bad.off, bad.whence); !errors.Is(err, common.ErrInvalidArgument) {
			t.Errorf("seek(%d, %d) = %v", bad.off, bad.whence, err)
		}
	}

	// A source that ends inside the window is a part of the wrong size.
	short := &partWindow{rs: strings.NewReader("01234"), start: 3, size: 4}
	if _, err := short.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(short); !errors.Is(err, errPartSize) {
		t.Errorf("short source = %v", err)
	}
}
