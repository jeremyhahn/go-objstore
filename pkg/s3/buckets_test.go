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
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"testing"
	"testing/iotest"
	"time"

	"github.com/jeremyhahn/go-objstore/internal/uploadtest"
	"github.com/jeremyhahn/go-objstore/pkg/common"

	"github.com/aws/aws-sdk-go/aws"                //nolint:staticcheck // Using v1 SDK, migration to v2 planned
	"github.com/aws/aws-sdk-go/aws/awserr"         //nolint:staticcheck // Using v1 SDK, migration to v2 planned
	"github.com/aws/aws-sdk-go/aws/request"        //nolint:staticcheck // Using v1 SDK, migration to v2 planned
	"github.com/aws/aws-sdk-go/service/s3"         //nolint:staticcheck // Using v1 SDK, migration to v2 planned
	"github.com/aws/aws-sdk-go/service/s3/s3iface" //nolint:staticcheck // Using v1 SDK, migration to v2 planned
)

// fakeObject is a stored object of the fake service.
type fakeObject struct {
	data     []byte
	meta     map[string]*string
	ctype    *string
	cenc     *string
	modified time.Time
}

type fakePart struct {
	data []byte
	etag string
}

type fakeUpload struct {
	key       string
	initiated time.Time
	meta      map[string]*string
	ctype     *string
	cenc      *string
	parts     map[int]fakePart
}

type fakeBucket struct {
	created time.Time
	objects map[string]*fakeObject
	uploads map[string]*fakeUpload
}

// fakeS3 is an in-memory service that answers the calls the provider makes
// the way the service does, including its error codes. errs injects a failure
// per operation name.
type fakeS3 struct {
	s3iface.S3API

	mu       sync.Mutex
	buckets  map[string]*fakeBucket
	errs     map[string]error
	nextID   int
	pageSize int // uploads per listing page; zero means all

	createInput *s3.CreateBucketInput
	creates     int // CreateBucket requests received
	bodyTypes   []string
}

func newFakeS3(buckets ...string) *fakeS3 {
	f := &fakeS3{buckets: map[string]*fakeBucket{}, errs: map[string]error{}}
	for _, b := range buckets {
		f.buckets[b] = &fakeBucket{
			created: time.Now(),
			objects: map[string]*fakeObject{},
			uploads: map[string]*fakeUpload{},
		}
	}
	return f
}

func awsErr(code string) error { return awserr.New(code, code, nil) }

// begin takes the lock and returns the injected failure of the operation, or
// the context's error.
func (f *fakeS3) begin(ctx context.Context, op string) error {
	f.mu.Lock()
	if err := ctx.Err(); err != nil {
		return err
	}
	return f.errs[op]
}

func (f *fakeS3) bucket(name string) (*fakeBucket, error) {
	b, ok := f.buckets[name]
	if !ok {
		return nil, awsErr(s3.ErrCodeNoSuchBucket)
	}
	return b, nil
}

func (f *fakeS3) CreateBucketWithContext(ctx context.Context, in *s3.CreateBucketInput, _ ...request.Option) (*s3.CreateBucketOutput, error) {
	defer f.mu.Unlock()
	if err := f.begin(ctx, "CreateBucket"); err != nil {
		return nil, err
	}
	f.creates++
	f.createInput = in
	name := aws.StringValue(in.Bucket)
	if _, ok := f.buckets[name]; ok {
		return nil, awsErr(s3.ErrCodeBucketAlreadyOwnedByYou)
	}
	f.buckets[name] = &fakeBucket{
		created: time.Now(),
		objects: map[string]*fakeObject{},
		uploads: map[string]*fakeUpload{},
	}
	return &s3.CreateBucketOutput{}, nil
}

func (f *fakeS3) DeleteBucketWithContext(ctx context.Context, in *s3.DeleteBucketInput, _ ...request.Option) (*s3.DeleteBucketOutput, error) {
	defer f.mu.Unlock()
	if err := f.begin(ctx, "DeleteBucket"); err != nil {
		return nil, err
	}
	name := aws.StringValue(in.Bucket)
	b, err := f.bucket(name)
	if err != nil {
		return nil, err
	}
	if len(b.objects) > 0 {
		return nil, awsErr("BucketNotEmpty")
	}
	delete(f.buckets, name)
	return &s3.DeleteBucketOutput{}, nil
}

func (f *fakeS3) ListBucketsWithContext(ctx context.Context, _ *s3.ListBucketsInput, _ ...request.Option) (*s3.ListBucketsOutput, error) {
	defer f.mu.Unlock()
	if err := f.begin(ctx, "ListBuckets"); err != nil {
		return nil, err
	}
	out := &s3.ListBucketsOutput{}
	for name, b := range f.buckets {
		out.Buckets = append(out.Buckets, &s3.Bucket{Name: aws.String(name), CreationDate: aws.Time(b.created)})
	}
	// The service lists in no promised order; the provider must sort.
	sort.Slice(out.Buckets, func(i, j int) bool { return *out.Buckets[i].Name > *out.Buckets[j].Name })
	return out, nil
}

func (f *fakeS3) HeadBucketWithContext(ctx context.Context, in *s3.HeadBucketInput, _ ...request.Option) (*s3.HeadBucketOutput, error) {
	defer f.mu.Unlock()
	if err := f.begin(ctx, "HeadBucket"); err != nil {
		return nil, err
	}
	if _, ok := f.buckets[aws.StringValue(in.Bucket)]; !ok {
		return nil, awsErr("NotFound")
	}
	return &s3.HeadBucketOutput{}, nil
}

func (f *fakeS3) putObject(in *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
	b, err := f.bucket(aws.StringValue(in.Bucket))
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(in.Body)
	if err != nil {
		return nil, err
	}
	b.objects[aws.StringValue(in.Key)] = &fakeObject{
		data: data, meta: in.Metadata, ctype: in.ContentType, cenc: in.ContentEncoding, modified: time.Now(),
	}
	return &s3.PutObjectOutput{}, nil
}

func (f *fakeS3) PutObject(in *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.putObject(in)
}

func (f *fakeS3) PutObjectWithContext(ctx context.Context, in *s3.PutObjectInput, _ ...request.Option) (*s3.PutObjectOutput, error) {
	defer f.mu.Unlock()
	if err := f.begin(ctx, "PutObject"); err != nil {
		return nil, err
	}
	return f.putObject(in)
}

func (f *fakeS3) getObject(in *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	b, err := f.bucket(aws.StringValue(in.Bucket))
	if err != nil {
		return nil, err
	}
	o, ok := b.objects[aws.StringValue(in.Key)]
	if !ok {
		return nil, awsErr(s3.ErrCodeNoSuchKey)
	}
	return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(o.data))}, nil
}

func (f *fakeS3) GetObject(in *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.getObject(in)
}

func (f *fakeS3) GetObjectWithContext(ctx context.Context, in *s3.GetObjectInput, _ ...request.Option) (*s3.GetObjectOutput, error) {
	defer f.mu.Unlock()
	if err := f.begin(ctx, "GetObject"); err != nil {
		return nil, err
	}
	return f.getObject(in)
}

func (f *fakeS3) HeadObjectWithContext(ctx context.Context, in *s3.HeadObjectInput, _ ...request.Option) (*s3.HeadObjectOutput, error) {
	defer f.mu.Unlock()
	if err := f.begin(ctx, "HeadObject"); err != nil {
		return nil, err
	}
	b, err := f.bucket(aws.StringValue(in.Bucket))
	if err != nil {
		return nil, err
	}
	o, ok := b.objects[aws.StringValue(in.Key)]
	if !ok {
		return nil, awsErr("NotFound")
	}
	return &s3.HeadObjectOutput{
		ContentLength:   aws.Int64(int64(len(o.data))),
		LastModified:    aws.Time(o.modified),
		ETag:            aws.String(etagOf(o.data)),
		ContentType:     o.ctype,
		ContentEncoding: o.cenc,
		Metadata:        o.meta,
	}, nil
}

func (f *fakeS3) deleteObject(in *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
	b, err := f.bucket(aws.StringValue(in.Bucket))
	if err != nil {
		return nil, err
	}
	delete(b.objects, aws.StringValue(in.Key))
	return &s3.DeleteObjectOutput{}, nil
}

func (f *fakeS3) DeleteObject(in *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.deleteObject(in)
}

func (f *fakeS3) DeleteObjectWithContext(ctx context.Context, in *s3.DeleteObjectInput, _ ...request.Option) (*s3.DeleteObjectOutput, error) {
	defer f.mu.Unlock()
	if err := f.begin(ctx, "DeleteObject"); err != nil {
		return nil, err
	}
	return f.deleteObject(in)
}

func (f *fakeS3) listObjects(in *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
	b, err := f.bucket(aws.StringValue(in.Bucket))
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(b.objects))
	for k := range b.objects {
		if strings.HasPrefix(k, aws.StringValue(in.Prefix)) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	out := &s3.ListObjectsV2Output{}
	for _, k := range keys {
		out.Contents = append(out.Contents, &s3.Object{Key: aws.String(k)})
	}
	return out, nil
}

func (f *fakeS3) ListObjectsV2(in *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.listObjects(in)
}

func (f *fakeS3) ListObjectsV2WithContext(ctx context.Context, in *s3.ListObjectsV2Input, _ ...request.Option) (*s3.ListObjectsV2Output, error) {
	defer f.mu.Unlock()
	if err := f.begin(ctx, "ListObjectsV2"); err != nil {
		return nil, err
	}
	return f.listObjects(in)
}

func etagOf(data []byte) string {
	sum := sha256.Sum256(data)
	return `"` + hex.EncodeToString(sum[:16]) + `"`
}

func (f *fakeS3) CreateMultipartUploadWithContext(ctx context.Context, in *s3.CreateMultipartUploadInput, _ ...request.Option) (*s3.CreateMultipartUploadOutput, error) {
	defer f.mu.Unlock()
	if err := f.begin(ctx, "CreateMultipartUpload"); err != nil {
		return nil, err
	}
	b, err := f.bucket(aws.StringValue(in.Bucket))
	if err != nil {
		return nil, err
	}
	f.nextID++
	id := fmt.Sprintf("upload-%d", f.nextID)
	b.uploads[id] = &fakeUpload{
		key: aws.StringValue(in.Key), initiated: time.Now(), meta: in.Metadata,
		ctype: in.ContentType, cenc: in.ContentEncoding, parts: map[int]fakePart{},
	}
	return &s3.CreateMultipartUploadOutput{UploadId: aws.String(id)}, nil
}

func (f *fakeS3) UploadPartWithContext(ctx context.Context, in *s3.UploadPartInput, _ ...request.Option) (*s3.UploadPartOutput, error) {
	data, err := io.ReadAll(in.Body)
	defer f.mu.Unlock()
	opErr := f.begin(ctx, "UploadPart")
	f.bodyTypes = append(f.bodyTypes, fmt.Sprintf("%T", in.Body))
	if opErr != nil {
		return nil, opErr
	}
	if err != nil {
		return nil, err
	}
	if int64(len(data)) != aws.Int64Value(in.ContentLength) {
		return nil, awsErr("IncompleteBody")
	}
	b, err := f.bucket(aws.StringValue(in.Bucket))
	if err != nil {
		return nil, err
	}
	u, ok := b.uploads[aws.StringValue(in.UploadId)]
	if !ok || u.key != aws.StringValue(in.Key) {
		return nil, awsErr(s3.ErrCodeNoSuchUpload)
	}
	etag := etagOf(data)
	u.parts[int(aws.Int64Value(in.PartNumber))] = fakePart{data: data, etag: etag}
	return &s3.UploadPartOutput{ETag: aws.String(etag)}, nil
}

func (f *fakeS3) CompleteMultipartUploadWithContext(ctx context.Context, in *s3.CompleteMultipartUploadInput, _ ...request.Option) (*s3.CompleteMultipartUploadOutput, error) {
	defer f.mu.Unlock()
	if err := f.begin(ctx, "CompleteMultipartUpload"); err != nil {
		return nil, err
	}
	b, err := f.bucket(aws.StringValue(in.Bucket))
	if err != nil {
		return nil, err
	}
	id := aws.StringValue(in.UploadId)
	u, ok := b.uploads[id]
	if !ok || u.key != aws.StringValue(in.Key) {
		return nil, awsErr(s3.ErrCodeNoSuchUpload)
	}
	listed := in.MultipartUpload.Parts
	var data []byte
	prev := int64(0)
	for i, p := range listed {
		n := aws.Int64Value(p.PartNumber)
		if n <= prev {
			return nil, awsErr("InvalidPartOrder")
		}
		prev = n
		stored, ok := u.parts[int(n)]
		if !ok || stored.etag != aws.StringValue(p.ETag) {
			return nil, awsErr("InvalidPart")
		}
		if i < len(listed)-1 && len(stored.data) < common.MinPartSize {
			return nil, awsErr("EntityTooSmall")
		}
		data = append(data, stored.data...)
	}
	b.objects[u.key] = &fakeObject{data: data, meta: u.meta, ctype: u.ctype, cenc: u.cenc, modified: time.Now()}
	delete(b.uploads, id)
	return &s3.CompleteMultipartUploadOutput{}, nil
}

func (f *fakeS3) AbortMultipartUploadWithContext(ctx context.Context, in *s3.AbortMultipartUploadInput, _ ...request.Option) (*s3.AbortMultipartUploadOutput, error) {
	defer f.mu.Unlock()
	if err := f.begin(ctx, "AbortMultipartUpload"); err != nil {
		return nil, err
	}
	b, err := f.bucket(aws.StringValue(in.Bucket))
	if err != nil {
		return nil, err
	}
	id := aws.StringValue(in.UploadId)
	if u, ok := b.uploads[id]; !ok || u.key != aws.StringValue(in.Key) {
		return nil, awsErr(s3.ErrCodeNoSuchUpload)
	}
	delete(b.uploads, id)
	return &s3.AbortMultipartUploadOutput{}, nil
}

func (f *fakeS3) ListMultipartUploadsWithContext(ctx context.Context, in *s3.ListMultipartUploadsInput, _ ...request.Option) (*s3.ListMultipartUploadsOutput, error) {
	defer f.mu.Unlock()
	if err := f.begin(ctx, "ListMultipartUploads"); err != nil {
		return nil, err
	}
	b, err := f.bucket(aws.StringValue(in.Bucket))
	if err != nil {
		return nil, err
	}
	type entry struct {
		id string
		u  *fakeUpload
	}
	var all []entry
	for id, u := range b.uploads {
		if strings.HasPrefix(u.key, aws.StringValue(in.Prefix)) {
			all = append(all, entry{id, u})
		}
	}
	// The service orders by key and then by upload ID.
	sort.Slice(all, func(i, j int) bool {
		if all[i].u.key != all[j].u.key {
			return all[i].u.key < all[j].u.key
		}
		return all[i].id < all[j].id
	})

	out := &s3.ListMultipartUploadsOutput{}
	limit := f.pageSize
	if in.MaxUploads != nil {
		limit = int(*in.MaxUploads)
	}
	after := func(e entry) bool {
		if in.KeyMarker == nil {
			return true
		}
		if e.u.key != *in.KeyMarker {
			return e.u.key > *in.KeyMarker
		}
		return in.UploadIdMarker != nil && e.id > *in.UploadIdMarker
	}
	for _, e := range all {
		if !after(e) {
			continue
		}
		if limit > 0 && len(out.Uploads) == limit {
			out.IsTruncated = aws.Bool(true)
			break
		}
		out.Uploads = append(out.Uploads, &s3.MultipartUpload{
			Key: aws.String(e.u.key), UploadId: aws.String(e.id), Initiated: aws.Time(e.u.initiated),
		})
		out.NextKeyMarker, out.NextUploadIdMarker = aws.String(e.u.key), aws.String(e.id)
	}
	return out, nil
}

// --- conformance ---

func TestS3_MultipartConformance(t *testing.T) {
	uploadtest.RunMultipart(t, func(t *testing.T) common.Storage {
		return &S3{svc: newFakeS3("bucket-one"), bucket: "bucket-one"}
	})
}

func TestS3_BucketsConformance(t *testing.T) {
	uploadtest.RunBuckets(t, func(t *testing.T) common.BucketManager {
		return &S3{svc: newFakeS3()}
	})
}

// --- configuration ---

func TestS3_Configure_MultipartExpiry(t *testing.T) {
	s := &S3{}
	if err := s.Configure(map[string]string{"bucket": "b", "region": "us-east-1", "multipartExpiry": "2h"}); err != nil {
		t.Fatal(err)
	}
	if s.multipartExpiry != 2*time.Hour {
		t.Errorf("multipartExpiry = %v", s.multipartExpiry)
	}
	if s.region != "us-east-1" {
		t.Errorf("region = %q", s.region)
	}
	for _, bad := range []string{"x", "-1h", "0s"} {
		err := (&S3{}).Configure(map[string]string{"bucket": "b", "multipartExpiry": bad})
		if !errors.Is(err, common.ErrInvalidArgument) {
			t.Errorf("multipartExpiry %q: err = %v", bad, err)
		}
	}
	if got := (&S3{}).expiry(); got <= 0 {
		t.Errorf("default expiry = %v", got)
	}
}

func TestNewManager(t *testing.T) {
	m, err := NewManager(map[string]string{"region": "eu-west-1", "endpoint": "http://localhost:9000"})
	if err != nil {
		t.Fatal(err)
	}
	if m.svc == nil || m.bucket != "" || m.region != "eu-west-1" {
		t.Errorf("manager = %+v", m)
	}
	if _, err := NewManager(map[string]string{"multipartExpiry": "nope"}); !errors.Is(err, common.ErrInvalidArgument) {
		t.Errorf("bad expiry = %v", err)
	}
}

func TestS3_ManagerWithoutBucket(t *testing.T) {
	m := &S3{svc: newFakeS3()}
	ctx := context.Background()
	if _, err := m.InitiateMultipartUpload(ctx, "k", nil); !errors.Is(err, common.ErrBucketNotSet) {
		t.Errorf("Initiate = %v", err)
	}
	if _, err := m.UploadPart(ctx, "k", "id", 1, strings.NewReader("x"), 1); !errors.Is(err, common.ErrBucketNotSet) {
		t.Errorf("UploadPart = %v", err)
	}
	if err := m.CompleteMultipartUpload(ctx, "k", "id", []common.CompletedPart{{PartNumber: 1, ETag: "e"}}); !errors.Is(err, common.ErrBucketNotSet) {
		t.Errorf("Complete = %v", err)
	}
	if err := m.AbortMultipartUpload(ctx, "k", "id"); !errors.Is(err, common.ErrBucketNotSet) {
		t.Errorf("Abort = %v", err)
	}
	if _, err := m.ListMultipartUploads(ctx, ""); !errors.Is(err, common.ErrBucketNotSet) {
		t.Errorf("List = %v", err)
	}
}

// --- buckets ---

func TestS3_CreateBucket_LocationConstraint(t *testing.T) {
	tests := []struct {
		region string
		want   string // empty means no constraint
	}{
		{"", ""},
		{"us-east-1", ""},
		{"eu-west-1", "eu-west-1"},
	}
	for _, tt := range tests {
		t.Run("region "+tt.region, func(t *testing.T) {
			f := newFakeS3()
			s := &S3{svc: f, region: tt.region}
			if err := s.CreateBucket(context.Background(), "bucket-one"); err != nil {
				t.Fatal(err)
			}
			cfg := f.createInput.CreateBucketConfiguration
			switch {
			case tt.want == "" && cfg != nil:
				t.Errorf("constraint set to %v", cfg)
			case tt.want != "" && aws.StringValue(cfg.LocationConstraint) != tt.want:
				t.Errorf("constraint = %+v, want %s", cfg, tt.want)
			}
		})
	}
}

// A create of a bucket the caller owns is answered with success in the default
// region, and resets its ACLs, so the request must not be sent.
func TestS3_CreateBucket_ExistingIsNotCreated(t *testing.T) {
	for _, region := range []string{"", "us-east-1", "eu-west-1"} {
		f := newFakeS3("bucket-one")
		s := &S3{svc: f, region: region}
		if err := s.CreateBucket(context.Background(), "bucket-one"); !errors.Is(err, common.ErrBucketAlreadyExists) {
			t.Errorf("region %q: CreateBucket of an existing bucket = %v", region, err)
		}
		if f.creates != 0 || f.createInput != nil {
			t.Errorf("region %q: CreateBucket was sent for an existing bucket", region)
		}
	}
}

func TestS3_CreateBucket_MissingIsCreated(t *testing.T) {
	f := newFakeS3()
	s := &S3{svc: f}
	if err := s.CreateBucket(context.Background(), "bucket-one"); err != nil {
		t.Fatal(err)
	}
	if f.creates != 1 {
		t.Errorf("CreateBucket requests = %d, want 1", f.creates)
	}
	for _, code := range []string{"NoSuchBucket", "NotFound"} {
		f := newFakeS3()
		f.errs["HeadBucket"] = awsErr(code)
		s := &S3{svc: f}
		if err := s.CreateBucket(context.Background(), "bucket-one"); err != nil {
			t.Errorf("HeadBucket %s: CreateBucket = %v", code, err)
		}
		if f.creates != 1 {
			t.Errorf("HeadBucket %s: CreateBucket requests = %d, want 1", code, f.creates)
		}
	}
}

func TestS3_CreateBucket_HeadFailureIsPassedThrough(t *testing.T) {
	boom := errors.New("boom")
	for _, tt := range []struct {
		name string
		err  error
		want error
	}{
		{"untyped", boom, boom},
		{"forbidden", awsErr("Forbidden"), nil},
	} {
		f := newFakeS3()
		f.errs["HeadBucket"] = tt.err
		s := &S3{svc: f}
		err := s.CreateBucket(context.Background(), "bucket-one")
		if err == nil || (tt.want != nil && !errors.Is(err, tt.want)) {
			t.Errorf("%s: CreateBucket = %v", tt.name, err)
		}
		if tt.want == nil && awsCode(err) != "Forbidden" {
			t.Errorf("%s: code = %q, want the service error unchanged", tt.name, awsCode(err))
		}
		if f.creates != 0 {
			t.Errorf("%s: CreateBucket was sent after a failed look", tt.name)
		}
	}
}

// A bucket made between the look and the create is still reported.
func TestS3_CreateBucket_RaceIsMapped(t *testing.T) {
	f := newFakeS3()
	f.errs["CreateBucket"] = awsErr(s3.ErrCodeBucketAlreadyOwnedByYou)
	s := &S3{svc: f}
	if err := s.CreateBucket(context.Background(), "bucket-one"); !errors.Is(err, common.ErrBucketAlreadyExists) {
		t.Errorf("CreateBucket = %v", err)
	}
}

func TestS3_BucketErrors(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("boom")

	tests := []struct {
		name    string
		op      string
		code    error
		call    func(s *S3) error
		wantErr error
	}{
		{"create: exists", "CreateBucket", awsErr(s3.ErrCodeBucketAlreadyExists), func(s *S3) error { return s.CreateBucket(ctx, "bucket-two") }, common.ErrBucketAlreadyExists},
		{"create: owned", "CreateBucket", awsErr(s3.ErrCodeBucketAlreadyOwnedByYou), func(s *S3) error { return s.CreateBucket(ctx, "bucket-two") }, common.ErrBucketAlreadyExists},
		{"create: service rejects name", "CreateBucket", awsErr("InvalidBucketName"), func(s *S3) error { return s.CreateBucket(ctx, "bucket-two") }, common.ErrInvalidBucketName},
		{"create: other", "CreateBucket", boom, func(s *S3) error { return s.CreateBucket(ctx, "bucket-two") }, boom},
		{"delete: missing", "DeleteBucket", awsErr(s3.ErrCodeNoSuchBucket), func(s *S3) error { return s.DeleteBucket(ctx, "bucket-one") }, common.ErrBucketNotFound},
		{"delete: not empty", "DeleteBucket", awsErr("BucketNotEmpty"), func(s *S3) error { return s.DeleteBucket(ctx, "bucket-one") }, common.ErrBucketNotEmpty},
		{"delete: other", "DeleteBucket", boom, func(s *S3) error { return s.DeleteBucket(ctx, "bucket-one") }, boom},
		{"delete: uploads listing fails", "ListMultipartUploads", boom, func(s *S3) error { return s.DeleteBucket(ctx, "bucket-one") }, boom},
		{"delete: uploads listing, missing", "ListMultipartUploads", awsErr(s3.ErrCodeNoSuchBucket), func(s *S3) error { return s.DeleteBucket(ctx, "bucket-one") }, common.ErrBucketNotFound},
		{"list: fails", "ListBuckets", boom, func(s *S3) error { _, err := s.ListBuckets(ctx); return err }, boom},
		{"bucket: missing", "HeadBucket", awsErr("NotFound"), func(s *S3) error { _, err := s.Bucket(ctx, "bucket-one"); return err }, common.ErrBucketNotFound},
		{"bucket: other", "HeadBucket", boom, func(s *S3) error { _, err := s.Bucket(ctx, "bucket-one"); return err }, boom},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFakeS3("bucket-one")
			f.errs[tt.op] = tt.code
			if err := tt.call(&S3{svc: f}); !errors.Is(err, tt.wantErr) {
				t.Errorf("err = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestS3_BucketNameValidation(t *testing.T) {
	s := &S3{svc: newFakeS3()}
	ctx := context.Background()
	for _, name := range []string{"", "AB", "has_underscore", "-lead", "a/b"} {
		if err := s.CreateBucket(ctx, name); !errors.Is(err, common.ErrInvalidBucketName) {
			t.Errorf("CreateBucket(%q) = %v", name, err)
		}
		if err := s.DeleteBucket(ctx, name); !errors.Is(err, common.ErrInvalidBucketName) {
			t.Errorf("DeleteBucket(%q) = %v", name, err)
		}
		if _, err := s.Bucket(ctx, name); !errors.Is(err, common.ErrInvalidBucketName) {
			t.Errorf("Bucket(%q) = %v", name, err)
		}
	}
}

func TestS3_ListBuckets_SortedWithCreationTime(t *testing.T) {
	s := &S3{svc: newFakeS3("bucket-b", "bucket-a", "bucket-c")}
	infos, err := s.ListBuckets(context.Background())
	if err != nil || len(infos) != 3 {
		t.Fatalf("ListBuckets = %+v, %v", infos, err)
	}
	for i, want := range []string{"bucket-a", "bucket-b", "bucket-c"} {
		if infos[i].Name != want || infos[i].Created.IsZero() {
			t.Errorf("infos[%d] = %+v", i, infos[i])
		}
	}
}

func TestS3_DeleteBucket_WithUploadInProgress(t *testing.T) {
	f := newFakeS3("bucket-one")
	s := &S3{svc: f}
	ctx := context.Background()
	bs, err := s.Bucket(ctx, "bucket-one")
	if err != nil {
		t.Fatal(err)
	}
	id, err := bs.(common.MultipartUploader).InitiateMultipartUpload(ctx, "k", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteBucket(ctx, "bucket-one"); !errors.Is(err, common.ErrBucketNotEmpty) {
		t.Fatalf("delete with an upload = %v", err)
	}
	if err := bs.(common.MultipartUploader).AbortMultipartUpload(ctx, "k", id); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteBucket(ctx, "bucket-one"); err != nil {
		t.Errorf("delete after abort = %v", err)
	}
}

func TestS3_Bucket_SharesClient(t *testing.T) {
	f := newFakeS3("bucket-one")
	m, err := (&S3{svc: f, region: "eu-west-1", multipartExpiry: time.Hour}).Bucket(context.Background(), "bucket-one")
	if err != nil {
		t.Fatal(err)
	}
	got := m.(*S3)
	if got.svc != s3iface.S3API(f) || got.bucket != "bucket-one" || got.region != "eu-west-1" || got.multipartExpiry != time.Hour {
		t.Errorf("bucket storage = %+v", got)
	}
}

// --- multipart ---

func TestS3_UploadPart_BodyIsWrapped(t *testing.T) {
	f := newFakeS3("bucket-one")
	s := &S3{svc: f, bucket: "bucket-one"}
	ctx := context.Background()
	id, _ := s.InitiateMultipartUpload(ctx, "k", nil)
	if _, err := s.UploadPart(ctx, "k", id, 1, strings.NewReader("abc"), 3); err != nil {
		t.Fatal(err)
	}
	if len(f.bodyTypes) != 1 || f.bodyTypes[0] != "aws.ReaderSeekerCloser" {
		t.Errorf("body types = %v, want a wrapped reader", f.bodyTypes)
	}
}

func TestSizedReader(t *testing.T) {
	boom := errors.New("boom")
	tests := []struct {
		name         string
		r            io.Reader
		size         int64
		wantErr      error
		wantMismatch bool
		wantRead     string
	}{
		{"exact", strings.NewReader("abcd"), 4, nil, false, "abcd"},
		{"empty", strings.NewReader(""), 0, nil, false, ""},
		{"short", strings.NewReader("ab"), 4, errPartSize, true, "ab"},
		{"long", strings.NewReader("abcdef"), 4, errPartSize, true, "abcd"},
		{"one byte at a time", iotest.OneByteReader(strings.NewReader("abcd")), 4, nil, false, "abcd"},
		{"reader fails", iotest.ErrReader(boom), 4, boom, false, ""},
		{"reader fails after the data", io.MultiReader(strings.NewReader("abcd"), iotest.ErrReader(boom)), 4, boom, false, "abcd"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sr := &sizedReader{r: tt.r, size: tt.size}
			got, err := io.ReadAll(sr)
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("err = %v, want %v", err, tt.wantErr)
			}
			if string(got) != tt.wantRead {
				t.Errorf("read %q, want %q", got, tt.wantRead)
			}
			if sr.mismatch != tt.wantMismatch {
				t.Errorf("mismatch = %v", sr.mismatch)
			}
			if tt.wantErr == boom && !errors.Is(sr.err, boom) {
				t.Errorf("recorded err = %v", sr.err)
			}
		})
	}
}

func TestS3_UploadPart_ErrorMapping(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("boom")
	tests := []struct {
		name    string
		inject  error
		wantErr error
	}{
		{"no such upload", awsErr(s3.ErrCodeNoSuchUpload), common.ErrUploadNotFound},
		{"too large", awsErr("EntityTooLarge"), common.ErrPartTooLarge},
		{"other", boom, boom},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFakeS3("bucket-one")
			s := &S3{svc: f, bucket: "bucket-one"}
			f.errs["UploadPart"] = tt.inject
			_, err := s.UploadPart(ctx, "k", "id", 1, strings.NewReader("a"), 1)
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("err = %v, want %v", err, tt.wantErr)
			}
		})
	}

	s := &S3{svc: newFakeS3("bucket-one"), bucket: "bucket-one"}
	if _, err := s.UploadPart(ctx, "k", "", 1, strings.NewReader("a"), 1); !errors.Is(err, common.ErrUploadNotFound) {
		t.Errorf("empty upload ID = %v", err)
	}
	if _, err := s.UploadPart(ctx, ".objstore-multipart/x", "id", 1, strings.NewReader("a"), 1); !errors.Is(err, common.ErrInvalidArgument) {
		t.Errorf("reserved key = %v", err)
	}
}

func TestS3_Complete_ErrorMapping(t *testing.T) {
	ctx := context.Background()
	parts := []common.CompletedPart{{PartNumber: 1, ETag: "e"}}
	tests := []struct {
		name    string
		inject  error
		wantErr error
	}{
		{"no such upload", awsErr(s3.ErrCodeNoSuchUpload), common.ErrUploadNotFound},
		{"invalid part", awsErr("InvalidPart"), common.ErrPartMismatch},
		{"invalid order", awsErr("InvalidPartOrder"), common.ErrInvalidPartOrder},
		{"too small", awsErr("EntityTooSmall"), common.ErrPartTooSmall},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFakeS3("bucket-one")
			s := &S3{svc: f, bucket: "bucket-one"}
			f.errs["CompleteMultipartUpload"] = tt.inject
			if err := s.CompleteMultipartUpload(ctx, "k", "id", parts); !errors.Is(err, tt.wantErr) {
				t.Errorf("err = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestS3_Complete_StatelessChecks(t *testing.T) {
	s := &S3{svc: newFakeS3("bucket-one"), bucket: "bucket-one"}
	ctx := context.Background()
	tests := []struct {
		name    string
		key     string
		id      string
		parts   []common.CompletedPart
		wantErr error
	}{
		{"no parts", "k", "id", nil, common.ErrInvalidPartOrder},
		{"part zero", "k", "id", []common.CompletedPart{{PartNumber: 0, ETag: "e"}}, common.ErrInvalidPartNumber},
		{"descending", "k", "id", []common.CompletedPart{{PartNumber: 2, ETag: "e"}, {PartNumber: 1, ETag: "e"}}, common.ErrInvalidPartOrder},
		{"duplicate", "k", "id", []common.CompletedPart{{PartNumber: 1, ETag: "e"}, {PartNumber: 1, ETag: "e"}}, common.ErrInvalidPartOrder},
		{"empty upload", "k", "", []common.CompletedPart{{PartNumber: 1, ETag: "e"}}, common.ErrUploadNotFound},
		{"reserved key", ".objstore-multipart/x", "id", []common.CompletedPart{{PartNumber: 1, ETag: "e"}}, common.ErrInvalidArgument},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := s.CompleteMultipartUpload(ctx, tt.key, tt.id, tt.parts); !errors.Is(err, tt.wantErr) {
				t.Errorf("err = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestS3_Abort_Validation(t *testing.T) {
	s := &S3{svc: newFakeS3("bucket-one"), bucket: "bucket-one"}
	ctx := context.Background()
	if err := s.AbortMultipartUpload(ctx, "k", ""); !errors.Is(err, common.ErrUploadNotFound) {
		t.Errorf("empty upload = %v", err)
	}
	if err := s.AbortMultipartUpload(ctx, ".objstore-multipart/x", "id"); !errors.Is(err, common.ErrInvalidArgument) {
		t.Errorf("reserved key = %v", err)
	}
}

func TestS3_Initiate_Errors(t *testing.T) {
	ctx := context.Background()
	f := newFakeS3("bucket-one")
	s := &S3{svc: f, bucket: "bucket-one"}
	f.errs["CreateMultipartUpload"] = awsErr(s3.ErrCodeNoSuchBucket)
	if _, err := s.InitiateMultipartUpload(ctx, "k", nil); !errors.Is(err, common.ErrBucketNotFound) {
		t.Errorf("missing bucket = %v", err)
	}
	if _, err := s.InitiateMultipartUpload(ctx, "../k", nil); !errors.Is(err, common.ErrInvalidArgument) {
		t.Errorf("bad key = %v", err)
	}
}

func TestS3_ListMultipartUploads_Pagination(t *testing.T) {
	f := newFakeS3("bucket-one")
	f.pageSize = 2
	s := &S3{svc: f, bucket: "bucket-one"}
	ctx := context.Background()
	var want []string
	for _, k := range []string{"d/5", "a/1", "d/4", "a/2", "b/3"} {
		if _, err := s.InitiateMultipartUpload(ctx, k, nil); err != nil {
			t.Fatal(err)
		}
		want = append(want, k)
	}
	sort.Strings(want)
	got, err := s.ListMultipartUploads(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	var keys []string
	for _, u := range got {
		keys = append(keys, u.Key)
	}
	if strings.Join(keys, ",") != strings.Join(want, ",") {
		t.Errorf("keys = %v, want %v", keys, want)
	}

	got, err = s.ListMultipartUploads(ctx, "d/")
	if err != nil || len(got) != 2 {
		t.Errorf("prefix d/ = %+v, %v", got, err)
	}
	f.errs["ListMultipartUploads"] = errors.New("boom")
	if _, err := s.ListMultipartUploads(ctx, ""); err == nil {
		t.Error("listing failure was swallowed")
	}
}

func TestS3_ListMultipartUploads_SortsSameKeyByInitiation(t *testing.T) {
	f := newFakeS3("bucket-one")
	s := &S3{svc: f, bucket: "bucket-one"}
	ctx := context.Background()
	older, _ := s.InitiateMultipartUpload(ctx, "k", nil)
	newer, _ := s.InitiateMultipartUpload(ctx, "k", nil)
	// The fake orders equal keys by ID, so make the later ID the older upload.
	f.buckets["bucket-one"].uploads[newer].initiated = time.Now().Add(-time.Hour)
	got, err := s.ListMultipartUploads(ctx, "")
	if err != nil || len(got) != 2 {
		t.Fatalf("list = %+v, %v", got, err)
	}
	if got[0].UploadID != newer || got[1].UploadID != older {
		t.Errorf("order = %v, %v; want by initiation time", got[0].UploadID, got[1].UploadID)
	}
}

func TestS3_Complete_ExpiredUploadIsAborted(t *testing.T) {
	tests := []struct {
		name    string
		expiry  time.Duration
		age     time.Duration
		wantErr error
	}{
		{"default expiry, young", 0, time.Hour, nil},
		{"default expiry, old", 0, 8 * 24 * time.Hour, common.ErrUploadNotFound},
		{"custom expiry, old", time.Minute, 2 * time.Minute, common.ErrUploadNotFound},
		{"custom expiry, young", time.Hour, 2 * time.Minute, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFakeS3("bucket-one")
			s := &S3{svc: f, bucket: "bucket-one", multipartExpiry: tt.expiry}
			ctx := context.Background()
			id, _ := s.InitiateMultipartUpload(ctx, "k", nil)
			part, err := s.UploadPart(ctx, "k", id, 1, strings.NewReader("tiny"), 4)
			if err != nil {
				t.Fatal(err)
			}
			f.buckets["bucket-one"].uploads[id].initiated = time.Now().Add(-tt.age)

			err = s.CompleteMultipartUpload(ctx, "k", id, []common.CompletedPart{{PartNumber: 1, ETag: part.ETag}})
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			// An expired upload stays abortable.
			if tt.wantErr != nil {
				if err := s.AbortMultipartUpload(ctx, "k", id); err != nil {
					t.Errorf("abort of an expired upload = %v", err)
				}
			}
		})
	}
}

func TestS3_Complete_ExpiryListingFailure(t *testing.T) {
	f := newFakeS3("bucket-one")
	s := &S3{svc: f, bucket: "bucket-one"}
	ctx := context.Background()
	id, _ := s.InitiateMultipartUpload(ctx, "k", nil)
	f.errs["ListMultipartUploads"] = errors.New("boom")
	err := s.CompleteMultipartUpload(ctx, "k", id, []common.CompletedPart{{PartNumber: 1, ETag: "e"}})
	if err == nil || errors.Is(err, common.ErrUploadNotFound) {
		t.Errorf("err = %v, want the listing failure", err)
	}
}

func TestS3_Multipart_CanceledContext(t *testing.T) {
	s := &S3{svc: newFakeS3("bucket-one"), bucket: "bucket-one"}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	parts := []common.CompletedPart{{PartNumber: 1, ETag: "e"}}
	if _, err := s.InitiateMultipartUpload(ctx, "k", nil); !errors.Is(err, context.Canceled) {
		t.Errorf("Initiate = %v", err)
	}
	if _, err := s.UploadPart(ctx, "k", "id", 1, strings.NewReader("a"), 1); !errors.Is(err, context.Canceled) {
		t.Errorf("UploadPart = %v", err)
	}
	if err := s.CompleteMultipartUpload(ctx, "k", "id", parts); !errors.Is(err, context.Canceled) {
		t.Errorf("Complete = %v", err)
	}
	if err := s.AbortMultipartUpload(ctx, "k", "id"); !errors.Is(err, context.Canceled) {
		t.Errorf("Abort = %v", err)
	}
	if _, err := s.ListMultipartUploads(ctx, ""); !errors.Is(err, context.Canceled) {
		t.Errorf("List = %v", err)
	}
	if err := s.CreateBucket(ctx, "bucket-two"); !errors.Is(err, context.Canceled) {
		t.Errorf("CreateBucket = %v", err)
	}
	if err := s.DeleteBucket(ctx, "bucket-one"); !errors.Is(err, context.Canceled) {
		t.Errorf("DeleteBucket = %v", err)
	}
	if _, err := s.ListBuckets(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("ListBuckets = %v", err)
	}
	if _, err := s.Bucket(ctx, "bucket-one"); !errors.Is(err, context.Canceled) {
		t.Errorf("Bucket = %v", err)
	}
}

func TestS3_Initiate_AppliesMetadata(t *testing.T) {
	f := newFakeS3("bucket-one")
	s := &S3{svc: f, bucket: "bucket-one"}
	ctx := context.Background()
	id, err := s.InitiateMultipartUpload(ctx, "k", &common.Metadata{
		ContentType: "text/plain", ContentEncoding: "gzip", Custom: map[string]string{"a": "b"},
	})
	if err != nil {
		t.Fatal(err)
	}
	u := f.buckets["bucket-one"].uploads[id]
	if aws.StringValue(u.ctype) != "text/plain" || aws.StringValue(u.cenc) != "gzip" || aws.StringValue(u.meta["a"]) != "b" {
		t.Errorf("upload = %+v", u)
	}
}
