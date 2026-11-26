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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"testing"
	"testing/iotest"
	"time"

	"github.com/jeremyhahn/go-objstore/internal/multipart"
	"github.com/jeremyhahn/go-objstore/internal/uploadtest"
	"github.com/jeremyhahn/go-objstore/pkg/common"

	"cloud.google.com/go/storage"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
)

// svcObj is an object of the fake service.
// fakeDigest is the opaque 16-byte digest the fake service reports for an
// object; it is derived from SHA-256 so the tests run in FIPS 140-only mode.
func fakeDigest(data []byte) [16]byte {
	full := sha256.Sum256(data)
	var sum [16]byte
	copy(sum[:], full[:16])
	return sum
}

type svcObj struct {
	data    []byte
	size    int64 // reported size when set; lets a test stand in for a large part
	md5     []byte
	ctype   string
	cenc    string
	meta    map[string]string
	created time.Time
	gen     int64     // the generation, new on every write
	updated time.Time // when the object was last written
}

type svcBucketData struct {
	created time.Time
	objects map[string]*svcObj
}

// svcStore is an in-memory service that answers the calls the provider makes
// the way the service does, including its limits: a compose call takes at
// most 32 sources, and a write that is canceled before Close stores nothing.
// errs injects a failure per operation name.
type svcStore struct {
	mu       sync.Mutex
	buckets  map[string]*svcBucketData
	errs     map[string]error
	composes [][]string // the sources of each compose call
	project  string     // the project of the last bucket call
	gens     int64      // the last generation given to a write
}

func newSvcStore(buckets ...string) *svcStore {
	st := &svcStore{buckets: map[string]*svcBucketData{}, errs: map[string]error{}}
	for _, b := range buckets {
		st.buckets[b] = &svcBucketData{created: time.Now(), objects: map[string]*svcObj{}}
	}
	return st
}

func (st *svcStore) client() *svcClient { return &svcClient{st: st} }

func (st *svcStore) object(bucket, name string) (*svcObj, bool) {
	st.mu.Lock()
	defer st.mu.Unlock()
	b, ok := st.buckets[bucket]
	if !ok {
		return nil, false
	}
	o, ok := b.objects[name]
	return o, ok
}

func (st *svcStore) names(bucket string) []string {
	st.mu.Lock()
	defer st.mu.Unlock()
	var out []string
	for n := range st.buckets[bucket].objects {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

func (st *svcStore) setErr(op string, err error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if err == nil {
		delete(st.errs, op)
		return
	}
	st.errs[op] = err
}

// ageUpload makes an upload look as if it was initiated age ago.
func (st *svcStore) ageUpload(t *testing.T, bucket, id string, age time.Duration) {
	t.Helper()
	st.mu.Lock()
	defer st.mu.Unlock()
	o, ok := st.buckets[bucket].objects[uploadDir(id)+manifestName]
	if !ok {
		t.Fatalf("upload %s has no manifest", id)
	}
	var m manifest
	if err := json.Unmarshal(o.data, &m); err != nil {
		t.Fatal(err)
	}
	m.Initiated = time.Now().Add(-age)
	o.data, _ = json.Marshal(m)
}

// putPart stores a part object directly, reporting the minimum part size so
// that a test of many parts need not hold many megabytes.
func (st *svcStore) putPart(bucket, id string, n int, data []byte) string {
	st.mu.Lock()
	defer st.mu.Unlock()
	sum := fakeDigest(data)
	st.buckets[bucket].objects[uploadDir(id)+fmt.Sprint(n)] = &svcObj{
		data: data, size: common.MinPartSize, md5: sum[:], created: time.Now(),
	}
	return hex.EncodeToString(sum[:])
}

// svcClient implements gcsClient and gcsBucketAdmin.
type svcClient struct{ st *svcStore }

func (c *svcClient) Bucket(name string) gcsBucket { return &svcBucket{st: c.st, name: name} }

func (c *svcClient) CreateBucket(ctx context.Context, name, project string) error {
	c.st.mu.Lock()
	defer c.st.mu.Unlock()
	c.st.project = project
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := c.st.errs["CreateBucket"]; err != nil {
		return err
	}
	if _, ok := c.st.buckets[name]; ok {
		return &googleapi.Error{Code: 409}
	}
	c.st.buckets[name] = &svcBucketData{created: time.Now(), objects: map[string]*svcObj{}}
	return nil
}

func (c *svcClient) DeleteBucket(ctx context.Context, name string) error {
	c.st.mu.Lock()
	defer c.st.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := c.st.errs["DeleteBucket"]; err != nil {
		return err
	}
	b, ok := c.st.buckets[name]
	if !ok {
		return &googleapi.Error{Code: 404}
	}
	if len(b.objects) > 0 {
		return &googleapi.Error{Code: 409}
	}
	delete(c.st.buckets, name)
	return nil
}

type svcBucketIter struct {
	items []*storage.BucketAttrs
	err   error
}

func (it *svcBucketIter) Next() (*storage.BucketAttrs, error) {
	if it.err != nil {
		return nil, it.err
	}
	if len(it.items) == 0 {
		return nil, iterator.Done
	}
	a := it.items[0]
	it.items = it.items[1:]
	return a, nil
}

func (c *svcClient) ListBuckets(ctx context.Context, project string) gcsBucketIterator {
	c.st.mu.Lock()
	defer c.st.mu.Unlock()
	c.st.project = project
	if err := ctx.Err(); err != nil {
		return &svcBucketIter{err: err}
	}
	if err := c.st.errs["ListBuckets"]; err != nil {
		return &svcBucketIter{err: err}
	}
	it := &svcBucketIter{}
	for name, b := range c.st.buckets {
		it.items = append(it.items, &storage.BucketAttrs{Name: name, Created: b.created})
	}
	// The service lists in no promised order; the provider must sort.
	sort.Slice(it.items, func(i, j int) bool { return it.items[i].Name > it.items[j].Name })
	return it
}

type svcBucket struct {
	st   *svcStore
	name string
}

func (b *svcBucket) Object(name string) gcsObject {
	return &svcObject{st: b.st, bucket: b.name, name: name}
}

type svcIter struct {
	items []*storage.ObjectAttrs
	err   error
}

func (it *svcIter) Next() (*storage.ObjectAttrs, error) {
	if it.err != nil {
		return nil, it.err
	}
	if len(it.items) == 0 {
		return nil, iterator.Done
	}
	a := it.items[0]
	it.items = it.items[1:]
	return a, nil
}

func attrsOf(bucket, name string, o *svcObj) *storage.ObjectAttrs {
	size := int64(len(o.data))
	if o.size > 0 {
		size = o.size
	}
	return &storage.ObjectAttrs{
		Bucket: bucket, Name: name, Size: size, MD5: o.md5, Created: o.created,
		ContentType: o.ctype, ContentEncoding: o.cenc, Metadata: o.meta,
		Generation: o.gen, Updated: o.updated,
	}
}

func (b *svcBucket) Objects(ctx context.Context, q *storage.Query) gcsIterator {
	b.st.mu.Lock()
	defer b.st.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return &svcIter{err: err}
	}
	if err := b.st.errs["Objects"]; err != nil {
		return &svcIter{err: err}
	}
	data, ok := b.st.buckets[b.name]
	if !ok {
		return &svcIter{err: storage.ErrBucketNotExist}
	}
	var names []string
	for n := range data.objects {
		if strings.HasPrefix(n, q.Prefix) {
			names = append(names, n)
		}
	}
	sort.Strings(names)

	it := &svcIter{}
	seen := map[string]bool{}
	for _, n := range names {
		if q.Delimiter != "" {
			rest := strings.TrimPrefix(n, q.Prefix)
			if i := strings.Index(rest, q.Delimiter); i >= 0 {
				p := q.Prefix + rest[:i+len(q.Delimiter)]
				if !seen[p] {
					seen[p] = true
					it.items = append(it.items, &storage.ObjectAttrs{Prefix: p})
				}
				continue
			}
		}
		it.items = append(it.items, attrsOf(b.name, n, data.objects[n]))
	}
	return it
}

func (b *svcBucket) Attrs(ctx context.Context) (*storage.BucketAttrs, error) {
	b.st.mu.Lock()
	defer b.st.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := b.st.errs["BucketAttrs"]; err != nil {
		return nil, err
	}
	data, ok := b.st.buckets[b.name]
	if !ok {
		return nil, storage.ErrBucketNotExist
	}
	return &storage.BucketAttrs{Name: b.name, Created: data.created}, nil
}

func (b *svcBucket) Update(context.Context, storage.BucketAttrsToUpdate) (*storage.BucketAttrs, error) {
	return nil, errors.New("not used")
}

// Compose implements gcsComposer.
func (b *svcBucket) Compose(ctx context.Context, dst string, sources []string, attrs *storage.ObjectAttrs) error {
	b.st.mu.Lock()
	defer b.st.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := b.st.errs["Compose"]; err != nil {
		return err
	}
	b.st.composes = append(b.st.composes, append([]string(nil), sources...))
	if len(sources) == 0 || len(sources) > composeLimit {
		return &googleapi.Error{Code: 400, Message: "a compose takes 1 to 32 sources"}
	}
	data, ok := b.st.buckets[b.name]
	if !ok {
		return storage.ErrBucketNotExist
	}
	var joined []byte
	for _, s := range sources {
		o, ok := data.objects[s]
		if !ok {
			return storage.ErrObjectNotExist
		}
		joined = append(joined, o.data...)
	}
	o := &svcObj{data: joined, created: time.Now()} // a composite has no MD5
	if attrs != nil {
		o.ctype, o.cenc, o.meta = attrs.ContentType, attrs.ContentEncoding, attrs.Metadata
	}
	data.objects[dst] = o
	return nil
}

type svcObject struct {
	st     *svcStore
	bucket string
	name   string
}

type svcWriter struct {
	ctx    context.Context
	o      *svcObject
	buf    bytes.Buffer
	attrs  *storage.ObjectAttrs // set by NewWriterWithAttrs
	result *storage.ObjectAttrs // the stored object, set by Close
}

func (w *svcWriter) Write(p []byte) (int, error) { return w.buf.Write(p) }

// Close stores the object, unless the context was canceled first.
func (w *svcWriter) Close() error {
	st := w.o.st
	st.mu.Lock()
	defer st.mu.Unlock()
	if err := w.ctx.Err(); err != nil {
		return err
	}
	if err := st.errs["Write"]; err != nil {
		return err
	}
	data, ok := st.buckets[w.o.bucket]
	if !ok {
		return storage.ErrBucketNotExist
	}
	body := append([]byte(nil), w.buf.Bytes()...)
	sum := fakeDigest(body)
	st.gens++
	now := time.Now()
	obj := &svcObj{data: body, md5: sum[:], created: now, gen: st.gens, updated: now}
	if w.attrs != nil {
		obj.ctype, obj.cenc, obj.meta = w.attrs.ContentType, w.attrs.ContentEncoding, w.attrs.Metadata
	}
	data.objects[w.o.name] = obj
	w.result = attrsOf(w.o.bucket, w.o.name, obj)
	return nil
}

// Attrs reports the object the upload stored, as storage.Writer does after
// Close.
func (w *svcWriter) Attrs() *storage.ObjectAttrs { return w.result }

func (o *svcObject) NewWriter(ctx context.Context) io.WriteCloser {
	return &svcWriter{ctx: ctx, o: o}
}

// NewWriterWithAttrs stores the attributes with the data on Close.
func (o *svcObject) NewWriterWithAttrs(ctx context.Context, attrs storage.ObjectAttrs) io.WriteCloser {
	return &svcWriter{ctx: ctx, o: o, attrs: &attrs}
}

func (o *svcObject) get(ctx context.Context, op string) (*svcObj, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := o.st.errs[op]; err != nil {
		return nil, err
	}
	data, ok := o.st.buckets[o.bucket]
	if !ok {
		return nil, storage.ErrBucketNotExist
	}
	obj, ok := data.objects[o.name]
	if !ok {
		return nil, storage.ErrObjectNotExist
	}
	return obj, nil
}

func (o *svcObject) NewReader(ctx context.Context) (io.ReadCloser, error) {
	o.st.mu.Lock()
	defer o.st.mu.Unlock()
	obj, err := o.get(ctx, "Read")
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(append([]byte(nil), obj.data...))), nil
}

func (o *svcObject) Delete(ctx context.Context) error {
	o.st.mu.Lock()
	defer o.st.mu.Unlock()
	if _, err := o.get(ctx, "Delete"); err != nil {
		return err
	}
	delete(o.st.buckets[o.bucket].objects, o.name)
	return nil
}

func (o *svcObject) Attrs(ctx context.Context) (*storage.ObjectAttrs, error) {
	o.st.mu.Lock()
	defer o.st.mu.Unlock()
	obj, err := o.get(ctx, "ObjectAttrs")
	if err != nil {
		return nil, err
	}
	return attrsOf(o.bucket, o.name, obj), nil
}

func (o *svcObject) Update(context.Context, storage.ObjectAttrsToUpdate) (*storage.ObjectAttrs, error) {
	return nil, errors.New("not used")
}

// plainBucket hides the compose capability of a bucket.
type plainBucket struct{ gcsBucket }

// plainClient is a client with neither bucket management nor compose.
type plainClient struct{ inner gcsClient }

func (c plainClient) Bucket(name string) gcsBucket { return plainBucket{c.inner.Bucket(name)} }

// --- conformance ---

func TestGCS_MultipartConformance(t *testing.T) {
	uploadtest.RunMultipart(t, func(t *testing.T) common.Storage {
		return &GCS{client: newSvcStore("bucket-one").client(), bucket: "bucket-one"}
	})
}

func TestGCS_BucketsConformance(t *testing.T) {
	uploadtest.RunBuckets(t, func(t *testing.T) common.BucketManager {
		return &GCS{client: newSvcStore().client(), project: "proj"}
	})
}

// --- configuration ---

func TestGCS_Configure_ProjectAndExpiry(t *testing.T) {
	g := &GCS{}
	if err := g.Configure(map[string]string{
		"bucket": "b", "skip_client": "true", "project": "proj", "multipartExpiry": "2h",
	}, noCreds); err != nil {
		t.Fatal(err)
	}
	if g.project != "proj" || g.multipartExpiry != 2*time.Hour {
		t.Errorf("project = %q, expiry = %v", g.project, g.multipartExpiry)
	}
	for _, bad := range []string{"x", "-1h", "0s"} {
		err := (&GCS{}).Configure(map[string]string{"bucket": "b", "skip_client": "true", "multipartExpiry": bad}, noCreds)
		if !errors.Is(err, common.ErrInvalidArgument) {
			t.Errorf("multipartExpiry %q: err = %v", bad, err)
		}
	}
	if got := (&GCS{}).expiry(); got != multipart.DefaultExpiry {
		t.Errorf("default expiry = %v", got)
	}
}

func TestNewManager(t *testing.T) {
	m, err := NewManager(map[string]string{"project": "proj", "skip_client": "true", "multipartExpiry": "1h"})
	if err != nil {
		t.Fatal(err)
	}
	if m.bucket != "" || m.project != "proj" || m.multipartExpiry != time.Hour {
		t.Errorf("manager = %+v", m)
	}

	if _, err := NewManager(map[string]string{"skip_client": "true"}); !errors.Is(err, common.ErrProjectNotSet) {
		t.Errorf("no project = %v", err)
	}
	if !errors.Is(common.ErrProjectNotSet, common.ErrInvalidArgument) {
		t.Error("ErrProjectNotSet is not an invalid argument")
	}
	if _, err := NewManager(map[string]string{"project": "p", "skip_client": "true", "multipartExpiry": "x"}); !errors.Is(err, common.ErrInvalidArgument) {
		t.Errorf("bad expiry = %v", err)
	}

	boom := errors.New("no credentials")
	restore := gcsNewClient
	gcsNewClient = func(context.Context) (*storage.Client, error) { return nil, boom }
	defer func() { gcsNewClient = restore }()
	if _, err := NewManager(map[string]string{"project": "p"}); !errors.Is(err, boom) {
		t.Errorf("client failure = %v", err)
	}
}

// --- buckets ---

func TestGCS_Buckets_UseTheProject(t *testing.T) {
	st := newSvcStore()
	g := &GCS{client: st.client(), project: "proj"}
	ctx := context.Background()
	if err := g.CreateBucket(ctx, "bucket-one"); err != nil || st.project != "proj" {
		t.Errorf("CreateBucket = %v, project %q", err, st.project)
	}
	st.project = ""
	if _, err := g.ListBuckets(ctx); err != nil || st.project != "proj" {
		t.Errorf("ListBuckets = %v, project %q", err, st.project)
	}
}

func TestGCS_Buckets_RequireProject(t *testing.T) {
	g := &GCS{client: newSvcStore().client()}
	ctx := context.Background()
	if err := g.CreateBucket(ctx, "bucket-one"); !errors.Is(err, common.ErrProjectNotSet) {
		t.Errorf("CreateBucket = %v", err)
	}
	if _, err := g.ListBuckets(ctx); !errors.Is(err, common.ErrProjectNotSet) {
		t.Errorf("ListBuckets = %v", err)
	}
}

func TestGCS_BucketErrors(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("boom")
	tests := []struct {
		name    string
		op      string
		inject  error
		call    func(g *GCS) error
		wantErr error
	}{
		{"create: exists", "CreateBucket", &googleapi.Error{Code: 409}, func(g *GCS) error { return g.CreateBucket(ctx, "bucket-two") }, common.ErrBucketAlreadyExists},
		{"create: service rejects name", "CreateBucket", &googleapi.Error{Code: 400}, func(g *GCS) error { return g.CreateBucket(ctx, "bucket-two") }, common.ErrInvalidBucketName},
		{"create: other", "CreateBucket", boom, func(g *GCS) error { return g.CreateBucket(ctx, "bucket-two") }, boom},
		{"delete: missing", "DeleteBucket", &googleapi.Error{Code: 404}, func(g *GCS) error { return g.DeleteBucket(ctx, "bucket-one") }, common.ErrBucketNotFound},
		{"delete: sentinel missing", "DeleteBucket", storage.ErrBucketNotExist, func(g *GCS) error { return g.DeleteBucket(ctx, "bucket-one") }, common.ErrBucketNotFound},
		{"delete: not empty", "DeleteBucket", &googleapi.Error{Code: 409}, func(g *GCS) error { return g.DeleteBucket(ctx, "bucket-one") }, common.ErrBucketNotEmpty},
		{"delete: other", "DeleteBucket", boom, func(g *GCS) error { return g.DeleteBucket(ctx, "bucket-one") }, boom},
		{"list: fails", "ListBuckets", boom, func(g *GCS) error { _, err := g.ListBuckets(ctx); return err }, boom},
		{"bucket: missing", "BucketAttrs", storage.ErrBucketNotExist, func(g *GCS) error { _, err := g.Bucket(ctx, "bucket-one"); return err }, common.ErrBucketNotFound},
		{"bucket: other", "BucketAttrs", boom, func(g *GCS) error { _, err := g.Bucket(ctx, "bucket-one"); return err }, boom},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := newSvcStore("bucket-one")
			st.setErr(tt.op, tt.inject)
			g := &GCS{client: st.client(), project: "proj"}
			if err := tt.call(g); !errors.Is(err, tt.wantErr) {
				t.Errorf("err = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestGCS_BucketNameValidation(t *testing.T) {
	g := &GCS{client: newSvcStore().client(), project: "proj"}
	ctx := context.Background()
	for _, name := range []string{"", "AB", "has_underscore", "-lead", "a/b"} {
		if err := g.CreateBucket(ctx, name); !errors.Is(err, common.ErrInvalidBucketName) {
			t.Errorf("CreateBucket(%q) = %v", name, err)
		}
		if err := g.DeleteBucket(ctx, name); !errors.Is(err, common.ErrInvalidBucketName) {
			t.Errorf("DeleteBucket(%q) = %v", name, err)
		}
		if _, err := g.Bucket(ctx, name); !errors.Is(err, common.ErrInvalidBucketName) {
			t.Errorf("Bucket(%q) = %v", name, err)
		}
	}
}

func TestGCS_ListBuckets_SortedWithCreationTime(t *testing.T) {
	g := &GCS{client: newSvcStore("bucket-b", "bucket-a", "bucket-c").client(), project: "proj"}
	infos, err := g.ListBuckets(context.Background())
	if err != nil || len(infos) != 3 {
		t.Fatalf("ListBuckets = %+v, %v", infos, err)
	}
	for i, want := range []string{"bucket-a", "bucket-b", "bucket-c"} {
		if infos[i].Name != want || infos[i].Created.IsZero() {
			t.Errorf("infos[%d] = %+v", i, infos[i])
		}
	}
}

func TestGCS_DeleteBucket_WithUploadInProgress(t *testing.T) {
	g := &GCS{client: newSvcStore("bucket-one").client(), project: "proj"}
	ctx := context.Background()
	bs, err := g.Bucket(ctx, "bucket-one")
	if err != nil {
		t.Fatal(err)
	}
	mu := bs.(common.MultipartUploader)
	id, err := mu.InitiateMultipartUpload(ctx, "k", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := g.DeleteBucket(ctx, "bucket-one"); !errors.Is(err, common.ErrBucketNotEmpty) {
		t.Fatalf("delete with an upload = %v", err)
	}
	if err := mu.AbortMultipartUpload(ctx, "k", id); err != nil {
		t.Fatal(err)
	}
	if err := g.DeleteBucket(ctx, "bucket-one"); err != nil {
		t.Errorf("delete after abort = %v", err)
	}
}

func TestGCS_Bucket_SharesClient(t *testing.T) {
	c := newSvcStore("bucket-one").client()
	s, err := (&GCS{client: c, project: "proj", multipartExpiry: time.Hour}).Bucket(context.Background(), "bucket-one")
	if err != nil {
		t.Fatal(err)
	}
	got := s.(*GCS)
	if got.client != gcsClient(c) || got.bucket != "bucket-one" || got.project != "proj" || got.multipartExpiry != time.Hour {
		t.Errorf("bucket storage = %+v", got)
	}
}

func TestGCS_ClientWithoutBucketManagement(t *testing.T) {
	g := &GCS{client: plainClient{newSvcStore("bucket-one").client()}, project: "proj"}
	ctx := context.Background()
	if err := g.CreateBucket(ctx, "bucket-two"); !errors.Is(err, common.ErrNotSupported) {
		t.Errorf("CreateBucket = %v", err)
	}
	if err := g.DeleteBucket(ctx, "bucket-one"); !errors.Is(err, common.ErrNotSupported) {
		t.Errorf("DeleteBucket = %v", err)
	}
	if _, err := g.ListBuckets(ctx); !errors.Is(err, common.ErrNotSupported) {
		t.Errorf("ListBuckets = %v", err)
	}
}

// --- multipart ---

func newUploader(t *testing.T) (*GCS, *svcStore) {
	t.Helper()
	st := newSvcStore("bucket-one")
	return &GCS{client: st.client(), bucket: "bucket-one", project: "proj"}, st
}

func TestGCS_Multipart_ManagerWithoutBucket(t *testing.T) {
	g := &GCS{client: newSvcStore().client(), project: "proj"}
	ctx := context.Background()
	if _, err := g.InitiateMultipartUpload(ctx, "k", nil); !errors.Is(err, common.ErrBucketNotSet) {
		t.Errorf("Initiate = %v", err)
	}
	if _, err := g.UploadPart(ctx, "k", "id", 1, strings.NewReader("x"), 1); !errors.Is(err, common.ErrBucketNotSet) {
		t.Errorf("UploadPart = %v", err)
	}
	if err := g.CompleteMultipartUpload(ctx, "k", "id", []common.CompletedPart{{PartNumber: 1, ETag: "e"}}); !errors.Is(err, common.ErrBucketNotSet) {
		t.Errorf("Complete = %v", err)
	}
	if err := g.AbortMultipartUpload(ctx, "k", "id"); !errors.Is(err, common.ErrBucketNotSet) {
		t.Errorf("Abort = %v", err)
	}
	if _, err := g.ListMultipartUploads(ctx, ""); !errors.Is(err, common.ErrBucketNotSet) {
		t.Errorf("List = %v", err)
	}
}

func TestGCS_Initiate_WritesManifestUnderReservedPrefix(t *testing.T) {
	g, st := newUploader(t)
	ctx := context.Background()
	md := &common.Metadata{ContentType: "text/plain", Custom: map[string]string{"a": "b"}}
	id, err := g.InitiateMultipartUpload(ctx, "dir/key", md)
	if err != nil {
		t.Fatal(err)
	}
	if !multipart.ValidUploadID(id) {
		t.Errorf("upload ID %q", id)
	}
	names := st.names("bucket-one")
	if len(names) != 1 || names[0] != common.MultipartPrefix+id+"/manifest" {
		t.Fatalf("objects = %v", names)
	}
	o, _ := st.object("bucket-one", names[0])
	var m manifest
	if err := json.Unmarshal(o.data, &m); err != nil {
		t.Fatal(err)
	}
	if m.Key != "dir/key" || m.Metadata.ContentType != "text/plain" || m.Metadata.Custom["a"] != "b" || time.Since(m.Initiated) > time.Minute {
		t.Errorf("manifest = %+v", m)
	}
}

func TestGCS_UploadPart_StoresPartObjectWithMD5ETag(t *testing.T) {
	g, st := newUploader(t)
	ctx := context.Background()
	id, _ := g.InitiateMultipartUpload(ctx, "k", nil)
	info, err := g.UploadPart(ctx, "k", id, 3, strings.NewReader("hello"), 5)
	if err != nil {
		t.Fatal(err)
	}
	sum := fakeDigest([]byte("hello"))
	if info.PartNumber != 3 || info.Size != 5 || info.ETag != hex.EncodeToString(sum[:]) {
		t.Errorf("part = %+v", info)
	}
	if o, ok := st.object("bucket-one", common.MultipartPrefix+id+"/3"); !ok || string(o.data) != "hello" {
		t.Errorf("part object = %v, %v", o, ok)
	}
}

func TestGCS_UploadPart_FailedWriteKeepsThePreviousPart(t *testing.T) {
	g, st := newUploader(t)
	ctx := context.Background()
	id, _ := g.InitiateMultipartUpload(ctx, "k", nil)
	old, err := g.UploadPart(ctx, "k", id, 1, strings.NewReader("old"), 3)
	if err != nil {
		t.Fatal(err)
	}
	boom := errors.New("boom")
	if _, err := g.UploadPart(ctx, "k", id, 1, io.MultiReader(strings.NewReader("ne"), iotest.ErrReader(boom)), 3); !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
	if _, err := g.UploadPart(ctx, "k", id, 1, strings.NewReader("too long"), 3); !errors.Is(err, common.ErrInvalidArgument) {
		t.Fatalf("long body = %v", err)
	}
	if o, _ := st.object("bucket-one", common.MultipartPrefix+id+"/1"); string(o.data) != "old" {
		t.Errorf("part object = %q after failed writes", o.data)
	}
	if err := g.CompleteMultipartUpload(ctx, "k", id, []common.CompletedPart{{PartNumber: 1, ETag: old.ETag}}); err != nil {
		t.Errorf("complete = %v", err)
	}
}

func TestGCS_UploadPart_ServiceErrors(t *testing.T) {
	g, st := newUploader(t)
	ctx := context.Background()
	id, _ := g.InitiateMultipartUpload(ctx, "k", nil)

	boom := errors.New("boom")
	st.setErr("Write", boom)
	if _, err := g.UploadPart(ctx, "k", id, 1, strings.NewReader("a"), 1); !errors.Is(err, boom) {
		t.Errorf("write failure = %v", err)
	}
	st.setErr("Write", nil)

	st.setErr("ObjectAttrs", boom)
	if _, err := g.UploadPart(ctx, "k", id, 1, strings.NewReader("a"), 1); !errors.Is(err, boom) {
		t.Errorf("attrs failure = %v", err)
	}
	st.setErr("ObjectAttrs", nil)

	st.setErr("Read", boom)
	if _, err := g.UploadPart(ctx, "k", id, 1, strings.NewReader("a"), 1); !errors.Is(err, boom) {
		t.Errorf("manifest read failure = %v", err)
	}
}

func TestGCS_UploadIDsAreChecked(t *testing.T) {
	g, st := newUploader(t)
	ctx := context.Background()
	id, _ := g.InitiateMultipartUpload(ctx, "k", nil)
	other, _ := g.InitiateMultipartUpload(ctx, "other", nil)
	before := len(st.names("bucket-one"))

	parts := []common.CompletedPart{{PartNumber: 1, ETag: "e"}}
	for _, bad := range []string{"", "../" + id, id + "/1", "not-hex", strings.ToUpper(id)} {
		if _, err := g.UploadPart(ctx, "k", bad, 1, strings.NewReader("a"), 1); !errors.Is(err, common.ErrUploadNotFound) {
			t.Errorf("UploadPart(%q) = %v", bad, err)
		}
		if err := g.CompleteMultipartUpload(ctx, "k", bad, parts); !errors.Is(err, common.ErrUploadNotFound) {
			t.Errorf("Complete(%q) = %v", bad, err)
		}
		if err := g.AbortMultipartUpload(ctx, "k", bad); !errors.Is(err, common.ErrUploadNotFound) {
			t.Errorf("Abort(%q) = %v", bad, err)
		}
	}
	// An upload of another key is not this key's.
	if _, err := g.UploadPart(ctx, "k", other, 1, strings.NewReader("a"), 1); !errors.Is(err, common.ErrUploadNotFound) {
		t.Errorf("UploadPart of another key's upload = %v", err)
	}
	if err := g.AbortMultipartUpload(ctx, "k", other); !errors.Is(err, common.ErrUploadNotFound) {
		t.Errorf("Abort of another key's upload = %v", err)
	}
	if got := len(st.names("bucket-one")); got != before {
		t.Errorf("objects changed from %d to %d", before, got)
	}
}

func TestGCS_Complete_ComposesInOrderAndAppliesMetadata(t *testing.T) {
	g, st := newUploader(t)
	ctx := context.Background()
	md := &common.Metadata{ContentType: "text/plain", ContentEncoding: "gzip", Custom: map[string]string{"a": "b"}}
	id, _ := g.InitiateMultipartUpload(ctx, "dir/key", md)

	big := bytes.Repeat([]byte("x"), common.MinPartSize)
	p1, err := g.UploadPart(ctx, "dir/key", id, 1, bytes.NewReader(big), int64(len(big)))
	if err != nil {
		t.Fatal(err)
	}
	p2, err := g.UploadPart(ctx, "dir/key", id, 2, strings.NewReader("tail"), 4)
	if err != nil {
		t.Fatal(err)
	}
	if err := g.CompleteMultipartUpload(ctx, "dir/key", id, []common.CompletedPart{
		{PartNumber: 1, ETag: p1.ETag}, {PartNumber: 2, ETag: p2.ETag},
	}); err != nil {
		t.Fatal(err)
	}

	o, ok := st.object("bucket-one", "dir/key")
	if !ok || !bytes.Equal(o.data, append(append([]byte(nil), big...), "tail"...)) {
		t.Fatalf("object missing or wrong, ok = %v", ok)
	}
	if o.ctype != "text/plain" || o.cenc != "gzip" || o.meta["a"] != "b" {
		t.Errorf("object attrs = %q %q %v", o.ctype, o.cenc, o.meta)
	}
	if names := st.names("bucket-one"); len(names) != 1 || names[0] != "dir/key" {
		t.Errorf("objects after complete = %v", names)
	}
	if len(st.composes) != 1 {
		t.Errorf("compose calls = %d, want 1", len(st.composes))
	}
}

func TestGCS_Complete_ComposeTree(t *testing.T) {
	tests := []struct {
		parts        int
		wantComposes int
	}{
		{1, 1},
		{32, 1},
		{33, 2},    // 32 + the 33rd part carried over: one intermediate, one final
		{64, 3},    // two intermediates, one final
		{70, 4},    // 32 + 32 + 6, then the final
		{1024, 33}, // 32 intermediates, one final
		{1025, 34}, // 32 intermediates and a lone part, one more intermediate, the final
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%d parts", tt.parts), func(t *testing.T) {
			g, st := newUploader(t)
			ctx := context.Background()
			id, _ := g.InitiateMultipartUpload(ctx, "key", nil)
			var completed []common.CompletedPart
			var want []byte
			for n := 1; n <= tt.parts; n++ {
				data := []byte(fmt.Sprintf("<%d>", n))
				completed = append(completed, common.CompletedPart{PartNumber: n, ETag: st.putPart("bucket-one", id, n, data)})
				want = append(want, data...)
			}
			if err := g.CompleteMultipartUpload(ctx, "key", id, completed); err != nil {
				t.Fatal(err)
			}
			o, ok := st.object("bucket-one", "key")
			if !ok || !bytes.Equal(o.data, want) {
				t.Fatalf("object missing or out of order, ok = %v", ok)
			}
			for _, c := range st.composes {
				if len(c) > composeLimit {
					t.Fatalf("a compose call took %d sources", len(c))
				}
			}
			if len(st.composes) != tt.wantComposes {
				t.Errorf("compose calls = %d, want %d", len(st.composes), tt.wantComposes)
			}
			if names := st.names("bucket-one"); len(names) != 1 {
				t.Errorf("objects left behind: %v", names)
			}
		})
	}
}

func TestGCS_Complete_PartSubsetAndSkippedNumbers(t *testing.T) {
	g, st := newUploader(t)
	ctx := context.Background()
	id, _ := g.InitiateMultipartUpload(ctx, "key", nil)
	e1 := st.putPart("bucket-one", id, 1, []byte("a"))
	st.putPart("bucket-one", id, 2, []byte("skipped"))
	e3 := st.putPart("bucket-one", id, 3, []byte("c"))
	if err := g.CompleteMultipartUpload(ctx, "key", id, []common.CompletedPart{{PartNumber: 1, ETag: e1}, {PartNumber: 3, ETag: e3}}); err != nil {
		t.Fatal(err)
	}
	if o, _ := st.object("bucket-one", "key"); string(o.data) != "ac" {
		t.Errorf("object = %q", o.data)
	}
}

func TestGCS_Complete_IgnoresIntermediateObjectsOfAnEarlierAttempt(t *testing.T) {
	g, st := newUploader(t)
	ctx := context.Background()
	id, _ := g.InitiateMultipartUpload(ctx, "key", nil)
	var completed []common.CompletedPart
	for n := 1; n <= 40; n++ {
		completed = append(completed, common.CompletedPart{PartNumber: n, ETag: st.putPart("bucket-one", id, n, []byte("p"))})
	}
	boom := errors.New("boom")
	st.setErr("Compose", boom)
	if err := g.CompleteMultipartUpload(ctx, "key", id, completed); !errors.Is(err, boom) {
		t.Fatalf("first attempt = %v", err)
	}
	st.setErr("Compose", nil)
	if err := g.CompleteMultipartUpload(ctx, "key", id, completed); err != nil {
		t.Fatalf("second attempt = %v", err)
	}
	if o, _ := st.object("bucket-one", "key"); string(o.data) != strings.Repeat("p", 40) {
		t.Errorf("object = %q", o.data)
	}
}

func TestGCS_Complete_FailureKeepsTheUpload(t *testing.T) {
	g, st := newUploader(t)
	ctx := context.Background()
	id, _ := g.InitiateMultipartUpload(ctx, "key", nil)
	etag := st.putPart("bucket-one", id, 1, []byte("a"))
	parts := []common.CompletedPart{{PartNumber: 1, ETag: etag}}

	boom := errors.New("boom")
	st.setErr("Compose", boom)
	if err := g.CompleteMultipartUpload(ctx, "key", id, parts); !errors.Is(err, boom) {
		t.Fatalf("complete = %v", err)
	}
	if _, ok := st.object("bucket-one", "key"); ok {
		t.Error("object exists after a failed compose")
	}
	st.setErr("Compose", nil)
	if err := g.CompleteMultipartUpload(ctx, "key", id, parts); err != nil {
		t.Errorf("retry = %v", err)
	}
}

func TestGCS_Complete_DeleteFailureLeavesTheUploadForCleanup(t *testing.T) {
	g, st := newUploader(t)
	ctx := context.Background()
	id, _ := g.InitiateMultipartUpload(ctx, "key", nil)
	etag := st.putPart("bucket-one", id, 1, []byte("a"))

	st.setErr("Delete", errors.New("boom"))
	if err := g.CompleteMultipartUpload(ctx, "key", id, []common.CompletedPart{{PartNumber: 1, ETag: etag}}); err != nil {
		t.Fatalf("complete = %v; the object is complete, so cleanup failure must not fail it", err)
	}
	if _, ok := st.object("bucket-one", "key"); !ok {
		t.Fatal("object missing")
	}
	// The manifest goes last, so the upload is still listed and can be aborted.
	got, err := g.ListMultipartUploads(ctx, "")
	if err != nil || len(got) != 1 || got[0].UploadID != id {
		t.Fatalf("uploads = %+v, %v", got, err)
	}
	st.setErr("Delete", nil)
	if err := g.AbortMultipartUpload(ctx, "key", id); err != nil {
		t.Errorf("abort = %v", err)
	}
	if names := st.names("bucket-one"); len(names) != 1 || names[0] != "key" {
		t.Errorf("objects = %v", names)
	}
}

func TestGCS_Complete_WithoutCompose(t *testing.T) {
	st := newSvcStore("bucket-one")
	g := &GCS{client: plainClient{st.client()}, bucket: "bucket-one"}
	ctx := context.Background()
	id, _ := g.InitiateMultipartUpload(ctx, "key", nil)
	etag := st.putPart("bucket-one", id, 1, []byte("a"))
	err := g.CompleteMultipartUpload(ctx, "key", id, []common.CompletedPart{{PartNumber: 1, ETag: etag}})
	if !errors.Is(err, common.ErrNotSupported) {
		t.Errorf("err = %v", err)
	}
}

func TestGCS_Complete_Expired(t *testing.T) {
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
			g, st := newUploader(t)
			g.multipartExpiry = tt.expiry
			ctx := context.Background()
			id, _ := g.InitiateMultipartUpload(ctx, "key", nil)
			part, err := g.UploadPart(ctx, "key", id, 1, strings.NewReader("tiny"), 4)
			if err != nil {
				t.Fatal(err)
			}
			st.ageUpload(t, "bucket-one", id, tt.age)

			err = g.CompleteMultipartUpload(ctx, "key", id, []common.CompletedPart{{PartNumber: 1, ETag: part.ETag}})
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if tt.wantErr != nil {
				if _, ok := st.object("bucket-one", "key"); ok {
					t.Error("an expired upload was completed")
				}
				// The expired upload was dropped, parts and all.
				if names := st.names("bucket-one"); len(names) != 0 {
					t.Errorf("objects left behind: %v", names)
				}
			}
		})
	}
}

func TestGCS_Expired_UploadStaysListedAndAbortable(t *testing.T) {
	g, st := newUploader(t)
	ctx := context.Background()
	id, _ := g.InitiateMultipartUpload(ctx, "key", nil)
	if _, err := g.UploadPart(ctx, "key", id, 1, strings.NewReader("tiny"), 4); err != nil {
		t.Fatal(err)
	}
	st.ageUpload(t, "bucket-one", id, 30*24*time.Hour)

	// The facade's cleanup finds expired uploads by listing them.
	got, err := g.ListMultipartUploads(ctx, "")
	if err != nil || len(got) != 1 || got[0].UploadID != id || time.Since(got[0].Initiated) < 29*24*time.Hour {
		t.Fatalf("uploads = %+v, %v", got, err)
	}
	if _, err := g.UploadPart(ctx, "key", id, 2, strings.NewReader("x"), 1); !errors.Is(err, common.ErrUploadNotFound) {
		t.Errorf("UploadPart on an expired upload = %v", err)
	}
}

func TestGCS_Abort_RemovesEveryObjectOfTheUpload(t *testing.T) {
	g, st := newUploader(t)
	ctx := context.Background()
	if err := g.Put("keep", strings.NewReader("data")); err != nil {
		t.Fatal(err)
	}
	id, _ := g.InitiateMultipartUpload(ctx, "key", nil)
	keep, _ := g.InitiateMultipartUpload(ctx, "other", nil)
	for n := 1; n <= 3; n++ {
		if _, err := g.UploadPart(ctx, "key", id, n, strings.NewReader("p"), 1); err != nil {
			t.Fatal(err)
		}
	}
	if err := g.AbortMultipartUpload(ctx, "key", id); err != nil {
		t.Fatal(err)
	}
	for _, n := range st.names("bucket-one") {
		if strings.Contains(n, id) {
			t.Errorf("object %s survived the abort", n)
		}
	}
	if _, err := g.readManifest(ctx, keep); err != nil {
		t.Errorf("another upload was disturbed: %v", err)
	}
	if err := g.AbortMultipartUpload(ctx, "key", id); !errors.Is(err, common.ErrUploadNotFound) {
		t.Errorf("second abort = %v", err)
	}
}

func TestGCS_Abort_DeleteFailureKeepsTheManifest(t *testing.T) {
	g, st := newUploader(t)
	ctx := context.Background()
	id, _ := g.InitiateMultipartUpload(ctx, "key", nil)
	if _, err := g.UploadPart(ctx, "key", id, 1, strings.NewReader("p"), 1); err != nil {
		t.Fatal(err)
	}
	st.setErr("Delete", errors.New("boom"))
	if err := g.AbortMultipartUpload(ctx, "key", id); err == nil {
		t.Fatal("abort hid a delete failure")
	}
	st.setErr("Delete", nil)
	if err := g.AbortMultipartUpload(ctx, "key", id); err != nil {
		t.Errorf("retry = %v; the upload must stay findable", err)
	}
}

func TestGCS_ListMultipartUploads(t *testing.T) {
	g, st := newUploader(t)
	ctx := context.Background()

	if got, err := g.ListMultipartUploads(ctx, ""); err != nil || got == nil || len(got) != 0 {
		t.Errorf("empty list = %#v, %v", got, err)
	}

	older, _ := g.InitiateMultipartUpload(ctx, "k", nil)
	newer, _ := g.InitiateMultipartUpload(ctx, "k", nil)
	st.ageUpload(t, "bucket-one", older, time.Hour)
	if _, err := g.InitiateMultipartUpload(ctx, "a/1", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := g.UploadPart(ctx, "k", newer, 1, strings.NewReader("p"), 1); err != nil {
		t.Fatal(err)
	}

	// An unreadable manifest and stray objects under the prefix are skipped.
	st.mu.Lock()
	st.buckets["bucket-one"].objects[uploadDir(strings.Repeat("a", 32))+manifestName] = &svcObj{data: []byte("{not json")}
	st.buckets["bucket-one"].objects[common.MultipartPrefix+"stray"] = &svcObj{data: []byte("x")}
	st.mu.Unlock()

	got, err := g.ListMultipartUploads(ctx, "")
	if err != nil || len(got) != 3 {
		t.Fatalf("list = %+v, %v", got, err)
	}
	if got[0].Key != "a/1" || got[1].UploadID != older || got[2].UploadID != newer {
		t.Errorf("order = %+v", got)
	}
	if sub, err := g.ListMultipartUploads(ctx, "a/"); err != nil || len(sub) != 1 {
		t.Errorf("prefix a/ = %+v, %v", sub, err)
	}

	boom := errors.New("boom")
	st.setErr("Objects", boom)
	if _, err := g.ListMultipartUploads(ctx, ""); !errors.Is(err, boom) {
		t.Errorf("listing failure = %v", err)
	}
	st.setErr("Objects", nil)
	st.setErr("Read", boom)
	if _, err := g.ListMultipartUploads(ctx, ""); !errors.Is(err, boom) {
		t.Errorf("manifest read failure = %v", err)
	}
}

func TestGCS_StagedStateIsNeverAnObject(t *testing.T) {
	g, _ := newUploader(t)
	ctx := context.Background()
	if err := g.Put("real", strings.NewReader("data")); err != nil {
		t.Fatal(err)
	}
	id, _ := g.InitiateMultipartUpload(ctx, "pending", nil)
	if _, err := g.UploadPart(ctx, "pending", id, 1, strings.NewReader("p"), 1); err != nil {
		t.Fatal(err)
	}

	if keys, err := g.List(""); err != nil || len(keys) != 1 || keys[0] != "real" {
		t.Errorf("List = %v, %v", keys, err)
	}
	if keys, err := g.ListWithContext(ctx, ""); err != nil || len(keys) != 1 || keys[0] != "real" {
		t.Errorf("ListWithContext = %v, %v", keys, err)
	}
	if keys, err := g.ListWithContext(ctx, ".objstore"); err != nil || len(keys) != 0 {
		t.Errorf("ListWithContext(.objstore) = %v, %v", keys, err)
	}
	for _, delim := range []string{"", "/"} {
		res, err := g.ListWithOptions(ctx, &common.ListOptions{Delimiter: delim})
		if err != nil {
			t.Fatal(err)
		}
		if len(res.Objects) != 1 || res.Objects[0].Key != "real" || len(res.CommonPrefixes) != 0 {
			t.Errorf("ListWithOptions(delimiter %q) = %+v", delim, res)
		}
	}
	if ok, err := g.Exists(ctx, common.MultipartPrefix+id+"/1"); !errors.Is(err, common.ErrInvalidArgument) || ok {
		t.Errorf("Exists of a part = %v, %v", ok, err)
	}
	if _, err := g.Get(common.MultipartPrefix + id + "/1"); !errors.Is(err, common.ErrInvalidArgument) {
		t.Errorf("Get of a part = %v", err)
	}
}

func TestGCS_Multipart_CanceledContext(t *testing.T) {
	g, _ := newUploader(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	parts := []common.CompletedPart{{PartNumber: 1, ETag: "e"}}
	if _, err := g.InitiateMultipartUpload(ctx, "k", nil); !errors.Is(err, context.Canceled) {
		t.Errorf("Initiate = %v", err)
	}
	if _, err := g.UploadPart(ctx, "k", "id", 1, strings.NewReader("a"), 1); !errors.Is(err, context.Canceled) {
		t.Errorf("UploadPart = %v", err)
	}
	if err := g.CompleteMultipartUpload(ctx, "k", "id", parts); !errors.Is(err, context.Canceled) {
		t.Errorf("Complete = %v", err)
	}
	if err := g.AbortMultipartUpload(ctx, "k", "id"); !errors.Is(err, context.Canceled) {
		t.Errorf("Abort = %v", err)
	}
	if _, err := g.ListMultipartUploads(ctx, ""); !errors.Is(err, context.Canceled) {
		t.Errorf("List = %v", err)
	}
	if err := g.CreateBucket(ctx, "bucket-two"); !errors.Is(err, context.Canceled) {
		t.Errorf("CreateBucket = %v", err)
	}
	if err := g.DeleteBucket(ctx, "bucket-one"); !errors.Is(err, context.Canceled) {
		t.Errorf("DeleteBucket = %v", err)
	}
	if _, err := g.ListBuckets(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("ListBuckets = %v", err)
	}
	if _, err := g.Bucket(ctx, "bucket-one"); !errors.Is(err, context.Canceled) {
		t.Errorf("Bucket = %v", err)
	}
}
