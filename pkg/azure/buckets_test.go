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
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"

	"github.com/jeremyhahn/go-objstore/internal/multipart"
	"github.com/jeremyhahn/go-objstore/internal/uploadtest"
	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// svcError is a service error that carries only a code.
type svcError struct{ code azblob.ServiceCodeType }

func (e *svcError) Error() string                       { return string(e.code) }
func (e *svcError) Timeout() bool                       { return false }
func (e *svcError) Temporary() bool                     { return false }
func (e *svcError) Response() *http.Response            { return nil }
func (e *svcError) ServiceCode() azblob.ServiceCodeType { return e.code }

var _ azblob.StorageError = (*svcError)(nil)

type svcObject struct {
	data []byte
	// blocks are the committed blocks of a blob that was committed from a
	// block list.
	blocks   []svcBlock
	modified time.Time
	headers  azblob.BlobHTTPHeaders
	metadata map[string]string
}

type svcBlock struct {
	id   string
	data []byte
}

type svcBucket struct {
	objects map[string]*svcObject
	// staged holds the uncommitted blocks of each blob by block ID.
	staged map[string]map[string][]byte
}

// svcAccount is an in-memory storage account. One lock serves all of it.
type svcAccount struct {
	mu      sync.Mutex
	buckets map[string]*svcBucket
	created map[string]time.Time
	// fail returns an error for the named call, or nil.
	fail func(op string) error
}

func newSvcAccount() *svcAccount {
	return &svcAccount{buckets: map[string]*svcBucket{}, created: map[string]time.Time{}}
}

func (s *svcAccount) failure(op string) error {
	if s.fail == nil {
		return nil
	}
	return s.fail(op)
}

func (s *svcAccount) bucket(name string) (*svcBucket, error) {
	b, ok := s.buckets[name]
	if !ok {
		return nil, &svcError{azblob.ServiceCodeContainerNotFound}
	}
	return b, nil
}

func (s *svcAccount) CreateContainer(_ context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.failure("createContainer"); err != nil {
		return err
	}
	if _, ok := s.buckets[name]; ok {
		return &svcError{azblob.ServiceCodeContainerAlreadyExists}
	}
	s.buckets[name] = &svcBucket{objects: map[string]*svcObject{}, staged: map[string]map[string][]byte{}}
	s.created[name] = time.Now()
	return nil
}

func (s *svcAccount) DeleteContainer(_ context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.failure("deleteContainer"); err != nil {
		return err
	}
	if _, err := s.bucket(name); err != nil {
		return err
	}
	delete(s.buckets, name)
	return nil
}

func (s *svcAccount) ContainerExists(_ context.Context, name string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.failure("containerExists"); err != nil {
		return false, err
	}
	_, ok := s.buckets[name]
	return ok, nil
}

func (s *svcAccount) ContainerHasBlobs(_ context.Context, name string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.failure("containerHasBlobs"); err != nil {
		return false, err
	}
	b, err := s.bucket(name)
	if err != nil {
		return false, err
	}
	return len(b.objects) > 0, nil
}

func (s *svcAccount) ListContainers(_ context.Context) ([]containerInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.failure("listContainers"); err != nil {
		return nil, err
	}
	var out []containerInfo
	for n := range s.buckets {
		out = append(out, containerInfo{Name: n, Created: s.created[n]})
	}
	return out, nil
}

func (s *svcAccount) Container(name string) ContainerAPI {
	return &svcContainer{acct: s, name: name}
}

// names returns the blob names of a container, sorted.
func (s *svcAccount) names(bucket string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	if b, ok := s.buckets[bucket]; ok {
		for n := range b.objects {
			out = append(out, n)
		}
	}
	sort.Strings(out)
	return out
}

type svcContainer struct {
	acct *svcAccount
	name string
}

func (c *svcContainer) NewBlockBlob(name string) BlobAPI {
	return &svcBlob{c: c, name: name}
}

func (c *svcContainer) ListBlobsFlat(_ context.Context, prefix string) ([]string, error) {
	c.acct.mu.Lock()
	defer c.acct.mu.Unlock()
	if err := c.acct.failure("list"); err != nil {
		return nil, err
	}
	b, err := c.acct.bucket(c.name)
	if err != nil {
		return nil, err
	}
	var out []string
	for n := range b.objects {
		if strings.HasPrefix(n, prefix) {
			out = append(out, n)
		}
	}
	sort.Strings(out)
	return out, nil
}

// CopyBlob models a server-side copy: the destination takes the data,
// properties and metadata of the source, replacing whatever it held.
// SourceURL names a blob of the container the way the service would be told
// to read it.
func (c *svcContainer) SourceURL(name string) url.URL {
	return url.URL{Scheme: "https", Host: "svc.test", Path: "/" + c.name + "/" + name}
}

type svcBlob struct {
	c    *svcContainer
	name string
}

// locked runs fn with the container's bucket and the lock held.
func (b *svcBlob) locked(op string, fn func(bk *svcBucket) error) error {
	b.c.acct.mu.Lock()
	defer b.c.acct.mu.Unlock()
	if err := b.c.acct.failure(op); err != nil {
		return err
	}
	bk, err := b.c.acct.bucket(b.c.name)
	if err != nil {
		return err
	}
	return fn(bk)
}

func (b *svcBlob) UploadFromReader(_ context.Context, r io.Reader) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	return b.locked("upload", func(bk *svcBucket) error {
		bk.objects[b.name] = &svcObject{data: data, modified: time.Now()}
		return nil
	})
}

func (b *svcBlob) NewReader(_ context.Context) (io.ReadCloser, error) {
	var data []byte
	err := b.locked("read", func(bk *svcBucket) error {
		o, ok := bk.objects[b.name]
		if !ok {
			return &svcError{azblob.ServiceCodeBlobNotFound}
		}
		data = append([]byte(nil), o.data...)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (b *svcBlob) Delete(_ context.Context) error {
	return b.locked("delete", func(bk *svcBucket) error {
		if _, ok := bk.objects[b.name]; !ok {
			return &svcError{azblob.ServiceCodeBlobNotFound}
		}
		delete(bk.objects, b.name)
		return nil
	})
}

func (b *svcBlob) GetProperties(_ context.Context) (*BlobProperties, error) {
	var props *BlobProperties
	err := b.locked("properties", func(bk *svcBucket) error {
		o, ok := bk.objects[b.name]
		if !ok {
			return &svcError{azblob.ServiceCodeBlobNotFound}
		}
		props = &BlobProperties{
			Size:            int64(len(o.data)),
			ContentType:     o.headers.ContentType,
			ContentEncoding: o.headers.ContentEncoding,
			LastModified:    o.modified,
			ETag:            fmt.Sprintf("%x", len(o.data)),
			Metadata:        o.metadata,
		}
		return nil
	})
	return props, err
}

func (b *svcBlob) SetMetadata(_ context.Context, metadata map[string]string) error {
	return b.locked("metadata", func(bk *svcBucket) error {
		o, ok := bk.objects[b.name]
		if !ok {
			return &svcError{azblob.ServiceCodeBlobNotFound}
		}
		o.metadata = metadata
		return nil
	})
}

func (b *svcBlob) SetHTTPHeaders(_ context.Context, headers azblob.BlobHTTPHeaders) error {
	return b.locked("headers", func(bk *svcBucket) error {
		o, ok := bk.objects[b.name]
		if !ok {
			return &svcError{azblob.ServiceCodeBlobNotFound}
		}
		o.headers = headers
		return nil
	})
}

func (b *svcBlob) StageBlock(_ context.Context, blockID string, body io.ReadSeeker) error {
	// Like the transport: learn the length, rewind, send.
	if _, err := body.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	if _, err := body.Seek(0, io.SeekStart); err != nil {
		return err
	}
	data, err := io.ReadAll(body)
	if err != nil {
		return err
	}
	return b.locked("stage", func(bk *svcBucket) error {
		if bk.staged[b.name] == nil {
			bk.staged[b.name] = map[string][]byte{}
		}
		bk.staged[b.name][blockID] = data
		return nil
	})
}

func (b *svcBlob) CommitBlockList(_ context.Context, ids []string, headers azblob.BlobHTTPHeaders, metadata map[string]string) error {
	return b.locked("commit", func(bk *svcBucket) error {
		// Like the service, a listed ID is the staged block or else the
		// committed one.
		var data []byte
		var blocks []svcBlock
		for _, id := range ids {
			blk, ok := bk.staged[b.name][id]
			if !ok {
				if old := bk.objects[b.name]; old != nil {
					for _, c := range old.blocks {
						if c.id == id {
							blk, ok = c.data, true
						}
					}
				}
			}
			if !ok {
				return &svcError{"InvalidBlockList"}
			}
			data = append(data, blk...)
			blocks = append(blocks, svcBlock{id: id, data: blk})
		}
		bk.objects[b.name] = &svcObject{data: data, blocks: blocks, headers: headers, metadata: metadata, modified: time.Now()}
		delete(bk.staged, b.name) // blocks left out of the list are discarded
		return nil
	})
}

// StageBlockFromURL reads a range of a committed blob of the same container.
func (b *svcBlob) StageBlockFromURL(_ context.Context, blockID string, source url.URL, offset, count int64) error {
	return b.locked("stageurl", func(bk *svcBucket) error {
		name := strings.TrimPrefix(source.Path, "/"+b.c.name+"/")
		src, ok := bk.objects[name]
		if !ok {
			return &svcError{azblob.ServiceCodeBlobNotFound}
		}
		if offset < 0 || count < 0 || offset+count > int64(len(src.data)) {
			return &svcError{"InvalidRange"}
		}
		if bk.staged[b.name] == nil {
			bk.staged[b.name] = map[string][]byte{}
		}
		bk.staged[b.name][blockID] = append([]byte(nil), src.data[offset:offset+count]...)
		return nil
	})
}

func (b *svcBlob) CommittedBlocks(_ context.Context) ([]blockSize, error) {
	var out []blockSize
	err := b.locked("blocks", func(bk *svcBucket) error {
		if obj := bk.objects[b.name]; obj != nil {
			for _, blk := range obj.blocks {
				out = append(out, blockSize{ID: blk.id, Size: int64(len(blk.data))})
			}
		}
		return nil
	})
	return out, err
}

func (b *svcBlob) UncommittedBlocks(_ context.Context) (map[string]int64, error) {
	out := map[string]int64{}
	err := b.locked("blocks", func(bk *svcBucket) error {
		for id, blk := range bk.staged[b.name] {
			out[id] = int64(len(blk))
		}
		return nil
	})
	return out, err
}

const svcTestBucket = "data"

// newSvcAzure returns a backend bound to a fresh container of a fake account.
func newSvcAzure(t *testing.T) (*Azure, *svcAccount) {
	t.Helper()
	acct := newSvcAccount()
	if err := acct.CreateContainer(context.Background(), svcTestBucket); err != nil {
		t.Fatal(err)
	}
	return &Azure{account: acct, container: acct.Container(svcTestBucket), containerName: svcTestBucket}, acct
}

func TestAzureMultipartConformance(t *testing.T) {
	uploadtest.RunMultipart(t, func(t *testing.T) common.Storage {
		a, _ := newSvcAzure(t)
		return a
	})
}

func TestAzureBucketsConformance(t *testing.T) {
	uploadtest.RunBuckets(t, func(t *testing.T) common.BucketManager {
		return &Azure{account: newSvcAccount()}
	})
}

func TestAzureBucketErrorMapping(t *testing.T) {
	ctx := context.Background()
	acct := newSvcAccount()
	a := &Azure{account: acct}

	if err := a.CreateBucket(ctx, "one"); err != nil {
		t.Fatal(err)
	}
	if err := a.CreateBucket(ctx, "one"); !errors.Is(err, common.ErrBucketAlreadyExists) {
		t.Errorf("create twice = %v", err)
	}
	if err := a.DeleteBucket(ctx, "missing"); !errors.Is(err, common.ErrBucketNotFound) {
		t.Errorf("delete missing = %v", err)
	}
	if _, err := a.Bucket(ctx, "missing"); !errors.Is(err, common.ErrBucketNotFound) {
		t.Errorf("bucket missing = %v", err)
	}
	if err := a.CreateBucket(ctx, "UPPER"); !errors.Is(err, common.ErrInvalidBucketName) {
		t.Errorf("invalid name = %v", err)
	}

	for _, code := range []azblob.ServiceCodeType{azblob.ServiceCodeInvalidResourceName, azblob.ServiceCodeOutOfRangeInput} {
		acct.fail = func(op string) error { return &svcError{code} }
		if err := a.CreateBucket(ctx, "rejected"); !errors.Is(err, common.ErrInvalidBucketName) {
			t.Errorf("service code %s = %v", code, err)
		}
	}

	boom := errors.New("boom")
	acct.fail = func(op string) error { return boom }
	if err := a.CreateBucket(ctx, "other"); !errors.Is(err, boom) {
		t.Errorf("create failure = %v", err)
	}
	if err := a.DeleteBucket(ctx, "one"); !errors.Is(err, boom) {
		t.Errorf("delete failure = %v", err)
	}
	if _, err := a.ListBuckets(ctx); !errors.Is(err, boom) {
		t.Errorf("list failure = %v", err)
	}
	if _, err := a.Bucket(ctx, "one"); !errors.Is(err, boom) {
		t.Errorf("bucket failure = %v", err)
	}
	acct.fail = nil

	// A backend given only a container cannot reach the account.
	only := &Azure{container: acct.Container("one")}
	if err := only.CreateBucket(ctx, "two"); !errors.Is(err, common.ErrNotSupported) {
		t.Errorf("no account = %v", err)
	}
}

func TestAzureDeleteBucketListsFirst(t *testing.T) {
	ctx := context.Background()
	a, acct := newSvcAzure(t)

	if err := a.Put("k", strings.NewReader("v")); err != nil {
		t.Fatal(err)
	}
	if err := a.DeleteBucket(ctx, svcTestBucket); !errors.Is(err, common.ErrBucketNotEmpty) {
		t.Fatalf("delete with an object = %v", err)
	}
	if err := a.Delete("k"); err != nil {
		t.Fatal(err)
	}

	// An upload in progress is content too.
	id, err := a.InitiateMultipartUpload(ctx, "big", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.DeleteBucket(ctx, svcTestBucket); !errors.Is(err, common.ErrBucketNotEmpty) {
		t.Fatalf("delete with an upload = %v", err)
	}
	if err := a.AbortMultipartUpload(ctx, "big", id); err != nil {
		t.Fatal(err)
	}
	if err := a.DeleteBucket(ctx, svcTestBucket); err != nil {
		t.Fatalf("delete empty = %v", err)
	}
	if len(acct.buckets) != 0 {
		t.Error("the container is still there")
	}
}

func TestAzureNewManagerSettings(t *testing.T) {
	settings := map[string]string{"accountName": "acct", "accountKey": base64.StdEncoding.EncodeToString([]byte("key"))}

	m, err := NewManager(splitKey(t, settings))
	if err != nil {
		t.Fatalf("NewManager without container = %v", err)
	}
	if m.account == nil || m.container != nil {
		t.Errorf("account %v, container %v", m.account, m.container)
	}
	if err := m.requireContainer(); !errors.Is(err, common.ErrBucketNotSet) {
		t.Errorf("manager without container = %v", err)
	}
	if _, err := m.InitiateMultipartUpload(context.Background(), "k", nil); !errors.Is(err, common.ErrBucketNotSet) {
		t.Errorf("initiate on a manager = %v", err)
	}

	if err := (&Azure{}).Configure(splitKey(t, settings)); !errors.Is(err, common.ErrAccountNotSet) {
		t.Errorf("Configure without container = %v", err)
	}
	if _, err := NewManager(map[string]string{"accountName": "acct"}, noCreds); !errors.Is(err, common.ErrAccountNotSet) {
		t.Errorf("NewManager without key = %v", err)
	}

	withExpiry := map[string]string{"containerName": "c", "multipartExpiry": "2h"}
	for k, v := range settings {
		withExpiry[k] = v
	}
	a := &Azure{}
	if err := a.Configure(splitKey(t, withExpiry)); err != nil {
		t.Fatal(err)
	}
	if a.multipartExpiry != 2*time.Hour || a.container == nil {
		t.Errorf("expiry %v, container %v", a.multipartExpiry, a.container)
	}
	withExpiry["multipartExpiry"] = "soon"
	if err := (&Azure{}).Configure(splitKey(t, withExpiry)); !errors.Is(err, common.ErrInvalidArgument) {
		t.Errorf("bad expiry = %v", err)
	}
	if _, err := NewManager(splitKey(t, withExpiry)); !errors.Is(err, common.ErrInvalidArgument) {
		t.Errorf("bad expiry on a manager = %v", err)
	}

	if got := (&Azure{}).expiry(); got != multipart.DefaultExpiry {
		t.Errorf("default expiry = %v", got)
	}
}

func TestAzureNotConfigured(t *testing.T) {
	ctx := context.Background()
	a := &Azure{}
	if _, err := a.InitiateMultipartUpload(ctx, "k", nil); !errors.Is(err, common.ErrNotConfigured) {
		t.Errorf("initiate = %v", err)
	}
	if _, err := a.ListMultipartUploads(ctx, ""); !errors.Is(err, common.ErrNotConfigured) {
		t.Errorf("list = %v", err)
	}
}

func TestAzureBlockID(t *testing.T) {
	id, err := multipart.NewUploadID()
	if err != nil {
		t.Fatal(err)
	}
	first, last := blockID(id, 1), blockID(id, common.MaxParts)
	if len(first) != len(last) {
		t.Errorf("block IDs differ in length: %d and %d", len(first), len(last))
	}
	raw, err := base64.StdEncoding.DecodeString(first)
	if err != nil || string(raw) != id+"/00001" {
		t.Errorf("block ID %q decodes to %q, %v", first, raw, err)
	}
}

func TestAzureUploadWritesNoObjectsVisibleToList(t *testing.T) {
	ctx := context.Background()
	a, acct := newSvcAzure(t)
	if err := a.Put("visible", strings.NewReader("v")); err != nil {
		t.Fatal(err)
	}
	id, err := a.InitiateMultipartUpload(ctx, "pending", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.UploadPart(ctx, "pending", id, 1, strings.NewReader("part"), 4); err != nil {
		t.Fatal(err)
	}
	if n := len(acct.names(svcTestBucket)); n < 3 {
		t.Fatalf("expected the manifest and a part record next to the object, got %d blobs", n)
	}

	check := func(what string, keys []string, err error) {
		t.Helper()
		if err != nil {
			t.Errorf("%s: %v", what, err)
			return
		}
		if len(keys) != 1 || keys[0] != "visible" {
			t.Errorf("%s = %v", what, keys)
		}
	}
	keys, err := a.List("")
	check("List", keys, err)
	keys, err = a.ListWithContext(ctx, "")
	check("ListWithContext", keys, err)
	res, err := a.ListWithOptions(ctx, &common.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, o := range res.Objects {
		got = append(got, o.Key)
	}
	check("ListWithOptions", got, nil)

	for _, key := range []string{common.MultipartPrefix + id + "/manifest", common.MultipartPrefix} {
		if ok, err := a.Exists(ctx, key); err == nil && ok {
			t.Errorf("Exists(%q) = true", key)
		}
		if _, err := a.Get(key); err == nil {
			t.Errorf("Get(%q) succeeded", key)
		}
	}
}

func TestAzureCompleteLeavesNoState(t *testing.T) {
	ctx := context.Background()
	a, acct := newSvcAzure(t)
	md := &common.Metadata{ContentType: "text/plain", Custom: map[string]string{"k": "v"}}
	id, err := a.InitiateMultipartUpload(ctx, "obj", md)
	if err != nil {
		t.Fatal(err)
	}
	big := bytes.Repeat([]byte("a"), common.MinPartSize)
	p1, err := a.UploadPart(ctx, "obj", id, 1, bytes.NewReader(big), int64(len(big)))
	if err != nil {
		t.Fatal(err)
	}
	p2, err := a.UploadPart(ctx, "obj", id, 2, strings.NewReader("tail"), 4)
	if err != nil {
		t.Fatal(err)
	}
	err = a.CompleteMultipartUpload(ctx, "obj", id, []common.CompletedPart{
		{PartNumber: 1, ETag: p1.ETag}, {PartNumber: 2, ETag: p2.ETag},
	})
	if err != nil {
		t.Fatal(err)
	}
	if names := acct.names(svcTestBucket); len(names) != 1 || names[0] != "obj" {
		t.Errorf("blobs after complete = %v", names)
	}
	got, err := a.GetMetadata(ctx, "obj")
	if err != nil || got.Size != int64(len(big)+4) || got.ContentType != "text/plain" || got.Custom["k"] != "v" {
		t.Errorf("metadata = %+v, %v", got, err)
	}
	if err := a.CompleteMultipartUpload(ctx, "obj", id, nil); !errors.Is(err, common.ErrUploadNotFound) {
		t.Errorf("complete again = %v", err)
	}
}

func TestAzureFailedPartIsNotUploaded(t *testing.T) {
	ctx := context.Background()
	a, acct := newSvcAzure(t)
	id, err := a.InitiateMultipartUpload(ctx, "obj", nil)
	if err != nil {
		t.Fatal(err)
	}
	old, err := a.UploadPart(ctx, "obj", id, 1, strings.NewReader("old"), 3)
	if err != nil {
		t.Fatal(err)
	}

	boom := errors.New("boom")
	acct.fail = func(op string) error {
		if op == "stage" {
			return boom
		}
		return nil
	}
	if _, err := a.UploadPart(ctx, "obj", id, 1, strings.NewReader("new"), 3); !errors.Is(err, boom) {
		t.Fatalf("failing upload = %v", err)
	}
	acct.fail = nil

	// The replaced part is gone rather than stale.
	err = a.CompleteMultipartUpload(ctx, "obj", id, []common.CompletedPart{{PartNumber: 1, ETag: old.ETag}})
	if !errors.Is(err, common.ErrPartMismatch) {
		t.Errorf("complete with the failed part = %v", err)
	}

	// A part that was never recorded, though its block is staged.
	acct.fail = func(op string) error {
		if op == "upload" {
			return boom
		}
		return nil
	}
	if _, err := a.UploadPart(ctx, "obj", id, 2, strings.NewReader("x"), 1); !errors.Is(err, boom) {
		t.Fatalf("unrecorded part = %v", err)
	}
	acct.fail = nil
	err = a.CompleteMultipartUpload(ctx, "obj", id, []common.CompletedPart{{PartNumber: 2, ETag: "x"}})
	if !errors.Is(err, common.ErrPartMismatch) {
		t.Errorf("complete with the unrecorded part = %v", err)
	}
}

func TestAzureLostBlockIsAMismatch(t *testing.T) {
	ctx := context.Background()
	a, acct := newSvcAzure(t)
	id, err := a.InitiateMultipartUpload(ctx, "obj", nil)
	if err != nil {
		t.Fatal(err)
	}
	p, err := a.UploadPart(ctx, "obj", id, 1, strings.NewReader("data"), 4)
	if err != nil {
		t.Fatal(err)
	}
	acct.mu.Lock()
	delete(acct.buckets[svcTestBucket].staged, stagingBlob(id)) // the service discarded the block
	acct.mu.Unlock()
	err = a.CompleteMultipartUpload(ctx, "obj", id, []common.CompletedPart{{PartNumber: 1, ETag: p.ETag}})
	if !errors.Is(err, common.ErrPartMismatch) {
		t.Errorf("complete without the block = %v", err)
	}
}

func TestAzureCompleteChecksPartSize(t *testing.T) {
	ctx := context.Background()
	a, _ := newSvcAzure(t)
	id, err := a.InitiateMultipartUpload(ctx, "obj", nil)
	if err != nil {
		t.Fatal(err)
	}
	p1, _ := a.UploadPart(ctx, "obj", id, 1, strings.NewReader("small"), 5)
	p2, _ := a.UploadPart(ctx, "obj", id, 2, strings.NewReader("last"), 4)
	err = a.CompleteMultipartUpload(ctx, "obj", id, []common.CompletedPart{
		{PartNumber: 1, ETag: p1.ETag}, {PartNumber: 2, ETag: p2.ETag},
	})
	if !errors.Is(err, common.ErrPartTooSmall) {
		t.Errorf("complete with a small first part = %v", err)
	}
}

func TestAzurePartBodyMustMatchSize(t *testing.T) {
	ctx := context.Background()
	a, _ := newSvcAzure(t)
	id, err := a.InitiateMultipartUpload(ctx, "obj", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.UploadPart(ctx, "obj", id, 1, strings.NewReader("ab"), 5); !errors.Is(err, common.ErrInvalidArgument) {
		t.Errorf("short body = %v", err)
	}
	if _, err := a.UploadPart(ctx, "obj", id, 1, strings.NewReader("abcdef"), 5); !errors.Is(err, common.ErrInvalidArgument) {
		t.Errorf("long body = %v", err)
	}
	if _, err := a.UploadPart(ctx, "obj", id, 1, strings.NewReader(""), -1); !errors.Is(err, common.ErrInvalidArgument) {
		t.Errorf("negative size = %v", err)
	}
	if _, err := a.UploadPart(ctx, "obj", id, 1, strings.NewReader(""), maxBlockSize+1); !errors.Is(err, common.ErrPartTooLarge) {
		t.Errorf("oversized part = %v", err)
	}
	broken := io.MultiReader(strings.NewReader("ab"), failingReader{})
	if _, err := a.UploadPart(ctx, "obj", id, 1, broken, 5); !errors.Is(err, errReadFailure) {
		t.Errorf("failing source = %v", err)
	}
}

var errReadFailure = errors.New("read failure")

type failingReader struct{}

func (failingReader) Read([]byte) (int, error) { return 0, errReadFailure }

func TestAzurePartBodySeek(t *testing.T) {
	p := &partBody{r: strings.NewReader("abcd"), size: 4, h: sha256.New()}
	if n, err := p.Seek(0, io.SeekEnd); n != 4 || err != nil {
		t.Errorf("seek end = %d, %v", n, err)
	}
	if n, err := p.Seek(0, io.SeekStart); n != 0 || err != nil {
		t.Errorf("seek start = %d, %v", n, err)
	}
	if _, err := p.Seek(2, io.SeekStart); err == nil {
		t.Error("seek into the middle succeeded")
	}
	buf := make([]byte, 2)
	if _, err := io.ReadFull(p, buf); err != nil {
		t.Fatal(err)
	}
	if n, err := p.Seek(0, io.SeekCurrent); n != 2 || err != nil {
		t.Errorf("seek current = %d, %v", n, err)
	}
	if _, err := p.Seek(0, io.SeekStart); err == nil {
		t.Error("rewind after a read succeeded")
	}
}

func TestAzureExpiredUploadCountsAsAborted(t *testing.T) {
	ctx := context.Background()
	a, acct := newSvcAzure(t)
	a.multipartExpiry = 20 * time.Millisecond
	id, err := a.InitiateMultipartUpload(ctx, "obj", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.UploadPart(ctx, "obj", id, 1, strings.NewReader("p"), 1); err != nil {
		t.Fatal(err)
	}
	time.Sleep(40 * time.Millisecond)

	// The cleanup still finds it, and can abort it.
	list, err := a.ListMultipartUploads(ctx, "")
	if err != nil || len(list) != 1 || list[0].UploadID != id {
		t.Fatalf("list = %v, %v", list, err)
	}
	if _, err := a.UploadPart(ctx, "obj", id, 2, strings.NewReader("p"), 1); !errors.Is(err, common.ErrUploadNotFound) {
		t.Errorf("upload to an expired upload = %v", err)
	}
	if names := acct.names(svcTestBucket); len(names) != 0 {
		t.Errorf("an expired upload left %v", names)
	}

	id, err = a.InitiateMultipartUpload(ctx, "obj", nil)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(40 * time.Millisecond)
	if err := a.AbortMultipartUpload(ctx, "obj", id); err != nil {
		t.Errorf("abort an expired upload = %v", err)
	}
	if err := a.CompleteMultipartUpload(ctx, "obj", id, nil); !errors.Is(err, common.ErrUploadNotFound) {
		t.Errorf("complete an aborted upload = %v", err)
	}
}

func TestAzureUnreadableManifest(t *testing.T) {
	ctx := context.Background()
	a, _ := newSvcAzure(t)
	id, err := multipart.NewUploadID()
	if err != nil {
		t.Fatal(err)
	}
	if err := a.container.NewBlockBlob(uploadDir(id)+manifestName).UploadFromReader(ctx, strings.NewReader("{")); err != nil {
		t.Fatal(err)
	}
	if _, err := a.UploadPart(ctx, "obj", id, 1, strings.NewReader("p"), 1); !errors.Is(err, common.ErrUploadNotFound) {
		t.Errorf("garbled manifest = %v", err)
	}
	if list, err := a.ListMultipartUploads(ctx, ""); err != nil || len(list) != 0 {
		t.Errorf("list = %v, %v", list, err)
	}
}

func TestAzureListMultipartUploadsFiltersAndSorts(t *testing.T) {
	ctx := context.Background()
	a, _ := newSvcAzure(t)
	for _, key := range []string{"b/2", "a/1", "b/1"} {
		if _, err := a.InitiateMultipartUpload(ctx, key, nil); err != nil {
			t.Fatal(err)
		}
	}
	list, err := a.ListMultipartUploads(ctx, "b/")
	if err != nil || len(list) != 2 || list[0].Key != "b/1" || list[1].Key != "b/2" {
		t.Errorf("list = %v, %v", list, err)
	}
}

func TestAzureBlobWithoutBlocks(t *testing.T) {
	ctx := context.Background()
	a, acct := newSvcAzure(t)
	a.container = plainContainer{acct.Container(svcTestBucket)}
	id, err := a.InitiateMultipartUpload(ctx, "obj", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.UploadPart(ctx, "obj", id, 1, strings.NewReader("p"), 1); !errors.Is(err, common.ErrNotSupported) {
		t.Errorf("upload without block support = %v", err)
	}
}

// plainContainer hides the block operations of its blobs.
type plainContainer struct{ ContainerAPI }

func (c plainContainer) NewBlockBlob(name string) BlobAPI {
	return struct{ BlobAPI }{c.ContainerAPI.NewBlockBlob(name)}
}

func TestAzureBucketBindsToContainer(t *testing.T) {
	ctx := context.Background()
	a := &Azure{account: newSvcAccount(), multipartExpiry: time.Hour}
	for _, n := range []string{"bbb", "aaa"} {
		if err := a.CreateBucket(ctx, n); err != nil {
			t.Fatal(err)
		}
	}
	infos, err := a.ListBuckets(ctx)
	if err != nil || len(infos) != 2 || infos[0].Name != "aaa" {
		t.Fatalf("buckets = %v, %v", infos, err)
	}
	s, err := a.Bucket(ctx, "aaa")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Put("k", strings.NewReader("v")); err != nil {
		t.Fatal(err)
	}
	other, err := a.Bucket(ctx, "bbb")
	if err != nil {
		t.Fatal(err)
	}
	if keys, err := other.List(""); err != nil || len(keys) != 0 {
		t.Errorf("other bucket = %v, %v", keys, err)
	}
	if bound, ok := s.(*Azure); !ok || bound.multipartExpiry != time.Hour {
		t.Errorf("the bound storage lost the expiry: %v", s)
	}
}

// newServiceServer starts a fake blob endpoint and returns the account-level
// client of the SDK pointed at it.
func newServiceServer(t *testing.T, handler http.HandlerFunc) serviceWrapper {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	p := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{
		Retry: azblob.RetryOptions{MaxTries: 1},
	})
	return serviceWrapper{azblob.NewServiceURL(*u, p)}
}

func writeCode(w http.ResponseWriter, status int, code string) {
	w.Header().Set("x-ms-error-code", code)
	w.WriteHeader(status)
}

func TestAzureServiceWrapper(t *testing.T) {
	ctx := context.Background()
	var mu sync.Mutex
	var seen []string
	svc := newServiceServer(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = append(seen, r.Method+" "+r.URL.Path+"?"+r.URL.RawQuery)
		mu.Unlock()
		q := r.URL.Query()
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/" && q.Get("comp") == "list":
			w.Header().Set("Content-Type", "application/xml")
			io.WriteString(w, `<?xml version="1.0"?><EnumerationResults><Containers>`+ //nolint:errcheck // test server
				`<Container><Name>one</Name><Properties><Last-Modified>Mon, 02 Jan 2006 15:04:05 GMT</Last-Modified></Properties></Container>`+
				`</Containers><NextMarker/></EnumerationResults>`)
		case r.Method == http.MethodPut && r.URL.Path == "/dup":
			writeCode(w, http.StatusConflict, "ContainerAlreadyExists")
		case r.Method == http.MethodPut && q.Get("restype") == "container":
			w.WriteHeader(http.StatusCreated)
		case r.Method == http.MethodDelete && r.URL.Path == "/gone":
			writeCode(w, http.StatusNotFound, "ContainerNotFound")
		case r.Method == http.MethodDelete:
			w.WriteHeader(http.StatusAccepted)
		case r.URL.Path == "/gone" && q.Get("comp") != "list":
			writeCode(w, http.StatusNotFound, "ContainerNotFound")
		case r.Method == http.MethodGet && q.Get("comp") == "list" && r.URL.Path == "/full":
			w.Header().Set("Content-Type", "application/xml")
			io.WriteString(w, `<?xml version="1.0"?><EnumerationResults ContainerName="full"><Blobs>`+ //nolint:errcheck // test server
				`<Blob><Name>x</Name><Properties/></Blob></Blobs><NextMarker/></EnumerationResults>`)
		case r.Method == http.MethodGet && q.Get("comp") == "list":
			w.Header().Set("Content-Type", "application/xml")
			io.WriteString(w, `<?xml version="1.0"?><EnumerationResults ContainerName="empty"><Blobs/><NextMarker/></EnumerationResults>`) //nolint:errcheck // test server
		default:
			w.WriteHeader(http.StatusOK)
		}
	})

	containers, err := svc.ListContainers(ctx)
	if err != nil || len(containers) != 1 || containers[0].Name != "one" || containers[0].Created.IsZero() {
		t.Fatalf("ListContainers = %v, %v", containers, err)
	}
	if err := svc.CreateContainer(ctx, "new"); err != nil {
		t.Errorf("CreateContainer = %v", err)
	}
	if err := bucketError(svc.CreateContainer(ctx, "dup"), "dup"); !errors.Is(err, common.ErrBucketAlreadyExists) {
		t.Errorf("CreateContainer twice = %v", err)
	}
	if err := svc.DeleteContainer(ctx, "new"); err != nil {
		t.Errorf("DeleteContainer = %v", err)
	}
	if err := bucketError(svc.DeleteContainer(ctx, "gone"), "gone"); !errors.Is(err, common.ErrBucketNotFound) {
		t.Errorf("DeleteContainer missing = %v", err)
	}
	if ok, err := svc.ContainerExists(ctx, "new"); !ok || err != nil {
		t.Errorf("ContainerExists = %v, %v", ok, err)
	}
	if ok, err := svc.ContainerExists(ctx, "gone"); ok || err != nil {
		t.Errorf("ContainerExists missing = %v, %v", ok, err)
	}
	if has, err := svc.ContainerHasBlobs(ctx, "full"); !has || err != nil {
		t.Errorf("ContainerHasBlobs full = %v, %v", has, err)
	}
	if has, err := svc.ContainerHasBlobs(ctx, "empty"); has || err != nil {
		t.Errorf("ContainerHasBlobs empty = %v, %v", has, err)
	}
	if svc.Container("new") == nil {
		t.Error("Container returned nil")
	}
}

func TestAzureBlockWrapper(t *testing.T) {
	ctx := context.Background()
	var mu sync.Mutex
	var seen []string
	svc := newServiceServer(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body) //nolint:errcheck // test server
		mu.Lock()
		seen = append(seen, r.Method+" "+r.URL.Path+"?"+r.URL.RawQuery+" "+string(body))
		mu.Unlock()
		q := r.URL.Query()
		switch {
		case r.URL.Path == "/c/missing":
			writeCode(w, http.StatusNotFound, "BlobNotFound")
		case r.Method == http.MethodGet && q.Get("comp") == "blocklist":
			w.Header().Set("Content-Type", "application/xml")
			io.WriteString(w, `<?xml version="1.0"?><BlockList><UncommittedBlocks>`+ //nolint:errcheck // test server
				`<Block><Name>YWJj</Name><Size>5</Size></Block></UncommittedBlocks></BlockList>`)
		default:
			w.WriteHeader(http.StatusCreated)
		}
	})
	blob, ok := svc.Container("c").NewBlockBlob("obj").(blockAPI)
	if !ok {
		t.Fatal("the azure blob offers no block operations")
	}
	if err := blob.StageBlock(ctx, "YWJj", strings.NewReader("hello")); err != nil {
		t.Fatalf("StageBlock = %v", err)
	}
	if err := blob.CommitBlockList(ctx, []string{"YWJj"}, azblob.BlobHTTPHeaders{ContentType: "text/plain"}, map[string]string{"k": "v"}); err != nil {
		t.Fatalf("CommitBlockList = %v", err)
	}
	blocks, err := blob.UncommittedBlocks(ctx)
	if err != nil || len(blocks) != 1 || blocks["YWJj"] != 5 {
		t.Fatalf("UncommittedBlocks = %v, %v", blocks, err)
	}
	missing, ok := svc.Container("c").NewBlockBlob("missing").(blockAPI)
	if !ok {
		t.Fatal("no block operations")
	}
	if blocks, err := missing.UncommittedBlocks(ctx); err != nil || len(blocks) != 0 {
		t.Errorf("UncommittedBlocks of a missing blob = %v, %v", blocks, err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(seen) < 3 || !strings.Contains(seen[0], "comp=block") || !strings.HasSuffix(seen[0], "hello") ||
		!strings.Contains(seen[1], "comp=blocklist") || !strings.Contains(seen[1], "<Latest>YWJj</Latest>") {
		t.Errorf("requests = %q", seen)
	}
}
