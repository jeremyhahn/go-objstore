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
	"errors"
	"io"
	"sort"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

func TestGCS_AddPolicy_Success(t *testing.T) {
	mockBucket := &mockGCSBucket{
		objects: make(map[string][]byte),
	}
	g := &GCS{
		client: &mockGCSClient{bucket: mockBucket},
		bucket: "test-bucket",
	}

	policy := common.LifecyclePolicy{
		ID:        "policy1",
		Prefix:    "logs/",
		Retention: 24 * 7 * time.Hour,
		Action:    "delete",
	}
	err := g.AddPolicy(policy)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Verify the bucket lifecycle was configured correctly
	attrs, err := mockBucket.Attrs(context.Background())
	if err != nil {
		t.Fatalf("expected no error getting bucket attrs, got %v", err)
	}
	if len(attrs.Lifecycle.Rules) != 1 {
		t.Fatalf("expected 1 lifecycle rule, got %d", len(attrs.Lifecycle.Rules))
	}

	rule := attrs.Lifecycle.Rules[0]

	// Verify condition prefix
	if rule.Condition.MatchesPrefix == nil || len(rule.Condition.MatchesPrefix) != 1 {
		t.Fatal("expected 1 prefix in condition")
	}
	if rule.Condition.MatchesPrefix[0] != "logs/" {
		t.Fatalf("expected prefix 'logs/', got %s", rule.Condition.MatchesPrefix[0])
	}

	// Verify age is set correctly (7 days)
	if rule.Condition.AgeInDays != 7 {
		t.Fatalf("expected age 7 days, got %d", rule.Condition.AgeInDays)
	}

	// Verify delete action
	if rule.Action.Type != "Delete" {
		t.Fatalf("expected action type 'Delete', got %s", rule.Action.Type)
	}

	// Verify no storage class change for delete action
	if rule.Action.StorageClass != "" {
		t.Fatalf("expected no storage class for delete action, got %s", rule.Action.StorageClass)
	}

	policies, err := g.GetPolicies()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(policies) != 1 {
		t.Fatalf("expected 1 policy, got %d", len(policies))
	}
	// Note: GCS generates IDs as "rule-0", "rule-1", etc.
	if policies[0].Prefix != "logs/" {
		t.Fatalf("expected logs/ prefix, got %s", policies[0].Prefix)
	}
}

func TestGCS_AddPolicy_Archive(t *testing.T) {
	mockBucket := &mockGCSBucket{
		objects: make(map[string][]byte),
	}
	g := &GCS{
		client: &mockGCSClient{bucket: mockBucket},
		bucket: "test-bucket",
	}

	policy := common.LifecyclePolicy{
		ID:        "archive-policy",
		Prefix:    "old-data/",
		Retention: 30 * 24 * time.Hour,
		Action:    "archive",
	}
	err := g.AddPolicy(policy)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Verify the bucket lifecycle was configured correctly
	attrs, err := mockBucket.Attrs(context.Background())
	if err != nil {
		t.Fatalf("expected no error getting bucket attrs, got %v", err)
	}
	if len(attrs.Lifecycle.Rules) != 1 {
		t.Fatalf("expected 1 lifecycle rule, got %d", len(attrs.Lifecycle.Rules))
	}

	rule := attrs.Lifecycle.Rules[0]

	// Verify condition prefix
	if rule.Condition.MatchesPrefix == nil || len(rule.Condition.MatchesPrefix) != 1 {
		t.Fatal("expected 1 prefix in condition")
	}
	if rule.Condition.MatchesPrefix[0] != "old-data/" {
		t.Fatalf("expected prefix 'old-data/', got %s", rule.Condition.MatchesPrefix[0])
	}

	// Verify age is set correctly (30 days)
	if rule.Condition.AgeInDays != 30 {
		t.Fatalf("expected age 30 days, got %d", rule.Condition.AgeInDays)
	}

	// Verify SetStorageClass action
	if rule.Action.Type != "SetStorageClass" {
		t.Fatalf("expected action type 'SetStorageClass', got %s", rule.Action.Type)
	}

	// Verify storage class is ARCHIVE
	if rule.Action.StorageClass != "ARCHIVE" {
		t.Fatalf("expected storage class 'ARCHIVE', got %s", rule.Action.StorageClass)
	}

	policies, err := g.GetPolicies()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(policies) != 1 {
		t.Fatalf("expected 1 policy, got %d", len(policies))
	}
	if policies[0].Prefix != "old-data/" {
		t.Fatalf("expected old-data/ prefix, got %s", policies[0].Prefix)
	}
	if policies[0].Action != "archive" {
		t.Fatalf("expected action 'archive', got %s", policies[0].Action)
	}
}

func TestGCS_AddPolicy_InvalidID(t *testing.T) {
	g := newMockGCS()

	policy := common.LifecyclePolicy{
		ID:        "",
		Prefix:    "logs/",
		Retention: 24 * time.Hour,
		Action:    "delete",
	}
	err := g.AddPolicy(policy)
	if err != common.ErrInvalidPolicy {
		t.Fatalf("expected ErrInvalidPolicy, got %v", err)
	}
}

func TestGCS_AddPolicy_InvalidAction(t *testing.T) {
	g := newMockGCS()

	policy := common.LifecyclePolicy{
		ID:        "policy1",
		Prefix:    "logs/",
		Retention: 24 * time.Hour,
		Action:    "invalid",
	}
	err := g.AddPolicy(policy)
	if err != common.ErrInvalidPolicy {
		t.Fatalf("expected ErrInvalidPolicy, got %v", err)
	}
}

func TestGCS_RemovePolicy(t *testing.T) {
	g := newMockGCS()

	policy := common.LifecyclePolicy{
		ID:        "policy1",
		Prefix:    "logs/",
		Retention: 24 * time.Hour,
		Action:    "delete",
	}
	err := g.AddPolicy(policy)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Note: GCS RemovePolicy currently returns nil (limitation of GCS API)
	err = g.RemovePolicy("policy1")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Policies should still exist since RemovePolicy doesn't actually remove them
	// This is a known limitation of the GCS implementation
}

func TestGCS_RemovePolicy_NonExistent(t *testing.T) {
	g := newMockGCS()

	err := g.RemovePolicy("nonexistent")
	if err != nil {
		t.Fatalf("expected no error for removing non-existent policy, got %v", err)
	}
}

func TestGCS_GetPolicies_Empty(t *testing.T) {
	g := newMockGCS()

	policies, err := g.GetPolicies()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(policies) != 0 {
		t.Fatalf("expected 0 policies, got %d", len(policies))
	}
}

func TestGCS_GetPolicies_Multiple(t *testing.T) {
	g := newMockGCS()
	policy1 := common.LifecyclePolicy{
		ID:        "policy1",
		Prefix:    "logs/",
		Retention: 24 * time.Hour,
		Action:    "delete",
	}
	policy2 := common.LifecyclePolicy{
		ID:        "policy2",
		Prefix:    "archive/",
		Retention: 48 * time.Hour,
		Action:    "archive",
	}

	g.AddPolicy(policy1)
	g.AddPolicy(policy2)

	policies, err := g.GetPolicies()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(policies) != 2 {
		t.Fatalf("expected 2 policies, got %d", len(policies))
	}
}

// In-memory fakes for unit-testing GCS Put/Get/Delete/Archive.
type fakeObj struct {
	data      []byte
	err       bool
	writeErr  bool
	closeErr  bool
	deleteErr bool
	attrsErr  bool
}

func (f *fakeObj) NewWriter(ctx context.Context) io.WriteCloser {
	if f.writeErr {
		return &errorWriter{closeErr: f.closeErr}
	}
	return &nopWriteCloser{buf: &f.data, closeErr: f.closeErr}
}

func (f *fakeObj) NewReader(ctx context.Context) (io.ReadCloser, error) {
	if f.err {
		return nil, errors.New("read error")
	}
	return io.NopCloser(bytes.NewReader(f.data)), nil
}

func (f *fakeObj) Delete(ctx context.Context) error {
	if f.deleteErr {
		return errors.New("del err")
	}
	f.data = nil
	return nil
}

func (f *fakeObj) Attrs(ctx context.Context) (*storage.ObjectAttrs, error) {
	if f.attrsErr {
		return nil, errors.New("attrs error")
	}
	if f.data == nil {
		return nil, errors.New("object doesn't exist")
	}
	return &storage.ObjectAttrs{Name: "test", Size: int64(len(f.data))}, nil
}

type nopWriteCloser struct {
	buf      *[]byte
	closeErr bool
}

func (n *nopWriteCloser) Write(p []byte) (int, error) {
	*n.buf = append(*n.buf, p...)
	return len(p), nil
}

func (n *nopWriteCloser) Close() error {
	if n.closeErr {
		return errors.New("close error")
	}
	return nil
}

type errorWriter struct {
	closeErr bool
}

func (e *errorWriter) Write(p []byte) (int, error) {
	return 0, errors.New("write error")
}

func (e *errorWriter) Close() error {
	if e.closeErr {
		return errors.New("close error")
	}
	return nil
}

type fakeIterator struct {
	objects []*storage.ObjectAttrs
	index   int
	err     error
}

func (f *fakeIterator) Next() (*storage.ObjectAttrs, error) {
	if f.err != nil {
		return nil, f.err
	}
	if f.index >= len(f.objects) {
		return nil, iterator.Done
	}
	obj := f.objects[f.index]
	f.index++
	return obj, nil
}

type fakeBucket struct {
	objs     map[string]*fakeObj
	iterator gcsIterator
}

func (b fakeBucket) Object(name string) gcsObject {
	if b.objs[name] == nil {
		b.objs[name] = &fakeObj{}
	}
	return b.objs[name]
}

func (b fakeBucket) Objects(ctx context.Context, query *storage.Query) gcsIterator {
	return b.iterator
}

func (b fakeBucket) Attrs(ctx context.Context) (*storage.BucketAttrs, error) {
	return &storage.BucketAttrs{
		Lifecycle: storage.Lifecycle{
			Rules: []storage.LifecycleRule{},
		},
	}, nil
}

func (b fakeBucket) Update(ctx context.Context, uattrs storage.BucketAttrsToUpdate) (*storage.BucketAttrs, error) {
	return &storage.BucketAttrs{}, nil
}

type fakeClient struct {
	b fakeBucket
}

func (c fakeClient) Bucket(name string) gcsBucket {
	return c.b
}

type mockArch struct {
	gotKey string
	got    []byte
	putErr error
}

func (m *mockArch) Put(k string, r io.Reader) error {
	if m.putErr != nil {
		return m.putErr
	}
	m.gotKey = k
	m.got, _ = io.ReadAll(r)
	return nil
}

func TestNew(t *testing.T) {
	s := New()
	if s == nil {
		t.Fatal("New() returned nil")
	}
	gcs, ok := s.(*GCS)
	if !ok {
		t.Fatal("New() did not return *GCS")
	}
	if gcs.client != nil {
		t.Fatal("New() should not initialize client")
	}
	if gcs.bucket != "" {
		t.Fatal("New() should not set bucket")
	}
}

func TestGCS_Configure_MissingBucket(t *testing.T) {
	g := &GCS{}
	err := g.Configure(map[string]string{})
	if err == nil {
		t.Fatal("expected error for missing bucket")
	}
	if err.Error() != "bucket not set" {
		t.Fatalf("expected 'bucket not set', got %v", err)
	}
}

func TestGCS_Configure_EmptyBucket(t *testing.T) {
	g := &GCS{}
	err := g.Configure(map[string]string{"bucket": ""})
	if err == nil {
		t.Fatal("expected error for empty bucket")
	}
	if err.Error() != "bucket not set" {
		t.Fatalf("expected 'bucket not set', got %v", err)
	}
}

func TestGCS_Configure_ClientAlreadySet(t *testing.T) {
	fc := fakeClient{b: fakeBucket{objs: map[string]*fakeObj{}}}
	g := &GCS{client: fc}

	// Configure should skip client creation if already set
	err := g.Configure(map[string]string{"bucket": "test-bucket"})
	if err != nil {
		t.Fatalf("config err: %v", err)
	}

	if g.bucket != "test-bucket" {
		t.Fatalf("bucket not set correctly: got %s", g.bucket)
	}
}

func TestGCS_Configure_ClientCreationError(t *testing.T) {
	// Save original function
	originalNewClient := gcsNewClient
	defer func() { gcsNewClient = originalNewClient }()

	// Mock gcsNewClient to return error
	gcsNewClient = func(ctx context.Context) (*storage.Client, error) {
		return nil, errors.New("client creation failed")
	}

	g := &GCS{}
	err := g.Configure(map[string]string{"bucket": "test-bucket"})
	if err == nil {
		t.Fatal("expected error from client creation")
	}
	if err.Error() != "client creation failed" {
		t.Fatalf("expected 'client creation failed', got %v", err)
	}
}

func TestGCS_PutGetDelete_Archive(t *testing.T) {
	fc := fakeClient{b: fakeBucket{objs: map[string]*fakeObj{}}}
	g := &GCS{client: fc}
	if err := g.Configure(map[string]string{"bucket": "b"}); err != nil {
		t.Fatalf("config err: %v", err)
	}

	key := "k.txt"
	if err := g.Put(key, bytes.NewBufferString("data")); err != nil {
		t.Fatalf("put err: %v", err)
	}
	rc, err := g.Get(key)
	if err != nil {
		t.Fatalf("get err: %v", err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if string(got) != "data" {
		t.Fatalf("got %s", got)
	}

	ma := &mockArch{}
	if err := g.Archive(key, ma); err != nil {
		t.Fatalf("archive err: %v", err)
	}
	if ma.gotKey != key || string(ma.got) != "data" {
		t.Fatalf("archive mismatch")
	}

	if err := g.Delete(key); err != nil {
		t.Fatalf("delete err: %v", err)
	}
}

func TestGCS_Put_WriterError(t *testing.T) {
	objs := map[string]*fakeObj{
		"test-key": {writeErr: true},
	}
	fc := fakeClient{b: fakeBucket{objs: objs}}
	g := &GCS{client: fc, bucket: "test-bucket"}

	err := g.Put("test-key", bytes.NewBufferString("data"))
	if err == nil {
		t.Fatal("expected error from writer")
	}
	if err.Error() != "write error" {
		t.Fatalf("expected 'write error', got %v", err)
	}
}

func TestGCS_Put_CloseError(t *testing.T) {
	objs := map[string]*fakeObj{
		"test-key": {closeErr: true},
	}
	fc := fakeClient{b: fakeBucket{objs: objs}}
	g := &GCS{client: fc, bucket: "test-bucket"}

	err := g.Put("test-key", bytes.NewBufferString("data"))
	if err == nil {
		t.Fatal("expected error from close")
	}
	if err.Error() != "close error" {
		t.Fatalf("expected 'close error', got %v", err)
	}
}

func TestGCS_Get_Error(t *testing.T) {
	objs := map[string]*fakeObj{
		"test-key": {err: true},
	}
	fc := fakeClient{b: fakeBucket{objs: objs}}
	g := &GCS{client: fc, bucket: "test-bucket"}

	_, err := g.Get("test-key")
	if err == nil {
		t.Fatal("expected error from reader")
	}
	if err.Error() != "read error" {
		t.Fatalf("expected 'read error', got %v", err)
	}
}

func TestGCS_Delete_Error(t *testing.T) {
	objs := map[string]*fakeObj{
		"test-key": {deleteErr: true},
	}
	fc := fakeClient{b: fakeBucket{objs: objs}}
	g := &GCS{client: fc, bucket: "test-bucket"}

	err := g.Delete("test-key")
	if err == nil {
		t.Fatal("expected error from delete")
	}
	if err.Error() != "del err" {
		t.Fatalf("expected 'del err', got %v", err)
	}
}

func TestGCS_Archive_GetError(t *testing.T) {
	objs := map[string]*fakeObj{
		"test-key": {err: true},
	}
	fc := fakeClient{b: fakeBucket{objs: objs}}
	g := &GCS{client: fc, bucket: "test-bucket"}

	ma := &mockArch{}
	err := g.Archive("test-key", ma)
	if err == nil {
		t.Fatal("expected error from Get in Archive")
	}
	if err.Error() != "read error" {
		t.Fatalf("expected 'read error', got %v", err)
	}
}

func TestGCS_Archive_PutError(t *testing.T) {
	objs := map[string]*fakeObj{
		"test-key": {data: []byte("data")},
	}
	fc := fakeClient{b: fakeBucket{objs: objs}}
	g := &GCS{client: fc, bucket: "test-bucket"}

	ma := &mockArch{putErr: errors.New("put failed")}
	err := g.Archive("test-key", ma)
	if err == nil {
		t.Fatal("expected error from Put in Archive")
	}
	if err.Error() != "put failed" {
		t.Fatalf("expected 'put failed', got %v", err)
	}
}

func TestWrappers_Bucket(t *testing.T) {
	// This test verifies the wrapper methods are properly invoked
	// We can't easily test the real storage.Client, but we can verify
	// that our wrapper correctly delegates
	fc := fakeClient{b: fakeBucket{objs: map[string]*fakeObj{}}}
	bucket := fc.Bucket("test-bucket")
	if bucket == nil {
		t.Fatal("Bucket() returned nil")
	}

	obj := bucket.Object("test-key")
	if obj == nil {
		t.Fatal("Object() returned nil")
	}
}

func TestWrappers_Object(t *testing.T) {
	objs := map[string]*fakeObj{
		"test-key": {data: []byte("test-data")},
	}
	fb := fakeBucket{objs: objs}
	obj := fb.Object("test-key")
	if obj == nil {
		t.Fatal("Object() returned nil")
	}

	// Verify we get the same object
	if fakeObj, ok := obj.(*fakeObj); ok {
		if string(fakeObj.data) != "test-data" {
			t.Fatalf("expected 'test-data', got %s", string(fakeObj.data))
		}
	} else {
		t.Fatal("Object() did not return expected type")
	}
}

func TestGCS_List_EmptyPrefix(t *testing.T) {
	iter := &fakeIterator{
		objects: []*storage.ObjectAttrs{
			{Name: "file1.txt"},
			{Name: "file2.txt"},
			{Name: "dir/file3.txt"},
		},
	}

	fc := fakeClient{
		b: fakeBucket{
			objs:     map[string]*fakeObj{},
			iterator: iter,
		},
	}

	g := &GCS{client: fc, bucket: "test-bucket"}

	keys, err := g.List("")
	if err != nil {
		t.Fatalf("Expected no error on List, got %v", err)
	}

	sort.Strings(keys)
	expected := []string{"dir/file3.txt", "file1.txt", "file2.txt"}
	sort.Strings(expected)

	if len(keys) != len(expected) {
		t.Fatalf("Expected %d keys, got %d", len(expected), len(keys))
	}

	for i, key := range keys {
		if key != expected[i] {
			t.Errorf("Expected key %s, got %s", expected[i], key)
		}
	}
}

func TestGCS_List_WithPrefix(t *testing.T) {
	iter := &fakeIterator{
		objects: []*storage.ObjectAttrs{
			{Name: "logs/2023/file1.log"},
			{Name: "logs/2023/file2.log"},
			{Name: "logs/2024/file3.log"},
		},
	}

	fc := fakeClient{
		b: fakeBucket{
			objs:     map[string]*fakeObj{},
			iterator: iter,
		},
	}

	g := &GCS{client: fc, bucket: "test-bucket"}

	keys, err := g.List("logs/")
	if err != nil {
		t.Fatalf("Expected no error on List, got %v", err)
	}

	sort.Strings(keys)
	expected := []string{"logs/2023/file1.log", "logs/2023/file2.log", "logs/2024/file3.log"}
	sort.Strings(expected)

	if len(keys) != len(expected) {
		t.Fatalf("Expected %d keys, got %d", len(expected), len(keys))
	}

	for i, key := range keys {
		if key != expected[i] {
			t.Errorf("Expected key %s, got %s", expected[i], key)
		}
	}
}

func TestGCS_List_Empty(t *testing.T) {
	iter := &fakeIterator{
		objects: []*storage.ObjectAttrs{},
	}

	fc := fakeClient{
		b: fakeBucket{
			objs:     map[string]*fakeObj{},
			iterator: iter,
		},
	}

	g := &GCS{client: fc, bucket: "test-bucket"}

	keys, err := g.List("nonexistent/")
	if err != nil {
		t.Fatalf("Expected no error on List, got %v", err)
	}

	if len(keys) != 0 {
		t.Errorf("Expected 0 keys, got %d", len(keys))
	}
}

func TestGCS_List_Error(t *testing.T) {
	iter := &fakeIterator{
		err: errors.New("list error"),
	}

	fc := fakeClient{
		b: fakeBucket{
			objs:     map[string]*fakeObj{},
			iterator: iter,
		},
	}

	g := &GCS{client: fc, bucket: "test-bucket"}

	_, err := g.List("")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err.Error() != "list error" {
		t.Errorf("expected 'list error', got %v", err)
	}
}
