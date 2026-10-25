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

package local

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/internal/uploadtest"
	"github.com/jeremyhahn/go-objstore/pkg/common"
)

func TestLocal_MultipartConformance(t *testing.T) {
	uploadtest.RunMultipart(t, func(t *testing.T) common.Storage {
		return newConfigured(t, t.TempDir())
	})
}

func TestLocal_CapabilityDiscovery(t *testing.T) {
	var s common.Storage = New()
	if _, ok := s.(common.MultipartUploader); !ok {
		t.Error("Local is not a MultipartUploader")
	}
}

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time { return c.t }

func newClockedLocal(t *testing.T, settings map[string]string) (*Local, *fakeClock, string) {
	t.Helper()
	dir := t.TempDir()
	s := map[string]string{"path": dir}
	for k, v := range settings {
		s[k] = v
	}
	l := New().(*Local)
	if err := l.Configure(s, noCreds); err != nil {
		t.Fatal(err)
	}
	c := &fakeClock{t: time.Now()}
	l.now = c.now
	return l, c, dir
}

func completeOne(t *testing.T, l *Local, key, id string, data string) error {
	t.Helper()
	ctx := context.Background()
	info, err := l.UploadPart(ctx, key, id, 1, strings.NewReader(data), int64(len(data)))
	if err != nil {
		return err
	}
	return l.CompleteMultipartUpload(ctx, key, id, []common.CompletedPart{{PartNumber: 1, ETag: info.ETag}})
}

func TestLocal_Configure_MultipartExpiry(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		want    time.Duration
		wantErr bool
	}{
		{"default", "", 168 * time.Hour, false},
		{"explicit", "2h", 2 * time.Hour, false},
		{"invalid", "never", 0, true},
		{"non-positive", "0s", 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			settings := map[string]string{"path": t.TempDir()}
			if tt.value != "" {
				settings["multipartExpiry"] = tt.value
			}
			l := New().(*Local)
			err := l.Configure(settings, noCreds)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil {
				if !errors.Is(err, common.ErrInvalidArgument) {
					t.Errorf("error %v does not match ErrInvalidArgument", err)
				}
				return
			}
			if got := l.expiry(); got != tt.want {
				t.Errorf("expiry = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLocal_StagingLayout(t *testing.T) {
	ctx := context.Background()
	l, _, dir := newClockedLocal(t, nil)

	id, err := l.InitiateMultipartUpload(ctx, "a/b.bin", nil)
	if err != nil {
		t.Fatal(err)
	}
	upDir := filepath.Join(dir, ".objstore-multipart", id)
	if _, err := os.Stat(filepath.Join(upDir, "upload.json")); err != nil {
		t.Fatalf("upload.json missing: %v", err)
	}
	if _, err := l.UploadPart(ctx, "a/b.bin", id, 1, strings.NewReader("abc"), 3); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(upDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) < 2 {
		t.Errorf("expected part files beside upload.json, got %d entries", len(entries))
	}

	// Staged state is never an object.
	keys, err := l.List("")
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 0 {
		t.Errorf("List returned staged files: %v", keys)
	}

	if err := l.AbortMultipartUpload(ctx, "a/b.bin", id); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(upDir); !os.IsNotExist(err) {
		t.Errorf("upload directory survived abort: %v", err)
	}
}

func TestLocal_CompleteRemovesStagingAndWritesSidecar(t *testing.T) {
	ctx := context.Background()
	l, _, dir := newClockedLocal(t, nil)
	md := &common.Metadata{ContentType: "text/plain", Custom: map[string]string{"k": "v"}}
	id, err := l.InitiateMultipartUpload(ctx, "obj.txt", md)
	if err != nil {
		t.Fatal(err)
	}
	if err := completeOne(t, l, "obj.txt", id, "hello"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, ".objstore-multipart", id)); !os.IsNotExist(err) {
		t.Errorf("upload directory survived complete: %v", err)
	}
	got, err := l.GetMetadata(ctx, "obj.txt")
	if err != nil {
		t.Fatal(err)
	}
	if got.ContentType != "text/plain" || got.Custom["k"] != "v" || got.Size != 5 || got.ETag == "" {
		t.Errorf("metadata = %+v", got)
	}
	keys, _ := l.List("")
	if len(keys) != 1 || keys[0] != "obj.txt" {
		t.Errorf("List = %v", keys)
	}
}

func TestLocal_UploadIDIsCheckedBeforeUseAsPath(t *testing.T) {
	ctx := context.Background()
	l, _, dir := newClockedLocal(t, nil)
	victim := filepath.Join(dir, "victim")
	if err := os.MkdirAll(victim, 0750); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"", "..", "../victim", "a/b", strings.Repeat("g", 32), "ABCDEF"} {
		if _, err := l.UploadPart(ctx, "k", id, 1, strings.NewReader("x"), 1); !errors.Is(err, common.ErrUploadNotFound) {
			t.Errorf("UploadPart(%q) = %v", id, err)
		}
		if err := l.CompleteMultipartUpload(ctx, "k", id, []common.CompletedPart{{PartNumber: 1, ETag: "e"}}); !errors.Is(err, common.ErrUploadNotFound) {
			t.Errorf("Complete(%q) = %v", id, err)
		}
		if err := l.AbortMultipartUpload(ctx, "k", id); !errors.Is(err, common.ErrUploadNotFound) {
			t.Errorf("Abort(%q) = %v", id, err)
		}
	}
	if _, err := os.Stat(victim); err != nil {
		t.Errorf("a directory outside the staging area was touched: %v", err)
	}
}

func TestLocal_KeyAliasesCannotReachTheStagingArea(t *testing.T) {
	ctx := context.Background()
	l, _, dir := newClockedLocal(t, nil)
	id, err := l.InitiateMultipartUpload(ctx, "k", nil)
	if err != nil {
		t.Fatal(err)
	}
	record := filepath.Join(dir, ".objstore-multipart", id, uploadFile)
	before, err := os.ReadFile(record)
	if err != nil {
		t.Fatal(err)
	}

	for _, alias := range []string{
		"./.objstore-multipart/" + id + "/" + uploadFile,
		"././.objstore-multipart/" + id + "/" + uploadFile,
		".objstore-multipart/./" + id + "/" + uploadFile,
		".\\.objstore-multipart\\" + id + "\\" + uploadFile,
	} {
		if err := l.Put(alias, strings.NewReader("overwritten")); !errors.Is(err, common.ErrInvalidArgument) {
			t.Errorf("Put(%q) = %v", alias, err)
		}
		if err := l.PutWithContext(ctx, alias, strings.NewReader("overwritten")); !errors.Is(err, common.ErrInvalidArgument) {
			t.Errorf("PutWithContext(%q) = %v", alias, err)
		}
		if err := l.Delete(alias); !errors.Is(err, common.ErrInvalidArgument) {
			t.Errorf("Delete(%q) = %v", alias, err)
		}
		if _, err := l.Get(alias); !errors.Is(err, common.ErrInvalidArgument) {
			t.Errorf("Get(%q) = %v", alias, err)
		}
	}

	for _, prefix := range []string{"./.objstore-multipart/", ".objstore-multipart", ".\\.objstore-multipart\\"} {
		keys, err := l.List(prefix)
		if err == nil && len(keys) > 0 {
			t.Errorf("List(%q) = %v", prefix, keys)
		}
	}

	after, err := os.ReadFile(record)
	if err != nil || !bytes.Equal(before, after) {
		t.Errorf("the upload record changed: %v", err)
	}
	if _, err := l.UploadPart(ctx, "k", id, 1, strings.NewReader("x"), 1); err != nil {
		t.Errorf("the upload is no longer usable: %v", err)
	}
}

func TestLocal_ExpiredUploadIsTreatedAsAborted(t *testing.T) {
	ctx := context.Background()
	l, c, dir := newClockedLocal(t, map[string]string{"multipartExpiry": "1h"})
	id, err := l.InitiateMultipartUpload(ctx, "k", nil)
	if err != nil {
		t.Fatal(err)
	}
	info, err := l.UploadPart(ctx, "k", id, 1, strings.NewReader("data"), 4)
	if err != nil {
		t.Fatal(err)
	}

	c.t = c.t.Add(time.Hour) // exactly at the limit: still open
	if _, err := l.UploadPart(ctx, "k", id, 2, strings.NewReader("more"), 4); err != nil {
		t.Fatalf("upload at the limit: %v", err)
	}

	c.t = c.t.Add(time.Second) // past the limit
	ups, err := l.ListMultipartUploads(ctx, "")
	if err != nil || len(ups) != 1 {
		t.Fatalf("an expired upload must stay listable for cleanup: %v, %v", ups, err)
	}
	if _, err := l.UploadPart(ctx, "k", id, 3, strings.NewReader("x"), 1); !errors.Is(err, common.ErrUploadNotFound) {
		t.Errorf("UploadPart after expiry = %v", err)
	}
	err = l.CompleteMultipartUpload(ctx, "k", id, []common.CompletedPart{{PartNumber: 1, ETag: info.ETag}})
	if !errors.Is(err, common.ErrUploadNotFound) {
		t.Errorf("Complete after expiry = %v", err)
	}
	if ok, _ := l.Exists(ctx, "k"); ok {
		t.Error("an expired upload produced an object")
	}
	if _, err := os.Stat(filepath.Join(dir, ".objstore-multipart", id)); !os.IsNotExist(err) {
		t.Errorf("expired upload not dropped: %v", err)
	}
}

func TestLocal_AbortRemovesExpiredUpload(t *testing.T) {
	ctx := context.Background()
	l, c, _ := newClockedLocal(t, map[string]string{"multipartExpiry": "1h"})
	id, _ := l.InitiateMultipartUpload(ctx, "k", nil)
	c.t = c.t.Add(2 * time.Hour)
	if err := l.AbortMultipartUpload(ctx, "k", id); err != nil {
		t.Fatalf("abort of an expired upload: %v", err)
	}
	if ups, _ := l.ListMultipartUploads(ctx, ""); len(ups) != 0 {
		t.Errorf("still listed: %v", ups)
	}
}

func TestLocal_ExpireUploads(t *testing.T) {
	ctx := context.Background()
	l, c, dir := newClockedLocal(t, map[string]string{"multipartExpiry": "1h"})
	old1, _ := l.InitiateMultipartUpload(ctx, "one", nil)
	old2, _ := l.InitiateMultipartUpload(ctx, "two", nil)
	c.t = c.t.Add(90 * time.Minute)
	fresh, _ := l.InitiateMultipartUpload(ctx, "three", nil)

	// Garbage that is not an upload is left alone while it is young and
	// removed once it is older than the expiry.
	junk := filepath.Join(dir, ".objstore-multipart", "not-an-upload")
	if err := os.MkdirAll(junk, 0750); err != nil {
		t.Fatal(err)
	}
	corrupt := filepath.Join(dir, ".objstore-multipart", strings.Repeat("a", 32))
	if err := os.MkdirAll(corrupt, 0750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(corrupt, "upload.json"), []byte("{"), 0600); err != nil {
		t.Fatal(err)
	}
	past := c.t.Add(-2 * time.Hour)
	if err := os.Chtimes(corrupt, past, past); err != nil {
		t.Fatal(err)
	}
	// A temporary directory left by a crashed initiate is judged by its age.
	leftover := filepath.Join(dir, ".objstore-multipart", tmpFilePrefix+"crashed")
	if err := os.MkdirAll(leftover, 0750); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(leftover, past, past); err != nil {
		t.Fatal(err)
	}

	if n := l.expireUploads(); n != 4 {
		t.Errorf("expireUploads = %d, want 4 (two expired uploads, one corrupt, one leftover)", n)
	}
	if _, err := os.Stat(leftover); !os.IsNotExist(err) {
		t.Errorf("leftover temporary directory survived: %v", err)
	}
	for _, id := range []string{old1, old2, strings.Repeat("a", 32)} {
		if _, err := os.Stat(filepath.Join(dir, ".objstore-multipart", id)); !os.IsNotExist(err) {
			t.Errorf("upload %s survived cleanup: %v", id, err)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, ".objstore-multipart", fresh)); err != nil {
		t.Errorf("a live upload was removed: %v", err)
	}
	if _, err := os.Stat(junk); err != nil {
		t.Errorf("an entry that is not an upload ID was removed: %v", err)
	}
}

func TestLocal_ExpireUploads_NoStagingDirectory(t *testing.T) {
	l, _, _ := newClockedLocal(t, nil)
	if n := l.expireUploads(); n != 0 {
		t.Errorf("expireUploads = %d", n)
	}
	if n := (&Local{}).expireUploads(); n != 0 {
		t.Errorf("expireUploads without a path = %d", n)
	}
}

func TestLocal_LifecycleProcessExpiresUploads(t *testing.T) {
	ctx := context.Background()
	l, c, dir := newClockedLocal(t, map[string]string{"multipartExpiry": "1h"})
	id, _ := l.InitiateMultipartUpload(ctx, "k", nil)
	c.t = c.t.Add(2 * time.Hour)
	NewLifecycleManager().Process(l)
	if _, err := os.Stat(filepath.Join(dir, ".objstore-multipart", id)); !os.IsNotExist(err) {
		t.Errorf("Process did not clean up the expired upload: %v", err)
	}
}

func TestLocal_LifecyclePoliciesIgnoreStaging(t *testing.T) {
	ctx := context.Background()
	l, _, dir := newClockedLocal(t, nil)
	id, _ := l.InitiateMultipartUpload(ctx, "k", nil)
	if _, err := l.UploadPart(ctx, "k", id, 1, strings.NewReader("x"), 1); err != nil {
		t.Fatal(err)
	}
	past := time.Now().Add(-48 * time.Hour)
	_ = filepath.Walk(filepath.Join(dir, ".objstore-multipart"), func(p string, _ os.FileInfo, _ error) error {
		return os.Chtimes(p, past, past)
	})

	lm := NewLifecycleManager()
	if err := lm.AddPolicy(common.LifecyclePolicy{ID: "p", Prefix: "", Retention: time.Hour, Action: "delete"}); err != nil {
		t.Fatal(err)
	}
	lm.Process(l)
	if ups, _ := l.ListMultipartUploads(ctx, ""); len(ups) != 1 {
		t.Errorf("a lifecycle policy reached into the staging area: %v", ups)
	}
}

func TestLocal_CompleteChangeLogRecordsOneEvent(t *testing.T) {
	ctx := context.Background()
	l, _, _ := newClockedLocal(t, nil)
	cl := newMockChangeLog()
	l.SetChangeLog(cl)

	id, _ := l.InitiateMultipartUpload(ctx, "k", nil)
	info1, _ := l.UploadPart(ctx, "k", id, 1, bytes.NewReader(bytes.Repeat([]byte("a"), common.MinPartSize)), common.MinPartSize)
	info2, _ := l.UploadPart(ctx, "k", id, 2, strings.NewReader("tail"), 4)
	if n := len(cl.getEvents()); n != 0 {
		t.Fatalf("events before complete = %d", n)
	}
	err := l.CompleteMultipartUpload(ctx, "k", id, []common.CompletedPart{
		{PartNumber: 1, ETag: info1.ETag}, {PartNumber: 2, ETag: info2.ETag},
	})
	if err != nil {
		t.Fatal(err)
	}
	ev := cl.getEvents()
	if len(ev) != 1 {
		t.Fatalf("events = %d, want 1", len(ev))
	}
	if ev[0].Key != "k" || ev[0].Operation != "put" || ev[0].Size != common.MinPartSize+4 {
		t.Errorf("event = %+v", ev[0])
	}
}

func TestLocal_EncryptedParts(t *testing.T) {
	ctx := context.Background()
	l, _, dir := newClockedLocal(t, nil)
	l.SetAtRestEncrypterFactory(&mockEncrypterFactory{
		defaultKeyID: "key1",
		encrypters:   map[string]common.Encrypter{"key1": &mockEncrypter{keyID: "key1", algorithm: "AES256-TEST"}},
	})

	id, err := l.InitiateMultipartUpload(ctx, "secret", nil)
	if err != nil {
		t.Fatal(err)
	}
	first := bytes.Repeat([]byte("p"), common.MinPartSize)
	info1, err := l.UploadPart(ctx, "secret", id, 1, bytes.NewReader(first), int64(len(first)))
	if err != nil {
		t.Fatal(err)
	}
	info2, err := l.UploadPart(ctx, "secret", id, 2, strings.NewReader("tail"), 4)
	if err != nil {
		t.Fatal(err)
	}
	if info1.Size != int64(len(first)) {
		t.Errorf("part size = %d, want the plaintext size", info1.Size)
	}

	// No plaintext part on disk.
	_ = filepath.Walk(filepath.Join(dir, ".objstore-multipart", id), func(p string, fi os.FileInfo, _ error) error {
		if fi != nil && !fi.IsDir() && strings.HasSuffix(p, ".json") {
			return nil
		}
		if fi != nil && !fi.IsDir() {
			raw, _ := os.ReadFile(p)
			if bytes.Contains(raw, []byte("tail")) && !bytes.HasPrefix(raw, []byte("ENCRYPTED:")) {
				t.Errorf("plaintext part on disk: %s", p)
			}
		}
		return nil
	})

	err = l.CompleteMultipartUpload(ctx, "secret", id, []common.CompletedPart{
		{PartNumber: 1, ETag: info1.ETag}, {PartNumber: 2, ETag: info2.ETag},
	})
	if err != nil {
		t.Fatal(err)
	}

	// The object is encrypted once, as a whole, and reads back as plaintext.
	raw, err := os.ReadFile(filepath.Join(dir, "secret"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(raw, []byte("ENCRYPTED:")) || bytes.Count(raw, []byte("ENCRYPTED:")) != 1 {
		t.Errorf("object not encrypted as one stream (prefix count %d)", bytes.Count(raw, []byte("ENCRYPTED:")))
	}
	rc, err := l.GetWithContext(ctx, "secret")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rc.Close() }()
	got, _ := io.ReadAll(rc)
	if !bytes.Equal(got, append(first, []byte("tail")...)) {
		t.Errorf("round trip mismatch: %d bytes", len(got))
	}
	md, err := l.GetMetadata(ctx, "secret")
	if err != nil {
		t.Fatal(err)
	}
	if md.Custom["at_rest_encryption_algorithm"] != "AES256-TEST" || md.Custom["at_rest_encryption_key_id"] != "key1" {
		t.Errorf("encryption metadata = %v", md.Custom)
	}
	if md.Size != int64(len(raw)) {
		t.Errorf("metadata size = %d, want stored size %d", md.Size, len(raw))
	}
}

// plaintextWatcher serves a part body and, each time the reader is polled after
// the body has been handed out, looks through the store for a file that holds
// the body in the clear.
type plaintextWatcher struct {
	body []byte
	root string
	off  int
	t    *testing.T
	leak string
}

func (w *plaintextWatcher) Read(p []byte) (int, error) {
	if w.off < len(w.body) {
		n := copy(p, w.body[w.off:])
		w.off += n
		return n, nil
	}
	w.scan()
	return 0, io.EOF
}

func (w *plaintextWatcher) scan() {
	_ = filepath.Walk(w.root, func(p string, fi os.FileInfo, _ error) error {
		if fi == nil || fi.IsDir() {
			return nil
		}
		// The stand-in cipher only prefixes the body, so a sealed file also
		// contains it; what gives a leak away is the missing prefix.
		raw, err := os.ReadFile(p) // #nosec G304 -- test walks its own temp dir
		if err == nil && bytes.Contains(raw, w.body) && !bytes.HasPrefix(raw, []byte("ENCRYPTED:")) {
			w.leak = p
		}
		return nil
	})
}

func TestLocal_EncryptedPartsNeverTouchDiskInTheClear(t *testing.T) {
	ctx := context.Background()
	l, _, dir := newClockedLocal(t, nil)
	l.SetAtRestEncrypterFactory(&mockEncrypterFactory{
		defaultKeyID: "key1",
		encrypters:   map[string]common.Encrypter{"key1": &mockEncrypter{keyID: "key1", algorithm: "AES256-TEST"}},
	})

	id, err := l.InitiateMultipartUpload(ctx, "secret", nil)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte("top-secret-part-body")
	watch := &plaintextWatcher{body: body, root: dir, t: t}
	if _, err := l.UploadPart(ctx, "secret", id, 1, watch, int64(len(body))); err != nil {
		t.Fatal(err)
	}
	if watch.leak != "" {
		t.Errorf("plaintext part body on disk while uploading: %s", watch.leak)
	}
	watch.leak = ""
	watch.scan()
	if watch.leak != "" {
		t.Errorf("plaintext part body on disk after uploading: %s", watch.leak)
	}

	// A body that is too long or too short is refused without leaving a trace.
	for _, size := range []int64{int64(len(body)) - 1, int64(len(body)) + 1} {
		watch = &plaintextWatcher{body: body, root: dir, t: t}
		if _, err := l.UploadPart(ctx, "secret", id, 2, watch, size); !errors.Is(err, common.ErrInvalidArgument) {
			t.Errorf("size %d: err = %v, want ErrInvalidArgument", size, err)
		}
		if watch.leak != "" {
			t.Errorf("size %d: plaintext on disk: %s", size, watch.leak)
		}
	}
}

func TestLocal_EncrypterFactoryFailure(t *testing.T) {
	ctx := context.Background()
	l, _, _ := newClockedLocal(t, nil)
	id, _ := l.InitiateMultipartUpload(ctx, "k", nil)
	boom := errors.New("no key")
	l.SetAtRestEncrypterFactory(&errorEncrypterFactory{err: boom})
	if _, err := l.UploadPart(ctx, "k", id, 1, strings.NewReader("x"), 1); !errors.Is(err, boom) {
		t.Errorf("UploadPart = %v", err)
	}
}

func TestLocal_CompleteNeedsEncrypterForEncryptedParts(t *testing.T) {
	ctx := context.Background()
	l, _, _ := newClockedLocal(t, nil)
	l.SetAtRestEncrypterFactory(&mockEncrypterFactory{
		defaultKeyID: "key1",
		encrypters:   map[string]common.Encrypter{"key1": &mockEncrypter{keyID: "key1", algorithm: "A"}},
	})
	id, _ := l.InitiateMultipartUpload(ctx, "k", nil)
	info, err := l.UploadPart(ctx, "k", id, 1, strings.NewReader("x"), 1)
	if err != nil {
		t.Fatal(err)
	}
	l.SetAtRestEncrypterFactory(nil)
	err = l.CompleteMultipartUpload(ctx, "k", id, []common.CompletedPart{{PartNumber: 1, ETag: info.ETag}})
	if err == nil {
		t.Fatal("complete succeeded without the encrypter that wrote the parts")
	}
	if ok, _ := l.Exists(ctx, "k"); ok {
		t.Error("a failed complete left an object")
	}
}

func TestLocal_FaultInjection(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("cannot test permission errors as root")
	}
	ctx := context.Background()

	t.Run("staging path is a file", func(t *testing.T) {
		l, _, dir := newClockedLocal(t, nil)
		if err := os.WriteFile(filepath.Join(dir, ".objstore-multipart"), []byte("x"), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := l.InitiateMultipartUpload(ctx, "k", nil); err == nil {
			t.Error("Initiate succeeded with the staging path blocked")
		}
		if _, err := l.ListMultipartUploads(ctx, ""); err == nil {
			t.Error("List succeeded with the staging path blocked")
		}
	})

	t.Run("part file removed before complete", func(t *testing.T) {
		l, _, dir := newClockedLocal(t, nil)
		id, _ := l.InitiateMultipartUpload(ctx, "k", nil)
		info, err := l.UploadPart(ctx, "k", id, 1, strings.NewReader("data"), 4)
		if err != nil {
			t.Fatal(err)
		}
		entries, _ := os.ReadDir(filepath.Join(dir, ".objstore-multipart", id))
		for _, e := range entries {
			if e.Name() != "upload.json" && !strings.HasSuffix(e.Name(), ".json") {
				_ = os.Remove(filepath.Join(dir, ".objstore-multipart", id, e.Name()))
			}
		}
		err = l.CompleteMultipartUpload(ctx, "k", id, []common.CompletedPart{{PartNumber: 1, ETag: info.ETag}})
		if err == nil {
			t.Fatal("complete succeeded with a missing part file")
		}
		if ok, _ := l.Exists(ctx, "k"); ok {
			t.Error("a failed complete left an object")
		}
		if _, statErr := os.Stat(filepath.Join(dir, ".objstore-multipart", id)); statErr != nil {
			t.Errorf("a failed complete discarded the upload: %v", statErr)
		}
	})

	t.Run("object directory is blocked", func(t *testing.T) {
		l, _, dir := newClockedLocal(t, nil)
		if err := os.WriteFile(filepath.Join(dir, "block"), []byte("x"), 0600); err != nil {
			t.Fatal(err)
		}
		id, _ := l.InitiateMultipartUpload(ctx, "block/obj", nil)
		info, err := l.UploadPart(ctx, "block/obj", id, 1, strings.NewReader("data"), 4)
		if err != nil {
			t.Fatal(err)
		}
		err = l.CompleteMultipartUpload(ctx, "block/obj", id, []common.CompletedPart{{PartNumber: 1, ETag: info.ETag}})
		if err == nil {
			t.Error("complete succeeded with the object path blocked")
		}
	})

	t.Run("corrupt upload.json", func(t *testing.T) {
		l, _, dir := newClockedLocal(t, nil)
		id, _ := l.InitiateMultipartUpload(ctx, "k", nil)
		if err := os.WriteFile(filepath.Join(dir, ".objstore-multipart", id, "upload.json"), []byte("{"), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := l.UploadPart(ctx, "k", id, 1, strings.NewReader("x"), 1); !errors.Is(err, common.ErrUploadNotFound) {
			t.Errorf("UploadPart = %v", err)
		}
		if err := l.AbortMultipartUpload(ctx, "k", id); !errors.Is(err, common.ErrUploadNotFound) {
			t.Errorf("Abort = %v", err)
		}
		if ups, err := l.ListMultipartUploads(ctx, ""); err != nil || len(ups) != 0 {
			t.Errorf("List = %v, %v", ups, err)
		}
	})

	t.Run("corrupt part record", func(t *testing.T) {
		l, _, dir := newClockedLocal(t, nil)
		id, _ := l.InitiateMultipartUpload(ctx, "k", nil)
		info, err := l.UploadPart(ctx, "k", id, 1, strings.NewReader("data"), 4)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, ".objstore-multipart", id, "p00001.json"), []byte("{"), 0600); err != nil {
			t.Fatal(err)
		}
		err = l.CompleteMultipartUpload(ctx, "k", id, []common.CompletedPart{{PartNumber: 1, ETag: info.ETag}})
		if !errors.Is(err, common.ErrPartMismatch) {
			t.Errorf("Complete = %v, want ErrPartMismatch", err)
		}
	})

	t.Run("short body leaves no part", func(t *testing.T) {
		l, _, dir := newClockedLocal(t, nil)
		id, _ := l.InitiateMultipartUpload(ctx, "k", nil)
		if _, err := l.UploadPart(ctx, "k", id, 1, strings.NewReader("ab"), 4); !errors.Is(err, common.ErrInvalidArgument) {
			t.Errorf("UploadPart = %v", err)
		}
		entries, _ := os.ReadDir(filepath.Join(dir, ".objstore-multipart", id))
		if len(entries) != 1 {
			t.Errorf("leftover files after a failed part: %d entries", len(entries))
		}
	})
}

func TestLocal_PartReplacementKeepsOnePartFile(t *testing.T) {
	ctx := context.Background()
	l, _, dir := newClockedLocal(t, nil)
	id, _ := l.InitiateMultipartUpload(ctx, "k", nil)
	if _, err := l.UploadPart(ctx, "k", id, 1, strings.NewReader("first"), 5); err != nil {
		t.Fatal(err)
	}
	if _, err := l.UploadPart(ctx, "k", id, 1, strings.NewReader("second"), 6); err != nil {
		t.Fatal(err)
	}
	entries, _ := os.ReadDir(filepath.Join(dir, ".objstore-multipart", id))
	if len(entries) != 3 { // upload.json, the part record and one part file
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Errorf("entries after replacement: %v", names)
	}
}

func TestLocal_ListMultipartUploads_Filters(t *testing.T) {
	ctx := context.Background()
	l, _, dir := newClockedLocal(t, nil)
	_, _ = l.InitiateMultipartUpload(ctx, "logs/a", nil)
	_, _ = l.InitiateMultipartUpload(ctx, "logs/b", nil)
	_, _ = l.InitiateMultipartUpload(ctx, "data/c", nil)
	// A stray file and a leftover temporary directory are not uploads.
	_ = os.WriteFile(filepath.Join(dir, ".objstore-multipart", "stray"), []byte("x"), 0600)
	_ = os.MkdirAll(filepath.Join(dir, ".objstore-multipart", tmpFilePrefix+"123"), 0750)

	ups, err := l.ListMultipartUploads(ctx, "logs/")
	if err != nil || len(ups) != 2 || ups[0].Key != "logs/a" || ups[1].Key != "logs/b" {
		t.Errorf("filtered list = %+v, %v", ups, err)
	}
	all, _ := l.ListMultipartUploads(ctx, "")
	if len(all) != 3 {
		t.Errorf("full list = %d", len(all))
	}
}

func TestWalkObjects_SkipsOnlyTheRootStagingDirectory(t *testing.T) {
	dir := t.TempDir()
	for _, p := range []string{
		".objstore-multipart/id/p00001-abc",
		"sub/.objstore-multipart/keep",
		"plain",
	} {
		full := filepath.Join(dir, p)
		if err := os.MkdirAll(filepath.Dir(full), 0750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte("x"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	var seen []string
	err := walkObjects(context.Background(), dir, func(p string, _ os.DirEntry) error {
		rel, _ := filepath.Rel(dir, p)
		seen = append(seen, filepath.ToSlash(rel))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{"plain": true, "sub/.objstore-multipart/keep": true}
	if len(seen) != len(want) {
		t.Fatalf("seen = %v", seen)
	}
	for _, s := range seen {
		if !want[s] {
			t.Errorf("unexpected %s", s)
		}
	}
}
