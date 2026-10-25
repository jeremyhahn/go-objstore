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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/internal/uploadtest"
	"github.com/jeremyhahn/go-objstore/pkg/common"
)

func newBuckets(t *testing.T, extra map[string]string) (*Buckets, string) {
	t.Helper()
	root := filepath.Join(t.TempDir(), "root")
	settings := map[string]string{"bucketRoot": root}
	for k, v := range extra {
		settings[k] = v
	}
	b, err := NewBuckets(settings)
	if err != nil {
		t.Fatal(err)
	}
	return b, root
}

func TestBuckets_Conformance(t *testing.T) {
	uploadtest.RunBuckets(t, func(t *testing.T) common.BucketManager {
		b, _ := newBuckets(t, nil)
		return b
	})
}

func TestNewBuckets_Settings(t *testing.T) {
	tests := []struct {
		name     string
		settings map[string]string
		wantErr  error
	}{
		{"ok", map[string]string{"bucketRoot": filepath.Join(t.TempDir(), "r")}, nil},
		{"missing", map[string]string{"path": t.TempDir()}, common.ErrBucketRootNotSet},
		{"nil settings", nil, common.ErrBucketRootNotSet},
		{"empty", map[string]string{"bucketRoot": ""}, common.ErrBucketRootNotSet},
		{"bad expiry", map[string]string{"bucketRoot": t.TempDir(), "multipartExpiry": "x"}, common.ErrInvalidArgument},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := NewBuckets(tt.settings)
			if tt.wantErr == nil {
				if err != nil || b == nil {
					t.Fatalf("NewBuckets = %v, %v", b, err)
				}
				return
			}
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("err = %v, want %v", err, tt.wantErr)
			}
			if !errors.Is(err, common.ErrInvalidArgument) {
				t.Errorf("err %v does not match ErrInvalidArgument", err)
			}
		})
	}
}

func TestNewBuckets_RootIsAFile(t *testing.T) {
	f := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(f, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewBuckets(map[string]string{"bucketRoot": f}); err == nil {
		t.Error("NewBuckets accepted a file as the root")
	}
}

func TestBuckets_CreateIsADirectoryUnderTheRoot(t *testing.T) {
	ctx := context.Background()
	b, root := newBuckets(t, nil)
	if err := b.CreateBucket(ctx, "alpha"); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(filepath.Join(root, "alpha"))
	if err != nil || !fi.IsDir() {
		t.Fatalf("bucket directory: %v", err)
	}
	err = b.CreateBucket(ctx, "alpha")
	if !errors.Is(err, common.ErrBucketAlreadyExists) || !errors.Is(err, common.ErrAlreadyExists) {
		t.Errorf("duplicate create = %v", err)
	}
}

func TestBuckets_NamesAreValidatedBeforeTouchingTheDisk(t *testing.T) {
	ctx := context.Background()
	b, root := newBuckets(t, nil)
	for _, name := range []string{"", "..", "../escape", "a/b", "UPPER", "ab"} {
		if err := b.CreateBucket(ctx, name); !errors.Is(err, common.ErrInvalidBucketName) {
			t.Errorf("CreateBucket(%q) = %v", name, err)
		}
		if err := b.DeleteBucket(ctx, name); !errors.Is(err, common.ErrInvalidBucketName) {
			t.Errorf("DeleteBucket(%q) = %v", name, err)
		}
		if _, err := b.Bucket(ctx, name); !errors.Is(err, common.ErrInvalidBucketName) {
			t.Errorf("Bucket(%q) = %v", name, err)
		}
	}
	entries, _ := os.ReadDir(filepath.Dir(root))
	for _, e := range entries {
		if e.Name() != "root" {
			t.Errorf("a bucket name escaped the root: %s", e.Name())
		}
	}
}

func TestBuckets_ListIgnoresWhatIsNotABucket(t *testing.T) {
	ctx := context.Background()
	b, root := newBuckets(t, nil)
	for _, n := range []string{"zeta", "alpha"} {
		if err := b.CreateBucket(ctx, n); err != nil {
			t.Fatal(err)
		}
	}
	_ = os.WriteFile(filepath.Join(root, "afile"), []byte("x"), 0600)
	_ = os.MkdirAll(filepath.Join(root, "Not_A_Bucket"), 0750)

	infos, err := b.ListBuckets(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(infos) != 2 || infos[0].Name != "alpha" || infos[1].Name != "zeta" {
		t.Fatalf("ListBuckets = %+v", infos)
	}
	if infos[0].Created.IsZero() || time.Since(infos[0].Created) > time.Hour {
		t.Errorf("Created = %v", infos[0].Created)
	}
}

func TestBuckets_ListMissingRoot(t *testing.T) {
	b, root := newBuckets(t, nil)
	if err := os.RemoveAll(root); err != nil {
		t.Fatal(err)
	}
	infos, err := b.ListBuckets(context.Background())
	if err != nil || len(infos) != 0 {
		t.Errorf("ListBuckets = %v, %v", infos, err)
	}
}

func TestBuckets_BucketStorageIsScopedToTheDirectory(t *testing.T) {
	ctx := context.Background()
	b, root := newBuckets(t, nil)
	_ = b.CreateBucket(ctx, "one")
	_ = b.CreateBucket(ctx, "two")
	one, err := b.Bucket(ctx, "one")
	if err != nil {
		t.Fatal(err)
	}
	two, _ := b.Bucket(ctx, "two")
	if err := one.Put("k", strings.NewReader("data")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "one", "k")); err != nil {
		t.Errorf("object not under the bucket directory: %v", err)
	}
	if ok, _ := two.Exists(ctx, "k"); ok {
		t.Error("an object leaked between buckets")
	}
	if _, ok := one.(common.MultipartUploader); !ok {
		t.Error("bucket storage is not a MultipartUploader")
	}
}

func TestBuckets_BucketNotFound(t *testing.T) {
	ctx := context.Background()
	b, _ := newBuckets(t, nil)
	if _, err := b.Bucket(ctx, "missing"); !errors.Is(err, common.ErrBucketNotFound) {
		t.Errorf("Bucket = %v", err)
	}
	if err := b.DeleteBucket(ctx, "missing"); !errors.Is(err, common.ErrBucketNotFound) {
		t.Errorf("DeleteBucket = %v", err)
	}
}

func TestBuckets_BucketThatIsAFile(t *testing.T) {
	ctx := context.Background()
	b, root := newBuckets(t, nil)
	if err := os.WriteFile(filepath.Join(root, "afile"), []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Bucket(ctx, "afile"); !errors.Is(err, common.ErrBucketNotFound) {
		t.Errorf("Bucket = %v", err)
	}
	if err := b.DeleteBucket(ctx, "afile"); err == nil {
		t.Error("DeleteBucket removed a file")
	}
	if _, err := os.Stat(filepath.Join(root, "afile")); err != nil {
		t.Errorf("file removed: %v", err)
	}
}

func TestBuckets_DeleteRules(t *testing.T) {
	ctx := context.Background()
	b, root := newBuckets(t, nil)
	_ = b.CreateBucket(ctx, "bkt")
	s, _ := b.Bucket(ctx, "bkt")

	if err := s.Put("k", strings.NewReader("x")); err != nil {
		t.Fatal(err)
	}
	if err := b.DeleteBucket(ctx, "bkt"); !errors.Is(err, common.ErrBucketNotEmpty) {
		t.Errorf("delete with an object = %v", err)
	}
	if err := s.Delete("k"); err != nil {
		t.Fatal(err)
	}

	id, err := s.(common.MultipartUploader).InitiateMultipartUpload(ctx, "m", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.DeleteBucket(ctx, "bkt"); !errors.Is(err, common.ErrBucketNotEmpty) {
		t.Errorf("delete with an open upload = %v", err)
	}
	if err := s.(common.MultipartUploader).AbortMultipartUpload(ctx, "m", id); err != nil {
		t.Fatal(err)
	}

	// The empty staging directory left behind does not make it non-empty.
	if err := b.DeleteBucket(ctx, "bkt"); err != nil {
		t.Fatalf("delete of an empty bucket: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "bkt")); !os.IsNotExist(err) {
		t.Errorf("directory survived: %v", err)
	}
	if err := b.CreateBucket(ctx, "bkt"); err != nil {
		t.Errorf("name not reusable: %v", err)
	}
}

func TestBuckets_ChildrenInheritSettings(t *testing.T) {
	ctx := context.Background()
	b, _ := newBuckets(t, map[string]string{"multipartExpiry": "1h", "runLifecycle": "true"})
	c := &fakeClock{t: time.Now()}
	b.now = c.now
	_ = b.CreateBucket(ctx, "bkt")
	s, err := b.Bucket(ctx, "bkt")
	if err != nil {
		t.Fatal(err)
	}
	l := s.(*Local)
	defer l.Close()
	if l.expiry() != time.Hour {
		t.Errorf("expiry = %v", l.expiry())
	}
	if l.lifecycleCancel != nil {
		t.Error("a bucket started its own background lifecycle goroutine")
	}
	mu := s.(common.MultipartUploader)
	id, _ := mu.InitiateMultipartUpload(ctx, "k", nil)
	c.t = c.t.Add(2 * time.Hour)
	if _, err := mu.UploadPart(ctx, "k", id, 1, strings.NewReader("x"), 1); !errors.Is(err, common.ErrUploadNotFound) {
		t.Errorf("clock or expiry not inherited: %v", err)
	}
}

func TestBuckets_FaultInjection(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("cannot test permission errors as root")
	}
	ctx := context.Background()

	t.Run("root removed and replaced by a file", func(t *testing.T) {
		b, root := newBuckets(t, nil)
		if err := os.RemoveAll(root); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(root, []byte("x"), 0600); err != nil {
			t.Fatal(err)
		}
		if err := b.CreateBucket(ctx, "bkt"); err == nil {
			t.Error("CreateBucket succeeded under a file")
		}
		if _, err := b.ListBuckets(ctx); err == nil {
			t.Error("ListBuckets succeeded under a file")
		}
	})

	t.Run("root recreated after removal", func(t *testing.T) {
		b, root := newBuckets(t, nil)
		if err := os.RemoveAll(root); err != nil {
			t.Fatal(err)
		}
		if err := b.CreateBucket(ctx, "bkt"); err != nil {
			t.Errorf("CreateBucket did not recreate the root: %v", err)
		}
	})

	t.Run("unreadable root", func(t *testing.T) {
		b, root := newBuckets(t, nil)
		if err := os.Chmod(root, 0); err != nil {
			t.Fatal(err)
		}
		defer func() { _ = os.Chmod(root, 0750) }()
		if _, err := b.ListBuckets(ctx); err == nil {
			t.Error("ListBuckets succeeded on an unreadable root")
		}
	})
}

func TestLocal_AsBucketManager(t *testing.T) {
	ctx := context.Background()
	root := filepath.Join(t.TempDir(), "root")
	l := New().(*Local)
	if err := l.Configure(map[string]string{"path": t.TempDir(), "bucketRoot": root}, noCreds); err != nil {
		t.Fatal(err)
	}
	var s common.Storage = l
	bm, ok := s.(common.BucketManager)
	if !ok {
		t.Fatal("Local is not a BucketManager")
	}
	if err := bm.CreateBucket(ctx, "bkt"); err != nil {
		t.Fatal(err)
	}
	infos, err := bm.ListBuckets(ctx)
	if err != nil || len(infos) != 1 || infos[0].Name != "bkt" {
		t.Fatalf("ListBuckets = %+v, %v", infos, err)
	}
	st, err := bm.Bucket(ctx, "bkt")
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Put("k", strings.NewReader("x")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "bkt", "k")); err != nil {
		t.Errorf("object not under the bucket root: %v", err)
	}
	if err := bm.DeleteBucket(ctx, "bkt"); !errors.Is(err, common.ErrBucketNotEmpty) {
		t.Errorf("DeleteBucket = %v", err)
	}
	// The storage path itself is not a bucket root.
	if _, err := os.Stat(filepath.Join(l.path, "bkt")); !os.IsNotExist(err) {
		t.Errorf("bucket created under the storage path: %v", err)
	}
}

func TestLocal_AsBucketManager_WithoutBucketRoot(t *testing.T) {
	ctx := context.Background()
	l := newConfigured(t, t.TempDir())
	if err := l.CreateBucket(ctx, "bkt"); !errors.Is(err, common.ErrBucketRootNotSet) {
		t.Errorf("CreateBucket = %v", err)
	}
	if err := l.DeleteBucket(ctx, "bkt"); !errors.Is(err, common.ErrBucketRootNotSet) {
		t.Errorf("DeleteBucket = %v", err)
	}
	if _, err := l.ListBuckets(ctx); !errors.Is(err, common.ErrBucketRootNotSet) {
		t.Errorf("ListBuckets = %v", err)
	}
	if _, err := l.Bucket(ctx, "bkt"); !errors.Is(err, common.ErrBucketRootNotSet) {
		t.Errorf("Bucket = %v", err)
	}
}

func atRestFactory() common.EncrypterFactory {
	return &mockEncrypterFactory{
		defaultKeyID: "key1",
		encrypters:   map[string]common.Encrypter{"key1": &mockEncrypter{keyID: "key1", algorithm: "AES256-TEST"}},
	}
}

// requireSealed fails the test unless the file at path holds ciphertext.
func requireSealed(t *testing.T, path string) {
	t.Helper()
	raw, err := os.ReadFile(path) // #nosec G304 -- path is inside the test's temp dir
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(raw, []byte("ENCRYPTED:")) {
		t.Errorf("%s is not encrypted at rest: %q", path, raw)
	}
}

func TestBuckets_ChildrenEncryptAtRest(t *testing.T) {
	ctx := context.Background()

	configure := func(t *testing.T, before bool) (*Local, string) {
		root := filepath.Join(t.TempDir(), "root")
		l := New().(*Local)
		if before {
			l.SetAtRestEncrypterFactory(atRestFactory())
		}
		if err := l.Configure(map[string]string{"path": t.TempDir(), "bucketRoot": root}, noCreds); err != nil {
			t.Fatal(err)
		}
		if !before {
			l.SetAtRestEncrypterFactory(atRestFactory())
		}
		return l, root
	}

	for _, tc := range []struct {
		name   string
		before bool
	}{
		{"factory set before Configure", true},
		{"factory set after Configure", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l, root := configure(t, tc.before)
			if err := l.CreateBucket(ctx, "bkt"); err != nil {
				t.Fatal(err)
			}
			st, err := l.Bucket(ctx, "bkt")
			if err != nil {
				t.Fatal(err)
			}

			if err := st.Put("plain", strings.NewReader("secret")); err != nil {
				t.Fatal(err)
			}
			requireSealed(t, filepath.Join(root, "bkt", "plain"))
			rc, err := st.Get("plain")
			if err != nil {
				t.Fatal(err)
			}
			got, _ := io.ReadAll(rc)
			_ = rc.Close()
			if string(got) != "secret" {
				t.Errorf("Get = %q, want the plaintext back", got)
			}

			mp, ok := st.(common.MultipartUploader)
			if !ok {
				t.Fatal("bucket storage is not a MultipartUploader")
			}
			id, err := mp.InitiateMultipartUpload(ctx, "big", nil)
			if err != nil {
				t.Fatal(err)
			}
			part, err := mp.UploadPart(ctx, "big", id, 1, strings.NewReader("secret"), 6)
			if err != nil {
				t.Fatal(err)
			}
			if err := mp.CompleteMultipartUpload(ctx, "big", id, []common.CompletedPart{{PartNumber: 1, ETag: part.ETag}}); err != nil {
				t.Fatal(err)
			}
			requireSealed(t, filepath.Join(root, "bkt", "big"))
		})
	}
}

func TestBuckets_SetAtRestEncrypterFactory(t *testing.T) {
	ctx := context.Background()
	b, root := newBuckets(t, nil)
	b.SetAtRestEncrypterFactory(atRestFactory())
	if err := b.CreateBucket(ctx, "bkt"); err != nil {
		t.Fatal(err)
	}
	st, err := b.Bucket(ctx, "bkt")
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Put("k", strings.NewReader("secret")); err != nil {
		t.Fatal(err)
	}
	requireSealed(t, filepath.Join(root, "bkt", "k"))
}

func TestBuckets_DeleteAfterNestedKeys(t *testing.T) {
	ctx := context.Background()
	b, root := newBuckets(t, nil)
	if err := b.CreateBucket(ctx, "bkt"); err != nil {
		t.Fatal(err)
	}
	st, err := b.Bucket(ctx, "bkt")
	if err != nil {
		t.Fatal(err)
	}

	if err := st.Put("dir/sub/object", strings.NewReader("x")); err != nil {
		t.Fatal(err)
	}
	if err := b.DeleteBucket(ctx, "bkt"); !errors.Is(err, common.ErrBucketNotEmpty) {
		t.Fatalf("DeleteBucket with a live nested object = %v, want ErrBucketNotEmpty", err)
	}
	if err := st.Delete("dir/sub/object"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "bkt", "dir")); !os.IsNotExist(err) {
		t.Errorf("the empty object directories were left behind: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "bkt")); err != nil {
		t.Errorf("deleting the last object removed the bucket: %v", err)
	}
	if err := b.DeleteBucket(ctx, "bkt"); err != nil {
		t.Fatalf("DeleteBucket after the nested object was deleted = %v", err)
	}
}

func TestBuckets_DeleteKeepsDirectoriesInUse(t *testing.T) {
	ctx := context.Background()
	b, root := newBuckets(t, nil)
	if err := b.CreateBucket(ctx, "bkt"); err != nil {
		t.Fatal(err)
	}
	st, err := b.Bucket(ctx, "bkt")
	if err != nil {
		t.Fatal(err)
	}

	// A sibling keeps the shared directory.
	for _, k := range []string{"dir/a", "dir/b"} {
		if err := st.Put(k, strings.NewReader("x")); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.Delete("dir/a"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "bkt", "dir", "b")); err != nil {
		t.Errorf("the sibling object is gone: %v", err)
	}
	if err := st.Delete("dir/b"); err != nil {
		t.Fatal(err)
	}

	// An upload in progress keeps the bucket, even with no object in it.
	mp := st.(common.MultipartUploader)
	id, err := mp.InitiateMultipartUpload(ctx, "dir/big", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Put("dir/c", strings.NewReader("x")); err != nil {
		t.Fatal(err)
	}
	if err := st.Delete("dir/c"); err != nil {
		t.Fatal(err)
	}
	if err := b.DeleteBucket(ctx, "bkt"); !errors.Is(err, common.ErrBucketNotEmpty) {
		t.Fatalf("DeleteBucket with an upload in progress = %v, want ErrBucketNotEmpty", err)
	}
	if _, err := mp.UploadPart(ctx, "dir/big", id, 1, strings.NewReader("x"), 1); err != nil {
		t.Errorf("the upload did not survive the deletes: %v", err)
	}
	if err := mp.AbortMultipartUpload(ctx, "dir/big", id); err != nil {
		t.Fatal(err)
	}
	if err := b.DeleteBucket(ctx, "bkt"); err != nil {
		t.Fatalf("DeleteBucket after the abort = %v", err)
	}
}

func TestLocal_DeletePrunesEmptyDirectoriesOnly(t *testing.T) {
	l := newConfigured(t, t.TempDir())
	if err := l.Put("a/b/c/object", strings.NewReader("x")); err != nil {
		t.Fatal(err)
	}
	if err := l.Put("a/other", strings.NewReader("x")); err != nil {
		t.Fatal(err)
	}
	if err := l.Delete("a/b/c/object"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(l.path, "a", "b")); !os.IsNotExist(err) {
		t.Errorf("empty directory a/b was left behind: %v", err)
	}
	if _, err := os.Stat(filepath.Join(l.path, "a", "other")); err != nil {
		t.Errorf("a/other = %v", err)
	}
	if err := l.Delete("a/other"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(l.path); err != nil {
		t.Errorf("the store's own directory was pruned: %v", err)
	}
	if err := l.Delete("a/other"); !errors.Is(err, common.ErrKeyNotFound) {
		t.Errorf("second Delete = %v, want ErrKeyNotFound", err)
	}
}

func TestLocal_PutRacesWithPruning(t *testing.T) {
	l := newConfigured(t, t.TempDir())
	const writers = 8
	// Several levels deep, so a prune can remove a parent between the mkdir of
	// one level and the next.
	dir := filepath.Join(l.path, "shared", "a", "b", "c", "dir")

	// Pruners model the Deletes of other keys, and of other processes sharing
	// the root, which prune the same directories without ever writing.
	stop := make(chan struct{})
	var pruners sync.WaitGroup
	for p := 0; p < 4; p++ {
		pruners.Add(1)
		go func() {
			defer pruners.Done()
			for {
				select {
				case <-stop:
					return
				default:
					l.pruneEmptyDirs(dir)
				}
			}
		}()
	}

	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for g := 0; g < writers; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			key := fmt.Sprintf("shared/a/b/c/dir/k%d", g)
			for i := 0; i < 25; i++ {
				if err := l.Put(key, strings.NewReader("x")); err != nil {
					errs <- fmt.Errorf("Put: %w", err)
					return
				}
				if err := l.Delete(key); err != nil {
					errs <- fmt.Errorf("Delete: %w", err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	close(stop)
	pruners.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func TestLocal_Configure_BucketRootErrors(t *testing.T) {
	f := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(f, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	l := New().(*Local)
	if err := l.Configure(map[string]string{"path": t.TempDir(), "bucketRoot": f}, noCreds); err == nil {
		t.Error("Configure accepted a file as the bucket root")
	}
}
