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
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// openTestRoot opens dir as an os.Root that is closed when the test ends.
func openTestRoot(t *testing.T, dir string) *os.Root {
	t.Helper()
	r, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatalf("OpenRoot: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r
}

const outsideContent = "outside, must stay untouched"

// escapeFixture builds a store whose root holds two symlinks leading out of
// it: "link" to a file and "linkdir" to a directory, both outside the root.
func escapeFixture(t *testing.T) (l *Local, outsideFile, outsideDir string) {
	t.Helper()
	base := t.TempDir()
	outsideDir = filepath.Join(base, "outside")
	if err := os.Mkdir(outsideDir, 0750); err != nil {
		t.Fatal(err)
	}
	outsideFile = filepath.Join(outsideDir, "obj")
	if err := os.WriteFile(outsideFile, []byte(outsideContent), 0600); err != nil {
		t.Fatal(err)
	}
	rootDir := filepath.Join(base, "root")
	if err := os.Mkdir(rootDir, 0750); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outsideFile, filepath.Join(rootDir, "link")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if err := os.Symlink(outsideDir, filepath.Join(rootDir, "linkdir")); err != nil {
		t.Fatal(err)
	}
	l = newConfigured(t, rootDir)
	t.Cleanup(l.Close)
	return l, outsideFile, outsideDir
}

func assertOutsideUntouched(t *testing.T, outsideFile, outsideDir string) {
	t.Helper()
	got, err := os.ReadFile(outsideFile)
	if err != nil {
		t.Fatalf("outside file: %v", err)
	}
	if string(got) != outsideContent {
		t.Errorf("outside file changed: %q", got)
	}
	entries, err := os.ReadDir(outsideDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Errorf("outside dir gained entries: %v", names)
	}
}

func TestRootEscape_SymlinksOutsideRootFail(t *testing.T) {
	for _, key := range []string{"link", "linkdir/obj", "linkdir/new"} {
		t.Run(key, func(t *testing.T) {
			l, outsideFile, outsideDir := escapeFixture(t)
			ctx := context.Background()

			if rc, err := l.Get(key); err == nil {
				data, _ := io.ReadAll(rc)
				_ = rc.Close()
				t.Errorf("Get succeeded and read %q", data)
			}
			if err := l.Put(key, strings.NewReader("evil")); err == nil {
				t.Error("Put succeeded")
			}
			if err := l.PutWithMetadata(ctx, key, strings.NewReader("evil"), &common.Metadata{ContentType: "text/plain"}); err == nil {
				t.Error("PutWithMetadata succeeded")
			}
			if err := l.UpdateMetadata(ctx, key, &common.Metadata{ContentType: "text/plain"}); err == nil {
				t.Error("UpdateMetadata succeeded")
			}
			if ok, err := l.Exists(ctx, key); err == nil || ok {
				t.Errorf("Exists = %v, %v; want an error", ok, err)
			}
			if err := l.Delete(key); err == nil {
				t.Error("Delete succeeded")
			}

			id, err := l.InitiateMultipartUpload(ctx, key, nil)
			if err != nil {
				t.Fatalf("Initiate: %v", err)
			}
			part, err := l.UploadPart(ctx, key, id, 1, strings.NewReader("evil"), 4)
			if err != nil {
				t.Fatalf("UploadPart: %v", err)
			}
			if err := l.CompleteMultipartUpload(ctx, key, id, []common.CompletedPart{{PartNumber: 1, ETag: part.ETag}}); err == nil {
				t.Error("CompleteMultipartUpload succeeded")
			}

			assertOutsideUntouched(t, outsideFile, outsideDir)
		})
	}
}

func TestRootEscape_SymlinksInsideRootWork(t *testing.T) {
	rootDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(rootDir, "real.txt"), []byte("inside"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(rootDir, "realdir"), 0750); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("real.txt", filepath.Join(rootDir, "alias")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if err := os.Symlink("realdir", filepath.Join(rootDir, "aliasdir")); err != nil {
		t.Fatal(err)
	}
	l := newConfigured(t, rootDir)
	t.Cleanup(l.Close)
	ctx := context.Background()

	rc, err := l.Get("alias")
	if err != nil {
		t.Fatalf("Get through in-root link: %v", err)
	}
	data, _ := io.ReadAll(rc)
	_ = rc.Close()
	if string(data) != "inside" {
		t.Errorf("Get = %q", data)
	}
	if ok, err := l.Exists(ctx, "alias"); err != nil || !ok {
		t.Errorf("Exists = %v, %v", ok, err)
	}

	if err := l.Put("aliasdir/obj", strings.NewReader("via dir link")); err != nil {
		t.Fatalf("Put through in-root dir link: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(rootDir, "realdir", "obj"))
	if err != nil || string(got) != "via dir link" {
		t.Errorf("realdir/obj = %q, %v", got, err)
	}
	rc, err = l.Get("aliasdir/obj")
	if err != nil {
		t.Fatalf("Get aliasdir/obj: %v", err)
	}
	_ = rc.Close()
	if err := l.Delete("aliasdir/obj"); err != nil {
		t.Errorf("Delete aliasdir/obj: %v", err)
	}
}
