//go:build !windows

package srcsnap

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLock(t *testing.T) {
	g := newRig(t, Config{Method: ZFS}, fakeLocator{})
	unlock, err := g.s.lock()
	if err != nil {
		t.Fatal(err)
	}
	_, err = g.s.lock()
	wantErr(t, err, ErrBusy)
	if err := unlock(); err != nil {
		t.Fatal(err)
	}
	unlocked(t, g.s)

	// The directory is made if it is missing, private.
	dir := filepath.Join(t.TempDir(), "a", "b")
	g = newRig(t, Config{Method: ZFS, Dir: dir}, fakeLocator{})
	unlocked(t, g.s)
	if fi, err := os.Stat(dir); err != nil || fi.Mode().Perm() != 0o700 {
		t.Errorf("made %v, %v", fi.Mode(), err)
	}
}

func TestLockRefusesUnsafeDir(t *testing.T) {
	base := t.TempDir()
	open := filepath.Join(base, "open")
	if err := os.Mkdir(open, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(open, 0o777); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(base, "file")
	link := filepath.Join(base, "link")
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(t.TempDir(), link); err != nil {
		t.Fatal(err)
	}
	for _, dir := range []string{open, link, file, filepath.Join(file, "sub")} {
		g := newRig(t, Config{Method: ZFS, Dir: dir}, fakeLocator{})
		_, err := g.s.lock()
		wantErr(t, err, ErrFilesystem)
	}
}

func TestDefaultDir(t *testing.T) {
	if d := DefaultDir(); !filepath.IsAbs(d) || !strings.Contains(d, "objstore-backup") {
		t.Errorf("DefaultDir() = %q", d)
	}
	if d := hostLockDir(); d != DefaultDir() {
		t.Errorf("hostLockDir() = %q", d)
	}
}

func TestHostLock(t *testing.T) {
	a := newRig(t, Config{Method: ZFS}, fakeLocator{})
	b := newRig(t, Config{Method: ZFS, Dir: a.dir}, fakeLocator{})
	unlock, err := a.s.lock()
	if err != nil {
		t.Fatal(err)
	}
	// The snapshot directory is busy; the host lock taken first is let go.
	_, err = b.s.lock()
	wantErr(t, err, ErrBusy)
	unlockHost, err := lockDir(b.s.env.HostDir, hostLockName)
	if err != nil {
		t.Fatalf("host lock still held: %v", err)
	}
	if err := errors.Join(unlockHost(), unlock()); err != nil {
		t.Fatal(err)
	}

	// A host lock directory others can write is refused.
	open := t.TempDir()
	if err := os.Chmod(open, 0o777); err != nil {
		t.Fatal(err)
	}
	b.s.env.HostDir = open
	_, err = b.s.lock()
	wantErr(t, err, ErrFilesystem)
}

func TestExecRunner(t *testing.T) {
	ctx := context.Background()
	if _, err := (ExecRunner{}).Run(ctx, []string{"echo", "x"}); err == nil {
		t.Error("ran a tool by relative path")
	}
	echo, err := lookTool("echo")
	if err != nil {
		t.Skipf("no echo in the tool directories: %v", err)
	}
	out, err := ExecRunner{}.Run(ctx, []string{echo, "a b", "$HOME"})
	if err != nil || string(out) != "a b $HOME\n" {
		t.Errorf("Run = %q, %v", out, err)
	}
	ls, err := lookTool("ls")
	if err != nil {
		t.Skipf("no ls in the tool directories: %v", err)
	}
	_, err = ExecRunner{}.Run(ctx, []string{ls, filepath.Join(t.TempDir(), "missing")})
	if err == nil || !strings.Contains(err.Error(), "missing") {
		t.Errorf("Run err = %v, want the tool's stderr", err)
	}
}

func TestLookTool(t *testing.T) {
	if _, err := lookTool("objstore-backup-no-such-tool"); err == nil {
		t.Error("found a missing tool")
	}
	p, err := lookTool("sh")
	if err != nil {
		t.Skipf("no sh in the tool directories: %v", err)
	}
	if !filepath.IsAbs(p) {
		t.Errorf("lookTool(sh) = %q", p)
	}
}
