//go:build linux

package srcsnap

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestZFSBindMount refuses a source under a bind mount of a directory in a
// ZFS dataset, whose snapshot is not under the bind mount's .zfs, before
// any snapshot is made.
func TestZFSBindMount(t *testing.T) {
	es, err := parseMountinfo(strings.NewReader(mountinfo + "35 22 0:41 /data /srv/data rw - zfs tank/over rw\n"))
	if err != nil {
		t.Fatal(err)
	}
	v, ok := volumeAt(es, "/srv/data/x")
	if !ok || v.MountPoint != "/srv/data" || v.Root != "/data" || v.Device != "tank/over" {
		t.Fatalf("volumeAt = %+v, %v", v, ok)
	}
	plain, ok := volumeAt(es, "/tank/x")
	if !ok || plain.Root != "/" {
		t.Fatalf("volumeAt = %+v, %v", plain, ok)
	}
	for _, m := range []Method{ZFS, Auto} {
		g := newRig(t, Config{Method: m}, fakeLocator{vols: map[string]Volume{"/tank": plain, "/srv/data": v}})
		_, err := g.s.Create(context.Background(), []string{"/tank/x", "/srv/data/x"})
		wantErr(t, err, ErrUnsupported)
		wantCalls(t, g.r.take(), nil)
	}
}

func TestBtrfsMountedOnOtherRoot(t *testing.T) {
	// The root file system is not btrfs and a btrfs subvolume is mounted
	// below it, so the subvolume root is the mount point.
	mp := t.TempDir()
	es, err := parseMountinfo(strings.NewReader(mountinfo + fmt.Sprintf("36 22 0:42 /@home %s rw - btrfs /dev/sda2 rw,subvol=/@home\n", mp)))
	if err != nil {
		t.Fatal(err)
	}
	src := filepath.Join(mp, "u")
	v, ok := volumeAt(es, src)
	if !ok || v.FSType != "btrfs" || v.MountPoint != mp || v.Subvolume != mp || v.Root != "/@home" {
		t.Fatalf("volumeAt = %+v, %v", v, ok)
	}
	g := newRig(t, Config{Method: Btrfs}, fakeLocator{vols: map[string]Volume{mp: v}})
	set, err := g.s.Create(context.Background(), []string{src})
	if err != nil {
		t.Fatal(err)
	}
	hidden := filepath.Join(mp, BtrfsHiddenDir)
	snap := filepath.Join(hidden, name1)
	wantCalls(t, g.r.take(), []string{"btrfs subvolume snapshot -r " + mp + " " + snap})
	if got := set.Path(src); got != filepath.Join(snap, "u") {
		t.Errorf("Path = %q", got)
	}
	if got := set.Skip(); len(got) != 1 || got[0] != hidden {
		t.Errorf("Skip = %q", got)
	}
	if err := set.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(hidden); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("hidden dir left: %v", err)
	}
}

func TestPlatformLocator(t *testing.T) {
	dir := t.TempDir()
	link := filepath.Join(t.TempDir(), "link")
	if err := os.Symlink(dir, link); err != nil {
		t.Fatal(err)
	}
	v, resolved, err := platformLocator{}.Locate(link)
	if err != nil {
		t.Skipf("no mountinfo: %v", err)
	}
	real, err := filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatal(err)
	}
	if resolved != real || !within(v.MountPoint, resolved) || v.FSType == "" {
		t.Errorf("Locate = %+v, %q", v, resolved)
	}
	if v.FSType == "btrfs" && !within(v.MountPoint, v.Subvolume) {
		t.Errorf("subvolume %q is outside %q", v.Subvolume, v.MountPoint)
	}
	mounts, err := platformLocator{}.MountPoints()
	if err != nil || !mounts[v.MountPoint] {
		t.Errorf("MountPoints lacks %q: %v", v.MountPoint, err)
	}
	if _, _, err := (platformLocator{}).Locate(filepath.Join(dir, "missing")); err == nil {
		t.Error("located a missing path")
	}
	if s := btrfsSubvolume(real, "/"); !within("/", s) {
		t.Errorf("btrfsSubvolume = %q", s)
	}
}
