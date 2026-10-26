package srcsnap

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

const leftover = NamePrefix + "0123456789abcdef"

func mkdirs(t *testing.T, dir string, names ...string) {
	t.Helper()
	for _, n := range names {
		if err := os.Mkdir(filepath.Join(dir, n), 0o700); err != nil {
			t.Fatal(err)
		}
	}
}

func writeFiles(t *testing.T, dir string, files map[string]string) {
	t.Helper()
	for n, body := range files {
		if err := os.WriteFile(filepath.Join(dir, n), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

// The sweep unmounts and removes prefixed mount points and leaves every
// other name alone.
func TestSweepMounts(t *testing.T) {
	dir := t.TempDir()
	other := NamePrefix + "fedcba9876543210"
	mkdirs(t, dir, leftover, other, "keep", NamePrefix+"short")
	writeFiles(t, dir, map[string]string{NamePrefix + "aaaaaaaaaaaaaaaa": ""})
	loc := fakeLocator{mounts: map[string]bool{filepath.Join(dir, leftover): true, filepath.Join(dir, "keep"): true}}
	g := newRig(t, Config{Method: ZFS, Dir: dir}, loc)
	g.r.out["zfs list"] = "tank/a@" + leftover + "\ntank/a@daily\ntank/b@" + NamePrefix + "x\nbad ds@" + leftover + "\ntank/c\n"
	if err := g.s.Sweep(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	wantCalls(t, g.r.take(), []string{
		"umount " + filepath.Join(dir, leftover),
		"zfs list -H -t snapshot -o name",
		"zfs destroy tank/a@" + leftover,
	})
	if e := entries(t, dir); !slices.Equal(e, []string{"keep", NamePrefix + "aaaaaaaaaaaaaaaa", NamePrefix + "short"}) {
		t.Errorf("snapshot directory holds %q", e)
	}
}

func TestSweepMountFailures(t *testing.T) {
	dir := t.TempDir()
	mkdirs(t, dir, leftover)
	g := newRig(t, Config{Method: ZFS, Dir: dir}, fakeLocator{mounts: map[string]bool{filepath.Join(dir, leftover): true}})
	g.r.fail["umount"] = errors.New("busy")
	wantErr(t, g.s.Sweep(context.Background(), nil), ErrCommand)
	if e := entries(t, dir); !slices.Equal(e, []string{leftover}) {
		t.Errorf("removed a mounted leftover: %q", e)
	}

	g = newRig(t, Config{Method: ZFS, Dir: dir}, fakeLocator{err: errors.New("no mountinfo")})
	wantErr(t, g.s.Sweep(context.Background(), nil), ErrFilesystem)
}

func TestSweepLVM(t *testing.T) {
	g := newRig(t, Config{Method: LVM}, fakeLocator{})
	g.r.out["lvs"] = "  vg0/root\n  vg0/" + leftover + "\n  vg0/" + NamePrefix + "x\n  vg1/" + NamePrefix + "fedcba9876543210\n"
	g.r.fail["lvremove --force vg1/"] = errors.New("in use")
	wantErr(t, g.s.Sweep(context.Background(), nil), ErrCommand)
	wantCalls(t, g.r.take(), []string{
		"lvs --noheadings --separator / -o vg_name,lv_name",
		"lvremove --force vg0/" + leftover,
		"lvremove --force vg1/" + NamePrefix + "fedcba9876543210",
	})
	g.r.fail["lvs"] = errors.New("no lvm")
	wantErr(t, g.s.Sweep(context.Background(), nil), ErrCommand)
}

func TestSweepBtrfs(t *testing.T) {
	bdir := t.TempDir()
	mkdirs(t, bdir, leftover, "@home", NamePrefix+"fedcba9876543210")
	writeFiles(t, bdir, map[string]string{NamePrefix + "aaaaaaaaaaaaaaaa": ""})
	vols := map[string]Volume{"/home": {MountPoint: "/home", FSType: "btrfs", Subvolume: "/home"}}
	g := newRig(t, Config{Method: Btrfs, BtrfsDir: bdir}, fakeLocator{vols: vols})
	g.r.fail["btrfs subvolume delete "+filepath.Join(bdir, NamePrefix+"fedcba9876543210")] = errors.New("busy")
	wantErr(t, g.s.Sweep(context.Background(), []string{"/home/a", "/home/b"}), ErrCommand)
	wantCalls(t, g.r.take(), []string{
		"btrfs subvolume delete " + filepath.Join(bdir, leftover),
		"btrfs subvolume delete " + filepath.Join(bdir, NamePrefix+"fedcba9876543210"),
	})

	g = newRig(t, Config{Method: Btrfs, BtrfsDir: filepath.Join(bdir, "missing")}, fakeLocator{vols: vols})
	wantErr(t, g.s.Sweep(context.Background(), []string{"/home"}), ErrFilesystem)
}

func TestSweepBtrfsHidden(t *testing.T) {
	mp := t.TempDir()
	hidden := filepath.Join(mp, BtrfsHiddenDir)
	mkdirs(t, mp, BtrfsHiddenDir, filepath.Join(BtrfsHiddenDir, leftover))
	vols := map[string]Volume{mp: {MountPoint: mp, FSType: "btrfs", Subvolume: mp}}
	g := newRig(t, Config{Method: Btrfs}, fakeLocator{vols: vols})
	g.r.hook = func(line string) {
		f := strings.Fields(line)
		_ = os.Remove(f[len(f)-1])
	}
	if err := g.s.Sweep(context.Background(), []string{mp}); err != nil {
		t.Fatal(err)
	}
	wantCalls(t, g.r.take(), []string{"btrfs subvolume delete " + filepath.Join(hidden, leftover)})
	if _, err := os.Lstat(hidden); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("hidden dir left: %v", err)
	}

	// No hidden directory means no earlier run left anything.
	if err := g.s.Sweep(context.Background(), []string{mp}); err != nil {
		t.Fatal(err)
	}
	wantCalls(t, g.r.take(), nil)

	// A leftover that will not go keeps its directory.
	mkdirs(t, mp, BtrfsHiddenDir, filepath.Join(BtrfsHiddenDir, leftover))
	g = newRig(t, Config{Method: Btrfs}, fakeLocator{vols: vols})
	g.r.fail["btrfs subvolume delete"] = errors.New("busy")
	wantErr(t, g.s.Sweep(context.Background(), []string{mp}), ErrCommand)
	if _, err := os.Lstat(filepath.Join(hidden, leftover)); err != nil {
		t.Errorf("leftover: %v", err)
	}

	// A hidden path that is not a directory is reported.
	other := t.TempDir()
	writeFiles(t, other, map[string]string{BtrfsHiddenDir: ""})
	vols = map[string]Volume{other: {MountPoint: other, FSType: "btrfs", Subvolume: other}}
	g = newRig(t, Config{Method: Btrfs}, fakeLocator{vols: vols})
	wantErr(t, g.s.Sweep(context.Background(), []string{other}), ErrFilesystem)
	wantCalls(t, g.r.take(), nil)
}

func TestSweepAPFS(t *testing.T) {
	dir := t.TempDir()
	n2, n3 := NamePrefix+"fedcba9876543210", NamePrefix+"aaaaaaaaaaaaaaaa"
	writeFiles(t, dir, map[string]string{
		leftover + ".apfs": "2026-10-26-101112\n",
		n2 + ".apfs":       "2026-10-26-101112\n",
		n3 + ".apfs":       "garbage\n",
		"other.apfs":       "2026-10-25-000000\n",
		leftover + ".vss":  shadowID,
	})
	g := newRig(t, Config{Method: APFS, Dir: dir}, fakeLocator{})
	wantErr(t, g.s.Sweep(context.Background(), nil), ErrName)
	wantCalls(t, g.r.take(), []string{"tmutil deletelocalsnapshots 2026-10-26-101112"})
	if e := entries(t, dir); !slices.Equal(e, []string{leftover + ".vss", n3 + ".apfs", "other.apfs"}) {
		t.Errorf("snapshot directory holds %q", e)
	}

	writeFiles(t, dir, map[string]string{leftover + ".apfs": "2026-10-26-101112\n"})
	g.r.fail["tmutil"] = errors.New("exit status 1")
	wantErr(t, g.s.Sweep(context.Background(), nil), ErrCommand)
	if _, err := os.Stat(filepath.Join(dir, leftover+".apfs")); err != nil {
		t.Error("removed the state file of a snapshot it failed to delete")
	}
}

func TestSweepVSS(t *testing.T) {
	dir := t.TempDir()
	n2 := NamePrefix + "fedcba9876543210"
	writeFiles(t, dir, map[string]string{leftover + ".vss": shadowID + "\r\n", n2 + ".vss": "'; Remove-Item C:\\"})
	g := newRig(t, Config{Method: VSS, Dir: dir}, fakeLocator{})
	wantErr(t, g.s.Sweep(context.Background(), nil), ErrName)
	wantCalls(t, g.r.take(), []string{"powershell.exe -NoLogo -NoProfile -NonInteractive -Command " + vssDeleteScript(shadowID)})
	if e := entries(t, dir); !slices.Equal(e, []string{n2 + ".vss"}) {
		t.Errorf("snapshot directory holds %q", e)
	}

	writeFiles(t, dir, map[string]string{leftover + ".vss": shadowID})
	g.r.fail["powershell.exe"] = errors.New("exit status 1")
	wantErr(t, g.s.Sweep(context.Background(), nil), ErrCommand)
}

// Auto sweeps only the methods the sources' volumes use.
func TestSweepAuto(t *testing.T) {
	bdir := t.TempDir()
	mkdirs(t, bdir, leftover)
	vols := map[string]Volume{"/home": {MountPoint: "/home", FSType: "btrfs", Subvolume: "/home"}}
	g := newRig(t, Config{Method: Auto, BtrfsDir: bdir}, fakeLocator{vols: vols})
	if err := g.s.Sweep(context.Background(), []string{"/home"}); err != nil {
		t.Fatal(err)
	}
	wantCalls(t, g.r.take(), []string{"btrfs subvolume delete " + filepath.Join(bdir, leftover)})

	g = newRig(t, Config{Method: Auto}, fakeLocator{err: errors.New("gone")})
	wantErr(t, g.s.Sweep(context.Background(), []string{"/home"}), ErrFilesystem)
	wantCalls(t, g.r.take(), nil)
}

func TestSweepCanceled(t *testing.T) {
	g := newRig(t, Config{Method: ZFS}, fakeLocator{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	wantErr(t, g.s.Sweep(ctx, nil), ErrCanceled)
	wantCalls(t, g.r.take(), nil)
}

// A sweep waits for no one: it fails while a run holds the lock.
func TestSweepBusy(t *testing.T) {
	g := newRig(t, Config{Method: ZFS}, fakeLocator{vols: map[string]Volume{"/t": {MountPoint: "/t", Device: "t"}}})
	set, err := g.s.Create(context.Background(), []string{"/t"})
	if err != nil {
		t.Fatal(err)
	}
	wantErr(t, g.s.Sweep(context.Background(), nil), ErrBusy)
	_, err = g.s.Create(context.Background(), []string{"/t"})
	wantErr(t, err, ErrBusy)
	if err := set.Close(); err != nil {
		t.Fatal(err)
	}
	if err := g.s.Sweep(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
}

// TestSweepAcrossDirs runs two snapshotters with different snapshot
// directories on one host: the second's sweep leaves the first's live
// snapshot in place, and removes it once the first is gone without cleaning
// up, as a crash leaves it.
func TestSweepAcrossDirs(t *testing.T) {
	ctx := context.Background()
	for _, c := range []struct {
		method Method
		vol    Volume
		list   string
		want   []string
	}{
		{ZFS, Volume{MountPoint: "/t", Root: "/", FSType: "zfs", Device: "tank/t"},
			"tank/t@" + name1 + "\n",
			[]string{"zfs list -H -t snapshot -o name", "zfs destroy tank/t@" + name1}},
		{LVM, Volume{MountPoint: "/t", Root: "/", FSType: "ext4", Device: "/dev/mapper/vg0-t"},
			"  vg0/" + name1 + "\n",
			[]string{"lvs --noheadings --separator / -o vg_name,lv_name", "lvremove --force vg0/" + name1}},
	} {
		vols := map[string]Volume{"/t": c.vol}
		a := newRig(t, Config{Method: c.method}, fakeLocator{vols: vols})
		a.r.out["lvs"] = "  vg0/t\n"
		b := newRig(t, Config{Method: c.method}, fakeLocator{vols: vols})
		b.s.env.HostDir = a.s.env.HostDir
		b.r.out["zfs list"] = c.list
		b.r.out["lvs"] = c.list
		set, err := a.s.Create(ctx, []string{"/t"})
		if err != nil {
			t.Fatal(err)
		}
		wantErr(t, b.s.Sweep(ctx, nil), ErrBusy)
		_, err = b.s.Create(ctx, []string{"/t"})
		wantErr(t, err, ErrBusy)
		wantCalls(t, b.r.take(), nil)

		// The first run's locks go, its snapshot stays.
		if err := set.undo[0](ctx); err != nil {
			t.Fatal(err)
		}
		if err := b.s.Sweep(ctx, nil); err != nil {
			t.Fatal(err)
		}
		wantCalls(t, b.r.take(), c.want)
	}
}
