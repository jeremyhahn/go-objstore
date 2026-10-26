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

var lvmVol = Volume{MountPoint: "/srv", Root: "/", FSType: "xfs", Device: "/dev/mapper/vg0-data"}

const lvsData = "lvs --noheadings --separator / -o vg_name,lv_name /dev/mapper/vg0-data"

func TestLVM(t *testing.T) {
	g := newRig(t, Config{Method: LVM}, fakeLocator{vols: map[string]Volume{"/srv": lvmVol}})
	g.r.out[lvsData] = "  vg0/data\n"
	ctx := context.Background()
	set, err := g.s.Create(ctx, []string{"/srv", "/srv/a/b"})
	if err != nil {
		t.Fatal(err)
	}
	mnt := filepath.Join(g.dir, name1)
	wantCalls(t, g.r.take(), []string{
		lvsData,
		"lvcreate --snapshot --name " + name1 + " --extents 10%ORIGIN vg0/data",
		"mount -t xfs -o ro,nouuid /dev/vg0/" + name1 + " " + mnt,
	})
	if got := set.Path("/srv"); got != mnt {
		t.Errorf("Path(/srv) = %q, want %q", got, mnt)
	}
	if got, want := set.Path("/srv/a/b"), filepath.Join(mnt, "a/b"); got != want {
		t.Errorf("Path(/srv/a/b) = %q, want %q", got, want)
	}
	if got := set.Path("/other"); got != "/other" {
		t.Errorf("Path(/other) = %q", got)
	}
	if err := set.Close(); err != nil {
		t.Fatal(err)
	}
	wantCalls(t, g.r.take(), []string{"umount " + mnt, "lvremove --force vg0/" + name1})
	if e := entries(t, g.dir); len(e) != 0 {
		t.Errorf("left behind %q", e)
	}
	unlocked(t, g.s)
	for _, want := range []string{"method=lvm", "volume=/dev/mapper/vg0-data", "snapshot=" + name1, "source snapshot mounted", "source snapshot removed"} {
		if !strings.Contains(g.log.String(), want) {
			t.Errorf("log lacks %q", want)
		}
	}
	// A second Close does nothing more.
	if err := set.Close(); err != nil {
		t.Fatal(err)
	}
	wantCalls(t, g.r.take(), nil)
}

func TestLVMSizeAndExt4(t *testing.T) {
	v := lvmVol
	v.FSType = "ext4"
	v.Root = "/data"
	g := newRig(t, Config{Method: LVM, LVMSize: "4G"}, fakeLocator{vols: map[string]Volume{"/srv": v}})
	g.r.out[lvsData] = "vg0/data\n"
	set, err := g.s.Create(context.Background(), []string{"/srv/x"})
	if err != nil {
		t.Fatal(err)
	}
	defer set.Close()
	mnt := filepath.Join(g.dir, name1)
	wantCalls(t, g.r.take(), []string{
		lvsData,
		"lvcreate --snapshot --name " + name1 + " --size 4G vg0/data",
		"mount -t ext4 -o ro /dev/vg0/" + name1 + " " + mnt,
	})
	if got, want := set.Path("/srv/x"), filepath.Join(mnt, "data/x"); got != want {
		t.Errorf("Path = %q, want %q", got, want)
	}
}

func TestLVMRefusals(t *testing.T) {
	for name, tc := range map[string]struct {
		vol Volume
		lvs string
	}{
		"no logical volume":  {lvmVol, "\n"},
		"two logical volume": {lvmVol, "vg0/a\nvg0/b\n"},
		"relative device":    {Volume{MountPoint: "/srv", Root: "/", FSType: "xfs", Device: "vg0-data"}, ""},
		"odd fs type":        {Volume{MountPoint: "/srv", Root: "/", FSType: "xfs,ro", Device: "/dev/mapper/vg0-data"}, ""},
	} {
		t.Run(name, func(t *testing.T) {
			g := newRig(t, Config{Method: LVM}, fakeLocator{vols: map[string]Volume{"/srv": tc.vol}})
			g.r.out[lvsData] = tc.lvs
			_, err := g.s.Create(context.Background(), []string{"/srv"})
			wantErr(t, err, ErrUnsupported)
			for _, c := range g.r.take() {
				if strings.HasPrefix(c, "lvcreate") {
					t.Errorf("ran %q", c)
				}
			}
			unlocked(t, g.s)
		})
	}
}

// A mount that fails removes the snapshot volume and the mount point, and
// unmounts nothing.
func TestLVMMountFails(t *testing.T) {
	g := newRig(t, Config{Method: LVM}, fakeLocator{vols: map[string]Volume{"/srv": lvmVol}})
	g.r.out[lvsData] = "vg0/data\n"
	g.r.fail["mount "] = errors.New("exit status 32")
	set, err := g.s.Create(context.Background(), []string{"/srv"})
	if set != nil {
		t.Fatal("Set returned with an error")
	}
	wantErr(t, err, ErrCommand)
	calls := g.r.take()
	wantCalls(t, calls[len(calls)-1:], []string{"lvremove --force vg0/" + name1})
	if e := entries(t, g.dir); len(e) != 0 {
		t.Errorf("left behind %q", e)
	}
	unlocked(t, g.s)
}

func TestBtrfs(t *testing.T) {
	// The subvolume is the mount point, so the snapshot goes to a hidden
	// directory at the top of the same file system.
	mp := t.TempDir()
	vol := Volume{MountPoint: mp, Root: "/@home", FSType: "btrfs", Device: "/dev/sda2", Subvolume: mp}
	g := newRig(t, Config{Method: Btrfs}, fakeLocator{vols: map[string]Volume{mp: vol}})
	docs := filepath.Join(mp, "u", "docs")
	set, err := g.s.Create(context.Background(), []string{mp, docs})
	if err != nil {
		t.Fatal(err)
	}
	hidden := filepath.Join(mp, BtrfsHiddenDir)
	snap := filepath.Join(hidden, name1)
	wantCalls(t, g.r.take(), []string{"btrfs subvolume snapshot -r " + mp + " " + snap})
	if got := set.Path(mp); got != snap {
		t.Errorf("Path(mp) = %q", got)
	}
	if got := set.Path(docs); got != filepath.Join(snap, "u", "docs") {
		t.Errorf("Path(docs) = %q", got)
	}
	if fi, err := os.Lstat(hidden); err != nil || !fi.IsDir() || fi.Mode().Perm() != 0o700 {
		t.Fatalf("hidden dir %v, %v", fi, err)
	}
	if got := set.Skip(); !slices.Equal(got, []string{hidden}) {
		t.Errorf("Skip = %q", got)
	}
	if err := set.Close(); err != nil {
		t.Fatal(err)
	}
	wantCalls(t, g.r.take(), []string{"btrfs subvolume delete " + snap})
	if _, err := os.Lstat(hidden); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("hidden dir left: %v", err)
	}
}

func TestBtrfsNested(t *testing.T) {
	vol := Volume{MountPoint: "/", Root: "/", FSType: "btrfs", Device: "/dev/sda2", Subvolume: "/data/b"}
	g := newRig(t, Config{Method: Btrfs}, fakeLocator{vols: map[string]Volume{"/data/b": vol}})
	set, err := g.s.Create(context.Background(), []string{"/data/b/x"})
	if err != nil {
		t.Fatal(err)
	}
	snap := "/data/" + name1
	wantCalls(t, g.r.take(), []string{"btrfs subvolume snapshot -r /data/b " + snap})
	if got := set.Path("/data/b/x"); got != snap+"/x" {
		t.Errorf("Path = %q", got)
	}
	if got := set.Skip(); !slices.Equal(got, []string{snap}) {
		t.Errorf("Skip = %q", got)
	}
	var none *Set
	if got := none.Skip(); got != nil {
		t.Errorf("nil Skip = %q", got)
	}
	if err := set.Close(); err != nil {
		t.Fatal(err)
	}
	wantCalls(t, g.r.take(), []string{"btrfs subvolume delete " + snap})
}

func TestBtrfsHiddenKept(t *testing.T) {
	// A hidden directory that still holds something after the run stays.
	mp := t.TempDir()
	hidden := filepath.Join(mp, BtrfsHiddenDir)
	a := Volume{MountPoint: mp, FSType: "btrfs", Device: "/dev/sda2", Subvolume: mp}
	g := newRig(t, Config{Method: Btrfs}, fakeLocator{vols: map[string]Volume{mp: a}})
	g.r.hook = func(line string) {
		f := strings.Fields(line)
		switch {
		case strings.HasPrefix(line, "btrfs subvolume snapshot"):
			_ = os.Mkdir(f[len(f)-1], 0o700)
		case strings.HasPrefix(line, "btrfs subvolume delete"):
			_ = os.Remove(f[len(f)-1])
		}
	}
	set, err := g.s.Create(context.Background(), []string{mp})
	if err != nil {
		t.Fatal(err)
	}
	foreign := filepath.Join(hidden, "keep")
	if err := os.WriteFile(foreign, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := set.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(foreign); err != nil {
		t.Fatalf("foreign file: %v", err)
	}
	if entries, _ := os.ReadDir(hidden); len(entries) != 1 {
		t.Errorf("hidden dir holds %d entries", len(entries))
	}
}

func TestBtrfsHiddenRefused(t *testing.T) {
	mp := t.TempDir()
	vol := Volume{MountPoint: mp, FSType: "btrfs", Device: "/dev/sda2", Subvolume: mp}
	if err := os.Symlink(t.TempDir(), filepath.Join(mp, BtrfsHiddenDir)); err != nil {
		t.Fatal(err)
	}
	g := newRig(t, Config{Method: Btrfs}, fakeLocator{vols: map[string]Volume{mp: vol}})
	_, err := g.s.Create(context.Background(), []string{mp})
	wantErr(t, err, ErrFilesystem)
	wantCalls(t, g.r.take(), nil)
}

func TestBtrfsHiddenErrors(t *testing.T) {
	// A mount point that cannot hold the hidden directory.
	mp := filepath.Join(t.TempDir(), "missing")
	vol := Volume{MountPoint: mp, FSType: "btrfs", Device: "/dev/sda2", Subvolume: mp}
	g := newRig(t, Config{Method: Btrfs}, fakeLocator{vols: map[string]Volume{mp: vol}})
	_, err := g.s.Create(context.Background(), []string{mp})
	wantErr(t, err, ErrFilesystem)
	wantCalls(t, g.r.take(), nil)

	// A hidden directory replaced by a file during the run fails cleanup.
	mp = t.TempDir()
	hidden := filepath.Join(mp, BtrfsHiddenDir)
	vol = Volume{MountPoint: mp, FSType: "btrfs", Device: "/dev/sda2", Subvolume: mp}
	g = newRig(t, Config{Method: Btrfs}, fakeLocator{vols: map[string]Volume{mp: vol}})
	g.r.hook = func(line string) {
		if strings.HasPrefix(line, "btrfs subvolume delete") {
			_ = os.Remove(hidden)
			_ = os.WriteFile(hidden, nil, 0o600)
		}
	}
	set, err := g.s.Create(context.Background(), []string{mp})
	if err != nil {
		t.Fatal(err)
	}
	wantErr(t, set.Close(), ErrFilesystem)
}

func TestBtrfsDirAndTwoSubvolumes(t *testing.T) {
	bdir := t.TempDir()
	vols := map[string]Volume{
		"/data":   {MountPoint: "/data", FSType: "btrfs", Subvolume: "/data"},
		"/data/b": {MountPoint: "/data", FSType: "btrfs", Subvolume: "/data/b"},
	}
	g := newRig(t, Config{Method: Btrfs, BtrfsDir: bdir}, fakeLocator{vols: vols})
	set, err := g.s.Create(context.Background(), []string{"/data/a", "/data/b/c"})
	if err != nil {
		t.Fatal(err)
	}
	s1, s2 := filepath.Join(bdir, name1), filepath.Join(bdir, name2)
	wantCalls(t, g.r.take(), []string{
		"btrfs subvolume snapshot -r /data " + s1,
		"btrfs subvolume snapshot -r /data/b " + s2,
	})
	if set.Path("/data/a") != s1+"/a" || set.Path("/data/b/c") != s2+"/c" {
		t.Errorf("paths %v", set.paths)
	}
	if err := set.Close(); err != nil {
		t.Fatal(err)
	}
	wantCalls(t, g.r.take(), []string{"btrfs subvolume delete " + s2, "btrfs subvolume delete " + s1})
}

func TestZFS(t *testing.T) {
	vols := map[string]Volume{
		"/tank/a": {MountPoint: "/tank/a", FSType: "zfs", Device: "tank/a"},
		"/tank/b": {MountPoint: "/tank/b", FSType: "zfs", Device: "tank/b"},
	}
	g := newRig(t, Config{Method: ZFS}, fakeLocator{vols: vols})
	set, err := g.s.Create(context.Background(), []string{"/tank/a/x", "/tank/b", "/tank/a/y"})
	if err != nil {
		t.Fatal(err)
	}
	wantCalls(t, g.r.take(), []string{"zfs snapshot tank/a@" + name1, "zfs snapshot tank/b@" + name2})
	for src, want := range map[string]string{
		"/tank/a/x": "/tank/a/.zfs/snapshot/" + name1 + "/x",
		"/tank/a/y": "/tank/a/.zfs/snapshot/" + name1 + "/y",
		"/tank/b":   "/tank/b/.zfs/snapshot/" + name2,
	} {
		if got := set.Path(src); got != want {
			t.Errorf("Path(%s) = %q, want %q", src, got, want)
		}
	}
	if err := set.Close(); err != nil {
		t.Fatal(err)
	}
	wantCalls(t, g.r.take(), []string{"zfs destroy tank/b@" + name2, "zfs destroy tank/a@" + name1})
}

// A failure on the second volume removes only the first volume's snapshot.
func TestZFSSecondFails(t *testing.T) {
	vols := map[string]Volume{
		"/tank/a": {MountPoint: "/tank/a", FSType: "zfs", Device: "tank/a"},
		"/tank/b": {MountPoint: "/tank/b", FSType: "zfs", Device: "tank/b"},
	}
	g := newRig(t, Config{Method: ZFS}, fakeLocator{vols: vols})
	g.r.fail["zfs snapshot tank/b@"] = errors.New("out of space")
	_, err := g.s.Create(context.Background(), []string{"/tank/a", "/tank/b"})
	wantErr(t, err, ErrCommand)
	wantCalls(t, g.r.take(), []string{
		"zfs snapshot tank/a@" + name1,
		"zfs snapshot tank/b@" + name2,
		"zfs destroy tank/a@" + name1,
	})
	unlocked(t, g.s)
}

func TestZFSBadDataset(t *testing.T) {
	g := newRig(t, Config{Method: ZFS}, fakeLocator{vols: map[string]Volume{"/t": {MountPoint: "/t", Device: "-t; x"}}})
	_, err := g.s.Create(context.Background(), []string{"/t"})
	wantErr(t, err, ErrUnsupported)
	wantCalls(t, g.r.take(), nil)
}

// Canceling between volumes stops creation and still removes the first.
func TestCancelMidway(t *testing.T) {
	vols := map[string]Volume{
		"/tank/a": {MountPoint: "/tank/a", FSType: "zfs", Device: "tank/a"},
		"/tank/b": {MountPoint: "/tank/b", FSType: "zfs", Device: "tank/b"},
	}
	g := newRig(t, Config{Method: ZFS}, fakeLocator{vols: vols})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	g.r.hook = func(line string) {
		if strings.HasPrefix(line, "zfs snapshot tank/a@") {
			cancel()
		}
	}
	_, err := g.s.Create(ctx, []string{"/tank/a", "/tank/b"})
	wantErr(t, err, ErrCanceled)
	wantCalls(t, g.r.take(), []string{"zfs snapshot tank/a@" + name1, "zfs destroy tank/a@" + name1})
	unlocked(t, g.s)
}

// A tool killed by cancellation reports ErrCanceled.
func TestCancelDuringTool(t *testing.T) {
	g := newRig(t, Config{Method: ZFS}, fakeLocator{vols: map[string]Volume{"/t": {MountPoint: "/t", Device: "t"}}})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	g.r.hook = func(string) { cancel() }
	g.r.fail["zfs snapshot"] = errors.New("signal: killed")
	_, err := g.s.Create(ctx, []string{"/t"})
	wantErr(t, err, ErrCanceled)
	wantErr(t, err, context.Canceled)
}

func TestCanceledBeforeCreate(t *testing.T) {
	g := newRig(t, Config{Method: ZFS}, fakeLocator{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := g.s.Create(ctx, []string{"/t"})
	wantErr(t, err, ErrCanceled)
	wantCalls(t, g.r.take(), nil)
}

// A backup that fails after Create still has Close remove everything, and
// a failed removal step does not stop the others.
func TestCloseAfterFailedWalk(t *testing.T) {
	g := newRig(t, Config{Method: LVM}, fakeLocator{vols: map[string]Volume{"/srv": lvmVol}})
	g.r.out[lvsData] = "vg0/data\n"
	set, err := g.s.Create(context.Background(), []string{"/srv"})
	if err != nil {
		t.Fatal(err)
	}
	g.r.take()
	walk := func() (err error) {
		defer func() { err = errors.Join(err, set.Close()) }()
		return errors.New("walk failed")
	}
	g.r.fail["umount"] = errors.New("busy")
	err = walk()
	wantErr(t, err, ErrCommand)
	wantCalls(t, g.r.take(), []string{"umount " + filepath.Join(g.dir, name1), "lvremove --force vg0/" + name1})
	unlocked(t, g.s)
}

func TestAPFS(t *testing.T) {
	vols := map[string]Volume{
		"/":                    {MountPoint: "/", Root: "/", FSType: "apfs", Device: "/dev/disk3s1s1"},
		"/System/Volumes/Data": {MountPoint: "/System/Volumes/Data", Root: "/", FSType: "apfs", Device: "/dev/disk3s5"},
	}
	g := newRig(t, Config{Method: APFS}, fakeLocator{vols: vols})
	g.r.out["tmutil localsnapshot"] = "NOTE: local snapshots are considered purgeable\nCreated local snapshot with date: 2026-10-26-101112\n"
	set, err := g.s.Create(context.Background(), []string{"/System/Volumes/Data/Users/u", "/etc"})
	if err != nil {
		t.Fatal(err)
	}
	snap := "com.apple.TimeMachine.2026-10-26-101112.local"
	m1, m2 := filepath.Join(g.dir, name1), filepath.Join(g.dir, name2)
	wantCalls(t, g.r.take(), []string{
		"tmutil localsnapshot",
		"mount_apfs -o rdonly,nobrowse -s " + snap + " /dev/disk3s5 " + m1,
		"mount_apfs -o rdonly,nobrowse -s " + snap + " /dev/disk3s1s1 " + m2,
	})
	if set.Path("/System/Volumes/Data/Users/u") != m1+"/Users/u" || set.Path("/etc") != m2+"/etc" {
		t.Errorf("paths %v", set.paths)
	}
	if e := entries(t, g.dir); !slices.Equal(e, []string{name1, name1 + ".apfs", name2, name2 + ".apfs"}) {
		t.Errorf("snapshot directory holds %q", e)
	}
	if err := set.Close(); err != nil {
		t.Fatal(err)
	}
	wantCalls(t, g.r.take(), []string{"umount " + m2, "umount " + m1, "tmutil deletelocalsnapshots 2026-10-26-101112"})
	if e := entries(t, g.dir); len(e) != 0 {
		t.Errorf("left behind %q", e)
	}
}

func TestAPFSFailures(t *testing.T) {
	vol := map[string]Volume{"/": {MountPoint: "/", Root: "/", FSType: "apfs", Device: "/dev/disk3s1"}}
	t.Run("no date", func(t *testing.T) {
		g := newRig(t, Config{Method: APFS}, fakeLocator{vols: vol})
		g.r.out["tmutil localsnapshot"] = "nothing\n"
		_, err := g.s.Create(context.Background(), []string{"/"})
		wantErr(t, err, ErrCommand)
		wantCalls(t, g.r.take(), []string{"tmutil localsnapshot"})
	})
	t.Run("mount fails", func(t *testing.T) {
		g := newRig(t, Config{Method: APFS}, fakeLocator{vols: vol})
		g.r.out["tmutil localsnapshot"] = "Created local snapshot with date: 2026-10-26-101112\n"
		g.r.fail["mount_apfs"] = errors.New("exit status 1")
		_, err := g.s.Create(context.Background(), []string{"/"})
		wantErr(t, err, ErrCommand)
		calls := g.r.take()
		wantCalls(t, calls[len(calls)-1:], []string{"tmutil deletelocalsnapshots 2026-10-26-101112"})
		if e := entries(t, g.dir); len(e) != 0 {
			t.Errorf("left behind %q", e)
		}
	})
	t.Run("bad device", func(t *testing.T) {
		g := newRig(t, Config{Method: APFS}, fakeLocator{vols: map[string]Volume{"/": {MountPoint: "/", Device: "map auto_home"}}})
		_, err := g.s.Create(context.Background(), []string{"/"})
		wantErr(t, err, ErrUnsupported)
		wantCalls(t, g.r.take(), nil)
	})
}

const shadowID = "{3F2504E0-4F89-11D3-9A0C-0305E82C3301}"

func TestVSS(t *testing.T) {
	g := newRig(t, Config{Method: VSS}, fakeLocator{})
	ps := "powershell.exe -NoLogo -NoProfile -NonInteractive -Command "
	g.r.out[ps+vssCreateScript(`C:\`)] = shadowID + "\r\n"
	g.r.out[ps+vssDeviceScript(shadowID)] = `\\?\GLOBALROOT\Device\HarddiskVolumeShadowCopy7` + "\r\n"
	set := &Set{s: g.s, paths: map[string]string{}}
	root, err := g.s.driver(VSS).create(context.Background(), Volume{MountPoint: `C:\`, FSType: "NTFS"}, name1, set)
	if err != nil {
		t.Fatal(err)
	}
	if root != `\\?\GLOBALROOT\Device\HarddiskVolumeShadowCopy7\` {
		t.Errorf("root = %q", root)
	}
	wantCalls(t, g.r.take(), []string{ps + vssCreateScript(`C:\`), ps + vssDeviceScript(shadowID)})
	if b, err := os.ReadFile(filepath.Join(g.dir, name1+".vss")); err != nil || strings.TrimSpace(string(b)) != shadowID {
		t.Errorf("state file = %q, %v", b, err)
	}
	if err := set.Close(); err != nil {
		t.Fatal(err)
	}
	wantCalls(t, g.r.take(), []string{ps + vssDeleteScript(shadowID)})
	if e := entries(t, g.dir); len(e) != 0 {
		t.Errorf("left behind %q", e)
	}
	for _, want := range []string{"Win32_ShadowCopy", "Context = 'ClientAccessible'", "Volume = 'C:\\'"} {
		if !strings.Contains(vssCreateScript(`C:\`), want) {
			t.Errorf("create script lacks %q", want)
		}
	}
}

func TestVSSFailures(t *testing.T) {
	ps := "powershell.exe -NoLogo -NoProfile -NonInteractive -Command "
	vol := Volume{MountPoint: `D:\`, FSType: "NTFS"}
	for name, tc := range map[string]struct {
		vol     Volume
		out     map[string]string
		fail    string
		want    error
		cleanup bool
	}{
		"bad volume":   {Volume{MountPoint: `D:\it's`}, nil, "", ErrUnsupported, false},
		"no id":        {vol, map[string]string{ps + vssCreateScript(`D:\`): "oops"}, "", ErrCommand, false},
		"create fails": {vol, nil, ps + vssCreateScript(`D:\`), ErrCommand, false},
		"no device": {vol, map[string]string{ps + vssCreateScript(`D:\`): shadowID, ps + vssDeviceScript(shadowID): "C:"},
			"", ErrCommand, true},
		"device fails": {vol, map[string]string{ps + vssCreateScript(`D:\`): shadowID}, ps + vssDeviceScript(shadowID), ErrCommand, true},
	} {
		t.Run(name, func(t *testing.T) {
			g := newRig(t, Config{Method: VSS}, fakeLocator{})
			for k, v := range tc.out {
				g.r.out[k] = v
			}
			if tc.fail != "" {
				g.r.fail[tc.fail] = errors.New("exit status 1")
			}
			set := &Set{s: g.s, paths: map[string]string{}}
			_, err := g.s.driver(VSS).create(context.Background(), tc.vol, name1, set)
			wantErr(t, err, tc.want)
			g.r.take()
			if err := set.Close(); err != nil {
				t.Fatal(err)
			}
			if got := len(g.r.take()) == 1; got != tc.cleanup {
				t.Errorf("deleted the shadow copy: %v, want %v", got, tc.cleanup)
			}
			if e := entries(t, g.dir); len(e) != 0 {
				t.Errorf("left behind %q", e)
			}
		})
	}
}

func TestAuto(t *testing.T) {
	home := t.TempDir()
	vols := map[string]Volume{
		"/srv":  lvmVol,
		home:    {MountPoint: home, FSType: "btrfs", Subvolume: home},
		"/tank": {MountPoint: "/tank", FSType: "zfs", Device: "tank"},
	}
	g := newRig(t, Config{Method: Auto}, fakeLocator{vols: vols})
	g.r.out[lvsData] = "vg0/data\n"
	set, err := g.s.Create(context.Background(), []string{"/srv/a", filepath.Join(home, "b"), "/tank/c"})
	if err != nil {
		t.Fatal(err)
	}
	calls := g.r.take()
	if len(calls) != 5 || !strings.HasPrefix(calls[3], "btrfs subvolume snapshot") || !strings.HasPrefix(calls[4], "zfs snapshot") {
		t.Errorf("calls %q", calls)
	}
	if err := set.Close(); err != nil {
		t.Fatal(err)
	}

	g = newRig(t, Config{Method: Auto}, fakeLocator{vols: map[string]Volume{"/": {MountPoint: "/", FSType: "ext4", Device: "/dev/sda1"}}})
	_, err = g.s.Create(context.Background(), []string{"/etc"})
	wantErr(t, err, ErrUnsupported)
}

func TestPick(t *testing.T) {
	for _, tc := range []struct {
		fs, dev string
		want    Method
	}{
		{"btrfs", "", Btrfs}, {"zfs", "", ZFS}, {"apfs", "", APFS}, {"NTFS", "", VSS}, {"ReFS", "", VSS},
		{"ext4", "/dev/mapper/vg-root", LVM}, {"xfs", "/dev/dm-3", LVM}, {"ext4", "/dev/nvme0n1p2", ""},
		{"tmpfs", "tmpfs", ""}, {"nfs", "host:/x", ""},
	} {
		got, ok := pick(Volume{FSType: tc.fs, Device: tc.dev})
		if got != tc.want || ok != (tc.want != "") {
			t.Errorf("pick(%s, %s) = %q, %v", tc.fs, tc.dev, got, ok)
		}
	}
}

func TestNone(t *testing.T) {
	g := newRig(t, Config{}, fakeLocator{})
	set, err := g.s.Create(context.Background(), []string{"/x"})
	if set != nil || err != nil {
		t.Fatalf("Create = %v, %v", set, err)
	}
	if err := g.s.Sweep(context.Background(), []string{"/x"}); err != nil {
		t.Fatal(err)
	}
	if set.Path("/x") != "/x" || set.Close() != nil {
		t.Error("a nil Set is not the live source")
	}
	wantCalls(t, g.r.take(), nil)
}

func TestLocateFailures(t *testing.T) {
	g := newRig(t, Config{Method: ZFS}, fakeLocator{err: errors.New("no such file")})
	_, err := g.s.Create(context.Background(), []string{"/x"})
	wantErr(t, err, ErrFilesystem)

	// A source outside the subvolume it was located on is refused.
	g = newRig(t, Config{Method: Btrfs}, fakeLocator{vols: map[string]Volume{"/": {MountPoint: "/", Subvolume: "/home"}}})
	_, err = g.s.Create(context.Background(), []string{"/etc"})
	wantErr(t, err, ErrUnsupported)
}

func TestToolErrors(t *testing.T) {
	g := newRig(t, Config{Method: ZFS}, fakeLocator{vols: map[string]Volume{"/t": {MountPoint: "/t", Device: "t"}}})
	g.s.env.LookPath = func(string) (string, error) { return "", errors.New("not found") }
	_, err := g.s.Create(context.Background(), []string{"/t"})
	wantErr(t, err, ErrTool)

	n := 0
	g.s.env.LookPath = func(tool string) (string, error) { n++; return "zfs", nil }
	_, err = g.s.Create(context.Background(), []string{"/t"})
	wantErr(t, err, ErrTool)

	g.s.env.LookPath = func(tool string) (string, error) { n++; return toolDir + "/" + tool, nil }
	for range 2 {
		set, err := g.s.Create(context.Background(), []string{"/t"})
		if err != nil {
			t.Fatal(err)
		}
		if err := set.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if n != 2 {
		t.Errorf("LookPath ran %d times; a tool is resolved once", n)
	}
}

func TestName(t *testing.T) {
	for name, ok := range map[string]bool{
		name1:                                 true,
		NamePrefix + "0123456789abcdef":       true,
		NamePrefix + "0123456789ABCDEF":       false,
		NamePrefix + "0123456789abcde":        false,
		NamePrefix + "0123456789abcdef0":      false,
		NamePrefix + "0123456789abcdef/..":    false,
		"x" + NamePrefix + "0123456789abcdef": false,
		"objstore-backup":                     false,
		"daily":                               false,
		"":                                    false,
	} {
		if ValidName(name) != ok {
			t.Errorf("ValidName(%q) = %v", name, !ok)
		}
	}
	g := newRig(t, Config{Method: ZFS}, fakeLocator{vols: map[string]Volume{"/t": {MountPoint: "/t", Device: "t"}}})
	g.s.env.Rand = strings.NewReader("short")
	_, err := g.s.Create(context.Background(), []string{"/t"})
	wantErr(t, err, ErrName)
	wantCalls(t, g.r.take(), nil)

	// Every step refuses a name that fails validation.
	set := &Set{s: g.s, paths: map[string]string{}}
	for _, m := range []Method{LVM, Btrfs, ZFS, APFS, VSS} {
		if _, err := g.s.driver(m).create(context.Background(), Volume{}, "daily", set); !errors.Is(err, ErrName) {
			t.Errorf("%s create took a bad name: %v", m, err)
		}
	}
	if _, err := g.s.mountPoint(LVM, "", "../x", set); !errors.Is(err, ErrName) {
		t.Errorf("mountPoint took a bad name: %v", err)
	}
	if err := g.s.writeState(VSS, "", "../x", "v"); !errors.Is(err, ErrName) {
		t.Errorf("writeState took a bad name: %v", err)
	}
	wantCalls(t, g.r.take(), nil)
}

func TestError(t *testing.T) {
	cause := errors.New("exit status 5")
	err := &Error{Op: "create", Method: LVM, Volume: "/dev/vg/x", Snapshot: name1, Err: ErrCommand, Cause: cause}
	want := "source snapshot create lvm volume /dev/vg/x snapshot " + name1 + ": " + ErrCommand.Error() + ": exit status 5"
	if err.Error() != want {
		t.Errorf("Error() = %q\nwant %q", err.Error(), want)
	}
	if !errors.Is(err, ErrCommand) || !errors.Is(err, cause) {
		t.Error("Unwrap misses the sentinel or the cause")
	}
	if got := (&Error{Op: "lock", Err: ErrBusy}).Error(); got != "source snapshot lock: "+ErrBusy.Error() {
		t.Errorf("Error() = %q", got)
	}
}
