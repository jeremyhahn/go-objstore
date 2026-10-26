package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/jeremyhahn/go-objstore/backup/pkg/srcsnap"
)

// fakeBtrfs stands in for the btrfs tool: a snapshot copies the subvolume,
// and then, through late, changes the live tree, which the backup must not
// see.
type fakeBtrfs struct {
	t     *testing.T
	fail  error
	late  func()
	mu    sync.Mutex
	calls []string
}

func (f *fakeBtrfs) Run(_ context.Context, argv []string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, strings.Join(argv[1:], " "))
	switch {
	case len(argv) == 6 && argv[2] == "snapshot":
		if f.fail != nil {
			return nil, f.fail
		}
		if err := os.CopyFS(argv[5], os.DirFS(argv[4])); err != nil {
			f.t.Error(err)
		}
		if f.late != nil {
			f.late()
		}
	case len(argv) == 4 && argv[2] == "delete":
		if err := os.RemoveAll(argv[3]); err != nil {
			f.t.Error(err)
		}
	}
	return nil, nil
}

// subvolumes puts each path on a btrfs subvolume of its own.
type subvolumes struct{}

func (subvolumes) Locate(p string) (srcsnap.Volume, string, error) {
	r, err := filepath.EvalSymlinks(p)
	if err != nil {
		return srcsnap.Volume{}, "", err
	}
	return srcsnap.Volume{MountPoint: r, FSType: "btrfs", Device: "/dev/sda1", Subvolume: r}, r, nil
}

func (subvolumes) MountPoints() (map[string]bool, error) { return map[string]bool{}, nil }

// btrfsRig points the harness at a fake btrfs and returns the flags that
// select it.
func btrfsRig(h *harness) (*fakeBtrfs, []string, string) {
	f := &fakeBtrfs{t: h.t}
	h.snap = srcsnap.Env{
		Runner:   f,
		Locator:  subvolumes{},
		HostDir:  h.t.TempDir(),
		LookPath: func(tool string) (string, error) { return "/fake/" + tool, nil },
	}
	bdir := h.t.TempDir()
	return f, []string{"--snapshot", "btrfs", "--snapshot-dir", h.t.TempDir(), "--snapshot-btrfs-dir", bdir}, bdir
}

func emptyDir(t *testing.T, dir string) {
	t.Helper()
	if e, err := os.ReadDir(dir); err != nil || len(e) != 0 {
		t.Errorf("%s holds %v, %v", dir, e, err)
	}
}

// A backup through a snapshot reads the frozen tree, records the original
// path, restores as one without, and removes the snapshot.
func TestBackupThroughSnapshot(t *testing.T) {
	h := newHarness(t)
	h.must(exitOK, h.repo("init")...)
	f, flags, bdir := btrfsRig(h)
	src := h.source(map[string]string{"a.txt": "hello", "sub/b.txt": "world"})
	f.late = func() {
		if err := os.WriteFile(filepath.Join(src, "late.txt"), []byte("after the snapshot"), 0o600); err != nil {
			t.Error(err)
		}
	}
	out := h.must(exitOK, h.repo(append([]string{"backup", src}, flags...)...)...)
	if !strings.Contains(out, "2 files, 2 dirs") {
		t.Errorf("backup: %q", out)
	}
	if len(f.calls) != 2 || !strings.HasPrefix(f.calls[0], "subvolume snapshot -r "+src+" "+bdir) ||
		!strings.HasPrefix(f.calls[1], "subvolume delete "+bdir) {
		t.Errorf("btrfs calls %q", f.calls)
	}
	emptyDir(t, bdir)

	var list []snapshotInfo
	if err := json.Unmarshal([]byte(h.must(exitOK, h.repo("snapshots", "--json")...)), &list); err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].Root != src {
		t.Fatalf("snapshots: %+v, want root %q", list, src)
	}
	target := filepath.Join(t.TempDir(), "out")
	h.must(exitOK, h.repo("restore", list[0].ID.String(), target)...)
	var got []string
	err := filepath.WalkDir(target, func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		b, err := os.ReadFile(p)
		rel, _ := filepath.Rel(target, p)
		got = append(got, filepath.ToSlash(rel)+"="+string(b))
		return err
	})
	if err != nil || !slices.Equal(got, []string{"a.txt=hello", "sub/b.txt=world"}) {
		t.Errorf("restored %q, %v", got, err)
	}
}

func TestBackupSnapshotFailure(t *testing.T) {
	h := newHarness(t)
	h.must(exitOK, h.repo("init")...)
	f, flags, bdir := btrfsRig(h)
	f.fail = errors.New("exit status 1: permission denied")
	src := h.source(map[string]string{"a.txt": "hello"})

	code, _, errOut := h.run("", h.repo(append([]string{"backup", src}, flags...)...)...)
	if code != exitFailure || !strings.Contains(errOut, "permission denied") {
		t.Errorf("fail policy: %d, %q", code, errOut)
	}
	code, out, errOut := h.run("", h.repo(append([]string{"backup", src, "--snapshot-on-failure", "fallback"}, flags...)...)...)
	if code != exitOK || !strings.Contains(out, "1 files") || !strings.Contains(errOut, "backing up the live sources") {
		t.Errorf("fallback policy: %d, %q, %q", code, out, errOut)
	}
	emptyDir(t, bdir)
}

func TestBackupSnapshotFlagsRefused(t *testing.T) {
	h := newHarness(t)
	for _, flags := range [][]string{
		{"--snapshot", "fsfreeze"},
		{"--snapshot", "zfs", "--snapshot-lvm-size", "4G"},
		{"--snapshot", "lvm", "--snapshot-lvm-size", "4G;reboot"},
		{"--snapshot", "btrfs", "--snapshot-on-failure", "ignore"},
		{"--snapshot", "btrfs", "--snapshot-dir", "relative"},
	} {
		code, _, errOut := h.run("", h.repo(append([]string{"backup", "/s"}, flags...)...)...)
		if code != exitUsage || !strings.Contains(errOut, "snapshot") {
			t.Errorf("%q = %d, %q", flags, code, errOut)
		}
	}
}

func TestSnapshotOpts(t *testing.T) {
	fs := newFlags("backup")
	var o snapOpts
	o.register(fs)
	if err := fs.Parse([]string{"--snapshot", "lvm", "--snapshot-on-failure", "fallback", "--snapshot-dir", "/run/s", "--snapshot-lvm-size", "20%ORIGIN"}); err != nil {
		t.Fatal(err)
	}
	c, err := o.config()
	want := srcsnap.Config{Method: srcsnap.LVM, OnFailure: srcsnap.Fallback, Dir: "/run/s", LVMSize: "20%ORIGIN"}
	if err != nil || c != want {
		t.Errorf("config() = %+v, %v; want %+v", c, err, want)
	}
	o = snapOpts{}
	if c, err := o.config(); err != nil || c.Method != srcsnap.None || c.OnFailure != srcsnap.Fail {
		t.Errorf("default config() = %+v, %v", c, err)
	}
}

// The service sweeps at start and snapshots each run; a snapshot that
// fails under the fail policy fails the run.
func TestServiceSnapshots(t *testing.T) {
	h := newHarness(t)
	h.must(exitOK, h.repo("init")...)
	f, _, bdir := btrfsRig(h)
	src := h.source(map[string]string{"a.txt": "hello"})
	extra := `, "source_snapshot": {"method": "btrfs", "dir": "` + t.TempDir() + `", "btrfs": {"dir": "` + bdir + `"}}`
	code, logs := h.serviceRun(h.serviceConfig(src, extra), func(n int) bool { return n == 2 }, new(int))
	if code != exitOK {
		t.Fatalf("service = %d", code)
	}
	var created, saved int
	for _, rec := range logs {
		switch rec["msg"] {
		case "source snapshot created":
			created++
		case "snapshot saved":
			saved++
		case "run failed":
			t.Errorf("run failed: %v", rec["error"])
		}
	}
	if created != 2 || saved != 2 || len(f.calls) != 4 {
		t.Errorf("%d snapshots created, %d saved, btrfs calls %q", created, saved, f.calls)
	}
	emptyDir(t, bdir)

	f.fail = errors.New("exit status 1")
	_, logs = h.serviceRun(h.serviceConfig(src, extra), func(int) bool { return true }, new(int))
	failed := slices.ContainsFunc(logs, func(rec map[string]any) bool {
		e, _ := rec["error"].(string)
		return rec["msg"] == "run failed" && strings.Contains(e, "btrfs")
	})
	if !failed {
		t.Errorf("a failed snapshot did not fail the run: %v", logs)
	}
}
