package srcsnap

import (
	"strings"
	"testing"
)

const mountinfo = `22 1 8:2 / / rw,relatime shared:1 - ext4 /dev/mapper/vg0-root rw
30 22 0:26 /@home /home rw,relatime shared:2 - btrfs /dev/sda2 rw,subvol=/@home
31 22 8:3 / /mnt/my\040disk rw - xfs /dev/mapper/vg0-my\040disk rw
32 22 0:40 / /tank rw - zfs tank rw
33 32 0:41 / /tank rw - zfs tank/over rw
34 22 0:42 / /srv rw master:3 unbindable - ext4 /dev/sdb1 rw
`

func TestParseMountinfo(t *testing.T) {
	es, err := parseMountinfo(strings.NewReader(mountinfo))
	if err != nil {
		t.Fatal(err)
	}
	if len(es) != 6 {
		t.Fatalf("%d entries", len(es))
	}
	if e := es[1]; e.root != "/@home" || e.mountPoint != "/home" || e.fsType != "btrfs" || e.source != "/dev/sda2" {
		t.Errorf("entry %+v", e)
	}
	if e := es[2]; e.mountPoint != "/mnt/my disk" || e.source != "/dev/mapper/vg0-my disk" {
		t.Errorf("escapes not decoded: %+v", e)
	}
	if e := es[5]; e.fsType != "ext4" || e.source != "/dev/sdb1" {
		t.Errorf("optional fields mishandled: %+v", e)
	}
	for _, bad := range []string{"1 2 3\n", "22 1 8:2 / / rw shared:1 ext4 /dev/x rw\n", "22 1 8:2 / / rw - ext4\n"} {
		if _, err := parseMountinfo(strings.NewReader(bad)); err == nil {
			t.Errorf("parsed %q", bad)
		}
	}
}

func TestFindMount(t *testing.T) {
	es, err := parseMountinfo(strings.NewReader(mountinfo))
	if err != nil {
		t.Fatal(err)
	}
	for path, want := range map[string]string{
		"/":              "/dev/mapper/vg0-root",
		"/etc":           "/dev/mapper/vg0-root",
		"/home":          "/dev/sda2",
		"/home/u/docs":   "/dev/sda2",
		"/homework":      "/dev/mapper/vg0-root",
		"/mnt/my disk/a": "/dev/mapper/vg0-my disk",
		"/tank/x":        "tank/over",
	} {
		e, ok := findMount(es, path)
		if !ok || e.source != want {
			t.Errorf("findMount(%s) = %s, %v; want %s", path, e.source, ok, want)
		}
	}
	if _, ok := findMount(es[1:2], "/etc"); ok {
		t.Error("found a mount for a path outside every mount")
	}
}

func TestUnescapeAndWithin(t *testing.T) {
	for in, want := range map[string]string{`a\040b`: "a b", `a\011`: "a\t", `a\\b`: `a\\b`, `a\04`: `a\04`, `\999x`: `\999x`, "plain": "plain"} {
		if got := unescapeMount(in); got != want {
			t.Errorf("unescapeMount(%q) = %q, want %q", in, got, want)
		}
	}
	for _, tc := range []struct {
		dir, path string
		want      bool
	}{
		{"/", "/x", true}, {"/a", "/a", true}, {"/a", "/a/b", true}, {"/a", "/ab", false}, {"/a/b", "/a", false},
	} {
		if within(tc.dir, tc.path) != tc.want {
			t.Errorf("within(%s, %s) = %v", tc.dir, tc.path, !tc.want)
		}
	}
}
