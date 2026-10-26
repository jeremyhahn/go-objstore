package srcsnap

import (
	"strings"
	"testing"
)

func TestParse(t *testing.T) {
	for _, s := range []string{"", "none", "auto", "lvm", "btrfs", "zfs", "apfs", "vss"} {
		m, err := ParseMethod(s)
		if err != nil || (s != "" && string(m) != s) || (s == "" && m != None) {
			t.Errorf("ParseMethod(%q) = %q, %v", s, m, err)
		}
	}
	for _, s := range []string{"LVM", "fsfreeze", "lvm "} {
		_, err := ParseMethod(s)
		wantErr(t, err, ErrConfig)
	}
	for s, want := range map[string]Policy{"": Fail, "fail": Fail, "fallback": Fallback} {
		if p, err := ParsePolicy(s); err != nil || p != want {
			t.Errorf("ParsePolicy(%q) = %q, %v", s, p, err)
		}
	}
	_, err := ParsePolicy("ignore")
	wantErr(t, err, ErrConfig)
}

func TestValidate(t *testing.T) {
	ok := []Config{
		{},
		{Method: LVM, LVMSize: "4G"},
		{Method: LVM, LVMSize: "1.5t"},
		{Method: LVM, LVMSize: "100%ORIGIN"},
		{Method: Auto, LVMSize: "20%FREE", BtrfsDir: "/snaps/"},
		{Method: Btrfs, BtrfsDir: "/snaps", OnFailure: Fallback},
		{Method: LVM},
		{Method: Auto},
	}
	for _, c := range ok {
		if err := c.Validate(); err != nil {
			t.Errorf("Validate(%+v) = %v", c, err)
		}
		lvm := c.Method == LVM || c.Method == Auto
		if c.Dir != DefaultDir() || (c.LVMSize != "") != lvm || c.OnFailure == "" {
			t.Errorf("defaults not filled: %+v", c)
		}
		// A checked configuration checks again unchanged, as New does.
		again := c
		if err := again.Validate(); err != nil || again != c {
			t.Errorf("Validate again = %+v, %v", again, err)
		}
		if strings.HasSuffix(c.BtrfsDir, "/") {
			t.Errorf("BtrfsDir not cleaned: %q", c.BtrfsDir)
		}
	}
	bad := []Config{
		{Method: "snapper"},
		{OnFailure: "retry"},
		{Method: LVM, Dir: "snapshots"},
		{Method: ZFS, LVMSize: "4G"},
		{Method: LVM, LVMSize: "0G"},
		{Method: LVM, LVMSize: "4"},
		{Method: LVM, LVMSize: "101%ORIGIN"},
		{Method: LVM, LVMSize: "10%"},
		{Method: LVM, LVMSize: "4G --yes"},
		{Method: ZFS, BtrfsDir: "/snaps"},
		{Method: Btrfs, BtrfsDir: "snaps"},
	}
	for _, c := range bad {
		err := c.Validate()
		wantErr(t, err, ErrConfig)
	}
	if _, err := New(Config{Method: "snapper"}, Env{}); err == nil {
		t.Error("New took an unknown method")
	}
	s, err := New(Config{Method: ZFS, Dir: t.TempDir()}, Env{})
	if err != nil {
		t.Fatal(err)
	}
	if s.env.Runner == nil || s.env.Locator == nil || s.env.LookPath == nil || s.env.Log == nil || s.env.Rand == nil {
		t.Error("New left a nil dependency")
	}
}
