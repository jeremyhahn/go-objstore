package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestKeyCommands(t *testing.T) {
	h := newHarness(t)
	h.must(exitOK, h.repo("init")...)
	src := h.source(map[string]string{"a.txt": strings.Repeat("alpha ", 3000)})
	snap := strings.Fields(h.must(exitOK, h.repo("backup", src)...))[1]

	const second, third = "second passphrase", "third passphrase"
	h.secrets = append(h.secrets, second, third)
	secondFile := h.file("second", second+"\n", 0o600)
	thirdFile := h.file("third", third+"\n", 0o600)
	oldPass := h.pass

	// add: two slots, either opens.
	out := h.must(exitOK, h.repo("key", "add", "--new-passphrase-file", secondFile)...)
	if !strings.HasPrefix(out, "added key slot ") {
		t.Fatalf("key add = %q", out)
	}
	added := strings.Fields(out)[3]
	list := h.must(exitOK, h.repo("key", "list")...)
	if strings.Count(list, "\n") != 2 || !strings.Contains(list, "  "+added) {
		t.Fatalf("key list = %q", list)
	}
	h.pass = secondFile
	if list := h.must(exitOK, h.repo("key", "list")...); !strings.Contains(list, "* "+added) {
		t.Fatalf("key list with the second passphrase = %q", list)
	}
	h.must(exitOK, h.repo("snapshots")...)

	// remove: the added slot goes, then the last one is refused.
	h.pass = oldPass
	h.must(exitOK, h.repo("key", "remove", added[:12])...)
	h.pass = secondFile
	h.must(exitFailure, h.repo("snapshots")...)
	h.pass = oldPass
	list = h.must(exitOK, h.repo("key", "list")...)
	only := strings.Fields(list)[1]
	code, _, errOut := h.run("", h.repo("key", "remove", only)...)
	if code != exitFailure || !strings.Contains(errOut, "last key slot") {
		t.Fatalf("remove last = %d, %q", code, errOut)
	}
	for arg, want := range map[string]string{"zz": "hexadecimal", "ffff": "no key slot", added: "no key slot"} {
		if code, _, errOut := h.run("", h.repo("key", "remove", arg)...); code != exitFailure || !strings.Contains(errOut, want) {
			t.Fatalf("remove %q = %d, %q", arg, code, errOut)
		}
	}

	// passwd: the old passphrase is refused, the snapshot restores.
	out = h.must(exitOK, h.repo("key", "passwd", "--new-passphrase-file", thirdFile)...)
	if !strings.Contains(out, "removed key slot "+only) {
		t.Fatalf("key passwd = %q", out)
	}
	h.must(exitFailure, h.repo("snapshots")...)
	h.pass = thirdFile
	target := t.TempDir()
	h.must(exitOK, h.repo("restore", snap, target)...)
	got, err := os.ReadFile(filepath.Join(target, "a.txt"))
	if err != nil || string(got) != strings.Repeat("alpha ", 3000) {
		t.Fatalf("restored a.txt: %v", err)
	}
	// Nothing was deleted from the write-once store.
	if names, _ := h.st.List(t.Context(), "hosts/a/keys/"); len(names) != 3 {
		t.Fatalf("key objects = %d, want 3", len(names))
	}
}

func TestKeyUsage(t *testing.T) {
	h := newHarness(t)
	newFile := h.file("new", "new one\n", 0o600)
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"no subcommand", []string{"key"}, "a subcommand is required"},
		{"unknown subcommand", []string{"key", "rotate"}, `unknown subcommand "rotate"`},
		{"add without new", h.repo("key", "add"), "a new passphrase is required"},
		{"add both new", h.repo("key", "add", "--new-passphrase-file", newFile, "--new-passphrase-stdin"), "exclusive"},
		{"both stdin", []string{"key", "add", "--address", "a:1", "--passphrase-stdin", "--new-passphrase-stdin"}, "cannot both read"},
		{"iterations", h.repo("key", "passwd", "--new-passphrase-file", newFile, "--iterations", "1"), "iteration"},
		{"remove no id", h.repo("key", "remove"), "missing arguments"},
		{"unknown provider", h.repo("snapshots", "--key-provider", "xkms"), "unknown key provider"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, _, errOut := h.run("", tc.args...)
			if code != exitUsage || !strings.Contains(errOut, tc.want) {
				t.Fatalf("%q = %d, %q; want usage error %q", tc.args, code, errOut, tc.want)
			}
		})
	}
	if out := h.must(exitOK, "key", "-h"); !strings.Contains(out, "passwd") {
		t.Fatalf("key -h = %q", out)
	}
}
