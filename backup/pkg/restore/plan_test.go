package restore

import (
	"encoding/json"
	"errors"
	"io/fs"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jeremyhahn/go-objstore/backup/pkg/repo"
)

func TestPlanAccepts(t *testing.T) {
	s := &repo.Snapshot{Nodes: []repo.Node{
		dirNode("."), dirNode("a"), fileNode("a/f"), linkNode("a/l", "f"),
		linkNode("up", "a/../a/f"), dirNode("d"), linkNode("d/s", ".."),
		linkNode("y", "d/s/a/f"), linkNode("dangling", "missing/x"),
		linkNode("z", "./a//f"), linkNode("via", "y"),
	}}
	pl, err := newPlan(s, func(string) (fs.FileMode, bool) { return fs.ModeDir, true })
	if err != nil {
		t.Fatalf("newPlan: %v", err)
	}
	if pl.root == nil || len(pl.dirs) != 2 || len(pl.files) != 1 || len(pl.links) != 7 {
		t.Fatalf("plan %+v", pl)
	}
}

func TestPlanRefuses(t *testing.T) {
	badSize := fileNode("f")
	badSize.Size = -1
	upper := fileNode("f")
	upper.SHA256 = strings.ToUpper(emptyDigest)
	short := fileNode("f")
	short.SHA256 = emptyDigest[:10]
	for name, tc := range map[string]struct {
		nodes []repo.Node
		cause error
	}{
		"root twice":    {[]repo.Node{dirNode("."), dirNode(".")}, errRoot},
		"root file":     {[]repo.Node{fileNode(".")}, errRoot},
		"empty":         {[]repo.Node{fileNode("")}, errNotLocal},
		"trailing":      {[]repo.Node{fileNode("a/")}, errNotLocal},
		"double slash":  {[]repo.Node{dirNode("a"), fileNode("a//b")}, errNotLocal},
		"dot":           {[]repo.Node{fileNode("./a")}, errNotLocal},
		"nul":           {[]repo.Node{fileNode("a\x00b")}, errNotLocal},
		"duplicate":     {[]repo.Node{dirNode("a"), fileNode("a")}, errDuplicate},
		"no parent":     {[]repo.Node{fileNode("x/f")}, errParent},
		"file parent":   {[]repo.Node{fileNode("f"), fileNode("f/g")}, errParent},
		"type":          {[]repo.Node{{Path: "x", Type: "device"}}, errType},
		"size":          {[]repo.Node{badSize}, errFile},
		"upper digest":  {[]repo.Node{upper}, errFile},
		"short digest":  {[]repo.Node{short}, errFile},
		"empty target":  {[]repo.Node{linkNode("l", "")}, errTarget},
		"abs target":    {[]repo.Node{linkNode("l", "/x")}, errTarget},
		"nul target":    {[]repo.Node{linkNode("l", "a\x00")}, errTarget},
		"loop":          {[]repo.Node{linkNode("l", "l")}, errEscape},
		"long target":   {[]repo.Node{linkNode("l", strings.Repeat("./", maxLinkSteps))}, errEscape},
		"up and out":    {[]repo.Node{dirNode("a"), linkNode("a/l", "../../x")}, errEscape},
		"through chain": {[]repo.Node{dirNode("d"), linkNode("d/s", ".."), linkNode("x", "d/s/..")}, errEscape},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := newPlan(&repo.Snapshot{Nodes: tc.nodes}, nil)
			assertError(t, err, ErrUnsafeEntry, tc.cause)
		})
	}

	// A symlink found in the target is never passed through.
	links := func(name string) (fs.FileMode, bool) { return fs.ModeSymlink, name == "ext" }
	_, err := newPlan(&repo.Snapshot{Nodes: []repo.Node{linkNode("x", "ext/y")}}, links)
	assertError(t, err, ErrUnsafeEntry, errThrough)
	if _, err := newPlan(&repo.Snapshot{Nodes: []repo.Node{linkNode("x", "other/y")}}, links); err != nil {
		t.Fatalf("newPlan: %v", err)
	}
}

func FuzzPlan(f *testing.F) {
	for _, nodes := range [][]repo.Node{
		{dirNode("."), dirNode("a"), fileNode("a/f"), linkNode("a/l", "../a/f")},
		{dirNode("d"), linkNode("d/s", ".."), linkNode("x", "d/s/../outside")},
		{fileNode("/etc/passwd")},
		{fileNode("../x")},
		{linkNode("l", "l")},
	} {
		b, err := json.Marshal(repo.Snapshot{Version: 1, Nodes: nodes})
		if err != nil {
			f.Fatalf("Marshal: %v", err)
		}
		f.Add(b)
	}
	f.Add([]byte(`{"nodes":[{"path":"a","type":"symlink","target":"..\\x"}]}`))
	f.Fuzz(func(t *testing.T, data []byte) {
		var s repo.Snapshot
		if json.Unmarshal(data, &s) != nil {
			return
		}
		pl, err := newPlan(&s, nil)
		if err != nil {
			if !errors.Is(err, ErrUnsafeEntry) {
				t.Fatalf("newPlan error %v is not ErrUnsafeEntry", err)
			}
			return
		}
		for p, n := range pl.nodes {
			if !filepath.IsLocal(filepath.FromSlash(p)) {
				t.Fatalf("accepted path %q is not local", p)
			}
			if n.Type != repo.NodeSymlink {
				continue
			}
			steps := 0
			to, err := pl.resolve(path.Dir(p), n.Target, 0, &steps, nil)
			if err != nil || to != "." && !filepath.IsLocal(filepath.FromSlash(to)) {
				t.Fatalf("accepted symlink %q leads to %q, %v", p, to, err)
			}
		}
	})
}
