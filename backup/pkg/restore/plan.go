package restore

import (
	"crypto/sha256"
	"encoding/hex"
	"io/fs"
	"path"
	"path/filepath"
	"strings"

	"github.com/jeremyhahn/go-objstore/backup/pkg/repo"
)

const (
	// maxLinkDepth bounds how many symlinks resolving one target may enter.
	maxLinkDepth = 40
	// maxLinkSteps bounds the path elements resolving one target may visit.
	maxLinkSteps = 4096
)

// lstatFunc reports the mode of an existing entry at a slash path under the
// target, and whether one exists.
type lstatFunc func(name string) (fs.FileMode, bool)

// plan is a manifest checked for safe placement. Every path in it is local,
// every parent is a directory in the manifest, and every symlink resolves
// inside the target.
type plan struct {
	root  *repo.Node
	dirs  []*repo.Node
	files []*repo.Node
	links []*repo.Node
	nodes map[string]*repo.Node
}

// newPlan checks the manifest's tree. lstat reports entries already under
// the target, so that a symlink the restore did not make is never passed
// through; nil means the target is empty.
func newPlan(s *repo.Snapshot, lstat lstatFunc) (*plan, error) {
	pl := &plan{nodes: make(map[string]*repo.Node, len(s.Nodes))}
	for i := range s.Nodes {
		if err := pl.add(&s.Nodes[i]); err != nil {
			return nil, err
		}
	}
	for _, n := range pl.links {
		steps := 0
		if _, err := pl.resolve(path.Dir(n.Path), n.Target, 0, &steps, lstat); err != nil {
			return nil, unsafe(n.Path, err)
		}
	}
	return pl, nil
}

func unsafe(p string, cause error) *Error {
	return newError("check entry", "", p, ErrUnsafeEntry, cause)
}

func (pl *plan) add(n *repo.Node) error {
	if n.Path == "." {
		if n.Type != repo.NodeDir || pl.root != nil {
			return unsafe(n.Path, errRoot)
		}
		pl.root = n
		return nil
	}
	if !localPath(n.Path) {
		return unsafe(n.Path, errNotLocal)
	}
	if _, dup := pl.nodes[n.Path]; dup {
		return unsafe(n.Path, errDuplicate)
	}
	if parent := path.Dir(n.Path); parent != "." {
		if p, ok := pl.nodes[parent]; !ok || p.Type != repo.NodeDir {
			return unsafe(n.Path, errParent)
		}
	}
	switch n.Type {
	case repo.NodeDir:
		pl.dirs = append(pl.dirs, n)
	case repo.NodeFile:
		if n.Size < 0 || !validDigest(n.SHA256) {
			return unsafe(n.Path, errFile)
		}
		pl.files = append(pl.files, n)
	case repo.NodeSymlink:
		if !validTarget(n.Target) {
			return unsafe(n.Path, errTarget)
		}
		pl.links = append(pl.links, n)
	default:
		return unsafe(n.Path, errType)
	}
	pl.nodes[n.Path] = n
	return nil
}

// localPath reports whether p is a slash-separated path that names an entry
// strictly inside the target on this system.
func localPath(p string) bool {
	return p != "." && fs.ValidPath(p) && !strings.ContainsRune(p, 0) &&
		portable(p) && filepath.IsLocal(filepath.FromSlash(p))
}

// validTarget reports whether t is a relative symlink target this system
// reads the same way the manifest does.
func validTarget(t string) bool {
	return t != "" && !strings.ContainsRune(t, 0) && portable(t) &&
		!path.IsAbs(t) && !filepath.IsAbs(t) && filepath.VolumeName(t) == ""
}

// portable reports whether s holds no separator other than a slash.
func portable(s string) bool {
	return filepath.Separator == '/' || !strings.ContainsRune(s, filepath.Separator)
}

func validDigest(s string) bool {
	b, err := hex.DecodeString(s)
	return err == nil && len(b) == sha256.Size && hex.EncodeToString(b) == s
}

// resolve follows target from the directory dir, through the manifest's own
// symlinks, and returns where it leads. It fails if the walk leaves the
// target, passes through a symlink the manifest does not hold, or does not
// end within its bounds.
func (pl *plan) resolve(dir, target string, depth int, steps *int, lstat lstatFunc) (string, error) {
	if depth > maxLinkDepth {
		return "", errEscape
	}
	var cur []string
	if dir != "." {
		cur = strings.Split(dir, "/")
	}
	for _, elem := range strings.Split(target, "/") {
		if *steps++; *steps > maxLinkSteps {
			return "", errEscape
		}
		switch elem {
		case "", ".":
			continue
		case "..":
			if len(cur) == 0 {
				return "", errEscape
			}
			cur = cur[:len(cur)-1]
			continue
		}
		cur = append(cur, elem)
		p := strings.Join(cur, "/")
		n, ok := pl.nodes[p]
		switch {
		case ok && n.Type == repo.NodeSymlink:
			next, err := pl.resolve(path.Dir(p), n.Target, depth+1, steps, lstat)
			if err != nil {
				return "", err
			}
			cur = nil
			if next != "." {
				cur = strings.Split(next, "/")
			}
		case ok || lstat == nil:
		default:
			if mode, exists := lstat(p); exists && mode&fs.ModeSymlink != 0 {
				return "", errThrough
			}
		}
	}
	if len(cur) == 0 {
		return ".", nil
	}
	return strings.Join(cur, "/"), nil
}
