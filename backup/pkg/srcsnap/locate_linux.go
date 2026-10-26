//go:build linux

package srcsnap

import (
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// mountinfoPath lists the mounts the process sees.
const mountinfoPath = "/proc/self/mountinfo"

// btrfsRootIno is the inode number of every btrfs subvolume's root.
const btrfsRootIno = 256

// platformLocator finds volumes through /proc/self/mountinfo.
type platformLocator struct{}

func (platformLocator) mounts() ([]mountEntry, error) {
	f, err := os.Open(mountinfoPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return parseMountinfo(f)
}

// Locate implements Locator.
func (l platformLocator) Locate(path string) (Volume, string, error) {
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return Volume{}, "", err
	}
	entries, err := l.mounts()
	if err != nil {
		return Volume{}, "", err
	}
	v, ok := volumeAt(entries, resolved)
	if !ok {
		return Volume{}, "", detail("no mount holds %s", resolved)
	}
	return v, resolved, nil
}

// volumeAt describes the mount among entries that holds path.
func volumeAt(entries []mountEntry, path string) (Volume, bool) {
	e, ok := findMount(entries, path)
	if !ok {
		return Volume{}, false
	}
	v := Volume{MountPoint: e.mountPoint, Root: e.root, FSType: e.fsType, Device: e.source}
	if e.fsType == "btrfs" {
		v.Subvolume = btrfsSubvolume(path, e.mountPoint)
	}
	return v, true
}

// MountPoints implements Locator.
func (l platformLocator) MountPoints() (map[string]bool, error) {
	entries, err := l.mounts()
	if err != nil {
		return nil, err
	}
	m := make(map[string]bool, len(entries))
	for _, e := range entries {
		m[e.mountPoint] = true
	}
	return m, nil
}

// btrfsSubvolume returns the root of the subvolume that holds path: the
// nearest directory at or above it, up to the mount point, whose inode is
// a subvolume root's.
func btrfsSubvolume(path, mountPoint string) string {
	for dir := path; ; dir = filepath.Dir(dir) {
		var st unix.Stat_t
		if err := unix.Stat(dir, &st); err == nil && st.Ino == btrfsRootIno {
			return dir
		}
		if dir == mountPoint || dir == filepath.Dir(dir) {
			return mountPoint
		}
	}
}
