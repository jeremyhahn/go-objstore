//go:build darwin

package srcsnap

import (
	"path/filepath"

	"golang.org/x/sys/unix"
)

// dataVolume is where macOS mounts the data volume that the firmlinked
// directories, such as /Users, live on.
const dataVolume = "/System/Volumes/Data"

// platformLocator finds volumes with statfs(2).
type platformLocator struct{}

// Locate implements Locator.
func (platformLocator) Locate(path string) (Volume, string, error) {
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return Volume{}, "", err
	}
	var st unix.Statfs_t
	if err := unix.Statfs(resolved, &st); err != nil {
		return Volume{}, "", err
	}
	v := Volume{
		MountPoint: unix.ByteSliceToString(st.Mntonname[:]),
		Root:       "/",
		FSType:     unix.ByteSliceToString(st.Fstypename[:]),
		Device:     unix.ByteSliceToString(st.Mntfromname[:]),
	}
	if v.MountPoint == dataVolume && !within(dataVolume, resolved) {
		// A firmlink, such as /Users, leads from the system volume to the
		// same path on the data volume.
		resolved = filepath.Join(dataVolume, resolved)
	}
	return v, resolved, nil
}

// MountPoints implements Locator.
func (platformLocator) MountPoints() (map[string]bool, error) {
	n, err := unix.Getfsstat(nil, unix.MNT_NOWAIT)
	if err != nil {
		return nil, err
	}
	buf := make([]unix.Statfs_t, n)
	if n, err = unix.Getfsstat(buf, unix.MNT_NOWAIT); err != nil {
		return nil, err
	}
	m := make(map[string]bool, n)
	for _, st := range buf[:n] {
		m[unix.ByteSliceToString(st.Mntonname[:])] = true
	}
	return m, nil
}
