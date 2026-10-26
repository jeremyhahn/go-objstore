//go:build windows

package srcsnap

import (
	"path/filepath"

	"golang.org/x/sys/windows"
)

// platformLocator finds volumes with GetVolumePathName.
type platformLocator struct{}

// Locate implements Locator.
func (platformLocator) Locate(path string) (Volume, string, error) {
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return Volume{}, "", err
	}
	p, err := windows.UTF16PtrFromString(resolved)
	if err != nil {
		return Volume{}, "", err
	}
	buf := make([]uint16, windows.MAX_LONG_PATH)
	if err := windows.GetVolumePathName(p, &buf[0], uint32(len(buf))); err != nil {
		return Volume{}, "", err
	}
	vol := windows.UTF16ToString(buf)
	vp, err := windows.UTF16PtrFromString(vol)
	if err != nil {
		return Volume{}, "", err
	}
	fs := make([]uint16, windows.MAX_PATH+1)
	if err := windows.GetVolumeInformation(vp, nil, 0, nil, nil, nil, &fs[0], uint32(len(fs))); err != nil {
		return Volume{}, "", err
	}
	return Volume{MountPoint: vol, Root: `\`, FSType: windows.UTF16ToString(fs), Device: vol}, resolved, nil
}

// MountPoints implements Locator. Windows snapshots mount nothing.
func (platformLocator) MountPoints() (map[string]bool, error) { return map[string]bool{}, nil }
