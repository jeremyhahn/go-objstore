//go:build !windows

package main

import (
	"io/fs"
	"os"
)

// modeRestricted reports whether info's permission bits give group and
// others none of the access a names.
func modeRestricted(info fs.FileInfo, a access) bool {
	deny := fs.FileMode(0o077)
	if a == othersWrite {
		deny = 0o022
	}
	return info.Mode().Perm()&deny == 0
}

// aclRestricted has nothing to check where access is in the permission bits.
func aclRestricted(*os.File, access) (bool, error) { return true, nil }
