//go:build !windows

package srcsnap

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"syscall"

	"golang.org/x/sys/unix"
)

// lockName is the lock file in the snapshot directory.
const lockName = ".lock"

// DefaultDir is the snapshot directory when none is set: a directory
// under the run-time directory only root can write.
func DefaultDir() string {
	if runtime.GOOS == "linux" {
		return "/run/objstore-backup"
	}
	return "/var/run/objstore-backup"
}

// lockDir makes dir if it is missing, checks that only this user can write
// it, and takes its lock file name without waiting.
func lockDir(dir, name string) (func() error, error) {
	fail := func(sentinel error, cause error) error {
		return &Error{Op: "lock", Volume: dir, Err: sentinel, Cause: cause}
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fail(ErrFilesystem, err)
	}
	fi, err := os.Lstat(dir)
	if err != nil {
		return nil, fail(ErrFilesystem, err)
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	switch {
	case !fi.IsDir():
		return nil, fail(ErrFilesystem, detail("not a directory"))
	case !ok || int(st.Uid) != os.Geteuid() || fi.Mode().Perm()&0o022 != 0:
		return nil, fail(ErrFilesystem, detail("must be owned by user %d and writable by no one else", os.Geteuid()))
	}
	f, err := os.OpenFile(filepath.Join(dir, name), os.O_RDWR|os.O_CREATE|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, fail(ErrFilesystem, err)
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, unix.EWOULDBLOCK) {
			return nil, fail(ErrBusy, nil)
		}
		return nil, fail(ErrFilesystem, err)
	}
	return f.Close, nil
}

// hostLockDir is the directory of the host lock: the default snapshot
// directory, whatever directory a run is configured with.
func hostLockDir() string { return DefaultDir() }

// readRecord reads a state file. The snapshot directory, checked by
// lockDir, admits no one else's files.
func readRecord(path string) ([]byte, error) { return os.ReadFile(path) }
