//go:build windows

package srcsnap

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"unsafe"

	"golang.org/x/sys/windows"
)

// lockName is the lock file in the snapshot directory.
const lockName = ".lock"

// DefaultDir is the snapshot directory when none is set, under the
// service's ProgramData directory, which the install locks down to SYSTEM
// and Administrators.
func DefaultDir() string {
	pd := os.Getenv("ProgramData")
	if pd == "" || !filepath.IsAbs(pd) {
		pd = `C:\ProgramData`
	}
	return filepath.Join(pd, "objstore-backup", "snapshots")
}

// lockDir makes dir if it is missing, private to SYSTEM, Administrators and
// this user; refuses it, before reading anything in it, unless its owner
// and DACL keep it so; and takes its lock file name without waiting. The
// directory stays open until unlock, so it cannot be swapped meanwhile.
func lockDir(dir, name string) (func() error, error) {
	fail := func(sentinel error, cause error) error {
		return &Error{Op: "lock", Volume: dir, Err: sentinel, Cause: cause}
	}
	user, err := userSID()
	if err != nil {
		return nil, fail(ErrFilesystem, err)
	}
	if err := mkPrivateDir(dir, user); err != nil {
		return nil, fail(ErrFilesystem, err)
	}
	d, err := openChecked(dir, windows.FILE_LIST_DIRECTORY, true, user)
	if err != nil {
		return nil, fail(ErrFilesystem, err)
	}
	f, err := os.OpenFile(filepath.Join(dir, name), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		_ = windows.CloseHandle(d)
		return nil, fail(ErrFilesystem, err)
	}
	closeAll := func() error { return errors.Join(f.Close(), windows.CloseHandle(d)) }
	if err := checkHandle(windows.Handle(f.Fd()), user); err != nil {
		_ = closeAll()
		return nil, fail(ErrFilesystem, err)
	}
	ol := new(windows.Overlapped)
	if err := windows.LockFileEx(windows.Handle(f.Fd()),
		windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, ol); err != nil {
		_ = closeAll()
		if errors.Is(err, windows.ERROR_LOCK_VIOLATION) {
			return nil, fail(ErrBusy, nil)
		}
		return nil, fail(ErrFilesystem, err)
	}
	return closeAll, nil
}

// hostLockDir is the directory of the host lock: the default snapshot
// directory under the system's ProgramData folder, which no run's
// environment changes.
func hostLockDir() string {
	pd, err := windows.KnownFolderPath(windows.FOLDERID_ProgramData, 0)
	if err != nil || !filepath.IsAbs(pd) {
		pd = `C:\ProgramData`
	}
	return filepath.Join(pd, "objstore-backup", "snapshots")
}

// readRecord reads a state file, refusing one that is not a regular file
// private to SYSTEM, Administrators and this user.
func readRecord(path string) ([]byte, error) {
	user, err := userSID()
	if err != nil {
		return nil, err
	}
	h, err := openChecked(path, windows.GENERIC_READ, false, user)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(h), path)
	defer f.Close()
	return io.ReadAll(f)
}

// userSID is the SID of the user the process runs as.
func userSID() (string, error) {
	tu, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		return "", err
	}
	return tu.User.Sid.String(), nil
}

// mkPrivateDir makes dir, if it is missing, with a protected DACL granting
// full control to SYSTEM, Administrators and user alone, inherited by what
// is made in it. Its parents are made as os.MkdirAll makes them.
func mkPrivateDir(dir, user string) error {
	if err := os.MkdirAll(filepath.Dir(dir), 0o700); err != nil {
		return err
	}
	sd, err := windows.SecurityDescriptorFromString("D:P(A;OICI;FA;;;SY)(A;OICI;FA;;;BA)(A;OICI;FA;;;" + user + ")")
	if err != nil {
		return err
	}
	p, err := windows.UTF16PtrFromString(dir)
	if err != nil {
		return err
	}
	sa := &windows.SecurityAttributes{SecurityDescriptor: sd}
	sa.Length = uint32(unsafe.Sizeof(*sa))
	if err := windows.CreateDirectory(p, sa); err != nil && !errors.Is(err, windows.ERROR_ALREADY_EXISTS) {
		return err
	}
	return nil
}

// openChecked opens path, a directory when dir is set and a regular file
// otherwise, without following a reparse point, and refuses it unless
// checkHandle passes. A directory is opened without delete sharing, so it
// cannot be renamed or removed while the handle is open.
func openChecked(path string, access uint32, dir bool, user string) (windows.Handle, error) {
	p, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return 0, err
	}
	flags := uint32(windows.FILE_FLAG_OPEN_REPARSE_POINT)
	if dir {
		flags |= windows.FILE_FLAG_BACKUP_SEMANTICS
	}
	h, err := windows.CreateFile(p, access|windows.READ_CONTROL,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE, nil, windows.OPEN_EXISTING, flags, 0)
	if err != nil {
		return 0, err
	}
	var fi windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(h, &fi); err != nil {
		_ = windows.CloseHandle(h)
		return 0, err
	}
	isDir := fi.FileAttributes&windows.FILE_ATTRIBUTE_DIRECTORY != 0
	switch {
	case fi.FileAttributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0:
		err = detail("is a reparse point")
	case dir && !isDir:
		err = detail("not a directory")
	case !dir && isDir:
		err = detail("not a regular file")
	default:
		err = checkHandle(h, user)
	}
	if err != nil {
		_ = windows.CloseHandle(h)
		return 0, err
	}
	return h, nil
}

// checkHandle reads the owner and DACL of the open file or directory h and
// applies checkACL to them.
func checkHandle(h windows.Handle, user string) error {
	sd, err := windows.GetSecurityInfo(h, windows.SE_FILE_OBJECT,
		windows.OWNER_SECURITY_INFORMATION|windows.DACL_SECURITY_INFORMATION)
	if err != nil {
		return err
	}
	owner, _, err := sd.Owner()
	if err != nil {
		return err
	}
	acl, _, err := sd.DACL()
	if errors.Is(err, windows.ERROR_OBJECT_NOT_FOUND) {
		return checkACL(owner.String(), nil, false, user)
	}
	if err != nil {
		return err
	}
	if acl == nil {
		return checkACL(owner.String(), nil, false, user)
	}
	entries := make([]aclEntry, 0, acl.AceCount)
	for i := uint32(0); i < uint32(acl.AceCount); i++ {
		var ace *windows.ACCESS_ALLOWED_ACE
		if err := windows.GetAce(acl, i, &ace); err != nil {
			return err
		}
		e := aclEntry{typ: ace.Header.AceType, mask: uint32(ace.Mask)}
		if e.typ == aceAllowed || e.typ == aceDenied {
			// Both types share the layout: the SID starts at SidStart.
			e.sid = (*windows.SID)(unsafe.Pointer(&ace.SidStart)).String()
		}
		entries = append(entries, e)
	}
	return checkACL(owner.String(), entries, true, user)
}
