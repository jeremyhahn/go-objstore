//go:build windows

package main

import (
	"io/fs"
	"os"
	"unsafe"

	"golang.org/x/sys/windows"
)

// modeRestricted has nothing to check on Windows, whose permission bits
// only reflect the read-only attribute: aclRestricted reads the file's ACL.
func modeRestricted(fs.FileInfo, access) bool { return true }

// Access rights an ACE grants on a file.
const (
	fileReadData   = 0x1
	fileWriteData  = 0x2
	fileAppendData = 0x4
	deleteRight    = 0x10000
	writeDAC       = 0x40000
	writeOwner     = 0x80000
	genericAll     = 0x10000000
	genericWrite   = 0x40000000
	genericRead    = 0x80000000

	writeRights = fileWriteData | fileAppendData | deleteRight | writeDAC | writeOwner | genericAll | genericWrite
	anyRights   = writeRights | fileReadData | genericRead

	accessAllowedACE = 0
)

// broadSIDs are the groups that stand for "others" on Windows.
var broadSIDs = []windows.WELL_KNOWN_SID_TYPE{
	windows.WinWorldSid,
	windows.WinAuthenticatedUserSid,
	windows.WinBuiltinUsersSid,
	windows.WinBuiltinGuestsSid,
	windows.WinAnonymousSid,
}

// aclRestricted reports whether the open file's DACL grants none of
// Everyone, Authenticated Users, Users, Guests or Anonymous the access a
// names. A file with no DACL grants everyone everything.
func aclRestricted(f *os.File, a access) (bool, error) {
	sd, err := windows.GetSecurityInfo(windows.Handle(f.Fd()), windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION)
	if err != nil {
		return false, err
	}
	dacl, _, err := sd.DACL()
	if err != nil || dacl == nil {
		return false, nil
	}
	rights := windows.ACCESS_MASK(anyRights)
	if a == othersWrite {
		rights = writeRights
	}
	broad := make([]*windows.SID, 0, len(broadSIDs))
	for _, t := range broadSIDs {
		sid, err := windows.CreateWellKnownSid(t)
		if err != nil {
			return false, err
		}
		broad = append(broad, sid)
	}
	for i := range uint32(dacl.AceCount) {
		var ace *windows.ACCESS_ALLOWED_ACE
		if err := windows.GetAce(dacl, i, &ace); err != nil {
			return false, err
		}
		if ace.Header.AceType != accessAllowedACE || ace.Mask&rights == 0 {
			continue
		}
		sid := (*windows.SID)(unsafe.Pointer(&ace.SidStart)) //nolint:gosec // an ACE's SID starts at SidStart
		for _, b := range broad {
			if sid.Equals(b) {
				return false, nil
			}
		}
	}
	return true, nil
}
