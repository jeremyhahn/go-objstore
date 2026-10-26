package srcsnap

// Well-known SIDs that may own, or be granted write to, a Windows snapshot
// directory and its files.
const (
	sidSystem       = "S-1-5-18"
	sidAdmins       = "S-1-5-32-544"
	sidCreatorOwner = "S-1-3-0"
	sidOwnerRights  = "S-1-3-4"
)

// The ACE types whose SID aclEntry carries; any other type's SID is not
// read.
const (
	aceAllowed = 0 // ACCESS_ALLOWED_ACE_TYPE
	aceDenied  = 1 // ACCESS_DENIED_ACE_TYPE
)

// aclWriteRights are the rights that let a principal add, change or remove
// a file in a directory, change a file, or take either over: write data,
// append data, write extended attributes, delete child, write attributes,
// delete, write DACL, write owner, and the generic all and write rights.
const aclWriteRights uint32 = 0x2 | 0x4 | 0x10 | 0x40 | 0x100 |
	0x10000 | 0x40000 | 0x80000 | 0x10000000 | 0x40000000

// aclEntry is one ACE of a DACL: its type, access mask and, for the allowed
// and denied types, its SID.
type aclEntry struct {
	typ  byte
	mask uint32
	sid  string
}

// checkACL decides whether a Windows snapshot directory, or a file in it,
// is private to SYSTEM, Administrators and user, the running user's SID:
// owner must be one of them, and no ACE may grant a write right to anyone
// else. Inherit-only ACEs count, since they reach the files made later.
// Denied ACEs only take rights away; an ACE of a type other than allowed or
// denied that grants a write right is refused, its SID unread. hasDACL is
// false for a missing (NULL) DACL, which grants everyone everything. It
// returns nil or the reason for refusal.
func checkACL(owner string, dacl []aclEntry, hasDACL bool, user string) error {
	trusted := func(sid string) bool { return sid == sidSystem || sid == sidAdmins || sid == user }
	if !trusted(owner) {
		return detail("owned by %s, not SYSTEM, Administrators or %s", owner, user)
	}
	if !hasDACL {
		return detail("has no DACL, which grants everyone full control")
	}
	for _, e := range dacl {
		switch {
		case e.typ == aceDenied || e.mask&aclWriteRights == 0:
		case e.typ != aceAllowed:
			return detail("an ACE of type %d grants write access", e.typ)
		case !trusted(e.sid) && e.sid != sidCreatorOwner && e.sid != sidOwnerRights:
			return detail("grants write, delete or ownership change to %s", e.sid)
		}
	}
	return nil
}
