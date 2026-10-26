package srcsnap

import "testing"

func TestCheckACL(t *testing.T) {
	const user = "S-1-5-21-1-2-3-1001"
	const users = "S-1-5-32-545"
	const full, read = 0x1f01ff, 0x1200a9
	private := []aclEntry{
		{aceAllowed, full, sidSystem},
		{aceAllowed, full, sidAdmins},
		{aceAllowed, full, user},
		{aceAllowed, 0x10000000, sidCreatorOwner},
		{aceAllowed, full, sidOwnerRights},
		{aceAllowed, read, users},
		{aceDenied, full, users},
		{9, read, ""},
	}
	grant := func(e aclEntry) []aclEntry { return append(append([]aclEntry{}, private...), e) }
	for _, c := range []struct {
		name    string
		owner   string
		dacl    []aclEntry
		hasDACL bool
		ok      bool
	}{
		{"private", user, private, true, true},
		{"owned by SYSTEM", sidSystem, private, true, true},
		{"owned by Administrators, empty DACL", sidAdmins, nil, true, true},
		{"owned by another user", users, private, true, false},
		{"NULL DACL", user, nil, false, false},
		{"write data", user, grant(aclEntry{aceAllowed, 0x2, users}), true, false},
		{"append data", user, grant(aclEntry{aceAllowed, 0x4, users}), true, false},
		{"write EA", user, grant(aclEntry{aceAllowed, 0x10, users}), true, false},
		{"delete child", user, grant(aclEntry{aceAllowed, 0x40, users}), true, false},
		{"write attributes", user, grant(aclEntry{aceAllowed, 0x100, users}), true, false},
		{"delete", user, grant(aclEntry{aceAllowed, 0x10000, users}), true, false},
		{"write DACL", user, grant(aclEntry{aceAllowed, 0x40000, users}), true, false},
		{"write owner", user, grant(aclEntry{aceAllowed, 0x80000, users}), true, false},
		{"generic all", user, grant(aclEntry{aceAllowed, 0x10000000, users}), true, false},
		{"generic write", user, grant(aclEntry{aceAllowed, 0x40000000, users}), true, false},
		{"callback ACE granting write", user, grant(aclEntry{9, 0x2, ""}), true, false},
	} {
		err := checkACL(c.owner, c.dacl, c.hasDACL, user)
		if (err == nil) != c.ok {
			t.Errorf("%s: checkACL = %v, want ok %v", c.name, err, c.ok)
		}
	}
}
