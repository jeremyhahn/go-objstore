//go:build linux

package unix

import (
	"fmt"
	"net"
	"os/user"
	"strconv"

	"golang.org/x/sys/unix"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
)

// peerCredPrincipal builds a "unix-peer" principal (uid:<uid>, username, primary-group role, uid/gid/pid attributes) from SO_PEERCRED; non-UnixConn yields ErrPeerCredUnsupported.
func peerCredPrincipal(conn net.Conn) (*adapters.Principal, error) {
	unixConn, ok := conn.(*net.UnixConn)
	if !ok {
		return nil, ErrPeerCredUnsupported
	}
	raw, err := unixConn.SyscallConn()
	if err != nil {
		return nil, fmt.Errorf("unix: failed to access raw connection: %w", err)
	}
	var cred *unix.Ucred
	var credErr error
	if ctrlErr := raw.Control(func(fd uintptr) {
		// #nosec G115 -- fd from SyscallConn().Control is a valid descriptor that
		// fits in an int on all supported platforms; SO_PEERCRED reads kernel-set
		// peer credentials and performs no untrusted conversion.
		cred, credErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
	}); ctrlErr != nil {
		return nil, fmt.Errorf("unix: failed to read peer credentials: %w", ctrlErr)
	}
	if credErr != nil {
		return nil, fmt.Errorf("unix: SO_PEERCRED failed: %w", credErr)
	}

	uidStr := strconv.FormatUint(uint64(cred.Uid), 10)
	gidStr := strconv.FormatUint(uint64(cred.Gid), 10)
	pidStr := strconv.FormatInt(int64(cred.Pid), 10)

	principal := &adapters.Principal{
		ID:   "uid:" + uidStr,
		Name: uidStr,
		Type: "unix-peer",
		Attributes: map[string]any{
			"uid": uidStr,
			"gid": gidStr,
			"pid": pidStr,
		},
	}

	// Best-effort: resolve the username for a friendlier Name.
	if u, lookupErr := user.LookupId(uidStr); lookupErr == nil {
		principal.Name = u.Username
	}
	// Best-effort: map the primary group to a role so RBAC can authorize by OS group.
	if g, lookupErr := user.LookupGroupId(gidStr); lookupErr == nil {
		principal.Roles = []string{g.Name}
		principal.Attributes["group"] = g.Name
	} else {
		principal.Roles = []string{"gid:" + gidStr}
	}

	return principal, nil
}
