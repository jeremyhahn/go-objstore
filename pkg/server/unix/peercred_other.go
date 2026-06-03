//go:build !linux

package unix

import (
	"net"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
)

// peerCredPrincipal is a no-op on platforms without SO_PEERCRED support.
// It always returns ErrPeerCredUnsupported so the unix server falls back to the
// configured Authenticator (NoOp/anonymous by default), keeping the package
// buildable and behavior consistent across platforms.
func peerCredPrincipal(conn net.Conn) (*adapters.Principal, error) {
	_ = conn
	return nil, ErrPeerCredUnsupported
}
