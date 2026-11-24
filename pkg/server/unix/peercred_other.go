//go:build !linux

package unix

import (
	"net"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
)

// peerCredPrincipal always returns ErrPeerCredUnsupported where SO_PEERCRED is unavailable.
func peerCredPrincipal(conn net.Conn) (*adapters.Principal, error) {
	_ = conn
	return nil, ErrPeerCredUnsupported
}
