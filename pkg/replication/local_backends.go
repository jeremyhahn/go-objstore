// Copyright (c) 2025 Jeremy Hahn
// Copyright (c) 2025 Automate The Things, LLC
//
// This file is part of go-objstore.
//
// go-objstore is dual-licensed:
//
// 1. GNU Affero General Public License v3.0 (AGPL-3.0)
//    See LICENSE file or visit https://www.gnu.org/licenses/agpl-3.0.html
//
// 2. Commercial License
//    Contact licensing@automatethethings.com for commercial licensing options.

package replication

import (
	"fmt"
	"strings"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// ErrClientLocalBackend is returned when a replication policy names a backend
// type whose settings take a filesystem path (such as "local") while the
// manager refuses them. A policy's backend type and settings come from the
// client, so such a backend could open any directory the service user can
// read. It wraps common.ErrInvalidArgument, so every transport reports it as
// an invalid argument.
var ErrClientLocalBackend = fmt.Errorf(
	"replication: backend type takes a filesystem path and client-supplied filesystem backends are disabled: %w",
	common.ErrInvalidArgument)

// pathBackendTypes lists the storage backend types whose settings take a
// filesystem path.
var pathBackendTypes = map[string]bool{
	"local": true,
}

// IsPathBackendType reports whether the storage backend type takes a
// filesystem path in its settings.
func IsPathBackendType(backendType string) bool {
	return pathBackendTypes[strings.ToLower(strings.TrimSpace(backendType))]
}

// WithClientLocalBackends sets whether policies may name a backend type whose
// settings take a filesystem path (see IsPathBackendType). The default is to
// refuse them: AddPolicy returns ErrClientLocalBackend and loading a policy
// file that holds one fails.
func WithClientLocalBackends(allow bool) ManagerOption {
	return func(prm *PersistentReplicationManager) {
		prm.allowClientLocalBackends = allow
	}
}

// checkPolicyBackends refuses a policy whose source or destination backend
// type takes a filesystem path, unless the manager allows them.
func (prm *PersistentReplicationManager) checkPolicyBackends(policy common.ReplicationPolicy) error {
	if prm.allowClientLocalBackends {
		return nil
	}
	if IsPathBackendType(policy.SourceBackend) {
		return fmt.Errorf("%w: policy %q source backend %q", ErrClientLocalBackend, policy.ID, policy.SourceBackend)
	}
	if IsPathBackendType(policy.DestinationBackend) {
		return fmt.Errorf("%w: policy %q destination backend %q",
			ErrClientLocalBackend, policy.ID, policy.DestinationBackend)
	}
	return nil
}
