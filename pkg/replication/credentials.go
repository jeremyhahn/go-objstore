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
	"context"
	"errors"
	"fmt"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// TargetSide names which end of a replication policy a credential is for.
type TargetSide string

const (
	// SourceSide is the backend objects are read from.
	SourceSide TargetSide = "source"
	// DestinationSide is the backend objects are written to.
	DestinationSide TargetSide = "destination"
)

// CredentialTarget identifies the backend of one policy a credential is for.
type CredentialTarget struct {
	// PolicyID is the ID of the replication policy being synced.
	PolicyID string

	// Side is the end of that policy the backend is.
	Side TargetSide
}

// CredentialProvider returns a fresh credential handle per target per sync, which the manager destroys; a zero value selects the SDK default chain.
type CredentialProvider interface {
	Credentials(ctx context.Context, target CredentialTarget) (common.Credentials, error)
}

// CredentialProviderFunc adapts a function to a CredentialProvider.
type CredentialProviderFunc func(ctx context.Context, target CredentialTarget) (common.Credentials, error)

// Credentials calls f.
func (f CredentialProviderFunc) Credentials(ctx context.Context, target CredentialTarget) (common.Credentials, error) {
	return f(ctx, target)
}

// ManagerOption sets an optional setting of a PersistentReplicationManager.
type ManagerOption func(*PersistentReplicationManager)

// WithCredentialProvider makes the manager take each target's credential
// handle from p. A nil p keeps the SDK default credential chain.
func WithCredentialProvider(p CredentialProvider) ManagerOption {
	return func(prm *PersistentReplicationManager) {
		prm.credentials = p
	}
}

// ErrCredentialProvider is matched by errors.Is for a credential provider
// failure.
var ErrCredentialProvider = errors.New("credential provider failed")

// ErrCredentialDestroy is returned when a credential handle could not be
// destroyed after its backend was configured.
var ErrCredentialDestroy = errors.New("credential handle not destroyed")

// CredentialProviderError names only the target; the provider's error is reachable only through Unwrap.
type CredentialProviderError struct {
	// Target is the backend the credential was for.
	Target CredentialTarget

	// Err is the error the provider returned.
	Err error
}

// Error describes the failure without the provider's error text.
func (e *CredentialProviderError) Error() string {
	return fmt.Sprintf("credential provider failed for policy %q %s backend", e.Target.PolicyID, e.Target.Side)
}

// Unwrap returns the provider's error.
func (e *CredentialProviderError) Unwrap() error { return e.Err }

// Is matches ErrCredentialProvider.
func (e *CredentialProviderError) Is(target error) bool { return target == ErrCredentialProvider }

// destroyCredentials destroys both handles of creds. The error names no
// handle content.
func destroyCredentials(creds common.Credentials) error {
	var failed bool
	if creds.Key != nil && creds.Key.Destroy() != nil {
		failed = true
	}
	if creds.Secret != nil && creds.Secret.Destroy() != nil {
		failed = true
	}
	if failed {
		return ErrCredentialDestroy
	}
	return nil
}
