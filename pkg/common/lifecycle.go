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

package common

import (
	"time"
)

var (
	ErrInvalidPolicy         = ErrPolicyNil // Alias for backward compatibility
	ErrLifecycleNotSupported = ErrInvalidLifecycleManagerType
)

// LifecyclePolicy defines a lifecycle policy for an object.
type LifecyclePolicy struct {
	// ID is the unique identifier for the policy.
	ID string
	// Prefix is the prefix of the objects to which the policy applies.
	Prefix string
	// Retention is the duration for which the object is retained.
	Retention time.Duration
	// Action is the action to be taken after the retention period.
	// It can be "delete" or "archive".
	Action string
	// Destination specifies where to archive to when Action=="archive".
	// For non-archive actions, this is ignored.
	Destination Archiver
}

// LifecycleManager is the interface for managing lifecycle policies.
type LifecycleManager interface {
	// AddPolicy adds a new lifecycle policy.
	AddPolicy(policy LifecyclePolicy) error
	// RemovePolicy removes a lifecycle policy.
	RemovePolicy(id string) error
	// GetPolicies returns all the lifecycle policies.
	GetPolicies() ([]LifecyclePolicy, error)
}
