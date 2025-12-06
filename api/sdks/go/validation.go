// Copyright (c) 2025 Jeremy Hahn
// Copyright (c) 2025 Automate The Things, LLC
//
// This file is part of go-objstore.
//
// go-objstore is dual-licensed under AGPL-3.0 and a Commercial License.

package objstore

import (
	"strings"
)

// validateKey checks if a key is valid (non-empty).
func validateKey(key string) error {
	if strings.TrimSpace(key) == "" {
		return ErrInvalidKey
	}
	return nil
}

// validateData checks if data is valid (non-nil).
func validateData(data []byte) error {
	if data == nil {
		return ErrInvalidData
	}
	return nil
}

// validatePolicyID checks if a policy ID is valid (non-empty).
func validatePolicyID(policyID string) error {
	if strings.TrimSpace(policyID) == "" {
		return ErrInvalidPolicyID
	}
	return nil
}

// validateLifecyclePolicy checks if a lifecycle policy is valid.
func validateLifecyclePolicy(policy *LifecyclePolicy) error {
	if policy == nil {
		return ErrInvalidPolicy
	}
	if strings.TrimSpace(policy.ID) == "" {
		return ErrInvalidPolicyID
	}
	return nil
}

// validateReplicationPolicy checks if a replication policy is valid.
func validateReplicationPolicy(policy *ReplicationPolicy) error {
	if policy == nil {
		return ErrInvalidPolicy
	}
	if strings.TrimSpace(policy.ID) == "" {
		return ErrInvalidPolicyID
	}
	return nil
}

// validateMetadata checks if metadata is valid for operations that require it.
func validateMetadata(metadata *Metadata) error {
	if metadata == nil {
		return ErrInvalidMetadata
	}
	return nil
}
