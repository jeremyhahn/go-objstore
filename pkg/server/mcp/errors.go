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

package mcp

import "errors"

var (
	// Tool and parameter errors

	// ErrUnknownTool is returned when an unknown tool is requested.
	ErrUnknownTool = errors.New("unknown tool")

	// ErrMissingParameter is returned when a required parameter is missing.
	ErrMissingParameter = errors.New("missing parameter")

	// ErrInvalidParameter is returned when a parameter has an invalid value or type.
	ErrInvalidParameter = errors.New("invalid parameter")

	// ErrInvalidAction is returned when an invalid policy action is specified.
	ErrInvalidAction = errors.New("action must be 'delete' or 'archive'")

	// ErrDestinationTypeRequired is returned when destination_type is required for archive action.
	ErrDestinationTypeRequired = errors.New("destination_type required for archive action")

	// ErrRetentionMustBePositive is returned when retention_seconds is not positive.
	ErrRetentionMustBePositive = errors.New("retention_seconds must be positive")

	// Server errors

	// ErrStorageRequired is returned when storage backend is required but not provided.
	ErrStorageRequired = errors.New("storage backend is required")

	// ErrUnknownServerMode is returned when an unknown server mode is specified.
	ErrUnknownServerMode = errors.New("unknown server mode")

	// Resource errors

	// ErrResourceSubscriptionsNotImplemented is returned when resource subscriptions are not yet implemented.
	ErrResourceSubscriptionsNotImplemented = errors.New("resource subscriptions not yet implemented")

	// Policy errors

	// ErrPolicyAlreadyExists is returned when attempting to add a policy that already exists.
	ErrPolicyAlreadyExists = errors.New("policy already exists")

	// ErrPolicyNotFound is returned when a policy is not found.
	ErrPolicyNotFound = errors.New("policy not found")

	// ErrPolicyNil is returned when a policy is nil.
	ErrPolicyNil = errors.New("policy cannot be nil")
)
