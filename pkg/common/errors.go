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

import "errors"

var (
	// Configuration errors

	// ErrNotConfigured is returned when a storage backend is not properly configured.
	ErrNotConfigured = errors.New("not configured")

	// ErrPathNotSet is returned when the required path is not set.
	ErrPathNotSet = errors.New("path not set")

	// ErrBucketNotSet is returned when the required bucket is not set.
	ErrBucketNotSet = errors.New("bucket not set")

	// ErrAccountNotSet is returned when required account credentials are not set.
	ErrAccountNotSet = errors.New("accountName, accountKey, or containerName not set")

	// ErrVaultNotSet is returned when the required vault name is not set.
	ErrVaultNotSet = errors.New("vaultName not set")

	// ErrRegionNotSet is returned when the required region is not set.
	ErrRegionNotSet = errors.New("region not set")

	// ErrContainerNotSet is returned when the required container name is not set.
	ErrContainerNotSet = errors.New("containerName not set")

	// ErrEndpointNotSet is returned when the required endpoint is not set.
	ErrEndpointNotSet = errors.New("endpoint not set")

	// ErrAccessKeyNotSet is returned when the required access key is not set.
	ErrAccessKeyNotSet = errors.New("accessKey not set")

	// ErrSecretKeyNotSet is returned when the required secret key is not set.
	ErrSecretKeyNotSet = errors.New("secretKey not set")

	// ErrInvalidLifecycleManagerType is returned when an invalid lifecycle manager type is specified.
	ErrInvalidLifecycleManagerType = errors.New("invalid lifecycleManagerType")

	// Storage operation errors

	// ErrStorageRequired is returned when a storage backend is required but not provided.
	ErrStorageRequired = errors.New("storage backend is required")

	// ErrArchiveDestinationNil is returned when the archive destination is nil.
	ErrArchiveDestinationNil = errors.New("archive destination cannot be nil")

	// ErrInvalidStorageHandle is returned when an invalid storage handle is provided.
	ErrInvalidStorageHandle = errors.New("invalid storage handle")

	// ErrBufferTooSmall is returned when a buffer is too small for the requested operation.
	ErrBufferTooSmall = errors.New("buffer too small")

	// ErrKeyNotFound is returned when a key is not found in storage.
	ErrKeyNotFound = errors.New("key not found")

	// ErrInternal is returned for internal errors during operations.
	ErrInternal = errors.New("internal error")

	// Lifecycle policy errors

	// ErrPolicyNil is returned when a policy is nil.
	ErrPolicyNil = errors.New("policy cannot be nil")

	// ErrDestinationTypeRequired is returned when destination_type is required for archive action.
	ErrDestinationTypeRequired = errors.New("destination_type required for archive action")

	// Replication policy errors

	// ErrPolicyNotFound is returned when a replication policy is not found.
	ErrPolicyNotFound = errors.New("replication policy not found")
)
