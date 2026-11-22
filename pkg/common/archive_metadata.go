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
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
)

// ErrArchiveMetadataUnsupported is returned by Archive when the object carries
// client-side encryption metadata (encryption_*) and the destination cannot
// store object metadata: the archived ciphertext could never be decrypted.
var ErrArchiveMetadataUnsupported = errors.New("archive destination cannot store the object's encryption metadata")

// MetadataArchiver is an Archiver that can also store object metadata.
// Storage backends satisfy it through PutWithMetadata.
type MetadataArchiver interface {
	Archiver
	PutWithMetadata(ctx context.Context, key string, data io.Reader, metadata *Metadata) error
}

const (
	clientEncryptionPrefix = "encryption_"
	atRestEncryptionPrefix = "at_rest_encryption_"
)

// HasClientEncryption reports whether metadata records client-side
// encryption (the encryption_* keys written by the encrypted storage wrapper).
func HasClientEncryption(metadata *Metadata) bool {
	if metadata == nil {
		return false
	}
	// Case-insensitive: S3 returns user metadata keys canonicalised
	// ("Encryption_key_id").
	for k := range metadata.Custom {
		if strings.HasPrefix(strings.ToLower(k), clientEncryptionPrefix) {
			return true
		}
	}
	return false
}

// HasMetadataFields reports whether metadata carries anything a backend must
// store with the object: a content type or encoding, or a custom key.
func HasMetadataFields(metadata *Metadata) bool {
	if metadata == nil {
		return false
	}
	return metadata.ContentType != "" || metadata.ContentEncoding != "" || len(metadata.Custom) > 0
}

// ArchiveCopy writes data, the content of key as read from the source
// backend, to destination.
//
// An object with client-side encryption metadata is only usable together
// with that metadata, so it is written with PutWithMetadata, carrying its
// content type, content encoding and custom metadata. The source's
// at_rest_encryption_* keys are dropped: the source read has already removed
// its at-rest layer, and the destination applies its own. A destination that
// cannot store metadata is refused with ErrArchiveMetadataUnsupported before
// anything is written. Other objects are copied with Put, as before.
func ArchiveCopy(ctx context.Context, key string, data io.Reader, metadata *Metadata, destination Archiver) error {
	if destination == nil {
		return ErrArchiveDestinationNil
	}
	if !HasClientEncryption(metadata) {
		return destination.Put(key, data)
	}
	md, ok := destination.(MetadataArchiver)
	if !ok {
		return fmt.Errorf("%w: %s (destination %T)", ErrArchiveMetadataUnsupported, key, destination)
	}
	return md.PutWithMetadata(ctx, key, data, archivedMetadata(metadata))
}

// archivedMetadata is the metadata the archived copy carries: the source's
// descriptive fields and custom keys, without at-rest keys or the
// size/ETag/timestamp the destination computes itself. Client "encryption_*"
// names are written lowercased; other custom names are kept as they are.
func archivedMetadata(src *Metadata) *Metadata {
	out := &Metadata{
		ContentType:     src.ContentType,
		ContentEncoding: src.ContentEncoding,
		Custom:          make(map[string]string, len(src.Custom)),
	}
	for k, v := range src.Custom {
		lower := strings.ToLower(k)
		if strings.HasPrefix(lower, atRestEncryptionPrefix) {
			continue
		}
		if strings.HasPrefix(lower, clientEncryptionPrefix) {
			// The encryption wrapper reads only the lowercase names: a
			// source that canonicalises them (S3 "Encryption_key_id")
			// would otherwise give a copy read with the default key. An
			// exact lowercase entry in the source wins over another case.
			if _, exact := src.Custom[lower]; exact && lower != k {
				continue
			}
			k = lower
		}
		out.Custom[k] = v
	}
	return out
}
