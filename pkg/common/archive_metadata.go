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
	for k := range metadata.Custom {
		if strings.HasPrefix(k, clientEncryptionPrefix) {
			return true
		}
	}
	return false
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
// size/ETag/timestamp the destination computes itself.
func archivedMetadata(src *Metadata) *Metadata {
	out := &Metadata{
		ContentType:     src.ContentType,
		ContentEncoding: src.ContentEncoding,
		Custom:          make(map[string]string, len(src.Custom)),
	}
	for k, v := range src.Custom {
		if strings.HasPrefix(k, atRestEncryptionPrefix) {
			continue
		}
		out.Custom[k] = v
	}
	return out
}
