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

// Package multipart holds the rules shared by the providers that emulate
// multipart upload (memory and local): upload IDs, part and completion
// checks, part ETags and the expiry setting. Keeping them here makes every
// emulated provider behave the same way.
package multipart

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

const (
	// ExpirySetting is the settings key that overrides DefaultExpiry.
	ExpirySetting = "multipartExpiry"

	// DefaultExpiry is how long an upload may stay open before it is
	// treated as aborted. It matches Azure's fixed discard of uncommitted
	// blocks.
	DefaultExpiry = 168 * time.Hour

	// idLength is the length of an upload ID: 128 random bits, hex-encoded.
	idLength = 32
)

// ParseExpiry reads the multipartExpiry setting. A missing or empty value
// yields DefaultExpiry; a value that is not a positive duration is an error
// wrapping common.ErrInvalidArgument.
func ParseExpiry(settings map[string]string) (time.Duration, error) {
	raw, ok := settings[ExpirySetting]
	if !ok || raw == "" {
		return DefaultExpiry, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("%w: %s must be a positive duration, got %q", common.ErrInvalidArgument, ExpirySetting, raw)
	}
	return d, nil
}

// Expired reports whether an upload initiated at the given time has outlived
// the expiry as of now.
func Expired(initiated time.Time, expiry time.Duration, now time.Time) bool {
	return now.Sub(initiated) > expiry
}

// NewUploadID returns 128 random bits, hex-encoded.
func NewUploadID() (string, error) {
	var b [idLength / 2]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("multipart: generate upload id: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

// ValidUploadID reports whether id has the shape NewUploadID produces. A
// provider that maps the ID to a path must check this first.
func ValidUploadID(id string) bool {
	if len(id) != idLength {
		return false
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// ValidateInitiate checks the key and the metadata as Put does.
func ValidateInitiate(key string, metadata *common.Metadata) error {
	if err := common.ValidateKey(key); err != nil {
		return err
	}
	if metadata != nil {
		if err := common.ValidateMetadata(metadata.Custom); err != nil {
			return err
		}
	}
	return nil
}

// ValidatePartNumber checks 1..MaxParts.
func ValidatePartNumber(n int) error {
	if n < 1 || n > common.MaxParts {
		return fmt.Errorf("%w: %d is outside 1..%d", common.ErrInvalidPartNumber, n, common.MaxParts)
	}
	return nil
}

// CloneMetadata returns a deep copy of the metadata, or an empty value when
// it is nil, so a staged upload does not alias the caller's struct.
func CloneMetadata(m *common.Metadata) *common.Metadata {
	if m == nil {
		return &common.Metadata{}
	}
	c := *m
	if m.Custom != nil {
		c.Custom = make(map[string]string, len(m.Custom))
		for k, v := range m.Custom {
			c.Custom[k] = v
		}
	}
	return &c
}

// CopyPart copies exactly size bytes from src to dst and returns the part's
// ETag. It rejects a size outside 0..MaxPartSize before reading, and a body
// shorter or longer than size with common.ErrInvalidArgument.
func CopyPart(dst io.Writer, src io.Reader, size int64) (string, error) {
	r, err := NewPartReader(src, size)
	if err != nil {
		return "", err
	}
	if _, err := io.Copy(dst, r); err != nil {
		return "", err
	}
	return r.Finish()
}

// PartReader hands a part body to a consumer while it hashes the bytes that
// pass and counts them, so the body can be checked against its declared size
// without keeping a copy of it. Wrap the body, give the reader to whatever
// stores or seals it, then call Finish.
type PartReader struct {
	src  io.Reader
	size int64
	n    int64
	h    hash.Hash
}

// NewPartReader returns a reader over the first size bytes of src. It rejects
// a size that is negative or larger than common.MaxPartSize.
func NewPartReader(src io.Reader, size int64) (*PartReader, error) {
	if size < 0 {
		return nil, fmt.Errorf("%w: part size %d is negative", common.ErrInvalidArgument, size)
	}
	if size > common.MaxPartSize {
		return nil, fmt.Errorf("%w: part size %d exceeds %d", common.ErrPartTooLarge, size, int64(common.MaxPartSize))
	}
	return &PartReader{src: src, size: size, h: sha256.New()}, nil
}

// Read implements io.Reader; it reports EOF once size bytes have been served.
func (r *PartReader) Read(p []byte) (int, error) {
	if remaining := r.size - r.n; int64(len(p)) > remaining {
		p = p[:remaining]
	}
	if len(p) == 0 {
		return 0, io.EOF
	}
	n, err := r.src.Read(p)
	r.n += int64(n)
	_, _ = r.h.Write(p[:n]) // hash.Hash.Write never fails
	return n, err
}

// Finish checks that the body was exactly the declared size and returns the
// hex SHA-256 of it, the part's entity tag. It must be called after the
// consumer has read everything it wants.
func (r *PartReader) Finish() (string, error) {
	if r.n != r.size {
		return "", fmt.Errorf("%w: part body is %d bytes, size says %d", common.ErrInvalidArgument, r.n, r.size)
	}
	var extra [1]byte
	if m, rerr := io.ReadFull(r.src, extra[:]); m > 0 {
		return "", fmt.Errorf("%w: part body is longer than size %d", common.ErrInvalidArgument, r.size)
	} else if rerr != nil && rerr != io.EOF {
		return "", rerr
	}
	return hex.EncodeToString(r.h.Sum(nil)), nil
}

// CheckComplete validates the parts named at completion against the stored
// parts, which lookup returns by part number. It returns, in order, the part
// numbers to assemble.
//
// The parts must be non-empty and in strictly ascending part-number order
// (common.ErrInvalidPartOrder), every number must be in range
// (common.ErrInvalidPartNumber), each must match a stored part's ETag
// (common.ErrPartMismatch), and every part except the last must be at least
// common.MinPartSize (common.ErrPartTooSmall).
func CheckComplete(parts []common.CompletedPart, lookup func(partNumber int) (common.PartInfo, bool)) ([]int, error) {
	if len(parts) == 0 {
		return nil, fmt.Errorf("%w: no parts given", common.ErrInvalidPartOrder)
	}
	prev := 0
	for _, p := range parts {
		if err := ValidatePartNumber(p.PartNumber); err != nil {
			return nil, err
		}
		if p.PartNumber <= prev {
			return nil, fmt.Errorf("%w: part %d follows part %d", common.ErrInvalidPartOrder, p.PartNumber, prev)
		}
		prev = p.PartNumber
	}
	numbers := make([]int, 0, len(parts))
	for i, p := range parts {
		stored, ok := lookup(p.PartNumber)
		if !ok {
			return nil, fmt.Errorf("%w: part %d was not uploaded", common.ErrPartMismatch, p.PartNumber)
		}
		if stored.ETag != p.ETag {
			return nil, fmt.Errorf("%w: part %d etag does not match", common.ErrPartMismatch, p.PartNumber)
		}
		if i < len(parts)-1 && stored.Size < common.MinPartSize {
			return nil, fmt.Errorf("%w: part %d is %d bytes, minimum is %d", common.ErrPartTooSmall,
				p.PartNumber, stored.Size, common.MinPartSize)
		}
		numbers = append(numbers, p.PartNumber)
	}
	return numbers, nil
}
