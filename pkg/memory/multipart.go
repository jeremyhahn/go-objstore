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

package memory

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/jeremyhahn/go-objstore/internal/multipart"
	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// Compile-time check that Memory satisfies the multipart capability.
var _ common.MultipartUploader = (*Memory)(nil)

// upload is one multipart upload in progress.
type upload struct {
	key       string
	metadata  *common.Metadata
	initiated time.Time
	parts     map[int]*part
}

// part is one staged part.
type part struct {
	data []byte
	etag string
}

func (m *Memory) clock() time.Time {
	if m.now != nil {
		return m.now()
	}
	return time.Now()
}

func (m *Memory) expiry() time.Duration {
	if m.multipartExpiry > 0 {
		return m.multipartExpiry
	}
	return multipart.DefaultExpiry
}

// liveUploadLocked returns the upload for the ID and key. An upload past its
// expiry is treated as aborted: it is dropped and reported as not found.
// The caller holds m.mu for writing.
func (m *Memory) liveUploadLocked(key, uploadID string) (*upload, error) {
	u, ok := m.uploads[uploadID]
	if !ok || u.key != key {
		return nil, fmt.Errorf("%w: %s", common.ErrUploadNotFound, uploadID)
	}
	if multipart.Expired(u.initiated, m.expiry(), m.clock()) {
		delete(m.uploads, uploadID)
		return nil, fmt.Errorf("%w: %s (expired)", common.ErrUploadNotFound, uploadID)
	}
	return u, nil
}

// InitiateMultipartUpload starts a multipart upload for the key.
func (m *Memory) InitiateMultipartUpload(ctx context.Context, key string, metadata *common.Metadata) (string, error) {
	if err := multipart.ValidateInitiate(key, metadata); err != nil {
		return "", err
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	id, err := multipart.NewUploadID()
	if err != nil {
		return "", err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.uploads == nil {
		m.uploads = make(map[string]*upload)
	}
	m.uploads[id] = &upload{
		key:       key,
		metadata:  multipart.CloneMetadata(metadata),
		initiated: m.clock(),
		parts:     make(map[int]*part),
	}
	return id, nil
}

// UploadPart stages one part of an upload.
func (m *Memory) UploadPart(ctx context.Context, key, uploadID string, partNumber int, data io.Reader, size int64) (common.PartInfo, error) {
	if err := common.ValidateKey(key); err != nil {
		return common.PartInfo{}, err
	}
	if err := multipart.ValidatePartNumber(partNumber); err != nil {
		return common.PartInfo{}, err
	}
	if err := ctx.Err(); err != nil {
		return common.PartInfo{}, err
	}

	// Fail before reading the body when the upload is unknown.
	m.mu.Lock()
	_, err := m.liveUploadLocked(key, uploadID)
	m.mu.Unlock()
	if err != nil {
		return common.PartInfo{}, err
	}

	var buf bytes.Buffer
	etag, err := multipart.CopyPart(&buf, data, size)
	if err != nil {
		return common.PartInfo{}, err
	}
	if err := ctx.Err(); err != nil {
		return common.PartInfo{}, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	u, err := m.liveUploadLocked(key, uploadID)
	if err != nil {
		return common.PartInfo{}, err
	}
	u.parts[partNumber] = &part{data: buf.Bytes(), etag: etag}
	return common.PartInfo{PartNumber: partNumber, ETag: etag, Size: size}, nil
}

// CompleteMultipartUpload assembles the parts into the object. The object
// appears whole or not at all, and the staged parts are dropped on success.
func (m *Memory) CompleteMultipartUpload(ctx context.Context, key, uploadID string, parts []common.CompletedPart) error {
	if err := common.ValidateKey(key); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	u, err := m.liveUploadLocked(key, uploadID)
	if err != nil {
		return err
	}
	numbers, err := multipart.CheckComplete(parts, func(n int) (common.PartInfo, bool) {
		p, ok := u.parts[n]
		if !ok {
			return common.PartInfo{}, false
		}
		return common.PartInfo{PartNumber: n, ETag: p.etag, Size: int64(len(p.data))}, true
	})
	if err != nil {
		return err
	}

	var size int
	for _, n := range numbers {
		size += len(u.parts[n].data)
	}
	assembled := make([]byte, 0, size)
	for _, n := range numbers {
		assembled = append(assembled, u.parts[n].data...)
	}

	md := multipart.CloneMetadata(u.metadata)
	md.Size = int64(len(assembled))
	md.LastModified = time.Now()
	md.ETag = fmt.Sprintf("%d-%d", md.LastModified.Unix(), md.Size)
	if m.objects == nil {
		m.objects = make(map[string]*object)
	}
	m.objects[key] = &object{data: assembled, metadata: md}
	delete(m.uploads, uploadID)
	return nil
}

// AbortMultipartUpload discards an upload and its staged parts. An upload
// past its expiry is still removed, so the cleanup that lists and aborts
// expired uploads succeeds.
func (m *Memory) AbortMultipartUpload(ctx context.Context, key, uploadID string) error {
	if err := common.ValidateKey(key); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	u, ok := m.uploads[uploadID]
	if !ok || u.key != key {
		return fmt.Errorf("%w: %s", common.ErrUploadNotFound, uploadID)
	}
	delete(m.uploads, uploadID)
	return nil
}

// ListMultipartUploads returns the uploads in progress whose key starts with
// the prefix, including those past their expiry that no call has dropped yet,
// sorted by key and then by initiation time.
func (m *Memory) ListMultipartUploads(ctx context.Context, prefix string) ([]common.MultipartUploadInfo, error) {
	if prefix != "" {
		if err := common.ValidateKey(prefix); err != nil {
			return nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]common.MultipartUploadInfo, 0, len(m.uploads))
	for id, u := range m.uploads {
		if strings.HasPrefix(u.key, prefix) {
			out = append(out, common.MultipartUploadInfo{Key: u.key, UploadID: id, Initiated: u.initiated})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Key != out[j].Key {
			return out[i].Key < out[j].Key
		}
		if !out[i].Initiated.Equal(out[j].Initiated) {
			return out[i].Initiated.Before(out[j].Initiated)
		}
		return out[i].UploadID < out[j].UploadID
	})
	return out, nil
}
