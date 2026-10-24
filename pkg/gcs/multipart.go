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

//go:build gcpstorage

package gcs

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jeremyhahn/go-objstore/internal/multipart"
	"github.com/jeremyhahn/go-objstore/pkg/common"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

// composeLimit is the most source objects one compose call takes.
const composeLimit = 32

// manifestName is the object, inside the directory of an upload, that records
// the upload. Part objects are named by their number, so no part can collide
// with it.
const manifestName = "manifest"

// gcsComposer is implemented by a bucket that composes objects. It is kept
// apart from gcsBucket so that a bucket without it still serves objects.
type gcsComposer interface {
	// Compose concatenates the source objects, in order, into dst. A non-nil
	// attrs sets the content type, content encoding and custom metadata of
	// dst.
	Compose(ctx context.Context, dst string, sources []string, attrs *storage.ObjectAttrs) error
}

// Compose concatenates the source objects into dst.
func (b bucketWrapper) Compose(ctx context.Context, dst string, sources []string, attrs *storage.ObjectAttrs) error {
	handles := make([]*storage.ObjectHandle, len(sources))
	for i, name := range sources {
		handles[i] = b.BucketHandle.Object(name)
	}
	c := b.BucketHandle.Object(dst).ComposerFrom(handles...)
	if attrs != nil {
		c.ContentType = attrs.ContentType
		c.ContentEncoding = attrs.ContentEncoding
		c.Metadata = attrs.Metadata
	}
	_, err := c.Run(ctx)
	return err
}

// manifest is the record of an upload, stored as the manifest object.
type manifest struct {
	Key       string           `json:"key"`
	Metadata  *common.Metadata `json:"metadata,omitempty"`
	Initiated time.Time        `json:"initiated"`
}

// uploadPart is a part object found in the directory of an upload.
type uploadPart struct {
	name string
	size int64
	etag string
}

func (g *GCS) expiry() time.Duration {
	if g.multipartExpiry > 0 {
		return g.multipartExpiry
	}
	return multipart.DefaultExpiry
}

// requireBucket reports ErrBucketNotSet for a manager that has no bucket of
// its own.
func (g *GCS) requireBucket() error {
	if g.bucket == "" {
		return common.ErrBucketNotSet
	}
	return nil
}

func notFound(uploadID string) error {
	return fmt.Errorf("%w: %s", common.ErrUploadNotFound, uploadID)
}

// uploadDir is the prefix of the objects of an upload.
func uploadDir(uploadID string) string {
	return common.MultipartPrefix + uploadID + "/"
}

// readManifest loads the record of an upload. An ID that is not one this
// provider issues, or an upload the bucket does not hold, is ErrUploadNotFound.
func (g *GCS) readManifest(ctx context.Context, uploadID string) (*manifest, error) {
	if !multipart.ValidUploadID(uploadID) {
		return nil, notFound(uploadID)
	}
	return g.readManifestObject(ctx, uploadDir(uploadID)+manifestName, uploadID)
}

func (g *GCS) readManifestObject(ctx context.Context, name, uploadID string) (*manifest, error) {
	r, err := g.client.Bucket(g.bucket).Object(name).NewReader(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return nil, notFound(uploadID)
		}
		return nil, err
	}
	defer func() { _ = r.Close() }()
	raw, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	var m manifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("%w: unreadable manifest of %s: %v", common.ErrUploadNotFound, uploadID, err)
	}
	return &m, nil
}

// liveUpload loads an upload and checks that it belongs to key and has not
// outlived the multipartExpiry setting. An expired upload is treated as
// aborted and removed.
func (g *GCS) liveUpload(ctx context.Context, key, uploadID string) (*manifest, error) {
	m, err := g.readManifest(ctx, uploadID)
	if err != nil {
		return nil, err
	}
	if m.Key != key {
		return nil, notFound(uploadID)
	}
	if multipart.Expired(m.Initiated, g.expiry(), time.Now()) {
		_ = g.deleteUpload(ctx, uploadID) //nolint:errcheck // best effort; the expiry cleanup finds what is left
		return nil, fmt.Errorf("%w: %s (expired)", common.ErrUploadNotFound, uploadID)
	}
	return m, nil
}

// writeObject stores data as the object name. The object appears only if the
// write completes: a failed write is canceled, so it never replaces what was
// there.
func (g *GCS) writeObject(ctx context.Context, name string, fill func(w io.Writer) error) error {
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	w := g.client.Bucket(g.bucket).Object(name).NewWriter(wctx)
	if err := fill(w); err != nil {
		cancel()
		_ = w.Close()
		return err
	}
	return w.Close()
}

// InitiateMultipartUpload records an upload as a manifest object under the
// reserved prefix and returns its ID.
func (g *GCS) InitiateMultipartUpload(ctx context.Context, key string, metadata *common.Metadata) (string, error) {
	if err := multipart.ValidateInitiate(key, metadata); err != nil {
		return "", err
	}
	if err := g.requireBucket(); err != nil {
		return "", err
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	id, err := multipart.NewUploadID()
	if err != nil {
		return "", err
	}
	raw, err := json.Marshal(manifest{Key: key, Metadata: multipart.CloneMetadata(metadata), Initiated: time.Now().UTC()})
	if err != nil {
		return "", err
	}
	err = g.writeObject(ctx, uploadDir(id)+manifestName, func(w io.Writer) error {
		_, werr := w.Write(raw)
		return werr
	})
	if err != nil {
		return "", err
	}
	return id, nil
}

// UploadPart stores one part as an object under the directory of the upload.
// Storing a part number again replaces the part. The entity tag is the MD5 the
// service computed for the part object.
func (g *GCS) UploadPart(ctx context.Context, key, uploadID string, partNumber int, data io.Reader, size int64) (common.PartInfo, error) {
	if err := common.ValidateKey(key); err != nil {
		return common.PartInfo{}, err
	}
	if err := multipart.ValidatePartNumber(partNumber); err != nil {
		return common.PartInfo{}, err
	}
	if size < 0 {
		return common.PartInfo{}, fmt.Errorf("%w: negative part size", common.ErrInvalidArgument)
	}
	if size > common.MaxPartSize {
		return common.PartInfo{}, fmt.Errorf("%w: %d bytes, maximum is %d", common.ErrPartTooLarge, size, int64(common.MaxPartSize))
	}
	if err := g.requireBucket(); err != nil {
		return common.PartInfo{}, err
	}
	if err := ctx.Err(); err != nil {
		return common.PartInfo{}, err
	}
	if _, err := g.liveUpload(ctx, key, uploadID); err != nil {
		return common.PartInfo{}, err
	}

	name := uploadDir(uploadID) + strconv.Itoa(partNumber)
	err := g.writeObject(ctx, name, func(w io.Writer) error {
		_, cerr := multipart.CopyPart(w, data, size)
		return cerr
	})
	if err != nil {
		return common.PartInfo{}, err
	}
	attrs, err := g.client.Bucket(g.bucket).Object(name).Attrs(ctx)
	if err != nil {
		return common.PartInfo{}, err
	}
	return common.PartInfo{PartNumber: partNumber, ETag: partETag(attrs), Size: size}, nil
}

// partETag is the entity tag of a part object.
func partETag(attrs *storage.ObjectAttrs) string {
	return hex.EncodeToString(attrs.MD5)
}

// listUpload returns the part objects of an upload by number and the names of
// every object in its directory.
func (g *GCS) listUpload(ctx context.Context, uploadID string) (map[int]uploadPart, []string, error) {
	dir := uploadDir(uploadID)
	parts := map[int]uploadPart{}
	var names []string
	it := g.client.Bucket(g.bucket).Objects(ctx, &storage.Query{Prefix: dir})
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, nil, err
		}
		names = append(names, attrs.Name)
		// Intermediate objects of an earlier attempt to complete are not parts.
		n, err := strconv.Atoi(strings.TrimPrefix(attrs.Name, dir))
		if err != nil || n < 1 || n > common.MaxParts {
			continue
		}
		parts[n] = uploadPart{name: attrs.Name, size: attrs.Size, etag: partETag(attrs)}
	}
	return parts, names, nil
}

// CompleteMultipartUpload composes the listed parts into the object. More
// parts than one compose call takes are composed in a tree of intermediate
// objects. The metadata of the manifest is applied to the final object, and
// the objects of the upload are deleted afterwards.
//
// An upload older than the multipartExpiry setting is treated as aborted.
func (g *GCS) CompleteMultipartUpload(ctx context.Context, key, uploadID string, parts []common.CompletedPart) error {
	if err := common.ValidateKey(key); err != nil {
		return err
	}
	if err := g.requireBucket(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	m, err := g.liveUpload(ctx, key, uploadID)
	if err != nil {
		return err
	}
	stored, _, err := g.listUpload(ctx, uploadID)
	if err != nil {
		return err
	}
	numbers, err := multipart.CheckComplete(parts, func(n int) (common.PartInfo, bool) {
		p, ok := stored[n]
		return common.PartInfo{PartNumber: n, ETag: p.etag, Size: p.size}, ok
	})
	if err != nil {
		return err
	}

	composer, ok := g.client.Bucket(g.bucket).(gcsComposer)
	if !ok {
		return fmt.Errorf("%w: the gcs client cannot compose objects", common.ErrNotSupported)
	}
	sources := make([]string, 0, len(numbers))
	for _, n := range numbers {
		sources = append(sources, stored[n].name)
	}
	if err := composeTree(ctx, composer, uploadDir(uploadID), key, sources, objectAttrs(m.Metadata)); err != nil {
		return err
	}
	// The object is complete. What cannot be deleted now is found again by the
	// expiry cleanup, because the manifest goes last.
	_ = g.deleteUpload(ctx, uploadID) //nolint:errcheck // the object is complete; see above
	return nil
}

// objectAttrs converts the metadata of a manifest to object attributes.
func objectAttrs(md *common.Metadata) *storage.ObjectAttrs {
	if md == nil {
		return nil
	}
	attrs := &storage.ObjectAttrs{ContentType: md.ContentType, ContentEncoding: md.ContentEncoding}
	if len(md.Custom) > 0 {
		attrs.Metadata = make(map[string]string, len(md.Custom))
		for k, v := range md.Custom {
			attrs.Metadata[k] = v
		}
	}
	return attrs
}

// composeTree composes sources into dst, in order. While there are more
// sources than one call takes, they are composed in groups into intermediate
// objects in dir.
func composeTree(ctx context.Context, c gcsComposer, dir, dst string, sources []string, attrs *storage.ObjectAttrs) error {
	for level := 0; len(sources) > composeLimit; level++ {
		next := make([]string, 0, (len(sources)+composeLimit-1)/composeLimit)
		for i := 0; i < len(sources); i += composeLimit {
			group := sources[i:min(i+composeLimit, len(sources))]
			if len(group) == 1 {
				next = append(next, group[0])
				continue
			}
			name := fmt.Sprintf("%sc%d-%d", dir, level, i/composeLimit)
			if err := c.Compose(ctx, name, group, nil); err != nil {
				return err
			}
			next = append(next, name)
		}
		sources = next
	}
	return c.Compose(ctx, dst, sources, attrs)
}

// deleteUpload removes every object of an upload, the manifest last.
func (g *GCS) deleteUpload(ctx context.Context, uploadID string) error {
	_, names, err := g.listUpload(ctx, uploadID)
	if err != nil {
		return err
	}
	manifestObject := uploadDir(uploadID) + manifestName
	bucket := g.client.Bucket(g.bucket)
	var first error
	for _, name := range names {
		if name == manifestObject {
			continue
		}
		if err := bucket.Object(name).Delete(ctx); err != nil && !errors.Is(err, storage.ErrObjectNotExist) && first == nil {
			first = err
		}
	}
	if first != nil {
		return first
	}
	if err := bucket.Object(manifestObject).Delete(ctx); err != nil && !errors.Is(err, storage.ErrObjectNotExist) {
		return err
	}
	return nil
}

// AbortMultipartUpload discards an upload and its parts. An upload that has
// expired can still be aborted.
func (g *GCS) AbortMultipartUpload(ctx context.Context, key, uploadID string) error {
	if err := common.ValidateKey(key); err != nil {
		return err
	}
	if err := g.requireBucket(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	m, err := g.readManifest(ctx, uploadID)
	if err != nil {
		return err
	}
	if m.Key != key {
		return notFound(uploadID)
	}
	return g.deleteUpload(ctx, uploadID)
}

// ListMultipartUploads returns the uploads in progress whose key starts with
// prefix, sorted by key and then by initiation time. It reads one manifest per
// upload.
func (g *GCS) ListMultipartUploads(ctx context.Context, prefix string) ([]common.MultipartUploadInfo, error) {
	if prefix != "" {
		if err := common.ValidateKey(prefix); err != nil {
			return nil, err
		}
	}
	if err := g.requireBucket(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	var manifests []string
	it := g.client.Bucket(g.bucket).Objects(ctx, &storage.Query{Prefix: common.MultipartPrefix})
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, err
		}
		if strings.HasSuffix(attrs.Name, "/"+manifestName) {
			manifests = append(manifests, attrs.Name)
		}
	}

	out := make([]common.MultipartUploadInfo, 0, len(manifests))
	for _, name := range manifests {
		id := strings.TrimSuffix(strings.TrimPrefix(name, common.MultipartPrefix), "/"+manifestName)
		if !multipart.ValidUploadID(id) {
			continue
		}
		m, err := g.readManifestObject(ctx, name, id)
		if err != nil {
			if errors.Is(err, common.ErrUploadNotFound) {
				continue // aborted or unreadable since the listing
			}
			return nil, err
		}
		if strings.HasPrefix(m.Key, prefix) {
			out = append(out, common.MultipartUploadInfo{Key: m.Key, UploadID: id, Initiated: m.Initiated})
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
