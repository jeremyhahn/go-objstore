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

//go:build azureblob

package azure

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jeremyhahn/go-objstore/internal/multipart"
	"github.com/jeremyhahn/go-objstore/pkg/common"

	"github.com/Azure/azure-storage-blob-go/azblob"
)

// Multipart upload on Azure is emulated over native blocks. The data of a
// part is staged as an uncommitted block of a staging blob that belongs to the
// upload, and completing commits the block list of that blob, stages the
// target's own blocks from ranges of it on the service, and commits those with
// one block list, which makes the object appear at once and leaves the old
// content of the key in place until then. The parts are not staged on the
// target: committing a blob discards every uncommitted block the list does not
// name, so uploads of one key, or a plain overwrite of it, would destroy each
// other's parts. A completion that failed after the staging blob was committed
// finds its blocks committed and only publishes again. The service has no upload
// ID or upload metadata, so a manifest blob under the reserved prefix stands
// for them, and a small record blob per part holds the ETag that the service
// does not keep for a block:
//
//	.objstore-multipart/<uploadID>/manifest   key, metadata, initiation time
//	.objstore-multipart/<uploadID>/staging    the blocks of the parts
//	.objstore-multipart/<uploadID>/<number>   size and ETag of the part
//
// A part counts as uploaded only while its record exists. The record is
// removed before the block is staged and written after, so a part whose
// upload failed is simply absent. A blob of uncommitted blocks cannot be
// deleted, so an aborted or expired upload commits an empty list to the
// staging blob first and then deletes it.
const (
	manifestName = "manifest"

	// stagingName is the blob of an upload that holds its blocks.
	stagingName = "staging"

	// partDigits pads the part number so that every block ID of a blob has the
	// same length, which the service requires.
	partDigits = 5

	// maxBlockSize is the largest block the service takes, which is below
	// the largest part of the portable contract.
	maxBlockSize = 4000 << 20

	// maxBlocks is the most blocks the service takes for a blob.
	maxBlocks = 50000
)

// publishBlockSize is the size of the blocks the service reads from the
// staging blob into the target. A block staged from a URL is limited to 100
// MiB by the older versions of the service, so this is the size that is taken
// everywhere. It is a variable so that a test can publish in small blocks.
var publishBlockSize int64 = 100 << 20

// publishAttempts is how often a publication stages its blocks again after
// the commit of another upload of the key discarded them.
const publishAttempts = 10

var (
	errPartSize = errors.New("part body does not match the declared size")
	errPartSeek = errors.New("part body of an unseekable source cannot seek")
)

// blockAPI is implemented by a blob that supports native blocks. It is kept
// apart from BlobAPI so that a blob without it still serves objects.
type blockAPI interface {
	StageBlock(ctx context.Context, blockID string, body io.ReadSeeker) error
	CommitBlockList(ctx context.Context, blockIDs []string, headers azblob.BlobHTTPHeaders, metadata map[string]string) error
	// StageBlockFromURL stages the range of a blob that the service reads
	// itself.
	StageBlockFromURL(ctx context.Context, blockID string, source url.URL, offset, count int64) error
	// UncommittedBlocks returns the staged blocks by ID with their sizes.
	UncommittedBlocks(ctx context.Context) (map[string]int64, error)
	// CommittedBlocks returns the blocks of the committed blob in order.
	CommittedBlocks(ctx context.Context) ([]blockSize, error)
}

// blockSize is a committed block.
type blockSize struct {
	ID   string
	Size int64
}

// sourceAPI is implemented by a container that names a blob of its own as the
// source of a block staged on the service, so that the data does not pass
// through the client.
type sourceAPI interface {
	SourceURL(name string) url.URL
}

func (c containerWrapper) SourceURL(name string) url.URL {
	return c.ContainerURL.NewBlobURL(name).URL()
}

// sourceLifetime is how long the read access to a staging blob lasts. It
// covers the staging of the blocks of one publication, and a retry signs
// again.
const sourceLifetime = time.Hour

// sourceURL names a blob of the container for a read by the service itself.
// With a shared key the URL carries a read-only service signature on that one
// blob, made here and never logged or stored; without one the URL is as the
// container names it.
func (a *Azure) sourceURL(src sourceAPI, name string) (url.URL, error) {
	u := src.SourceURL(name)
	if a.sharedKey == nil {
		return u, nil
	}
	now := time.Now().UTC()
	values := azblob.BlobSASSignatureValues{
		StartTime:     now.Add(-5 * time.Minute),
		ExpiryTime:    now.Add(sourceLifetime),
		ContainerName: a.containerName,
		BlobName:      name,
		Permissions:   azblob.BlobSASPermissions{Read: true}.String(),
	}
	if u.Scheme == "https" {
		values.Protocol = azblob.SASProtocolHTTPS
	}
	query, err := values.NewSASQueryParameters(a.sharedKey)
	if err != nil {
		return url.URL{}, fmt.Errorf("azure: sign the read of a staging blob: %w", err)
	}
	u.RawQuery = query.Encode()
	return u, nil
}

func (b blobWrapper) StageBlock(ctx context.Context, blockID string, body io.ReadSeeker) error {
	_, err := b.BlockBlobURL.StageBlock(ctx, blockID, body, azblob.LeaseAccessConditions{}, nil, azblob.ClientProvidedKeyOptions{})
	return err
}

func (b blobWrapper) CommitBlockList(ctx context.Context, blockIDs []string, headers azblob.BlobHTTPHeaders, metadata map[string]string) error {
	_, err := b.BlockBlobURL.CommitBlockList(ctx, blockIDs, headers, azblob.Metadata(metadata),
		azblob.BlobAccessConditions{}, azblob.DefaultAccessTier, nil, azblob.ClientProvidedKeyOptions{}, azblob.ImmutabilityPolicyOptions{})
	return err
}

func (b blobWrapper) StageBlockFromURL(ctx context.Context, blockID string, source url.URL, offset, count int64) error {
	_, err := b.BlockBlobURL.StageBlockFromURL(ctx, blockID, source, offset, count,
		azblob.LeaseAccessConditions{}, azblob.ModifiedAccessConditions{}, azblob.ClientProvidedKeyOptions{}, nil)
	return err
}

func (b blobWrapper) CommittedBlocks(ctx context.Context) ([]blockSize, error) {
	list, err := b.BlockBlobURL.GetBlockList(ctx, azblob.BlockListCommitted, azblob.LeaseAccessConditions{})
	if err != nil {
		if hasServiceCode(err, azblob.ServiceCodeBlobNotFound) {
			return nil, nil
		}
		return nil, err
	}
	blocks := make([]blockSize, 0, len(list.CommittedBlocks))
	for _, blk := range list.CommittedBlocks {
		blocks = append(blocks, blockSize{ID: blk.Name, Size: blk.Size})
	}
	return blocks, nil
}

func (b blobWrapper) UncommittedBlocks(ctx context.Context) (map[string]int64, error) {
	list, err := b.BlockBlobURL.GetBlockList(ctx, azblob.BlockListUncommitted, azblob.LeaseAccessConditions{})
	if err != nil {
		if hasServiceCode(err, azblob.ServiceCodeBlobNotFound) {
			return map[string]int64{}, nil
		}
		return nil, err
	}
	blocks := make(map[string]int64, len(list.UncommittedBlocks))
	for _, blk := range list.UncommittedBlocks {
		blocks[blk.Name] = blk.Size
	}
	return blocks, nil
}

// manifest is the persisted description of an upload in progress.
type manifest struct {
	Key       string           `json:"key"`
	Metadata  *common.Metadata `json:"metadata,omitempty"`
	Initiated time.Time        `json:"initiated"`
}

// partRecord is the persisted description of an uploaded part.
type partRecord struct {
	ETag string `json:"etag"`
	Size int64  `json:"size"`
}

func uploadDir(uploadID string) string { return common.MultipartPrefix + uploadID + "/" }

// stagingBlob is the name of the blob that holds the staged blocks of an upload.
func stagingBlob(uploadID string) string { return uploadDir(uploadID) + stagingName }

// blockID names the block of a part: the base64 of "<uploadID>/<number>",
// with the number zero-padded to a fixed width.
func blockID(uploadID string, partNumber int) string {
	return base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s/%0*d", uploadID, partDigits, partNumber)))
}

func notFound(uploadID string) error {
	return fmt.Errorf("%w: %s", common.ErrUploadNotFound, uploadID)
}

func (a *Azure) expiry() time.Duration {
	if a.multipartExpiry > 0 {
		return a.multipartExpiry
	}
	return multipart.DefaultExpiry
}

// requireContainer reports why the backend cannot serve one container.
func (a *Azure) requireContainer() error {
	switch {
	case a.container != nil:
		return nil
	case a.account != nil:
		return common.ErrBucketNotSet
	}
	return common.ErrNotConfigured
}

// blocks returns the block operations of the blob that will hold the object.
func (a *Azure) blocks(key string) (blockAPI, error) {
	b, ok := a.container.NewBlockBlob(key).(blockAPI)
	if !ok {
		return nil, fmt.Errorf("%w: the azure blob cannot stage blocks", common.ErrNotSupported)
	}
	return b, nil
}

// writeBlob stores a small blob.
func (a *Azure) writeBlob(ctx context.Context, name string, v any) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return a.container.NewBlockBlob(name).UploadFromReader(ctx, bytes.NewReader(raw))
}

// readBlob loads a small blob into v; a missing blob is common.ErrKeyNotFound.
func (a *Azure) readBlob(ctx context.Context, name string, v any) error {
	r, err := a.container.NewBlockBlob(name).NewReader(ctx)
	if err != nil {
		return mapNotFound(err, name)
	}
	defer func() { _ = r.Close() }()
	raw, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, v)
}

// readManifest loads the manifest of an upload, whether or not it expired.
func (a *Azure) readManifest(ctx context.Context, uploadID string) (*manifest, error) {
	if !multipart.ValidUploadID(uploadID) {
		return nil, notFound(uploadID)
	}
	return a.readManifestBlob(ctx, uploadDir(uploadID)+manifestName, uploadID)
}

func (a *Azure) readManifestBlob(ctx context.Context, name, uploadID string) (*manifest, error) {
	var m manifest
	if err := a.readBlob(ctx, name, &m); err != nil {
		var syntax *json.SyntaxError
		var typeErr *json.UnmarshalTypeError
		switch {
		case errors.Is(err, common.ErrKeyNotFound):
			return nil, notFound(uploadID)
		case errors.As(err, &syntax), errors.As(err, &typeErr):
			return nil, fmt.Errorf("%w: unreadable manifest of %s: %v", common.ErrUploadNotFound, uploadID, err)
		}
		return nil, err
	}
	return &m, nil
}

// liveUpload loads the manifest of an upload of key that has not expired. An
// expired upload counts as aborted and is dropped.
func (a *Azure) liveUpload(ctx context.Context, key, uploadID string) (*manifest, error) {
	m, err := a.readManifest(ctx, uploadID)
	if err != nil {
		return nil, err
	}
	if m.Key != key {
		return nil, notFound(uploadID)
	}
	if multipart.Expired(m.Initiated, a.expiry(), time.Now()) {
		_ = a.deleteUpload(ctx, uploadID) //nolint:errcheck // best effort; the expiry cleanup finds what is left
		return nil, fmt.Errorf("%w: %s (expired)", common.ErrUploadNotFound, uploadID)
	}
	return m, nil
}

// InitiateMultipartUpload starts an upload and returns its ID.
func (a *Azure) InitiateMultipartUpload(ctx context.Context, key string, metadata *common.Metadata) (string, error) {
	if err := multipart.ValidateInitiate(key, metadata); err != nil {
		return "", err
	}
	if err := a.requireContainer(); err != nil {
		return "", err
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	id, err := multipart.NewUploadID()
	if err != nil {
		return "", err
	}
	m := manifest{Key: key, Metadata: multipart.CloneMetadata(metadata), Initiated: time.Now().UTC()}
	if err := a.writeBlob(ctx, uploadDir(id)+manifestName, m); err != nil {
		return "", err
	}
	return id, nil
}

// partBody streams a part to the service while it counts and hashes the
// bytes. The service needs a seekable body to learn its length and to send
// it again, so Seek answers the size and a rewind to the start, but only
// while nothing has been read: a part of any size is never held in memory,
// and a retry after a partial send fails instead of sending garbage.
type partBody struct {
	r        io.Reader
	size     int64
	n        int64
	h        hash.Hash
	err      error // the failure of r itself
	mismatch bool  // r held fewer or more than size bytes
}

func (p *partBody) Read(b []byte) (int, error) {
	remaining := p.size - p.n
	if remaining == 0 {
		if err := p.probe(); err != nil {
			return 0, err
		}
		return 0, io.EOF
	}
	if int64(len(b)) > remaining {
		b = b[:remaining]
	}
	n, err := p.r.Read(b)
	p.n += int64(n)
	p.h.Write(b[:n])
	switch {
	case err == nil:
		return n, nil
	case err == io.EOF && p.n < p.size:
		p.mismatch = true
		return n, errPartSize
	case err == io.EOF:
		return n, nil
	}
	p.err = err
	return n, err
}

// probe checks that the source holds no byte beyond the declared size.
func (p *partBody) probe() error {
	var extra [1]byte
	n, err := io.ReadFull(p.r, extra[:])
	switch {
	case n > 0:
		p.mismatch = true
		return errPartSize
	case err != nil && err != io.EOF:
		p.err = err
		return err
	}
	return nil
}

func (p *partBody) Seek(offset int64, whence int) (int64, error) {
	switch {
	case offset == 0 && whence == io.SeekEnd:
		return p.size, nil
	case offset == 0 && whence == io.SeekCurrent:
		return p.n, nil
	case offset == 0 && whence == io.SeekStart && p.n == 0:
		return 0, nil
	}
	return 0, fmt.Errorf("%w: to %d (whence %d) after %d bytes", errPartSeek, offset, whence, p.n)
}

// finish reports why the body is not a faithful copy of the part, or nil.
func (p *partBody) finish() error {
	if p.err == nil && !p.mismatch && p.n == p.size {
		// The transport stops at the declared length without looking for more.
		if err := p.probe(); err != nil && !errors.Is(err, errPartSize) {
			return err
		}
	}
	switch {
	case p.err != nil:
		return p.err
	case p.mismatch || p.n != p.size:
		return fmt.Errorf("%w: part body does not match size %d", common.ErrInvalidArgument, p.size)
	}
	return nil
}

// UploadPart stages one part and returns its ETag, the SHA-256 of its data.
// Uploading a number again replaces the part.
func (a *Azure) UploadPart(ctx context.Context, key, uploadID string, partNumber int, data io.Reader, size int64) (common.PartInfo, error) {
	if err := common.ValidateKey(key); err != nil {
		return common.PartInfo{}, err
	}
	if err := multipart.ValidatePartNumber(partNumber); err != nil {
		return common.PartInfo{}, err
	}
	if size < 0 {
		return common.PartInfo{}, fmt.Errorf("%w: negative part size", common.ErrInvalidArgument)
	}
	if size > maxBlockSize {
		return common.PartInfo{}, fmt.Errorf("%w: %d bytes, the largest block is %d", common.ErrPartTooLarge, size, int64(maxBlockSize))
	}
	if err := a.requireContainer(); err != nil {
		return common.PartInfo{}, err
	}
	if err := ctx.Err(); err != nil {
		return common.PartInfo{}, err
	}
	if _, err := a.liveUpload(ctx, key, uploadID); err != nil {
		return common.PartInfo{}, err
	}
	blob, err := a.blocks(stagingBlob(uploadID))
	if err != nil {
		return common.PartInfo{}, err
	}

	// Until the new block is staged and recorded, the part is not uploaded.
	recordName := uploadDir(uploadID) + strconv.Itoa(partNumber)
	if err := a.container.NewBlockBlob(recordName).Delete(ctx); err != nil && !errors.Is(mapNotFound(err, recordName), common.ErrKeyNotFound) {
		return common.PartInfo{}, err
	}
	body := &partBody{r: data, size: size, h: sha256.New()}
	stageErr := blob.StageBlock(ctx, blockID(uploadID, partNumber), body)
	if err := body.finish(); err != nil {
		return common.PartInfo{}, err
	}
	if stageErr != nil {
		return common.PartInfo{}, stageErr
	}
	etag := hex.EncodeToString(body.h.Sum(nil))
	if err := a.writeBlob(ctx, recordName, partRecord{ETag: etag, Size: size}); err != nil {
		return common.PartInfo{}, err
	}
	return common.PartInfo{PartNumber: partNumber, ETag: etag, Size: size}, nil
}

// CompleteMultipartUpload commits the listed blocks in part order, which
// creates the object with the metadata of the manifest, and removes the
// staged state.
func (a *Azure) CompleteMultipartUpload(ctx context.Context, key, uploadID string, parts []common.CompletedPart) error {
	if err := common.ValidateKey(key); err != nil {
		return err
	}
	if err := a.requireContainer(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	m, err := a.liveUpload(ctx, key, uploadID)
	if err != nil {
		return err
	}
	src, ok := a.container.(sourceAPI)
	if !ok {
		return fmt.Errorf("%w: the azure container cannot name a source for a staged block", common.ErrNotSupported)
	}
	staging := stagingBlob(uploadID)
	blob, err := a.blocks(staging)
	if err != nil {
		return err
	}
	staged, err := blob.UncommittedBlocks(ctx)
	if err != nil {
		return err
	}
	committed, err := blob.CommittedBlocks(ctx)
	if err != nil {
		return err
	}
	committedSize := make(map[string]int64, len(committed))
	for _, blk := range committed {
		committedSize[blk.ID] = blk.Size
	}

	// A part is as recorded only if its block is still staged, or committed by
	// an earlier completion, with the recorded size. A block staged again
	// after that commit is the one the list takes.
	var lookupErr error
	numbers, err := multipart.CheckComplete(parts, func(n int) (common.PartInfo, bool) {
		var rec partRecord
		if err := a.readBlob(ctx, uploadDir(uploadID)+strconv.Itoa(n), &rec); err != nil {
			if !errors.Is(err, common.ErrKeyNotFound) && lookupErr == nil {
				lookupErr = err
			}
			return common.PartInfo{}, false
		}
		id := blockID(uploadID, n)
		size, ok := staged[id]
		if !ok {
			size, ok = committedSize[id]
		}
		if !ok || size != rec.Size {
			return common.PartInfo{}, false
		}
		return common.PartInfo{PartNumber: n, ETag: rec.ETag, Size: size}, true
	})
	if lookupErr != nil {
		return lookupErr
	}
	if err != nil {
		return err
	}

	ids := make([]string, 0, len(numbers))
	var total int64
	for _, n := range numbers {
		id := blockID(uploadID, n)
		ids = append(ids, id)
		if size, ok := staged[id]; ok {
			total += size
		} else {
			total += committedSize[id]
		}
	}
	if blocks := (total + publishBlockSize - 1) / publishBlockSize; blocks > maxBlocks {
		return fmt.Errorf("%w: %d bytes need %d blocks, the limit is %d", common.ErrPartTooLarge, total, blocks, maxBlocks)
	}
	var headers azblob.BlobHTTPHeaders
	var custom map[string]string
	if m.Metadata != nil {
		headers = azblob.BlobHTTPHeaders{ContentType: m.Metadata.ContentType, ContentEncoding: m.Metadata.ContentEncoding}
		custom = m.Metadata.Custom
	}
	if !committedAs(committed, staged, ids) {
		if err := blob.CommitBlockList(ctx, ids, headers, custom); err != nil {
			return err
		}
	}
	// Publishing replaces whatever the key holds in one commit, and no block
	// of another upload of the key is touched.
	if err := a.publish(ctx, src, key, staging, uploadID, total, headers, custom); err != nil {
		return err
	}
	// The object is complete. What cannot be deleted now is found again by the
	// expiry cleanup, because the manifest goes last.
	_ = a.deleteUpload(ctx, uploadID) //nolint:errcheck // the object is complete; see above
	return nil
}

// deleteUpload removes the records and the manifest of an upload, the
// manifest last so that a failure leaves the upload findable.
// committedAs reports whether the committed blocks of the staging blob are
// exactly ids and no block was staged after the commit that the list would take
// instead.
func committedAs(committed []blockSize, staged map[string]int64, ids []string) bool {
	if len(committed) != len(ids) {
		return false
	}
	for i, id := range ids {
		if committed[i].ID != id {
			return false
		}
		if _, ok := staged[id]; ok {
			return false
		}
	}
	return true
}

// publish makes the target hold the total bytes of the committed staging
// blob. The service stages the blocks of the target from ranges of the
// staging blob, then one block list commits them, so the target keeps its
// old content until that commit and a failure before it leaves nothing
// visible. The block IDs belong to the upload, so a repeat overwrites its own
// blocks and no block of another upload of the key.
func (a *Azure) publish(ctx context.Context, src sourceAPI, key, staging, uploadID string, total int64, headers azblob.BlobHTTPHeaders, custom map[string]string) error {
	dst, err := a.blocks(key)
	if err != nil {
		return err
	}
	source, err := a.sourceURL(src, staging)
	if err != nil {
		return err
	}
	// A commit of the target discards every uncommitted block it does not
	// name, the blocks another upload of the key has staged included, so a
	// commit that finds its blocks gone stages them again.
	for attempt := 1; ; attempt++ {
		var ids []string
		for offset := int64(0); offset < total; offset += publishBlockSize {
			count := min(publishBlockSize, total-offset)
			id := blockID(uploadID, len(ids))
			if err := dst.StageBlockFromURL(ctx, id, source, offset, count); err != nil {
				return err
			}
			ids = append(ids, id)
		}
		err := dst.CommitBlockList(ctx, ids, headers, custom)
		if err == nil || attempt == publishAttempts || !hasServiceCode(err, azblob.ServiceCodeInvalidBlockList) {
			return err
		}
	}
}

func (a *Azure) deleteUpload(ctx context.Context, uploadID string) error {
	dir := uploadDir(uploadID)
	// A blob of uncommitted blocks is not listed and cannot be deleted, so
	// commit it empty to make it a blob that can.
	if blob, err := a.blocks(stagingBlob(uploadID)); err == nil {
		staged, err := blob.UncommittedBlocks(ctx)
		if err != nil {
			return err
		}
		if len(staged) > 0 {
			if err := blob.CommitBlockList(ctx, nil, azblob.BlobHTTPHeaders{}, nil); err != nil {
				return err
			}
		}
	}
	names, err := a.container.ListBlobsFlat(ctx, dir)
	if err != nil {
		return err
	}
	for _, name := range names {
		if name == dir+manifestName {
			continue
		}
		if err := a.deleteBlob(ctx, name); err != nil {
			return err
		}
	}
	return a.deleteBlob(ctx, dir+manifestName)
}

func (a *Azure) deleteBlob(ctx context.Context, name string) error {
	err := a.container.NewBlockBlob(name).Delete(ctx)
	if err != nil && !errors.Is(mapNotFound(err, name), common.ErrKeyNotFound) {
		return err
	}
	return nil
}

// AbortMultipartUpload discards an upload. The staged blocks themselves are
// discarded by the service a week after they were staged.
func (a *Azure) AbortMultipartUpload(ctx context.Context, key, uploadID string) error {
	if err := common.ValidateKey(key); err != nil {
		return err
	}
	if err := a.requireContainer(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	m, err := a.readManifest(ctx, uploadID)
	if err != nil {
		return err
	}
	if m.Key != key {
		return notFound(uploadID)
	}
	return a.deleteUpload(ctx, uploadID)
}

// ListMultipartUploads returns the uploads in progress whose key starts with
// prefix, expired ones included so that the cleanup can find them.
func (a *Azure) ListMultipartUploads(ctx context.Context, prefix string) ([]common.MultipartUploadInfo, error) {
	if prefix != "" {
		if err := common.ValidateKey(prefix); err != nil {
			return nil, err
		}
	}
	if err := a.requireContainer(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	names, err := a.container.ListBlobsFlat(ctx, common.MultipartPrefix)
	if err != nil {
		return nil, err
	}
	out := make([]common.MultipartUploadInfo, 0)
	for _, name := range names {
		if !strings.HasSuffix(name, "/"+manifestName) {
			continue
		}
		id := strings.TrimSuffix(strings.TrimPrefix(name, common.MultipartPrefix), "/"+manifestName)
		if !multipart.ValidUploadID(id) {
			continue
		}
		m, err := a.readManifestBlob(ctx, name, id)
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
