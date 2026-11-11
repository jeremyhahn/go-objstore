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

package local

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/jeremyhahn/go-objstore/internal/multipart"
	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// Compile-time check that Local satisfies the multipart capability.
var _ common.MultipartUploader = (*Local)(nil)

// uploadFile is the record of one upload inside its staging directory.
const uploadFile = "upload.json"

// stagingDir is the directory under the storage path that holds the state of
// uploads in progress. The key prefix that names it is reserved, so no object
// can collide with it.
var stagingDir = strings.TrimSuffix(common.MultipartPrefix, "/")

// uploadRecord is the content of upload.json.
// errNoEncrypter is returned when staged parts are encrypted and the store
// has lost its encrypter, so they cannot be read back.
var errNoEncrypter = errors.New("local: staged parts are encrypted but no encrypter is configured")

type uploadRecord struct {
	Key       string           `json:"key"`
	Metadata  *common.Metadata `json:"metadata,omitempty"`
	Initiated time.Time        `json:"initiated"`
}

// partRecord is the content of a part's record file. File names the file that
// holds the part's bytes; Size is the size of the plaintext part.
type partRecord struct {
	ETag      string `json:"etag"`
	Size      int64  `json:"size"`
	Encrypted bool   `json:"encrypted"`
	File      string `json:"file"`
}

func (l *Local) clock() time.Time {
	if l.now != nil {
		return l.now()
	}
	return time.Now()
}

func (l *Local) expiry() time.Duration {
	if l.multipartExpiry > 0 {
		return l.multipartExpiry
	}
	return multipart.DefaultExpiry
}

func (l *Local) stagingPath() string {
	return stagingDir
}

// encrypter returns the at-rest encrypter, or nil when none is configured.
func (l *Local) encrypter() (common.Encrypter, error) {
	if l.atRestEncrypterFactory == nil {
		return nil, nil
	}
	enc, err := l.atRestEncrypterFactory.GetEncrypter("")
	if err != nil {
		return nil, fmt.Errorf("failed to get encrypter: %w", err)
	}
	return enc, nil
}

func notFound(uploadID string) error {
	return fmt.Errorf("%w: %s", common.ErrUploadNotFound, uploadID)
}

// readUpload loads the record of an upload. The ID is checked before it is
// used as a path. A missing or unreadable record means no such upload. The
// returned directory is relative to the store's root.
func (l *Local) readUpload(uploadID string) (*uploadRecord, string, error) {
	if !multipart.ValidUploadID(uploadID) {
		return nil, "", notFound(uploadID)
	}
	root, err := l.fsRoot()
	if err != nil {
		return nil, "", err
	}
	dir := filepath.Join(l.stagingPath(), uploadID)
	raw, err := root.ReadFile(filepath.Join(dir, uploadFile))
	if err != nil {
		return nil, "", notFound(uploadID)
	}
	var rec uploadRecord
	if err := json.Unmarshal(raw, &rec); err != nil {
		return nil, "", notFound(uploadID)
	}
	return &rec, dir, nil
}

// liveUpload returns the upload for the ID and key. An upload past its expiry
// is treated as aborted: it is removed and reported as not found.
func (l *Local) liveUpload(key, uploadID string) (*uploadRecord, string, error) {
	rec, dir, err := l.readUpload(uploadID)
	if err != nil {
		return nil, "", err
	}
	if rec.Key != key {
		return nil, "", notFound(uploadID)
	}
	if multipart.Expired(rec.Initiated, l.expiry(), l.clock()) {
		_ = l.removeAll(dir)
		return nil, "", fmt.Errorf("%w: %s (expired)", common.ErrUploadNotFound, uploadID)
	}
	return rec, dir, nil
}

// removeAll removes the root-relative name and everything below it.
func (l *Local) removeAll(name string) error {
	root, err := l.fsRoot()
	if err != nil {
		return err
	}
	return root.RemoveAll(name)
}

func partRecordPath(dir string, n int) string {
	return filepath.Join(dir, fmt.Sprintf("p%05d.json", n))
}

func loadPart(root *os.Root, dir string, n int) (partRecord, bool) {
	raw, err := root.ReadFile(partRecordPath(dir, n))
	if err != nil {
		return partRecord{}, false
	}
	var pr partRecord
	if err := json.Unmarshal(raw, &pr); err != nil || pr.File == "" || filepath.Base(pr.File) != pr.File {
		return partRecord{}, false
	}
	return pr, true
}

func writeJSONAtomic(root *os.Root, name string, v any) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return writeFileAtomic(root, name, 0600, func(w io.Writer) error {
		_, werr := w.Write(raw)
		return werr
	})
}

// InitiateMultipartUpload starts a multipart upload for the key. The upload
// directory appears whole, with its record inside, or not at all.
func (l *Local) InitiateMultipartUpload(ctx context.Context, key string, metadata *common.Metadata) (string, error) {
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

	root, err := l.fsRoot()
	if err != nil {
		return "", err
	}
	stage := l.stagingPath()
	release := holdDirs(&l.pruneMu)
	err = root.MkdirAll(stage, 0750)
	var tmp string
	if err == nil {
		tmp, err = mkdirTemp(root, stage)
	}
	release()
	if err != nil {
		return "", err
	}
	rec := uploadRecord{Key: key, Metadata: multipart.CloneMetadata(metadata), Initiated: l.clock()}
	if err := writeJSONAtomic(root, filepath.Join(tmp, uploadFile), rec); err != nil {
		_ = root.RemoveAll(tmp)
		return "", err
	}
	if err := root.Rename(tmp, filepath.Join(stage, id)); err != nil {
		_ = root.RemoveAll(tmp)
		return "", err
	}
	return id, nil
}

// UploadPart stages one part of an upload. The part is encrypted at rest when
// an encrypter is configured, like any other object.
func (l *Local) UploadPart(ctx context.Context, key, uploadID string, partNumber int, data io.Reader, size int64) (common.PartInfo, error) {
	if err := l.validateKey(key); err != nil {
		return common.PartInfo{}, err
	}
	if err := multipart.ValidatePartNumber(partNumber); err != nil {
		return common.PartInfo{}, err
	}
	if err := ctx.Err(); err != nil {
		return common.PartInfo{}, err
	}
	_, dir, err := l.liveUpload(key, uploadID)
	if err != nil {
		return common.PartInfo{}, err
	}
	root, err := l.fsRoot()
	if err != nil {
		return common.PartInfo{}, err
	}
	enc, err := l.encrypter()
	if err != nil {
		return common.PartInfo{}, err
	}

	token, err := multipart.NewUploadID()
	if err != nil {
		return common.PartInfo{}, err
	}
	file := fmt.Sprintf("p%05d-%s", partNumber, token)
	var etag string
	err = writeFileAtomic(root, filepath.Join(dir, file), 0600, func(w io.Writer) error {
		var cerr error
		if enc == nil {
			etag, cerr = multipart.CopyPart(w, data, size)
			return cerr
		}
		etag, cerr = copyEncryptedPart(ctx, enc, w, data, size)
		return cerr
	})
	if err != nil {
		return common.PartInfo{}, err
	}

	previous, hadPrevious := loadPart(root, dir, partNumber)
	rec := partRecord{ETag: etag, Size: size, Encrypted: enc != nil, File: file}
	if err := ctx.Err(); err != nil {
		_ = root.Remove(filepath.Join(dir, file))
		return common.PartInfo{}, err
	}
	if err := writeJSONAtomic(root, partRecordPath(dir, partNumber), rec); err != nil {
		_ = root.Remove(filepath.Join(dir, file))
		return common.PartInfo{}, err
	}
	if hadPrevious && previous.File != file {
		_ = root.Remove(filepath.Join(dir, previous.File))
	}
	return common.PartInfo{PartNumber: partNumber, ETag: etag, Size: size}, nil
}

// copyEncryptedPart writes the encrypted part to w. The plaintext goes straight
// from the caller's reader into the cipher: it is hashed and checked against
// its declared size on the way through and never lands on disk, so a crash
// leaves nothing but ciphertext behind.
func copyEncryptedPart(ctx context.Context, enc common.Encrypter, w io.Writer, data io.Reader, size int64) (string, error) {
	body, err := multipart.NewPartReader(data, size)
	if err != nil {
		return "", err
	}
	sealed, err := enc.Encrypt(ctx, body)
	if err != nil {
		return "", fmt.Errorf("encryption failed: %w", err)
	}
	defer func() { _ = sealed.Close() }()
	if _, err := io.Copy(w, sealed); err != nil {
		return "", err
	}
	return body.Finish()
}

// partStream reads the plaintext of the staged parts in order, decrypting
// each one that was stored encrypted.
type partStream struct {
	ctx     context.Context
	root    *os.Root
	dir     string
	parts   []partRecord
	enc     common.Encrypter
	next    int
	cur     io.Reader
	closers []io.Closer
}

func (s *partStream) open(pr partRecord) error {
	f, err := s.root.Open(filepath.Join(s.dir, pr.File))
	if err != nil {
		return err
	}
	s.closers = append(s.closers, f)
	s.cur = f
	if pr.Encrypted {
		rc, err := s.enc.Decrypt(s.ctx, f)
		if err != nil {
			return fmt.Errorf("decryption failed: %w", err)
		}
		s.closers = append(s.closers, rc)
		s.cur = rc
	}
	return nil
}

func (s *partStream) closeCurrent() {
	for i := len(s.closers) - 1; i >= 0; i-- {
		_ = s.closers[i].Close()
	}
	s.closers = nil
	s.cur = nil
}

func (s *partStream) Read(p []byte) (int, error) {
	for {
		if s.cur == nil {
			if s.next >= len(s.parts) {
				return 0, io.EOF
			}
			pr := s.parts[s.next]
			s.next++
			if err := s.open(pr); err != nil {
				return 0, err
			}
		}
		n, err := s.cur.Read(p)
		if errors.Is(err, io.EOF) {
			s.closeCurrent()
			err = nil
			if n == 0 {
				continue
			}
		}
		return n, err
	}
}

// CompleteMultipartUpload assembles the parts into the object. The object
// appears whole or not at all, and the staged parts are dropped on success.
func (l *Local) CompleteMultipartUpload(ctx context.Context, key, uploadID string, parts []common.CompletedPart) error {
	if err := l.validateKey(key); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	rec, dir, err := l.liveUpload(key, uploadID)
	if err != nil {
		return err
	}
	root, err := l.fsRoot()
	if err != nil {
		return err
	}

	records := make(map[int]partRecord, len(parts))
	numbers, err := multipart.CheckComplete(parts, func(n int) (common.PartInfo, bool) {
		pr, ok := loadPart(root, dir, n)
		if !ok {
			return common.PartInfo{}, false
		}
		records[n] = pr
		return common.PartInfo{PartNumber: n, ETag: pr.ETag, Size: pr.Size}, true
	})
	if err != nil {
		return err
	}

	enc, err := l.encrypter()
	if err != nil {
		return err
	}
	ordered := make([]partRecord, 0, len(numbers))
	for _, n := range numbers {
		if records[n].Encrypted && enc == nil {
			return errNoEncrypter
		}
		ordered = append(ordered, records[n])
	}

	stream := &partStream{ctx: ctx, root: root, dir: dir, parts: ordered, enc: enc}
	defer stream.closeCurrent()
	var in io.Reader = stream
	if enc != nil {
		sealed, err := enc.Encrypt(ctx, stream)
		if err != nil {
			return fmt.Errorf("encryption failed: %w", err)
		}
		defer func() { _ = sealed.Close() }()
		in = sealed
	}

	name := rootName(key)
	var size int64
	if err := writeObjectAtomic(root, &l.pruneMu, name, 0644, func(w io.Writer) error {
		n, werr := io.Copy(w, in)
		size = n
		return werr
	}); err != nil {
		return err
	}

	md := multipart.CloneMetadata(rec.Metadata)
	md.Size = size
	md.LastModified = time.Now()
	if info, err := root.Stat(name); err == nil {
		md.ETag = fmt.Sprintf("%d-%d", info.ModTime().Unix(), size)
	}
	if enc != nil {
		if md.Custom == nil {
			md.Custom = make(map[string]string)
		}
		md.Custom["at_rest_encryption_algorithm"] = enc.Algorithm()
		md.Custom["at_rest_encryption_key_id"] = enc.KeyID()
	}
	if err := l.saveMetadata(key, md); err != nil {
		return err
	}

	// A directory that cannot be removed is dropped by the expiry cleanup.
	_ = root.RemoveAll(dir)

	if l.changeLog != nil {
		_ = l.changeLog.RecordChange(ChangeEvent{
			Key:       key,
			Operation: "put",
			Timestamp: time.Now(),
			ETag:      md.ETag,
			Size:      md.Size,
		})
	}
	return nil
}

// AbortMultipartUpload discards an upload and its staged parts. An upload
// past its expiry is still removed, so the cleanup that lists and aborts
// expired uploads succeeds.
func (l *Local) AbortMultipartUpload(ctx context.Context, key, uploadID string) error {
	if err := l.validateKey(key); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	rec, dir, err := l.readUpload(uploadID)
	if err != nil {
		return err
	}
	if rec.Key != key {
		return notFound(uploadID)
	}
	return l.removeAll(dir)
}

// ListMultipartUploads returns the uploads in progress whose key starts with
// the prefix, including those past their expiry that no call has dropped yet,
// sorted by key and then by initiation time.
func (l *Local) ListMultipartUploads(ctx context.Context, prefix string) ([]common.MultipartUploadInfo, error) {
	if prefix != "" {
		if err := common.ValidateKey(prefix); err != nil {
			return nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	root, err := l.fsRoot()
	if errors.Is(err, fs.ErrNotExist) {
		return []common.MultipartUploadInfo{}, nil
	}
	if err != nil {
		return nil, err
	}
	entries, err := fs.ReadDir(root.FS(), filepath.ToSlash(l.stagingPath()))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return []common.MultipartUploadInfo{}, nil
		}
		return nil, err
	}
	out := make([]common.MultipartUploadInfo, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() || !multipart.ValidUploadID(e.Name()) {
			continue
		}
		rec, _, err := l.readUpload(e.Name())
		if err != nil || !strings.HasPrefix(rec.Key, prefix) {
			continue
		}
		out = append(out, common.MultipartUploadInfo{Key: rec.Key, UploadID: e.Name(), Initiated: rec.Initiated})
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

// expireUploads removes the uploads past their expiry and returns how many it
// removed. A directory whose record is missing or unreadable is judged by its
// modification time, so a crash during initiate leaves nothing behind for
// good. Entries that are not upload directories are left alone.
func (l *Local) expireUploads() int {
	if l.path == "" {
		return 0
	}
	root, err := l.fsRoot()
	if err != nil {
		return 0
	}
	entries, err := fs.ReadDir(root.FS(), filepath.ToSlash(l.stagingPath()))
	if err != nil {
		return 0
	}
	now := l.clock()
	expiry := l.expiry()
	removed := 0
	for _, e := range entries {
		name := e.Name()
		if !e.IsDir() || (!multipart.ValidUploadID(name) && !strings.HasPrefix(name, tmpFilePrefix)) {
			continue
		}
		dir := filepath.Join(l.stagingPath(), name)
		initiated := time.Time{}
		if rec, _, err := l.readUpload(name); err == nil {
			initiated = rec.Initiated
		} else if info, err := e.Info(); err == nil {
			initiated = info.ModTime()
		} else {
			continue
		}
		if multipart.Expired(initiated, expiry, now) {
			if root.RemoveAll(dir) == nil {
				removed++
			}
		}
	}
	return removed
}
