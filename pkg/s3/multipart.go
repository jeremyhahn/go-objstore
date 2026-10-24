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

//go:build awss3

package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/jeremyhahn/go-objstore/internal/multipart"
	"github.com/jeremyhahn/go-objstore/pkg/common"

	"github.com/aws/aws-sdk-go/aws"         //nolint:staticcheck // Using v1 SDK, migration to v2 planned
	"github.com/aws/aws-sdk-go/aws/request" //nolint:staticcheck // Using v1 SDK, migration to v2 planned
	"github.com/aws/aws-sdk-go/service/s3"  //nolint:staticcheck // Using v1 SDK, migration to v2 planned
)

// errPartSize is returned when a part's reader does not yield the declared
// number of bytes.
var errPartSize = fmt.Errorf("%w: part is not the declared size", common.ErrInvalidArgument)

// sizedReader yields exactly size bytes of r and fails if r holds fewer or
// more, so a part is never sent with a length that differs from its body. It
// remembers why it failed, because the SDK wraps the errors of a body.
type sizedReader struct {
	r        io.Reader
	size     int64
	n        int64
	err      error // the failure of r itself
	mismatch bool  // r held fewer or more than size bytes
}

func (s *sizedReader) Read(p []byte) (int, error) {
	remaining := s.size - s.n
	if remaining == 0 {
		var extra [1]byte
		n, err := io.ReadFull(s.r, extra[:])
		switch {
		case n > 0:
			s.mismatch = true
			return 0, errPartSize
		case err == io.EOF:
			return 0, io.EOF
		default:
			s.err = err
			return 0, err
		}
	}
	if int64(len(p)) > remaining {
		p = p[:remaining]
	}
	n, err := s.r.Read(p)
	s.n += int64(n)
	switch {
	case err == nil:
		return n, nil
	case err == io.EOF && s.n < s.size:
		s.mismatch = true
		return n, errPartSize
	case err == io.EOF:
		return n, nil
	default:
		s.err = err
		return n, err
	}
}

// sizedSeeker is a sizedReader over an io.ReadSeeker that can be rewound, so
// the v4 signer can hash the part and the SDK can resend it without the part
// ever being copied. It exposes the size bytes that follow start, the offset of
// rs when the upload began; the caller's reader is not read past them.
type sizedSeeker struct {
	sizedReader
	rs    io.ReadSeeker
	start int64
}

// Read fails for good once the part has failed. The v4 signer ignores a read
// error while it hashes the body and then sends the body anyway, so the
// failure has to outlive the rewind that follows the hashing pass: a part that
// is not the declared size must not reach the service as a complete request.
func (s *sizedSeeker) Read(p []byte) (int, error) {
	switch {
	case s.err != nil:
		return 0, s.err
	case s.mismatch:
		return 0, errPartSize
	}
	return s.sizedReader.Read(p)
}

// Seek positions the part, not the underlying reader: offsets run from 0 to
// size and are mapped onto rs.
func (s *sizedSeeker) Seek(offset int64, whence int) (int64, error) {
	var target int64
	switch whence {
	case io.SeekStart:
		target = offset
	case io.SeekCurrent:
		target = s.n + offset
	case io.SeekEnd:
		target = s.size + offset
	default:
		return 0, fmt.Errorf("%w: seek whence %d", common.ErrInvalidArgument, whence)
	}
	if target < 0 || target > s.size {
		return 0, fmt.Errorf("%w: seek to %d outside the %d byte part", common.ErrInvalidArgument, target, s.size)
	}
	if _, err := s.rs.Seek(s.start+target, io.SeekStart); err != nil {
		s.err = err
		return 0, err
	}
	s.n = target
	return target, nil
}

// errUnsignedPlain is returned for a part whose body cannot be rewound when the
// endpoint is not https.
var errUnsignedPlain = fmt.Errorf("%w: a part body that cannot be seeked is sent with an unsigned payload, which needs an https endpoint", common.ErrInvalidArgument)

// unsignedPayload sends the request with X-Amz-Content-Sha256 set to
// UNSIGNED-PAYLOAD. The v4 signer otherwise hashes the body, which means
// reading it twice, and a streamed part body cannot be read twice without
// buffering the part in memory or spooling its plaintext to disk. The header
// is still covered by the request signature; what is given up is the payload
// hash, so integrity in transit rests on TLS alone. Over plain http nothing
// would vouch for the payload, so the request is refused there.
func unsignedPayload(r *request.Request) {
	r.Handlers.Build.PushBack(func(r *request.Request) {
		if r.HTTPRequest.URL.Scheme != "https" {
			r.Error = errUnsignedPlain
			return
		}
		r.HTTPRequest.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
	})
}

// noContentMD5 stops the SDK from adding a Content-MD5 header, which it does by
// hashing a seekable body with MD5. The part is already covered by the
// signature's payload hash (or, for a streamed part, by TLS), and MD5 is not
// available under FIPS 140-only.
func noContentMD5(r *request.Request) {
	r.Config.S3DisableContentMD5Validation = aws.Bool(true)
}

// partBody returns the reader to send for a part, the sizedReader that
// reports how it failed, and the request options the body needs. A seekable
// caller reader is sent as a rewindable view of the part; any other reader is
// streamed with an unsigned payload.
func partBody(data io.Reader, size int64) (io.Reader, *sizedReader, []request.Option, error) {
	if rs, ok := data.(io.ReadSeeker); ok {
		if start, end, ok := seekExtent(rs); ok {
			// The signer only reads as many bytes as the request declares, so a
			// reader of another length is caught here, before anything is sent.
			if end-start != size {
				return nil, nil, nil, errPartSize
			}
			body := &sizedSeeker{sizedReader: sizedReader{r: rs, size: size}, rs: rs, start: start}
			return body, &body.sizedReader, []request.Option{noContentMD5}, nil
		}
	}
	body := &sizedReader{r: data, size: size}
	return body, body, []request.Option{noContentMD5, unsignedPayload}, nil
}

// seekExtent reports the current offset of rs and the offset of its end, and
// leaves rs where it was. It is false for a reader that implements io.Seeker
// but cannot seek (os.Stdin, a pipe).
func seekExtent(rs io.ReadSeeker) (start, end int64, ok bool) {
	start, err := rs.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, 0, false
	}
	end, err = rs.Seek(0, io.SeekEnd)
	if _, backErr := rs.Seek(start, io.SeekStart); err != nil || backErr != nil {
		return 0, 0, false
	}
	return start, end, true
}

// uploadError maps the service errors of a call on an upload.
func uploadError(err error, uploadID string) error {
	switch awsCode(err) {
	case "":
		return err
	case codeNoSuchUpload:
		return fmt.Errorf("%w: %s", common.ErrUploadNotFound, uploadID)
	case codeInvalidPart:
		return fmt.Errorf("%w: %v", common.ErrPartMismatch, err)
	case codeInvalidPartOrder:
		return fmt.Errorf("%w: %v", common.ErrInvalidPartOrder, err)
	case codeEntityTooSmall:
		return fmt.Errorf("%w: %v", common.ErrPartTooSmall, err)
	case codeEntityTooLarge:
		return fmt.Errorf("%w: %v", common.ErrPartTooLarge, err)
	}
	return err
}

func (s *S3) expiry() time.Duration {
	if s.multipartExpiry > 0 {
		return s.multipartExpiry
	}
	return multipart.DefaultExpiry
}

// InitiateMultipartUpload starts an upload on the service. The metadata is
// applied as PutWithMetadata applies it.
func (s *S3) InitiateMultipartUpload(ctx context.Context, key string, metadata *common.Metadata) (string, error) {
	if err := multipart.ValidateInitiate(key, metadata); err != nil {
		return "", err
	}
	if err := s.requireBucket(); err != nil {
		return "", err
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	input := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}
	if metadata != nil {
		if metadata.ContentType != "" {
			input.ContentType = aws.String(metadata.ContentType)
		}
		if metadata.ContentEncoding != "" {
			input.ContentEncoding = aws.String(metadata.ContentEncoding)
		}
		if len(metadata.Custom) > 0 {
			input.Metadata = make(map[string]*string, len(metadata.Custom))
			for k, v := range metadata.Custom {
				input.Metadata[k] = aws.String(v)
			}
		}
	}
	out, err := s.svc.CreateMultipartUploadWithContext(ctx, input)
	if err != nil {
		return "", bucketError(err, s.bucket)
	}
	return aws.StringValue(out.UploadId), nil
}

// UploadPart stores one part. The body is wrapped with aws.ReadSeekCloser, as
// Put wraps its body, and sent with its declared length.
func (s *S3) UploadPart(ctx context.Context, key, uploadID string, partNumber int, data io.Reader, size int64) (common.PartInfo, error) {
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
	if uploadID == "" {
		return common.PartInfo{}, fmt.Errorf("%w: empty upload ID", common.ErrUploadNotFound)
	}
	if err := s.requireBucket(); err != nil {
		return common.PartInfo{}, err
	}
	if err := ctx.Err(); err != nil {
		return common.PartInfo{}, err
	}

	body, sized, opts, err := partBody(data, size)
	if err != nil {
		return common.PartInfo{}, err
	}
	out, err := s.svc.UploadPartWithContext(ctx, &s3.UploadPartInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(key),
		UploadId:      aws.String(uploadID),
		PartNumber:    aws.Int64(int64(partNumber)),
		ContentLength: aws.Int64(size),
		Body:          aws.ReadSeekCloser(body),
	}, opts...)
	switch {
	case sized.err != nil:
		return common.PartInfo{}, sized.err
	case sized.mismatch:
		return common.PartInfo{}, errPartSize
	case errors.Is(err, errUnsignedPlain):
		return common.PartInfo{}, errUnsignedPlain
	case err != nil:
		return common.PartInfo{}, uploadError(err, uploadID)
	}
	return common.PartInfo{PartNumber: partNumber, ETag: aws.StringValue(out.ETag), Size: size}, nil
}

// CompleteMultipartUpload assembles the listed parts. The order and the
// numbers of the parts are checked here; their entity tags and sizes are
// checked by the service, which refuses the whole request on a mismatch.
//
// An upload older than the multipartExpiry setting is treated as aborted.
func (s *S3) CompleteMultipartUpload(ctx context.Context, key, uploadID string, parts []common.CompletedPart) error {
	if err := common.ValidateKey(key); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	etags := make(map[int]string, len(parts))
	for _, p := range parts {
		etags[p.PartNumber] = p.ETag
	}
	// The sizes are the service's to check, so each part is given the minimum.
	_, err := multipart.CheckComplete(parts, func(n int) (common.PartInfo, bool) {
		return common.PartInfo{PartNumber: n, ETag: etags[n], Size: common.MinPartSize}, true
	})
	if err != nil {
		return err
	}
	if uploadID == "" {
		return fmt.Errorf("%w: empty upload ID", common.ErrUploadNotFound)
	}
	if err := s.requireBucket(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.rejectExpired(ctx, key, uploadID); err != nil {
		return err
	}

	completed := make([]*s3.CompletedPart, 0, len(parts))
	for _, p := range parts {
		completed = append(completed, &s3.CompletedPart{
			ETag:       aws.String(p.ETag),
			PartNumber: aws.Int64(int64(p.PartNumber)),
		})
	}
	_, err = s.svc.CompleteMultipartUploadWithContext(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(s.bucket),
		Key:             aws.String(key),
		UploadId:        aws.String(uploadID),
		MultipartUpload: &s3.CompletedMultipartUpload{Parts: completed},
	})
	return uploadError(err, uploadID)
}

// rejectExpired returns ErrUploadNotFound for an upload that outlived the
// multipartExpiry setting. It finds the upload by listing, once per
// completion, which UploadPart is spared. An upload the listing does not show
// is left to the service to judge.
func (s *S3) rejectExpired(ctx context.Context, key, uploadID string) error {
	uploads, err := s.ListMultipartUploads(ctx, key)
	if err != nil {
		return err
	}
	for _, u := range uploads {
		if u.Key == key && u.UploadID == uploadID && multipart.Expired(u.Initiated, s.expiry(), time.Now()) {
			return fmt.Errorf("%w: %s expired", common.ErrUploadNotFound, uploadID)
		}
	}
	return nil
}

// AbortMultipartUpload discards an upload and its parts.
func (s *S3) AbortMultipartUpload(ctx context.Context, key, uploadID string) error {
	if err := common.ValidateKey(key); err != nil {
		return err
	}
	if uploadID == "" {
		return fmt.Errorf("%w: empty upload ID", common.ErrUploadNotFound)
	}
	if err := s.requireBucket(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err := s.svc.AbortMultipartUploadWithContext(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(s.bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
	})
	return uploadError(err, uploadID)
}

// ListMultipartUploads returns the uploads in progress whose key starts with
// prefix, sorted by key and then by initiation time.
func (s *S3) ListMultipartUploads(ctx context.Context, prefix string) ([]common.MultipartUploadInfo, error) {
	if prefix != "" {
		if err := common.ValidateKey(prefix); err != nil {
			return nil, err
		}
	}
	if err := s.requireBucket(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	uploads := []common.MultipartUploadInfo{}
	var keyMarker, idMarker *string
	for {
		out, err := s.svc.ListMultipartUploadsWithContext(ctx, &s3.ListMultipartUploadsInput{
			Bucket:         aws.String(s.bucket),
			Prefix:         aws.String(prefix),
			KeyMarker:      keyMarker,
			UploadIdMarker: idMarker,
		})
		if err != nil {
			return nil, bucketError(err, s.bucket)
		}
		for _, u := range out.Uploads {
			uploads = append(uploads, common.MultipartUploadInfo{
				Key:       aws.StringValue(u.Key),
				UploadID:  aws.StringValue(u.UploadId),
				Initiated: aws.TimeValue(u.Initiated),
			})
		}
		if !aws.BoolValue(out.IsTruncated) {
			break
		}
		keyMarker, idMarker = out.NextKeyMarker, out.NextUploadIdMarker
	}
	sort.SliceStable(uploads, func(i, j int) bool {
		if uploads[i].Key != uploads[j].Key {
			return uploads[i].Key < uploads[j].Key
		}
		return uploads[i].Initiated.Before(uploads[j].Initiated)
	})
	return uploads, nil
}
