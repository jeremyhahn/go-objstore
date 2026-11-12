//go:build awss3

package s3

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/jeremyhahn/go-objstore/pkg/common"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// A failed auto-multipart put whose abort also fails reports both errors,
// the abort error mapped to the package's typed error.
func TestS3_Put_AutoMultipart_AbortFails(t *testing.T) {
	data := patterned(2*common.MinPartSize + 1)
	boom := errors.New("boom")
	r := newClassRecorder()
	r.errs["UploadPart"] = boom
	r.errs["AbortMultipartUpload"] = awsErr(codeNoSuchUpload)
	s := &S3{svc: r, bucket: "bucket-one", multipartThreshold: 1, partSize: common.MinPartSize}
	err := s.PutWithMetadata(context.Background(), "big", bytes.NewReader(data), nil)
	if !errors.Is(err, boom) {
		t.Errorf("err = %v, want the part error", err)
	}
	if !errors.Is(err, common.ErrUploadNotFound) {
		t.Errorf("err = %v, want the abort error as ErrUploadNotFound", err)
	}
	if r.aborts != 1 {
		t.Errorf("aborts = %d, want 1", r.aborts)
	}
}

// restoreFailingSeeker seeks normally until it has been sent to its end, then
// fails to seek back: the reader is left at EOF.
type restoreFailingSeeker struct {
	*bytes.Reader
	err   error
	atEnd bool
}

func (r *restoreFailingSeeker) Seek(offset int64, whence int) (int64, error) {
	if r.atEnd && whence == io.SeekStart {
		return 0, r.err
	}
	n, err := r.Reader.Seek(offset, whence)
	if whence == io.SeekEnd {
		r.atEnd = true
	}
	return n, err
}

func TestS3_Put_SeekRestoreFails(t *testing.T) {
	boom := errors.New("cannot seek back")
	for _, threshold := range []int64{1, 1 << 30} {
		r := newClassRecorder()
		s := &S3{svc: r, bucket: "bucket-one", multipartThreshold: threshold, partSize: common.MinPartSize}
		body := &restoreFailingSeeker{Reader: bytes.NewReader([]byte("payload")), err: boom}
		err := s.PutWithMetadata(context.Background(), "k", body, nil)
		var restoreErr *SeekRestoreError
		if !errors.As(err, &restoreErr) || !errors.Is(err, boom) {
			t.Errorf("threshold %d: err = %v, want a *SeekRestoreError wrapping the seek error", threshold, err)
		}
		if r.puts != 0 || len(r.createClasses) != 0 {
			t.Errorf("threshold %d: puts = %d, creates = %d, want nothing sent", threshold, r.puts, len(r.createClasses))
		}
	}
}

func TestPartBody_SeekRestoreFails(t *testing.T) {
	boom := errors.New("cannot seek back")
	body := &restoreFailingSeeker{Reader: bytes.NewReader([]byte("part")), err: boom}
	if _, _, _, err := partBody(body, 4); !errors.Is(err, boom) {
		t.Errorf("err = %v, want the restore error", err)
	}
}

func TestSeekExtent_SeekEndFails(t *testing.T) {
	// SeekEnd fails but the reader can be put back: stream it.
	r := readSeeker{Reader: bytes.NewReader(nil), Seeker: &endFailingSeeker{}}
	if _, _, ok, err := seekExtent(r); ok || err != nil {
		t.Errorf("ok = %v, err = %v, want false, nil", ok, err)
	}
	// SeekEnd fails and so does the restore: report it.
	r = readSeeker{Reader: bytes.NewReader(nil), Seeker: &endFailingSeeker{failRestore: true}}
	if _, _, _, err := seekExtent(r); err == nil {
		t.Error("err = nil, want a restore error")
	}
}

type endFailingSeeker struct{ failRestore bool }

func (s *endFailingSeeker) Seek(_ int64, whence int) (int64, error) {
	switch {
	case whence == io.SeekEnd:
		return 0, errors.New("no end")
	case whence == io.SeekStart && s.failRestore:
		return 0, errors.New("no restore")
	}
	return 0, nil
}
