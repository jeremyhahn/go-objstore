//go:build awss3

package s3

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/jeremyhahn/go-objstore/pkg/common"

	"github.com/aws/aws-sdk-go/service/s3" //nolint:staticcheck // Using v1 SDK, migration to v2 planned
)

// A failed auto-multipart put whose abort also fails reports both errors,
// the abort error mapped to the package's typed error.
func TestS3_Put_AutoMultipart_AbortFails(t *testing.T) {
	data := patterned(2*common.MinPartSize + 1)
	boom := errors.New("boom")
	r := newClassRecorder()
	r.errs["UploadPart"] = boom
	r.errs["AbortMultipartUpload"] = awsErr(s3.ErrCodeNoSuchUpload)
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
