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
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

type mdCaptureArchiver struct {
	mockArchiver
	md *common.Metadata
}

func (m *mdCaptureArchiver) PutWithMetadata(_ context.Context, _ string, r io.Reader, md *common.Metadata) error {
	_, err := io.ReadAll(r)
	m.md = md
	return err
}

// encryptedS3 serves one object whose HEAD carries client-side encryption
// metadata, with the key canonicalised the way S3 returns it.
func encryptedS3() *S3 {
	return &S3{bucket: "b", svc: &mockS3Client{
		getObjectOutput: &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader([]byte("ct")))},
		headObjectOutput: &s3.HeadObjectOutput{Metadata: map[string]*string{
			"Encryption_key_id":    aws.String("dek-1"),
			"Encryption_algorithm": aws.String("AES-256-GCM"),
		}},
	}}
}

func TestS3_Archive_KeepsEncryptionMetadata(t *testing.T) {
	dst := &mdCaptureArchiver{}
	if err := encryptedS3().Archive("key", dst); err != nil {
		t.Fatalf("Archive: %v", err)
	}
	if dst.md == nil || dst.md.Custom["encryption_key_id"] != "dek-1" {
		t.Fatalf("archived metadata = %+v, want encryption key id carried", dst.md)
	}
}

func TestS3_Archive_EncryptedRefusesMetadataLessDestination(t *testing.T) {
	if err := encryptedS3().Archive("key", &mockArchiver{}); !errors.Is(err, common.ErrArchiveMetadataUnsupported) {
		t.Fatalf("Archive err = %v, want ErrArchiveMetadataUnsupported", err)
	}
}

func TestS3_Archive_MetadataError(t *testing.T) {
	s := &S3{bucket: "b", svc: &mockS3Client{
		getObjectOutput: &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader([]byte("x")))},
		headObjectError: errors.New("head failed"),
	}}
	if err := s.Archive("key", &mockArchiver{}); err == nil {
		t.Fatal("Archive with failing HEAD: want error")
	}
}
