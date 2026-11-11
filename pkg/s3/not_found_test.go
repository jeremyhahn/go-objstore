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
	"net/http"
	"testing"

	"github.com/jeremyhahn/go-objstore/pkg/common"

	"github.com/aws/aws-sdk-go/aws/awserr" //nolint:staticcheck // Using v1 SDK, migration to v2 planned
	"github.com/aws/aws-sdk-go/service/s3" //nolint:staticcheck // Using v1 SDK, migration to v2 planned
)

// sdkFailure builds the error the v1 SDK returns for a failed request: a
// RequestFailure carrying the service code and the HTTP status.
func sdkFailure(code string, status int) error {
	return awserr.NewRequestFailure(awserr.New(code, code, nil), status, "req-id")
}

// assertKeyNotFound checks err wraps common.ErrKeyNotFound and still reaches
// the SDK error through errors.As.
func assertKeyNotFound(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, common.ErrKeyNotFound) {
		t.Fatalf("expected common.ErrKeyNotFound, got %v", err)
	}
	var ae awserr.Error
	if !errors.As(err, &ae) {
		t.Fatalf("expected the SDK error in the chain, got %v", err)
	}
}

func TestS3_NotFound_MapsToErrKeyNotFound(t *testing.T) {
	ctx := context.Background()
	missingGet := map[string]error{
		"NoSuchKey":       sdkFailure(s3.ErrCodeNoSuchKey, http.StatusNotFound),
		"NoSuchKey plain": awsErr(s3.ErrCodeNoSuchKey),
	}
	missingHead := map[string]error{
		"NotFound":    sdkFailure(codeNotFound, http.StatusNotFound),
		"no code 404": sdkFailure("", http.StatusNotFound),
	}

	for name, sdkErr := range missingGet {
		t.Run("Get/"+name, func(t *testing.T) {
			s := &S3{svc: &mockS3Client{getObjectError: sdkErr}, bucket: "b"}
			_, err := s.Get("missing")
			assertKeyNotFound(t, err)
		})
		t.Run("GetWithContext/"+name, func(t *testing.T) {
			s := &S3{svc: &mockS3Client{getObjectError: sdkErr}, bucket: "b"}
			_, err := s.GetWithContext(ctx, "missing")
			assertKeyNotFound(t, err)
		})
		t.Run("UpdateMetadata/"+name, func(t *testing.T) {
			s := &S3{svc: &mockS3Client{copyObjectError: sdkErr}, bucket: "b"}
			err := s.UpdateMetadata(ctx, "missing", &common.Metadata{ContentType: "text/plain"})
			assertKeyNotFound(t, err)
		})
	}
	for name, sdkErr := range missingHead {
		t.Run("GetMetadata/"+name, func(t *testing.T) {
			s := &S3{svc: &mockS3Client{headObjectError: sdkErr}, bucket: "b"}
			_, err := s.GetMetadata(ctx, "missing")
			assertKeyNotFound(t, err)
		})
		t.Run("UpdateMetadata/"+name, func(t *testing.T) {
			s := &S3{svc: &mockS3Client{copyObjectError: sdkErr}, bucket: "b"}
			err := s.UpdateMetadata(ctx, "missing", nil)
			assertKeyNotFound(t, err)
		})
		t.Run("Exists/"+name, func(t *testing.T) {
			s := &S3{svc: &mockS3Client{headObjectError: sdkErr}, bucket: "b"}
			exists, err := s.Exists(ctx, "missing")
			if err != nil || exists {
				t.Fatalf("expected false, nil; got %v, %v", exists, err)
			}
		})
	}
}

func TestS3_AccessDenied_IsNotKeyNotFound(t *testing.T) {
	ctx := context.Background()
	denied := sdkFailure("AccessDenied", http.StatusForbidden)
	mock := &mockS3Client{getObjectError: denied, headObjectError: denied, copyObjectError: denied}
	s := &S3{svc: mock, bucket: "b"}

	_, getErr := s.Get("k")
	_, getCtxErr := s.GetWithContext(ctx, "k")
	_, metaErr := s.GetMetadata(ctx, "k")
	updErr := s.UpdateMetadata(ctx, "k", nil)
	for name, err := range map[string]error{
		"Get": getErr, "GetWithContext": getCtxErr, "GetMetadata": metaErr, "UpdateMetadata": updErr,
	} {
		if err == nil || errors.Is(err, common.ErrKeyNotFound) {
			t.Errorf("%s: expected AccessDenied, not ErrKeyNotFound; got %v", name, err)
		}
		if !errors.Is(err, denied) {
			t.Errorf("%s: expected the SDK error unchanged, got %v", name, err)
		}
	}

	exists, err := s.Exists(ctx, "k")
	if exists || !errors.Is(err, denied) {
		t.Fatalf("Exists: expected false and the AccessDenied error, got %v, %v", exists, err)
	}
}

func TestS3_NoSuchBucket_IsBucketNotFound(t *testing.T) {
	ctx := context.Background()
	noBucket := sdkFailure(s3.ErrCodeNoSuchBucket, http.StatusNotFound)
	s := &S3{svc: &mockS3Client{getObjectError: noBucket, copyObjectError: noBucket}, bucket: "b"}

	_, getErr := s.Get("k")
	_, getCtxErr := s.GetWithContext(ctx, "k")
	updErr := s.UpdateMetadata(ctx, "k", nil)
	for name, err := range map[string]error{
		"Get": getErr, "GetWithContext": getCtxErr, "UpdateMetadata": updErr,
	} {
		if !errors.Is(err, common.ErrBucketNotFound) {
			t.Errorf("%s: expected ErrBucketNotFound, got %v", name, err)
		}
		if errors.Is(err, common.ErrKeyNotFound) {
			t.Errorf("%s: a missing bucket must not read as ErrKeyNotFound: %v", name, err)
		}
		if !errors.Is(err, noBucket) {
			t.Errorf("%s: expected the SDK error in the chain, got %v", name, err)
		}
	}
}

func TestS3_OtherCoded404_IsNotKeyNotFound(t *testing.T) {
	ctx := context.Background()
	other := sdkFailure("SomeOtherCode", http.StatusNotFound)
	s := &S3{svc: &mockS3Client{getObjectError: other, headObjectError: other, copyObjectError: other}, bucket: "b"}

	_, getErr := s.Get("k")
	_, metaErr := s.GetMetadata(ctx, "k")
	updErr := s.UpdateMetadata(ctx, "k", nil)
	for name, err := range map[string]error{"Get": getErr, "GetMetadata": metaErr, "UpdateMetadata": updErr} {
		if errors.Is(err, common.ErrKeyNotFound) || errors.Is(err, common.ErrBucketNotFound) {
			t.Errorf("%s: expected the raw SDK error, got %v", name, err)
		}
		if !errors.Is(err, other) {
			t.Errorf("%s: expected the SDK error unchanged, got %v", name, err)
		}
	}
	if exists, err := s.Exists(ctx, "k"); exists || !errors.Is(err, other) {
		t.Fatalf("Exists: expected false and the SDK error, got %v, %v", exists, err)
	}
}
