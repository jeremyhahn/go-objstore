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
	"sort"

	"github.com/jeremyhahn/go-objstore/pkg/common"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// Compile-time checks that S3 satisfies the optional capabilities.
var (
	_ common.BucketManager     = (*S3)(nil)
	_ common.MultipartUploader = (*S3)(nil)
)

// Service error codes, as smithy.APIError.ErrorCode reports them.
const (
	codeBucketNotEmpty          = "BucketNotEmpty"
	codeNotFound                = "NotFound" // the code of a failed HEAD request
	codeNoSuchKey               = "NoSuchKey"
	codeNoSuchBucket            = "NoSuchBucket"
	codeNoSuchUpload            = "NoSuchUpload"
	codeNoSuchLifecycle         = "NoSuchLifecycleConfiguration"
	codeBucketAlreadyExists     = "BucketAlreadyExists"
	codeBucketAlreadyOwnedByYou = "BucketAlreadyOwnedByYou"
	codeInvalidPart             = "InvalidPart"
	codeInvalidPartOrder        = "InvalidPartOrder"
	codeEntityTooSmall          = "EntityTooSmall"
	codeEntityTooLarge          = "EntityTooLarge"
	codeInvalidBucketName       = "InvalidBucketName"

	// defaultRegion needs no location constraint when a bucket is created.
	defaultRegion = "us-east-1"
)

// awsCode returns the service error code of err, or "" for any other error.
func awsCode(err error) string {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		return ae.ErrorCode()
	}
	return ""
}

// requireBucket reports ErrBucketNotSet for a manager that has no bucket of
// its own.
func (s *S3) requireBucket() error {
	if s.bucket == "" {
		return common.ErrBucketNotSet
	}
	return nil
}

// CreateBucket creates a bucket. The location constraint is the configured
// region, except in the default region, which takes none.
func (s *S3) CreateBucket(ctx context.Context, name string) error {
	if err := common.ValidateBucketName(name); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// In the default region the service answers success when the caller
	// already owns the bucket, and resets its ACLs, so the bucket is looked
	// for first. A bucket made between the look and the create is still
	// reported by the service, and mapped below.
	_, err := s.svc.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(name)})
	if err == nil {
		return fmt.Errorf("%w: %s", common.ErrBucketAlreadyExists, name)
	}
	if berr := bucketError(err, name); !errors.Is(berr, common.ErrBucketNotFound) {
		return berr
	}
	input := &s3.CreateBucketInput{Bucket: aws.String(name)}
	if s.region != "" && s.region != defaultRegion {
		input.CreateBucketConfiguration = &types.CreateBucketConfiguration{
			LocationConstraint: types.BucketLocationConstraint(s.region),
		}
	}
	_, err = s.svc.CreateBucket(ctx, input)
	switch awsCode(err) {
	case "":
		return err
	case codeBucketAlreadyExists, codeBucketAlreadyOwnedByYou:
		return fmt.Errorf("%w: %s", common.ErrBucketAlreadyExists, name)
	case codeInvalidBucketName:
		return fmt.Errorf("%w: %s", common.ErrInvalidBucketName, name)
	}
	return err
}

// DeleteBucket removes an empty bucket. An upload in progress counts as
// content, as the interface requires, although the service alone would delete
// the bucket and orphan the upload.
func (s *S3) DeleteBucket(ctx context.Context, name string) error {
	if err := common.ValidateBucketName(name); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	out, err := s.svc.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{
		Bucket:     aws.String(name),
		MaxUploads: aws.Int32(1),
	})
	if err != nil {
		return bucketError(err, name)
	}
	if len(out.Uploads) > 0 {
		return fmt.Errorf("%w: %s", common.ErrBucketNotEmpty, name)
	}

	_, err = s.svc.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(name)})
	return bucketError(err, name)
}

// bucketError maps the service errors of a call on a named bucket.
func bucketError(err error, name string) error {
	switch awsCode(err) {
	case "":
		return err
	case codeNoSuchBucket, codeNotFound:
		return fmt.Errorf("%w: %s", common.ErrBucketNotFound, name)
	case codeBucketNotEmpty:
		return fmt.Errorf("%w: %s", common.ErrBucketNotEmpty, name)
	}
	return err
}

// ListBuckets returns the buckets of the account, sorted by name.
func (s *S3) ListBuckets(ctx context.Context) ([]common.BucketInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	out, err := s.svc.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		return nil, err
	}
	infos := make([]common.BucketInfo, 0, len(out.Buckets))
	for _, b := range out.Buckets {
		infos = append(infos, common.BucketInfo{
			Name:    aws.ToString(b.Name),
			Created: aws.ToTime(b.CreationDate),
		})
	}
	sort.Slice(infos, func(i, j int) bool { return infos[i].Name < infos[j].Name })
	return infos, nil
}

// Bucket returns the Storage bound to the named bucket. It shares this
// client and its credentials.
func (s *S3) Bucket(ctx context.Context, name string) (common.Storage, error) {
	if err := common.ValidateBucketName(name); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if _, err := s.svc.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(name)}); err != nil {
		return nil, bucketError(err, name)
	}
	return &S3{
		svc:                s.svc,
		bucket:             name,
		region:             s.region,
		multipartExpiry:    s.multipartExpiry,
		storageClass:       s.storageClass,
		transitionClass:    s.transitionClass,
		multipartThreshold: s.multipartThreshold,
		partSize:           s.partSize,
	}, nil
}
