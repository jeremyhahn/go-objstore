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

package common

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestValidateBucketName(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"minimum length", "abc", false},
		{"maximum length", strings.Repeat("a", 63), false},
		{"digits and hyphen", "my-bucket-01", false},
		{"all digits", "123", false},
		{"empty", "", true},
		{"too short", "ab", true},
		{"too long", strings.Repeat("a", 64), true},
		{"uppercase", "MyBucket", true},
		{"underscore", "my_bucket", true},
		{"dot", "my.bucket", true},
		{"slash", "my/bucket", true},
		{"space", "my bucket", true},
		{"leading hyphen", "-bucket", true},
		{"trailing hyphen", "bucket-", true},
		{"double hyphen", "my--bucket", true},
		{"non ascii", "bücket", true},
		{"null byte", "buck\x00et", true},
		{"traversal", "..", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateBucketName(tt.input)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ValidateBucketName(%q) = %v, wantErr %v", tt.input, err, tt.wantErr)
			}
			if err == nil {
				return
			}
			if !errors.Is(err, ErrInvalidBucketName) {
				t.Errorf("error %v does not match ErrInvalidBucketName", err)
			}
			if !errors.Is(err, ErrInvalidArgument) {
				t.Errorf("error %v does not match ErrInvalidArgument", err)
			}
			if Classify(err) != CodeInvalidArgument {
				t.Errorf("Classify(%v) = %v, want CodeInvalidArgument", err, Classify(err))
			}
		})
	}
}

func TestValidateKey_ReservedMultipartPrefix(t *testing.T) {
	tests := []struct {
		name    string
		key     string
		wantErr bool
	}{
		{"staged part", ".objstore-multipart/abc/1", true},
		{"prefix only", ".objstore-multipart/", true},
		{"bare directory name", ".objstore-multipart", true},
		{"dot segment", "./.objstore-multipart/abc/1", true},
		{"repeated dot segments", "././.objstore-multipart/abc/1", true},
		{"dot segment on the bare directory", "./.objstore-multipart", true},
		{"dot segment after the directory", ".objstore-multipart/./abc", true},
		{"dot-dot detour", "a/../.objstore-multipart/abc/1", true},
		{"backslash separator", ".objstore-multipart\\abc\\1", true},
		{"backslash after a dot segment", ".\\.objstore-multipart\\abc\\1", true},
		{"mixed separators", "./.objstore-multipart\\abc", true},
		{"other case", ".OBJSTORE-Multipart/abc/1", true},
		{"similar name", ".objstore-multipart-x/a", false},
		{"similar name after a dot segment", "./.objstore-multipart-x/a", false},
		{"dot segment before an ordinary key", "./photos/cat.jpg", false},
		{"nested not at start", "a/.objstore-multipart/b", false},
		{"ordinary key", "photos/cat.jpg", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateKey(tt.key)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ValidateKey(%q) = %v, wantErr %v", tt.key, err, tt.wantErr)
			}
			if err != nil && !errors.Is(err, ErrInvalidArgument) {
				t.Errorf("error %v does not match ErrInvalidArgument", err)
			}
		})
	}
}

func TestClassify_BucketAndMultipartErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want ErrorCode
	}{
		{"not supported", ErrNotSupported, CodeUnimplemented},
		{"wrapped not supported", fmt.Errorf("facade: %w", ErrNotSupported), CodeUnimplemented},
		{"replication not supported", ErrReplicationNotSupported, CodeUnimplemented},
		{"bucket not found", ErrBucketNotFound, CodeNotFound},
		{"bucket already exists", ErrBucketAlreadyExists, CodeAlreadyExists},
		{"bucket not empty", ErrBucketNotEmpty, CodeFailedPrecondition},
		{"wrapped bucket not empty", fmt.Errorf("delete: %w", ErrBucketNotEmpty), CodeFailedPrecondition},
		{"upload not found", ErrUploadNotFound, CodeNotFound},
		{"invalid bucket name", ErrInvalidBucketName, CodeInvalidArgument},
		{"invalid part number", ErrInvalidPartNumber, CodeInvalidArgument},
		{"part too small", ErrPartTooSmall, CodeInvalidArgument},
		{"part too large", ErrPartTooLarge, CodeInvalidArgument},
		{"invalid part order", ErrInvalidPartOrder, CodeInvalidArgument},
		{"part mismatch", ErrPartMismatch, CodeInvalidArgument},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Classify(tt.err); got != tt.want {
				t.Errorf("Classify(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestBucketErrors_PackagePrefixAndWrapping(t *testing.T) {
	prefixed := []error{
		ErrNotSupported, ErrBucketNotFound, ErrBucketAlreadyExists, ErrBucketNotEmpty,
		ErrUploadNotFound, ErrInvalidBucketName, ErrInvalidPartNumber, ErrPartTooSmall,
		ErrPartTooLarge, ErrInvalidPartOrder, ErrPartMismatch,
	}
	for _, err := range prefixed {
		if !strings.HasPrefix(err.Error(), "common: ") {
			t.Errorf("%q lacks the package prefix", err.Error())
		}
	}
	if !errors.Is(ErrBucketAlreadyExists, ErrAlreadyExists) {
		t.Error("ErrBucketAlreadyExists must wrap ErrAlreadyExists")
	}
	if !errors.Is(ErrReplicationNotSupported, ErrNotSupported) {
		t.Error("ErrReplicationNotSupported must wrap ErrNotSupported")
	}
}

func TestMultipartLimits(t *testing.T) {
	if MinPartSize != 5*1024*1024 {
		t.Errorf("MinPartSize = %d", MinPartSize)
	}
	if MaxPartSize != 5*1024*1024*1024 {
		t.Errorf("MaxPartSize = %d", MaxPartSize)
	}
	if MaxParts != 10000 {
		t.Errorf("MaxParts = %d", MaxParts)
	}
}
