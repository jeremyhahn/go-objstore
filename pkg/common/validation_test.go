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

package common_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

func TestValidateKey(t *testing.T) {
	tests := []struct {
		name    string
		key     string
		wantErr bool
		errMsg  string
	}{
		{
			name:    "valid simple key",
			key:     "myfile.txt",
			wantErr: false,
		},
		{
			name:    "valid nested key",
			key:     "path/to/myfile.txt",
			wantErr: false,
		},
		{
			name:    "valid deep nested key",
			key:     "a/b/c/d/e/file.txt",
			wantErr: false,
		},
		{
			name:    "empty key",
			key:     "",
			wantErr: true,
			errMsg:  "key cannot be empty",
		},
		{
			name:    "path traversal with ..",
			key:     "../etc/passwd",
			wantErr: true,
			errMsg:  "path traversal",
		},
		{
			name:    "path traversal in middle",
			key:     "path/../etc/passwd",
			wantErr: true,
			errMsg:  "path traversal",
		},
		{
			name:    "path traversal at end",
			key:     "path/to/..",
			wantErr: true,
			errMsg:  "path traversal",
		},
		{
			name:    "absolute unix path",
			key:     "/etc/passwd",
			wantErr: true,
			errMsg:  "absolute path",
		},
		{
			name:    "absolute windows path",
			key:     "C:\\Windows\\System32",
			wantErr: true,
			errMsg:  "absolute path",
		},
		{
			name:    "windows path traversal",
			key:     "path\\..\\file.txt",
			wantErr: true,
			errMsg:  "path traversal",
		},
		{
			name:    "null byte in key",
			key:     "file\x00.txt",
			wantErr: true,
			errMsg:  "null bytes",
		},
		{
			name:    "newline in key",
			key:     "file\n.txt",
			wantErr: true,
			errMsg:  "invalid character sequence",
		},
		{
			name:    "carriage return in key",
			key:     "file\r.txt",
			wantErr: true,
			errMsg:  "invalid character sequence",
		},
		{
			name:    "tab in key",
			key:     "file\t.txt",
			wantErr: true,
			errMsg:  "invalid character sequence",
		},
		{
			name:    "double slash",
			key:     "path//file.txt",
			wantErr: true,
			errMsg:  "invalid character sequence",
		},
		{
			name:    "double backslash",
			key:     "path\\\\file.txt",
			wantErr: true,
			errMsg:  "invalid character sequence",
		},
		{
			name:    "key exceeds max length",
			key:     strings.Repeat("a", common.MaxKeyLength+1),
			wantErr: true,
			errMsg:  "exceeds maximum",
		},
		{
			name:    "key at max length",
			key:     strings.Repeat("a", common.MaxKeyLength),
			wantErr: false,
		},
		{
			name:    "unicode key",
			key:     "path/файл.txt",
			wantErr: false,
		},
		{
			name:    "invalid UTF-8",
			key:     "file\xff\xfe.txt",
			wantErr: true,
			errMsg:  "valid UTF-8",
		},
		{
			name:    "dots in filename",
			key:     "my.file.name.txt",
			wantErr: false,
		},
		{
			name:    "hidden file",
			key:     ".hidden",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := common.ValidateKey(tt.key)
			if tt.wantErr {
				if err == nil {
					t.Errorf("ValidateKey() expected error but got nil")
					return
				}
				if tt.errMsg != "" && !strings.Contains(err.Error(), tt.errMsg) {
					t.Errorf("ValidateKey() error = %v, expected to contain %v", err.Error(), tt.errMsg)
				}
			} else {
				if err != nil {
					t.Errorf("ValidateKey() unexpected error = %v", err)
				}
			}
		})
	}
}

func TestValidateMetadata(t *testing.T) {
	tests := []struct {
		name     string
		metadata map[string]string
		wantErr  bool
		errMsg   string
	}{
		{
			name:     "nil metadata",
			metadata: nil,
			wantErr:  false,
		},
		{
			name:     "empty metadata",
			metadata: map[string]string{},
			wantErr:  false,
		},
		{
			name: "valid metadata",
			metadata: map[string]string{
				"author": "John Doe",
				"type":   "document",
			},
			wantErr: false,
		},
		{
			name: "empty key",
			metadata: map[string]string{
				"": "value",
			},
			wantErr: true,
			errMsg:  "key cannot be empty",
		},
		{
			name: "key too long",
			metadata: map[string]string{
				strings.Repeat("a", common.MaxMetadataKeyLength+1): "value",
			},
			wantErr: true,
			errMsg:  "exceeds maximum length",
		},
		{
			name: "value too long",
			metadata: map[string]string{
				"key": strings.Repeat("a", common.MaxMetadataValueLength+1),
			},
			wantErr: true,
			errMsg:  "exceeds maximum length",
		},
		{
			name: "key with null byte",
			metadata: map[string]string{
				"key\x00": "value",
			},
			wantErr: true,
			errMsg:  "null bytes",
		},
		{
			name: "value with null byte",
			metadata: map[string]string{
				"key": "value\x00",
			},
			wantErr: true,
			errMsg:  "null bytes",
		},
		{
			name: "invalid UTF-8 in key",
			metadata: map[string]string{
				"key\xff\xfe": "value",
			},
			wantErr: true,
			errMsg:  "valid UTF-8",
		},
		{
			name: "invalid UTF-8 in value",
			metadata: map[string]string{
				"key": "value\xff\xfe",
			},
			wantErr: true,
			errMsg:  "valid UTF-8",
		},
		{
			name: "too many entries",
			metadata: func() map[string]string {
				m := make(map[string]string)
				for i := 0; i <= common.MaxMetadataEntries; i++ {
					m[string(rune(i))] = "value"
				}
				return m
			}(),
			wantErr: true,
			errMsg:  "cannot have more than",
		},
		{
			name: "at max entries",
			metadata: func() map[string]string {
				m := make(map[string]string)
				for i := 0; i < common.MaxMetadataEntries; i++ {
					// Use valid keys (avoid control characters that would fail validation)
					m["key"+string(rune('a'+i%26))+string(rune('0'+i/26))] = "value"
				}
				return m
			}(),
			wantErr: false,
		},
		{
			name: "unicode metadata",
			metadata: map[string]string{
				"автор": "Иван Иванов",
				"类型":    "文档",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := common.ValidateMetadata(tt.metadata)
			if tt.wantErr {
				if err == nil {
					t.Errorf("ValidateMetadata() expected error but got nil")
					return
				}
				if tt.errMsg != "" && !strings.Contains(err.Error(), tt.errMsg) {
					t.Errorf("ValidateMetadata() error = %v, expected to contain %v", err.Error(), tt.errMsg)
				}
			} else {
				if err != nil {
					t.Errorf("ValidateMetadata() unexpected error = %v", err)
				}
			}
		})
	}
}

func TestSanitizeErrorMessage(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected string
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: "",
		},
		{
			name:     "file not found",
			err:      errors.New("open /path/to/file: no such file or directory"),
			expected: "object not found",
		},
		{
			name:     "does not exist",
			err:      errors.New("file does not exist"),
			expected: "object not found",
		},
		{
			name:     "permission denied",
			err:      errors.New("open /path/to/file: permission denied"),
			expected: "access denied",
		},
		{
			name:     "file exists",
			err:      errors.New("file exists"),
			expected: "object already exists",
		},
		{
			name:     "already exists",
			err:      errors.New("object already exists"),
			expected: "object already exists",
		},
		{
			name:     "connection refused",
			err:      errors.New("dial tcp: connection refused"),
			expected: "service unavailable",
		},
		{
			name:     "timeout",
			err:      errors.New("request timeout"),
			expected: "request timeout",
		},
		{
			name:     "context deadline exceeded",
			err:      errors.New("context deadline exceeded"),
			expected: "request timeout",
		},
		{
			name:     "context canceled",
			err:      errors.New("context canceled"),
			expected: "request canceled",
		},
		{
			name:     "EOF",
			err:      errors.New("EOF"),
			expected: "request failed",
		},
		{
			name:     "unexpected EOF",
			err:      errors.New("unexpected EOF"),
			expected: "request failed",
		},
		{
			name:     "broken pipe",
			err:      errors.New("write: broken pipe"),
			expected: "connection error",
		},
		{
			name:     "validation error preserved",
			err:      &common.ValidationError{Field: "key", Message: "invalid key"},
			expected: "validation error on field 'key': invalid key",
		},
		{
			name:     "generic error",
			err:      errors.New("some internal error"),
			expected: "internal server error",
		},
		{
			name:     "path in error message",
			err:      errors.New("failed to read /home/user/secrets/password.txt"),
			expected: "internal server error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := common.SanitizeErrorMessage(tt.err)
			if result != tt.expected {
				t.Errorf("SanitizeErrorMessage() = %v, expected %v", result, tt.expected)
			}
		})
	}
}

func TestSanitizeCustomMetadata(t *testing.T) {
	tests := []struct {
		name     string
		metadata map[string]string
		expected map[string]string
	}{
		{
			name:     "nil metadata",
			metadata: nil,
			expected: nil,
		},
		{
			name:     "empty metadata",
			metadata: map[string]string{},
			expected: map[string]string{},
		},
		{
			name: "clean metadata",
			metadata: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
			expected: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
		},
		{
			name: "metadata with control characters",
			metadata: map[string]string{
				"key": "value\x01\x02\x03",
			},
			expected: map[string]string{
				"key": "value",
			},
		},
		{
			name: "metadata with allowed newlines and tabs",
			metadata: map[string]string{
				"key": "value\nwith\tnewline",
			},
			expected: map[string]string{
				"key": "value\nwith\tnewline",
			},
		},
		{
			name: "metadata with unicode",
			metadata: map[string]string{
				"key": "значение с юникодом 中文",
			},
			expected: map[string]string{
				"key": "значение с юникодом 中文",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := common.SanitizeCustomMetadata(tt.metadata)

			if tt.expected == nil {
				if result != nil {
					t.Errorf("SanitizeCustomMetadata() = %v, expected nil", result)
				}
				return
			}

			if len(result) != len(tt.expected) {
				t.Errorf("SanitizeCustomMetadata() length = %v, expected %v", len(result), len(tt.expected))
				return
			}

			for key, expectedValue := range tt.expected {
				if result[key] != expectedValue {
					t.Errorf("SanitizeCustomMetadata()[%v] = %v, expected %v", key, result[key], expectedValue)
				}
			}
		})
	}
}

func TestValidationError(t *testing.T) {
	tests := []struct {
		name     string
		err      *common.ValidationError
		expected string
	}{
		{
			name: "error with field",
			err: &common.ValidationError{
				Field:   "key",
				Message: "invalid key",
			},
			expected: "validation error on field 'key': invalid key",
		},
		{
			name: "error without field",
			err: &common.ValidationError{
				Message: "validation failed",
			},
			expected: "validation error: validation failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.err.Error()
			if result != tt.expected {
				t.Errorf("ValidationError.Error() = %v, expected %v", result, tt.expected)
			}
		})
	}
}
