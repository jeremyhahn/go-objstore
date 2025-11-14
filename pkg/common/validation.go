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
	"fmt"
	"path/filepath"
	"strings"
	"unicode/utf8"
)

const (
	// MaxKeyLength is the maximum allowed length for object keys
	MaxKeyLength = 1024

	// MaxMetadataKeyLength is the maximum allowed length for metadata keys
	MaxMetadataKeyLength = 256

	// MaxMetadataValueLength is the maximum allowed length for metadata values
	MaxMetadataValueLength = 2048

	// MaxMetadataEntries is the maximum number of custom metadata entries
	MaxMetadataEntries = 100
)

// ValidationError represents a validation error
type ValidationError struct {
	Field   string
	Message string
}

func (e *ValidationError) Error() string {
	if e.Field != "" {
		return fmt.Sprintf("validation error on field '%s': %s", e.Field, e.Message)
	}
	return fmt.Sprintf("validation error: %s", e.Message)
}

// ValidateKey validates an object key for security issues
// Returns error if the key:
// - Is empty
// - Contains path traversal sequences (..)
// - Is an absolute path
// - Contains null bytes
// - Exceeds maximum length
// - Contains invalid characters
func ValidateKey(key string) error {
	if key == "" {
		return &ValidationError{
			Field:   "key",
			Message: "key cannot be empty",
		}
	}

	// Check key length first (fast check, no allocations)
	keyLen := len(key)
	if keyLen > MaxKeyLength {
		return &ValidationError{
			Field:   "key",
			Message: fmt.Sprintf("key length exceeds maximum of %d bytes", MaxKeyLength),
		}
	}

	// Check for Windows-style absolute paths (C:\, D:\, etc.)
	if keyLen >= 2 && key[1] == ':' {
		return &ValidationError{
			Field:   "key",
			Message: "key cannot be an absolute path",
		}
	}

	// Single pass through string to check for multiple issues
	// Avoids multiple passes and allocations
	hasBackslash := false
	for i := 0; i < keyLen; i++ {
		c := key[i]

		// Check for null bytes
		if c == '\x00' {
			return &ValidationError{
				Field:   "key",
				Message: "key cannot contain null bytes",
			}
		}

		// Check for control characters
		if c == '\n' || c == '\r' || c == '\t' {
			return &ValidationError{
				Field:   "key",
				Message: fmt.Sprintf("key contains invalid character sequence: %q", string(c)),
			}
		}

		// Check for backslashes
		if c == '\\' {
			hasBackslash = true
			// Check for double backslash
			if i+1 < keyLen && key[i+1] == '\\' {
				return &ValidationError{
					Field:   "key",
					Message: `key contains invalid character sequence: "\\"`,
				}
			}
			// Check for ".." with backslashes
			if i+2 < keyLen && key[i+1] == '.' && key[i+2] == '.' {
				return &ValidationError{
					Field:   "key",
					Message: "key cannot contain path traversal sequences (..)",
				}
			}
		}

		// Check for double forward slashes
		if c == '/' {
			if i+1 < keyLen && key[i+1] == '/' {
				return &ValidationError{
					Field:   "key",
					Message: `key contains invalid character sequence: "//"`,
				}
			}
			// Check for "../" pattern
			if i >= 2 && key[i-1] == '.' && key[i-2] == '.' && (i == 2 || key[i-3] == '/') {
				return &ValidationError{
					Field:   "key",
					Message: "key cannot contain path traversal sequences (..)",
				}
			}
			// Check for "/.." at end or followed by /
			if i+2 < keyLen && key[i+1] == '.' && key[i+2] == '.' {
				if i+3 >= keyLen || key[i+3] == '/' {
					return &ValidationError{
						Field:   "key",
						Message: "key cannot contain path traversal sequences (..)",
					}
				}
			}
		}
	}

	// Check if starts with ".."
	if keyLen >= 2 && key[0] == '.' && key[1] == '.' {
		if keyLen == 2 || key[2] == '/' || key[2] == '\\' {
			return &ValidationError{
				Field:   "key",
				Message: "key cannot contain path traversal sequences (..)",
			}
		}
	}

	// Check if key is valid UTF-8 (only if necessary)
	if !utf8.ValidString(key) {
		return &ValidationError{
			Field:   "key",
			Message: "key must be valid UTF-8",
		}
	}

	// Check for absolute paths (Unix-style)
	// Only do filepath.IsAbs if no backslashes (Windows check already done above)
	if !hasBackslash && filepath.IsAbs(key) {
		return &ValidationError{
			Field:   "key",
			Message: "key cannot be an absolute path",
		}
	}

	return nil
}

// ValidateMetadata validates metadata for security and size constraints
// Returns error if metadata:
// - Has too many entries
// - Has keys or values that are too long
// - Contains null bytes
// - Contains invalid UTF-8
func ValidateMetadata(metadata map[string]string) error {
	if metadata == nil {
		return nil
	}

	// Check number of entries
	if len(metadata) > MaxMetadataEntries {
		return &ValidationError{
			Field:   "metadata",
			Message: fmt.Sprintf("metadata cannot have more than %d entries", MaxMetadataEntries),
		}
	}

	// Validate each key-value pair
	for key, value := range metadata {
		// Check key
		if key == "" {
			return &ValidationError{
				Field:   "metadata.key",
				Message: "metadata key cannot be empty",
			}
		}

		if len(key) > MaxMetadataKeyLength {
			return &ValidationError{
				Field:   "metadata.key",
				Message: fmt.Sprintf("metadata key '%s' exceeds maximum length of %d bytes", key, MaxMetadataKeyLength),
			}
		}

		if strings.ContainsRune(key, '\x00') {
			return &ValidationError{
				Field:   "metadata.key",
				Message: "metadata key cannot contain null bytes",
			}
		}

		if !utf8.ValidString(key) {
			return &ValidationError{
				Field:   "metadata.key",
				Message: "metadata key must be valid UTF-8",
			}
		}

		// Check value
		if len(value) > MaxMetadataValueLength {
			return &ValidationError{
				Field:   "metadata.value",
				Message: fmt.Sprintf("metadata value for key '%s' exceeds maximum length of %d bytes", key, MaxMetadataValueLength),
			}
		}

		if strings.ContainsRune(value, '\x00') {
			return &ValidationError{
				Field:   "metadata.value",
				Message: "metadata value cannot contain null bytes",
			}
		}

		if !utf8.ValidString(value) {
			return &ValidationError{
				Field:   "metadata.value",
				Message: "metadata value must be valid UTF-8",
			}
		}
	}

	return nil
}

// SanitizeErrorMessage removes sensitive internal details from error messages
// to prevent information disclosure to clients
func SanitizeErrorMessage(err error) string {
	if err == nil {
		return ""
	}

	errMsg := err.Error()

	// If it's already a ValidationError, return it as-is (safe to expose)
	if _, ok := err.(*ValidationError); ok {
		return errMsg
	}

	// Patterns to sanitize (remove absolute paths, internal details)
	// Replace absolute file paths with generic message
	if strings.Contains(errMsg, "/") || strings.Contains(errMsg, "\\") {
		// Check for common filesystem error patterns
		if strings.Contains(errMsg, "no such file or directory") ||
			strings.Contains(errMsg, "does not exist") {
			return "object not found"
		}
		if strings.Contains(errMsg, "permission denied") {
			return "access denied"
		}
		if strings.Contains(errMsg, "file exists") ||
			strings.Contains(errMsg, "already exists") {
			return "object already exists"
		}
	}

	// Sanitize common error patterns
	sanitizedPatterns := map[string]string{
		"no such file or directory": "object not found",
		"does not exist":            "object not found",
		"permission denied":         "access denied",
		"file exists":               "object already exists",
		"already exists":            "object already exists",
		"invalid argument":          "invalid request",
		"connection refused":        "service unavailable",
		"connection reset":          "service unavailable",
		"timeout":                   "request timeout",
		"context canceled":          "request canceled",
		"context deadline exceeded": "request timeout",
		"broken pipe":               "connection error",
	}

	// Check for exact EOF match
	if errMsg == "EOF" || errMsg == "unexpected EOF" {
		return "request failed"
	}

	lowerMsg := strings.ToLower(errMsg)
	for pattern, replacement := range sanitizedPatterns {
		if strings.Contains(lowerMsg, pattern) {
			return replacement
		}
	}

	// If no specific pattern matched, return a generic error
	return "internal server error"
}

// SanitizeCustomMetadata sanitizes custom metadata values
// This ensures that metadata values are safe to store and return
func SanitizeCustomMetadata(metadata map[string]string) map[string]string {
	if metadata == nil {
		return nil
	}

	sanitized := make(map[string]string, len(metadata))
	for key, value := range metadata {
		// Remove control characters except newlines and tabs
		cleaned := strings.Map(func(r rune) rune {
			// Allow printable characters, newlines, and tabs
			if r == '\n' || r == '\t' || (r >= 32 && r < 127) || r > 127 {
				return r
			}
			// Remove other control characters
			return -1
		}, value)

		sanitized[key] = cleaned
	}

	return sanitized
}
