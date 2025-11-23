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

// Package validation provides centralized input validation for all go-objstore APIs.
// ALL public interfaces (gRPC, REST, QUIC, CLI, MCP) use the facade which enforces
// these validations, preventing injection attacks across all entry points.
//
//nolint:err113 // Validation errors are intentionally dynamic for descriptive messages
package validation

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
)

var (
	// keyPattern matches safe key identifiers (without backend prefix)
	// Allows: alphanumeric, dash, underscore, dot, forward slash (for paths)
	keyPattern = regexp.MustCompile(`^[a-zA-Z0-9_\-\./]+$`)

	// backendPattern matches safe backend names (lowercase alphanumeric + hyphens)
	backendPattern = regexp.MustCompile(`^[a-z0-9\-]+$`)

	// keyReferencePattern matches key references with optional backend prefix
	// Format: "backend:key" or just "key"
	keyReferencePattern = regexp.MustCompile(`^([a-z0-9\-]+:)?[a-zA-Z0-9_\-\./]+$`)
)

// ValidateKey validates an object key.
// Prevents path traversal, injection, and other attacks by:
// - Rejecting empty strings
// - Rejecting null bytes
// - Rejecting absolute paths
// - Rejecting parent directory references (..)
// - Allowing only safe characters
// - Enforcing length limits
func ValidateKey(key string) error {
	if key == "" {
		return fmt.Errorf("key cannot be empty")
	}

	// Check for null bytes (can bypass some path checks)
	if strings.Contains(key, "\x00") {
		return fmt.Errorf("key contains null byte")
	}

	// Check length before other validations (prevent ReDoS)
	if len(key) > 1024 {
		return fmt.Errorf("key too long (max 1024 characters)")
	}

	// Check for absolute paths
	if filepath.IsAbs(key) {
		return fmt.Errorf("key cannot be an absolute path")
	}

	// Check for path traversal attempts
	// Check for ".." as a path component (not just anywhere in string)
	// This catches: "..", "../foo", "foo/..", "foo/../bar", "foo/..", etc.
	// But allows: "file..txt", "foo..bar" (where .. is part of the name)
	if key == ".." ||
		strings.HasPrefix(key, "../") ||
		strings.HasSuffix(key, "/..") ||
		strings.Contains(key, "/../") {
		return fmt.Errorf("key contains path traversal attempt")
	}

	// Check for control characters
	for _, r := range key {
		if r < 32 || r == 127 {
			return fmt.Errorf("key contains control characters")
		}
	}

	// Only allow safe characters
	if !keyPattern.MatchString(key) {
		return fmt.Errorf("key contains invalid characters (allowed: a-z, A-Z, 0-9, -, _, ., /)")
	}

	return nil
}

// ValidateKeyReference validates a key reference which may include backend prefix.
// Format: "backend:key" or "key"
func ValidateKeyReference(keyRef string) error {
	if keyRef == "" {
		return fmt.Errorf("key reference cannot be empty")
	}

	// Check for null bytes
	if strings.Contains(keyRef, "\x00") {
		return fmt.Errorf("key reference contains null byte")
	}

	// Check length (64 for backend + 1 for colon + 1024 for key)
	if len(keyRef) > 1089 {
		return fmt.Errorf("key reference too long (max 1089 characters)")
	}

	// Check for control characters
	for _, r := range keyRef {
		if r < 32 || r == 127 {
			return fmt.Errorf("key reference contains control characters")
		}
	}

	// Parse and validate components
	parts := strings.SplitN(keyRef, ":", 2)
	if len(parts) == 2 {
		// Format: "backend:key"
		if err := ValidateBackendName(parts[0]); err != nil {
			return fmt.Errorf("invalid backend in key reference: %w", err)
		}
		if err := ValidateKey(parts[1]); err != nil {
			return fmt.Errorf("invalid key in key reference: %w", err)
		}
	} else {
		// Format: "key" only
		if err := ValidateKey(keyRef); err != nil {
			return err
		}
	}

	return nil
}

// ValidateBackendName validates a backend name.
// Backend names must be simple lowercase identifiers.
func ValidateBackendName(backend string) error {
	if backend == "" {
		return fmt.Errorf("backend name cannot be empty")
	}

	// Check for null bytes
	if strings.Contains(backend, "\x00") {
		return fmt.Errorf("backend name contains null byte")
	}

	// Check length
	if len(backend) > 64 {
		return fmt.Errorf("backend name too long (max 64 characters)")
	}

	// Check for control characters
	for _, r := range backend {
		if r < 32 || r == 127 {
			return fmt.Errorf("backend name contains control characters")
		}
	}

	// Only allow lowercase alphanumeric and hyphens
	if !backendPattern.MatchString(backend) {
		return fmt.Errorf("backend name contains invalid characters (allowed: a-z, 0-9, -)")
	}

	return nil
}

// ValidatePrefix validates a list prefix.
// Prefixes follow similar rules to keys but can be empty.
func ValidatePrefix(prefix string) error {
	// Empty prefix is valid (list all objects)
	if prefix == "" {
		return nil
	}

	// Apply same validation as keys
	return ValidateKey(prefix)
}

// SanitizeForLog sanitizes a string for safe logging (prevents log injection).
func SanitizeForLog(s string) string {
	// Remove control characters and null bytes
	s = strings.Map(func(r rune) rune {
		if r < 32 || r == 127 {
			return -1
		}
		return r
	}, s)

	// Limit length to prevent log flooding
	if len(s) > 1000 {
		s = s[:1000] + "...[truncated]"
	}

	return s
}
