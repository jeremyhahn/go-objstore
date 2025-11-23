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

package validation

import (
	"strings"
	"testing"
)

func TestValidateKey(t *testing.T) {
	tests := []struct {
		name    string
		key     string
		wantErr bool
	}{
		// Valid keys
		{"valid simple key", "mykey", false},
		{"valid key with dashes", "my-key", false},
		{"valid key with underscores", "my_key", false},
		{"valid key with dots", "my.key", false},
		{"valid path key", "path/to/object", false},
		{"valid nested path", "a/b/c/d/file.txt", false},
		{"valid alphanumeric", "key123", false},
		{"valid mixed case", "MyKey123", false},

		// Invalid keys - empty
		{"empty key", "", true},

		// Invalid keys - null bytes
		{"null byte", "key\x00data", true},

		// Invalid keys - path traversal
		{"path traversal ..", "..", true},
		{"path traversal ../", "../file", true},
		{"path traversal /..", "path/../file", true},
		{"path traversal /../../", "path/../../file", true},
		{"path traversal at end", "path/..", true},

		// Invalid keys - absolute paths
		{"absolute path unix", "/etc/passwd", true},
		{"absolute path windows", "C:\\Windows\\System32", true},

		// Invalid keys - control characters
		{"newline", "key\ndata", true},
		{"carriage return", "key\rdata", true},
		{"tab", "key\tdata", true},

		// Invalid keys - length
		{"too long", strings.Repeat("a", 1025), true},

		// Invalid keys - special characters
		{"space", "my key", true},
		{"question mark", "key?", true},
		{"asterisk", "key*", true},
		{"pipe", "key|", true},
		{"less than", "key<", true},
		{"greater than", "key>", true},
		{"double quote", "key\"", true},
		{"colon", "key:value", true},

		// Edge cases
		{"single char", "a", false},
		{"max length", strings.Repeat("a", 1024), false},
		{"dot file", ".hidden", false},
		{"dotdot as part of name", "file..txt", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateKey(tt.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateKey(%q) error = %v, wantErr %v", tt.key, err, tt.wantErr)
			}
		})
	}
}

func TestValidateKeyReference(t *testing.T) {
	tests := []struct {
		name    string
		keyRef  string
		wantErr bool
	}{
		// Valid key references
		{"valid simple key", "mykey", false},
		{"valid backend:key", "s3:mykey", false},
		{"valid backend:path", "local:path/to/file", false},
		{"valid complex backend", "my-backend:my-key", false},
		{"valid nested path", "gcs:folder/subfolder/file.txt", false},

		// Invalid key references - empty
		{"empty reference", "", true},

		// Invalid key references - bad backend
		{"invalid backend uppercase", "S3:mykey", true},
		{"invalid backend special char", "s3_backend:mykey", true},
		{"invalid backend space", "my backend:mykey", true},

		// Invalid key references - bad key
		{"invalid key null byte", "s3:key\x00data", true},
		{"invalid key path traversal", "s3:../etc/passwd", true},
		{"invalid key absolute", "s3:/etc/passwd", true},

		// Invalid key references - control characters
		{"newline in reference", "s3:key\ndata", true},
		{"tab in reference", "s3:key\tdata", true},

		// Invalid key references - length
		{"too long", strings.Repeat("a", 1090), true},
		{"backend too long", strings.Repeat("a", 65) + ":key", true},

		// Edge cases
		{"just colon", ":", true},
		{"multiple colons", "s3:path:to:file", true}, // Colons not allowed in keys
		{"colon at end", "s3:", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateKeyReference(tt.keyRef)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateKeyReference(%q) error = %v, wantErr %v", tt.keyRef, err, tt.wantErr)
			}
		})
	}
}

func TestValidateBackendName(t *testing.T) {
	tests := []struct {
		name        string
		backendName string
		wantErr     bool
	}{
		// Valid backend names
		{"valid simple", "s3", false},
		{"valid with dash", "my-backend", false},
		{"valid with numbers", "backend123", false},
		{"valid complex", "my-s3-backend-1", false},

		// Invalid backend names - empty
		{"empty", "", true},

		// Invalid backend names - uppercase
		{"uppercase", "S3", true},
		{"mixed case", "myBackend", true},

		// Invalid backend names - special characters
		{"underscore", "my_backend", true},
		{"dot", "my.backend", true},
		{"space", "my backend", true},
		{"slash", "my/backend", true},

		// Invalid backend names - null bytes
		{"null byte", "backend\x00", true},

		// Invalid backend names - control characters
		{"newline", "backend\n", true},
		{"tab", "backend\t", true},

		// Invalid backend names - length
		{"too long", strings.Repeat("a", 65), true},

		// Edge cases
		{"single char", "a", false},
		{"max length", strings.Repeat("a", 64), false},
		{"just dash", "-", false},
		{"start with dash", "-backend", false},
		{"end with dash", "backend-", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateBackendName(tt.backendName)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateBackendName(%q) error = %v, wantErr %v", tt.backendName, err, tt.wantErr)
			}
		})
	}
}

func TestValidatePrefix(t *testing.T) {
	tests := []struct {
		name    string
		prefix  string
		wantErr bool
	}{
		// Valid prefixes
		{"empty prefix", "", false}, // Empty is valid for listing all
		{"valid simple", "logs/", false},
		{"valid nested", "logs/2024/", false},
		{"valid without trailing slash", "logs", false},
		{"valid file prefix", "file-", false},

		// Invalid prefixes - same as key validation
		{"path traversal", "../", true},
		{"absolute path", "/var/log/", true},
		{"null byte", "logs\x00/", true},
		{"control char", "logs\n/", true},

		// Edge cases
		{"single char", "a", false},
		{"dot", ".", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidatePrefix(tt.prefix)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidatePrefix(%q) error = %v, wantErr %v", tt.prefix, err, tt.wantErr)
			}
		})
	}
}

func TestSanitizeForLog(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"clean string", "hello world", "hello world"},
		{"with null byte", "hello\x00world", "helloworld"},
		{"with newline", "hello\nworld", "helloworld"},
		{"with tab", "hello\tworld", "helloworld"},
		{"with multiple control chars", "hello\n\r\t\x00world", "helloworld"},
		{"long string", strings.Repeat("a", 1500), strings.Repeat("a", 1000) + "...[truncated]"},
		{"exactly 1000 chars", strings.Repeat("a", 1000), strings.Repeat("a", 1000)},
		{"empty string", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := SanitizeForLog(tt.input)
			if result != tt.expected {
				t.Errorf("SanitizeForLog() = %q, want %q", result, tt.expected)
			}
		})
	}
}

// Benchmark tests
func BenchmarkValidateKey(b *testing.B) {
	key := "path/to/my/object.txt"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ValidateKey(key)
	}
}

func BenchmarkValidateKeyReference(b *testing.B) {
	keyRef := "s3:path/to/my/object.txt"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ValidateKeyReference(keyRef)
	}
}

func BenchmarkValidateBackendName(b *testing.B) {
	backend := "my-s3-backend"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ValidateBackendName(backend)
	}
}

func BenchmarkSanitizeForLog(b *testing.B) {
	input := "some log message with data"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = SanitizeForLog(input)
	}
}
