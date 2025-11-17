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

package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/viper"
)

func TestInitConfig(t *testing.T) {
	t.Run("with no config file", func(t *testing.T) {
		v, err := InitConfig("")
		if err != nil {
			t.Fatalf("InitConfig failed: %v", err)
		}
		if v == nil {
			t.Fatal("Expected viper instance, got nil")
		}

		// Check defaults
		if v.GetString("backend") != "local" {
			t.Errorf("Expected default backend 'local', got %s", v.GetString("backend"))
		}
		if v.GetString("backend-path") != "./storage" {
			t.Errorf("Expected default path './storage', got %s", v.GetString("backend-path"))
		}
		if v.GetString("output-format") != "text" {
			t.Errorf("Expected default format 'text', got %s", v.GetString("output-format"))
		}
	})

	t.Run("with config file", func(t *testing.T) {
		// Create a temporary config file
		tmpDir := t.TempDir()
		configPath := filepath.Join(tmpDir, ".objstore.yaml")
		configContent := `backend: s3
backend-bucket: test-bucket
backend-region: us-west-2
output-format: json
`
		if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
			t.Fatalf("Failed to write config file: %v", err)
		}

		v, err := InitConfig(configPath)
		if err != nil {
			t.Fatalf("InitConfig failed: %v", err)
		}

		if v.GetString("backend") != "s3" {
			t.Errorf("Expected backend 's3', got %s", v.GetString("backend"))
		}
		if v.GetString("backend-bucket") != "test-bucket" {
			t.Errorf("Expected bucket 'test-bucket', got %s", v.GetString("backend-bucket"))
		}
		if v.GetString("backend-region") != "us-west-2" {
			t.Errorf("Expected region 'us-west-2', got %s", v.GetString("backend-region"))
		}
		if v.GetString("output-format") != "json" {
			t.Errorf("Expected format 'json', got %s", v.GetString("output-format"))
		}
	})

	t.Run("with invalid config file", func(t *testing.T) {
		// Create a temporary invalid config file
		tmpDir := t.TempDir()
		configPath := filepath.Join(tmpDir, ".objstore.yaml")
		configContent := `backend: s3
invalid yaml content: [
`
		if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
			t.Fatalf("Failed to write config file: %v", err)
		}

		_, err := InitConfig(configPath)
		if err == nil {
			t.Error("Expected error for invalid config file, got nil")
		}
	})

	t.Run("with environment variables", func(t *testing.T) {
		os.Setenv("OBJECTSTORE_BACKEND", "azure")
		os.Setenv("OBJECTSTORE_BACKEND_BUCKET", "env-bucket")
		defer func() {
			os.Unsetenv("OBJECTSTORE_BACKEND")
			os.Unsetenv("OBJECTSTORE_BACKEND_BUCKET")
		}()

		v, err := InitConfig("")
		if err != nil {
			t.Fatalf("InitConfig failed: %v", err)
		}

		if v.GetString("backend") != "azure" {
			t.Errorf("Expected backend 'azure' from env, got %s", v.GetString("backend"))
		}
		// Viper uses underscores in env var names but dashes in config keys
		// So OBJECTSTORE_BACKEND_BUCKET maps to backend-bucket
		bucket := v.GetString("backend-bucket")
		if bucket == "" {
			// Try with underscore as fallback
			bucket = v.GetString("backend_bucket")
		}
		if bucket != "env-bucket" {
			t.Errorf("Expected bucket 'env-bucket' from env, got %s", bucket)
		}
	})
}

func TestGetConfig(t *testing.T) {
	v := viper.New()
	v.Set("backend", "s3")
	v.Set("backend-path", "/data")
	v.Set("backend-bucket", "my-bucket")
	v.Set("backend-region", "us-east-1")
	v.Set("backend-key", "AKIAIOSFODNN7EXAMPLE")
	v.Set("backend-secret", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
	v.Set("backend-url", "https://s3.amazonaws.com")
	v.Set("output-format", "json")

	cfg := GetConfig(v)

	if cfg.Backend != "s3" {
		t.Errorf("Expected backend 's3', got %s", cfg.Backend)
	}
	if cfg.BackendPath != "/data" {
		t.Errorf("Expected path '/data', got %s", cfg.BackendPath)
	}
	if cfg.BackendBucket != "my-bucket" {
		t.Errorf("Expected bucket 'my-bucket', got %s", cfg.BackendBucket)
	}
	if cfg.BackendRegion != "us-east-1" {
		t.Errorf("Expected region 'us-east-1', got %s", cfg.BackendRegion)
	}
	if cfg.BackendKey != "AKIAIOSFODNN7EXAMPLE" {
		t.Errorf("Expected key, got %s", cfg.BackendKey)
	}
	if cfg.BackendSecret != "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" {
		t.Errorf("Expected secret, got %s", cfg.BackendSecret)
	}
	if cfg.BackendURL != "https://s3.amazonaws.com" {
		t.Errorf("Expected URL 'https://s3.amazonaws.com', got %s", cfg.BackendURL)
	}
	if cfg.OutputFormat != "json" {
		t.Errorf("Expected format 'json', got %s", cfg.OutputFormat)
	}
}

func TestGetStorageSettings(t *testing.T) {
	t.Run("local backend", func(t *testing.T) {
		cfg := &Config{
			Backend:     "local",
			BackendPath: "/tmp/storage",
		}

		settings := cfg.GetStorageSettings()
		if settings["path"] != "/tmp/storage" {
			t.Errorf("Expected path '/tmp/storage', got %s", settings["path"])
		}
	})

	t.Run("s3 backend", func(t *testing.T) {
		cfg := &Config{
			Backend:       "s3",
			BackendBucket: "my-bucket",
			BackendRegion: "us-west-2",
			BackendKey:    "test-key",
			BackendSecret: "test-secret",
		}

		settings := cfg.GetStorageSettings()
		if settings["bucket"] != "my-bucket" {
			t.Errorf("Expected bucket 'my-bucket', got %s", settings["bucket"])
		}
		if settings["region"] != "us-west-2" {
			t.Errorf("Expected region 'us-west-2', got %s", settings["region"])
		}
		if settings["access_key_id"] != "test-key" {
			t.Errorf("Expected key, got %s", settings["access_key_id"])
		}
		if settings["secret_access_key"] != "test-secret" {
			t.Errorf("Expected secret, got %s", settings["secret_access_key"])
		}
	})

	t.Run("with custom endpoint", func(t *testing.T) {
		cfg := &Config{
			Backend:       "s3",
			BackendBucket: "my-bucket",
			BackendRegion: "us-west-2",
			BackendURL:    "http://localhost:9000",
		}

		settings := cfg.GetStorageSettings()
		if settings["endpoint"] != "http://localhost:9000" {
			t.Errorf("Expected endpoint 'http://localhost:9000', got %s", settings["endpoint"])
		}
	})

	t.Run("empty values are omitted", func(t *testing.T) {
		cfg := &Config{
			Backend:     "local",
			BackendPath: "/tmp/storage",
		}

		settings := cfg.GetStorageSettings()
		if _, exists := settings["bucket"]; exists {
			t.Error("Expected bucket to be omitted")
		}
		if _, exists := settings["region"]; exists {
			t.Error("Expected region to be omitted")
		}
	})
}

func TestDisplayConfig(t *testing.T) {
	cfg := &Config{
		Backend:       "s3",
		BackendBucket: "my-bucket",
		BackendRegion: "us-west-2",
		BackendKey:    "AKIAIOSFODNN7EXAMPLE",
		BackendSecret: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
		OutputFormat:  "text",
	}

	t.Run("text format", func(t *testing.T) {
		output := DisplayConfig(cfg, "text")
		if !strings.Contains(output, "Backend: s3") {
			t.Error("Text output missing backend")
		}
		if !strings.Contains(output, "Backend Bucket: my-bucket") {
			t.Error("Text output missing bucket")
		}
		if !strings.Contains(output, "AKIA****") {
			t.Error("Text output should mask key")
		}
		if strings.Contains(output, "AKIAIOSFODNN7EXAMPLE") {
			t.Error("Text output should not show full key")
		}
	})

	t.Run("json format", func(t *testing.T) {
		output := DisplayConfig(cfg, "json")
		if !strings.Contains(output, `"backend": "s3"`) {
			t.Error("JSON output missing backend")
		}
		if !strings.Contains(output, `"backend_bucket": "my-bucket"`) {
			t.Error("JSON output missing bucket")
		}
		if !strings.Contains(output, "AKIA****") {
			t.Error("JSON output should mask key")
		}
	})

	t.Run("table format", func(t *testing.T) {
		output := DisplayConfig(cfg, "table")
		if !strings.Contains(output, "Backend") {
			t.Error("Table output missing backend header")
		}
		if !strings.Contains(output, "s3") {
			t.Error("Table output missing backend value")
		}
		if !strings.Contains(output, "my-bucket") {
			t.Error("Table output missing bucket")
		}
	})
}

func TestMaskSecret(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"", "****"},
		{"a", "****"},
		{"ab", "****"},
		{"abc", "****"},
		{"test", "****"},
		{"12345", "1234****"},
		{"AKIAIOSFODNN7EXAMPLE", "AKIA****"},
		{"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "wJal****"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := maskSecret(tt.input)
			if result != tt.expected {
				t.Errorf("maskSecret(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestTruncate(t *testing.T) {
	tests := []struct {
		input    string
		maxLen   int
		expected string
	}{
		{"short", 10, "short"},
		{"exactly ten", 11, "exactly ten"},
		{"this is a very long string", 10, "this is..."},
		{"test", 5, "test"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := truncate(tt.input, tt.maxLen)
			if result != tt.expected {
				t.Errorf("truncate(%q, %d) = %q, want %q", tt.input, tt.maxLen, result, tt.expected)
			}
		})
	}
}

func TestValidateConfig(t *testing.T) {
	t.Run("valid local backend", func(t *testing.T) {
		cfg := &Config{
			Backend:      "local",
			BackendPath:  "/tmp/storage",
			OutputFormat: "text",
		}
		if err := ValidateConfig(cfg); err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
	})

	t.Run("local backend without path", func(t *testing.T) {
		cfg := &Config{
			Backend:      "local",
			OutputFormat: "text",
		}
		if err := ValidateConfig(cfg); err == nil {
			t.Error("Expected error for missing path")
		}
	})

	t.Run("valid s3 backend", func(t *testing.T) {
		cfg := &Config{
			Backend:       "s3",
			BackendBucket: "my-bucket",
			BackendRegion: "us-west-2",
			OutputFormat:  "json",
		}
		if err := ValidateConfig(cfg); err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
	})

	t.Run("s3 backend without bucket", func(t *testing.T) {
		cfg := &Config{
			Backend:       "s3",
			BackendRegion: "us-west-2",
			OutputFormat:  "text",
		}
		if err := ValidateConfig(cfg); err == nil {
			t.Error("Expected error for missing bucket")
		}
	})

	t.Run("s3 backend without region", func(t *testing.T) {
		cfg := &Config{
			Backend:       "s3",
			BackendBucket: "my-bucket",
			OutputFormat:  "text",
		}
		if err := ValidateConfig(cfg); err == nil {
			t.Error("Expected error for missing region")
		}
	})

	t.Run("valid gcs backend", func(t *testing.T) {
		cfg := &Config{
			Backend:       "gcs",
			BackendBucket: "my-bucket",
			OutputFormat:  "table",
		}
		if err := ValidateConfig(cfg); err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
	})

	t.Run("gcs backend without bucket", func(t *testing.T) {
		cfg := &Config{
			Backend:      "gcs",
			OutputFormat: "text",
		}
		if err := ValidateConfig(cfg); err == nil {
			t.Error("Expected error for missing bucket")
		}
	})

	t.Run("valid azure backend", func(t *testing.T) {
		cfg := &Config{
			Backend:       "azure",
			BackendBucket: "my-container",
			OutputFormat:  "json",
		}
		if err := ValidateConfig(cfg); err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
	})

	t.Run("azure backend without bucket", func(t *testing.T) {
		cfg := &Config{
			Backend:      "azure",
			OutputFormat: "text",
		}
		if err := ValidateConfig(cfg); err == nil {
			t.Error("Expected error for missing bucket")
		}
	})

	t.Run("unsupported backend", func(t *testing.T) {
		cfg := &Config{
			Backend:      "glacier",
			OutputFormat: "text",
		}
		if err := ValidateConfig(cfg); err == nil {
			t.Error("Expected error for unsupported backend")
		}
	})

	t.Run("invalid output format", func(t *testing.T) {
		cfg := &Config{
			Backend:      "local",
			BackendPath:  "/tmp/storage",
			OutputFormat: "xml",
		}
		if err := ValidateConfig(cfg); err == nil {
			t.Error("Expected error for invalid output format")
		}
	})

	t.Run("home directory expansion", func(t *testing.T) {
		cfg := &Config{
			Backend:      "local",
			BackendPath:  "~/storage",
			OutputFormat: "text",
		}
		if err := ValidateConfig(cfg); err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
		// Check that path was expanded
		if strings.HasPrefix(cfg.BackendPath, "~") {
			t.Error("Expected path to be expanded, still contains ~")
		}
	})
}
