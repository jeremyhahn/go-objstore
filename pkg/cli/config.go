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
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/viper"
)

// Config holds the CLI configuration settings.
type Config struct {
	Backend        string
	BackendPath    string
	BackendBucket  string
	BackendRegion  string
	BackendKey     string
	BackendSecret  string
	BackendURL     string
	OutputFormat   string
	Server         string // Server URL for remote operations (e.g., http://localhost:8080)
	ServerProtocol string // Server protocol: rest, grpc, or quic

	// Encryption settings
	EncryptionEnabled      bool
	EncryptionKeyID        string
	EncryptionBackend      string
	EncryptionBackendPath  string
	EncryptionKeychainPath string
}

// InitConfig initializes the configuration using Viper.
// Configuration priority: flags > env vars > config file > defaults.
func InitConfig(cfgFile string) (*viper.Viper, error) {
	v := viper.New()

	// Set defaults
	v.SetDefault("backend", "local")
	v.SetDefault("backend-path", "./storage")
	v.SetDefault("output-format", "text")

	// Set config file search paths
	if cfgFile != "" {
		// Use config file from the flag if provided
		v.SetConfigFile(cfgFile)
	} else {
		// Search for config in home directory and current directory
		home, err := os.UserHomeDir()
		if err == nil {
			v.AddConfigPath(home)
		}
		v.AddConfigPath(".")
		v.SetConfigName(".objstore")
		v.SetConfigType("yaml")
	}

	// Bind environment variables
	v.SetEnvPrefix("OBJECTSTORE")
	v.AutomaticEnv()

	// Read config file if it exists
	if err := v.ReadInConfig(); err != nil {
		// It's okay if config file doesn't exist
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, err
		}
	}

	return v, nil
}

// GetConfig extracts the configuration from Viper into a Config struct.
func GetConfig(v *viper.Viper) *Config {
	return &Config{
		Backend:        v.GetString("backend"),
		BackendPath:    v.GetString("backend-path"),
		BackendBucket:  v.GetString("backend-bucket"),
		BackendRegion:  v.GetString("backend-region"),
		BackendKey:     v.GetString("backend-key"),
		BackendSecret:  v.GetString("backend-secret"),
		BackendURL:     v.GetString("backend-url"),
		OutputFormat:   v.GetString("output-format"),
		Server:         v.GetString("server"),
		ServerProtocol: v.GetString("server-protocol"),
	}
}

// GetStorageSettings converts Config to storage backend settings map.
func (c *Config) GetStorageSettings() map[string]string {
	settings := make(map[string]string)

	// Add non-empty settings
	if c.BackendPath != "" {
		settings["path"] = c.BackendPath
	}
	if c.BackendBucket != "" {
		settings["bucket"] = c.BackendBucket
	}
	if c.BackendRegion != "" {
		settings["region"] = c.BackendRegion
	}
	if c.BackendKey != "" {
		settings["access_key_id"] = c.BackendKey
	}
	if c.BackendSecret != "" {
		settings["secret_access_key"] = c.BackendSecret
	}
	if c.BackendURL != "" {
		settings["endpoint"] = c.BackendURL
	}

	// Add encryption settings
	if c.EncryptionEnabled {
		settings["encryption_enabled"] = "true"
	}
	if c.EncryptionKeyID != "" {
		settings["encryption_key_id"] = c.EncryptionKeyID
	}
	if c.EncryptionBackend != "" {
		settings["encryption_backend"] = c.EncryptionBackend
	}
	if c.EncryptionBackendPath != "" {
		settings["encryption_backend_path"] = c.EncryptionBackendPath
	}
	if c.EncryptionKeychainPath != "" {
		settings["encryption_keychain_path"] = c.EncryptionKeychainPath
	}

	// For local backend, use persistent lifecycle manager so policies survive across CLI commands
	//nolint:goconst // Using literal for clarity in configuration
	if c.Backend == "local" {
		settings["lifecycleManagerType"] = "persistent"
		settings["lifecyclePolicyFile"] = ".lifecycle-policies.json"
	}

	return settings
}

// DisplayConfig formats and displays the current configuration.
func DisplayConfig(cfg *Config, format string) string {
	switch format {
	case string(FormatJSON):
		return formatConfigJSON(cfg)
	case "table":
		return formatConfigTable(cfg)
	default:
		return formatConfigText(cfg)
	}
}

func formatConfigText(cfg *Config) string {
	var result string
	result += fmt.Sprintf("Backend: %s\n", cfg.Backend)
	if cfg.BackendPath != "" {
		result += fmt.Sprintf("Backend Path: %s\n", cfg.BackendPath)
	}
	if cfg.BackendBucket != "" {
		result += fmt.Sprintf("Backend Bucket: %s\n", cfg.BackendBucket)
	}
	if cfg.BackendRegion != "" {
		result += fmt.Sprintf("Backend Region: %s\n", cfg.BackendRegion)
	}
	if cfg.BackendURL != "" {
		result += fmt.Sprintf("Backend URL: %s\n", cfg.BackendURL)
	}
	if cfg.BackendKey != "" {
		result += fmt.Sprintf("Backend Key: %s\n", maskSecret(cfg.BackendKey))
	}
	if cfg.BackendSecret != "" {
		result += fmt.Sprintf("Backend Secret: %s\n", maskSecret(cfg.BackendSecret))
	}
	result += fmt.Sprintf("Output Format: %s\n", cfg.OutputFormat)
	return result
}

func formatConfigTable(cfg *Config) string {
	var result string
	result += "┌──────────────────┬────────────────────────────────────────┐\n"
	result += "│ Setting          │ Value                                  │\n"
	result += "├──────────────────┼────────────────────────────────────────┤\n"
	result += fmt.Sprintf("│ %-16s │ %-38s │\n", "Backend", cfg.Backend)
	if cfg.BackendPath != "" {
		result += fmt.Sprintf("│ %-16s │ %-38s │\n", "Backend Path", truncate(cfg.BackendPath, 38))
	}
	if cfg.BackendBucket != "" {
		result += fmt.Sprintf("│ %-16s │ %-38s │\n", "Backend Bucket", truncate(cfg.BackendBucket, 38))
	}
	if cfg.BackendRegion != "" {
		result += fmt.Sprintf("│ %-16s │ %-38s │\n", "Backend Region", cfg.BackendRegion)
	}
	if cfg.BackendURL != "" {
		result += fmt.Sprintf("│ %-16s │ %-38s │\n", "Backend URL", truncate(cfg.BackendURL, 38))
	}
	if cfg.BackendKey != "" {
		result += fmt.Sprintf("│ %-16s │ %-38s │\n", "Backend Key", maskSecret(cfg.BackendKey))
	}
	if cfg.BackendSecret != "" {
		result += fmt.Sprintf("│ %-16s │ %-38s │\n", "Backend Secret", maskSecret(cfg.BackendSecret))
	}
	result += fmt.Sprintf("│ %-16s │ %-38s │\n", "Output Format", cfg.OutputFormat)
	result += "└──────────────────┴────────────────────────────────────────┘\n"
	return result
}

func formatConfigJSON(cfg *Config) string {
	result := "{\n"
	result += fmt.Sprintf("  \"backend\": %q,\n", cfg.Backend)
	if cfg.BackendPath != "" {
		result += fmt.Sprintf("  \"backend_path\": %q,\n", cfg.BackendPath)
	}
	if cfg.BackendBucket != "" {
		result += fmt.Sprintf("  \"backend_bucket\": %q,\n", cfg.BackendBucket)
	}
	if cfg.BackendRegion != "" {
		result += fmt.Sprintf("  \"backend_region\": %q,\n", cfg.BackendRegion)
	}
	if cfg.BackendURL != "" {
		result += fmt.Sprintf("  \"backend_url\": %q,\n", cfg.BackendURL)
	}
	if cfg.BackendKey != "" {
		result += fmt.Sprintf("  \"backend_key\": %q,\n", maskSecret(cfg.BackendKey))
	}
	if cfg.BackendSecret != "" {
		result += fmt.Sprintf("  \"backend_secret\": %q,\n", maskSecret(cfg.BackendSecret))
	}
	result += fmt.Sprintf("  \"output_format\": %q\n", cfg.OutputFormat)
	result += "}\n"
	return result
}

// maskSecret masks sensitive information, showing only first 4 characters.
func maskSecret(s string) string {
	if len(s) < 5 {
		return "****"
	}
	return s[:4] + "****"
}

// truncate truncates a string to maxLen characters.
func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}

// ValidateConfig validates the configuration for the given backend.
func ValidateConfig(cfg *Config) error {
	switch cfg.Backend {
	case "local":
		if cfg.BackendPath == "" {
			return ErrBackendPathRequired
		}
		// Expand path if it contains ~
		if strings.HasPrefix(cfg.BackendPath, "~") {
			home, err := os.UserHomeDir()
			if err != nil {
				return err
			}
			cfg.BackendPath = filepath.Join(home, cfg.BackendPath[1:])
		}
	case "s3":
		if cfg.BackendBucket == "" {
			return ErrBackendBucketRequired
		}
		if cfg.BackendRegion == "" {
			return ErrBackendRegionRequired
		}
	case "minio":
		if cfg.BackendBucket == "" {
			return ErrBackendBucketRequired
		}
		if cfg.BackendURL == "" {
			return ErrBackendURLRequired
		}
	case "gcs":
		if cfg.BackendBucket == "" {
			return ErrBackendBucketRequired
		}
	case "azure":
		if cfg.BackendBucket == "" {
			return ErrBackendBucketRequired
		}
	default:
		return ErrUnsupportedBackend
	}

	// Validate output format
	if cfg.OutputFormat != "text" && cfg.OutputFormat != "json" && cfg.OutputFormat != "table" {
		return ErrUnsupportedOutputFormat
	}

	return nil
}
