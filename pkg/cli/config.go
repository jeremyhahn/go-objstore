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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/viper"

	"github.com/jeremyhahn/go-enclave"
)

// Config holds the CLI configuration settings.
type Config struct {
	Backend       string
	BackendPath   string
	BackendBucket string
	BackendRegion string
	// BackendKey is the backend access key identifier.
	//
	// Deprecated: BackendKey holds credential material in an ordinary Go
	// string, which is immutable and therefore cannot be zeroized — it stays
	// readable in the heap until the garbage collector happens to reuse the
	// memory, and is pageable to swap and visible in a core dump throughout.
	// Use GetConfigProtected or SetBackendKeyEnclave, which hold the
	// credential in a locked, zeroized [enclave.MemoryEnclave]. This field
	// remains for compatibility and is removed in the next major version;
	// when both are set, the enclave wins.
	BackendKey string

	// BackendSecret is the backend secret access key.
	//
	// Deprecated: same reasoning as BackendKey. Use GetConfigProtected or
	// SetBackendSecretEnclave.
	BackendSecret string

	BackendURL     string
	OutputFormat   string
	Server         string // Server URL for remote operations (e.g., http://localhost:8080)
	ServerProtocol string // Server protocol: rest, grpc, or quic

	// Encryption settings
	EncryptionEnabled     bool
	EncryptionKeyID       string
	EncryptionBackend     string
	EncryptionBackendPath string
	EncryptionKMSPath     string

	// Archiver settings used by archive lifecycle policies in local mode.
	ArchiveVaultName string // AWS Glacier vault name (required for archive policies)
	ArchiveRegion    string // AWS region for the archiver (falls back to BackendRegion)

	// backendKeyEnclave and backendSecretEnclave hold the backend credentials
	// in locked, zeroized memory. They are unexported so the credentials can
	// only be reached through the enclave API, never copied out of the struct
	// by a consumer, and they take precedence over the deprecated plain
	// fields above wherever this package reads a credential.
	backendKeyEnclave    *enclave.MemoryEnclave
	backendSecretEnclave *enclave.MemoryEnclave
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
//
// The backend credentials are placed in the deprecated plain-string
// BackendKey and BackendSecret fields, where they cannot be zeroized. Prefer
// GetConfigProtected, which puts them in locked memory instead. This function
// remains for compatibility and keeps its current behavior.
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

		ArchiveVaultName: v.GetString("archive-vault-name"),
		ArchiveRegion:    v.GetString("archive-region"),
	}
}

// GetConfigProtected extracts the configuration from Viper into a Config
// struct, holding the backend credentials in locked, zeroized memory instead
// of in the deprecated plain-string fields, which are left empty.
//
// This is the protected replacement for GetConfig. The returned Config owns
// the enclaves; call [Config.Destroy] when it is no longer needed. Every
// reader in this package prefers the enclaves, so display and backend
// settings behave identically either way.
//
// What this protects, precisely: custody of the credential inside this
// package. It is NOT protection end to end, and callers should not treat it
// as such.
//
//   - Viper hands out credentials as Go strings, which cannot be wiped, so one
//     unprotected copy of each exists between v.GetString and the enclave.
//     That copy is unavoidable without owning the configuration loader.
//   - StorageSettings materializes each credential as a Go string again at the
//     backend Configure(map[string]string) boundary, and the backend and its
//     vendor SDK keep copies of those strings for their own lifetime.
//     [Config.Destroy] wipes the enclaves; it cannot reach those copies.
//     Closing that boundary requires a breaking change to the Storage
//     interface and is tracked as issue #16.
//
// Between those two boundaries the credential lives only in locked, zeroized
// memory, and is never held as a plain string in a struct for the process
// lifetime — which is what the deprecated BackendKey/BackendSecret fields did.
func GetConfigProtected(v *viper.Viper) (*Config, error) {
	cfg := GetConfig(v)

	// Both setters clear the plain field they read from.
	if err := cfg.SetBackendKeyFromString(cfg.BackendKey); err != nil {
		return nil, errors.Join(err, cfg.Destroy())
	}
	if err := cfg.SetBackendSecretFromString(cfg.BackendSecret); err != nil {
		return nil, errors.Join(err, cfg.Destroy())
	}

	return cfg, nil
}

// SetBackendKeyEnclave sets the backend access key from a locked memory
// enclave and clears the deprecated plain field. The Config TAKES OWNERSHIP of
// the enclave; any previously held one is destroyed. e is installed whatever
// the result: the error is the replaced enclave's release report.
func (c *Config) SetBackendKeyEnclave(e *enclave.MemoryEnclave) error {
	var err error
	if c.backendKeyEnclave != nil && c.backendKeyEnclave != e {
		err = c.backendKeyEnclave.Destroy()
	}
	c.backendKeyEnclave = e
	c.BackendKey = ""
	return err
}

// SetBackendSecretEnclave sets the backend secret access key from a locked
// memory enclave and clears the deprecated plain field. The Config TAKES
// OWNERSHIP of the enclave; any previously held one is destroyed. e is
// installed whatever the result: the error is the replaced enclave's release
// report.
func (c *Config) SetBackendSecretEnclave(e *enclave.MemoryEnclave) error {
	var err error
	if c.backendSecretEnclave != nil && c.backendSecretEnclave != e {
		err = c.backendSecretEnclave.Destroy()
	}
	c.backendSecretEnclave = e
	c.BackendSecret = ""
	return err
}

// SetBackendKeyFromString enclaves key, or clears the credential when key is
// empty. The string itself is immutable and cannot be wiped, so the caller's
// copy of it stays in ordinary memory; only the enclave's copy is protected.
func (c *Config) SetBackendKeyFromString(key string) error {
	if key == "" {
		return c.SetBackendKeyEnclave(nil)
	}
	e, err := enclave.NewMemoryEnclave([]byte(key))
	if err != nil {
		return err
	}
	return c.SetBackendKeyEnclave(e)
}

// SetBackendSecretFromString enclaves secret, or clears the credential when
// secret is empty. The same caveat on string sources applies as for
// SetBackendKeyFromString.
func (c *Config) SetBackendSecretFromString(secret string) error {
	if secret == "" {
		return c.SetBackendSecretEnclave(nil)
	}
	e, err := enclave.NewMemoryEnclave([]byte(secret))
	if err != nil {
		return err
	}
	return c.SetBackendSecretEnclave(e)
}

// BackendKeyEnclave returns the enclave holding the backend access key, or
// nil when the credential came from the deprecated plain field.
func (c *Config) BackendKeyEnclave() *enclave.MemoryEnclave {
	return c.backendKeyEnclave
}

// BackendSecretEnclave returns the enclave holding the backend secret access
// key, or nil when the credential came from the deprecated plain field.
func (c *Config) BackendSecretEnclave() *enclave.MemoryEnclave {
	return c.backendSecretEnclave
}

// Destroy zeroizes the credentials this Config owns. It is idempotent, safe on
// a nil *Config, and safe to defer. The deprecated plain-string fields are
// cleared as well, though a Go string cannot actually be wiped — that is
// precisely why they are deprecated.
//
// The error joins both enclaves' release reports. The material is wiped
// whatever they say, and a discarded error loses nothing: the next
// [enclave.WipeAll] retries a refused release and reports a corrupt enclave.
func (c *Config) Destroy() error {
	if c == nil {
		return nil
	}
	keyErr := c.backendKeyEnclave.Destroy()
	c.backendKeyEnclave = nil
	secretErr := c.backendSecretEnclave.Destroy()
	c.backendSecretEnclave = nil
	c.BackendKey = ""
	c.BackendSecret = ""
	return errors.Join(keyErr, secretErr)
}

// backendKeyValue materializes the backend access key as a plain string.
//
// This is the boundary with backend Configure, which takes a
// map[string]string; a plain copy is unavoidable there. Call it as late as
// possible and never store the result.
//
// Once an enclave has been selected for this credential it is the ONLY source:
// a destroyed enclave reports [enclave.ErrEnclaveDestroyed] rather than
// falling back to the deprecated plain field. An enclave can be destroyed
// independently of this call — a rotation or a Config teardown elsewhere — so
// authenticating with whatever stale plaintext happened to be left in
// BackendKey at that moment would be a fail-open. The deprecated field is read
// only when no enclave was ever set.
func (c *Config) backendKeyValue() (string, error) {
	if c.backendKeyEnclave != nil {
		return enclaveString(c.backendKeyEnclave)
	}
	return c.BackendKey, nil
}

// backendSecretValue materializes the backend secret access key as a plain
// string. Same boundary caveat and same no-fallback rule as backendKeyValue.
func (c *Config) backendSecretValue() (string, error) {
	if c.backendSecretEnclave != nil {
		return enclaveString(c.backendSecretEnclave)
	}
	return c.BackendSecret, nil
}

// enclaveString copies the enclave's material out as a string, or reports
// [enclave.ErrEnclaveDestroyed]. The copy is taken inside the borrow, so a
// concurrent Destroy waits rather than unmapping the pages mid-read.
func enclaveString(e *enclave.MemoryEnclave) (string, error) {
	var s string
	if err := e.WithBytes(func(b []byte) error {
		s = string(b)
		return nil
	}); err != nil {
		return "", err
	}
	return s, nil
}

// hasBackendKey reports whether a backend access key is configured, without
// materializing it. An enclave that has been destroyed still counts as
// configured, so that reading it reports the failure instead of silently
// selecting the deprecated plain field.
func (c *Config) hasBackendKey() bool {
	return c.backendKeyEnclave != nil || c.BackendKey != ""
}

// hasBackendSecret reports whether a backend secret access key is configured,
// without materializing it. Same rule as hasBackendKey.
func (c *Config) hasBackendSecret() bool {
	return c.backendSecretEnclave != nil || c.BackendSecret != ""
}

// maskedBackendKey renders the backend access key for display.
func (c *Config) maskedBackendKey() string {
	if c.backendKeyEnclave != nil {
		return maskEnclave(c.backendKeyEnclave)
	}
	return maskSecret(c.BackendKey)
}

// maskedBackendSecret renders the backend secret access key for display.
func (c *Config) maskedBackendSecret() string {
	if c.backendSecretEnclave != nil {
		return maskEnclave(c.backendSecretEnclave)
	}
	return maskSecret(c.BackendSecret)
}

// StorageSettings converts Config to a storage backend settings map.
//
// It is the protected replacement for GetStorageSettings: when a credential
// was placed in an enclave that has since been destroyed it returns
// [enclave.ErrEnclaveDestroyed] instead of authenticating with whatever stale
// value the deprecated plain field happens to hold.
//
// The returned map holds the credentials as ordinary Go strings, because the
// backend Configure boundary takes a map[string]string. Those strings cannot
// be wiped; build the map as late as possible, hand it straight to the
// backend, and do not retain it. Closing that boundary needs the Storage
// interface to change and is tracked separately.
func (c *Config) StorageSettings() (map[string]string, error) {
	settings := c.storageSettingsWithoutCredentials()

	if c.hasBackendKey() {
		v, err := c.backendKeyValue()
		if err != nil {
			return nil, &CredentialUnavailableError{Credential: "backend access key", Err: err}
		}
		settings["access_key_id"] = v
	}
	if c.hasBackendSecret() {
		v, err := c.backendSecretValue()
		if err != nil {
			return nil, &CredentialUnavailableError{Credential: "backend secret access key", Err: err}
		}
		settings["secret_access_key"] = v
	}

	return settings, nil
}

// GetStorageSettings converts Config to storage backend settings map.
//
// Deprecated: this signature cannot report a destroyed enclave. It fails
// CLOSED — a credential whose enclave has been destroyed is omitted from the
// map, so the backend fails to authenticate — but the caller never learns
// why. Use StorageSettings, which returns the error.
func (c *Config) GetStorageSettings() map[string]string {
	settings := c.storageSettingsWithoutCredentials()

	// An unreadable credential is omitted, never replaced by the deprecated
	// plain field: falling back would authenticate with stale unprotected
	// material after the protected path was chosen.
	if c.hasBackendKey() {
		if v, err := c.backendKeyValue(); err == nil {
			settings["access_key_id"] = v
		}
	}
	if c.hasBackendSecret() {
		if v, err := c.backendSecretValue(); err == nil {
			settings["secret_access_key"] = v
		}
	}

	return settings
}

// storageSettingsWithoutCredentials builds every backend setting that is not a
// credential. The two callers differ only in how they report a credential they
// cannot read, so the rest is shared here.
func (c *Config) storageSettingsWithoutCredentials() map[string]string {
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
	if c.EncryptionKMSPath != "" {
		settings["encryption_kms_path"] = c.EncryptionKMSPath
	}

	// For local backend, use persistent lifecycle manager so policies survive across CLI commands
	//nolint:goconst // Using literal for clarity in configuration
	if c.Backend == "local" {
		settings["lifecycleManagerType"] = "persistent"
		settings["lifecyclePolicyFile"] = ".lifecycle-policies.json"
	}

	return settings
}

// GetArchiverSettings returns the settings used to configure the AWS Glacier
// archiver for archive lifecycle policies. The dedicated archive settings
// (archive-vault-name, archive-region) take precedence; when no archive
// region is configured the storage backend region is used instead.
func (c *Config) GetArchiverSettings() map[string]string {
	settings := make(map[string]string)
	if c.ArchiveVaultName != "" {
		settings["vaultName"] = c.ArchiveVaultName
	}
	region := c.ArchiveRegion
	if region == "" {
		region = c.BackendRegion
	}
	if region != "" {
		settings["region"] = region
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
	if cfg.hasBackendKey() {
		result += fmt.Sprintf("Backend Key: %s\n", cfg.maskedBackendKey())
	}
	if cfg.hasBackendSecret() {
		result += fmt.Sprintf("Backend Secret: %s\n", cfg.maskedBackendSecret())
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
	if cfg.hasBackendKey() {
		result += fmt.Sprintf("│ %-16s │ %-38s │\n", "Backend Key", cfg.maskedBackendKey())
	}
	if cfg.hasBackendSecret() {
		result += fmt.Sprintf("│ %-16s │ %-38s │\n", "Backend Secret", cfg.maskedBackendSecret())
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
	if cfg.hasBackendKey() {
		result += fmt.Sprintf("  \"backend_key\": %q,\n", cfg.maskedBackendKey())
	}
	if cfg.hasBackendSecret() {
		result += fmt.Sprintf("  \"backend_secret\": %q,\n", cfg.maskedBackendSecret())
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

// maskEnclave masks material held in a locked enclave for display. Only the
// leading bytes maskSecret would reveal are copied out, so displaying a
// credential never materializes the whole of it in unprotected memory. The
// copy is taken through WithBytes, so a concurrent Destroy waits rather than
// unmapping the pages mid-read. A destroyed enclave masks completely.
func maskEnclave(e *enclave.MemoryEnclave) string {
	masked := "****"
	if err := e.WithBytes(func(b []byte) error {
		if len(b) >= 5 {
			masked = string(b[:4]) + "****"
		}
		return nil
	}); err != nil {
		return "****"
	}
	return masked
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
