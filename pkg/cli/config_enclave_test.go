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
	"strings"
	"testing"

	"github.com/spf13/viper"

	"github.com/jeremyhahn/go-objstore/pkg/enclave"
)

const (
	testAccessKey = "AKIAIOSFODNN7EXAMPLE"
	testSecretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
)

func protectedTestViper() *viper.Viper {
	v := viper.New()
	v.Set("backend", "s3")
	v.Set("backend-bucket", "test-bucket")
	v.Set("backend-region", "us-east-1")
	v.Set("backend-key", testAccessKey)
	v.Set("backend-secret", testSecretKey)
	v.Set("output-format", "text")
	return v
}

func TestGetConfigProtected(t *testing.T) {
	cfg, err := GetConfigProtected(protectedTestViper())
	if err != nil {
		t.Fatalf("GetConfigProtected: %v", err)
	}
	defer cfg.Destroy()

	if cfg.BackendKey != "" {
		t.Errorf("deprecated BackendKey = %q, want empty", cfg.BackendKey)
	}
	if cfg.BackendSecret != "" {
		t.Errorf("deprecated BackendSecret = %q, want empty", cfg.BackendSecret)
	}
	if !cfg.BackendKeyEnclave().EqualTo([]byte(testAccessKey)) {
		t.Error("access key not held in the enclave")
	}
	if !cfg.BackendSecretEnclave().EqualTo([]byte(testSecretKey)) {
		t.Error("secret key not held in the enclave")
	}
	// Non-credential settings are untouched.
	if cfg.Backend != "s3" || cfg.BackendBucket != "test-bucket" {
		t.Errorf("non-credential settings altered: %+v", cfg)
	}
}

func TestGetConfigProtected_NoCredentials(t *testing.T) {
	v := viper.New()
	v.Set("backend", "local")
	v.Set("backend-path", "./storage")
	v.Set("output-format", "text")

	cfg, err := GetConfigProtected(v)
	if err != nil {
		t.Fatalf("GetConfigProtected: %v", err)
	}
	defer cfg.Destroy()

	if cfg.BackendKeyEnclave() != nil || cfg.BackendSecretEnclave() != nil {
		t.Error("enclaves allocated for absent credentials")
	}
	if len(cfg.GetStorageSettings()) == 0 {
		t.Error("expected storage settings for the local backend")
	}
	if _, ok := cfg.GetStorageSettings()["access_key_id"]; ok {
		t.Error("access_key_id present without a credential")
	}
}

func TestGetStorageSettings_FromEnclaves(t *testing.T) {
	cfg, err := GetConfigProtected(protectedTestViper())
	if err != nil {
		t.Fatalf("GetConfigProtected: %v", err)
	}
	defer cfg.Destroy()

	settings := cfg.GetStorageSettings()
	if settings["access_key_id"] != testAccessKey {
		t.Errorf("access_key_id = %q, want %q", settings["access_key_id"], testAccessKey)
	}
	if settings["secret_access_key"] != testSecretKey {
		t.Errorf("secret_access_key = %q, want %q", settings["secret_access_key"], testSecretKey)
	}
}

func TestGetStorageSettings_DeprecatedFieldsStillWork(t *testing.T) {
	cfg := &Config{
		Backend:       "s3",
		BackendKey:    testAccessKey,
		BackendSecret: testSecretKey,
	}

	settings := cfg.GetStorageSettings()
	if settings["access_key_id"] != testAccessKey {
		t.Errorf("access_key_id = %q, want %q", settings["access_key_id"], testAccessKey)
	}
	if settings["secret_access_key"] != testSecretKey {
		t.Errorf("secret_access_key = %q, want %q", settings["secret_access_key"], testSecretKey)
	}
}

func TestSetBackendCredentialsFromString(t *testing.T) {
	cfg := &Config{BackendKey: "stale-key", BackendSecret: "stale-secret"}

	if err := cfg.SetBackendKeyFromString(testAccessKey); err != nil {
		t.Fatalf("SetBackendKeyFromString: %v", err)
	}
	if err := cfg.SetBackendSecretFromString(testSecretKey); err != nil {
		t.Fatalf("SetBackendSecretFromString: %v", err)
	}
	defer cfg.Destroy()

	if cfg.BackendKey != "" || cfg.BackendSecret != "" {
		t.Error("deprecated fields not cleared by the protected setters")
	}
	if !cfg.BackendKeyEnclave().EqualTo([]byte(testAccessKey)) {
		t.Error("access key not enclaved")
	}
	if !cfg.BackendSecretEnclave().EqualTo([]byte(testSecretKey)) {
		t.Error("secret key not enclaved")
	}

	// The enclave takes precedence over a value written to the deprecated
	// field afterwards.
	cfg.BackendKey = "should-be-ignored"
	if got := cfg.GetStorageSettings()["access_key_id"]; got != testAccessKey {
		t.Errorf("access_key_id = %q, want the enclaved key", got)
	}
}

func TestSetBackendCredentialsFromString_Empty(t *testing.T) {
	cfg := &Config{}
	if err := cfg.SetBackendKeyFromString(testAccessKey); err != nil {
		t.Fatalf("SetBackendKeyFromString: %v", err)
	}
	previous := cfg.BackendKeyEnclave()

	if err := cfg.SetBackendKeyFromString(""); err != nil {
		t.Fatalf("SetBackendKeyFromString(\"\"): %v", err)
	}
	if cfg.BackendKeyEnclave() != nil {
		t.Error("clearing the key left an enclave behind")
	}
	if previous.IsAlive() {
		t.Error("the replaced enclave was not destroyed")
	}

	if err := cfg.SetBackendSecretFromString(""); err != nil {
		t.Fatalf("SetBackendSecretFromString(\"\"): %v", err)
	}
	if cfg.BackendSecretEnclave() != nil {
		t.Error("clearing the secret left an enclave behind")
	}
}

func TestSetBackendCredentialEnclave_ReplacesPrevious(t *testing.T) {
	cfg := &Config{}

	firstKey, err := enclave.NewMemoryEnclaveFromString("first-key")
	if err != nil {
		t.Fatalf("NewMemoryEnclaveFromString: %v", err)
	}
	firstSecret, err := enclave.NewMemoryEnclaveFromString("first-secret")
	if err != nil {
		t.Fatalf("NewMemoryEnclaveFromString: %v", err)
	}
	cfg.SetBackendKeyEnclave(firstKey)
	cfg.SetBackendSecretEnclave(firstSecret)

	if err := cfg.SetBackendKeyFromString(testAccessKey); err != nil {
		t.Fatalf("SetBackendKeyFromString: %v", err)
	}
	if err := cfg.SetBackendSecretFromString(testSecretKey); err != nil {
		t.Fatalf("SetBackendSecretFromString: %v", err)
	}
	defer cfg.Destroy()

	if firstKey.IsAlive() || firstSecret.IsAlive() {
		t.Error("replaced enclaves were not destroyed")
	}

	// Re-setting the same enclave must not destroy it.
	current := cfg.BackendKeyEnclave()
	cfg.SetBackendKeyEnclave(current)
	if !current.IsAlive() {
		t.Error("re-setting the same enclave destroyed it")
	}
	currentSecret := cfg.BackendSecretEnclave()
	cfg.SetBackendSecretEnclave(currentSecret)
	if !currentSecret.IsAlive() {
		t.Error("re-setting the same secret enclave destroyed it")
	}
}

func TestConfigDestroy(t *testing.T) {
	cfg, err := GetConfigProtected(protectedTestViper())
	if err != nil {
		t.Fatalf("GetConfigProtected: %v", err)
	}

	key := cfg.BackendKeyEnclave()
	secret := cfg.BackendSecretEnclave()

	cfg.Destroy()
	cfg.Destroy() // idempotent

	if key.IsAlive() || secret.IsAlive() {
		t.Error("credentials still alive after Destroy")
	}
	if cfg.BackendKeyEnclave() != nil || cfg.BackendSecretEnclave() != nil {
		t.Error("enclave references not cleared")
	}
	if _, ok := cfg.GetStorageSettings()["access_key_id"]; ok {
		t.Error("destroyed credential still reaches the settings map")
	}

	var nilCfg *Config
	nilCfg.Destroy() // must not panic
}

func TestDisplayConfig_MasksEnclavedCredentials(t *testing.T) {
	cfg, err := GetConfigProtected(protectedTestViper())
	if err != nil {
		t.Fatalf("GetConfigProtected: %v", err)
	}
	defer cfg.Destroy()

	for _, format := range []string{"text", "table", "json"} {
		t.Run(format, func(t *testing.T) {
			out := DisplayConfig(cfg, format)
			if strings.Contains(out, testSecretKey) || strings.Contains(out, testAccessKey) {
				t.Errorf("credential leaked into %s output:\n%s", format, out)
			}
			if !strings.Contains(out, testAccessKey[:4]+"****") {
				t.Errorf("masked access key missing from %s output:\n%s", format, out)
			}
			if !strings.Contains(out, testSecretKey[:4]+"****") {
				t.Errorf("masked secret key missing from %s output:\n%s", format, out)
			}
		})
	}
}

func TestDisplayConfig_EnclavedAndPlainAgree(t *testing.T) {
	plain := GetConfig(protectedTestViper())
	protected, err := GetConfigProtected(protectedTestViper())
	if err != nil {
		t.Fatalf("GetConfigProtected: %v", err)
	}
	defer protected.Destroy()

	for _, format := range []string{"text", "table", "json"} {
		t.Run(format, func(t *testing.T) {
			if got, want := DisplayConfig(protected, format), DisplayConfig(plain, format); got != want {
				t.Errorf("protected output differs from plain output:\ngot:\n%s\nwant:\n%s", got, want)
			}
		})
	}
}

func TestMaskEnclave_ShortMaterial(t *testing.T) {
	short, err := enclave.NewMemoryEnclaveFromString("ab")
	if err != nil {
		t.Fatalf("NewMemoryEnclaveFromString: %v", err)
	}
	defer short.Destroy()

	if got := maskEnclave(short); got != "****" {
		t.Errorf("maskEnclave(short) = %q, want ****", got)
	}
	if got := maskEnclave(nil); got != "****" {
		t.Errorf("maskEnclave(nil) = %q, want ****", got)
	}
}

// TestStorageSettings_DestroyedEnclaveDoesNotFallBack proves the credential
// path fails CLOSED. memguard purges every live locked buffer when the process
// runs out of lockable memory, so an enclave can be destroyed without this
// package asking for it. If a stale value is still sitting in the deprecated
// plain field at that moment, reading it would authenticate the backend with
// unprotected credentials the caller believed it had replaced.
func TestStorageSettings_DestroyedEnclaveDoesNotFallBack(t *testing.T) {
	cfg := &Config{Backend: "s3", BackendBucket: "b", BackendRegion: "us-east-1"}
	if err := cfg.SetBackendKeyFromString(testAccessKey); err != nil {
		t.Fatalf("SetBackendKeyFromString: %v", err)
	}
	if err := cfg.SetBackendSecretFromString(testSecretKey); err != nil {
		t.Fatalf("SetBackendSecretFromString: %v", err)
	}

	// A consumer repopulating the deprecated fields after choosing the
	// protected path. They must never be reachable again.
	cfg.BackendKey = "AKIASTALEPLAINTEXTKEY"
	cfg.BackendSecret = "stale-plaintext-secret"

	cfg.BackendKeyEnclave().Destroy()
	cfg.BackendSecretEnclave().Destroy()

	settings, err := cfg.StorageSettings()
	if !errors.Is(err, enclave.ErrEnclaveDestroyed) {
		t.Fatalf("StorageSettings err = %v, want ErrEnclaveDestroyed", err)
	}
	if settings != nil {
		t.Errorf("settings = %v, want nil on the error path", settings)
	}

	// The deprecated accessor cannot report the error, so it must omit the
	// credential rather than substitute the stale one.
	deprecated := cfg.GetStorageSettings()
	if v, ok := deprecated["access_key_id"]; ok {
		t.Errorf("access_key_id = %q, want it omitted after the enclave was destroyed", v)
	}
	if v, ok := deprecated["secret_access_key"]; ok {
		t.Errorf("secret_access_key = %q, want it omitted after the enclave was destroyed", v)
	}

	// Display must not leak the stale plaintext either.
	out := DisplayConfig(cfg, "text")
	if strings.Contains(out, "AKIASTALE") || strings.Contains(out, "stale-plaintext") {
		t.Errorf("display leaked the stale plain credential:\n%s", out)
	}
}

// TestStorageSettings_PlainFieldsStillReadableWithoutAnyEnclave keeps the
// no-fallback rule from swallowing the deprecated path itself: the plain field
// is legitimate when no enclave was ever selected.
func TestStorageSettings_PlainFieldsStillReadableWithoutAnyEnclave(t *testing.T) {
	cfg := &Config{
		Backend:       "s3",
		BackendKey:    testAccessKey,
		BackendSecret: testSecretKey,
	}

	settings, err := cfg.StorageSettings()
	if err != nil {
		t.Fatalf("StorageSettings: %v", err)
	}
	if settings["access_key_id"] != testAccessKey {
		t.Errorf("access_key_id = %q, want %q", settings["access_key_id"], testAccessKey)
	}
	if settings["secret_access_key"] != testSecretKey {
		t.Errorf("secret_access_key = %q, want %q", settings["secret_access_key"], testSecretKey)
	}
}

// TestNewCommandContext_DestroyedEnclaveIsRefused checks the fail-closed rule
// where it matters: the backend is never constructed from a config whose
// protected credential cannot be read.
func TestNewCommandContext_DestroyedEnclaveIsRefused(t *testing.T) {
	cfg := &Config{
		Backend:       "s3",
		BackendBucket: "bucket",
		BackendRegion: "us-east-1",
		OutputFormat:  "text",
	}
	if err := cfg.SetBackendKeyFromString(testAccessKey); err != nil {
		t.Fatalf("SetBackendKeyFromString: %v", err)
	}
	cfg.BackendKey = "AKIASTALEPLAINTEXTKEY"
	cfg.BackendKeyEnclave().Destroy()

	ctx, err := NewCommandContext(cfg)
	if !errors.Is(err, enclave.ErrEnclaveDestroyed) {
		t.Fatalf("NewCommandContext err = %v, want ErrEnclaveDestroyed", err)
	}
	if ctx != nil {
		t.Error("a command context was built from an unreadable credential")
	}
}
