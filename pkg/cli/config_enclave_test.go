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

	"github.com/jeremyhahn/go-enclave"

	"github.com/jeremyhahn/go-objstore/pkg/common"
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
	if held, err := cfg.BackendKeyEnclave().EqualTo([]byte(testAccessKey)); err != nil || !held {
		t.Error("access key not held in the enclave")
	}
	if held, err := cfg.BackendSecretEnclave().EqualTo([]byte(testSecretKey)); err != nil || !held {
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
	if len(cfg.StorageSettings()) == 0 {
		t.Error("expected storage settings for the local backend")
	}
	creds, release, err := cfg.Credentials()
	if err != nil {
		t.Fatalf("Credentials: %v", err)
	}
	defer func() {
		if err := release(); err != nil {
			t.Errorf("release: %v", err)
		}
	}()
	if !creds.IsZero() {
		t.Error("Credentials set for absent credentials; want the SDK default chain")
	}
}

// handleString reads a credential handle for assertions.
func handleString(t *testing.T, h enclave.ByteAccess) string {
	t.Helper()
	if h == nil {
		t.Fatal("credential handle is nil")
	}
	var s string
	if err := h.WithBytes(func(b []byte) error { s = string(b); return nil }); err != nil {
		t.Fatalf("WithBytes: %v", err)
	}
	return s
}

func TestStorageSettings_ProtectedConfigHasNoCredentialEntries(t *testing.T) {
	cfg, err := GetConfigProtected(protectedTestViper())
	if err != nil {
		t.Fatalf("GetConfigProtected: %v", err)
	}
	defer cfg.Destroy()

	for name, settings := range map[string]map[string]string{
		"StorageSettings":    cfg.StorageSettings(),
		"GetStorageSettings": cfg.GetStorageSettings(),
	} {
		if err := common.RejectCredentialSettings(settings); err != nil {
			t.Errorf("%s carries a credential entry: %v", name, err)
		}
		for k, v := range settings {
			if v == testAccessKey || v == testSecretKey {
				t.Errorf("%s[%q] holds credential material", name, k)
			}
		}
	}

	creds, release, err := cfg.Credentials()
	if err != nil {
		t.Fatalf("Credentials: %v", err)
	}
	defer func() {
		if err := release(); err != nil {
			t.Errorf("release: %v", err)
		}
	}()
	if creds.Key != cfg.BackendKeyEnclave() || creds.Secret != cfg.BackendSecretEnclave() {
		t.Error("Credentials did not pass the Config's own handles through")
	}
	if got := handleString(t, creds.Key); got != testAccessKey {
		t.Errorf("key handle = %q, want %q", got, testAccessKey)
	}
	if got := handleString(t, creds.Secret); got != testSecretKey {
		t.Errorf("secret handle = %q, want %q", got, testSecretKey)
	}
}

func TestCredentials_DeprecatedFieldsMovedIntoTemporaryEnclaves(t *testing.T) {
	cfg := &Config{
		Backend:       "s3",
		BackendKey:    testAccessKey,
		BackendSecret: testSecretKey,
	}

	if err := common.RejectCredentialSettings(cfg.StorageSettings()); err != nil {
		t.Errorf("settings carry a credential entry: %v", err)
	}

	creds, release, err := cfg.Credentials()
	if err != nil {
		t.Fatalf("Credentials: %v", err)
	}
	if got := handleString(t, creds.Key); got != testAccessKey {
		t.Errorf("key handle = %q, want %q", got, testAccessKey)
	}
	if got := handleString(t, creds.Secret); got != testSecretKey {
		t.Errorf("secret handle = %q, want %q", got, testSecretKey)
	}

	if err := release(); err != nil {
		t.Errorf("release: %v", err)
	}
	for name, h := range map[string]enclave.ByteAccess{"key": creds.Key, "secret": creds.Secret} {
		if err := h.WithBytes(func([]byte) error { return nil }); !errors.Is(err, enclave.ErrEnclaveDestroyed) {
			t.Errorf("%s handle after release: err = %v, want ErrEnclaveDestroyed", name, err)
		}
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
	if held, err := cfg.BackendKeyEnclave().EqualTo([]byte(testAccessKey)); err != nil || !held {
		t.Error("access key not enclaved")
	}
	if held, err := cfg.BackendSecretEnclave().EqualTo([]byte(testSecretKey)); err != nil || !held {
		t.Error("secret key not enclaved")
	}

	// The enclave takes precedence over a value written to the deprecated
	// field afterwards.
	cfg.BackendKey = "should-be-ignored"
	creds, release, err := cfg.Credentials()
	if err != nil {
		t.Fatalf("Credentials: %v", err)
	}
	defer func() {
		if err := release(); err != nil {
			t.Errorf("release: %v", err)
		}
	}()
	if got := handleString(t, creds.Key); got != testAccessKey {
		t.Errorf("key handle = %q, want the enclaved key", got)
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

	firstKey, err := enclave.NewMemory([]byte("first-key"))
	if err != nil {
		t.Fatalf("NewMemory: %v", err)
	}
	firstSecret, err := enclave.NewMemory([]byte("first-secret"))
	if err != nil {
		t.Fatalf("NewMemory: %v", err)
	}
	if err := cfg.SetBackendKeyEnclave(firstKey); err != nil {
		t.Fatalf("SetBackendKeyEnclave: %v", err)
	}
	if err := cfg.SetBackendSecretEnclave(firstSecret); err != nil {
		t.Fatalf("SetBackendSecretEnclave: %v", err)
	}

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

	if err := cfg.Destroy(); err != nil {
		t.Errorf("Destroy: %v", err)
	}
	if err := cfg.Destroy(); err != nil { // idempotent
		t.Errorf("second Destroy: %v", err)
	}

	if key.IsAlive() || secret.IsAlive() {
		t.Error("credentials still alive after Destroy")
	}
	if cfg.BackendKeyEnclave() != nil || cfg.BackendSecretEnclave() != nil {
		t.Error("enclave references not cleared")
	}
	if _, _, err := cfg.Credentials(); err != nil {
		t.Errorf("Credentials after Destroy: %v, want none configured", err)
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
	short, err := enclave.NewMemory([]byte("ab"))
	if err != nil {
		t.Fatalf("NewMemory: %v", err)
	}
	defer short.Destroy()

	if got := maskEnclave(short); got != "****" {
		t.Errorf("maskEnclave(short) = %q, want ****", got)
	}
	if got := maskEnclave(nil); got != "****" {
		t.Errorf("maskEnclave(nil) = %q, want ****", got)
	}
}

// TestCredentials_DestroyedEnclaveDoesNotFallBack proves the credential
// path fails CLOSED. An enclave can be destroyed independently of the read — an
// explicit rotation or a Config teardown, as this test does below. If a stale
// value is still sitting in the deprecated plain field at that moment, reading
// it would authenticate the backend with unprotected credentials the caller
// believed it had replaced.
func TestCredentials_DestroyedEnclaveDoesNotFallBack(t *testing.T) {
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

	creds, release, err := cfg.Credentials()
	if err == nil {
		if err := release(); err != nil {
			t.Errorf("release: %v", err)
		}
		t.Fatal("Credentials: want a typed error for a destroyed enclave")
	}
	if err := release(); err != nil {
		t.Errorf("release: %v", err)
	}
	var unavailable *CredentialUnavailableError
	if !errors.As(err, &unavailable) {
		t.Fatalf("Credentials err = %T, want *CredentialUnavailableError", err)
	}
	if !errors.Is(err, enclave.ErrEnclaveDestroyed) {
		t.Errorf("Credentials err = %v, want it to wrap ErrEnclaveDestroyed", err)
	}
	if !creds.IsZero() {
		t.Error("Credentials returned handles on the error path")
	}

	// Neither settings accessor may carry the stale plaintext.
	for name, settings := range map[string]map[string]string{
		"StorageSettings":    cfg.StorageSettings(),
		"GetStorageSettings": cfg.GetStorageSettings(),
	} {
		for k, v := range settings {
			if strings.Contains(v, "AKIASTALE") || strings.Contains(v, "stale-plaintext") {
				t.Errorf("%s[%q] leaked the stale plain credential", name, k)
			}
		}
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
