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

	"github.com/jeremyhahn/go-enclave"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

func newHandle(t *testing.T, material string) enclave.ByteAccess {
	t.Helper()
	h, err := enclave.NewMemory([]byte(material))
	if err != nil {
		t.Fatalf("NewMemory: %v", err)
	}
	t.Cleanup(func() { _ = h.Destroy() })
	return h
}

// emptyHandle yields no bytes, as a handle that returns nil would.
type emptyHandle struct{ enclave.ByteAccess }

func (emptyHandle) WithBytes(fn func([]byte) error) error { return fn(nil) }

func TestCredentialsIsZero(t *testing.T) {
	if !(common.Credentials{}).IsZero() {
		t.Error("zero Credentials: IsZero = false")
	}
	if (common.Credentials{Secret: newHandle(t, "s")}).IsZero() {
		t.Error("Credentials with a secret: IsZero = true")
	}
}

func TestCredentialsOpenPassesNilForUnsetFields(t *testing.T) {
	called := false
	err := common.Credentials{}.Open(func(key, secret []byte) error {
		called = true
		if key != nil || secret != nil {
			t.Errorf("key = %v, secret = %v, want both nil", key, secret)
		}
		return nil
	})
	if err != nil || !called {
		t.Fatalf("Open: err = %v, called = %v", err, called)
	}
}

func TestCredentialsOpenCopiesThenZeroizes(t *testing.T) {
	creds := common.Credentials{Key: newHandle(t, "AKIAEXAMPLE"), Secret: newHandle(t, "s3cr3t-value")}

	var keySeen, secretSeen []byte
	err := creds.Open(func(key, secret []byte) error {
		if string(key) != "AKIAEXAMPLE" || string(secret) != "s3cr3t-value" {
			t.Errorf("fn saw key %q secret %q", key, secret)
		}
		keySeen, secretSeen = key, secret
		return nil
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for name, b := range map[string][]byte{"key": keySeen, "secret": secretSeen} {
		for i, c := range b {
			if c != 0 {
				t.Fatalf("%s copy not zeroized at byte %d", name, i)
			}
		}
	}

	// The handles are untouched: Open borrows, it does not consume.
	if held, err := creds.Key.EqualTo([]byte("AKIAEXAMPLE")); err != nil || !held {
		t.Errorf("key handle after Open: held = %v, err = %v", held, err)
	}
}

func TestCredentialsOpenPropagatesCallbackError(t *testing.T) {
	boom := errors.New("boom")
	err := common.Credentials{Secret: newHandle(t, "s")}.Open(func(_, _ []byte) error { return boom })
	if !errors.Is(err, boom) {
		t.Fatalf("Open err = %v, want the callback's error", err)
	}
}

func TestCredentialsOpenFailsClosed(t *testing.T) {
	destroyed := newHandle(t, "gone")
	if err := destroyed.Destroy(); err != nil {
		t.Fatalf("Destroy: %v", err)
	}

	tests := []struct {
		name  string
		creds common.Credentials
		cause error
	}{
		{"destroyed key", common.Credentials{Key: destroyed, Secret: newHandle(t, "s")}, enclave.ErrEnclaveDestroyed},
		{"destroyed secret", common.Credentials{Key: newHandle(t, "k"), Secret: destroyed}, enclave.ErrEnclaveDestroyed},
		{"empty secret", common.Credentials{Secret: emptyHandle{}}, common.ErrCredentialEmpty},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			called := false
			err := tt.creds.Open(func(_, _ []byte) error { called = true; return nil })
			if called {
				t.Error("callback ran for an unusable credential")
			}
			var unavailable *common.CredentialUnavailableError
			if !errors.As(err, &unavailable) {
				t.Fatalf("Open err = %T (%v), want *CredentialUnavailableError", err, err)
			}
			if !errors.Is(err, tt.cause) {
				t.Errorf("Open err = %v, want it to wrap %v", err, tt.cause)
			}
		})
	}
}

func TestRejectCredentialSettings(t *testing.T) {
	rejected := []string{
		"accessKey", "access_key", "accessKeyId", "access_key_id", "AccessKey",
		"secretKey", "secret-key", "secretAccessKey", "secret_access_key",
		"accountKey", "account_key", "sasToken", "sas_token", "sessionToken",
	}
	for _, k := range rejected {
		err := common.RejectCredentialSettings(map[string]string{"bucket": "b", k: "value"})
		var se *common.CredentialSettingError
		if !errors.As(err, &se) || se.Setting != k {
			t.Errorf("%q: err = %v, want *CredentialSettingError naming it", k, err)
			continue
		}
		if !errors.Is(err, common.ErrCredentialInSettings) || !errors.Is(err, common.ErrInvalidArgument) {
			t.Errorf("%q: err does not match ErrCredentialInSettings and ErrInvalidArgument", k)
		}
		if got := err.Error(); strings.Contains(got, "value") {
			t.Errorf("%q: error text %q must name the setting, never its value", k, got)
		}
	}

	plain := map[string]string{
		"path": "/p", "bucket": "b", "region": "r", "endpoint": "e",
		"accountName": "acct", "containerName": "c", "project": "p",
	}
	if err := common.RejectCredentialSettings(plain); err != nil {
		t.Errorf("plain settings rejected: %v", err)
	}
	if err := common.RejectCredentialSettings(nil); err != nil {
		t.Errorf("nil settings rejected: %v", err)
	}
}
