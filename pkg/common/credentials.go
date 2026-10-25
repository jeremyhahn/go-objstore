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
	"errors"
	"strings"

	"github.com/jeremyhahn/go-enclave"
)

// Credentials is the protected-memory credential handle a backend is
// configured with. Each field is a go-enclave handle, never a string: the
// material stays in locked, zeroized memory until the backend borrows it to
// build the vendor SDK credential.
//
// A nil field means "none": the backend falls back to the SDK's default
// credential chain. A backend never retains a Credentials value or either
// handle; it opens them through [Credentials.Open] for the length of the call
// that builds the SDK credential.
//
// Key is the access key identifier and Secret the secret access key. A backend
// whose only secret is one key (for example an Azure storage account key)
// reads Secret.
type Credentials struct {
	Key    enclave.ByteAccess
	Secret enclave.ByteAccess
}

// IsZero reports whether neither handle is set.
func (c Credentials) IsZero() bool {
	return c.Key == nil && c.Secret == nil
}

// Open copies each set credential out of its handle and calls fn with the
// copies, then zeroizes them. A credential that is not set reaches fn as nil.
// fn must not retain either slice.
//
// A handle that cannot be read, or that yields no bytes, fails closed with a
// [*CredentialUnavailableError]: the SDK's default chain is never substituted
// for a credential that was supplied but is unusable.
func (c Credentials) Open(fn func(key, secret []byte) error) error {
	key, err := copyCredential("access key", c.Key)
	defer clear(key)
	if err != nil {
		return err
	}
	secret, err := copyCredential("secret key", c.Secret)
	defer clear(secret)
	if err != nil {
		return err
	}
	return fn(key, secret)
}

// copyCredential returns a private copy of the material in h, or nil when h is
// nil. The copy is the caller's to zeroize.
func copyCredential(name string, h enclave.ByteAccess) ([]byte, error) {
	if h == nil {
		return nil, nil
	}
	var out []byte
	err := h.WithBytes(func(b []byte) error {
		out = append([]byte(nil), b...)
		return nil
	})
	if err != nil {
		clear(out)
		return nil, &CredentialUnavailableError{Credential: name, Err: err}
	}
	if len(out) == 0 {
		return nil, &CredentialUnavailableError{Credential: name, Err: ErrCredentialEmpty}
	}
	return out, nil
}

// ErrCredentialEmpty is the cause of a [CredentialUnavailableError] when a
// credential handle yields no bytes.
var ErrCredentialEmpty = errors.New("credential handle holds no material")

// ErrCredentialInSettings is returned when a credential is passed in the
// settings map instead of [Credentials]. Match it with errors.Is.
var ErrCredentialInSettings = errors.New("credential supplied in settings; pass it in Credentials")

// CredentialUnavailableError reports that a credential handle was supplied but
// could not be read: its enclave was destroyed, or it holds no material. The
// backend is left unconfigured. Unwrap exposes the cause, typically
// enclave.ErrEnclaveDestroyed.
type CredentialUnavailableError struct {
	// Credential names the credential, for example "secret key".
	Credential string

	// Err is why it could not be read.
	Err error
}

// Error implements error.
func (e *CredentialUnavailableError) Error() string {
	return "credential " + e.Credential + " unavailable: " + e.Err.Error()
}

// Unwrap returns the underlying cause.
func (e *CredentialUnavailableError) Unwrap() error { return e.Err }

// CredentialSettingError reports a credential found in the settings map. It
// names the setting, never its value. It matches [ErrCredentialInSettings] and
// [ErrInvalidArgument].
type CredentialSettingError struct {
	// Setting is the offending settings key.
	Setting string
}

// Error implements error.
func (e *CredentialSettingError) Error() string {
	return "setting " + e.Setting + ": " + ErrCredentialInSettings.Error()
}

// Is matches the sentinels this error stands for.
func (e *CredentialSettingError) Is(target error) bool {
	return target == ErrCredentialInSettings || target == ErrInvalidArgument
}

// credentialSettings holds the normalized names (lower case, without '_' and
// '-') of the settings keys that carry credential material.
var credentialSettings = map[string]struct{}{
	"accesskey":       {},
	"accesskeyid":     {},
	"secretkey":       {},
	"secretaccesskey": {},
	"accountkey":      {},
	"sastoken":        {},
	"sessiontoken":    {},
}

// RejectCredentialSettings returns a [*CredentialSettingError] when settings
// carries a credential entry. A credential never travels as a map value; every
// backend's Configure calls this before reading its settings.
func RejectCredentialSettings(settings map[string]string) error {
	for k := range settings {
		norm := strings.ToLower(strings.NewReplacer("_", "", "-", "").Replace(k))
		if _, ok := credentialSettings[norm]; ok {
			return &CredentialSettingError{Setting: k}
		}
	}
	return nil
}
