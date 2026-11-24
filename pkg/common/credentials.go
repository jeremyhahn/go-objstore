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
	"fmt"
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

// credentialRole names the Credentials handle a wire-format settings entry
// feeds. Session tokens and SAS tokens have no handle, so they are not here:
// they stay in the returned settings and Configure refuses them.
type credentialRole int

const (
	roleKey credentialRole = iota + 1
	roleSecret
)

var credentialRoles = map[string]credentialRole{
	"accesskey":       roleKey,
	"accesskeyid":     roleKey,
	"secretkey":       roleSecret,
	"secretaccesskey": roleSecret,
	"accountkey":      roleSecret,
}

// ExtractCredentialSettings moves the credential entries of a settings map
// received over a wire protocol into protected-memory handles. It is for the
// API boundary only: call it before anything else touches the map. The
// returned settings hold no credential entry, so they pass
// RejectCredentialSettings and Configure; the entries are also deleted from
// the map passed in, so the request holds no credential after the call.
//
// Entries with an empty value are dropped, which leaves that handle nil (the
// SDK default chain). Two entries for the same handle are ambiguous and
// rejected. release destroys the handles and must be called when the call that
// uses them ends; it is never nil, even on error.
//
// The wire format is a JSON, protobuf or text string, so the decoder has
// already made a Go string copy that Go cannot zeroize. The handle bounds the
// lifetime of every copy this package makes, not the decoder's.
func ExtractCredentialSettings(settings map[string]string) (clean map[string]string, creds Credentials, release func() error, err error) {
	clean = make(map[string]string, len(settings))
	release = func() error {
		return errors.Join(destroyHandle(creds.Key), destroyHandle(creds.Secret))
	}
	for k, v := range settings {
		norm := strings.ToLower(strings.NewReplacer("_", "", "-", "").Replace(k))
		role, isCred := credentialRoles[norm]
		if !isCred {
			clean[k] = v
			continue
		}
		delete(settings, k)
		if v == "" {
			continue
		}
		slot := &creds.Key
		if role == roleSecret {
			slot = &creds.Secret
		}
		if *slot != nil {
			err = fmt.Errorf("%w: more than one setting supplies the same credential (%q)", ErrInvalidArgument, k)
			break
		}
		material := []byte(v)
		h, nerr := enclave.NewMemory(material)
		clear(material)
		if nerr != nil {
			err = fmt.Errorf("protect credential setting %q: %w", k, nerr)
			break
		}
		*slot = h
	}
	if err != nil {
		// Drop the rest of the credential entries too, then free what was made.
		for k := range settings {
			if _, isCred := credentialRoles[strings.ToLower(strings.NewReplacer("_", "", "-", "").Replace(k))]; isCred {
				delete(settings, k)
			}
		}
		err = errors.Join(err, release())
		return nil, Credentials{}, func() error { return nil }, err
	}
	return clean, creds, release, nil
}

func destroyHandle(h enclave.ByteAccess) error {
	if h == nil {
		return nil
	}
	return h.Destroy()
}
