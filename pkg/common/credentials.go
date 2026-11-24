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

// Credentials holds go-enclave handles for Key and Secret (a single-key backend reads Secret); nil means the SDK default chain, and backends never retain them.
type Credentials struct {
	Key    enclave.ByteAccess
	Secret enclave.ByteAccess
}

// IsZero reports whether neither handle is set.
func (c Credentials) IsZero() bool {
	return c.Key == nil && c.Secret == nil
}

// Open calls fn with zeroized-after copies of the set credentials; an unreadable handle fails with *CredentialUnavailableError.
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

// CredentialUnavailableError reports an unreadable credential handle; Unwrap exposes the cause.
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

// ExtractCredentialSettings moves wire-received credential entries into protected handles and deletes them from the map; call release when done (never nil).
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
