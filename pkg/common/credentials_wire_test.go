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
	"reflect"
	"testing"
)

func openPair(t *testing.T, c Credentials) (key, secret string) {
	t.Helper()
	err := c.Open(func(k, s []byte) error {
		key, secret = string(k), string(s)
		return nil
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return key, secret
}

func TestExtractCredentialSettingsMovesEntriesIntoHandles(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   map[string]string
		key  string
		sec  string
	}{
		{"s3 camel", map[string]string{"accessKey": "AK", "secretKey": "SK", "region": "r"}, "AK", "SK"},
		{"s3 snake", map[string]string{"access_key_id": "AK", "secret_access_key": "SK", "region": "r"}, "AK", "SK"},
		{"azure", map[string]string{"accountKey": "SK", "region": "r"}, "", "SK"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := make(map[string]string, len(tc.in))
			for k, v := range tc.in {
				in[k] = v
			}
			clean, creds, release, err := ExtractCredentialSettings(in)
			if err != nil {
				t.Fatalf("ExtractCredentialSettings: %v", err)
			}
			if want := map[string]string{"region": "r"}; !reflect.DeepEqual(clean, want) {
				t.Errorf("clean = %v, want %v", clean, want)
			}
			if err := RejectCredentialSettings(clean); err != nil {
				t.Errorf("clean settings still carry a credential: %v", err)
			}
			if !reflect.DeepEqual(in, map[string]string{"region": "r"}) {
				t.Errorf("request map keeps credential entries: %d entries", len(in))
			}
			if (creds.Key != nil) != (tc.key != "") || creds.Secret == nil {
				t.Fatalf("handles: key %t secret %t", creds.Key != nil, creds.Secret != nil)
			}
			if tc.key != "" {
				if k, s := openPair(t, creds); k != tc.key || s != tc.sec {
					t.Errorf("handle material does not match the request")
				}
			}
			if err := release(); err != nil {
				t.Fatalf("release: %v", err)
			}
			var unavailable *CredentialUnavailableError
			if err := creds.Open(func(_, _ []byte) error { return nil }); !errors.As(err, &unavailable) {
				t.Errorf("after release Open err = %v, want *CredentialUnavailableError", err)
			}
		})
	}
}

func TestExtractCredentialSettingsWithoutCredentialUsesDefaultChain(t *testing.T) {
	clean, creds, release, err := ExtractCredentialSettings(map[string]string{"region": "r", "secretKey": ""})
	if err != nil {
		t.Fatal(err)
	}
	if creds.Key != nil || creds.Secret != nil {
		t.Error("an absent or empty credential must leave the handles nil")
	}
	if !reflect.DeepEqual(clean, map[string]string{"region": "r"}) {
		t.Errorf("clean = %v", clean)
	}
	if err := release(); err != nil {
		t.Errorf("release of nil handles: %v", err)
	}

	clean, creds, release, err = ExtractCredentialSettings(nil)
	if err != nil || clean == nil || !creds.IsZero() || release() != nil {
		t.Errorf("nil settings: clean=%v err=%v", clean, err)
	}
}

func TestExtractCredentialSettingsLeavesUnhandledTokensToBeRefused(t *testing.T) {
	clean, _, release, err := ExtractCredentialSettings(map[string]string{"sasToken": "x", "sessionToken": "y"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = release() }()
	if err := RejectCredentialSettings(clean); !errors.Is(err, ErrCredentialInSettings) {
		t.Errorf("a token with no handle must stay in settings and be refused, got %v", err)
	}
}

func TestExtractCredentialSettingsRejectsAmbiguousEntries(t *testing.T) {
	in := map[string]string{"secretKey": "a", "secret_access_key": "b", "accessKey": "c"}
	clean, creds, release, err := ExtractCredentialSettings(in)
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("err = %v, want ErrInvalidArgument", err)
	}
	if clean != nil || !creds.IsZero() || release == nil || release() != nil {
		t.Error("an error must return no settings, no handles and a safe release")
	}
	if len(in) != 0 {
		t.Errorf("request map keeps %d credential entries after an error", len(in))
	}
}
