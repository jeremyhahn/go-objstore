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

//go:build azureblob

package azure

import (
	"errors"
	"testing"

	"github.com/jeremyhahn/go-enclave"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

var credSettings = map[string]string{"accountName": "acct", "containerName": "c"}

func credHandle(t *testing.T, material string) enclave.ByteAccess {
	t.Helper()
	h, err := enclave.NewMemory([]byte(material))
	if err != nil {
		t.Fatalf("NewMemory: %v", err)
	}
	t.Cleanup(func() { _ = h.Destroy() })
	return h
}

// splitKey moves a table-driven accountKey entry out of the settings map and
// into a credential handle, the only place Configure accepts it.
func splitKey(t *testing.T, in map[string]string) (map[string]string, common.Credentials) {
	t.Helper()
	out := make(map[string]string, len(in))
	var creds common.Credentials
	for k, v := range in {
		if k == "accountKey" {
			creds.Secret = credHandle(t, v)
			continue
		}
		out[k] = v
	}
	return out, creds
}

func TestConfigureRejectsCredentialInSettings(t *testing.T) {
	for _, k := range []string{"accountKey", "account_key", "sasToken", "accessKey"} {
		settings := map[string]string{"accountName": "acct", "containerName": "c", k: "dGVzdC1rZXk="}
		if err := New().Configure(settings, credHandle2(t)); !errors.Is(err, common.ErrCredentialInSettings) {
			t.Errorf("Configure with %q in settings: err = %v, want ErrCredentialInSettings", k, err)
		}
		if _, err := NewManager(settings, credHandle2(t)); !errors.Is(err, common.ErrCredentialInSettings) {
			t.Errorf("NewManager with %q in settings: err = %v, want ErrCredentialInSettings", k, err)
		}
	}
}

// credHandle2 is a usable account key, so a rejection can only come from the
// settings map.
func credHandle2(t *testing.T) common.Credentials {
	t.Helper()
	return common.Credentials{Secret: credHandle(t, "dGVzdC1rZXk=")}
}

func TestConfigureFailsClosedOnUnusableHandle(t *testing.T) {
	destroyed := credHandle(t, "dGVzdC1rZXk=")
	if err := destroyed.Destroy(); err != nil {
		t.Fatalf("Destroy: %v", err)
	}

	a := New()
	err := a.Configure(credSettings, common.Credentials{Secret: destroyed})
	var unavailable *common.CredentialUnavailableError
	if !errors.As(err, &unavailable) || !errors.Is(err, enclave.ErrEnclaveDestroyed) {
		t.Fatalf("err = %v, want *CredentialUnavailableError wrapping ErrEnclaveDestroyed", err)
	}
	if a.(*Azure).sharedKey != nil {
		t.Error("backend configured despite an unusable credential")
	}
}

func TestConfigureWithoutKeyIsRefused(t *testing.T) {
	if err := New().Configure(credSettings, noCreds); !errors.Is(err, common.ErrAccountNotSet) {
		t.Errorf("err = %v, want ErrAccountNotSet", err)
	}
}

func TestConfigureWithHandle(t *testing.T) {
	a := New()
	key := credHandle(t, "dGVzdC1rZXk=")
	if err := a.Configure(credSettings, common.Credentials{Secret: key}); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	if a.(*Azure).sharedKey == nil {
		t.Fatal("backend not configured")
	}
	if held, err := key.EqualTo([]byte("dGVzdC1rZXk=")); err != nil || !held {
		t.Errorf("handle after Configure: held = %v, err = %v", held, err)
	}
}
