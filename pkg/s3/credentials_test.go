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

//go:build awss3

package s3

import (
	"errors"
	"testing"

	"github.com/jeremyhahn/go-enclave"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

func credHandle(t *testing.T, material string) enclave.ByteAccess {
	t.Helper()
	h, err := enclave.NewMemory([]byte(material))
	if err != nil {
		t.Fatalf("NewMemory: %v", err)
	}
	t.Cleanup(func() { _ = h.Destroy() })
	return h
}

func TestConfigureRejectsCredentialInSettings(t *testing.T) {
	for _, k := range []string{"accessKey", "secretKey", "access_key_id", "secret_access_key", "sessionToken"} {
		s := New().(*S3)
		err := s.Configure(map[string]string{"bucket": "b", "region": "us-east-1", k: "v"}, noCreds)
		if !errors.Is(err, common.ErrCredentialInSettings) {
			t.Errorf("Configure with %q in settings: err = %v, want ErrCredentialInSettings", k, err)
		}
		if _, err := NewManager(map[string]string{"region": "us-east-1", k: "v"}, noCreds); !errors.Is(err, common.ErrCredentialInSettings) {
			t.Errorf("NewManager with %q in settings: err = %v, want ErrCredentialInSettings", k, err)
		}
	}
}

func TestConfigureFailsClosedOnUnusableHandle(t *testing.T) {
	settings := map[string]string{"bucket": "b", "region": "us-east-1"}

	destroyed := credHandle(t, "gone")
	if err := destroyed.Destroy(); err != nil {
		t.Fatalf("Destroy: %v", err)
	}

	for name, creds := range map[string]common.Credentials{
		"destroyed key":    {Key: destroyed, Secret: credHandle(t, "secret")},
		"destroyed secret": {Key: credHandle(t, "AKIAEXAMPLE"), Secret: destroyed},
	} {
		s := New().(*S3)
		err := s.Configure(settings, creds)
		var unavailable *common.CredentialUnavailableError
		if !errors.As(err, &unavailable) || !errors.Is(err, enclave.ErrEnclaveDestroyed) {
			t.Errorf("%s: err = %v, want *CredentialUnavailableError wrapping ErrEnclaveDestroyed", name, err)
		}
		if s.svc != nil {
			t.Errorf("%s: backend configured despite an unusable credential", name)
		}
	}
}

func TestConfigureHalfCredentialIsRefused(t *testing.T) {
	settings := map[string]string{"bucket": "b", "region": "us-east-1"}
	if err := New().Configure(settings, common.Credentials{Key: credHandle(t, "k")}); !errors.Is(err, common.ErrSecretKeyNotSet) {
		t.Errorf("key only: err = %v, want ErrSecretKeyNotSet", err)
	}
	if err := New().Configure(settings, common.Credentials{Secret: credHandle(t, "s")}); !errors.Is(err, common.ErrAccessKeyNotSet) {
		t.Errorf("secret only: err = %v, want ErrAccessKeyNotSet", err)
	}
}

func TestConfigureWithHandles(t *testing.T) {
	s := New().(*S3)
	creds := common.Credentials{Key: credHandle(t, "AKIAEXAMPLE"), Secret: credHandle(t, "secret")}
	if err := s.Configure(map[string]string{"bucket": "b", "region": "us-east-1"}, creds); err != nil {
		t.Fatalf("Configure: %v", err)
	}
	if s.svc == nil {
		t.Fatal("backend not configured")
	}
	// The handles are borrowed, not consumed or retained.
	if held, err := creds.Key.EqualTo([]byte("AKIAEXAMPLE")); err != nil || !held {
		t.Errorf("key handle after Configure: held = %v, err = %v", held, err)
	}
}
