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

// Package wireprobe registers a recording archiver backend so a server test
// can prove what reached Configure: the settings map, and the credential
// handles.
package wireprobe

import (
	"errors"
	"io"
	"reflect"
	"sync"
	"testing"

	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/factory"
)

// Backend is the archiver type the probe registers.
const Backend = "wireprobe"

// Probe records one archiver construction.
type Probe struct {
	mu       sync.Mutex
	calls    int
	settings map[string]string
	key      string
	secret   string
	hasKey   bool
	hasSec   bool
	creds    common.Credentials
}

// Register installs the probe as the Backend archiver and returns it. Like the
// real backends, the probe refuses a credential left in the settings map.
func Register(tb testing.TB) *Probe {
	tb.Helper()
	p := &Probe{}
	factory.RegisterArchiver(Backend, func(settings map[string]string, creds common.Credentials) (common.Archiver, error) {
		if err := p.record(settings, creds); err != nil {
			return nil, err
		}
		return discard{}, nil
	})
	return p
}

// RegisterStorage installs the probe as the Backend storage type and returns
// it. The storage it builds is a stub that serves no operation.
func RegisterStorage(tb testing.TB) *Probe {
	tb.Helper()
	p := &Probe{}
	factory.RegisterStorage(Backend, func(settings map[string]string, creds common.Credentials) (common.Storage, error) {
		if err := p.record(settings, creds); err != nil {
			return nil, err
		}
		return stubStorage{}, nil
	})
	return p
}

func (p *Probe) record(settings map[string]string, creds common.Credentials) error {
	if err := common.RejectCredentialSettings(settings); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	p.settings = make(map[string]string, len(settings))
	for k, v := range settings {
		p.settings[k] = v
	}
	p.creds = creds
	p.hasKey = creds.Key != nil
	p.hasSec = creds.Secret != nil
	return creds.Open(func(key, secret []byte) error {
		p.key, p.secret = string(key), string(secret)
		return nil
	})
}

type stubStorage struct{ common.Storage }

// Calls is how many times the backend was configured.
func (p *Probe) Calls() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

// Settings is the map Configure received.
func (p *Probe) Settings() map[string]string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.settings
}

// Credential is the material read from the handles and whether each handle was
// supplied.
func (p *Probe) Credential() (key, secret string, hasKey, hasSecret bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.key, p.secret, p.hasKey, p.hasSec
}

// Destroyed reports whether every handle the backend received is now destroyed.
func (p *Probe) Destroyed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.creds.Key == nil && p.creds.Secret == nil {
		return false
	}
	var unavailable *common.CredentialUnavailableError
	return errors.As(p.creds.Open(func(_, _ []byte) error { return nil }), &unavailable)
}

// AssertCredential fails tb unless the backend was configured exactly once,
// received key and secret through its handles, received wantSettings in the
// settings map (so no credential there), and the handles are destroyed now.
func (p *Probe) AssertCredential(tb testing.TB, key, secret string, wantSettings map[string]string) {
	tb.Helper()
	if n := p.Calls(); n != 1 {
		tb.Fatalf("backend configured %d times, want 1", n)
	}
	gotKey, gotSecret, hasKey, hasSecret := p.Credential()
	if !hasKey || !hasSecret || gotKey != key || gotSecret != secret {
		tb.Errorf("credential did not reach the backend through the handles (key handle %t, secret handle %t, key match %t, secret match %t)",
			hasKey, hasSecret, gotKey == key, gotSecret == secret)
	}
	if got := p.Settings(); !reflect.DeepEqual(got, wantSettings) {
		tb.Errorf("settings reaching Configure = %v, want %v", got, wantSettings)
	}
	if !p.Destroyed() {
		tb.Error("credential handles were not destroyed when the call ended")
	}
}

type discard struct{}

func (discard) Put(_ string, data io.Reader) error {
	_, err := io.Copy(io.Discard, data)
	return err
}
