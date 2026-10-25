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

package factory_test

import (
	"errors"
	"testing"

	"github.com/jeremyhahn/go-objstore/internal/wireprobe"
	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/factory"
)

func TestNewArchiverFromWireCredentialReachesBackendThroughHandle(t *testing.T) {
	probe := wireprobe.Register(t)
	wire := map[string]string{"accessKey": "AK-wire", "secretKey": "SK-wire", "region": "eu-1"}

	if _, err := factory.NewArchiverFromWire(wireprobe.Backend, wire); err != nil {
		t.Fatalf("NewArchiverFromWire: %v", err)
	}
	probe.AssertCredential(t, "AK-wire", "SK-wire", map[string]string{"region": "eu-1"})
	if len(wire) != 1 {
		t.Errorf("request settings keep %d entries, want only region", len(wire))
	}
}

func TestNewArchiverFromWireWithoutCredentialUsesDefaultChain(t *testing.T) {
	probe := wireprobe.Register(t)
	if _, err := factory.NewArchiverFromWire(wireprobe.Backend, map[string]string{"region": "eu-1"}); err != nil {
		t.Fatalf("NewArchiverFromWire: %v", err)
	}
	if _, _, hasKey, hasSecret := probe.Credential(); hasKey || hasSecret || probe.Calls() != 1 {
		t.Errorf("no credential supplied: key handle %t, secret handle %t, calls %d", hasKey, hasSecret, probe.Calls())
	}
}

func TestNewArchiverFromWireRefusesTokenWithNoHandle(t *testing.T) {
	probe := wireprobe.Register(t)
	_, err := factory.NewArchiverFromWire(wireprobe.Backend, map[string]string{"sessionToken": "t0k"})
	if !errors.Is(err, common.ErrCredentialInSettings) {
		t.Fatalf("err = %v, want ErrCredentialInSettings", err)
	}
	if probe.Calls() != 0 {
		t.Error("a refused settings map must not configure the backend")
	}
}

func TestNewArchiverFromWireUnknownBackendFreesHandles(t *testing.T) {
	_, err := factory.NewArchiverFromWire("no-such-archiver", map[string]string{"accessKey": "a", "secretKey": "b"})
	if !errors.Is(err, factory.ErrUnknownArchiver) {
		t.Fatalf("err = %v, want ErrUnknownArchiver", err)
	}
}
