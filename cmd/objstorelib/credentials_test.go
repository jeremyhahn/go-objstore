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

package main

import (
	"strings"
	"testing"

	"github.com/jeremyhahn/go-objstore/internal/wireprobe"
)

func TestBuildStorageCredentialReachesBackendThroughHandle(t *testing.T) {
	probe := wireprobe.RegisterStorage(t)
	handle := buildStorage(wireprobe.Backend, map[string]string{"region": "eu-1"}, []byte("AK-clib"), []byte("SK-clib"))
	if handle < 0 {
		t.Fatalf("buildStorage failed: %s", lastError)
	}
	t.Cleanup(func() { unregisterStorage(int(handle)) })
	probe.AssertCredential(t, "AK-clib", "SK-clib", map[string]string{"region": "eu-1"})
}

func TestBuildStorageWithoutCredentialUsesDefaultChain(t *testing.T) {
	probe := wireprobe.RegisterStorage(t)
	handle := buildStorage(wireprobe.Backend, map[string]string{"region": "eu-1"}, nil, nil)
	if handle < 0 {
		t.Fatalf("buildStorage failed: %s", lastError)
	}
	t.Cleanup(func() { unregisterStorage(int(handle)) })
	if _, _, hasKey, hasSecret := probe.Credential(); hasKey || hasSecret || probe.Calls() != 1 {
		t.Errorf("no credential supplied: key handle %t, secret handle %t, calls %d", hasKey, hasSecret, probe.Calls())
	}
}

func TestBuildStorageRefusesCredentialInSettings(t *testing.T) {
	probe := wireprobe.RegisterStorage(t)
	handle := buildStorage(wireprobe.Backend, map[string]string{"secretKey": "SK-in-settings"}, []byte("AK-clib"), []byte("SK-clib"))
	if handle >= 0 {
		unregisterStorage(int(handle))
		t.Fatal("a credential in the settings map must be refused")
	}
	if lastError == "" || strings.Contains(lastError, "SK-in-settings") || strings.Contains(lastError, "SK-clib") {
		t.Errorf("last error must be set and carry no credential, got %q", lastError)
	}
	if probe.Calls() != 0 {
		t.Error("a refused settings map must not configure the backend")
	}
}

func TestProtectCredentialsEmptyBuffersLeaveHandlesNil(t *testing.T) {
	creds, release, err := protectCredentials(nil, []byte{})
	if err != nil {
		t.Fatal(err)
	}
	if !creds.IsZero() {
		t.Error("empty buffers must leave both handles nil")
	}
	if err := release(); err != nil {
		t.Errorf("release of nil handles: %v", err)
	}
}
