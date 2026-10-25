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
	"testing"

	"github.com/jeremyhahn/go-enclave"

	objcommon "github.com/jeremyhahn/go-objstore/pkg/common"
)

// NoCreds is the credential argument for backends that need none (local,
// memory) or that read the SDK default chain.
func NoCreds() objcommon.Credentials { return objcommon.Credentials{} }

// Creds returns protected-memory handles for the given credential material,
// destroyed when the test ends. An empty string leaves that handle nil, which
// means "use the SDK default chain". A credential never goes in a settings map.
func Creds(tb testing.TB, key, secret string) objcommon.Credentials {
	tb.Helper()
	return objcommon.Credentials{Key: handle(tb, key), Secret: handle(tb, secret)}
}

func handle(tb testing.TB, material string) enclave.ByteAccess {
	tb.Helper()
	if material == "" {
		return nil
	}
	h, err := enclave.NewMemory([]byte(material))
	if err != nil {
		tb.Fatalf("enclave.NewMemory: %v", err)
	}
	tb.Cleanup(func() { _ = h.Destroy() })
	return h
}
