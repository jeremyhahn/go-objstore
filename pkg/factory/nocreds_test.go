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

package factory

import (
	"testing"

	"github.com/jeremyhahn/go-enclave"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// noCreds configures a backend with no credential handles, so the SDK default
// credential chain applies.
var noCreds = common.Credentials{}

// keyCreds returns Credentials whose Secret handle holds an Azure account key;
// an empty key gives no credential.
func keyCreds(t *testing.T, key string) common.Credentials {
	t.Helper()
	if key == "" {
		return common.Credentials{}
	}
	h, err := enclave.NewMemory([]byte(key))
	if err != nil {
		t.Fatalf("NewMemory: %v", err)
	}
	t.Cleanup(func() { _ = h.Destroy() })
	return common.Credentials{Secret: h}
}

// splitKey moves a table-driven accountKey entry out of the settings map and
// into a credential handle, the only place a backend accepts it.
func splitKey(t *testing.T, in map[string]string) (map[string]string, common.Credentials) {
	t.Helper()
	out := make(map[string]string, len(in))
	for k, v := range in {
		if k != "accountKey" {
			out[k] = v
		}
	}
	return out, keyCreds(t, in["accountKey"])
}
