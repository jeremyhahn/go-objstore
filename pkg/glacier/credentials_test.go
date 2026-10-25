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

//go:build glacier

package glacier

import (
	"errors"
	"testing"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// A credential never travels in the settings map, even for a backend that
// ignores the Credentials argument.
func TestConfigureRejectsCredentialInSettings(t *testing.T) {
	for _, k := range []string{"accessKey", "secretKey", "accountKey", "sasToken", "secret_access_key"} {
		settings := map[string]string{"vaultName": "v", "region": "us-east-1", k: "v"}
		if err := New().(*Glacier).Configure(settings, noCreds); !errors.Is(err, common.ErrCredentialInSettings) {
			t.Errorf("Configure with %q in settings: err = %v, want ErrCredentialInSettings", k, err)
		}
	}
}
