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

package local

import (
	"bytes"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// TestLocal_PutToPolicyStagingKeyRefused: a client with object write access
// cannot PUT the key the persistent lifecycle manager stages its policy file
// under, for the default policy file or a custom one, and the policies it
// saved stay as they were.
func TestLocal_PutToPolicyStagingKeyRefused(t *testing.T) {
	for _, policyFile := range []string{"", "rules.json"} {
		l := new(Local)
		settings := map[string]string{
			"path":                 t.TempDir(),
			"lifecycleManagerType": "persistent",
		}
		if policyFile != "" {
			settings["lifecyclePolicyFile"] = policyFile
		}
		if err := l.Configure(settings, noCreds); err != nil {
			t.Fatal(err)
		}
		if err := l.AddPolicy(common.LifecyclePolicy{ID: "keep", Prefix: "logs/", Retention: time.Hour, Action: "delete"}); err != nil {
			t.Fatal(err)
		}
		file := policyFile
		if file == "" {
			file = common.LifecyclePolicyFile
		}
		staging := common.PolicyStagingName(file)
		if err := l.Put(staging, bytes.NewBufferString(`{"policies":[]}`)); err == nil {
			t.Errorf("policy file %q: PUT to staging key %q succeeded", file, staging)
		}
		if err := l.AddPolicy(common.LifecyclePolicy{ID: "second", Prefix: "tmp/", Retention: time.Hour, Action: "delete"}); err != nil {
			t.Fatal(err)
		}
		policies, err := l.GetPolicies()
		if err != nil {
			t.Fatal(err)
		}
		if len(policies) != 2 {
			t.Errorf("policy file %q: got %d policies, want 2", file, len(policies))
		}
		l.Close()
	}
}
