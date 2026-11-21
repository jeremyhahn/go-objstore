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
	"errors"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
)

// TestLocal_PutToPolicySidecarKeyRefused: when the lifecycle policy file is
// named like an object's metadata sidecar, a client cannot PUT (or update the
// metadata of, or delete) that object, and the policies survive a restart.
func TestLocal_PutToPolicySidecarKeyRefused(t *testing.T) {
	dir := t.TempDir()
	settings := map[string]string{
		"path":                 dir,
		"lifecycleManagerType": "persistent",
		"lifecyclePolicyFile":  "logs.metadata.json",
	}
	l := new(Local)
	if err := l.Configure(settings, noCreds); err != nil {
		t.Fatal(err)
	}
	if err := l.AddPolicy(common.LifecyclePolicy{ID: "keep", Prefix: "logs/", Retention: time.Hour, Action: "delete"}); err != nil {
		t.Fatal(err)
	}

	for _, key := range []string{"logs", "./logs", "LOGS"} {
		err := l.Put(key, bytes.NewBufferString("payload"))
		if !errors.Is(err, ErrKeyAliasesPolicyFile) {
			t.Errorf("Put(%q) = %v, want ErrKeyAliasesPolicyFile", key, err)
		}
		var ve *common.ValidationError
		if !errors.As(err, &ve) {
			t.Errorf("Put(%q) = %v, want a ValidationError", key, err)
		}
	}
	if err := l.UpdateMetadata(t.Context(), "logs", &common.Metadata{ContentType: "text/plain"}); err == nil {
		t.Error("UpdateMetadata(logs) succeeded")
	}
	if err := l.Delete("logs"); !errors.Is(err, ErrKeyAliasesPolicyFile) {
		t.Errorf("Delete(logs) = %v, want ErrKeyAliasesPolicyFile", err)
	}
	if err := l.Put("other", bytes.NewBufferString("payload")); err != nil {
		t.Fatalf("Put(other) = %v", err)
	}
	l.Close()

	restarted := new(Local)
	if err := restarted.Configure(settings, noCreds); err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	policies, err := restarted.GetPolicies()
	if err != nil {
		t.Fatal(err)
	}
	if len(policies) != 1 || policies[0].ID != "keep" {
		t.Errorf("policies after restart = %+v, want the one policy \"keep\"", policies)
	}
}
