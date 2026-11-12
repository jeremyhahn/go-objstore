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

package serverconfig

import (
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestLifecycleConfig_Defaults(t *testing.T) {
	got := Defaults().Lifecycle
	want := LifecycleConfig{Run: true, Manager: LifecycleManagerPersistent, Interval: time.Hour}
	if got != want {
		t.Fatalf("Defaults().Lifecycle = %+v, want %+v", got, want)
	}
}

func TestLifecycleConfig_Layers(t *testing.T) {
	cases := []struct {
		name string
		yaml string
		env  map[string]string
		args []string
		want LifecycleConfig
	}{
		{name: "default", want: LifecycleConfig{Run: true, Manager: "persistent", Interval: time.Hour}},
		{
			name: "file",
			yaml: "lifecycle:\n  run: false\n  manager: memory\n  policy_file: policies/life.json\n  interval: 5m\n",
			want: LifecycleConfig{Run: false, Manager: "memory", PolicyFile: "policies/life.json", Interval: 5 * time.Minute},
		},
		{
			name: "env",
			env:  map[string]string{"OBJSTORE_LIFECYCLE_RUN": "false", "OBJSTORE_LIFECYCLE_INTERVAL": "30s"},
			want: LifecycleConfig{Run: false, Manager: "persistent", Interval: 30 * time.Second},
		},
		{
			name: "flags",
			args: []string{"-lifecycle-run=false", "-lifecycle-manager", "memory",
				"-lifecycle-policy-file", "p.json", "-lifecycle-interval", "2m"},
			want: LifecycleConfig{Run: false, Manager: "memory", PolicyFile: "p.json", Interval: 2 * time.Minute},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			for k, v := range c.env {
				t.Setenv(k, v)
			}
			args := c.args
			if c.yaml != "" {
				args = append([]string{"-config", writeConfig(t, c.yaml)}, args...)
			}
			cfg := mustLoad(t, args, Defaults(), BindServerFlags)
			if cfg.Lifecycle != c.want {
				t.Fatalf("Lifecycle = %+v, want %+v", cfg.Lifecycle, c.want)
			}
		})
	}
}

func TestLifecycleConfig_Validate(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(c *Config)
		wantKey string
	}{
		{name: "bad manager", mutate: func(c *Config) { c.Lifecycle.Manager = "disk" }, wantKey: "lifecycle.manager"},
		{name: "zero interval", mutate: func(c *Config) { c.Lifecycle.Interval = 0 }, wantKey: "lifecycle.interval"},
		{name: "negative interval", mutate: func(c *Config) { c.Lifecycle.Interval = -time.Second }, wantKey: "lifecycle.interval"},
		{name: "zero interval, not run", mutate: func(c *Config) { c.Lifecycle.Run = false; c.Lifecycle.Interval = 0 }},
		{name: "bad manager, other backend", mutate: func(c *Config) { c.Backend = "s3"; c.Lifecycle.Manager = "disk" }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := Defaults()
			cfg.Path = testPath
			c.mutate(&cfg)
			err := cfg.Validate()
			if c.wantKey == "" {
				if err != nil {
					t.Fatalf("Validate() = %v, want nil", err)
				}
				return
			}
			var ise *InvalidSettingError
			if !errors.As(err, &ise) || ise.Key != c.wantKey {
				t.Fatalf("Validate() = %v, want an InvalidSettingError for %s", err, c.wantKey)
			}
		})
	}
}

func TestConfig_StorageSettings(t *testing.T) {
	cfg := Defaults()
	cfg.Path = "/data"
	cfg.Lifecycle.PolicyFile = "life.json"
	got := cfg.StorageSettings()
	want := map[string]string{
		"path":                 "/data",
		"lifecycleManagerType": "persistent",
		"lifecyclePolicyFile":  "life.json",
		"lifecycleInterval":    "1h0m0s",
		"runLifecycle":         "true",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("StorageSettings() = %v, want %v", got, want)
	}

	cfg.Lifecycle.Run = false
	if got := cfg.StorageSettings()["runLifecycle"]; got != "false" {
		t.Fatalf("runLifecycle = %q with lifecycle.run false, want \"false\"", got)
	}

	cfg.Backend = "s3"
	if got := cfg.StorageSettings(); !reflect.DeepEqual(got, map[string]string{"path": "/data"}) {
		t.Fatalf("StorageSettings() for s3 = %v, want path only", got)
	}
}

// TestLifecycleFlags_EveryBinary proves each server binary accepts the
// -lifecycle-* flags and that StorageSettings hands them to the backend, so
// a single-transport server runs lifecycle policies as objstore-server does.
func TestLifecycleFlags_EveryBinary(t *testing.T) {
	cases := []struct {
		binary   string
		defaults Config
		bind     BindFunc
	}{
		{"objstore-server", Defaults(), BindServerFlags},
		{"objstore-grpc-server", Defaults(), BindGRPCServerFlags},
		{"objstore-rest-server", RESTServerDefaults(), BindRESTServerFlags},
		{"objstore-quic-server", QUICServerDefaults(), BindQUICServerFlags},
		{"objstore-mcp-server", Defaults(), BindMCPServerFlags},
	}
	args := []string{
		"-backend", "local",
		"-lifecycle-run=false",
		"-lifecycle-manager", "memory",
		"-lifecycle-policy-file", "rules/life.json",
		"-lifecycle-interval", "90s",
	}
	for _, c := range cases {
		t.Run(c.binary, func(t *testing.T) {
			cfg := mustLoad(t, args, c.defaults, c.bind)
			want := LifecycleConfig{Run: false, Manager: "memory", PolicyFile: "rules/life.json", Interval: 90 * time.Second}
			if cfg.Lifecycle != want {
				t.Fatalf("Lifecycle = %+v, want %+v", cfg.Lifecycle, want)
			}
			got := cfg.StorageSettings()
			wantSettings := map[string]string{
				"path":                 cfg.Path,
				"lifecycleManagerType": "memory",
				"lifecyclePolicyFile":  "rules/life.json",
				"lifecycleInterval":    "1m30s",
				"runLifecycle":         "false",
			}
			if !reflect.DeepEqual(got, wantSettings) {
				t.Fatalf("StorageSettings() = %v, want %v", got, wantSettings)
			}
		})
	}
}

// Two spellings of one policy file reserve the same key, and a path that
// leaves the storage path is refused (#224).
func TestConfig_PolicyFileCanonical(t *testing.T) {
	for _, spelling := range []string{"policies/life.json", "./policies/life.json", "a/../policies/life.json", "policies//life.json", "policies/./life.json"} {
		cfg := Defaults()
		cfg.Path = testPath
		cfg.Lifecycle.PolicyFile = spelling
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate() with %q = %v, want nil", spelling, err)
		}
		if got := cfg.StorageSettings()["lifecyclePolicyFile"]; got != "policies/life.json" {
			t.Fatalf("lifecyclePolicyFile for %q = %q, want %q", spelling, got, "policies/life.json")
		}
	}

	for bad, want := range map[string]error{
		"..":                   ErrPolicyPathEscapes,
		"../life.json":         ErrPolicyPathEscapes,
		"a/../../life.json":    ErrPolicyPathEscapes,
		"/etc/life.json":       ErrPolicyPathNotRelative,
		".":                    ErrPolicyPathNoFile,
		"./":                   ErrPolicyPathNoFile,
		"a/..":                 ErrPolicyPathNoFile,
		"logs.metadata.json":   ErrPolicyPathSidecar,
		"a/LOGS.Metadata.JSON": ErrPolicyPathSidecar,
	} {
		cfg := Defaults()
		cfg.Path = testPath
		cfg.Lifecycle.PolicyFile = bad
		var ise *InvalidSettingError
		if err := cfg.Validate(); !errors.As(err, &ise) || ise.Key != "lifecycle.policy_file" {
			t.Fatalf("Validate() with %q = %v, want an InvalidSettingError for lifecycle.policy_file", bad, err)
		}
		if _, err := CanonicalPolicyFile(bad); !errors.Is(err, want) {
			t.Fatalf("CanonicalPolicyFile(%q) = %v, want %v", bad, err, want)
		}
	}
}
