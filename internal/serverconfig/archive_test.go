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

import "testing"

// allowLocalArchive reports, per transport, whether the options struct that
// transport is built from lets a client name a local archive destination.
var allowLocalArchive = map[string]func(t *testing.T, cfg *Config, mw Middleware) bool{
	"grpc": func(t *testing.T, cfg *Config, mw Middleware) bool {
		return applyGRPC(t, cfg, mw).AllowClientLocalArchiveDestinations
	},
	"rest": func(_ *testing.T, cfg *Config, mw Middleware) bool {
		return cfg.RESTServerConfig(mw).AllowClientLocalArchiveDestinations
	},
	"quic": func(_ *testing.T, cfg *Config, mw Middleware) bool {
		return cfg.QUICOptions(mw).AllowClientLocalArchiveDestinations
	},
	"mcp": func(t *testing.T, cfg *Config, mw Middleware) bool {
		sc, err := cfg.MCPServerConfig(mw)
		if err != nil {
			t.Fatalf("MCPServerConfig: %v", err)
		}
		return sc.AllowClientLocalArchiveDestinations
	},
	"unix": func(_ *testing.T, cfg *Config, mw Middleware) bool {
		return cfg.UnixServerConfig(mw).AllowClientLocalArchiveDestinations
	},
}

// TestArchiveAllowClientLocal_ReachesEveryTransport proves
// archive.allow_client_local_destinations is false by default and that the
// value set through the file, the environment or the flag reaches the
// options struct of all five transports.
func TestArchiveAllowClientLocal_ReachesEveryTransport(t *testing.T) {
	if Defaults().Archive.AllowClientLocalDestinations {
		t.Fatal("Defaults() allows client local archive destinations")
	}
	cases := []struct {
		name string
		env  map[string]string
		yaml string
		args []string
		want bool
	}{
		{name: "default", want: false},
		{name: "file true", yaml: "archive:\n  allow_client_local_destinations: true\n", want: true},
		{name: "env true", env: map[string]string{"OBJSTORE_ARCHIVE_ALLOW_CLIENT_LOCAL_DESTINATIONS": "true"}, want: true},
		{name: "flag true", args: []string{"-allow-client-local-archive"}, want: true},
		{name: "flag false overrides env", env: map[string]string{"OBJSTORE_ARCHIVE_ALLOW_CLIENT_LOCAL_DESTINATIONS": "true"},
			args: []string{"-allow-client-local-archive=false"}, want: false},
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
			mw := cfg.Middleware()
			for _, name := range []string{"grpc", "rest", "quic", "mcp", "unix"} {
				if got := allowLocalArchive[name](t, cfg, mw); got != c.want {
					t.Errorf("%s: AllowClientLocalArchiveDestinations = %v, want %v", name, got, c.want)
				}
			}
		})
	}
}
