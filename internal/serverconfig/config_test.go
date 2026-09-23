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
	"flag"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	grpcserver "github.com/jeremyhahn/go-objstore/pkg/server/grpc"
	mcpserver "github.com/jeremyhahn/go-objstore/pkg/server/mcp"
	quicserver "github.com/jeremyhahn/go-objstore/pkg/server/quic"
	restserver "github.com/jeremyhahn/go-objstore/pkg/server/rest"
	unixserver "github.com/jeremyhahn/go-objstore/pkg/server/unix"
)

// writeConfig writes a YAML config file and returns its path.
func writeConfig(t *testing.T, yaml string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "objstore.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

func mustLoad(t *testing.T, args []string, defaults Config, bind BindFunc) *Config {
	t.Helper()
	cfg, err := Load("test", args, defaults, bind)
	if err != nil {
		t.Fatalf("Load(%q): %v", args, err)
	}
	return cfg
}

// TestDefaults_ComeFromTransportPackages verifies the defaults layer takes
// every listen address from the owning transport package.
func TestDefaults_ComeFromTransportPackages(t *testing.T) {
	d := Defaults()
	rest := restserver.DefaultServerConfig()
	quic := quicserver.DefaultOptions()

	checks := []struct {
		name      string
		got, want any
	}{
		{"grpc.addr", d.GRPC.Addr, grpcserver.DefaultServerOptions().Address},
		{"rest.host", d.REST.Host, rest.Host},
		{"rest.port", d.REST.Port, rest.Port},
		{"quic.addr", d.QUIC.Addr, quic.Addr},
		{"quic.max_body_size", d.QUIC.MaxBodySize, quic.MaxRequestBodySize},
		{"quic.read_timeout", d.QUIC.ReadTimeout, quic.ReadTimeout},
		{"quic.max_streams", d.QUIC.MaxStreams, quic.MaxBiStreams},
		{"mcp.addr", d.MCP.Addr, mcpserver.DefaultServerConfig().HTTPAddress},
		{"mcp.mode", d.MCP.Mode, string(mcpserver.DefaultServerConfig().Mode)},
		{"unix.socket", d.Unix.Socket, unixserver.DefaultConfig().SocketPath},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s default = %v, want %v", c.name, c.got, c.want)
		}
	}
	if err := d.Validate(); err != nil {
		t.Errorf("defaults do not validate: %v", err)
	}
}

func TestBinaryDefaults(t *testing.T) {
	if got := RESTServerDefaults().REST.Host; got != "localhost" {
		t.Errorf("objstore-rest-server default host = %q, want localhost", got)
	}
	if got := QUICServerDefaults().Path; got != "./data" {
		t.Errorf("objstore-quic-server default path = %q, want ./data", got)
	}
}

// TestLoad_RESTHostPrecedence walks the REST bind host of objstore-server —
// the setting this layer was introduced for — through every layer.
func TestLoad_RESTHostPrecedence(t *testing.T) {
	file := writeConfig(t, "rest:\n  host: 10.0.0.1\n")

	t.Run("defaults", func(t *testing.T) {
		cfg := mustLoad(t, nil, Defaults(), BindServerFlags)
		if want := restserver.DefaultServerConfig().Host; cfg.REST.Host != want {
			t.Errorf("host = %q, want default %q", cfg.REST.Host, want)
		}
	})
	t.Run("file over defaults", func(t *testing.T) {
		cfg := mustLoad(t, []string{"-config", file}, Defaults(), BindServerFlags)
		if cfg.REST.Host != "10.0.0.1" {
			t.Errorf("host = %q, want file value 10.0.0.1", cfg.REST.Host)
		}
	})
	t.Run("env over file", func(t *testing.T) {
		t.Setenv("OBJSTORE_REST_HOST", "10.0.0.2")
		cfg := mustLoad(t, []string{"-config", file}, Defaults(), BindServerFlags)
		if cfg.REST.Host != "10.0.0.2" {
			t.Errorf("host = %q, want env value 10.0.0.2", cfg.REST.Host)
		}
	})
	t.Run("flag over env", func(t *testing.T) {
		t.Setenv("OBJSTORE_REST_HOST", "10.0.0.2")
		cfg := mustLoad(t, []string{"-config", file, "-rest-host", "10.0.0.3"}, Defaults(), BindServerFlags)
		if cfg.REST.Host != "10.0.0.3" {
			t.Errorf("host = %q, want flag value 10.0.0.3", cfg.REST.Host)
		}
	})
	t.Run("unset flag does not mask env", func(t *testing.T) {
		t.Setenv("OBJSTORE_REST_HOST", "10.0.0.2")
		cfg := mustLoad(t, []string{"-rest-port", "9090"}, Defaults(), BindServerFlags)
		if cfg.REST.Host != "10.0.0.2" || cfg.REST.Port != 9090 {
			t.Errorf("host:port = %s, want 10.0.0.2:9090", cfg.RESTAddr())
		}
	})
	t.Run("reaches the REST server config", func(t *testing.T) {
		t.Setenv("OBJSTORE_REST_HOST", "10.0.0.2")
		t.Setenv("OBJSTORE_REST_PORT", "9443")
		cfg := mustLoad(t, nil, Defaults(), BindServerFlags)
		sc := cfg.RESTServerConfig(cfg.Middleware())
		if sc.Host != "10.0.0.2" || sc.Port != 9443 {
			t.Errorf("REST server config host:port = %s:%d, want 10.0.0.2:9443", sc.Host, sc.Port)
		}
		if got := cfg.RESTAddr(); got != "10.0.0.2:9443" {
			t.Errorf("RESTAddr() = %q, want 10.0.0.2:9443", got)
		}
	})
}

// TestLoad_EveryListenerSettableByEveryPath proves each binary's listen
// settings are reachable from the file, the environment and its flag, with
// the flag beating the environment beating the file.
func TestLoad_EveryListenerSettableByEveryPath(t *testing.T) {
	type listener struct {
		binary   string
		defaults Config
		bind     BindFunc
		key      string // dotted config key
		flag     string
		values   [3]string // file, env, flag
		get      func(*Config) string
	}
	grpcAddr := func(c *Config) string { return c.GRPC.Addr }
	restHost := func(c *Config) string { return c.REST.Host }
	restPort := func(c *Config) string { return c.RESTAddr()[strings.LastIndex(c.RESTAddr(), ":")+1:] }
	quicAddr := func(c *Config) string { return c.QUICOptions(c.Middleware()).Addr }
	mcpAddr := func(c *Config) string {
		sc, err := c.MCPServerConfig(c.Middleware())
		if err != nil {
			t.Fatalf("MCPServerConfig: %v", err)
		}
		return sc.HTTPAddress
	}
	unixSocket := func(c *Config) string { return c.Unix.Socket }
	addrs := [3]string{"10.1.0.1:7001", "10.1.0.2:7002", "10.1.0.3:7003"}
	hosts := [3]string{"10.1.0.1", "10.1.0.2", "10.1.0.3"}
	ports := [3]string{"7001", "7002", "7003"}

	cases := []listener{
		{"objstore-server", Defaults(), BindServerFlags, "grpc.addr", "grpc-addr", addrs, grpcAddr},
		{"objstore-server", Defaults(), BindServerFlags, "rest.host", "rest-host", hosts, restHost},
		{"objstore-server", Defaults(), BindServerFlags, "rest.port", "rest-port", ports, restPort},
		{"objstore-server", Defaults(), BindServerFlags, "quic.addr", "quic-addr", addrs, quicAddr},
		{"objstore-server", Defaults(), BindServerFlags, "mcp.addr", "mcp-addr", addrs, mcpAddr},
		{"objstore-server", Defaults(), BindServerFlags, "unix.socket", "unix-socket", [3]string{"/a.sock", "/b.sock", "/c.sock"}, unixSocket},
		{"objstore-grpc-server", Defaults(), BindGRPCServerFlags, "grpc.addr", "addr", addrs, grpcAddr},
		{"objstore-rest-server", RESTServerDefaults(), BindRESTServerFlags, "rest.host", "host", hosts, restHost},
		{"objstore-rest-server", RESTServerDefaults(), BindRESTServerFlags, "rest.port", "port", ports, restPort},
		{"objstore-quic-server", QUICServerDefaults(), BindQUICServerFlags, "quic.addr", "addr", addrs, quicAddr},
		{"objstore-mcp-server", Defaults(), BindMCPServerFlags, "mcp.addr", "addr", addrs, mcpAddr},
	}

	for _, c := range cases {
		t.Run(c.binary+"/"+c.key, func(t *testing.T) {
			section, leaf, _ := strings.Cut(c.key, ".")
			file := writeConfig(t, section+":\n  "+leaf+": \""+c.values[0]+"\"\n")
			env := EnvVar(c.key)

			cfg := mustLoad(t, []string{"-config", file}, c.defaults, c.bind)
			if got := c.get(cfg); got != c.values[0] {
				t.Errorf("file: got %q, want %q", got, c.values[0])
			}

			t.Setenv(env, c.values[1])
			cfg = mustLoad(t, []string{"-config", file}, c.defaults, c.bind)
			if got := c.get(cfg); got != c.values[1] {
				t.Errorf("env %s over file: got %q, want %q", env, got, c.values[1])
			}

			cfg = mustLoad(t, []string{"-config", file, "-" + c.flag, c.values[2]}, c.defaults, c.bind)
			if got := c.get(cfg); got != c.values[2] {
				t.Errorf("flag -%s over env: got %q, want %q", c.flag, got, c.values[2])
			}
		})
	}
}

func TestLoad_ConfigFileFromEnvironment(t *testing.T) {
	envFile := writeConfig(t, "grpc:\n  addr: \"10.2.0.1:1\"\n")
	flagFile := writeConfig(t, "grpc:\n  addr: \"10.2.0.2:2\"\n")
	t.Setenv("OBJSTORE_CONFIG", envFile)

	cfg := mustLoad(t, nil, Defaults(), BindGRPCServerFlags)
	if cfg.GRPC.Addr != "10.2.0.1:1" {
		t.Errorf("OBJSTORE_CONFIG file not read: addr = %q", cfg.GRPC.Addr)
	}
	cfg = mustLoad(t, []string{"-config", flagFile}, Defaults(), BindGRPCServerFlags)
	if cfg.GRPC.Addr != "10.2.0.2:2" {
		t.Errorf("-config should win over OBJSTORE_CONFIG: addr = %q", cfg.GRPC.Addr)
	}
}

// TestLoad_TypedValues covers non-string settings through env and flags.
func TestLoad_TypedValues(t *testing.T) {
	t.Setenv("OBJSTORE_QUIC_READ_TIMEOUT", "5s")
	t.Setenv("OBJSTORE_QUIC_MAX_STREAMS", "7")
	t.Setenv("OBJSTORE_RATE_LIMIT_ENABLED", "true")
	t.Setenv("OBJSTORE_RATE_LIMIT_RPS", "2.5")
	t.Setenv("OBJSTORE_AUDIT_ENABLED", "false")
	file := writeConfig(t, "quic:\n  idle_timeout: 90s\nrate_limit:\n  burst: 3\n")

	cfg := mustLoad(t, []string{"-config", file, "-writetimeout", "1m"}, QUICServerDefaults(), BindQUICServerFlags)
	opts := cfg.QUICOptions(cfg.Middleware())
	if opts.ReadTimeout != 5*time.Second || opts.WriteTimeout != time.Minute || opts.IdleTimeout != 90*time.Second {
		t.Errorf("timeouts = %v/%v/%v, want 5s/1m/90s", opts.ReadTimeout, opts.WriteTimeout, opts.IdleTimeout)
	}
	if opts.MaxBiStreams != 7 || opts.MaxUniStreams != 7 {
		t.Errorf("stream limits = %d/%d, want 7/7", opts.MaxBiStreams, opts.MaxUniStreams)
	}
	if !cfg.RateLimit.Enabled || cfg.Audit.Enabled {
		t.Errorf("rate_limit.enabled=%v audit.enabled=%v, want true/false", cfg.RateLimit.Enabled, cfg.Audit.Enabled)
	}
	rl := cfg.RateLimitConfig()
	if rl.RequestsPerSecond != 2.5 || rl.Burst != 3 || rl.PerIP {
		t.Errorf("rate limit = %+v, want rps 2.5 burst 3 global", rl)
	}
}

func TestLoad_BoolFlagOverridesEnv(t *testing.T) {
	t.Setenv("OBJSTORE_GRPC_ENABLED", "true")
	cfg := mustLoad(t, []string{"-grpc=false"}, Defaults(), BindServerFlags)
	if cfg.GRPC.Enabled {
		t.Error("-grpc=false should override OBJSTORE_GRPC_ENABLED=true")
	}
}

func TestLoad_MCPMode(t *testing.T) {
	t.Setenv("OBJSTORE_MCP_MODE", "stdio")
	cfg := mustLoad(t, nil, Defaults(), BindMCPServerFlags)
	sc, err := cfg.MCPServerConfig(cfg.Middleware())
	if err != nil {
		t.Fatalf("MCPServerConfig: %v", err)
	}
	if sc.Mode != mcpserver.ModeStdio {
		t.Errorf("mode = %q, want stdio", sc.Mode)
	}
}

// invalidKeys returns the Key of every *InvalidSettingError in err.
func invalidKeys(err error) []string {
	var errs []error
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		errs = joined.Unwrap()
	} else if err != nil {
		errs = []error{err}
	}
	var keys []string
	for _, e := range errs {
		var ise *InvalidSettingError
		if errors.As(e, &ise) {
			keys = append(keys, ise.Key)
		}
	}
	return keys
}

func TestLoad_Errors(t *testing.T) {
	isFlagError := func(flagName string, args bool) func(*testing.T, error) {
		return func(t *testing.T, err error) {
			var fe *FlagError
			if !errors.As(err, &fe) {
				t.Fatalf("err = %v (%T), want *FlagError", err, err)
			}
			if fe.Flag != flagName || (len(fe.Args) > 0) != args {
				t.Errorf("FlagError = %+v, want Flag %q, positional args %v", fe, flagName, args)
			}
		}
	}
	isConfigFileError := func(t *testing.T, err error) {
		var cfe *ConfigFileError
		if !errors.As(err, &cfe) {
			t.Fatalf("err = %v (%T), want *ConfigFileError", err, err)
		}
		if cfe.Path == "" || cfe.Err == nil {
			t.Errorf("ConfigFileError = %+v, want path and cause", cfe)
		}
	}
	isDecodeError := func(t *testing.T, err error) {
		var de *DecodeError
		if !errors.As(err, &de) || de.Err == nil {
			t.Fatalf("err = %v (%T), want *DecodeError with cause", err, err)
		}
	}
	isInvalid := func(key string) func(*testing.T, error) {
		return func(t *testing.T, err error) {
			keys := invalidKeys(err)
			if len(keys) != 1 || keys[0] != key {
				t.Errorf("invalid keys = %v, want [%s] (err %v)", keys, key, err)
			}
		}
	}

	cases := []struct {
		name  string
		env   map[string]string
		yaml  string
		args  []string
		check func(*testing.T, error)
	}{
		{name: "missing config file", args: []string{"-config", "/nonexistent/objstore.yaml"}, check: isConfigFileError},
		{name: "malformed config file", yaml: "rest: [unclosed\n", check: isConfigFileError},
		{name: "unknown config key", yaml: "rest:\n  hots: 10.0.0.1\n", check: isDecodeError},
		{name: "env not a number", env: map[string]string{"OBJSTORE_REST_PORT": "http"}, check: isDecodeError},
		{name: "port out of range", args: []string{"-rest-port", "70000"}, check: isInvalid("rest.port")},
		{name: "address without port", env: map[string]string{"OBJSTORE_GRPC_ADDR": "10.0.0.1"}, check: func(t *testing.T, err error) {
			isInvalid("grpc.addr")(t, err)
			var ise *InvalidSettingError
			if errors.As(err, &ise) && ise.Err == nil {
				t.Error("address error does not carry the parse cause")
			}
		}},
		{name: "invalid mcp mode", args: []string{"-mcp-mode", "sse"}, check: isInvalid("mcp.mode")},
		{name: "grpc cert without key", args: []string{"-grpc-tls-cert", "/etc/objstore/grpc.crt"}, check: isInvalid("grpc.tls_cert/grpc.tls_key")},
		{name: "quic key without cert", env: map[string]string{"OBJSTORE_QUIC_TLS_KEY": "/etc/objstore/quic.key"}, check: isInvalid("quic.tls_cert/quic.tls_key")},
		{name: "empty backend", args: []string{"-backend", ""}, check: isInvalid("backend")},
		{name: "unknown flag", args: []string{"-no-such-flag"}, check: isFlagError("", false)},
		{name: "bad flag value", args: []string{"-rest-port", "http"}, check: isFlagError("", false)},
		{name: "positional argument", args: []string{"extra"}, check: isFlagError("", true)},
		{name: "help", args: []string{"-h"}, check: func(t *testing.T, err error) {
			if !errors.Is(err, flag.ErrHelp) {
				t.Errorf("err = %v, want flag.ErrHelp", err)
			}
		}},
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
			cfg, err := Load("test", args, Defaults(), BindServerFlags)
			if cfg != nil {
				t.Errorf("Load returned a config alongside error %v", err)
			}
			c.check(t, err)
			if err != nil && err.Error() == "" {
				t.Error("empty error message")
			}
		})
	}
}

func TestValidate_ReportsEveryProblem(t *testing.T) {
	cfg := Defaults()
	cfg.Backend = ""
	cfg.REST.Port = -1
	cfg.QUIC.Addr = "nope"
	cfg.MCP.Mode = "sse"
	err := cfg.Validate()
	want := []string{"backend", "rest.port", "quic.addr", "mcp.mode"}
	if got := invalidKeys(err); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("invalid keys = %v, want %v", got, want)
	}
	for _, key := range want {
		if !strings.Contains(err.Error(), key) {
			t.Errorf("error does not name %s: %v", key, err)
		}
	}
}

func TestEnvVar(t *testing.T) {
	if got := EnvVar("rate_limit.per_client"); got != "OBJSTORE_RATE_LIMIT_PER_CLIENT" {
		t.Errorf("EnvVar = %q", got)
	}
}

// TestExampleConfig_MatchesDefaults loads configs/objstore-server.yaml.example
// with every key uncommented and requires the result to equal the defaults
// layer, so the shipped template names every key and documents its default.
func TestExampleConfig_MatchesDefaults(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "configs", "objstore-server.yaml.example"))
	if err != nil {
		t.Fatalf("read example: %v", err)
	}
	// Keys are commented as "# key:" or "#   key:"; prose comments never start
	// with a lower-case key followed by a colon and are dropped.
	keyLine := regexp.MustCompile(`^# ( *[a-z_]+:.*)$`)
	var lines []string
	for _, line := range strings.Split(string(raw), "\n") {
		if m := keyLine.FindStringSubmatch(line); m != nil {
			lines = append(lines, m[1])
		}
	}
	file := writeConfig(t, strings.Join(lines, "\n")+"\n")

	var zero Config
	cfg := mustLoad(t, []string{"-config", file}, zero, BindServerFlags)
	if *cfg != Defaults() {
		t.Errorf("example config differs from Defaults():\n got %+v\nwant %+v", *cfg, Defaults())
	}
}
