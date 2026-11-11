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
	"math"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	grpcserver "github.com/jeremyhahn/go-objstore/pkg/server/grpc"
	mcpserver "github.com/jeremyhahn/go-objstore/pkg/server/mcp"
	quicserver "github.com/jeremyhahn/go-objstore/pkg/server/quic"
	restserver "github.com/jeremyhahn/go-objstore/pkg/server/rest"
	unixserver "github.com/jeremyhahn/go-objstore/pkg/server/unix"
)

// Config is the resolved configuration of an objstore server binary. The
// mapstructure tags are the config-file keys; the environment variable for a
// key is OBJSTORE_ followed by the upper-cased key path joined with "_".
// Each binary reads the sections for the transports it serves.
type Config struct {
	// Backend is the storage backend type (local, s3, gcs, azure, ...).
	Backend string `mapstructure:"backend"`
	// Path is the storage path for the local backend.
	Path string `mapstructure:"path"`

	GRPC      GRPCConfig      `mapstructure:"grpc"`
	REST      RESTConfig      `mapstructure:"rest"`
	QUIC      QUICConfig      `mapstructure:"quic"`
	MCP       MCPConfig       `mapstructure:"mcp"`
	Unix      UnixConfig      `mapstructure:"unix"`
	RateLimit RateLimitConfig `mapstructure:"rate_limit"`
	Audit     AuditConfig     `mapstructure:"audit"`
	Archive   ArchiveConfig   `mapstructure:"archive"`
	// Lifecycle configures lifecycle policies on the local backend
	// (objstore-server only).
	Lifecycle LifecycleConfig `mapstructure:"lifecycle"`
	// Auth, when set, makes every enabled listener authenticate and
	// authorize its clients (see auth.go). Empty keeps NoOp auth.
	Auth AuthConfig `mapstructure:"auth"`

	Replication ReplicationConfig `mapstructure:"replication"`
}

// Bounds of grpc.max_recv_msg_size and grpc.max_send_msg_size. Below 1 MiB a
// server cannot carry a Get chunk with its framing or any useful object; gRPC
// itself cannot frame a message above math.MaxInt32 bytes.
const (
	MinGRPCMessageSize = 1 << 20
	MaxGRPCMessageSize = math.MaxInt32
)

// GRPCConfig configures the gRPC transport.
type GRPCConfig struct {
	Enabled        bool   `mapstructure:"enabled"`
	Addr           string `mapstructure:"addr"`
	TLSCert        string `mapstructure:"tls_cert"`
	TLSKey         string `mapstructure:"tls_key"`
	MaxRecvMsgSize int    `mapstructure:"max_recv_msg_size"`
	MaxSendMsgSize int    `mapstructure:"max_send_msg_size"`
}

// RESTConfig configures the REST transport.
type RESTConfig struct {
	Enabled       bool   `mapstructure:"enabled"`
	Host          string `mapstructure:"host"`
	Port          int    `mapstructure:"port"`
	MetricsPublic bool   `mapstructure:"metrics_public"`
	TLSCert       string `mapstructure:"tls_cert"`
	TLSKey        string `mapstructure:"tls_key"`
}

// QUICConfig configures the QUIC/HTTP3 transport.
type QUICConfig struct {
	Enabled      bool          `mapstructure:"enabled"`
	Addr         string        `mapstructure:"addr"`
	TLSCert      string        `mapstructure:"tls_cert"`
	TLSKey       string        `mapstructure:"tls_key"`
	SelfSigned   bool          `mapstructure:"self_signed"`
	MaxBodySize  int64         `mapstructure:"max_body_size"`
	ReadTimeout  time.Duration `mapstructure:"read_timeout"`
	WriteTimeout time.Duration `mapstructure:"write_timeout"`
	IdleTimeout  time.Duration `mapstructure:"idle_timeout"`
	MaxStreams   int64         `mapstructure:"max_streams"`
}

// MCPConfig configures the MCP transport.
type MCPConfig struct {
	Enabled bool   `mapstructure:"enabled"`
	Mode    string `mapstructure:"mode"`
	Addr    string `mapstructure:"addr"`
	TLSCert string `mapstructure:"tls_cert"`
	TLSKey  string `mapstructure:"tls_key"`
}

// UnixConfig configures the Unix socket transport.
type UnixConfig struct {
	Enabled bool   `mapstructure:"enabled"`
	Socket  string `mapstructure:"socket"`
}

// RateLimitConfig configures rate limiting shared by every transport.
type RateLimitConfig struct {
	Enabled   bool    `mapstructure:"enabled"`
	RPS       float64 `mapstructure:"rps"`
	Burst     int     `mapstructure:"burst"`
	PerClient bool    `mapstructure:"per_client"`
}

// AuditConfig configures audit logging shared by every transport.
type AuditConfig struct {
	Enabled bool `mapstructure:"enabled"`
}

// ArchiveConfig holds the settings for archive calls and lifecycle policies
// that name a destination archiver.
type ArchiveConfig struct {
	// AllowClientLocalDestinations lets a client name a local (filesystem
	// path) archiver as an archive or lifecycle-policy destination. Off by
	// default: the client would choose a directory the service user writes
	// into.
	AllowClientLocalDestinations bool `mapstructure:"allow_client_local_destinations"`
}

// LifecycleConfig holds the lifecycle settings objstore-server passes to the
// local backend. Other backends ignore them.
type LifecycleConfig struct {
	// Run starts a scheduled lifecycle pass that applies every policy.
	Run bool `mapstructure:"run"`
	// Manager is the lifecycle manager type: "persistent" keeps policies
	// across restarts in PolicyFile; "memory" loses them on exit.
	Manager string `mapstructure:"manager"`
	// PolicyFile is the file, relative to path, that holds the persistent
	// policies. Empty uses the backend's default (.lifecycle-policies.json).
	PolicyFile string `mapstructure:"policy_file"`
	// Interval is the time between scheduled lifecycle passes.
	Interval time.Duration `mapstructure:"interval"`
}

// Lifecycle manager types accepted by lifecycle.manager.
const (
	LifecycleManagerPersistent = "persistent"
	LifecycleManagerMemory     = "memory"
)

// DefaultLifecycleInterval is the default time between scheduled lifecycle
// passes.
const DefaultLifecycleInterval = time.Hour

// StorageSettings returns the settings objstore-server passes to its storage
// backend: path, and for the local backend the lifecycle settings.
func (c *Config) StorageSettings() map[string]string {
	settings := map[string]string{"path": c.Path}
	if c.Backend != "local" {
		return settings
	}
	if c.Lifecycle.Manager != "" {
		settings["lifecycleManagerType"] = c.Lifecycle.Manager
	}
	if c.Lifecycle.PolicyFile != "" {
		// The store reserves the policy file's name as a key, so it gets
		// the canonical spelling: "./p" and "a/../p" reserve the key "p".
		// Validate refuses a path that has none.
		policyFile, err := CanonicalPolicyFile(c.Lifecycle.PolicyFile)
		if err != nil {
			policyFile = c.Lifecycle.PolicyFile
		}
		settings["lifecyclePolicyFile"] = policyFile
	}
	if c.Lifecycle.Interval > 0 {
		settings["lifecycleInterval"] = c.Lifecycle.Interval.String()
	}
	settings["runLifecycle"] = strconv.FormatBool(c.Lifecycle.Run)
	return settings
}

// validateLifecycle checks the lifecycle section when the local backend
// uses it.
func (c *Config) validateLifecycle() []error {
	if c.Backend != "local" {
		return nil
	}
	var errs []error
	switch c.Lifecycle.Manager {
	case "", LifecycleManagerPersistent, LifecycleManagerMemory:
	default:
		errs = append(errs, &InvalidSettingError{
			Key: "lifecycle.manager", Value: c.Lifecycle.Manager, Reason: "must be persistent or memory",
		})
	}
	if c.Lifecycle.PolicyFile != "" {
		if _, err := CanonicalPolicyFile(c.Lifecycle.PolicyFile); err != nil {
			errs = append(errs, &InvalidSettingError{
				Key: "lifecycle.policy_file", Value: c.Lifecycle.PolicyFile, Reason: err.Error(),
			})
		}
	}
	if c.Lifecycle.Run && c.Lifecycle.Interval <= 0 {
		errs = append(errs, &InvalidSettingError{
			Key: "lifecycle.interval", Value: c.Lifecycle.Interval.String(), Reason: "must be a positive duration",
		})
	}
	return errs
}

// CanonicalPolicyFile returns the canonical, slash-separated spelling of a
// lifecycle policy file path relative to the storage path, the spelling the
// store reserves as a key. A path that is empty once cleaned, absolute, or
// that climbs out of the storage path with ".." is refused.
// Errors CanonicalPolicyFile returns for a lifecycle policy file path it
// refuses (#224).
var (
	// ErrPolicyPathNotRelative reports an absolute policy file path.
	ErrPolicyPathNotRelative = errors.New("must be relative to the storage path")
	// ErrPolicyPathNoFile reports a policy file path that names no file.
	ErrPolicyPathNoFile = errors.New("must name a file")
	// ErrPolicyPathEscapes reports a policy file path that leaves the storage path.
	ErrPolicyPathEscapes = errors.New("must not leave the storage path")
)

func CanonicalPolicyFile(p string) (string, error) {
	if filepath.IsAbs(p) || filepath.VolumeName(p) != "" || strings.HasPrefix(filepath.ToSlash(p), "/") {
		return "", ErrPolicyPathNotRelative
	}
	clean := filepath.ToSlash(filepath.Clean(p))
	switch {
	case clean == "." || clean == "":
		return "", ErrPolicyPathNoFile
	case clean == ".." || strings.HasPrefix(clean, "../"):
		return "", ErrPolicyPathEscapes
	}
	return clean, nil
}

// LoopbackHost is the host the server's listeners bind by default. Exposing
// a listener beyond the machine takes an explicit address in the file, the
// environment or a flag.
const LoopbackHost = "127.0.0.1"

// onLoopback returns addr with its host replaced by LoopbackHost, keeping
// the port. An addr that is not host:port is returned unchanged, for
// Validate to report.
func onLoopback(addr string) string {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	return net.JoinHostPort(LoopbackHost, port)
}

// Defaults is the defaults layer. Every listen port is taken from the owning
// transport package's own defaults so the value exists in exactly one place;
// the file, environment and flag layers override it.
//
// The defaults are safe to start as is: there is no default storage path,
// so the local backend needs one set explicitly; gRPC, REST and MCP listen
// on loopback only; QUIC and MCP are off until enabled.
func Defaults() Config {
	rest := restserver.DefaultServerConfig()
	quic := quicserver.DefaultOptions()
	mcp := mcpserver.DefaultServerConfig()
	return Config{
		Backend: "local",
		GRPC: GRPCConfig{
			Enabled:        true,
			Addr:           onLoopback(grpcserver.DefaultServerOptions().Address),
			MaxRecvMsgSize: grpcserver.DefaultMaxReceiveMessageSize,
			MaxSendMsgSize: grpcserver.DefaultMaxSendMessageSize,
		},
		REST: RESTConfig{
			Enabled:       true,
			Host:          LoopbackHost,
			Port:          rest.Port,
			MetricsPublic: rest.MetricsPublic,
		},
		QUIC: QUICConfig{
			Enabled:      false,
			Addr:         onLoopback(quic.Addr),
			MaxBodySize:  quic.MaxRequestBodySize,
			ReadTimeout:  quic.ReadTimeout,
			WriteTimeout: quic.WriteTimeout,
			IdleTimeout:  quic.IdleTimeout,
			MaxStreams:   quic.MaxBiStreams,
		},
		MCP: MCPConfig{
			Enabled: false,
			Mode:    string(mcp.Mode),
			Addr:    onLoopback(mcp.HTTPAddress),
		},
		Unix: UnixConfig{
			Enabled: false,
			Socket:  unixserver.DefaultConfig().SocketPath,
		},
		RateLimit: RateLimitConfig{
			Enabled:   false,
			RPS:       100,
			Burst:     200,
			PerClient: false,
		},
		Audit: AuditConfig{Enabled: true},
		Lifecycle: LifecycleConfig{
			Run:      true,
			Manager:  LifecycleManagerPersistent,
			Interval: DefaultLifecycleInterval,
		},
	}
}

// RESTServerDefaults is the defaults layer of objstore-rest-server, which
// has always bound the loopback name by default rather than every interface.
func RESTServerDefaults() Config {
	d := Defaults()
	d.REST.Host = "localhost"
	return d
}

// QUICServerDefaults is the defaults layer of objstore-quic-server, which
// keeps local-backend data under ./data by default.
func QUICServerDefaults() Config {
	d := Defaults()
	d.Path = "./data"
	return d
}

// RESTAddr is the host:port the REST listener binds, for display.
func (c *Config) RESTAddr() string {
	return net.JoinHostPort(c.REST.Host, strconv.Itoa(c.REST.Port))
}

// MCPMode returns the validated MCP server mode.
func (c *Config) MCPMode() (mcpserver.ServerMode, error) {
	switch mode := mcpserver.ServerMode(c.MCP.Mode); mode {
	case mcpserver.ModeStdio, mcpserver.ModeHTTP:
		return mode, nil
	default:
		return "", &InvalidSettingError{
			Key:    "mcp.mode",
			Value:  c.MCP.Mode,
			Reason: "must be " + joinQuoted(string(mcpserver.ModeStdio), string(mcpserver.ModeHTTP)),
		}
	}
}

// Validate reports every problem in the configuration at once, as joined
// *InvalidSettingError values.
func (c *Config) Validate() error {
	var errs []error
	if c.Backend == "" {
		errs = append(errs, &InvalidSettingError{Key: "backend", Reason: "must not be empty"})
	}
	if c.Backend == "local" && c.Path == "" {
		errs = append(errs, &InvalidSettingError{Key: "path", Reason: "must be set when backend is local"})
	}
	if c.REST.Port < 0 || c.REST.Port > 65535 {
		errs = append(errs, &InvalidSettingError{
			Key: "rest.port", Value: strconv.Itoa(c.REST.Port), Reason: "must be within 0-65535",
		})
	}
	for _, a := range []struct{ key, addr string }{
		{"grpc.addr", c.GRPC.Addr},
		{"quic.addr", c.QUIC.Addr},
		{"mcp.addr", c.MCP.Addr},
	} {
		if _, _, err := net.SplitHostPort(a.addr); err != nil {
			errs = append(errs, &InvalidSettingError{Key: a.key, Value: a.addr, Reason: "must be host:port", Err: err})
		}
	}
	for _, pair := range []struct{ section, cert, key string }{
		{"grpc", c.GRPC.TLSCert, c.GRPC.TLSKey},
		{"quic", c.QUIC.TLSCert, c.QUIC.TLSKey},
	} {
		if (pair.cert == "") != (pair.key == "") {
			errs = append(errs, &InvalidSettingError{
				Key:    pair.section + ".tls_cert/" + pair.section + ".tls_key",
				Value:  pair.cert + "/" + pair.key,
				Reason: "certificate and key must be set together",
			})
		}
	}
	for _, m := range []struct {
		key  string
		size int
	}{
		{"grpc.max_recv_msg_size", c.GRPC.MaxRecvMsgSize},
		{"grpc.max_send_msg_size", c.GRPC.MaxSendMsgSize},
	} {
		if m.size < MinGRPCMessageSize || m.size > MaxGRPCMessageSize {
			errs = append(errs, &InvalidSettingError{
				Key: m.key, Value: strconv.Itoa(m.size),
				Reason: "must be within " + strconv.Itoa(MinGRPCMessageSize) + "-" + strconv.Itoa(MaxGRPCMessageSize),
			})
		}
	}
	if _, err := c.MCPMode(); err != nil {
		errs = append(errs, err)
	}
	errs = append(errs, c.validateLifecycle()...)
	errs = append(errs, c.validateAuth()...)
	return errors.Join(errs...)
}
