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
	"strings"
	"testing"
)

var errCause = errors.New("underlying cause")

// TestErrorTypes checks every error type names its context in the message
// and unwraps to its cause.
func TestErrorTypes(t *testing.T) {
	cases := []struct {
		name    string
		err     error
		mention []string
		cause   bool
	}{
		{"flag parse", &FlagError{Err: errCause}, []string{"invalid command line", "underlying cause"}, true},
		{"flag value", &FlagError{Flag: "rest-port", Err: errCause}, []string{"-rest-port", "underlying cause"}, true},
		{"positional args", &FlagError{Args: []string{"extra"}}, []string{"unexpected arguments", "extra"}, false},
		{"config file", &ConfigFileError{Path: "/etc/objstore.yaml", Err: errCause}, []string{"/etc/objstore.yaml", "underlying cause"}, true},
		{"decode", &DecodeError{Err: errCause}, []string{"cannot decode", "underlying cause"}, true},
		{"invalid setting", &InvalidSettingError{Key: "rest.port", Value: "-1", Reason: "must be within 0-65535"}, []string{"rest.port", `"-1"`, "0-65535"}, false},
		{"invalid setting with cause", &InvalidSettingError{Key: "grpc.addr", Value: "x", Reason: "must be host:port", Err: errCause}, []string{"grpc.addr", "host:port", "underlying cause"}, true},
		{"tls credentials", &TLSCredentialsError{Transport: "grpc", CertFile: "c.pem", KeyFile: "k.pem", Err: errCause}, []string{"grpc.tls_cert", "c.pem", "grpc.tls_key", "k.pem", "underlying cause"}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			msg := c.err.Error()
			for _, m := range c.mention {
				if !strings.Contains(msg, m) {
					t.Errorf("message %q does not mention %q", msg, m)
				}
			}
			if got := errors.Is(c.err, errCause); got != c.cause {
				t.Errorf("errors.Is(cause) = %v, want %v", got, c.cause)
			}
		})
	}
}

func TestMCPServerConfig_RejectsInvalidMode(t *testing.T) {
	cfg := Defaults()
	cfg.MCP.Mode = "sse"
	_, err := cfg.MCPServerConfig(cfg.Middleware())
	var ise *InvalidSettingError
	if !errors.As(err, &ise) || ise.Key != "mcp.mode" {
		t.Errorf("err = %v, want *InvalidSettingError for mcp.mode", err)
	}
}
