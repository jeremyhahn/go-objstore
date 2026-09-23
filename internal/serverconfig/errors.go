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
	"fmt"
	"strings"
)

// FlagError reports a command line that cannot be parsed or applied: a parse
// failure (Flag empty), a value a flag rejects (Flag set), or stray
// positional arguments (Args set).
type FlagError struct {
	Flag string
	Args []string
	Err  error
}

func (e *FlagError) Error() string {
	switch {
	case len(e.Args) > 0:
		return fmt.Sprintf("serverconfig: unexpected arguments %q", e.Args)
	case e.Flag != "":
		return "serverconfig: flag -" + e.Flag + ": " + e.Err.Error()
	default:
		return "serverconfig: invalid command line: " + e.Err.Error()
	}
}

func (e *FlagError) Unwrap() error { return e.Err }

// ConfigFileError reports a configuration file, named by -config or
// OBJSTORE_CONFIG, that cannot be read or parsed.
type ConfigFileError struct {
	Path string
	Err  error
}

func (e *ConfigFileError) Error() string {
	return fmt.Sprintf("serverconfig: cannot load configuration file %q: %v", e.Path, e.Err)
}

func (e *ConfigFileError) Unwrap() error { return e.Err }

// DecodeError reports layered settings (file and environment) that cannot be
// decoded into the configuration: an unknown file key or a value of the
// wrong type.
type DecodeError struct {
	Err error
}

func (e *DecodeError) Error() string {
	return "serverconfig: cannot decode configuration: " + e.Err.Error()
}

func (e *DecodeError) Unwrap() error { return e.Err }

// InvalidSettingError reports one resolved setting that fails validation.
// Validate joins one per problem. Err carries an underlying parse error when
// there is one.
type InvalidSettingError struct {
	Key    string
	Value  string
	Reason string
	Err    error
}

func (e *InvalidSettingError) Error() string {
	msg := fmt.Sprintf("serverconfig: invalid %s %q: %s", e.Key, e.Value, e.Reason)
	if e.Err != nil {
		msg += ": " + e.Err.Error()
	}
	return msg
}

func (e *InvalidSettingError) Unwrap() error { return e.Err }

// TLSCredentialsError reports a configured certificate/key pair that cannot
// be loaded for a transport.
type TLSCredentialsError struct {
	Transport string
	CertFile  string
	KeyFile   string
	Err       error
}

func (e *TLSCredentialsError) Error() string {
	return fmt.Sprintf("serverconfig: cannot load %s TLS credentials (%s.tls_cert %q, %s.tls_key %q): %v",
		e.Transport, e.Transport, e.CertFile, e.Transport, e.KeyFile, e.Err)
}

func (e *TLSCredentialsError) Unwrap() error { return e.Err }

// joinQuoted renders allowed values for an InvalidSettingError reason.
func joinQuoted(values ...string) string {
	quoted := make([]string, len(values))
	for i, v := range values {
		quoted[i] = fmt.Sprintf("%q", v)
	}
	return strings.Join(quoted, " or ")
}
