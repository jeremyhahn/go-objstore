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
	"reflect"
	"strings"

	"github.com/spf13/viper"
)

// EnvPrefix is the environment variable prefix for every server setting:
// the product name, per the company configuration convention.
const EnvPrefix = "OBJSTORE"

// configKey names both the -config flag and the OBJSTORE_CONFIG variable.
const configKey = "config"

// BindFunc registers a binary's flags on fs, each pointing at the Config
// field it sets and defaulting to that field's current value.
type BindFunc func(fs *flag.FlagSet, c *Config)

// Load resolves a binary's configuration from args and the process
// environment with the precedence flags > environment > file > defaults.
//
// defaults is the defaults layer. The YAML file named by -config (or, when
// that flag is not given, OBJSTORE_CONFIG) is optional; when named it must
// exist and may only contain known keys. Environment variables are
// OBJSTORE_<KEY>. A flag overrides the lower layers only when it was set on
// the command line, so a flag's default never masks the file or environment.
// The result is validated before it is returned.
//
// Errors are typed: *FlagError (command line), *ConfigFileError (file),
// *DecodeError (unknown key or mistyped value) and joined
// *InvalidSettingError values (validation). flag.ErrHelp is returned
// unwrapped when -h or -help is given.
func Load(name string, args []string, defaults Config, bind BindFunc) (*Config, error) {
	// Parse the command line into a scratch copy only to learn which flags
	// were set and to what.
	parsed := defaults
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	configPath := fs.String(configKey, "", "Path to a YAML configuration file (env "+EnvVar(configKey)+")")
	bind(fs, &parsed)
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil, err
		}
		return nil, &FlagError{Err: err}
	}
	if fs.NArg() > 0 {
		return nil, &FlagError{Args: fs.Args()}
	}
	changed := make(map[string]*flag.Flag)
	fs.Visit(func(f *flag.Flag) { changed[f.Name] = f })

	// Defaults → file → environment.
	v := viper.New()
	setDefaults(v, "", reflect.ValueOf(defaults))
	v.SetEnvPrefix(EnvPrefix)
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	path := v.GetString(configKey)
	if _, ok := changed[configKey]; ok {
		path = *configPath
	}
	if path != "" {
		v.SetConfigFile(path)
		v.SetConfigType("yaml")
		if err := v.ReadInConfig(); err != nil {
			return nil, &ConfigFileError{Path: path, Err: err}
		}
	}

	var resolved Config
	if err := v.UnmarshalExact(&resolved); err != nil {
		return nil, &DecodeError{Err: err}
	}

	// Flags last, and only the ones actually given.
	overlay := flag.NewFlagSet(name, flag.ContinueOnError)
	overlay.String(configKey, "", "")
	bind(overlay, &resolved)
	for flagName, f := range changed {
		if flagName == configKey {
			continue
		}
		if err := overlay.Set(flagName, f.Value.String()); err != nil {
			return nil, &FlagError{Flag: flagName, Err: err}
		}
	}

	if err := resolved.Validate(); err != nil {
		return nil, err
	}
	return &resolved, nil
}

// setDefaults registers every leaf field of the defaults struct under its
// dotted mapstructure key. Registering every key is also what makes
// AutomaticEnv visible to Unmarshal for keys absent from the file.
func setDefaults(v *viper.Viper, prefix string, rv reflect.Value) {
	rt := rv.Type()
	for i := 0; i < rt.NumField(); i++ {
		field := rt.Field(i)
		key := field.Tag.Get("mapstructure")
		if prefix != "" {
			key = prefix + "." + key
		}
		if field.Type.Kind() == reflect.Struct {
			setDefaults(v, key, rv.Field(i))
			continue
		}
		v.SetDefault(key, rv.Field(i).Interface())
	}
}

// EnvVar returns the environment variable that sets a dotted config key.
func EnvVar(key string) string {
	return EnvPrefix + "_" + strings.ToUpper(strings.ReplaceAll(key, ".", "_"))
}

func envHint(key string) string {
	return " (config " + key + ", env " + EnvVar(key) + ")"
}
