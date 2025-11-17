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

package quic

import (
	"crypto/tls"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/quic-go/quic-go"
)

// Options contains configuration options for the QUIC server.
type Options struct {
	// Addr is the UDP address to listen on (e.g., ":4433")
	Addr string

	// Storage is the storage backend to use
	Storage common.Storage

	// TLSConfig is the TLS configuration for QUIC
	// Must be TLS 1.3 or later
	TLSConfig *tls.Config

	// QUICConfig contains QUIC-specific configuration
	QUICConfig *quic.Config

	// MaxRequestBodySize is the maximum size of request bodies in bytes
	// Default: 100MB
	MaxRequestBodySize int64

	// ReadTimeout is the timeout for reading requests
	// Default: 30 seconds
	ReadTimeout time.Duration

	// WriteTimeout is the timeout for writing responses
	// Default: 30 seconds
	WriteTimeout time.Duration

	// IdleTimeout is the idle timeout for QUIC connections
	// Default: 60 seconds
	IdleTimeout time.Duration

	// MaxBiStreams is the maximum number of bidirectional streams per connection
	// Default: 100
	MaxBiStreams int64

	// MaxUniStreams is the maximum number of unidirectional streams per connection
	// Default: 100
	MaxUniStreams int64

	// EnableDatagrams enables QUIC datagram support (RFC 9221)
	EnableDatagrams bool

	// Logger is the pluggable logger adapter (default: DefaultLogger)
	Logger adapters.Logger

	// Authenticator is the pluggable authentication adapter (default: NoOpAuthenticator)
	Authenticator adapters.Authenticator

	// AdapterTLSConfig is the TLS/mTLS configuration using the adapter (preferred over TLSConfig)
	AdapterTLSConfig *adapters.TLSConfig
}

// DefaultOptions returns a new Options instance with sensible defaults.
func DefaultOptions() *Options {
	return &Options{
		Addr:               ":4433",
		MaxRequestBodySize: 100 * 1024 * 1024, // 100MB
		ReadTimeout:        30 * time.Second,
		WriteTimeout:       30 * time.Second,
		IdleTimeout:        60 * time.Second,
		MaxBiStreams:       100,
		MaxUniStreams:      100,
		EnableDatagrams:    false,
		Logger:             adapters.NewDefaultLogger(),
		Authenticator:      adapters.NewNoOpAuthenticator(),
		AdapterTLSConfig:   nil, // Must be set by user
		QUICConfig: &quic.Config{
			MaxIdleTimeout:                 60 * time.Second,
			MaxIncomingStreams:             100,
			MaxIncomingUniStreams:          100,
			KeepAlivePeriod:                30 * time.Second,
			EnableDatagrams:                false,
			MaxStreamReceiveWindow:         6 * 1024 * 1024,  // 6MB
			MaxConnectionReceiveWindow:     15 * 1024 * 1024, // 15MB
			DisablePathMTUDiscovery:        false,
			Allow0RTT:                      false,       // Disable 0-RTT for security
			InitialStreamReceiveWindow:     512 * 1024,  // 512KB
			InitialConnectionReceiveWindow: 1024 * 1024, // 1MB
		},
	}
}

// Validate checks if the options are valid.
func (o *Options) Validate() error {
	if o.Addr == "" {
		return ErrInvalidAddr
	}

	if o.Storage == nil {
		return ErrStorageRequired
	}

	// Build TLS config from AdapterTLSConfig if provided
	if o.AdapterTLSConfig != nil {
		tlsConfig, err := o.AdapterTLSConfig.Build()
		if err != nil {
			return err
		}
		o.TLSConfig = tlsConfig
	}

	if o.TLSConfig == nil {
		return ErrTLSConfigRequired
	}

	if o.MaxRequestBodySize <= 0 {
		o.MaxRequestBodySize = 100 * 1024 * 1024
	}

	if o.ReadTimeout <= 0 {
		o.ReadTimeout = 30 * time.Second
	}

	if o.WriteTimeout <= 0 {
		o.WriteTimeout = 30 * time.Second
	}

	if o.IdleTimeout <= 0 {
		o.IdleTimeout = 60 * time.Second
	}

	if o.QUICConfig == nil {
		o.QUICConfig = DefaultOptions().QUICConfig
	}

	// Sync QUIC config with options
	o.QUICConfig.MaxIdleTimeout = o.IdleTimeout
	o.QUICConfig.MaxIncomingStreams = o.MaxBiStreams
	o.QUICConfig.MaxIncomingUniStreams = o.MaxUniStreams
	o.QUICConfig.EnableDatagrams = o.EnableDatagrams

	return nil
}

// WithAddr sets the UDP address to listen on.
func (o *Options) WithAddr(addr string) *Options {
	o.Addr = addr
	return o
}

// WithStorage sets the storage backend.
func (o *Options) WithStorage(storage common.Storage) *Options {
	o.Storage = storage
	return o
}

// WithTLSConfig sets the TLS configuration.
func (o *Options) WithTLSConfig(config *tls.Config) *Options {
	o.TLSConfig = config
	return o
}

// WithQUICConfig sets the QUIC configuration.
func (o *Options) WithQUICConfig(config *quic.Config) *Options {
	o.QUICConfig = config
	return o
}

// WithMaxRequestBodySize sets the maximum request body size.
func (o *Options) WithMaxRequestBodySize(size int64) *Options {
	o.MaxRequestBodySize = size
	return o
}

// WithTimeouts sets the read, write, and idle timeouts.
func (o *Options) WithTimeouts(read, write, idle time.Duration) *Options {
	o.ReadTimeout = read
	o.WriteTimeout = write
	o.IdleTimeout = idle
	return o
}

// WithStreamLimits sets the maximum number of streams.
func (o *Options) WithStreamLimits(biStreams, uniStreams int64) *Options {
	o.MaxBiStreams = biStreams
	o.MaxUniStreams = uniStreams
	return o
}

// WithDatagrams enables or disables QUIC datagram support.
func (o *Options) WithDatagrams(enabled bool) *Options {
	o.EnableDatagrams = enabled
	return o
}

// WithLogger sets the logger adapter.
func (o *Options) WithLogger(logger adapters.Logger) *Options {
	o.Logger = logger
	return o
}

// WithAuthenticator sets the authentication adapter.
func (o *Options) WithAuthenticator(auth adapters.Authenticator) *Options {
	o.Authenticator = auth
	return o
}

// WithAdapterTLS sets the TLS configuration using the adapter.
func (o *Options) WithAdapterTLS(config *adapters.TLSConfig) *Options {
	o.AdapterTLSConfig = config
	return o
}
