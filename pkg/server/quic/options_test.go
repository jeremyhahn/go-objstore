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
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/local"

	"github.com/quic-go/quic-go"
)

func TestDefaultOptions(t *testing.T) {
	opts := DefaultOptions()

	if opts.Addr != ":4433" {
		t.Errorf("Expected default addr :4433, got %s", opts.Addr)
	}

	if opts.MaxRequestBodySize != 100*1024*1024 {
		t.Errorf("Expected default max body size 100MB, got %d", opts.MaxRequestBodySize)
	}

	if opts.ReadTimeout != 30*time.Second {
		t.Errorf("Expected default read timeout 30s, got %v", opts.ReadTimeout)
	}

	if opts.WriteTimeout != 30*time.Second {
		t.Errorf("Expected default write timeout 30s, got %v", opts.WriteTimeout)
	}

	if opts.IdleTimeout != 60*time.Second {
		t.Errorf("Expected default idle timeout 60s, got %v", opts.IdleTimeout)
	}

	if opts.MaxBiStreams != 100 {
		t.Errorf("Expected default max bi streams 100, got %d", opts.MaxBiStreams)
	}

	if opts.MaxUniStreams != 100 {
		t.Errorf("Expected default max uni streams 100, got %d", opts.MaxUniStreams)
	}

	if opts.EnableDatagrams {
		t.Error("Expected datagrams to be disabled by default")
	}

	if opts.QUICConfig == nil {
		t.Error("Expected QUIC config to be initialized")
	}
}

func TestOptionsValidate(t *testing.T) {
	storage := local.New()
	storage.Configure(map[string]string{"path": t.TempDir()})
	tlsConfig, _ := GenerateSelfSignedCert()

	tests := []struct {
		name    string
		opts    *Options
		wantErr error
	}{
		{
			name: "valid options",
			opts: &Options{
				Addr:               ":4433",
				Storage:            storage,
				TLSConfig:          tlsConfig,
				MaxRequestBodySize: 100 * 1024 * 1024,
				ReadTimeout:        30 * time.Second,
				WriteTimeout:       30 * time.Second,
				IdleTimeout:        60 * time.Second,
				QUICConfig:         &quic.Config{},
			},
			wantErr: nil,
		},
		{
			name: "missing address",
			opts: &Options{
				Storage:   storage,
				TLSConfig: tlsConfig,
			},
			wantErr: ErrInvalidAddr,
		},
		{
			name: "missing storage",
			opts: &Options{
				Addr:      ":4433",
				TLSConfig: tlsConfig,
			},
			wantErr: ErrStorageRequired,
		},
		{
			name: "missing TLS config",
			opts: &Options{
				Addr:    ":4433",
				Storage: storage,
			},
			wantErr: ErrTLSConfigRequired,
		},
		{
			name: "auto-fill defaults",
			opts: &Options{
				Addr:      ":4433",
				Storage:   storage,
				TLSConfig: tlsConfig,
			},
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.opts.Validate()
			if err != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}

			// Check auto-filled defaults
			if err == nil {
				if tt.opts.MaxRequestBodySize <= 0 {
					t.Error("Expected MaxRequestBodySize to be auto-filled")
				}
				if tt.opts.ReadTimeout <= 0 {
					t.Error("Expected ReadTimeout to be auto-filled")
				}
				if tt.opts.WriteTimeout <= 0 {
					t.Error("Expected WriteTimeout to be auto-filled")
				}
				if tt.opts.IdleTimeout <= 0 {
					t.Error("Expected IdleTimeout to be auto-filled")
				}
				if tt.opts.QUICConfig == nil {
					t.Error("Expected QUICConfig to be auto-filled")
				}
			}
		})
	}
}

func TestOptionsBuilderPattern(t *testing.T) {
	storage := local.New()
	storage.Configure(map[string]string{"path": t.TempDir()})
	tlsConfig, _ := GenerateSelfSignedCert()
	quicConfig := &quic.Config{}

	opts := DefaultOptions().
		WithAddr(":5000").
		WithStorage(storage).
		WithTLSConfig(tlsConfig).
		WithQUICConfig(quicConfig).
		WithMaxRequestBodySize(50*1024*1024).
		WithTimeouts(15*time.Second, 20*time.Second, 45*time.Second).
		WithStreamLimits(200, 150).
		WithDatagrams(true)

	if opts.Addr != ":5000" {
		t.Errorf("Expected addr :5000, got %s", opts.Addr)
	}

	if opts.Storage != storage {
		t.Error("Expected storage to be set")
	}

	if opts.TLSConfig != tlsConfig {
		t.Error("Expected TLS config to be set")
	}

	if opts.QUICConfig != quicConfig {
		t.Error("Expected QUIC config to be set")
	}

	if opts.MaxRequestBodySize != 50*1024*1024 {
		t.Errorf("Expected max body size 50MB, got %d", opts.MaxRequestBodySize)
	}

	if opts.ReadTimeout != 15*time.Second {
		t.Errorf("Expected read timeout 15s, got %v", opts.ReadTimeout)
	}

	if opts.WriteTimeout != 20*time.Second {
		t.Errorf("Expected write timeout 20s, got %v", opts.WriteTimeout)
	}

	if opts.IdleTimeout != 45*time.Second {
		t.Errorf("Expected idle timeout 45s, got %v", opts.IdleTimeout)
	}

	if opts.MaxBiStreams != 200 {
		t.Errorf("Expected max bi streams 200, got %d", opts.MaxBiStreams)
	}

	if opts.MaxUniStreams != 150 {
		t.Errorf("Expected max uni streams 150, got %d", opts.MaxUniStreams)
	}

	if !opts.EnableDatagrams {
		t.Error("Expected datagrams to be enabled")
	}
}

func TestOptionsValidateSyncsQUICConfig(t *testing.T) {
	storage := local.New()
	storage.Configure(map[string]string{"path": t.TempDir()})
	tlsConfig, _ := GenerateSelfSignedCert()

	opts := &Options{
		Addr:            ":4433",
		Storage:         storage,
		TLSConfig:       tlsConfig,
		IdleTimeout:     90 * time.Second,
		MaxBiStreams:    150,
		MaxUniStreams:   120,
		EnableDatagrams: true,
		QUICConfig:      &quic.Config{},
	}

	err := opts.Validate()
	if err != nil {
		t.Fatalf("Validate() failed: %v", err)
	}

	if opts.QUICConfig.MaxIdleTimeout != 90*time.Second {
		t.Errorf("Expected QUIC MaxIdleTimeout 90s, got %v", opts.QUICConfig.MaxIdleTimeout)
	}

	if opts.QUICConfig.MaxIncomingStreams != 150 {
		t.Errorf("Expected QUIC MaxIncomingStreams 150, got %d", opts.QUICConfig.MaxIncomingStreams)
	}

	if opts.QUICConfig.MaxIncomingUniStreams != 120 {
		t.Errorf("Expected QUIC MaxIncomingUniStreams 120, got %d", opts.QUICConfig.MaxIncomingUniStreams)
	}

	if !opts.QUICConfig.EnableDatagrams {
		t.Error("Expected QUIC EnableDatagrams to be true")
	}
}

func TestWithLogger(t *testing.T) {
	opts := DefaultOptions()
	logger := adapters.NewDefaultLogger()

	opts.WithLogger(logger)

	if opts.Logger == nil {
		t.Error("Logger should be set")
	}
}

func TestWithAuthenticator(t *testing.T) {
	opts := DefaultOptions()
	auth := adapters.NewNoOpAuthenticator()

	opts.WithAuthenticator(auth)

	if opts.Authenticator == nil {
		t.Error("Authenticator should be set")
	}
}

func TestWithAdapterTLS(t *testing.T) {
	opts := DefaultOptions()
	tlsConfig := adapters.NewTLSConfig()

	opts.WithAdapterTLS(tlsConfig)

	if opts.AdapterTLSConfig == nil {
		t.Error("AdapterTLSConfig should be set")
	}
}
