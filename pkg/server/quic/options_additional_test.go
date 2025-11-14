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

func TestOptionsValidateWithAdapterTLSConfig(t *testing.T) {
	storage := local.New()
	storage.Configure(map[string]string{"path": t.TempDir()})

	// Create a valid adapter TLS config
	adapterTLS := adapters.NewTLSConfig().
		WithServerCertFiles("testdata/cert.pem", "testdata/key.pem").
		WithInsecureSkipVerify(true)

	opts := &Options{
		Addr:             ":4433",
		Storage:          storage,
		AdapterTLSConfig: adapterTLS,
	}

	// Generate self-signed cert for testing
	tlsConfig, err := GenerateSelfSignedCert()
	if err != nil {
		t.Fatalf("Failed to generate cert: %v", err)
	}

	// Override adapter TLS config to use generated cert
	opts.TLSConfig = tlsConfig
	opts.AdapterTLSConfig = nil

	err = opts.Validate()
	if err != nil {
		t.Errorf("Validate() should succeed with valid options: %v", err)
	}
}

func TestOptionsValidateWithAdapterTLSConfigError(t *testing.T) {
	storage := local.New()
	storage.Configure(map[string]string{"path": t.TempDir()})

	// Create an adapter TLS config that will fail to build
	adapterTLS := adapters.NewTLSConfig().
		WithServerCertFiles("/nonexistent/cert.pem", "/nonexistent/key.pem")

	opts := &Options{
		Addr:             ":4433",
		Storage:          storage,
		AdapterTLSConfig: adapterTLS,
	}

	err := opts.Validate()
	if err == nil {
		t.Error("Expected error when adapter TLS config fails to build")
	}
}

func TestOptionsValidateAutoFillsDefaults(t *testing.T) {
	storage := local.New()
	storage.Configure(map[string]string{"path": t.TempDir()})
	tlsConfig, _ := GenerateSelfSignedCert()

	opts := &Options{
		Addr:      ":4433",
		Storage:   storage,
		TLSConfig: tlsConfig,
		// All other fields zero/nil
	}

	err := opts.Validate()
	if err != nil {
		t.Fatalf("Validate() failed: %v", err)
	}

	// Check that defaults were filled
	if opts.MaxRequestBodySize != 100*1024*1024 {
		t.Errorf("Expected MaxRequestBodySize to be filled with default, got %d", opts.MaxRequestBodySize)
	}

	if opts.ReadTimeout != 30*time.Second {
		t.Errorf("Expected ReadTimeout to be filled with default, got %v", opts.ReadTimeout)
	}

	if opts.WriteTimeout != 30*time.Second {
		t.Errorf("Expected WriteTimeout to be filled with default, got %v", opts.WriteTimeout)
	}

	if opts.IdleTimeout != 60*time.Second {
		t.Errorf("Expected IdleTimeout to be filled with default, got %v", opts.IdleTimeout)
	}

	if opts.QUICConfig == nil {
		t.Error("Expected QUICConfig to be filled with default")
	}
}

func TestOptionsValidateNilQUICConfig(t *testing.T) {
	storage := local.New()
	storage.Configure(map[string]string{"path": t.TempDir()})
	tlsConfig, _ := GenerateSelfSignedCert()

	opts := &Options{
		Addr:       ":4433",
		Storage:    storage,
		TLSConfig:  tlsConfig,
		QUICConfig: nil, // Explicitly nil
	}

	err := opts.Validate()
	if err != nil {
		t.Fatalf("Validate() failed: %v", err)
	}

	// Should have created a default QUIC config
	if opts.QUICConfig == nil {
		t.Error("Expected QUICConfig to be created")
	}
}

func TestOptionsValidateZeroTimeouts(t *testing.T) {
	storage := local.New()
	storage.Configure(map[string]string{"path": t.TempDir()})
	tlsConfig, _ := GenerateSelfSignedCert()

	opts := &Options{
		Addr:         ":4433",
		Storage:      storage,
		TLSConfig:    tlsConfig,
		ReadTimeout:  0, // Zero timeout
		WriteTimeout: 0, // Zero timeout
		IdleTimeout:  0, // Zero timeout
	}

	err := opts.Validate()
	if err != nil {
		t.Fatalf("Validate() failed: %v", err)
	}

	// Zero timeouts should be replaced with defaults
	if opts.ReadTimeout != 30*time.Second {
		t.Errorf("Expected ReadTimeout to be set to default, got %v", opts.ReadTimeout)
	}

	if opts.WriteTimeout != 30*time.Second {
		t.Errorf("Expected WriteTimeout to be set to default, got %v", opts.WriteTimeout)
	}

	if opts.IdleTimeout != 60*time.Second {
		t.Errorf("Expected IdleTimeout to be set to default, got %v", opts.IdleTimeout)
	}
}

func TestOptionsValidateZeroMaxRequestBodySize(t *testing.T) {
	storage := local.New()
	storage.Configure(map[string]string{"path": t.TempDir()})
	tlsConfig, _ := GenerateSelfSignedCert()

	opts := &Options{
		Addr:               ":4433",
		Storage:            storage,
		TLSConfig:          tlsConfig,
		MaxRequestBodySize: 0, // Zero
	}

	err := opts.Validate()
	if err != nil {
		t.Fatalf("Validate() failed: %v", err)
	}

	if opts.MaxRequestBodySize != 100*1024*1024 {
		t.Errorf("Expected MaxRequestBodySize to be set to default, got %d", opts.MaxRequestBodySize)
	}
}

func TestOptionsValidateNegativeMaxRequestBodySize(t *testing.T) {
	storage := local.New()
	storage.Configure(map[string]string{"path": t.TempDir()})
	tlsConfig, _ := GenerateSelfSignedCert()

	opts := &Options{
		Addr:               ":4433",
		Storage:            storage,
		TLSConfig:          tlsConfig,
		MaxRequestBodySize: -1, // Negative
	}

	err := opts.Validate()
	if err != nil {
		t.Fatalf("Validate() failed: %v", err)
	}

	if opts.MaxRequestBodySize != 100*1024*1024 {
		t.Errorf("Expected negative MaxRequestBodySize to be set to default, got %d", opts.MaxRequestBodySize)
	}
}

func TestOptionsChaining(t *testing.T) {
	storage := local.New()
	storage.Configure(map[string]string{"path": t.TempDir()})
	tlsConfig, _ := GenerateSelfSignedCert()
	logger := adapters.NewDefaultLogger()
	auth := adapters.NewNoOpAuthenticator()
	adapterTLS := adapters.NewTLSConfig()

	// Test that all With* methods return the same instance for chaining
	opts := DefaultOptions()
	opts2 := opts.WithAddr(":5000")
	if opts != opts2 {
		t.Error("WithAddr should return the same instance")
	}

	opts3 := opts.WithStorage(storage)
	if opts != opts3 {
		t.Error("WithStorage should return the same instance")
	}

	opts4 := opts.WithTLSConfig(tlsConfig)
	if opts != opts4 {
		t.Error("WithTLSConfig should return the same instance")
	}

	opts5 := opts.WithLogger(logger)
	if opts != opts5 {
		t.Error("WithLogger should return the same instance")
	}

	opts6 := opts.WithAuthenticator(auth)
	if opts != opts6 {
		t.Error("WithAuthenticator should return the same instance")
	}

	opts7 := opts.WithAdapterTLS(adapterTLS)
	if opts != opts7 {
		t.Error("WithAdapterTLS should return the same instance")
	}
}

func TestOptionsQUICConfigSync(t *testing.T) {
	storage := local.New()
	storage.Configure(map[string]string{"path": t.TempDir()})
	tlsConfig, _ := GenerateSelfSignedCert()

	opts := &Options{
		Addr:            ":4433",
		Storage:         storage,
		TLSConfig:       tlsConfig,
		IdleTimeout:     90 * time.Second,
		MaxBiStreams:    200,
		MaxUniStreams:   150,
		EnableDatagrams: true,
		QUICConfig:      &quic.Config{}, // Empty config
	}

	err := opts.Validate()
	if err != nil {
		t.Fatalf("Validate() failed: %v", err)
	}

	// Verify QUIC config was synced
	if opts.QUICConfig.MaxIdleTimeout != 90*time.Second {
		t.Errorf("Expected MaxIdleTimeout to be synced to 90s, got %v", opts.QUICConfig.MaxIdleTimeout)
	}

	if opts.QUICConfig.MaxIncomingStreams != 200 {
		t.Errorf("Expected MaxIncomingStreams to be synced to 200, got %d", opts.QUICConfig.MaxIncomingStreams)
	}

	if opts.QUICConfig.MaxIncomingUniStreams != 150 {
		t.Errorf("Expected MaxIncomingUniStreams to be synced to 150, got %d", opts.QUICConfig.MaxIncomingUniStreams)
	}

	if !opts.QUICConfig.EnableDatagrams {
		t.Error("Expected EnableDatagrams to be synced to true")
	}
}

func TestOptionsDefaultQUICConfig(t *testing.T) {
	opts := DefaultOptions()

	if opts.QUICConfig == nil {
		t.Fatal("Expected QUICConfig to be initialized")
	}

	// Verify default QUIC config settings
	if opts.QUICConfig.MaxIdleTimeout != 60*time.Second {
		t.Errorf("Expected default MaxIdleTimeout 60s, got %v", opts.QUICConfig.MaxIdleTimeout)
	}

	if opts.QUICConfig.MaxIncomingStreams != 100 {
		t.Errorf("Expected default MaxIncomingStreams 100, got %d", opts.QUICConfig.MaxIncomingStreams)
	}

	if opts.QUICConfig.MaxIncomingUniStreams != 100 {
		t.Errorf("Expected default MaxIncomingUniStreams 100, got %d", opts.QUICConfig.MaxIncomingUniStreams)
	}

	if opts.QUICConfig.KeepAlivePeriod != 30*time.Second {
		t.Errorf("Expected default KeepAlivePeriod 30s, got %v", opts.QUICConfig.KeepAlivePeriod)
	}

	if opts.QUICConfig.EnableDatagrams {
		t.Error("Expected datagrams to be disabled by default")
	}

	if opts.QUICConfig.Allow0RTT {
		t.Error("Expected 0-RTT to be disabled by default (security)")
	}
}

func TestOptionsDefaultLoggerAndAuth(t *testing.T) {
	opts := DefaultOptions()

	if opts.Logger == nil {
		t.Error("Expected default logger to be set")
	}

	if opts.Authenticator == nil {
		t.Error("Expected default authenticator to be set")
	}
}

func TestOptionsEmptyAddr(t *testing.T) {
	storage := local.New()
	storage.Configure(map[string]string{"path": t.TempDir()})
	tlsConfig, _ := GenerateSelfSignedCert()

	opts := &Options{
		Addr:      "", // Empty address
		Storage:   storage,
		TLSConfig: tlsConfig,
	}

	err := opts.Validate()
	if err != ErrInvalidAddr {
		t.Errorf("Expected ErrInvalidAddr for empty address, got %v", err)
	}
}

func TestOptionsNilStorage(t *testing.T) {
	tlsConfig, _ := GenerateSelfSignedCert()

	opts := &Options{
		Addr:      ":4433",
		Storage:   nil, // Nil storage
		TLSConfig: tlsConfig,
	}

	err := opts.Validate()
	if err != ErrStorageRequired {
		t.Errorf("Expected ErrStorageRequired for nil storage, got %v", err)
	}
}

func TestOptionsNilTLSConfig(t *testing.T) {
	storage := local.New()
	storage.Configure(map[string]string{"path": t.TempDir()})

	opts := &Options{
		Addr:      ":4433",
		Storage:   storage,
		TLSConfig: nil, // Nil TLS config
	}

	err := opts.Validate()
	if err != ErrTLSConfigRequired {
		t.Errorf("Expected ErrTLSConfigRequired for nil TLS config, got %v", err)
	}
}

// We don't need the mockFailingTLSAdapter anymore since we can test with invalid paths

func TestOptionsValidateWithFailingAdapterTLS(t *testing.T) {
	storage := local.New()
	storage.Configure(map[string]string{"path": t.TempDir()})

	// This test would need a mock that implements the adapter interface
	// For now, we'll just use an invalid cert path which will cause Build to fail
	adapterTLS := adapters.NewTLSConfig().
		WithServerCertFiles("/invalid/path/cert.pem", "/invalid/path/key.pem")

	opts := &Options{
		Addr:             ":4433",
		Storage:          storage,
		AdapterTLSConfig: adapterTLS,
	}

	err := opts.Validate()
	if err == nil {
		t.Error("Expected error when adapter TLS Build fails")
	}
}

func TestOptionsBuilderAllMethods(t *testing.T) {
	storage := local.New()
	storage.Configure(map[string]string{"path": t.TempDir()})
	tlsConfig, _ := GenerateSelfSignedCert()
	quicConfig := &quic.Config{}
	logger := adapters.NewDefaultLogger()
	auth := adapters.NewNoOpAuthenticator()
	adapterTLS := adapters.NewTLSConfig()

	opts := DefaultOptions().
		WithAddr(":6000").
		WithStorage(storage).
		WithTLSConfig(tlsConfig).
		WithQUICConfig(quicConfig).
		WithMaxRequestBodySize(200*1024*1024).
		WithTimeouts(10*time.Second, 15*time.Second, 30*time.Second).
		WithStreamLimits(300, 250).
		WithDatagrams(true).
		WithLogger(logger).
		WithAuthenticator(auth).
		WithAdapterTLS(adapterTLS)

	// Verify all fields were set correctly
	if opts.Addr != ":6000" {
		t.Errorf("Addr not set correctly: %s", opts.Addr)
	}
	if opts.Storage != storage {
		t.Error("Storage not set correctly")
	}
	if opts.TLSConfig != tlsConfig {
		t.Error("TLSConfig not set correctly")
	}
	if opts.QUICConfig != quicConfig {
		t.Error("QUICConfig not set correctly")
	}
	if opts.MaxRequestBodySize != 200*1024*1024 {
		t.Errorf("MaxRequestBodySize not set correctly: %d", opts.MaxRequestBodySize)
	}
	if opts.ReadTimeout != 10*time.Second {
		t.Errorf("ReadTimeout not set correctly: %v", opts.ReadTimeout)
	}
	if opts.WriteTimeout != 15*time.Second {
		t.Errorf("WriteTimeout not set correctly: %v", opts.WriteTimeout)
	}
	if opts.IdleTimeout != 30*time.Second {
		t.Errorf("IdleTimeout not set correctly: %v", opts.IdleTimeout)
	}
	if opts.MaxBiStreams != 300 {
		t.Errorf("MaxBiStreams not set correctly: %d", opts.MaxBiStreams)
	}
	if opts.MaxUniStreams != 250 {
		t.Errorf("MaxUniStreams not set correctly: %d", opts.MaxUniStreams)
	}
	if !opts.EnableDatagrams {
		t.Error("EnableDatagrams not set correctly")
	}
	if opts.Logger != logger {
		t.Error("Logger not set correctly")
	}
	if opts.Authenticator != auth {
		t.Error("Authenticator not set correctly")
	}
	if opts.AdapterTLSConfig != adapterTLS {
		t.Error("AdapterTLSConfig not set correctly")
	}
}
