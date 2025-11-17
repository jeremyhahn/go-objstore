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
	"context"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/local"
)

func TestNewServer(t *testing.T) {
	storage := local.New()
	storage.Configure(map[string]string{"path": t.TempDir()})
	tlsConfig, _ := GenerateSelfSignedCert()

	opts := DefaultOptions().
		WithAddr(":0"). // Use port 0 for random available port
		WithStorage(storage).
		WithTLSConfig(tlsConfig)

	server, err := New(opts)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	if server == nil {
		t.Fatal("Expected non-nil server")
	}

	if server.Handler() == nil {
		t.Error("Expected non-nil handler")
	}

	if server.Options() != opts {
		t.Error("Expected options to match")
	}

	if server.IsRunning() {
		t.Error("Expected server to not be running initially")
	}
}

func TestNewServer_InvalidOptions(t *testing.T) {
	opts := &Options{
		Addr: ":4433",
		// Missing required fields
	}

	_, err := New(opts)
	if err == nil {
		t.Error("Expected error for invalid options")
	}
}

func TestServerStartStop(t *testing.T) {
	storage := local.New()
	storage.Configure(map[string]string{"path": t.TempDir()})
	tlsConfig, _ := GenerateSelfSignedCert()

	opts := DefaultOptions().
		WithAddr(":0").
		WithStorage(storage).
		WithTLSConfig(tlsConfig)

	server, err := New(opts)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	// Start server
	err = server.Start()
	if err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}

	// Give it a moment to start
	time.Sleep(100 * time.Millisecond)

	if !server.IsRunning() {
		t.Error("Expected server to be running")
	}

	if server.Addr() == nil {
		t.Error("Expected non-nil address")
	}

	// Stop server
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = server.Stop(ctx)
	if err != nil {
		t.Fatalf("Failed to stop server: %v", err)
	}

	// Give it a moment to stop
	time.Sleep(100 * time.Millisecond)

	if server.IsRunning() {
		t.Error("Expected server to not be running")
	}
}

func TestServerStartTwice(t *testing.T) {
	storage := local.New()
	storage.Configure(map[string]string{"path": t.TempDir()})
	tlsConfig, _ := GenerateSelfSignedCert()

	opts := DefaultOptions().
		WithAddr(":0").
		WithStorage(storage).
		WithTLSConfig(tlsConfig)

	server, err := New(opts)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	// Start server
	err = server.Start()
	if err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		server.Stop(ctx)
	}()

	// Give it a moment to start
	time.Sleep(100 * time.Millisecond)

	// Try to start again
	err = server.Start()
	if err != ErrServerAlreadyStarted {
		t.Errorf("Expected ErrServerAlreadyStarted, got %v", err)
	}
}

func TestServerStopNotStarted(t *testing.T) {
	storage := local.New()
	storage.Configure(map[string]string{"path": t.TempDir()})
	tlsConfig, _ := GenerateSelfSignedCert()

	opts := DefaultOptions().
		WithAddr(":0").
		WithStorage(storage).
		WithTLSConfig(tlsConfig)

	server, err := New(opts)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	// Try to stop without starting
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = server.Stop(ctx)
	if err != ErrServerNotStarted {
		t.Errorf("Expected ErrServerNotStarted, got %v", err)
	}
}

func TestServerAddr(t *testing.T) {
	storage := local.New()
	storage.Configure(map[string]string{"path": t.TempDir()})
	tlsConfig, _ := GenerateSelfSignedCert()

	opts := DefaultOptions().
		WithAddr(":0").
		WithStorage(storage).
		WithTLSConfig(tlsConfig)

	server, err := New(opts)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	// Address should be nil before starting
	if server.Addr() != nil {
		t.Error("Expected nil address before starting")
	}

	// Start server
	err = server.Start()
	if err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		server.Stop(ctx)
	}()

	// Give it a moment to start
	time.Sleep(100 * time.Millisecond)

	// Address should be set after starting
	if server.Addr() == nil {
		t.Error("Expected non-nil address after starting")
	}

	// Verify it's a UDP address
	if server.Addr().Network() != "udp" {
		t.Errorf("Expected UDP network, got %s", server.Addr().Network())
	}
}

func TestServerOptions(t *testing.T) {
	storage := local.New()
	storage.Configure(map[string]string{"path": t.TempDir()})
	tlsConfig, _ := GenerateSelfSignedCert()

	opts := DefaultOptions().
		WithAddr(":5555").
		WithStorage(storage).
		WithTLSConfig(tlsConfig).
		WithMaxRequestBodySize(50 * 1024 * 1024)

	server, err := New(opts)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	retrievedOpts := server.Options()
	if retrievedOpts.Addr != ":5555" {
		t.Errorf("Expected addr :5555, got %s", retrievedOpts.Addr)
	}

	if retrievedOpts.MaxRequestBodySize != 50*1024*1024 {
		t.Errorf("Expected max body size 50MB, got %d", retrievedOpts.MaxRequestBodySize)
	}
}

func TestServerHandler(t *testing.T) {
	storage := local.New()
	storage.Configure(map[string]string{"path": t.TempDir()})
	tlsConfig, _ := GenerateSelfSignedCert()

	opts := DefaultOptions().
		WithAddr(":0").
		WithStorage(storage).
		WithTLSConfig(tlsConfig)

	server, err := New(opts)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	handler := server.Handler()
	if handler == nil {
		t.Fatal("Expected non-nil handler")
	}

	if handler.storage != storage {
		t.Error("Expected handler to use the configured storage")
	}
}

func TestServerInvalidAddr(t *testing.T) {
	storage := local.New()
	storage.Configure(map[string]string{"path": t.TempDir()})
	tlsConfig, _ := GenerateSelfSignedCert()

	opts := DefaultOptions().
		WithAddr("invalid-address").
		WithStorage(storage).
		WithTLSConfig(tlsConfig)

	server, err := New(opts)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	// Start should fail with invalid address
	err = server.Start()
	if err == nil {
		t.Error("Expected error for invalid address")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		server.Stop(ctx)
	}
}

func TestServerConcurrentStartStop(t *testing.T) {
	storage := local.New()
	storage.Configure(map[string]string{"path": t.TempDir()})
	tlsConfig, _ := GenerateSelfSignedCert()

	opts := DefaultOptions().
		WithAddr(":0").
		WithStorage(storage).
		WithTLSConfig(tlsConfig)

	server, err := New(opts)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	// Start server
	err = server.Start()
	if err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}

	// Give it a moment to start
	time.Sleep(100 * time.Millisecond)

	// Check running status multiple times concurrently
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func() {
			if !server.IsRunning() {
				t.Error("Expected server to be running")
			}
			if server.Addr() == nil {
				t.Error("Expected non-nil address")
			}
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	// Stop server
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = server.Stop(ctx)
	if err != nil {
		t.Fatalf("Failed to stop server: %v", err)
	}
}

func TestServerWithCustomTimeouts(t *testing.T) {
	storage := local.New()
	storage.Configure(map[string]string{"path": t.TempDir()})
	tlsConfig, _ := GenerateSelfSignedCert()

	opts := DefaultOptions().
		WithAddr(":0").
		WithStorage(storage).
		WithTLSConfig(tlsConfig).
		WithTimeouts(15*time.Second, 20*time.Second, 45*time.Second)

	server, err := New(opts)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	handler := server.Handler()
	if handler.readTimeout != 15*time.Second {
		t.Errorf("Expected read timeout 15s, got %v", handler.readTimeout)
	}

	if handler.writeTimeout != 20*time.Second {
		t.Errorf("Expected write timeout 20s, got %v", handler.writeTimeout)
	}
}

func TestServerWithStreamLimits(t *testing.T) {
	storage := local.New()
	storage.Configure(map[string]string{"path": t.TempDir()})
	tlsConfig, _ := GenerateSelfSignedCert()

	opts := DefaultOptions().
		WithAddr(":0").
		WithStorage(storage).
		WithTLSConfig(tlsConfig).
		WithStreamLimits(200, 150)

	err := opts.Validate()
	if err != nil {
		t.Fatalf("Failed to validate options: %v", err)
	}

	if opts.QUICConfig.MaxIncomingStreams != 200 {
		t.Errorf("Expected max incoming streams 200, got %d", opts.QUICConfig.MaxIncomingStreams)
	}

	if opts.QUICConfig.MaxIncomingUniStreams != 150 {
		t.Errorf("Expected max incoming uni streams 150, got %d", opts.QUICConfig.MaxIncomingUniStreams)
	}
}
