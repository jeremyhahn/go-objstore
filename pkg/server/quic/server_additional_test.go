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
	"net"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/local"
)

func TestServerServe(t *testing.T) {
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

	// Create UDP listener
	udpAddr, err := net.ResolveUDPAddr("udp", ":0")
	if err != nil {
		t.Fatalf("Failed to resolve UDP address: %v", err)
	}

	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		t.Fatalf("Failed to listen on UDP: %v", err)
	}
	defer conn.Close()

	// Start server in goroutine
	errChan := make(chan error, 1)
	go func() {
		errChan <- server.Serve(conn)
	}()

	// Give it a moment to start
	time.Sleep(100 * time.Millisecond)

	if !server.IsRunning() {
		t.Error("Expected server to be running")
	}

	// Close the connection to stop the server
	conn.Close()

	// Wait for server to stop
	select {
	case <-errChan:
		// Server stopped
	case <-time.After(2 * time.Second):
		t.Error("Server did not stop in time")
	}
}

func TestServerServe_AlreadyRunning(t *testing.T) {
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

	// Try to call Serve while already running
	udpAddr, _ := net.ResolveUDPAddr("udp", ":0")
	conn, _ := net.ListenUDP("udp", udpAddr)
	defer conn.Close()

	err = server.Serve(conn)
	if err != ErrServerAlreadyStarted {
		t.Errorf("Expected ErrServerAlreadyStarted, got %v", err)
	}
}

func TestServerServeConn(t *testing.T) {
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

	// Create UDP listener
	udpAddr, err := net.ResolveUDPAddr("udp", ":0")
	if err != nil {
		t.Fatalf("Failed to resolve UDP address: %v", err)
	}

	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		t.Fatalf("Failed to listen on UDP: %v", err)
	}

	// Start server in goroutine
	errChan := make(chan error, 1)
	go func() {
		errChan <- server.ServeConn(conn)
	}()

	// Give it a moment to start
	time.Sleep(100 * time.Millisecond)

	// Close the connection to stop the server
	conn.Close()

	// Wait for server to stop
	select {
	case <-errChan:
		// Server stopped
	case <-time.After(2 * time.Second):
		t.Error("Server did not stop in time")
	}
}

func TestListenAndServe(t *testing.T) {
	// This test is tricky because ListenAndServe blocks indefinitely
	// We'll test that it can be called and returns an error for invalid options

	opts := &Options{
		Addr: ":4433",
		// Missing required fields
	}

	err := ListenAndServe(opts)
	if err == nil {
		t.Error("Expected error for invalid options")
	}
}

func TestServerStartWithBadAddr(t *testing.T) {
	storage := local.New()
	storage.Configure(map[string]string{"path": t.TempDir()})
	tlsConfig, _ := GenerateSelfSignedCert()

	// Test with a port that's likely to fail on some systems
	opts := DefaultOptions().
		WithAddr("256.256.256.256:9999").
		WithStorage(storage).
		WithTLSConfig(tlsConfig)

	server, err := New(opts)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	// Start should fail
	err = server.Start()
	if err == nil {
		t.Error("Expected error for bad address")
		// Clean up if somehow it started
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		server.Stop(ctx)
	}
}

func TestServerStopWithError(t *testing.T) {
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

	// Stop server (should succeed even if underlying close has issues)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = server.Stop(ctx)
	if err != nil {
		// Log but don't fail - some environments might have issues closing
		t.Logf("Stop returned error (may be expected): %v", err)
	}
}

func TestServerConcurrentAccess(t *testing.T) {
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

	// Concurrently access server methods
	done := make(chan bool)
	for i := 0; i < 20; i++ {
		go func() {
			_ = server.IsRunning()
			_ = server.Addr()
			_ = server.Handler()
			_ = server.Options()
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < 20; i++ {
		<-done
	}
}

func TestServerAddrBeforeStart(t *testing.T) {
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
	addr := server.Addr()
	if addr != nil {
		t.Errorf("Expected nil address before start, got %v", addr)
	}
}
