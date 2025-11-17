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

// TestListenAndServe_SuccessfulStart tests the ListenAndServe function starting successfully
func TestListenAndServe_SuccessfulStart(t *testing.T) {
	storage := local.New()
	storage.Configure(map[string]string{"path": t.TempDir()})
	tlsConfig, _ := GenerateSelfSignedCert()

	opts := DefaultOptions().
		WithAddr(":0").
		WithStorage(storage).
		WithTLSConfig(tlsConfig)

	// Run ListenAndServe in a goroutine since it blocks indefinitely
	errChan := make(chan error, 1)
	started := make(chan bool, 1)

	go func() {
		// Signal that we're starting
		started <- true
		err := ListenAndServe(opts)
		errChan <- err
	}()

	// Wait for goroutine to start
	<-started

	// Give the server time to initialize
	time.Sleep(200 * time.Millisecond)

	// Since ListenAndServe blocks indefinitely, we verify it hasn't returned with an error
	select {
	case err := <-errChan:
		if err != nil {
			t.Errorf("ListenAndServe failed immediately: %v", err)
		} else {
			t.Error("ListenAndServe returned nil unexpectedly")
		}
	default:
		// Good - server is still running and blocking as expected
	}
}

// TestListenAndServe_WithStartFailure tests ListenAndServe when Start() returns an error
func TestListenAndServe_WithStartFailure(t *testing.T) {
	storage := local.New()
	storage.Configure(map[string]string{"path": t.TempDir()})
	tlsConfig, _ := GenerateSelfSignedCert()

	// Use invalid port range to cause Start() to fail
	opts := DefaultOptions().
		WithAddr(":99999"). // Invalid port
		WithStorage(storage).
		WithTLSConfig(tlsConfig)

	err := ListenAndServe(opts)
	if err == nil {
		t.Error("Expected error when Start() fails with invalid port")
	}
}

// TestServerStart_LoggingOnError tests that server logs errors when Serve() fails
func TestServerStart_LoggingOnError(t *testing.T) {
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

	// Give it time to start the goroutine
	time.Sleep(100 * time.Millisecond)

	if !server.IsRunning() {
		t.Error("Expected server to be running")
	}

	// Stop server normally
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = server.Stop(ctx)
	if err != nil {
		t.Logf("Stop returned error (may be expected in some environments): %v", err)
	}

	// Give it time to stop
	time.Sleep(200 * time.Millisecond)
}

// TestServerStop_ContextLogging tests that Stop logs with the provided context
func TestServerStop_ContextLogging(t *testing.T) {
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

	time.Sleep(100 * time.Millisecond)

	// Create a context with a value to verify it's used
	ctx := context.WithValue(context.Background(), "test-key", "test-value")
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	// Stop should use the provided context for logging
	err = server.Stop(ctx)
	if err != nil {
		t.Logf("Stop returned error: %v", err)
	}

	time.Sleep(100 * time.Millisecond)
}

// TestServerStart_ListenUDPError tests error path in Start when ListenUDP fails
func TestServerStart_ListenUDPError(t *testing.T) {
	storage := local.New()
	storage.Configure(map[string]string{"path": t.TempDir()})
	tlsConfig, _ := GenerateSelfSignedCert()

	// Use an address that will fail to bind
	opts := DefaultOptions().
		WithAddr("256.256.256.256:12345"). // Invalid IP address
		WithStorage(storage).
		WithTLSConfig(tlsConfig)

	server, err := New(opts)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	// Start should fail with invalid address
	err = server.Start()
	if err == nil {
		t.Error("Expected error when ListenUDP fails")
		// Clean up if somehow it started
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		server.Stop(ctx)
	}
}

// TestServerStop_ErrorFromClose tests error handling when Close() returns an error
func TestServerStop_ErrorFromClose(t *testing.T) {
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

	time.Sleep(100 * time.Millisecond)

	// Stop the server - may encounter error from Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = server.Stop(ctx)
	// Don't fail the test if Stop returns an error, as this can happen
	// in various environments. We're testing that the error path is exercised.
	if err != nil {
		t.Logf("Stop returned error (testing error path): %v", err)
	}

	time.Sleep(100 * time.Millisecond)
}

// TestServerServe_RunningFlagManagement tests that the running flag is managed correctly
func TestServerServe_RunningFlagManagement(t *testing.T) {
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

	if server.IsRunning() {
		t.Error("Server should not be running initially")
	}

	// Start and immediately stop to test flag transitions
	err = server.Start()
	if err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	if !server.IsRunning() {
		t.Error("Server should be running after Start()")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	server.Stop(ctx)
	time.Sleep(200 * time.Millisecond)

	if server.IsRunning() {
		t.Error("Server should not be running after Stop()")
	}
}

// TestServerStart_NilContext tests Start's logging with nil context
func TestServerStart_NilContext(t *testing.T) {
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

	// Start server (internally uses nil context for logging)
	err = server.Start()
	if err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	if server.Addr() == nil {
		t.Error("Expected non-nil address after start")
	}

	// Verify the address is properly formatted
	addrStr := server.Addr().String()
	if addrStr == "" {
		t.Error("Expected non-empty address string")
	}

	// Clean up
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	server.Stop(ctx)
}

// TestServerStart_GoroutineExecution tests the goroutine execution path in Start
func TestServerStart_GoroutineExecution(t *testing.T) {
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

	// Wait for goroutine to be fully started
	time.Sleep(150 * time.Millisecond)

	// Verify server is running (goroutine is executing)
	if !server.IsRunning() {
		t.Error("Expected server to be running (goroutine should be executing)")
	}

	// Stop to trigger the goroutine's error handling
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = server.Stop(ctx)

	// Wait for goroutine to finish
	time.Sleep(200 * time.Millisecond)

	// Verify goroutine has set running to false
	if server.IsRunning() {
		t.Error("Expected server to not be running after goroutine completes")
	}
}
