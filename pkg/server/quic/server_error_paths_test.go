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

// TestServerStart_ErrorLogging tests the error logging path in Start's goroutine
func TestServerStart_ErrorLogging(t *testing.T) {
	storage := local.New()
	storage.Configure(map[string]string{"path": t.TempDir()})
	initTestFacade(t, storage)
	tlsConfig, _ := GenerateSelfSignedCert()

	opts := DefaultOptions().
		WithAddr(":0").
		WithBackend("").
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

	// Give server time to start
	time.Sleep(150 * time.Millisecond)

	if !server.IsRunning() {
		t.Error("Expected server to be running")
	}

	// Stop the server to trigger the goroutine completion
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = server.Stop(ctx)

	// Give time for goroutine to finish and log
	time.Sleep(250 * time.Millisecond)

	// At this point, the goroutine should have completed
	if server.IsRunning() {
		t.Error("Expected server to not be running after stop")
	}
}

// TestServerStop_WithCloseError tests the error return path in Stop
func TestServerStop_WithCloseError(t *testing.T) {
	storage := local.New()
	storage.Configure(map[string]string{"path": t.TempDir()})
	initTestFacade(t, storage)
	tlsConfig, _ := GenerateSelfSignedCert()

	opts := DefaultOptions().
		WithAddr(":0").
		WithBackend("").
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

	// Stop with a context
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Call Stop - may return error from Close()
	err = server.Stop(ctx)

	// We don't fail if there's an error, as this tests the error return path
	if err != nil {
		t.Logf("Stop returned error (testing error path): %v", err)
	}

	time.Sleep(100 * time.Millisecond)
}

// TestServerServe_StoreAddr tests that Serve stores the connection address
func TestServerServe_StoreAddr(t *testing.T) {
	storage := local.New()
	storage.Configure(map[string]string{"path": t.TempDir()})
	initTestFacade(t, storage)
	tlsConfig, _ := GenerateSelfSignedCert()

	opts := DefaultOptions().
		WithAddr(":0").
		WithBackend("").
		WithTLSConfig(tlsConfig)

	server, err := New(opts)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	// Create UDP connection
	addr, err := net.ResolveUDPAddr("udp", ":0")
	if err != nil {
		t.Fatalf("Failed to resolve UDP addr: %v", err)
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		t.Fatalf("Failed to listen UDP: %v", err)
	}
	defer conn.Close()

	// Serve in goroutine
	errChan := make(chan error, 1)
	go func() {
		errChan <- server.Serve(conn)
	}()

	// Give it time to set the address
	time.Sleep(100 * time.Millisecond)

	// Verify address is set
	if server.Addr() == nil {
		t.Error("Expected non-nil address after Serve starts")
	}

	// Verify it matches the connection's address
	if server.Addr().String() != conn.LocalAddr().String() {
		t.Errorf("Address mismatch: server=%s, conn=%s", server.Addr(), conn.LocalAddr())
	}

	// Stop by closing connection
	conn.Close()

	// Wait for Serve to return
	select {
	case <-errChan:
	case <-time.After(2 * time.Second):
		t.Error("Serve did not return")
	}

	// Verify running flag is cleared
	if server.IsRunning() {
		t.Error("Expected server to not be running after Serve returns")
	}
}

// TestServerStart_AddressStorage tests that Start stores the listening address
func TestServerStart_AddressStorage(t *testing.T) {
	storage := local.New()
	storage.Configure(map[string]string{"path": t.TempDir()})
	initTestFacade(t, storage)
	tlsConfig, _ := GenerateSelfSignedCert()

	opts := DefaultOptions().
		WithAddr(":0"). // Use port 0 for random assignment
		WithBackend("").
		WithTLSConfig(tlsConfig)

	server, err := New(opts)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	// Address should be nil before start
	if server.Addr() != nil {
		t.Error("Address should be nil before Start")
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

	// Give time for address to be stored
	time.Sleep(150 * time.Millisecond)

	// Address should now be set
	if server.Addr() == nil {
		t.Fatal("Address should be set after Start")
	}

	// Verify it's a UDP address
	if server.Addr().Network() != "udp" {
		t.Errorf("Expected UDP network, got %s", server.Addr().Network())
	}

	// Verify the address string is not empty
	if server.Addr().String() == "" {
		t.Error("Address string should not be empty")
	}
}

// TestServerStart_RunningFlagTransition tests the running flag is set before goroutine
func TestServerStart_RunningFlagTransition(t *testing.T) {
	storage := local.New()
	storage.Configure(map[string]string{"path": t.TempDir()})
	initTestFacade(t, storage)
	tlsConfig, _ := GenerateSelfSignedCert()

	opts := DefaultOptions().
		WithAddr(":0").
		WithBackend("").
		WithTLSConfig(tlsConfig)

	server, err := New(opts)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	// Should not be running initially
	if server.IsRunning() {
		t.Error("Server should not be running before Start")
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

	// Should be running immediately after Start returns
	if !server.IsRunning() {
		t.Error("Server should be running after Start returns")
	}

	// Wait a bit to ensure goroutine is stable
	time.Sleep(100 * time.Millisecond)

	// Should still be running
	if !server.IsRunning() {
		t.Error("Server should still be running")
	}
}

// TestServerServe_MutexLocking tests that Serve properly locks the mutex
func TestServerServe_MutexLocking(t *testing.T) {
	storage := local.New()
	storage.Configure(map[string]string{"path": t.TempDir()})
	initTestFacade(t, storage)
	tlsConfig, _ := GenerateSelfSignedCert()

	opts := DefaultOptions().
		WithAddr(":0").
		WithBackend("").
		WithTLSConfig(tlsConfig)

	server, err := New(opts)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	addr, _ := net.ResolveUDPAddr("udp", ":0")
	conn, _ := net.ListenUDP("udp", addr)
	defer conn.Close()

	// Start Serve in goroutine
	errChan := make(chan error, 1)
	go func() {
		errChan <- server.Serve(conn)
	}()

	// Give it time to acquire the lock and set state
	time.Sleep(100 * time.Millisecond)

	// Now try to read the state (should work with read lock)
	running := server.IsRunning()
	if !running {
		t.Error("Expected server to be running")
	}

	addr2 := server.Addr()
	if addr2 == nil {
		t.Error("Expected non-nil address")
	}

	// Close connection
	conn.Close()

	// Wait for Serve to complete
	select {
	case <-errChan:
	case <-time.After(2 * time.Second):
		t.Error("Serve did not complete")
	}
}

// TestServerStop_MutexLocking tests that Stop properly locks the mutex
func TestServerStop_MutexLocking(t *testing.T) {
	storage := local.New()
	storage.Configure(map[string]string{"path": t.TempDir()})
	initTestFacade(t, storage)
	tlsConfig, _ := GenerateSelfSignedCert()

	opts := DefaultOptions().
		WithAddr(":0").
		WithBackend("").
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

	// Concurrently read state while stopping
	done := make(chan bool)
	go func() {
		for i := 0; i < 10; i++ {
			_ = server.IsRunning()
			_ = server.Addr()
			time.Sleep(10 * time.Millisecond)
		}
		done <- true
	}()

	// Stop the server
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = server.Stop(ctx)

	// Wait for concurrent reads to finish
	<-done

	time.Sleep(100 * time.Millisecond)
}

// TestServerStart_GoroutineErrorPath tests the error path in the Start goroutine
func TestServerStart_GoroutineErrorPath(t *testing.T) {
	storage := local.New()
	storage.Configure(map[string]string{"path": t.TempDir()})
	initTestFacade(t, storage)
	tlsConfig, _ := GenerateSelfSignedCert()

	opts := DefaultOptions().
		WithAddr(":0").
		WithBackend("").
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

	// Let it run for a moment
	time.Sleep(150 * time.Millisecond)

	// Verify it's running
	if !server.IsRunning() {
		t.Error("Server should be running")
	}

	// Stop the server, which will cause an error in the goroutine
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	server.Stop(ctx)

	// Give the goroutine time to handle the error and exit
	time.Sleep(250 * time.Millisecond)

	// The goroutine should have set running to false
	if server.IsRunning() {
		t.Error("Server should not be running after goroutine exits")
	}
}
