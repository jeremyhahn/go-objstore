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

package main

import (
	"context"
	"crypto/tls"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/objstore"
	quicserver "github.com/jeremyhahn/go-objstore/pkg/server/quic"
)

var (
	addr             = flag.String("addr", ":4433", "UDP address to listen on")
	backend          = flag.String("backend", "local", "Storage backend type (local, s3, gcs, azure)")
	storagePath      = flag.String("path", "./data", "Storage path (for local backend)")
	tlsCert          = flag.String("tlscert", "", "Path to TLS certificate file")
	tlsKey           = flag.String("tlskey", "", "Path to TLS private key file")
	maxBodySize      = flag.Int64("maxbodysize", 100*1024*1024, "Maximum request body size in bytes")
	readTimeout      = flag.Duration("readtimeout", 30*time.Second, "Read timeout")
	writeTimeout     = flag.Duration("writetimeout", 30*time.Second, "Write timeout")
	idleTimeout      = flag.Duration("idletimeout", 60*time.Second, "Idle timeout")
	maxStreams       = flag.Int64("maxstreams", 100, "Maximum bidirectional streams per connection")
	enableSelfSigned = flag.Bool("selfsigned", false, "Use self-signed certificate (for testing only)")
)

func main() {
	flag.Parse()

	slog.Info("Starting QUIC/HTTP3 Object Storage Server")

	// Initialize the objstore facade with simplified API
	if err := objstore.Initialize(&objstore.FacadeConfig{
		BackendConfigs: map[string]objstore.BackendConfig{
			"default": {
				Type:     *backend,
				Settings: map[string]string{"path": *storagePath},
			},
		},
		DefaultBackend: "default",
	}); err != nil {
		slog.Error("Failed to initialize objstore facade", "error", err)
		os.Exit(1)
	}

	// Enable replication on the default backend
	policyPath := *storagePath + "/.replication-policies.json"
	if err := objstore.EnableReplication("", &objstore.ReplicationConfig{
		PolicyFilePath:  policyPath,
		RunInBackground: false,
	}); err != nil {
		slog.Warn("Failed to enable replication", "error", err)
	} else {
		slog.Info("Replication enabled", "policy_file", policyPath)
	}

	// Configure TLS
	var tlsConfig *tls.Config
	var err error
	if *enableSelfSigned {
		slog.Warn("Using self-signed certificate. DO NOT USE IN PRODUCTION!")
		tlsConfig, err = quicserver.GenerateSelfSignedCert()
		if err != nil {
			slog.Error("Failed to generate self-signed certificate", "error", err)
			os.Exit(1)
		}
	} else {
		if *tlsCert == "" || *tlsKey == "" {
			slog.Error("TLS certificate and key are required. Use -tlscert and -tlskey flags, or -selfsigned for testing")
			os.Exit(1)
		}
		tlsConfig, err = quicserver.NewTLSConfig(*tlsCert, *tlsKey)
		if err != nil {
			slog.Error("Failed to load TLS configuration", "error", err)
			os.Exit(1)
		}
	}

	// Create server options
	opts := quicserver.DefaultOptions().
		WithAddr(*addr).
		WithBackend(""). // Use default backend
		WithTLSConfig(tlsConfig).
		WithMaxRequestBodySize(*maxBodySize).
		WithTimeouts(*readTimeout, *writeTimeout, *idleTimeout).
		WithStreamLimits(*maxStreams, *maxStreams)

	// Create and start server
	server, err := quicserver.New(opts)
	if err != nil {
		slog.Error("Failed to create QUIC server", "error", err)
		os.Exit(1)
	}

	if err := server.Start(); err != nil {
		slog.Error("Failed to start QUIC server", "error", err)
		os.Exit(1)
	}

	slog.Info("QUIC/HTTP3 server listening", "addr", server.Addr(), "backend", *backend, "protocol", "HTTP/3 over QUIC (TLS 1.3)")

	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Wait for interrupt signal
	sig := <-sigChan
	slog.Info("Received signal", "signal", sig.String())
	slog.Info("Shutting down gracefully")

	// Create shutdown context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Stop server
	if err := server.Stop(ctx); err != nil {
		cancel()
		slog.Error("Error during shutdown", "error", err)
		os.Exit(1)
	}

	slog.Info("Server stopped successfully")
}
