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
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/jeremyhahn/go-objstore/pkg/objstore"
	grpcserver "github.com/jeremyhahn/go-objstore/pkg/server/grpc"
)

func main() {
	// Command line flags
	addr := flag.String("addr", ":50051", "gRPC server address")
	backend := flag.String("backend", "local", "Storage backend (local, s3, gcs, azure)")
	storagePath := flag.String("path", "/tmp/objstore", "Storage path for local backend")
	tlsCert := flag.String("tls-cert", "", "TLS certificate file")
	tlsKey := flag.String("tls-key", "", "TLS key file")

	flag.Parse()

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

	slog.Info("Initialized storage backend", "backend", *backend)

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

	// Create server options
	opts := []grpcserver.ServerOption{
		grpcserver.WithAddress(*addr),
		grpcserver.WithBackend(""), // Use default backend
	}

	// Add TLS if certificates provided
	if *tlsCert != "" && *tlsKey != "" {
		tlsOpt, err := grpcserver.WithTLSFromFiles(*tlsCert, *tlsKey)
		if err != nil {
			slog.Error("Failed to configure TLS", "error", err)
			os.Exit(1)
		}
		opts = append(opts, tlsOpt)
		slog.Info("TLS enabled")
	}

	// Create and start server
	server, err := grpcserver.NewServer(opts...)
	if err != nil {
		slog.Error("Failed to create gRPC server", "error", err)
		os.Exit(1)
	}

	slog.Info("Starting gRPC server", "addr", *addr)

	// Start server in goroutine
	errChan := make(chan error, 1)
	go func() {
		if err := server.Start(); err != nil {
			errChan <- fmt.Errorf("gRPC server error: %w", err)
		}
	}()

	// Wait for interrupt signal or error
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-errChan:
		slog.Error("Server error", "error", err)
	case sig := <-sigChan:
		slog.Info("Received signal", "signal", sig.String())
	}

	slog.Info("Shutting down gRPC server")
	server.Stop()
	slog.Info("Server stopped")
}
