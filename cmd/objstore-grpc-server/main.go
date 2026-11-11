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
	"errors"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/jeremyhahn/go-objstore/internal/serverconfig"
	"github.com/jeremyhahn/go-objstore/pkg/objstore"
	grpcserver "github.com/jeremyhahn/go-objstore/pkg/server/grpc"
)

func main() {
	cfg, err := serverconfig.Load(os.Args[0], os.Args[1:], serverconfig.Defaults(), serverconfig.BindGRPCServerFlags)
	if errors.Is(err, flag.ErrHelp) {
		os.Exit(0)
	}
	if err != nil {
		slog.Error("Invalid configuration", "error", err)
		os.Exit(2)
	}

	// Initialize the objstore facade with simplified API
	if err := objstore.Initialize(&objstore.FacadeConfig{
		BackendConfigs: map[string]objstore.BackendConfig{
			"default": {
				Type:     cfg.Backend,
				Settings: map[string]string{"path": cfg.Path},
			},
		},
		DefaultBackend: "default",
	}); err != nil {
		slog.Error("Failed to initialize objstore facade", "error", err)
		os.Exit(1)
	}

	slog.Info("Initialized storage backend", "backend", cfg.Backend)

	// Enable replication on the default backend
	cfg.EnableReplication(objstore.EnableReplication)

	// Create server options: address, and TLS when grpc.tls_cert/tls_key are set
	opts, err := cfg.GRPCOptions(cfg.Middleware())
	if err != nil {
		slog.Error("Failed to configure gRPC server", "error", err)
		os.Exit(1)
	}
	opts = append(opts, grpcserver.WithBackend("")) // Use default backend
	if cfg.GRPCTLSEnabled() {
		slog.Info("TLS enabled")
	}

	// Create and start server
	server, err := grpcserver.NewServer(opts...)
	if err != nil {
		slog.Error("Failed to create gRPC server", "error", err)
		os.Exit(1)
	}

	slog.Info("Starting gRPC server", "addr", cfg.GRPC.Addr)

	// Start server in goroutine
	errChan := make(chan error, 1)
	go func() {
		if err := server.Start(); err != nil {
			errChan <- err
		}
	}()

	// Wait for interrupt signal or error
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-errChan:
		slog.Error("Server error", "transport", "gRPC", "error", err)
	case sig := <-sigChan:
		slog.Info("Received signal", "signal", sig.String())
	}

	slog.Info("Shutting down gRPC server")
	server.Stop()
	slog.Info("Server stopped")
}
