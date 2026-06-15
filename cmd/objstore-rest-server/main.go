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
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/objstore"
	restserver "github.com/jeremyhahn/go-objstore/pkg/server/rest"
)

func main() {
	// Command line flags
	host := flag.String("host", "localhost", "REST server host")
	port := flag.Int("port", 8080, "REST server port")
	backend := flag.String("backend", "local", "Storage backend (local, s3, gcs, azure)")
	storagePath := flag.String("path", "/tmp/objstore", "Storage path for local backend")
	metricsPublic := flag.Bool("metrics-public", false, "Expose /metrics without authorization")

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

	// Create server configuration
	config := restserver.DefaultServerConfig()
	config.Host = *host
	config.Port = *port
	config.MetricsPublic = *metricsPublic

	// Create and start server (storage param is nil since handler uses facade)
	server, err := restserver.NewServer(nil, config)
	if err != nil {
		slog.Error("Failed to create REST server", "error", err)
		os.Exit(1)
	}

	slog.Info("Starting REST server", "host", config.Host, "port", config.Port)

	// Start server in goroutine
	errChan := make(chan error, 1)
	go func() {
		if err := server.Start(); err != nil {
			errChan <- fmt.Errorf("REST server error: %w", err)
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

	slog.Info("Shutting down REST server")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		slog.Error("REST server shutdown error", "error", err)
	}
	slog.Info("Server stopped")
}
