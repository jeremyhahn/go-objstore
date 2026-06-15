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
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/jeremyhahn/go-objstore/pkg/objstore"
	mcpserver "github.com/jeremyhahn/go-objstore/pkg/server/mcp"
)

func main() {
	// Command line flags
	mode := flag.String("mode", "http", "Server mode: stdio or http")
	addr := flag.String("addr", ":8081", "HTTP server address (only for http mode)")
	backend := flag.String("backend", "local", "Storage backend (local, s3, gcs, azure)")
	storagePath := flag.String("path", "/tmp/objstore", "Storage path for local backend")

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

	// Configure MCP server
	var serverMode mcpserver.ServerMode
	switch *mode {
	case "stdio":
		serverMode = mcpserver.ModeStdio
	case "http":
		serverMode = mcpserver.ModeHTTP
	default:
		slog.Error("Invalid mode (must be 'stdio' or 'http')", "mode", *mode)
		os.Exit(1)
	}

	config := &mcpserver.ServerConfig{
		Mode:        serverMode,
		HTTPAddress: *addr,
		Backend:     "", // Use default backend
	}

	server, err := mcpserver.NewServer(config)
	if err != nil {
		slog.Error("Failed to create MCP server", "error", err)
		os.Exit(1)
	}

	// Create context that cancels on SIGINT/SIGTERM
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		slog.Info("Received signal", "signal", sig.String())
		cancel()
	}()

	// Start server (slog writes to stderr, keeping stdout free for stdio JSON-RPC)
	slog.Info("Starting MCP server", "mode", *mode)
	if err := server.Start(ctx); err != nil {
		cancel()
		slog.Error("Server error", "error", err)
		os.Exit(1)
	}

	slog.Info("MCP server stopped")
}
