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
	"errors"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/jeremyhahn/go-objstore/internal/serverconfig"
	"github.com/jeremyhahn/go-objstore/pkg/objstore"
	mcpserver "github.com/jeremyhahn/go-objstore/pkg/server/mcp"
)

func main() {
	cfg, err := serverconfig.Load(os.Args[0], os.Args[1:], serverconfig.Defaults(), serverconfig.BindMCPServerFlags)
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

	// Configure MCP server (Load has already validated the mode)
	config, err := cfg.MCPServerConfig(cfg.Middleware())
	if err != nil {
		slog.Error("Invalid MCP configuration", "error", err)
		os.Exit(2)
	}
	config.Backend = "" // Use default backend

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
	slog.Info("Starting MCP server", "mode", config.Mode, "addr", config.HTTPAddress)
	if err := server.Start(ctx); err != nil {
		cancel()
		slog.Error("Server error", "error", err)
		os.Exit(1)
	}

	slog.Info("MCP server stopped")
}
