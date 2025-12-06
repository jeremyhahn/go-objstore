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
	"log"
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
		log.Fatalf("Failed to initialize objstore facade: %v", err)
	}

	log.Printf("Initialized %s storage backend", *backend)

	// Enable replication on the default backend
	policyPath := *storagePath + "/.replication-policies.json"
	if err := objstore.EnableReplication("", &objstore.ReplicationConfig{
		PolicyFilePath:  policyPath,
		RunInBackground: false,
	}); err != nil {
		log.Printf("Warning: Failed to enable replication: %v", err)
	} else {
		log.Printf("Replication enabled with policy file: %s", policyPath)
	}

	// Configure MCP server
	var serverMode mcpserver.ServerMode
	switch *mode {
	case "stdio":
		serverMode = mcpserver.ModeStdio
	case "http":
		serverMode = mcpserver.ModeHTTP
	default:
		log.Fatalf("Invalid mode: %s (must be 'stdio' or 'http')", *mode)
	}

	config := &mcpserver.ServerConfig{
		Mode:        serverMode,
		HTTPAddress: *addr,
		Backend:     "", // Use default backend
	}

	server, err := mcpserver.NewServer(config)
	if err != nil {
		log.Fatalf("Failed to create MCP server: %v", err)
	}

	// Create context that cancels on SIGINT/SIGTERM
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		log.Printf("Received signal: %v", sig)
		cancel()
	}()

	// Start server
	log.Printf("Starting MCP server in %s mode", *mode)
	if err := server.Start(ctx); err != nil {
		cancel()
		log.Fatalf("Server error: %v", err)
	}

	log.Println("MCP server stopped")
}
