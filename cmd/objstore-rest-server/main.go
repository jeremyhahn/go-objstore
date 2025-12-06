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
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/jeremyhahn/go-objstore/pkg/objstore"
	restserver "github.com/jeremyhahn/go-objstore/pkg/server/rest"
)

func main() {
	// Command line flags
	host := flag.String("host", "localhost", "REST server host")
	port := flag.Int("port", 8080, "REST server port")
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

	// Create server configuration
	config := restserver.DefaultServerConfig()
	config.Host = *host
	config.Port = *port

	// Create and start server (storage param is nil since handler uses facade)
	server, err := restserver.NewServer(nil, config)
	if err != nil {
		log.Fatalf("Failed to create REST server: %v", err)
	}

	log.Printf("Starting REST server on %s:%d", config.Host, config.Port)
	log.Println("")
	log.Println("API Endpoints:")
	log.Println("  PUT    /objects/{key}      - Store an object")
	log.Println("  GET    /objects/{key}      - Retrieve an object")
	log.Println("  DELETE /objects/{key}      - Delete an object")
	log.Println("  HEAD   /objects/{key}      - Get object metadata")
	log.Println("  GET    /objects            - List objects")
	log.Println("")

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
		log.Printf("Server error: %v", err)
	case sig := <-sigChan:
		log.Printf("Received signal: %v", sig)
	}

	fmt.Println("\nShutting down REST server...")
	fmt.Println("Server stopped")
}
