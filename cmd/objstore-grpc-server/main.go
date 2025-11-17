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

	"github.com/jeremyhahn/go-objstore/pkg/factory"
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

	// Create storage backend
	settings := map[string]string{
		"path": *storagePath,
	}

	storage, err := factory.NewStorage(*backend, settings)
	if err != nil {
		log.Fatalf("Failed to create storage backend: %v", err)
	}

	log.Printf("Initialized %s storage backend", *backend)

	// Create server options
	opts := []grpcserver.ServerOption{
		grpcserver.WithAddress(*addr),
	}

	// Add TLS if certificates provided
	if *tlsCert != "" && *tlsKey != "" {
		tlsOpt, err := grpcserver.WithTLSFromFiles(*tlsCert, *tlsKey)
		if err != nil {
			log.Fatalf("Failed to configure TLS: %v", err)
		}
		opts = append(opts, tlsOpt)
		log.Println("TLS enabled")
	}

	// Create and start server
	server, err := grpcserver.NewServer(storage, opts...)
	if err != nil {
		log.Fatalf("Failed to create gRPC server: %v", err)
	}

	log.Printf("Starting gRPC server on %s", *addr)

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
		log.Printf("Server error: %v", err)
	case sig := <-sigChan:
		log.Printf("Received signal: %v", sig)
	}

	fmt.Println("\nShutting down gRPC server...")
	fmt.Println("Server stopped")
}
