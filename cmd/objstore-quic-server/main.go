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
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/factory"
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

	log.Println("Starting QUIC/HTTP3 Object Storage Server...")

	// Create storage backend
	settings := map[string]string{
		"path": *storagePath,
	}

	storage, err := factory.NewStorage(*backend, settings)
	if err != nil {
		log.Fatalf("Failed to create storage backend: %v", err)
	}

	// Configure TLS
	var tlsConfig *tls.Config
	if *enableSelfSigned {
		log.Println("WARNING: Using self-signed certificate. DO NOT USE IN PRODUCTION!")
		tlsConfig, err = quicserver.GenerateSelfSignedCert()
		if err != nil {
			log.Fatalf("Failed to generate self-signed certificate: %v", err)
		}
	} else {
		if *tlsCert == "" || *tlsKey == "" {
			log.Fatal("TLS certificate and key are required. Use -tlscert and -tlskey flags, or -selfsigned for testing")
		}
		tlsConfig, err = quicserver.NewTLSConfig(*tlsCert, *tlsKey)
		if err != nil {
			log.Fatalf("Failed to load TLS configuration: %v", err)
		}
	}

	// Create server options
	opts := quicserver.DefaultOptions().
		WithAddr(*addr).
		WithStorage(storage).
		WithTLSConfig(tlsConfig).
		WithMaxRequestBodySize(*maxBodySize).
		WithTimeouts(*readTimeout, *writeTimeout, *idleTimeout).
		WithStreamLimits(*maxStreams, *maxStreams)

	// Create and start server
	server, err := quicserver.New(opts)
	if err != nil {
		log.Fatalf("Failed to create QUIC server: %v", err)
	}

	if err := server.Start(); err != nil {
		log.Fatalf("Failed to start QUIC server: %v", err)
	}

	log.Printf("QUIC/HTTP3 server listening on %s", server.Addr())
	log.Printf("Storage backend: %s", *backend)
	log.Printf("Protocol: HTTP/3 over QUIC (TLS 1.3)")
	log.Println("")
	log.Println("API Endpoints:")
	log.Println("  PUT    /objects/{key}      - Store an object")
	log.Println("  GET    /objects/{key}      - Retrieve an object")
	log.Println("  DELETE /objects/{key}      - Delete an object")
	log.Println("  HEAD   /objects/{key}      - Get object metadata")
	log.Println("  GET    /objects            - List objects")
	log.Println("")

	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Wait for interrupt signal
	sig := <-sigChan
	log.Printf("Received signal: %v", sig)
	log.Println("Shutting down gracefully...")

	// Create shutdown context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Stop server
	if err := server.Stop(ctx); err != nil {
		cancel()
		log.Fatalf("Error during shutdown: %v", err)
	}

	log.Println("Server stopped successfully")
}
