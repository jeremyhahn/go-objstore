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
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/factory"
	"github.com/jeremyhahn/go-objstore/pkg/objstore"
	grpcserver "github.com/jeremyhahn/go-objstore/pkg/server/grpc"
	mcpserver "github.com/jeremyhahn/go-objstore/pkg/server/mcp"
	quicserver "github.com/jeremyhahn/go-objstore/pkg/server/quic"
	restserver "github.com/jeremyhahn/go-objstore/pkg/server/rest"
	unixserver "github.com/jeremyhahn/go-objstore/pkg/server/unix"
)

func main() {
	// Backend configuration
	backend := flag.String("backend", "local", "Storage backend (local, s3, gcs, azure)")
	basePath := flag.String("path", "/tmp/objstore", "Base path for local storage")

	// Server selection (all enabled by default)
	enableGRPC := flag.Bool("grpc", true, "Enable gRPC server")
	enableREST := flag.Bool("rest", true, "Enable REST server")
	enableQUIC := flag.Bool("quic", true, "Enable QUIC/HTTP3 server")
	enableMCP := flag.Bool("mcp", true, "Enable MCP server")
	enableUnix := flag.Bool("unix", false, "Enable Unix socket server")

	// gRPC server flags
	grpcAddr := flag.String("grpc-addr", ":50051", "gRPC server address")

	// REST server flags
	restPort := flag.Int("rest-port", 8080, "REST server port")

	// QUIC server flags
	quicAddr := flag.String("quic-addr", ":4433", "QUIC server address")
	quicTLSCert := flag.String("quic-tls-cert", "", "QUIC TLS certificate file")
	quicTLSKey := flag.String("quic-tls-key", "", "QUIC TLS key file")
	quicSelfSigned := flag.Bool("quic-self-signed", false, "Use self-signed cert for QUIC (testing only)")

	// MCP server flags
	mcpMode := flag.String("mcp-mode", "http", "MCP mode: stdio or http")
	mcpAddr := flag.String("mcp-addr", ":8081", "MCP HTTP server address")

	// Unix socket server flags
	unixSocket := flag.String("unix-socket", "/var/run/objstore.sock", "Unix socket path")

	flag.Parse()

	// Create storage backend
	settings := make(map[string]string)
	settings["path"] = *basePath

	storage, err := factory.NewStorage(*backend, settings)
	if err != nil {
		log.Fatalf("Failed to create storage backend: %v", err)
	}

	// Initialize the objstore facade
	if err := objstore.Initialize(&objstore.FacadeConfig{
		Backends:       map[string]common.Storage{"default": storage},
		DefaultBackend: "default",
	}); err != nil {
		log.Fatalf("Failed to initialize objstore facade: %v", err)
	}

	// Enhanced startup logging
	log.Println("============================================================")
	log.Println("  Object Storage Server")
	log.Println("============================================================")
	log.Printf("Storage Backend: %s", *backend)
	if *backend == "local" {
		log.Printf("Storage Location: %s", *basePath)
		log.Printf("  → Objects will be stored at: %s", *basePath)
	}
	log.Printf("Enabled Services:")
	if *enableGRPC {
		log.Printf("  ✓ gRPC Server: %s", *grpcAddr)
	}
	if *enableREST {
		log.Printf("  ✓ REST API: http://0.0.0.0:%d", *restPort)
	}
	if *enableQUIC {
		if *quicSelfSigned || (*quicTLSCert != "" && *quicTLSKey != "") {
			log.Printf("  ✓ QUIC/HTTP3: %s", *quicAddr)
		} else {
			log.Printf("  ⨯ QUIC/HTTP3: Disabled (no TLS configuration)")
		}
	}
	if *enableMCP {
		log.Printf("  ✓ MCP Server: %s mode on %s", *mcpMode, *mcpAddr)
	}
	if *enableUnix {
		log.Printf("  ✓ Unix Socket: %s", *unixSocket)
	}
	log.Println("============================================================")
	log.Printf("Initialized %s storage backend", *backend)

	// Channel for errors
	errChan := make(chan error, 5)

	// Start gRPC Server
	if *enableGRPC {
		go func() {
			opts := []grpcserver.ServerOption{
				grpcserver.WithAddress(*grpcAddr),
			}

			server, err := grpcserver.NewServer(opts...)
			if err != nil {
				errChan <- fmt.Errorf("failed to create gRPC server: %w", err)
				return
			}

			log.Printf("Starting gRPC server on %s", *grpcAddr)
			if err := server.Start(); err != nil {
				errChan <- fmt.Errorf("gRPC server error: %w", err)
			}
		}()
	}

	// Start REST Server
	if *enableREST {
		go func() {
			config := restserver.DefaultServerConfig()
			config.Port = *restPort

			server, err := restserver.NewServer(storage, config)
			if err != nil {
				errChan <- fmt.Errorf("failed to create REST server: %w", err)
				return
			}

			log.Printf("Starting REST server on %s:%d", config.Host, config.Port)
			if err := server.Start(); err != nil {
				errChan <- fmt.Errorf("REST server error: %w", err)
			}
		}()
	}

	// Start QUIC Server
	if *enableQUIC {
		go func() {
			// Configure TLS
			var tlsConfig *tls.Config
			var err error
			switch {
			case *quicSelfSigned:
				log.Println("WARNING: Using self-signed certificate for QUIC. DO NOT USE IN PRODUCTION!")
				tlsConfig, err = quicserver.GenerateSelfSignedCert()
				if err != nil {
					errChan <- fmt.Errorf("failed to generate self-signed certificate: %w", err)
					return
				}
			case *quicTLSCert != "" && *quicTLSKey != "":
				tlsConfig, err = quicserver.NewTLSConfig(*quicTLSCert, *quicTLSKey)
				if err != nil {
					errChan <- fmt.Errorf("failed to load TLS configuration: %w", err)
					return
				}
			default:
				log.Println("QUIC server requires TLS. Use --quic-tls-cert and --quic-tls-key, or --quic-self-signed for testing")
				return
			}

			// Create server options
			opts := quicserver.DefaultOptions().
				WithAddr(*quicAddr).
				WithTLSConfig(tlsConfig)

			server, err := quicserver.New(opts)
			if err != nil {
				errChan <- fmt.Errorf("failed to create QUIC server: %w", err)
				return
			}

			log.Printf("Starting QUIC server on %s", *quicAddr)
			if err := server.Start(); err != nil {
				errChan <- fmt.Errorf("QUIC server error: %w", err)
			}
		}()
	}

	// Start MCP Server
	if *enableMCP {
		go func() {
			// Configure MCP server
			var serverMode mcpserver.ServerMode
			switch *mcpMode {
			case "stdio":
				serverMode = mcpserver.ModeStdio
			case "http":
				serverMode = mcpserver.ModeHTTP
			default:
				log.Printf("Invalid MCP mode: %s (must be 'stdio' or 'http')", *mcpMode)
				return
			}

			config := &mcpserver.ServerConfig{
				Mode:        serverMode,
				HTTPAddress: *mcpAddr,
			}

			server, err := mcpserver.NewServer(config)
			if err != nil {
				errChan <- fmt.Errorf("failed to create MCP server: %w", err)
				return
			}

			log.Printf("Starting MCP server in %s mode", *mcpMode)
			if *mcpMode == "http" {
				log.Printf("MCP server listening on %s", *mcpAddr)
			}

			// Create context that cancels on shutdown signal
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			if err := server.Start(ctx); err != nil {
				errChan <- fmt.Errorf("MCP server error: %w", err)
			}
		}()
	}

	// Start Unix Socket Server
	if *enableUnix {
		go func() {
			config := &unixserver.ServerConfig{
				SocketPath: *unixSocket,
				Backend:    "default",
			}

			server, err := unixserver.NewServer(config)
			if err != nil {
				errChan <- fmt.Errorf("failed to create Unix socket server: %w", err)
				return
			}

			log.Printf("Starting Unix socket server on %s", *unixSocket)

			// Create context that cancels on shutdown signal
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			if err := server.Start(ctx); err != nil {
				errChan <- fmt.Errorf("Unix socket server error: %w", err)
			}
		}()
	}

	// Wait for interrupt signal or error
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-errChan:
		log.Printf("Server error: %v", err)
	case sig := <-sigChan:
		log.Printf("Received signal: %v", sig)
	}

	fmt.Println("\nShutting down servers...")
	fmt.Println("Servers stopped")
}
