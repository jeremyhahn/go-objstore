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
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/audit"
	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/factory"
	"github.com/jeremyhahn/go-objstore/pkg/objstore"
	grpcserver "github.com/jeremyhahn/go-objstore/pkg/server/grpc"
	mcpserver "github.com/jeremyhahn/go-objstore/pkg/server/mcp"
	"github.com/jeremyhahn/go-objstore/pkg/server/middleware"
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
	metricsPublic := flag.Bool("metrics-public", false, "Expose /metrics without authorization")

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

	// Cross-transport middleware flags
	rateLimit := flag.Bool("rate-limit", false, "Enable rate limiting on all transports")
	rateLimitRPS := flag.Float64("rate-limit-rps", 100, "Rate limit requests per second")
	rateLimitBurst := flag.Int("rate-limit-burst", 200, "Rate limit burst size")
	rateLimitPerClient := flag.Bool("rate-limit-per-client", false, "Rate limit per client instead of globally")
	enableAudit := flag.Bool("audit", true, "Enable audit logging on all transports")

	flag.Parse()

	// Shared middleware configuration applied to every enabled transport.
	rateLimitConfig := &middleware.RateLimitConfig{
		RequestsPerSecond: *rateLimitRPS,
		Burst:             *rateLimitBurst,
		PerIP:             *rateLimitPerClient,
	}
	var auditLogger audit.AuditLogger
	if *enableAudit {
		auditLogger = audit.NewDefaultAuditLogger()
	}

	// Create storage backend
	settings := make(map[string]string)
	settings["path"] = *basePath

	storage, err := factory.NewStorage(*backend, settings)
	if err != nil {
		slog.Error("Failed to create storage backend", "error", err)
		os.Exit(1)
	}

	// Initialize the objstore facade
	if err := objstore.Initialize(&objstore.FacadeConfig{
		Backends:       map[string]common.Storage{"default": storage},
		DefaultBackend: "default",
	}); err != nil {
		slog.Error("Failed to initialize objstore facade", "error", err)
		os.Exit(1)
	}

	// Enable replication on the default backend so the replication API
	// (policies, trigger, status) is fully functional. Backends that do not
	// support a replication manager simply log a warning and continue.
	replicationPolicyPath := *basePath + "/.replication-policies.json"
	if err := objstore.EnableReplication("", &objstore.ReplicationConfig{
		PolicyFilePath:  replicationPolicyPath,
		RunInBackground: false,
	}); err != nil {
		slog.Warn("Failed to enable replication", "error", err)
	} else {
		slog.Info("Replication enabled", "policy_file", replicationPolicyPath)
	}

	// Startup logging
	slog.Info("Object Storage Server starting", "backend", *backend)
	if *backend == "local" {
		slog.Info("Local storage location", "path", *basePath)
	}
	if *enableGRPC {
		slog.Info("Service enabled", "service", "grpc", "addr", *grpcAddr)
	}
	if *enableREST {
		slog.Info("Service enabled", "service", "rest", "addr", fmt.Sprintf("0.0.0.0:%d", *restPort))
	}
	if *enableQUIC {
		if *quicSelfSigned || (*quicTLSCert != "" && *quicTLSKey != "") {
			slog.Info("Service enabled", "service", "quic", "addr", *quicAddr)
		} else {
			slog.Warn("QUIC/HTTP3 disabled: no TLS configuration")
		}
	}
	if *enableMCP {
		slog.Info("Service enabled", "service", "mcp", "mode", *mcpMode, "addr", *mcpAddr)
	}
	if *enableUnix {
		slog.Info("Service enabled", "service", "unix", "socket", *unixSocket)
	}

	// Channel for errors
	errChan := make(chan error, 5)

	// Capture server references for graceful shutdown. Servers are constructed
	// synchronously here, before their goroutines start, so the shutdown path
	// below reads these variables without racing the transport goroutines.
	var grpcSrv *grpcserver.Server
	var restSrv *restserver.Server
	var quicSrv *quicserver.Server
	var mcpCancel context.CancelFunc
	var unixCancel context.CancelFunc

	// wg tracks the transport goroutines, which run only the blocking
	// Start/Serve calls.
	var wg sync.WaitGroup

	// Start gRPC Server
	if *enableGRPC {
		opts := []grpcserver.ServerOption{
			grpcserver.WithAddress(*grpcAddr),
		}
		if *rateLimit {
			opts = append(opts, grpcserver.WithRateLimit(true, rateLimitConfig))
		}

		server, err := grpcserver.NewServer(opts...)
		if err != nil {
			errChan <- fmt.Errorf("failed to create gRPC server: %w", err)
		} else {
			grpcSrv = server
			wg.Add(1)
			go func() {
				defer wg.Done()
				slog.Info("Starting gRPC server", "addr", *grpcAddr)
				if err := server.Start(); err != nil {
					errChan <- fmt.Errorf("gRPC server error: %w", err)
				}
			}()
		}
	}

	// Start REST Server
	if *enableREST {
		config := restserver.DefaultServerConfig()
		config.Port = *restPort
		config.MetricsPublic = *metricsPublic
		config.EnableRateLimit = *rateLimit
		config.RateLimitConfig = rateLimitConfig
		config.EnableAudit = *enableAudit
		if auditLogger != nil {
			config.AuditLogger = auditLogger
		}

		server, err := restserver.NewServer(storage, config)
		if err != nil {
			errChan <- fmt.Errorf("failed to create REST server: %w", err)
		} else {
			restSrv = server
			wg.Add(1)
			go func() {
				defer wg.Done()
				slog.Info("Starting REST server", "host", config.Host, "port", config.Port)
				if err := server.Start(); err != nil {
					errChan <- fmt.Errorf("REST server error: %w", err)
				}
			}()
		}
	}

	// Start QUIC Server
	if *enableQUIC {
		// Configure TLS
		var tlsConfig *tls.Config
		var tlsErr error
		switch {
		case *quicSelfSigned:
			slog.Warn("Using self-signed certificate for QUIC. DO NOT USE IN PRODUCTION!")
			tlsConfig, tlsErr = quicserver.GenerateSelfSignedCert()
			if tlsErr != nil {
				errChan <- fmt.Errorf("failed to generate self-signed certificate: %w", tlsErr)
			}
		case *quicTLSCert != "" && *quicTLSKey != "":
			tlsConfig, tlsErr = quicserver.NewTLSConfig(*quicTLSCert, *quicTLSKey)
			if tlsErr != nil {
				errChan <- fmt.Errorf("failed to load TLS configuration: %w", tlsErr)
			}
		default:
			slog.Warn("QUIC server requires TLS. Use --quic-tls-cert and --quic-tls-key, or --quic-self-signed for testing")
		}

		if tlsConfig != nil {
			// Create server options
			opts := quicserver.DefaultOptions().
				WithAddr(*quicAddr).
				WithTLSConfig(tlsConfig)
			if *rateLimit {
				opts = opts.WithRateLimit(rateLimitConfig)
			}
			if *enableAudit {
				opts = opts.WithAudit(auditLogger)
			}

			server, err := quicserver.New(opts)
			if err != nil {
				errChan <- fmt.Errorf("failed to create QUIC server: %w", err)
			} else {
				quicSrv = server
				wg.Add(1)
				go func() {
					defer wg.Done()
					slog.Info("Starting QUIC server", "addr", *quicAddr)
					if err := server.Start(); err != nil {
						errChan <- fmt.Errorf("QUIC server error: %w", err)
					}
				}()
			}
		}
	}

	// Start MCP Server
	if *enableMCP {
		// Configure MCP server
		var serverMode mcpserver.ServerMode
		validMode := true
		switch *mcpMode {
		case "stdio":
			serverMode = mcpserver.ModeStdio
		case "http":
			serverMode = mcpserver.ModeHTTP
		default:
			slog.Error("Invalid MCP mode (must be 'stdio' or 'http')", "mode", *mcpMode)
			validMode = false
		}

		if validMode {
			config := &mcpserver.ServerConfig{
				Mode:            serverMode,
				HTTPAddress:     *mcpAddr,
				EnableRateLimit: *rateLimit,
				RateLimitConfig: rateLimitConfig,
				EnableAudit:     *enableAudit,
				AuditLogger:     auditLogger,
			}

			server, err := mcpserver.NewServer(config)
			if err != nil {
				errChan <- fmt.Errorf("failed to create MCP server: %w", err)
			} else {
				ctx, cancel := context.WithCancel(context.Background())
				mcpCancel = cancel
				wg.Add(1)
				go func() {
					defer wg.Done()
					slog.Info("Starting MCP server", "mode", *mcpMode)
					if *mcpMode == "http" {
						slog.Info("MCP server listening", "addr", *mcpAddr)
					}
					if err := server.Start(ctx); err != nil {
						errChan <- fmt.Errorf("MCP server error: %w", err)
					}
				}()
			}
		}
	}

	// Start Unix Socket Server
	if *enableUnix {
		config := &unixserver.ServerConfig{
			SocketPath:      *unixSocket,
			Backend:         "default",
			EnableRateLimit: *rateLimit,
			RateLimitConfig: rateLimitConfig,
			EnableAudit:     *enableAudit,
			AuditLogger:     auditLogger,
		}

		server, err := unixserver.NewServer(config)
		if err != nil {
			errChan <- fmt.Errorf("failed to create Unix socket server: %w", err)
		} else {
			ctx, cancel := context.WithCancel(context.Background())
			unixCancel = cancel
			wg.Add(1)
			go func() {
				defer wg.Done()
				slog.Info("Starting Unix socket server", "socket", *unixSocket)
				if err := server.Start(ctx); err != nil {
					errChan <- fmt.Errorf("Unix socket server error: %w", err)
				}
			}()
		}
	}

	// Wait for interrupt signal or error
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-errChan:
		slog.Error("Server error", "error", err)
	case sig := <-sigChan:
		slog.Info("Received signal", "signal", sig.String())
	}

	slog.Info("Shutting down servers")

	// Bounded shutdown context.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	// Stop gRPC (GracefulStop is context-unaware; run in goroutine with deadline).
	if grpcSrv != nil {
		done := make(chan struct{})
		go func() {
			grpcSrv.Stop()
			close(done)
		}()
		select {
		case <-done:
		case <-shutdownCtx.Done():
			grpcSrv.ForceStop()
		}
	}

	// Stop REST.
	if restSrv != nil {
		if err := restSrv.Shutdown(shutdownCtx); err != nil {
			slog.Error("REST server shutdown error", "error", err)
		}
	}

	// Stop QUIC.
	if quicSrv != nil {
		if err := quicSrv.Stop(shutdownCtx); err != nil {
			slog.Error("QUIC server shutdown error", "error", err)
		}
	}

	// Cancel MCP context (startHTTP calls server.Shutdown on ctx.Done).
	if mcpCancel != nil {
		mcpCancel()
	}

	// Cancel Unix context (Start returns after Shutdown on ctx.Done).
	if unixCancel != nil {
		unixCancel()
	}

	// Wait for all transport goroutines to exit before cleaning up. The wait
	// is bounded by the shutdown context: MCP stdio mode only returns when
	// stdin closes, so a stuck transport must not prevent process exit.
	waitDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitDone)
	}()
	select {
	case <-waitDone:
	case <-shutdownCtx.Done():
		slog.Warn("Timed out waiting for servers to stop")
	}

	// Remove Unix socket file if it still exists.
	if *enableUnix {
		if err := os.Remove(*unixSocket); err != nil && !os.IsNotExist(err) {
			slog.Error("Failed to remove Unix socket", "error", err)
		}
	}

	slog.Info("Servers stopped")
}
