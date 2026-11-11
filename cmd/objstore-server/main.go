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
	"errors"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/jeremyhahn/go-objstore/internal/serverconfig"
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
	cfg, err := serverconfig.Load(os.Args[0], os.Args[1:], serverconfig.Defaults(), serverconfig.BindServerFlags)
	if errors.Is(err, flag.ErrHelp) {
		os.Exit(0)
	}
	if err != nil {
		slog.Error("Invalid configuration", "error", err)
		os.Exit(2)
	}

	// Shared middleware (rate limiting, one audit logger) applied to every
	// enabled transport by the serverconfig builders.
	mw := cfg.Middleware()

	// Create storage backend
	settings := make(map[string]string)
	settings["path"] = cfg.Path

	storage, err := factory.NewStorage(cfg.Backend, settings, common.Credentials{})
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
	replicationPolicyPath := cfg.Path + "/" + common.ReplicationPolicyFile
	if err := objstore.EnableReplication("", &objstore.ReplicationConfig{
		PolicyFilePath:  replicationPolicyPath,
		RunInBackground: false,
	}); err != nil {
		slog.Warn("Failed to enable replication", "error", err)
	} else {
		slog.Info("Replication enabled", "policy_file", replicationPolicyPath)
	}

	// Startup logging
	slog.Info("Object Storage Server starting", "backend", cfg.Backend)
	if cfg.Backend == "local" {
		slog.Info("Local storage location", "path", cfg.Path)
	}
	if cfg.GRPC.Enabled {
		slog.Info("Service enabled", "service", "grpc", "addr", cfg.GRPC.Addr)
	}
	if cfg.REST.Enabled {
		slog.Info("Service enabled", "service", "rest", "addr", cfg.RESTAddr())
	}
	if cfg.QUIC.Enabled {
		if cfg.QUIC.SelfSigned || (cfg.QUIC.TLSCert != "" && cfg.QUIC.TLSKey != "") {
			slog.Info("Service enabled", "service", "quic", "addr", cfg.QUIC.Addr)
		} else {
			slog.Warn("QUIC/HTTP3 disabled: no TLS configuration")
		}
	}
	if cfg.MCP.Enabled {
		slog.Info("Service enabled", "service", "mcp", "mode", cfg.MCP.Mode, "addr", cfg.MCP.Addr)
	}
	if cfg.Unix.Enabled {
		slog.Info("Service enabled", "service", "unix", "socket", cfg.Unix.Socket)
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
	if cfg.GRPC.Enabled {
		server, err := newGRPCServer(cfg, mw)
		if err != nil {
			errChan <- err
		} else {
			grpcSrv = server
			wg.Add(1)
			go func() {
				defer wg.Done()
				slog.Info("Starting gRPC server", "addr", cfg.GRPC.Addr, "tls", cfg.GRPCTLSEnabled())
				if err := server.Start(); err != nil {
					errChan <- &transportError{Transport: "gRPC", Op: opServe, Err: err}
				}
			}()
		}
	}

	// Start REST Server
	if cfg.REST.Enabled {
		config := cfg.RESTServerConfig(mw)

		server, err := restserver.NewServer(storage, config)
		if err != nil {
			errChan <- &transportError{Transport: "REST", Op: opCreate, Err: err}
		} else {
			restSrv = server
			wg.Add(1)
			go func() {
				defer wg.Done()
				slog.Info("Starting REST server", "host", config.Host, "port", config.Port)
				if err := server.Start(); err != nil {
					errChan <- &transportError{Transport: "REST", Op: opServe, Err: err}
				}
			}()
		}
	}

	// Start QUIC Server
	if cfg.QUIC.Enabled {
		// Configure TLS
		var tlsConfig *tls.Config
		var tlsErr error
		switch {
		case cfg.QUIC.SelfSigned:
			slog.Warn("Using self-signed certificate for QUIC. DO NOT USE IN PRODUCTION!")
			tlsConfig, tlsErr = quicserver.GenerateSelfSignedCert()
			if tlsErr != nil {
				errChan <- &transportError{Transport: "QUIC", Op: "generate self-signed certificate", Err: tlsErr}
			}
		case cfg.QUIC.TLSCert != "" && cfg.QUIC.TLSKey != "":
			tlsConfig, tlsErr = quicserver.NewTLSConfig(cfg.QUIC.TLSCert, cfg.QUIC.TLSKey)
			if tlsErr != nil {
				errChan <- &transportError{Transport: "QUIC", Op: "load TLS configuration", Err: tlsErr}
			}
		default:
			slog.Warn("QUIC server requires TLS. Use --quic-tls-cert and --quic-tls-key, or --quic-self-signed for testing")
		}

		if tlsConfig != nil {
			// Create server options
			opts := cfg.QUICOptions(mw).WithTLSConfig(tlsConfig)

			server, err := quicserver.New(opts)
			if err != nil {
				errChan <- &transportError{Transport: "QUIC", Op: opCreate, Err: err}
			} else {
				quicSrv = server
				wg.Add(1)
				go func() {
					defer wg.Done()
					slog.Info("Starting QUIC server", "addr", cfg.QUIC.Addr)
					if err := server.Start(); err != nil {
						errChan <- &transportError{Transport: "QUIC", Op: opServe, Err: err}
					}
				}()
			}
		}
	}

	// Start MCP Server
	if cfg.MCP.Enabled {
		// Load has already validated the mode.
		config, err := cfg.MCPServerConfig(mw)
		if err != nil {
			errChan <- &transportError{Transport: "MCP", Op: opConfigure, Err: err}
		} else {
			server, err := mcpserver.NewServer(config)
			if err != nil {
				errChan <- &transportError{Transport: "MCP", Op: opCreate, Err: err}
			} else {
				ctx, cancel := context.WithCancel(context.Background())
				mcpCancel = cancel
				wg.Add(1)
				go func() {
					defer wg.Done()
					slog.Info("Starting MCP server", "mode", cfg.MCP.Mode)
					if config.Mode == mcpserver.ModeHTTP {
						slog.Info("MCP server listening", "addr", config.HTTPAddress)
					}
					if err := server.Start(ctx); err != nil {
						errChan <- &transportError{Transport: "MCP", Op: opServe, Err: err}
					}
				}()
			}
		}
	}

	// Start Unix Socket Server
	if cfg.Unix.Enabled {
		server, err := unixserver.NewServer(cfg.UnixServerConfig(mw))
		if err != nil {
			errChan <- &transportError{Transport: "Unix socket", Op: opCreate, Err: err}
		} else {
			ctx, cancel := context.WithCancel(context.Background())
			unixCancel = cancel
			wg.Add(1)
			go func() {
				defer wg.Done()
				slog.Info("Starting Unix socket server", "socket", cfg.Unix.Socket)
				if err := server.Start(ctx); err != nil {
					errChan <- &transportError{Transport: "Unix socket", Op: opServe, Err: err}
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
	if cfg.Unix.Enabled {
		if err := os.Remove(cfg.Unix.Socket); err != nil && !os.IsNotExist(err) {
			slog.Error("Failed to remove Unix socket", "error", err)
		}
	}

	slog.Info("Servers stopped")
}

// newGRPCServer builds the gRPC server from the resolved configuration:
// address, TLS and the shared middleware from cfg.GRPCOptions.
func newGRPCServer(cfg *serverconfig.Config, mw serverconfig.Middleware) (*grpcserver.Server, error) {
	opts, err := cfg.GRPCOptions(mw)
	if err != nil {
		return nil, &transportError{Transport: "gRPC", Op: opConfigure, Err: err}
	}
	server, err := grpcserver.NewServer(opts...)
	if err != nil {
		return nil, &transportError{Transport: "gRPC", Op: opCreate, Err: err}
	}
	return server, nil
}

// Operations a transportError reports.
const (
	opConfigure = "configure"
	opCreate    = "create"
	opServe     = "serve"
)

// transportError reports a transport that failed to configure, start or
// serve, preserving the underlying cause.
type transportError struct {
	Transport string
	Op        string
	Err       error
}

func (e *transportError) Error() string {
	return e.Transport + " server " + e.Op + ": " + e.Err.Error()
}

func (e *transportError) Unwrap() error { return e.Err }
