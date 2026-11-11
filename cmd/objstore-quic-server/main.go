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
	"syscall"
	"time"

	"github.com/jeremyhahn/go-objstore/internal/serverconfig"
	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/objstore"
	quicserver "github.com/jeremyhahn/go-objstore/pkg/server/quic"
)

func main() {
	cfg, err := serverconfig.Load(os.Args[0], os.Args[1:], serverconfig.QUICServerDefaults(), serverconfig.BindQUICServerFlags)
	if errors.Is(err, flag.ErrHelp) {
		os.Exit(0)
	}
	if err != nil {
		slog.Error("Invalid configuration", "error", err)
		os.Exit(2)
	}

	slog.Info("Starting QUIC/HTTP3 Object Storage Server")

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

	// Enable replication on the default backend
	policyPath := cfg.Path + "/" + common.ReplicationPolicyFile
	if err := objstore.EnableReplication("", &objstore.ReplicationConfig{
		PolicyFilePath:  policyPath,
		RunInBackground: false,
	}); err != nil {
		slog.Warn("Failed to enable replication", "error", err)
	} else {
		slog.Info("Replication enabled", "policy_file", policyPath)
	}

	// Configure TLS
	var tlsConfig *tls.Config
	if cfg.QUIC.SelfSigned {
		slog.Warn("Using self-signed certificate. DO NOT USE IN PRODUCTION!")
		tlsConfig, err = quicserver.GenerateSelfSignedCert()
		if err != nil {
			slog.Error("Failed to generate self-signed certificate", "error", err)
			os.Exit(1)
		}
	} else {
		if cfg.QUIC.TLSCert == "" || cfg.QUIC.TLSKey == "" {
			slog.Error("TLS certificate and key are required. Use -tlscert and -tlskey flags, or -selfsigned for testing")
			os.Exit(1)
		}
		tlsConfig, err = quicserver.NewTLSConfig(cfg.QUIC.TLSCert, cfg.QUIC.TLSKey)
		if err != nil {
			slog.Error("Failed to load TLS configuration", "error", err)
			os.Exit(1)
		}
	}

	// Create server options
	opts := cfg.QUICOptions(cfg.Middleware()).
		WithBackend(""). // Use default backend
		WithTLSConfig(tlsConfig)

	// Create and start server
	server, err := quicserver.New(opts)
	if err != nil {
		slog.Error("Failed to create QUIC server", "error", err)
		os.Exit(1)
	}

	if err := server.Start(); err != nil {
		slog.Error("Failed to start QUIC server", "error", err)
		os.Exit(1)
	}

	slog.Info("QUIC/HTTP3 server listening", "addr", server.Addr(), "backend", cfg.Backend, "protocol", "HTTP/3 over QUIC (TLS 1.3)")

	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Wait for interrupt signal
	sig := <-sigChan
	slog.Info("Received signal", "signal", sig.String())
	slog.Info("Shutting down gracefully")

	// Create shutdown context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Stop server
	if err := server.Stop(ctx); err != nil {
		cancel()
		slog.Error("Error during shutdown", "error", err)
		os.Exit(1)
	}

	slog.Info("Server stopped successfully")
}
