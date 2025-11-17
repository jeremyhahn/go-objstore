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

package client

import (
	"testing"
)

func TestNewClient_REST(t *testing.T) {
	tests := []struct {
		name     string
		protocol string
	}{
		{"rest", "rest"},
		{"http", "http"},
		{"https", "https"},
		{"empty", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &Config{
				ServerURL: "http://localhost:8080",
				Protocol:  tt.protocol,
			}

			client, err := NewClient(config)
			if err != nil {
				t.Fatalf("NewClient failed: %v", err)
			}

			if _, ok := client.(*RESTClient); !ok {
				t.Errorf("expected RESTClient, got %T", client)
			}

			client.Close()
		})
	}
}

func TestNewClient_GRPC(t *testing.T) {
	config := &Config{
		ServerURL: "localhost:50051",
		Protocol:  "grpc",
	}

	client, err := NewClient(config)
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}

	if _, ok := client.(*GRPCClient); !ok {
		t.Errorf("expected GRPCClient, got %T", client)
	}

	client.Close()
}

func TestNewClient_QUIC(t *testing.T) {
	tests := []struct {
		name     string
		protocol string
	}{
		{"quic", "quic"},
		{"http3", "http3"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &Config{
				ServerURL: "https://localhost:4433",
				Protocol:  tt.protocol,
			}

			client, err := NewClient(config)
			if err != nil {
				t.Fatalf("NewClient failed: %v", err)
			}

			if _, ok := client.(*QUICClient); !ok {
				t.Errorf("expected QUICClient, got %T", client)
			}

			client.Close()
		})
	}
}

func TestNewClient_UnsupportedProtocol(t *testing.T) {
	config := &Config{
		ServerURL: "http://localhost:8080",
		Protocol:  "unknown",
	}

	_, err := NewClient(config)
	if err == nil {
		t.Error("expected error for unsupported protocol")
	}
}

func TestNewClient_EmptyURL(t *testing.T) {
	config := &Config{
		ServerURL: "",
		Protocol:  "rest",
	}

	_, err := NewClient(config)
	if err == nil {
		t.Error("expected error for empty URL")
	}
}

func TestNewClient_NilConfig(t *testing.T) {
	_, err := NewClient(nil)
	if err == nil {
		t.Error("expected error for nil config")
	}
}
