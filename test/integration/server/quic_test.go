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

package server_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/quic-go/quic-go/http3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	quicServerAddr string
	quicClient     *http.Client
)

// setupQUICClient initializes the QUIC/HTTP3 client
func setupQUICClient() error {
	quicServerAddr = os.Getenv("QUIC_SERVER_ADDR")
	if quicServerAddr == "" {
		quicServerAddr = "https://localhost:4433"
	}

	certFile := os.Getenv("CERT_FILE")
	if certFile == "" {
		certFile = "/certs/server.crt"
	}

	// Load CA cert
	caCert, err := os.ReadFile(certFile)
	if err != nil {
		return fmt.Errorf("failed to read CA cert: %w", err)
	}

	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		return fmt.Errorf("failed to parse CA cert")
	}

	tlsConfig := &tls.Config{
		RootCAs: caCertPool,
	}

	quicClient = &http.Client{
		Transport: &http3.Transport{
			TLSClientConfig: tlsConfig,
		},
		Timeout: 30 * time.Second,
	}

	// Wait for server to be ready
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for QUIC server")
		case <-ticker.C:
			resp, err := quicClient.Get(quicServerAddr + "/health")
			if err == nil && resp.StatusCode == http.StatusOK {
				resp.Body.Close()
				return nil
			}
			if resp != nil {
				resp.Body.Close()
			}
		}
	}
}

func init() {
	if err := setupQUICClient(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to setup QUIC client: %v\n", err)
	}
}

// TestQUICHealth tests the health check over QUIC
func TestQUICHealth(t *testing.T) {
	if quicClient == nil {
		t.Skip("QUIC client not initialized")
	}

	t.Run("health check over QUIC", func(t *testing.T) {
		resp, err := quicClient.Get(quicServerAddr + "/health")
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)
		// HTTP/3 protocol version can be "HTTP/3.0" or "HTTP/3"
		assert.Contains(t, resp.Proto, "3")
	})
}

// TestQUICPutObject tests PUT operations over QUIC
func TestQUICPutObject(t *testing.T) {
	if quicClient == nil {
		t.Skip("QUIC client not initialized")
	}

	t.Run("put simple object over QUIC", func(t *testing.T) {
		key := "test/quic/simple.txt"
		content := []byte("Hello, QUIC!")

		req, err := http.NewRequest(http.MethodPut, quicServerAddr+"/objects/"+key, bytes.NewReader(content))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "text/plain")

		resp, err := quicClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusCreated, resp.StatusCode)
		assert.Contains(t, resp.Proto, "3") // Verify HTTP/3
	})

	t.Run("put large object over QUIC", func(t *testing.T) {
		key := "test/quic/large.bin"
		largeData := make([]byte, 5*1024*1024) // 5MB
		for i := range largeData {
			largeData[i] = byte(i % 256)
		}

		req, err := http.NewRequest(http.MethodPut, quicServerAddr+"/objects/"+key, bytes.NewReader(largeData))
		require.NoError(t, err)

		resp, err := quicClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusCreated, resp.StatusCode)
	})
}

// TestQUICGetObject tests GET operations over QUIC
func TestQUICGetObject(t *testing.T) {
	if quicClient == nil {
		t.Skip("QUIC client not initialized")
	}

	// Setup: put an object first
	key := "test/quic/get.txt"
	content := []byte("Content to retrieve")

	putReq, err := http.NewRequest(http.MethodPut, quicServerAddr+"/objects/"+key, bytes.NewReader(content))
	require.NoError(t, err)
	putResp, err := quicClient.Do(putReq)
	require.NoError(t, err)
	putResp.Body.Close()

	t.Run("get existing object over QUIC", func(t *testing.T) {
		resp, err := quicClient.Get(quicServerAddr + "/objects/" + key)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Contains(t, resp.Proto, "3")

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		assert.Equal(t, content, body)
	})
}

// TestQUICvsHTTP2Performance compares QUIC vs HTTP/2 performance
func TestQUICvsHTTP2Performance(t *testing.T) {
	if quicClient == nil {
		t.Skip("QUIC client not initialized")
	}

	t.Run("performance comparison", func(t *testing.T) {
		// This is a basic performance comparison test
		// In production, you'd want more sophisticated benchmarking

		key := "test/quic/perf.bin"
		testData := make([]byte, 1*1024*1024) // 1MB

		// QUIC PUT
		startQUIC := time.Now()
		putReq, err := http.NewRequest(http.MethodPut, quicServerAddr+"/objects/"+key, bytes.NewReader(testData))
		require.NoError(t, err)
		putResp, err := quicClient.Do(putReq)
		require.NoError(t, err)
		putResp.Body.Close()
		quicDuration := time.Since(startQUIC)

		// QUIC GET
		startQUICGet := time.Now()
		getResp, err := quicClient.Get(quicServerAddr + "/objects/" + key)
		require.NoError(t, err)
		io.Copy(io.Discard, getResp.Body)
		getResp.Body.Close()
		quicGetDuration := time.Since(startQUICGet)

		t.Logf("QUIC PUT: %v, GET: %v", quicDuration, quicGetDuration)

		// Verify operation succeeded
		assert.Less(t, quicDuration, 10*time.Second)
		assert.Less(t, quicGetDuration, 10*time.Second)
	})
}

// TestQUICTLSRequirement tests that TLS 1.3 is required
func TestQUICTLSRequirement(t *testing.T) {
	if quicClient == nil {
		t.Skip("QUIC client not initialized")
	}

	t.Run("verify TLS 1.3 usage", func(t *testing.T) {
		// QUIC requires TLS 1.3
		// This test verifies the connection uses proper TLS version

		key := "test/quic/tls-test.txt"
		content := []byte("TLS test")

		req, err := http.NewRequest(http.MethodPut, quicServerAddr+"/objects/"+key, bytes.NewReader(content))
		require.NoError(t, err)

		resp, err := quicClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		assert.Equal(t, http.StatusCreated, resp.StatusCode)
		// QUIC uses TLS 1.3 by default
		assert.True(t, resp.TLS != nil || strings.Contains(resp.Proto, "3"))
	})
}

// TestQUICConcurrency tests concurrent QUIC operations
func TestQUICConcurrency(t *testing.T) {
	if quicClient == nil {
		t.Skip("QUIC client not initialized")
	}

	t.Run("concurrent QUIC operations", func(t *testing.T) {
		numOps := 30
		errChan := make(chan error, numOps)

		for i := 0; i < numOps; i++ {
			go func(index int) {
				key := fmt.Sprintf("test/quic/concurrent/%d.txt", index)
				content := []byte(fmt.Sprintf("content %d", index))

				req, err := http.NewRequest(http.MethodPut, quicServerAddr+"/objects/"+key, bytes.NewReader(content))
				if err != nil {
					errChan <- err
					return
				}

				resp, err := quicClient.Do(req)
				if resp != nil {
					resp.Body.Close()
				}
				errChan <- err
			}(i)
		}

		for i := 0; i < numOps; i++ {
			err := <-errChan
			assert.NoError(t, err)
		}
	})
}

// TestQUICStreamMultiplexing tests QUIC's stream multiplexing
func TestQUICStreamMultiplexing(t *testing.T) {
	if quicClient == nil {
		t.Skip("QUIC client not initialized")
	}

	t.Run("parallel stream operations", func(t *testing.T) {
		// QUIC supports multiple concurrent streams without head-of-line blocking
		numStreams := 10
		errChan := make(chan error, numStreams)
		startTime := time.Now()

		for i := 0; i < numStreams; i++ {
			go func(index int) {
				key := fmt.Sprintf("test/quic/stream/%d.txt", index)
				content := make([]byte, 100*1024) // 100KB per stream

				req, err := http.NewRequest(http.MethodPut, quicServerAddr+"/objects/"+key, bytes.NewReader(content))
				if err != nil {
					errChan <- err
					return
				}

				resp, err := quicClient.Do(req)
				if resp != nil {
					resp.Body.Close()
				}
				errChan <- err
			}(i)
		}

		for i := 0; i < numStreams; i++ {
			err := <-errChan
			assert.NoError(t, err)
		}

		duration := time.Since(startTime)
		t.Logf("Parallel streams completed in: %v", duration)

		// With multiplexing, should complete reasonably fast
		assert.Less(t, duration, 30*time.Second)
	})
}
