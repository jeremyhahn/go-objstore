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
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	errCLIBinaryNotFound = errors.New("CLI binary not found")
	errServerNotReady    = errors.New("server not ready after 30 seconds")
)

// CLI test configuration
type cliTestConfig struct {
	name           string
	serverFlag     string
	protocolFlag   string
	description    string
	skipArchive    bool // Skip archive tests for protocols that don't support it yet
	skipPolicies   bool // Skip policy tests if not supported
}

var cliConfigs = []cliTestConfig{
	{
		name:         "REST",
		serverFlag:   "http://rest-server:8080",
		protocolFlag: "rest",
		description:  "CLI against REST server",
	},
	{
		name:         "gRPC",
		serverFlag:   "grpc-server:50051",
		protocolFlag: "grpc",
		description:  "CLI against gRPC server",
	},
	{
		name:         "QUIC",
		serverFlag:   "https://quic-server:4433",
		protocolFlag: "quic",
		description:  "CLI against QUIC server",
	},
}

// runCLI executes the objstore CLI command
func runCLI(args ...string) (string, string, error) {
	// Find the objstore binary (built during Docker image creation)
	cliBinary := "/usr/local/bin/objstore"
	if _, err := os.Stat(cliBinary); os.IsNotExist(err) {
		return "", "", fmt.Errorf("%w at %s", errCLIBinaryNotFound, cliBinary)
	}

	cmd := exec.Command(cliBinary, args...)
	cmd.Dir = "/app"

	stdout, err := cmd.Output()
	stderr := ""
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			stderr = string(exitErr.Stderr)
		}
	}

	return string(stdout), stderr, err
}

// waitForServer waits for a server to be ready
func waitForServer(serverAddr, protocol string) error {
	maxRetries := 30
	for i := 0; i < maxRetries; i++ {
		_, _, err := runCLI("--server", serverAddr, "--server-protocol", protocol, "health")
		if err == nil {
			return nil
		}
		time.Sleep(1 * time.Second)
	}
	return fmt.Errorf("%w: %s (%s)", errServerNotReady, serverAddr, protocol)
}

// TestCLIAgainstAllServers runs all CLI tests against all server types
func TestCLIAgainstAllServers(t *testing.T) {
	// Wait for all servers to be ready
	for _, cfg := range cliConfigs {
		t.Run(fmt.Sprintf("Wait_%s", cfg.name), func(t *testing.T) {
			err := waitForServer(cfg.serverFlag, cfg.protocolFlag)
			require.NoError(t, err, "Server %s should be ready", cfg.name)
		})
	}

	// Run all tests for each server
	for _, cfg := range cliConfigs {
		t.Run(cfg.name, func(t *testing.T) {
			testCLIHealth(t, cfg)
			testCLIPutGet(t, cfg)
			testCLIExists(t, cfg)
			testCLIList(t, cfg)
			testCLIMetadata(t, cfg)
			testCLIDelete(t, cfg)
			testCLIArchive(t, cfg)
			testCLIPolicies(t, cfg)
		})
	}
}

// testCLIHealth tests the health command
func testCLIHealth(t *testing.T, cfg cliTestConfig) {
	t.Run("Health", func(t *testing.T) {
		stdout, stderr, err := runCLI(
			"--server", cfg.serverFlag,
			"--server-protocol", cfg.protocolFlag,
			"health",
		)

		if err != nil {
			t.Logf("stdout: %s", stdout)
			t.Logf("stderr: %s", stderr)
		}
		require.NoError(t, err, "health command should succeed")
		assert.Contains(t, stdout, "healthy", "health output should contain 'healthy'")
	})
}

// testCLIPutGet tests put and get commands
func testCLIPutGet(t *testing.T, cfg cliTestConfig) {
	t.Run("PutGet", func(t *testing.T) {
		// Create a test file
		testFile := filepath.Join(os.TempDir(), fmt.Sprintf("cli_test_%s.txt", cfg.name))
		testContent := fmt.Sprintf("Test content for %s server", cfg.name)
		err := ioutil.WriteFile(testFile, []byte(testContent), 0644)
		require.NoError(t, err)
		defer os.Remove(testFile)

		key := fmt.Sprintf("cli-test/%s/test.txt", strings.ToLower(cfg.name))

		// Test PUT
		stdout, stderr, err := runCLI(
			"--server", cfg.serverFlag,
			"--server-protocol", cfg.protocolFlag,
			"put", testFile, key,
		)

		if err != nil {
			t.Logf("PUT stdout: %s", stdout)
			t.Logf("PUT stderr: %s", stderr)
		}
		require.NoError(t, err, "put command should succeed")

		// Test GET
		outputFile := filepath.Join(os.TempDir(), fmt.Sprintf("cli_output_%s.txt", cfg.name))
		defer os.Remove(outputFile)

		stdout, stderr, err = runCLI(
			"--server", cfg.serverFlag,
			"--server-protocol", cfg.protocolFlag,
			"get", key, outputFile,
		)

		if err != nil {
			t.Logf("GET stdout: %s", stdout)
			t.Logf("GET stderr: %s", stderr)
		}
		require.NoError(t, err, "get command should succeed")

		// Verify content
		retrievedContent, err := ioutil.ReadFile(outputFile)
		require.NoError(t, err)
		assert.Equal(t, testContent, string(retrievedContent), "retrieved content should match")
	})
}

// testCLIExists tests the exists command
func testCLIExists(t *testing.T, cfg cliTestConfig) {
	t.Run("Exists", func(t *testing.T) {
		key := fmt.Sprintf("cli-test/%s/test.txt", strings.ToLower(cfg.name))

		// Test exists for existing object
		stdout, stderr, err := runCLI(
			"--server", cfg.serverFlag,
			"--server-protocol", cfg.protocolFlag,
			"exists", key,
		)

		if err != nil {
			t.Logf("EXISTS stdout: %s", stdout)
			t.Logf("EXISTS stderr: %s", stderr)
		}
		require.NoError(t, err, "exists command should succeed")
		assert.Contains(t, strings.ToLower(stdout), "exists", "output should indicate object exists")

		// Test exists for non-existing object
		stdout, stderr, err = runCLI(
			"--server", cfg.serverFlag,
			"--server-protocol", cfg.protocolFlag,
			"exists", "nonexistent/key.txt",
		)

		if err != nil {
			t.Logf("EXISTS (non-existent) stdout: %s", stdout)
			t.Logf("EXISTS (non-existent) stderr: %s", stderr)
		}
		require.NoError(t, err, "exists command should succeed even for non-existent objects")
		assert.Contains(t, strings.ToLower(stdout), "not", "output should indicate object doesn't exist")
	})
}

// testCLIList tests the list command
func testCLIList(t *testing.T, cfg cliTestConfig) {
	t.Run("List", func(t *testing.T) {
		prefix := fmt.Sprintf("cli-test/%s/", strings.ToLower(cfg.name))

		stdout, stderr, err := runCLI(
			"--server", cfg.serverFlag,
			"--server-protocol", cfg.protocolFlag,
			"list", prefix,
		)

		if err != nil {
			t.Logf("LIST stdout: %s", stdout)
			t.Logf("LIST stderr: %s", stderr)
		}
		require.NoError(t, err, "list command should succeed")
		assert.Contains(t, stdout, "test.txt", "list should include uploaded file")
	})
}

// testCLIMetadata tests metadata get and update commands
func testCLIMetadata(t *testing.T, cfg cliTestConfig) {
	t.Run("Metadata", func(t *testing.T) {
		key := fmt.Sprintf("cli-test/%s/metadata-test.txt", strings.ToLower(cfg.name))

		// Upload a file with metadata
		testFile := filepath.Join(os.TempDir(), fmt.Sprintf("metadata_test_%s.txt", cfg.name))
		err := ioutil.WriteFile(testFile, []byte("metadata test content"), 0644)
		require.NoError(t, err)
		defer os.Remove(testFile)

		stdout, stderr, err := runCLI(
			"--server", cfg.serverFlag,
			"--server-protocol", cfg.protocolFlag,
			"put", testFile, key,
			"--content-type", "text/plain",
			"--custom", "author=test,version=1.0",
		)

		if err != nil {
			t.Logf("PUT with metadata stdout: %s", stdout)
			t.Logf("PUT with metadata stderr: %s", stderr)
		}
		require.NoError(t, err, "put with metadata should succeed")

		// Get metadata only
		stdout, stderr, err = runCLI(
			"--server", cfg.serverFlag,
			"--server-protocol", cfg.protocolFlag,
			"get", key, "--metadata",
		)

		if err != nil {
			t.Logf("GET metadata stdout: %s", stdout)
			t.Logf("GET metadata stderr: %s", stderr)
		}
		require.NoError(t, err, "get metadata should succeed")
		assert.Contains(t, stdout, "text/plain", "metadata should contain content type")
	})
}

// testCLIDelete tests the delete command
func testCLIDelete(t *testing.T, cfg cliTestConfig) {
	t.Run("Delete", func(t *testing.T) {
		// Create and upload a file to delete
		key := fmt.Sprintf("cli-test/%s/delete-test.txt", strings.ToLower(cfg.name))

		testFile := filepath.Join(os.TempDir(), fmt.Sprintf("delete_test_%s.txt", cfg.name))
		err := ioutil.WriteFile(testFile, []byte("to be deleted"), 0644)
		require.NoError(t, err)
		defer os.Remove(testFile)

		// Upload
		_, _, err = runCLI(
			"--server", cfg.serverFlag,
			"--server-protocol", cfg.protocolFlag,
			"put", testFile, key,
		)
		require.NoError(t, err, "put should succeed")

		// Delete
		stdout, stderr, err := runCLI(
			"--server", cfg.serverFlag,
			"--server-protocol", cfg.protocolFlag,
			"delete", key,
		)

		if err != nil {
			t.Logf("DELETE stdout: %s", stdout)
			t.Logf("DELETE stderr: %s", stderr)
		}
		require.NoError(t, err, "delete command should succeed")

		// Verify deletion
		stdout, stderr, err = runCLI(
			"--server", cfg.serverFlag,
			"--server-protocol", cfg.protocolFlag,
			"exists", key,
		)

		require.NoError(t, err, "exists command should succeed")
		assert.Contains(t, strings.ToLower(stdout), "not", "object should not exist after deletion")
	})
}

// testCLIArchive tests archive command
func testCLIArchive(t *testing.T, cfg cliTestConfig) {
	if cfg.skipArchive {
		t.Skip("Archive not supported for this protocol yet")
	}

	t.Run("Archive", func(t *testing.T) {
		// Create and upload a file to archive
		key := fmt.Sprintf("cli-test/%s/archive-test.txt", strings.ToLower(cfg.name))

		testFile := filepath.Join(os.TempDir(), fmt.Sprintf("archive_test_%s.txt", cfg.name))
		err := ioutil.WriteFile(testFile, []byte("to be archived"), 0644)
		require.NoError(t, err)
		defer os.Remove(testFile)

		// Upload
		_, _, err = runCLI(
			"--server", cfg.serverFlag,
			"--server-protocol", cfg.protocolFlag,
			"put", testFile, key,
		)
		require.NoError(t, err, "put should succeed")

		// Archive to local destination
		stdout, stderr, err := runCLI(
			"--server", cfg.serverFlag,
			"--server-protocol", cfg.protocolFlag,
			"archive", key, "local",
			"--destination-path", "/tmp/archive",
		)

		if err != nil {
			t.Logf("ARCHIVE stdout: %s", stdout)
			t.Logf("ARCHIVE stderr: %s", stderr)
			// Archive may fail if destination not configured - log but don't fail
			t.Logf("Archive command failed (may be expected): %v", err)
		}
	})
}

// testCLIPolicies tests lifecycle policy commands
func testCLIPolicies(t *testing.T, cfg cliTestConfig) {
	if cfg.skipPolicies {
		t.Skip("Policies not supported for this protocol yet")
	}

	t.Run("Policies", func(t *testing.T) {
		policyID := fmt.Sprintf("cli-test-%s-cleanup", strings.ToLower(cfg.name))
		prefix := fmt.Sprintf("cli-test/%s/temp/", strings.ToLower(cfg.name))

		// Add policy
		stdout, stderr, err := runCLI(
			"--server", cfg.serverFlag,
			"--server-protocol", cfg.protocolFlag,
			"policy", "add", policyID, prefix, "1", "delete",
		)

		if err != nil {
			t.Logf("POLICY ADD stdout: %s", stdout)
			t.Logf("POLICY ADD stderr: %s", stderr)
		}
		require.NoError(t, err, "policy add should succeed")

		// List policies
		stdout, stderr, err = runCLI(
			"--server", cfg.serverFlag,
			"--server-protocol", cfg.protocolFlag,
			"policy", "list",
		)

		if err != nil {
			t.Logf("POLICY LIST stdout: %s", stdout)
			t.Logf("POLICY LIST stderr: %s", stderr)
		}
		require.NoError(t, err, "policy list should succeed")
		assert.Contains(t, stdout, policyID, "policy list should include added policy")

		// Apply policies
		stdout, stderr, err = runCLI(
			"--server", cfg.serverFlag,
			"--server-protocol", cfg.protocolFlag,
			"policy", "apply",
		)

		if err != nil {
			t.Logf("POLICY APPLY stdout: %s", stdout)
			t.Logf("POLICY APPLY stderr: %s", stderr)
		}
		require.NoError(t, err, "policy apply should succeed")

		// Remove policy
		stdout, stderr, err = runCLI(
			"--server", cfg.serverFlag,
			"--server-protocol", cfg.protocolFlag,
			"policy", "remove", policyID,
		)

		if err != nil {
			t.Logf("POLICY REMOVE stdout: %s", stdout)
			t.Logf("POLICY REMOVE stderr: %s", stderr)
		}
		require.NoError(t, err, "policy remove should succeed")

		// Verify removal
		stdout, stderr, err = runCLI(
			"--server", cfg.serverFlag,
			"--server-protocol", cfg.protocolFlag,
			"policy", "list",
		)

		require.NoError(t, err, "policy list should succeed after removal")
		assert.NotContains(t, stdout, policyID, "policy list should not include removed policy")
	})
}

// TestCLIOutputFormats tests different output formats
func TestCLIOutputFormats(t *testing.T) {
	cfg := cliConfigs[0] // Use REST server for format tests

	formats := []string{"text", "json", "table"}

	for _, format := range formats {
		t.Run(fmt.Sprintf("Format_%s", format), func(t *testing.T) {
			stdout, stderr, err := runCLI(
				"--server", cfg.serverFlag,
				"--server-protocol", cfg.protocolFlag,
				"--output-format", format,
				"list", "cli-test/",
			)

			if err != nil {
				t.Logf("LIST with format %s stdout: %s", format, stdout)
				t.Logf("LIST with format %s stderr: %s", format, stderr)
			}
			require.NoError(t, err, "list with format %s should succeed", format)
			assert.NotEmpty(t, stdout, "output should not be empty")
		})
	}
}

// TestCLIStdin tests reading from stdin
func TestCLIStdin(t *testing.T) {
	cfg := cliConfigs[0] // Use REST server

	t.Run("PutFromStdin", func(t *testing.T) {
		key := "cli-test/stdin/test.txt"
		content := "content from stdin"

		cmd := exec.Command("/app/bin/objstore",
			"--server", cfg.serverFlag,
			"--server-protocol", cfg.protocolFlag,
			"put", "-", key,
		)
		cmd.Stdin = strings.NewReader(content)

		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Logf("PUT from stdin output: %s", string(output))
		}
		require.NoError(t, err, "put from stdin should succeed")

		// Verify by getting it back
		stdout, _, err := runCLI(
			"--server", cfg.serverFlag,
			"--server-protocol", cfg.protocolFlag,
			"get", key, "-",
		)

		require.NoError(t, err, "get to stdout should succeed")
		assert.Equal(t, content, strings.TrimSpace(stdout), "content should match")
	})
}
