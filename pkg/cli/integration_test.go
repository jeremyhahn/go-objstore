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

//go:build integration || local

package cli

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jeremyhahn/go-objstore/pkg/version"
)

// testCLI wraps the CLI binary for integration testing
type testCLI struct {
	binaryPath string
	configPath string
	backendDir string
	t          *testing.T
}

// setupCLI builds the CLI binary and creates a test configuration
func setupCLI(t *testing.T) *testCLI {
	// Create temp directory for test storage
	backendDir := t.TempDir()

	// Create config file
	configDir := t.TempDir()
	configPath := filepath.Join(configDir, "config.yaml")
	configContent := `backend: local
backend_path: ` + backendDir + `
output_format: json
`
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("Failed to create config file: %v", err)
	}

	// Build the CLI binary
	binaryDir := t.TempDir()
	binaryPath := filepath.Join(binaryDir, "objstore")

	// Build command
	cmd := exec.Command("go", "build", "-tags", "local", "-o", binaryPath, "../../cmd/objstore")
	cmd.Env = os.Environ()

	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Failed to build CLI binary: %v\nOutput: %s", err, output)
	}

	return &testCLI{
		binaryPath: binaryPath,
		configPath: configPath,
		backendDir: backendDir,
		t:          t,
	}
}

// run executes the CLI with the given arguments
func (tc *testCLI) run(args ...string) (string, string, error) {
	// Always use JSON output for easier parsing in tests
	fullArgs := append([]string{"--config", tc.configPath, "-o", "json"}, args...)
	cmd := exec.Command(tc.binaryPath, fullArgs...)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	return stdout.String(), stderr.String(), err
}

// runWithStdin executes the CLI with stdin input
func (tc *testCLI) runWithStdin(input string, args ...string) (string, string, error) {
	// Always use JSON output for easier parsing in tests
	fullArgs := append([]string{"--config", tc.configPath, "-o", "json"}, args...)
	cmd := exec.Command(tc.binaryPath, fullArgs...)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	cmd.Stdin = strings.NewReader(input)

	err := cmd.Run()
	return stdout.String(), stderr.String(), err
}

// TestCLI_HealthCommand tests the health command
func TestCLI_HealthCommand(t *testing.T) {
	cli := setupCLI(t)

	stdout, stderr, err := cli.run("health")
	if err != nil {
		t.Fatalf("health command failed: %v\nStderr: %s", err, stderr)
	}

	// Parse JSON output
	var result map[string]interface{}
	if err := json.Unmarshal([]byte(stdout), &result); err != nil {
		t.Fatalf("Failed to parse health output: %v\nStdout: %s", err, stdout)
	}

	// Verify status
	if status, ok := result["status"].(string); !ok || status != "healthy" {
		t.Errorf("Expected status 'healthy', got %v", result["status"])
	}

	// Verify version matches VERSION file
	if versionStr, ok := result["version"].(string); !ok || versionStr != version.Get() {
		t.Errorf("Expected version %q, got %v", version.Get(), result["version"])
	}

	// Verify backend
	if backend, ok := result["backend"].(string); !ok || backend != "local" {
		t.Errorf("Expected backend 'local', got %v", result["backend"])
	}
}

// TestCLI_ConfigCommand tests the config command
func TestCLI_ConfigCommand(t *testing.T) {
	cli := setupCLI(t)

	stdout, stderr, err := cli.run("config")
	if err != nil {
		t.Fatalf("config command failed: %v\nStderr: %s", err, stderr)
	}

	// Verify output contains expected config fields
	if !strings.Contains(stdout, "backend") {
		t.Error("Expected 'backend' in config output")
	}
	if !strings.Contains(stdout, "local") {
		t.Error("Expected 'local' in config output")
	}
}

// TestCLI_PutCommand tests the put command
func TestCLI_PutCommand(t *testing.T) {
	cli := setupCLI(t)

	// Create a test file
	testFile := filepath.Join(cli.backendDir, "test-input.txt")
	testContent := "Hello, World! This is test content."
	if err := os.WriteFile(testFile, []byte(testContent), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Put the file (put <source-file> <destination-key>)
	stdout, stderr, err := cli.run("put", testFile, "test/hello.txt")
	if err != nil {
		t.Fatalf("put command failed: %v\nStderr: %s\nStdout: %s", err, stderr, stdout)
	}

	// Parse JSON output
	var result map[string]interface{}
	if err := json.Unmarshal([]byte(stdout), &result); err != nil {
		t.Fatalf("Failed to parse put output: %v\nStdout: %s", err, stdout)
	}

	// Verify success
	if success, ok := result["success"].(bool); !ok || !success {
		t.Errorf("Expected success true, got %v", result["success"])
	}
}

// TestCLI_PutFromStdin tests putting data from stdin
func TestCLI_PutFromStdin(t *testing.T) {
	cli := setupCLI(t)

	testContent := "Data from stdin"
	stdout, stderr, err := cli.runWithStdin(testContent, "put", "-", "test/stdin.txt")
	if err != nil {
		t.Fatalf("put from stdin failed: %v\nStderr: %s\nStdout: %s", err, stderr, stdout)
	}

	// Verify the file was created
	// exists command exits with 0 if object exists, 1 if not
	stdout, _, err = cli.run("exists", "test/stdin.txt")
	if err != nil {
		t.Error("Expected object to exist after put from stdin (exists returned non-zero exit code)")
	}

	var result map[string]interface{}
	if err := json.Unmarshal([]byte(stdout), &result); err != nil {
		t.Fatalf("Failed to parse exists output: %v", err)
	}

	data, ok := result["data"].(map[string]interface{})
	if !ok {
		t.Fatalf("Expected data field in result, got %v", result)
	}
	if exists, ok := data["exists"].(bool); !ok || !exists {
		t.Error("Expected exists field to be true")
	}
}

// TestCLI_GetCommand tests the get command
func TestCLI_GetCommand(t *testing.T) {
	cli := setupCLI(t)

	// First put a file
	testFile := filepath.Join(cli.backendDir, "test-input-get.txt")
	testContent := "Content to retrieve"
	if err := os.WriteFile(testFile, []byte(testContent), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	_, _, err := cli.run("put", testFile, "test/retrieve.txt")
	if err != nil {
		t.Fatalf("put command failed: %v", err)
	}

	// Get the file
	outputFile := filepath.Join(cli.backendDir, "output.txt")
	stdout, stderr, err := cli.run("get", "test/retrieve.txt", outputFile)
	if err != nil {
		t.Fatalf("get command failed: %v\nStderr: %s\nStdout: %s", err, stderr, stdout)
	}

	// Verify the output file content
	retrievedContent, err := os.ReadFile(outputFile)
	if err != nil {
		t.Fatalf("Failed to read output file: %v", err)
	}

	if string(retrievedContent) != testContent {
		t.Errorf("Content mismatch: got %q, want %q", string(retrievedContent), testContent)
	}
}

// TestCLI_ExistsCommand tests the exists command
func TestCLI_ExistsCommand(t *testing.T) {
	cli := setupCLI(t)

	// Test non-existent object
	// exists command exits with 0 if object exists, 1 if not
	stdout, _, err := cli.run("exists", "test/nonexistent.txt")
	if err == nil {
		t.Error("Expected non-zero exit code for non-existent object")
	}

	var result map[string]interface{}
	if err := json.Unmarshal([]byte(stdout), &result); err != nil {
		t.Fatalf("Failed to parse exists output: %v", err)
	}

	data, ok := result["data"].(map[string]interface{})
	if !ok {
		t.Fatalf("Expected data field in result, got %v", result)
	}
	if exists, ok := data["exists"].(bool); !ok || exists {
		t.Error("Expected exists field to be false")
	}

	// Put an object
	testFile := filepath.Join(cli.backendDir, "test-exists.txt")
	if err := os.WriteFile(testFile, []byte("exists test"), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	_, _, err = cli.run("put", testFile, "test/exists.txt")
	if err != nil {
		t.Fatalf("put command failed: %v", err)
	}

	// Test existing object - should return exit 0
	stdout, _, err = cli.run("exists", "test/exists.txt")
	if err != nil {
		t.Error("Expected zero exit code for existing object")
	}

	if err := json.Unmarshal([]byte(stdout), &result); err != nil {
		t.Fatalf("Failed to parse exists output: %v", err)
	}

	data, ok = result["data"].(map[string]interface{})
	if !ok {
		t.Fatalf("Expected data field in result, got %v", result)
	}
	if exists, ok := data["exists"].(bool); !ok || !exists {
		t.Error("Expected exists field to be true")
	}
}

// TestCLI_ListCommand tests the list command
func TestCLI_ListCommand(t *testing.T) {
	cli := setupCLI(t)

	// Put some test files
	for i, name := range []string{"file1.txt", "file2.txt", "file3.txt"} {
		testFile := filepath.Join(cli.backendDir, name)
		if err := os.WriteFile(testFile, []byte("content "+string(rune('1'+i))), 0644); err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}

		_, _, err := cli.run("put", testFile, "test/list/"+name)
		if err != nil {
			t.Fatalf("put command failed: %v", err)
		}
	}

	// List with prefix
	stdout, stderr, err := cli.run("list", "test/list/")
	if err != nil {
		t.Fatalf("list command failed: %v\nStderr: %s\nStdout: %s", err, stderr, stdout)
	}

	// Parse JSON output
	var result map[string]interface{}
	if err := json.Unmarshal([]byte(stdout), &result); err != nil {
		t.Fatalf("Failed to parse list output: %v\nStdout: %s", err, stdout)
	}

	// Verify we got objects back
	objects, ok := result["objects"].([]interface{})
	if !ok {
		t.Fatalf("Expected objects array in result, got %T", result["objects"])
	}

	if len(objects) != 3 {
		t.Errorf("Expected 3 objects, got %d", len(objects))
	}
}

// TestCLI_DeleteCommand tests the delete command
func TestCLI_DeleteCommand(t *testing.T) {
	cli := setupCLI(t)

	// Put a file
	testFile := filepath.Join(cli.backendDir, "test-delete.txt")
	if err := os.WriteFile(testFile, []byte("delete me"), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	_, _, err := cli.run("put", testFile, "test/delete.txt")
	if err != nil {
		t.Fatalf("put command failed: %v", err)
	}

	// Verify it exists - should return exit 0
	stdout, _, err := cli.run("exists", "test/delete.txt")
	if err != nil {
		t.Fatal("Object should exist before deletion (exists returned non-zero exit code)")
	}

	var existsResult map[string]interface{}
	if err := json.Unmarshal([]byte(stdout), &existsResult); err != nil {
		t.Fatalf("Failed to parse exists output: %v", err)
	}
	data, ok := existsResult["data"].(map[string]interface{})
	if !ok {
		t.Fatalf("Expected data field in result, got %v", existsResult)
	}
	if exists, ok := data["exists"].(bool); !ok || !exists {
		t.Fatal("Object should exist before deletion")
	}

	// Delete the file
	stdout, stderr, err := cli.run("delete", "test/delete.txt")
	if err != nil {
		t.Fatalf("delete command failed: %v\nStderr: %s\nStdout: %s", err, stderr, stdout)
	}

	// Verify success
	var deleteResult map[string]interface{}
	if err := json.Unmarshal([]byte(stdout), &deleteResult); err != nil {
		t.Fatalf("Failed to parse delete output: %v", err)
	}

	if success, ok := deleteResult["success"].(bool); !ok || !success {
		t.Error("Expected delete to succeed")
	}

	// Verify it no longer exists - should return exit 1
	stdout, _, err = cli.run("exists", "test/delete.txt")
	if err == nil {
		t.Error("Object should not exist after deletion (exists returned zero exit code)")
	}

	if err := json.Unmarshal([]byte(stdout), &existsResult); err != nil {
		t.Fatalf("Failed to parse exists output: %v", err)
	}
	data, ok = existsResult["data"].(map[string]interface{})
	if !ok {
		t.Fatalf("Expected data field in result, got %v", existsResult)
	}
	if exists, ok := data["exists"].(bool); !ok || exists {
		t.Error("Expected exists field to be false after deletion")
	}
}

// TestCLI_GetMetadataCommand tests the get --metadata flag
func TestCLI_GetMetadataCommand(t *testing.T) {
	cli := setupCLI(t)

	// Put a file
	testFile := filepath.Join(cli.backendDir, "test-metadata.txt")
	testContent := "metadata test content"
	if err := os.WriteFile(testFile, []byte(testContent), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	_, _, err := cli.run("put", testFile, "test/metadata.txt")
	if err != nil {
		t.Fatalf("put command failed: %v", err)
	}

	// Get metadata using --metadata flag
	stdout, stderr, err := cli.run("get", "test/metadata.txt", "--metadata")
	if err != nil {
		t.Fatalf("get --metadata command failed: %v\nStderr: %s\nStdout: %s", err, stderr, stdout)
	}

	// Parse JSON output
	var result map[string]interface{}
	if err := json.Unmarshal([]byte(stdout), &result); err != nil {
		t.Fatalf("Failed to parse metadata output: %v\nStdout: %s", err, stdout)
	}

	// Verify size is present
	if size, ok := result["size"].(float64); !ok || size != float64(len(testContent)) {
		t.Errorf("Expected size %d, got %v", len(testContent), result["size"])
	}
}

// TestCLI_PutWithMetadataCommand tests putting a file with metadata flags
func TestCLI_PutWithMetadataCommand(t *testing.T) {
	cli := setupCLI(t)

	// Put a file with metadata
	testFile := filepath.Join(cli.backendDir, "test-with-metadata.txt")
	if err := os.WriteFile(testFile, []byte("file with metadata"), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Put with metadata flags
	stdout, stderr, err := cli.run(
		"put", testFile, "test/with-metadata.txt",
		"--content-type", "application/json",
		"--content-encoding", "gzip",
		"--custom", "author=testuser,version=1.0.0",
	)
	if err != nil {
		t.Fatalf("put with metadata failed: %v\nStderr: %s\nStdout: %s", err, stderr, stdout)
	}

	// Verify success
	var putResult map[string]interface{}
	if err := json.Unmarshal([]byte(stdout), &putResult); err != nil {
		t.Fatalf("Failed to parse put output: %v", err)
	}

	if success, ok := putResult["success"].(bool); !ok || !success {
		t.Error("Expected put with metadata to succeed")
	}

	// Verify metadata was stored
	stdout, _, err = cli.run("get", "test/with-metadata.txt", "--metadata")
	if err != nil {
		t.Fatalf("get --metadata command failed: %v", err)
	}

	var metadata map[string]interface{}
	if err := json.Unmarshal([]byte(stdout), &metadata); err != nil {
		t.Fatalf("Failed to parse metadata: %v", err)
	}

	if contentType, ok := metadata["content_type"].(string); !ok || contentType != "application/json" {
		t.Errorf("Expected content_type 'application/json', got %v", metadata["content_type"])
	}

	if encoding, ok := metadata["content_encoding"].(string); !ok || encoding != "gzip" {
		t.Errorf("Expected content_encoding 'gzip', got %v", metadata["content_encoding"])
	}
}

// TestCLI_ArchiveCommand tests the archive command
func TestCLI_ArchiveCommand(t *testing.T) {
	t.Skip("Archive command requires archiver backend configuration")
	cli := setupCLI(t)

	// Create a second backend for archiving
	archiveDir := t.TempDir()

	// Put a file
	testFile := filepath.Join(cli.backendDir, "test-archive.txt")
	if err := os.WriteFile(testFile, []byte("archive me"), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	_, _, err := cli.run("put", testFile, "test/archive.txt")
	if err != nil {
		t.Fatalf("put command failed: %v", err)
	}

	// Archive the file
	stdout, stderr, err := cli.run(
		"archive", "test/archive.txt",
		"--destination-type", "local",
		"--destination-path", archiveDir,
	)
	if err != nil {
		t.Fatalf("archive command failed: %v\nStderr: %s\nStdout: %s", err, stderr, stdout)
	}

	// Verify success
	var result map[string]interface{}
	if err := json.Unmarshal([]byte(stdout), &result); err != nil {
		t.Fatalf("Failed to parse archive output: %v", err)
	}

	if success, ok := result["success"].(bool); !ok || !success {
		t.Error("Expected archive to succeed")
	}
}

// TestCLI_PolicyCommands tests the policy add/list/remove commands
func TestCLI_PolicyCommands(t *testing.T) {
	t.Skip("Policy commands require backend with lifecycle policy support")
	cli := setupCLI(t)

	// Add a policy (uses positional arguments: id prefix retention-days action)
	stdout, stderr, err := cli.run(
		"policy", "add",
		"test-policy",
		"test/old/",
		"7", // retention in days
		"delete",
	)
	if err != nil {
		t.Fatalf("policy add command failed: %v\nStderr: %s\nStdout: %s", err, stderr, stdout)
	}

	// Verify success
	var addResult map[string]interface{}
	if err := json.Unmarshal([]byte(stdout), &addResult); err != nil {
		t.Fatalf("Failed to parse policy add output: %v", err)
	}

	if success, ok := addResult["success"].(bool); !ok || !success {
		t.Error("Expected policy add to succeed")
	}

	// List policies
	stdout, stderr, err = cli.run("policy", "list")
	if err != nil {
		t.Fatalf("policy list command failed: %v\nStderr: %s\nStdout: %s", err, stderr, stdout)
	}

	// Verify policy is in the list
	var listResult map[string]interface{}
	if err := json.Unmarshal([]byte(stdout), &listResult); err != nil {
		t.Fatalf("Failed to parse policy list output: %v", err)
	}

	policies, ok := listResult["policies"].([]interface{})
	if !ok {
		t.Fatalf("Expected policies array, got %T", listResult["policies"])
	}

	found := false
	for _, p := range policies {
		policy := p.(map[string]interface{})
		if id, ok := policy["id"].(string); ok && id == "test-policy" {
			found = true
			break
		}
	}

	if !found {
		t.Error("Expected to find test-policy in policy list")
	}

	// Remove the policy
	stdout, stderr, err = cli.run("policy", "remove", "test-policy")
	if err != nil {
		t.Fatalf("policy remove command failed: %v\nStderr: %s\nStdout: %s", err, stderr, stdout)
	}

	// Verify success
	var removeResult map[string]interface{}
	if err := json.Unmarshal([]byte(stdout), &removeResult); err != nil {
		t.Fatalf("Failed to parse policy remove output: %v", err)
	}

	if success, ok := removeResult["success"].(bool); !ok || !success {
		t.Error("Expected policy remove to succeed")
	}
}

// TestCLI_VersionInHealthOutput tests that version is correctly reported
func TestCLI_VersionInHealthOutput(t *testing.T) {
	cli := setupCLI(t)

	stdout, stderr, err := cli.run("health")
	if err != nil {
		t.Fatalf("health command failed: %v\nStderr: %s", err, stderr)
	}

	var result map[string]interface{}
	if err := json.Unmarshal([]byte(stdout), &result); err != nil {
		t.Fatalf("Failed to parse health output: %v", err)
	}

	// Verify version matches VERSION file
	expectedVersion := version.Get()
	if versionStr, ok := result["version"].(string); !ok || versionStr != expectedVersion {
		t.Errorf("Version mismatch: got %q, want %q (from VERSION file)", versionStr, expectedVersion)
	}

	// Also verify it's not the old hardcoded "1.0.0"
	if versionStr, ok := result["version"].(string); ok && versionStr == "1.0.0" {
		t.Error("CLI is still using hardcoded version '1.0.0' instead of VERSION file")
	}
}

// TestCLI_AllCommandsBasicSmokeTest ensures all commands are accessible
func TestCLI_AllCommandsBasicSmokeTest(t *testing.T) {
	cli := setupCLI(t)

	tests := []struct {
		name        string
		args        []string
		expectError bool
	}{
		{"health", []string{"health"}, false},
		{"config", []string{"config"}, false},
		{"list empty", []string{"list", ""}, false},
		// Note: exists command returns exit code 1 when object doesn't exist
		// This is by design, so we ignore the error for this test
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, stderr, err := cli.run(tt.args...)
			if tt.expectError && err == nil {
				t.Errorf("Expected error for %v", tt.args)
			}
			if !tt.expectError && err != nil {
				t.Errorf("Unexpected error for %v: %v\nStderr: %s", tt.args, err, stderr)
			}
		})
	}
}
