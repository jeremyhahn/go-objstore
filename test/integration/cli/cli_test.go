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

//go:build integration

package cli_test

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	// cliBinaryPath is the full path to the CLI binary
	cliBinaryPath string
	// testDir is the directory for test files
	testDir = "/tmp/cli-test"
	// testStorageDir is the directory for test storage backend
	testStorageDir = "/tmp/cli-test-storage"
)

// TestMain sets up and tears down the test environment
func TestMain(m *testing.M) {
	// Find project root by looking for go.mod
	projectRoot, err := findProjectRoot()
	if err != nil {
		log.Fatalf("Failed to find project root: %v", err)
	}

	// Determine binary path - check if it exists in PATH first, otherwise build it
	cliBinaryPath, err = findOrBuildCLI(projectRoot)
	if err != nil {
		log.Fatalf("Failed to set up CLI binary: %v", err)
	}

	log.Printf("Using CLI binary: %s", cliBinaryPath)

	// Create test directories
	os.RemoveAll(testDir)
	os.MkdirAll(testDir, 0755)
	os.RemoveAll(testStorageDir)
	os.MkdirAll(testStorageDir, 0755)

	// Run tests
	code := m.Run()

	// Cleanup
	os.RemoveAll(testDir)
	os.RemoveAll(testStorageDir)
	os.Exit(code)
}

// findProjectRoot walks up from the current directory to find go.mod
func findProjectRoot() (string, error) {
	// Start from test file directory
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("could not determine current file path")
	}

	dir := filepath.Dir(filename)
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("go.mod not found")
		}
		dir = parent
	}
}

// findOrBuildCLI finds the CLI binary in PATH or builds it
func findOrBuildCLI(projectRoot string) (string, error) {
	// First check if running in Docker (binary already built)
	if binaryPath, err := exec.LookPath("objstore"); err == nil {
		return binaryPath, nil
	}

	// Check for binary in project's bin directory
	binPath := filepath.Join(projectRoot, "bin", "objstore")
	if _, err := os.Stat(binPath); err == nil {
		return binPath, nil
	}

	// Build the binary
	log.Println("CLI binary not found, building...")

	// Create bin directory
	binDir := filepath.Join(projectRoot, "bin")
	if err := os.MkdirAll(binDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create bin directory: %w", err)
	}

	// Build with all backend tags
	cmd := exec.Command("go", "build",
		"-tags", "local awss3 minio gcpstorage azureblob glacier azurearchive",
		"-o", binPath,
		"./cmd/objstore",
	)
	cmd.Dir = projectRoot
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("failed to build CLI: %w", err)
	}

	log.Printf("Built CLI binary: %s", binPath)
	return binPath, nil
}

// runCLI executes the CLI with the given arguments and returns stdout, stderr, and error
// It automatically uses the test storage directory as the default backend path
func runCLI(args ...string) (string, string, error) {
	// Prepend default backend configuration if not already specified
	hasBackendPath := false
	for _, arg := range args {
		if arg == "--backend-path" || strings.HasPrefix(arg, "--backend-path=") {
			hasBackendPath = true
			break
		}
	}

	var fullArgs []string
	if !hasBackendPath {
		fullArgs = append(fullArgs, "--backend", "local", "--backend-path", testStorageDir)
	}
	fullArgs = append(fullArgs, args...)

	cmd := exec.Command(cliBinaryPath, fullArgs...)
	var stdout, stderr strings.Builder
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	return stdout.String(), stderr.String(), err
}

// runCLIWithEnv executes the CLI with custom environment variables
func runCLIWithEnv(env []string, args ...string) (string, string, error) {
	cmd := exec.Command(cliBinaryPath, args...)
	cmd.Env = append(os.Environ(), env...)
	var stdout, stderr strings.Builder
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	return stdout.String(), stderr.String(), err
}

// TestCLIPutCommand tests the put command with various scenarios
func TestCLIPutCommand(t *testing.T) {
	t.Run("put simple file", func(t *testing.T) {
		// Create test file
		testFile := filepath.Join(testDir, "test.txt")
		testContent := "Hello, World!"
		err := os.WriteFile(testFile, []byte(testContent), 0644)
		require.NoError(t, err)

		// Put the file
		stdout, stderr, err := runCLI("put", testFile, "test-key")
		require.NoError(t, err, "stderr: %s", stderr)
		assert.Contains(t, stdout, "Successfully uploaded")
		assert.Contains(t, stdout, "test-key")
	})

	t.Run("put with json output", func(t *testing.T) {
		testFile := filepath.Join(testDir, "test2.txt")
		err := os.WriteFile(testFile, []byte("test data"), 0644)
		require.NoError(t, err)

		stdout, stderr, err := runCLI("put", testFile, "test-key-2", "-o", "json")
		require.NoError(t, err, "stderr: %s", stderr)

		// Verify JSON output
		var result map[string]any
		err = json.Unmarshal([]byte(stdout), &result)
		require.NoError(t, err, "output should be valid JSON")
		assert.Equal(t, true, result["success"])
	})

	t.Run("put non-existent file", func(t *testing.T) {
		_, stderr, err := runCLI("put", "/non/existent/file.txt", "test-key-fail")
		assert.Error(t, err)
		assert.NotEmpty(t, stderr)
	})

	t.Run("put with custom backend path", func(t *testing.T) {
		customPath := filepath.Join(testDir, "custom-storage")
		os.MkdirAll(customPath, 0755)

		testFile := filepath.Join(testDir, "test3.txt")
		err := os.WriteFile(testFile, []byte("custom backend test"), 0644)
		require.NoError(t, err)

		// Use custom storage path - runCLI will not add default backend-path
		// since we're specifying --backend-path explicitly
		cmd := exec.Command(cliBinaryPath,
			"--backend", "local",
			"--backend-path", customPath,
			"put", testFile, "custom-key",
		)
		var stdout, stderr strings.Builder
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr
		err = cmd.Run()
		require.NoError(t, err, "stderr: %s", stderr.String())
		assert.Contains(t, stdout.String(), "Successfully uploaded")
	})
}

// TestCLIGetCommand tests the get command with various scenarios
func TestCLIGetCommand(t *testing.T) {
	// Setup: put a file first
	testFile := filepath.Join(testDir, "get-test.txt")
	testContent := "Content for get test"
	err := os.WriteFile(testFile, []byte(testContent), 0644)
	require.NoError(t, err)

	_, _, err = runCLI("put", testFile, "get-test-key")
	require.NoError(t, err)

	t.Run("get file to path", func(t *testing.T) {
		outputFile := filepath.Join(testDir, "output.txt")
		stdout, stderr, err := runCLI("get", "get-test-key", outputFile)
		require.NoError(t, err, "stderr: %s", stderr)
		assert.Contains(t, stdout, "Successfully downloaded")

		// Verify content
		content, err := os.ReadFile(outputFile)
		require.NoError(t, err)
		assert.Equal(t, testContent, string(content))
	})

	t.Run("get file to stdout", func(t *testing.T) {
		stdout, stderr, err := runCLI("get", "get-test-key", "-")
		require.NoError(t, err, "stderr: %s", stderr)
		assert.Equal(t, testContent, stdout)
	})

	t.Run("get file without output path", func(t *testing.T) {
		stdout, _, err := runCLI("get", "get-test-key")
		require.NoError(t, err)
		assert.Equal(t, testContent, stdout)
	})

	t.Run("get non-existent file", func(t *testing.T) {
		_, stderr, err := runCLI("get", "non-existent-key", filepath.Join(testDir, "fail.txt"))
		assert.Error(t, err)
		assert.NotEmpty(t, stderr)
	})

	t.Run("get with json output", func(t *testing.T) {
		outputFile := filepath.Join(testDir, "output-json.txt")
		stdout, stderr, err := runCLI("get", "get-test-key", outputFile, "-o", "json")
		require.NoError(t, err, "stderr: %s", stderr)

		var result map[string]any
		err = json.Unmarshal([]byte(stdout), &result)
		require.NoError(t, err)
		assert.Equal(t, true, result["success"])
	})
}

// TestCLIDeleteCommand tests the delete command
func TestCLIDeleteCommand(t *testing.T) {
	// Setup: put a file first
	testFile := filepath.Join(testDir, "delete-test.txt")
	err := os.WriteFile(testFile, []byte("to be deleted"), 0644)
	require.NoError(t, err)

	_, _, err = runCLI("put", testFile, "delete-test-key")
	require.NoError(t, err)

	t.Run("delete existing file", func(t *testing.T) {
		stdout, stderr, err := runCLI("delete", "delete-test-key")
		require.NoError(t, err, "stderr: %s", stderr)
		assert.Contains(t, stdout, "Successfully deleted")
		assert.Contains(t, stdout, "delete-test-key")

		// Verify deletion
		_, _, err = runCLI("exists", "delete-test-key")
		assert.Error(t, err, "exists should return error (exit 1) for deleted file")
	})

	t.Run("delete with json output", func(t *testing.T) {
		// Put file again
		_, _, err := runCLI("put", testFile, "delete-test-key-2")
		require.NoError(t, err)

		stdout, stderr, err := runCLI("delete", "delete-test-key-2", "-o", "json")
		require.NoError(t, err, "stderr: %s", stderr)

		var result map[string]any
		err = json.Unmarshal([]byte(stdout), &result)
		require.NoError(t, err)
		assert.Equal(t, true, result["success"])
	})

	t.Run("delete non-existent file", func(t *testing.T) {
		_, stderr, err := runCLI("delete", "non-existent-delete-key")
		assert.Error(t, err)
		assert.NotEmpty(t, stderr)
	})
}

// TestCLIListCommand tests the list command
func TestCLIListCommand(t *testing.T) {
	// Setup: put multiple files
	testFiles := map[string]string{
		"list-test/file1.txt":     "content1",
		"list-test/file2.txt":     "content2",
		"list-test/sub/file3.txt": "content3",
		"other/file4.txt":         "content4",
	}

	for key, content := range testFiles {
		testFile := filepath.Join(testDir, strings.ReplaceAll(key, "/", "-"))
		err := os.WriteFile(testFile, []byte(content), 0644)
		require.NoError(t, err)
		_, _, err = runCLI("put", testFile, key)
		require.NoError(t, err)
	}

	t.Run("list all files", func(t *testing.T) {
		stdout, stderr, err := runCLI("list")
		require.NoError(t, err, "stderr: %s", stderr)

		// Check that we can find our test files
		for key := range testFiles {
			assert.Contains(t, stdout, key)
		}
	})

	t.Run("list with prefix", func(t *testing.T) {
		stdout, stderr, err := runCLI("list", "list-test/")
		require.NoError(t, err, "stderr: %s", stderr)

		assert.Contains(t, stdout, "list-test/file1.txt")
		assert.Contains(t, stdout, "list-test/file2.txt")
		assert.Contains(t, stdout, "list-test/sub/file3.txt")
		assert.NotContains(t, stdout, "other/file4.txt")
	})

	t.Run("list with json output", func(t *testing.T) {
		stdout, stderr, err := runCLI("list", "list-test/", "-o", "json")
		require.NoError(t, err, "stderr: %s", stderr)

		// List command returns an object with "objects" array, not a bare array
		var result map[string]any
		err = json.Unmarshal([]byte(stdout), &result)
		require.NoError(t, err, "output should be valid JSON")

		objects, ok := result["objects"].([]any)
		if ok {
			assert.GreaterOrEqual(t, len(objects), 3)
		} else {
			// Fallback: try as direct array
			var arrResult []any
			err = json.Unmarshal([]byte(stdout), &arrResult)
			require.NoError(t, err, "output should be valid JSON")
			assert.GreaterOrEqual(t, len(arrResult), 3)
		}
	})

	t.Run("list with table output", func(t *testing.T) {
		stdout, stderr, err := runCLI("list", "-o", "table")
		require.NoError(t, err, "stderr: %s", stderr)

		// Table output should have headers (case-insensitive check)
		assert.Contains(t, stdout, "Key")
		assert.Contains(t, stdout, "Size")
	})
}

// TestCLIExistsCommand tests the exists command
func TestCLIExistsCommand(t *testing.T) {
	// Setup: put a file
	testFile := filepath.Join(testDir, "exists-test.txt")
	err := os.WriteFile(testFile, []byte("exists test"), 0644)
	require.NoError(t, err)

	_, _, err = runCLI("put", testFile, "exists-test-key")
	require.NoError(t, err)

	t.Run("exists for existing file", func(t *testing.T) {
		stdout, stderr, err := runCLI("exists", "exists-test-key")
		require.NoError(t, err, "stderr: %s", stderr)
		assert.Contains(t, stdout, "exists")
		assert.Contains(t, stdout, "true")
	})

	t.Run("exists for non-existent file", func(t *testing.T) {
		stdout, _, err := runCLI("exists", "non-existent-exists-key")
		assert.Error(t, err, "exists should return exit code 1 for non-existent files")
		assert.Contains(t, stdout, "false")
	})

	t.Run("exists with json output", func(t *testing.T) {
		stdout, stderr, err := runCLI("exists", "exists-test-key", "-o", "json")
		require.NoError(t, err, "stderr: %s", stderr)

		var result map[string]any
		err = json.Unmarshal([]byte(stdout), &result)
		require.NoError(t, err, "output should be valid JSON")

		// Check for exists field - it's nested in the "data" object
		if data, ok := result["data"].(map[string]any); ok {
			assert.Equal(t, true, data["exists"])
		} else if exists, ok := result["exists"]; ok {
			assert.Equal(t, true, exists)
		} else {
			t.Fatalf("Could not find exists field in JSON output: %v", result)
		}
	})
}

// TestCLIConfigCommand tests the config command
func TestCLIConfigCommand(t *testing.T) {
	t.Run("show default config", func(t *testing.T) {
		stdout, stderr, err := runCLI("config")
		require.NoError(t, err, "stderr: %s", stderr)

		// Config output uses title case (Backend, not backend)
		assert.Contains(t, stdout, "Backend")
		assert.Contains(t, stdout, "local")
	})

	t.Run("show config with json output", func(t *testing.T) {
		stdout, stderr, err := runCLI("config", "-o", "json")
		require.NoError(t, err, "stderr: %s", stderr)

		var result map[string]any
		err = json.Unmarshal([]byte(stdout), &result)
		require.NoError(t, err, "output should be valid JSON")
		assert.NotNil(t, result["backend"])
	})

	t.Run("config with custom backend", func(t *testing.T) {
		customPath := filepath.Join(testDir, "custom-config-path")
		os.MkdirAll(customPath, 0755)

		cmd := exec.Command(cliBinaryPath, "--backend", "local", "--backend-path", customPath, "config")
		var stdout, stderr strings.Builder
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr
		err := cmd.Run()
		require.NoError(t, err, "stderr: %s", stderr.String())
		assert.Contains(t, stdout.String(), customPath)
	})
}

// TestCLIConfigFile tests configuration from file
func TestCLIConfigFile(t *testing.T) {
	t.Run("config from yaml file", func(t *testing.T) {
		customStoragePath := filepath.Join(testDir, "config-file-storage")
		os.MkdirAll(customStoragePath, 0755)

		configFile := filepath.Join(testDir, "test-config.yaml")
		configContent := fmt.Sprintf(`backend: local
backend-path: %s
output-format: json
`, customStoragePath)
		err := os.WriteFile(configFile, []byte(configContent), 0644)
		require.NoError(t, err)

		cmd := exec.Command(cliBinaryPath, "--config", configFile, "config", "-o", "json")
		var stdout, stderr strings.Builder
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr
		err = cmd.Run()
		require.NoError(t, err, "stderr: %s", stderr.String())

		var result map[string]any
		err = json.Unmarshal([]byte(stdout.String()), &result)
		require.NoError(t, err)
		assert.Equal(t, "local", result["backend"])
	})
}

// TestCLIEnvVars tests configuration from environment variables
func TestCLIEnvVars(t *testing.T) {
	t.Run("config from environment variables", func(t *testing.T) {
		envStoragePath := filepath.Join(testDir, "env-storage")
		os.MkdirAll(envStoragePath, 0755)

		cmd := exec.Command(cliBinaryPath, "config", "-o", "json")
		cmd.Env = append(os.Environ(),
			"OBJECTSTORE_BACKEND=local",
			"OBJECTSTORE_BACKEND_PATH="+envStoragePath,
			"OBJECTSTORE_OUTPUT_FORMAT=json",
		)

		var stdout strings.Builder
		cmd.Stdout = &stdout
		err := cmd.Run()
		require.NoError(t, err)

		var result map[string]any
		err = json.Unmarshal([]byte(stdout.String()), &result)
		require.NoError(t, err)
		assert.Equal(t, "local", result["backend"])
	})
}

// TestCLIOutputFormats tests all output formats
func TestCLIOutputFormats(t *testing.T) {
	// Setup
	testFile := filepath.Join(testDir, "format-test.txt")
	err := os.WriteFile(testFile, []byte("format test"), 0644)
	require.NoError(t, err)

	_, _, err = runCLI("put", testFile, "format-test-key")
	require.NoError(t, err)

	formats := []string{"text", "json", "table"}

	for _, format := range formats {
		t.Run(fmt.Sprintf("list with %s format", format), func(t *testing.T) {
			stdout, stderr, err := runCLI("list", "-o", format)
			require.NoError(t, err, "stderr: %s", stderr)
			assert.NotEmpty(t, stdout)

			if format == "json" {
				// Verify it's valid JSON
				var result any
				err = json.Unmarshal([]byte(stdout), &result)
				assert.NoError(t, err, "output should be valid JSON")
			}
		})
	}
}

// TestCLIErrorScenarios tests various error scenarios
func TestCLIErrorScenarios(t *testing.T) {
	t.Run("invalid command", func(t *testing.T) {
		_, stderr, err := runCLI("invalid-command")
		assert.Error(t, err)
		assert.NotEmpty(t, stderr)
	})

	t.Run("missing required argument", func(t *testing.T) {
		_, stderr, err := runCLI("put")
		assert.Error(t, err)
		assert.NotEmpty(t, stderr)
	})

	t.Run("invalid backend", func(t *testing.T) {
		_, stderr, err := runCLI("--backend", "invalid-backend", "list")
		assert.Error(t, err)
		assert.NotEmpty(t, stderr)
	})

	t.Run("invalid output format", func(t *testing.T) {
		_, stderr, err := runCLI("list", "-o", "invalid-format")
		assert.Error(t, err)
		assert.NotEmpty(t, stderr)
	})

	t.Run("empty key", func(t *testing.T) {
		testFile := filepath.Join(testDir, "empty-key-test.txt")
		err := os.WriteFile(testFile, []byte("test"), 0644)
		require.NoError(t, err)

		_, stderr, err := runCLI("put", testFile, "")
		assert.Error(t, err)
		assert.NotEmpty(t, stderr)
	})
}

// TestCLIPriority tests flag/env/file priority
func TestCLIPriority(t *testing.T) {
	t.Run("flags override env vars", func(t *testing.T) {
		flagPath := filepath.Join(testDir, "flag-path")
		envPath := filepath.Join(testDir, "env-path")
		os.MkdirAll(flagPath, 0755)
		os.MkdirAll(envPath, 0755)

		cmd := exec.Command(cliBinaryPath, "--backend-path", flagPath, "config", "-o", "json")
		cmd.Env = append(os.Environ(),
			"OBJECTSTORE_BACKEND_PATH="+envPath,
		)

		var stdout strings.Builder
		cmd.Stdout = &stdout
		err := cmd.Run()
		require.NoError(t, err)

		output := stdout.String()
		assert.Contains(t, output, flagPath)
		assert.NotContains(t, output, envPath)
	})
}

// TestCLIConcurrentOperations tests concurrent CLI operations
func TestCLIConcurrentOperations(t *testing.T) {
	t.Run("concurrent puts", func(t *testing.T) {
		numOperations := 10
		errChan := make(chan error, numOperations)

		for i := 0; i < numOperations; i++ {
			go func(index int) {
				testFile := filepath.Join(testDir, fmt.Sprintf("concurrent-%d.txt", index))
				err := os.WriteFile(testFile, []byte(fmt.Sprintf("content-%d", index)), 0644)
				if err != nil {
					errChan <- err
					return
				}

				key := fmt.Sprintf("concurrent-key-%d", index)
				_, _, err = runCLI("put", testFile, key)
				errChan <- err
			}(i)
		}

		// Collect errors
		for i := 0; i < numOperations; i++ {
			err := <-errChan
			assert.NoError(t, err)
		}
	})
}

// TestCLILargeFile tests handling of large files
func TestCLILargeFile(t *testing.T) {
	t.Run("put and get large file", func(t *testing.T) {
		// Create a 10MB file
		largeFile := filepath.Join(testDir, "large-file.bin")
		size := 10 * 1024 * 1024 // 10MB

		f, err := os.Create(largeFile)
		require.NoError(t, err)
		defer f.Close()

		// Write random-ish data
		chunk := make([]byte, 1024)
		for i := 0; i < size/1024; i++ {
			for j := range chunk {
				chunk[j] = byte(i % 256)
			}
			_, err = f.Write(chunk)
			require.NoError(t, err)
		}
		f.Close()

		// Put the file
		stdout, stderr, err := runCLI("put", largeFile, "large-file-key")
		require.NoError(t, err, "stderr: %s", stderr)
		assert.Contains(t, stdout, "Successfully uploaded")

		// Get the file back
		outputFile := filepath.Join(testDir, "large-file-output.bin")
		stdout, stderr, err = runCLI("get", "large-file-key", outputFile)
		require.NoError(t, err, "stderr: %s", stderr)

		// Verify size
		info, err := os.Stat(outputFile)
		require.NoError(t, err)
		assert.Equal(t, int64(size), info.Size())
	})
}
