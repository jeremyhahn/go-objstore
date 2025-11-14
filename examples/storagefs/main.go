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
	"fmt"
	"io"
	"log"

	"github.com/jeremyhahn/go-objstore/pkg/factory"
	"github.com/jeremyhahn/go-objstore/pkg/storagefs"
)

// Example demonstrating how to use the StorageFS filesystem abstraction
// with different storage backends (Local, S3, Azure, GCS)

func main() {
	// Example 1: Using local storage with filesystem interface
	fmt.Println("=== Example 1: Local Storage ===")
	localExample()

	// Example 2: Using S3 storage with filesystem interface
	fmt.Println("\n=== Example 2: S3 Storage ===")
	s3Example()

	// Example 3: Using filesystem utilities
	fmt.Println("\n=== Example 3: Filesystem Utilities ===")
	utilsExample()
}

func localExample() {
	// Create local storage backend
	storage, err := factory.NewStorage("local", map[string]string{
		"basePath": "/tmp/objstore-local",
	})
	if err != nil {
		log.Fatal(err)
	}

	// Wrap storage backend with filesystem interface
	fs := storagefs.New(storage)

	// Now you can use standard filesystem operations
	// Create a directory
	if err := fs.MkdirAll("docs/examples", 0755); err != nil {
		log.Fatal(err)
	}

	// Create a file
	file, err := fs.Create("docs/examples/readme.txt")
	if err != nil {
		log.Fatal(err)
	}

	// Write to the file
	if _, err := file.WriteString("Hello from StorageFS!\n"); err != nil {
		log.Fatal(err)
	}
	if err := file.Close(); err != nil {
		log.Fatal(err)
	}

	// Read the file back
	readFile, err := fs.Open("docs/examples/readme.txt")
	if err != nil {
		log.Fatal(err)
	}
	content, err := io.ReadAll(readFile)
	if err != nil {
		log.Fatal(err)
	}
	readFile.Close()
	fmt.Printf("File content: %s", content)

	// Get file stats
	info, err := fs.Stat("docs/examples/readme.txt")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("File size: %d bytes\n", info.Size())
	fmt.Printf("Modified: %v\n", info.ModTime())

	// Clean up
	if err := fs.RemoveAll("docs"); err != nil {
		log.Fatal(err)
	}
}

func s3Example() {
	// Create S3 storage backend (requires AWS credentials)
	// This example assumes AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are set
	storage, err := factory.NewStorage("s3", map[string]string{
		"bucket": "my-objstore-bucket",
		"region": "us-east-1",
	})
	if err != nil {
		log.Printf("Skipping S3 example: %v", err)
		return
	}

	// Wrap with filesystem interface
	fs := storagefs.New(storage)

	// Use the same filesystem operations as local storage
	if err := fs.MkdirAll("data/reports", 0755); err != nil {
		log.Fatal(err)
	}

	// Create and write to a file
	file, err := fs.Create("data/reports/summary.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	if _, err := file.WriteString("Q4 2024 Summary\n"); err != nil {
		log.Fatal(err)
	}

	fmt.Println("Successfully wrote to S3 using filesystem interface")
}

func utilsExample() {
	// Create a mock storage for demonstration
	storage, _ := factory.NewStorage("local", map[string]string{
		"basePath": "/tmp/objstore-utils",
	})
	fs := storagefs.New(storage)

	// Example 1: Write and read files
	file, _ := fs.Create("test.txt")
	file.WriteString("This is a test file\n")
	file.Close()

	// Read the file
	readFile, _ := fs.Open("test.txt")
	content, _ := io.ReadAll(readFile)
	readFile.Close()
	fmt.Printf("Read content: %s", content)

	// Example 2: Check if file exists
	if _, err := fs.Stat("test.txt"); err == nil {
		fmt.Println("File exists: true")
	}

	// Example 3: Create directories
	if err := fs.MkdirAll("mydir/subdir", 0755); err != nil {
		log.Fatal(err)
	}

	// Check if directory exists
	if info, err := fs.Stat("mydir"); err == nil && info.IsDir() {
		fmt.Println("Directory exists: true")
	}

	// Example 4: Copy files between filesystems
	srcFs := storagefs.New(storage)
	dstStorage, _ := factory.NewStorage("local", map[string]string{
		"basePath": "/tmp/objstore-dest",
	})
	dstFs := storagefs.New(dstStorage)

	// Copy a file using standard io operations
	srcFile, err := srcFs.Open("test.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer srcFile.Close()

	dstFile, err := dstFs.Create("test-copy.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer dstFile.Close()

	if _, err := io.Copy(dstFile, srcFile); err != nil {
		log.Fatal(err)
	}

	fmt.Println("Successfully copied file between storage backends")

	// Clean up
	fs.RemoveAll("")
	dstFs.RemoveAll("")
}
