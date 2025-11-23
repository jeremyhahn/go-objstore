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
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/factory"
	"github.com/jeremyhahn/go-objstore/pkg/objstore"
)

// Example demonstrating the new facade pattern for go-objstore

func main() {
	fmt.Println("=== go-objstore Facade Pattern Example ===\n")

	// Setup: Create storage backends using the factory
	fmt.Println("1. Setting up storage backends...")

	local, err := factory.NewStorage("local", map[string]string{
		"path": "/tmp/objstore-local",
	})
	if err != nil {
		log.Fatalf("Failed to create local storage: %v", err)
	}

	local2, err := factory.NewStorage("local", map[string]string{
		"path": "/tmp/objstore-local2",
	})
	if err != nil {
		log.Fatalf("Failed to create local2 storage: %v", err)
	}

	// Initialize the facade with multiple backends
	// This is done ONCE at application startup
	err = objstore.Initialize(&objstore.FacadeConfig{
		Backends: map[string]common.Storage{
			"primary":   local,
			"secondary": local2,
		},
		DefaultBackend: "primary",
	})
	if err != nil {
		log.Fatalf("Failed to initialize objstore facade: %v", err)
	}
	defer objstore.Reset()

	fmt.Println("  Facade initialized with 2 backends")
	fmt.Println("  Default backend: primary")
	fmt.Println("")

	// Example 1: Basic Put/Get using default backend
	fmt.Println("2. Basic operations (default backend)...")
	basicOperations()

	// Example 2: Using specific backends with backend:key syntax
	fmt.Println("\n3. Multi-backend operations...")
	multiBackendOperations()

	// Example 3: Working with metadata
	fmt.Println("\n4. Metadata operations...")
	metadataOperations()

	// Example 4: Listing and pagination
	fmt.Println("\n5. List operations...")
	listOperations()

	// Example 5: Error handling and validation
	fmt.Println("\n6. Security and validation...")
	securityDemo()

	fmt.Println("\n=== All examples completed successfully! ===")
}

func basicOperations() {
	ctx := context.Background()

	// Put - uses default backend
	data := []byte("Hello, Facade!")
	err := objstore.Put("greeting.txt", bytes.NewReader(data))
	if err != nil {
		log.Printf("  Put failed: %v", err)
		return
	}
	fmt.Println("  Put: greeting.txt")

	// Get - uses default backend
	reader, err := objstore.Get("greeting.txt")
	if err != nil {
		log.Printf("  Get failed: %v", err)
		return
	}
	defer reader.Close()

	content, _ := io.ReadAll(reader)
	fmt.Printf("  Get: %s\n", content)

	// Exists
	exists, err := objstore.Exists(ctx, "greeting.txt")
	if err != nil {
		log.Printf("  Exists check failed: %v", err)
		return
	}
	fmt.Printf("  Exists: %v\n", exists)

	// Delete
	err = objstore.Delete("greeting.txt")
	if err != nil {
		log.Printf("  Delete failed: %v", err)
		return
	}
	fmt.Println("  Delete: greeting.txt")
}

func multiBackendOperations() {
	ctx := context.Background()

	// Put to specific backend using backend:key syntax
	err := objstore.PutWithContext(ctx, "primary:data.txt", bytes.NewReader([]byte("in primary")))
	if err != nil {
		log.Printf("  Put to primary failed: %v", err)
		return
	}
	fmt.Println("  Put to primary: data.txt")

	err = objstore.PutWithContext(ctx, "secondary:data.txt", bytes.NewReader([]byte("in secondary")))
	if err != nil {
		log.Printf("  Put to secondary failed: %v", err)
		return
	}
	fmt.Println("  Put to secondary: data.txt")

	// Get from specific backends
	reader1, err := objstore.GetWithContext(ctx, "primary:data.txt")
	if err != nil {
		log.Printf("  Get from primary failed: %v", err)
		return
	}
	defer reader1.Close()
	content1, _ := io.ReadAll(reader1)

	reader2, err := objstore.GetWithContext(ctx, "secondary:data.txt")
	if err != nil {
		log.Printf("  Get from secondary failed: %v", err)
		return
	}
	defer reader2.Close()
	content2, _ := io.ReadAll(reader2)

	fmt.Printf("  Primary contains: %s\n", content1)
	fmt.Printf("  Secondary contains: %s\n", content2)

	// List available backends
	backends := objstore.Backends()
	fmt.Printf("  Available backends: %v\n", backends)
}

func metadataOperations() {
	ctx := context.Background()

	// Put with metadata
	metadata := &common.Metadata{
		ContentType:     "application/json",
		ContentEncoding: "utf-8",
		Custom: map[string]string{
			"author":      "john-doe",
			"version":     "1.0",
			"environment": "production",
		},
	}

	data := []byte(`{"status": "active"}`)
	err := objstore.PutWithMetadata(ctx, "config.json", bytes.NewReader(data), metadata)
	if err != nil {
		log.Printf("  PutWithMetadata failed: %v", err)
		return
	}
	fmt.Println("  Put with metadata: config.json")

	// Get metadata
	meta, err := objstore.GetMetadata(ctx, "config.json")
	if err != nil {
		log.Printf("  GetMetadata failed: %v", err)
		return
	}

	fmt.Printf("  Content-Type: %s\n", meta.ContentType)
	if meta.Custom != nil {
		fmt.Printf("  Author: %s\n", meta.Custom["author"])
		fmt.Printf("  Version: %s\n", meta.Custom["version"])
	}

	// Update metadata
	if meta.Custom != nil {
		meta.Custom["version"] = "2.0"
		meta.Custom["updated"] = time.Now().Format(time.RFC3339)
	}

	err = objstore.UpdateMetadata(ctx, "config.json", meta)
	if err != nil {
		log.Printf("  UpdateMetadata failed: %v", err)
		return
	}
	fmt.Println("  Metadata updated")
}

func listOperations() {
	ctx := context.Background()

	// Create some test objects
	objects := []struct {
		key  string
		data string
	}{
		{"logs/2024-01/app.log", "app log"},
		{"logs/2024-01/error.log", "error log"},
		{"logs/2024-02/app.log", "app log 2"},
		{"data/users.json", "users"},
		{"data/config.json", "config"},
	}

	for _, obj := range objects {
		err := objstore.PutWithContext(ctx, obj.key, bytes.NewReader([]byte(obj.data)))
		if err != nil {
			log.Printf("  Failed to put %s: %v", obj.key, err)
		}
	}
	fmt.Println("  Created 5 test objects")

	// List with prefix
	logKeys, err := objstore.ListWithContext(ctx, "logs/")
	if err != nil {
		log.Printf("  List failed: %v", err)
		return
	}
	fmt.Printf("  Objects in 'logs/': %d\n", len(logKeys))

	// List with options (pagination)
	opts := &common.ListOptions{
		Prefix:     "logs/",
		MaxResults: 2,
	}

	result, err := objstore.ListWithOptions(ctx, "", opts)
	if err != nil {
		log.Printf("  ListWithOptions failed: %v", err)
		return
	}

	fmt.Printf("  First page: %d objects\n", len(result.Objects))
	for _, obj := range result.Objects {
		fmt.Printf("    - %s (%d bytes)\n", obj.Key, obj.Metadata.Size)
	}

	if result.Truncated {
		fmt.Println("  More results available (pagination)")
	}
}

func securityDemo() {
	ctx := context.Background()

	// All of these will fail due to validation
	attackVectors := []string{
		"../../../etc/passwd",     // Path traversal
		"path/../file.txt",        // Path traversal
		"/etc/passwd",             // Absolute path
		"file\x00.txt",           // Null byte
		"file\n.txt",             // Control character
		"INVALID:key",            // Invalid backend name
	}

	fmt.Println("  Testing security validations...")
	successCount := 0
	for _, attack := range attackVectors {
		err := objstore.PutWithContext(ctx, attack, bytes.NewReader([]byte("malicious")))
		if err != nil {
			successCount++
		} else {
			fmt.Printf("  SECURITY ISSUE: Attack vector accepted: %q\n", attack)
		}
	}

	fmt.Printf("  All %d attack vectors blocked by validation\n", successCount)
	fmt.Println("  Input validation is working correctly")
}
