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
	"errors"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/factory"
)

// Example demonstrating basic storage operations with different backends

func main() {
	fmt.Println("=== go-objstore Basic Usage Examples ===")

	// Example 1: Local storage
	fmt.Println("1. Local Storage")
	localStorageExample()

	// Example 2: S3 storage (MinIO for local development)
	fmt.Println("\n2. S3 Storage (MinIO)")
	s3StorageExample()

	// Example 3: Context-aware operations
	fmt.Println("\n3. Context-Aware Operations")
	contextExample()

	// Example 4: Metadata operations
	fmt.Println("\n4. Metadata Operations")
	metadataExample()

	// Example 5: List and pagination
	fmt.Println("\n5. List and Pagination")
	listExample()
}

func localStorageExample() {
	// Create local storage backend
	storage, err := factory.NewStorage("local", map[string]string{
		"path": "/tmp/objstore-demo",
	})
	if err != nil {
		log.Fatalf("Failed to create storage: %v", err)
	}

	// Store some data
	data := []byte("Hello, World!")
	if err := storage.Put("greeting.txt", bytes.NewReader(data)); err != nil {
		log.Fatalf("Failed to put object: %v", err)
	}
	fmt.Println("  ✓ Stored: greeting.txt")

	// Retrieve the data
	reader, err := storage.Get("greeting.txt")
	if err != nil {
		log.Fatalf("Failed to get object: %v", err)
	}
	content, _ := io.ReadAll(reader)
	_ = reader.Close()
	fmt.Printf("  ✓ Retrieved: %s\n", content)

	// Check if object exists
	ctx := context.Background()
	exists, err := storage.Exists(ctx, "greeting.txt")
	if err != nil {
		log.Fatalf("Failed to check existence: %v", err)
	}
	fmt.Printf("  ✓ Exists: %v\n", exists)

	// Delete the object
	if err := storage.Delete("greeting.txt"); err != nil {
		log.Fatalf("Failed to delete object: %v", err)
	}
	fmt.Println("  ✓ Deleted: greeting.txt")
}

func s3StorageExample() {
	// For this example to work, you need MinIO running locally:
	// docker run -p 9000:9000 -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin minio/minio server /data

	storage, err := factory.NewStorage("s3", map[string]string{
		"bucket":         "demo-bucket",
		"region":         "us-east-1",
		"endpoint":       "http://localhost:9000",
		"forcePathStyle": "true",
		"accessKey":      "minioadmin",
		"secretKey":      "minioadmin",
	})

	if err != nil {
		fmt.Printf("  ⚠ Skipping S3 example (MinIO not running): %v\n", err)
		return
	}

	// Store nested objects
	objects := map[string]string{
		"docs/readme.txt": "Documentation",
		"logs/app.log":    "Application logs",
		"data/users.json": `{"users": []}`,
	}

	for key, content := range objects {
		if err := storage.Put(key, bytes.NewReader([]byte(content))); err != nil {
			fmt.Printf("  ⚠ Failed to store %s: %v\n", key, err)
			continue
		}
		fmt.Printf("  ✓ Stored: %s\n", key)
	}

	// List all objects
	keys, err := storage.List("")
	if err != nil {
		log.Printf("  ⚠ Failed to list objects: %v", err)
		return
	}
	fmt.Printf("  ✓ Total objects: %d\n", len(keys))
}

func contextExample() {
	storage, _ := factory.NewStorage("local", map[string]string{
		"path": "/tmp/objstore-context",
	})

	// Example 1: Context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	data := []byte("Data with timeout")
	if err := storage.PutWithContext(ctx, "timeout-test.txt", bytes.NewReader(data)); err != nil {
		log.Printf("  ⚠ Put with context failed: %v", err)
		return
	}
	fmt.Println("  ✓ Put with context successful")

	// Example 2: Context with cancellation
	ctx2, cancel2 := context.WithCancel(context.Background())

	// Simulate cancellation after operation starts
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel2()
	}()

	reader, err := storage.GetWithContext(ctx2, "timeout-test.txt")
	if err != nil {
		if errors.Is(err, context.Canceled) {
			fmt.Println("  ✓ Context cancellation detected")
		} else {
			fmt.Printf("  ✓ Get completed before cancellation\n")
			_ = reader.Close()
		}
	}

	// Cleanup
	_ = storage.DeleteWithContext(context.Background(), "timeout-test.txt")
}

func metadataExample() {
	storage, _ := factory.NewStorage("local", map[string]string{
		"path": "/tmp/objstore-metadata",
	})

	ctx := context.Background()

	// Create object with metadata
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
	if err := storage.PutWithMetadata(ctx, "config.json", bytes.NewReader(data), metadata); err != nil {
		log.Printf("  ⚠ Failed to put with metadata: %v", err)
		return
	}
	fmt.Println("  ✓ Stored object with metadata")

	// Retrieve metadata
	meta, err := storage.GetMetadata(ctx, "config.json")
	if err != nil {
		log.Printf("  ⚠ Failed to get metadata: %v", err)
		return
	}

	if meta != nil && meta.Custom != nil {
		fmt.Printf("  ✓ Author: %s\n", meta.Custom["author"])
		fmt.Printf("  ✓ Version: %s\n", meta.Custom["version"])

		// Update metadata
		meta.Custom["version"] = "2.0"
		meta.Custom["updated"] = time.Now().Format(time.RFC3339)
		if err := storage.UpdateMetadata(ctx, "config.json", meta); err != nil {
			log.Printf("  ⚠ Failed to update metadata: %v", err)
		} else {
			fmt.Println("  ✓ Metadata updated")
		}
	}

	// Cleanup
	_ = storage.DeleteWithContext(ctx, "config.json")
}

func listExample() {
	storage, _ := factory.NewStorage("local", map[string]string{
		"path": "/tmp/objstore-list",
	})

	ctx := context.Background()

	// Create multiple objects with different prefixes
	objects := []string{
		"logs/2024-01/app.log",
		"logs/2024-01/error.log",
		"logs/2024-02/app.log",
		"data/users.json",
		"data/config.json",
	}

	for _, key := range objects {
		_ = storage.Put(key, bytes.NewReader([]byte("test")))
	}
	fmt.Printf("  ✓ Created %d test objects\n", len(objects))

	// List with prefix
	logKeys, _ := storage.List("logs/")
	fmt.Printf("  ✓ Objects in 'logs/': %d\n", len(logKeys))

	dataKeys, _ := storage.List("data/")
	fmt.Printf("  ✓ Objects in 'data/': %d\n", len(dataKeys))

	// List with pagination
	opts := &common.ListOptions{
		Prefix:     "logs/",
		MaxResults: 2,
	}

	result, err := storage.ListWithOptions(ctx, opts)
	if err != nil {
		log.Printf("  ⚠ Failed to list with options: %v", err)
		return
	}

	fmt.Printf("  ✓ First page: %d objects\n", len(result.Objects))
	for _, obj := range result.Objects {
		fmt.Printf("    - %s\n", obj.Key)
	}

	// Get next page if available
	if result.Truncated {
		opts.ContinueFrom = result.NextToken
		nextPage, _ := storage.ListWithOptions(ctx, opts)
		fmt.Printf("  ✓ Next page: %d objects\n", len(nextPage.Objects))
	}

	// Cleanup
	for _, key := range objects {
		_ = storage.Delete(key)
	}
}
