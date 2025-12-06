// Copyright (c) 2025 Jeremy Hahn
// Copyright (c) 2025 Automate The Things, LLC
//
// This file is part of go-objstore.
//
// go-objstore is dual-licensed under AGPL-3.0 and a Commercial License.

//go:build ignore
// +build ignore

package main

import (
	"context"
	"fmt"
	"log"
	"time"

	objstore "github.com/jeremyhahn/go-objstore/api/sdks/go"
)

func main() {
	// Example using gRPC client
	grpcExample()

	// Example using REST client
	restExample()
}

func grpcExample() {
	fmt.Println("=== gRPC Client Example ===")

	config := &objstore.ClientConfig{
		Protocol:          objstore.ProtocolGRPC,
		Address:           "localhost:50051",
		ConnectionTimeout: 10 * time.Second,
		RequestTimeout:    30 * time.Second,
	}

	client, err := objstore.NewClient(config)
	if err != nil {
		log.Fatalf("Failed to create gRPC client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	// Health check
	health, err := client.Health(ctx)
	if err != nil {
		log.Printf("Health check failed: %v", err)
		return
	}
	fmt.Printf("Server status: %s\n", health.Status)

	// Put an object
	data := []byte("Hello from gRPC!")
	metadata := &objstore.Metadata{
		ContentType: "text/plain",
		Custom: map[string]string{
			"author":  "example-user",
			"version": "1.0",
		},
	}

	result, err := client.Put(ctx, "example/grpc-test.txt", data, metadata)
	if err != nil {
		log.Printf("Failed to put object: %v", err)
		return
	}
	fmt.Printf("Object stored with ETag: %s\n", result.ETag)

	// Get the object
	getResult, err := client.Get(ctx, "example/grpc-test.txt")
	if err != nil {
		log.Printf("Failed to get object: %v", err)
		return
	}
	fmt.Printf("Retrieved data: %s\n", string(getResult.Data))

	// List objects
	listOpts := &objstore.ListOptions{
		Prefix:     "example/",
		MaxResults: 10,
	}

	listResult, err := client.List(ctx, listOpts)
	if err != nil {
		log.Printf("Failed to list objects: %v", err)
		return
	}

	fmt.Printf("Found %d objects:\n", len(listResult.Objects))
	for _, obj := range listResult.Objects {
		fmt.Printf("  - %s (size: %d bytes)\n", obj.Key, obj.Metadata.Size)
	}

	// Delete the object
	err = client.Delete(ctx, "example/grpc-test.txt")
	if err != nil {
		log.Printf("Failed to delete object: %v", err)
		return
	}
	fmt.Println("Object deleted successfully")
}

func restExample() {
	fmt.Println("\n=== REST Client Example ===")

	config := &objstore.ClientConfig{
		Protocol:       objstore.ProtocolREST,
		Address:        "localhost:8080",
		RequestTimeout: 30 * time.Second,
	}

	client, err := objstore.NewClient(config)
	if err != nil {
		log.Fatalf("Failed to create REST client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	// Health check
	health, err := client.Health(ctx)
	if err != nil {
		log.Printf("Health check failed: %v", err)
		return
	}
	fmt.Printf("Server status: %s\n", health.Status)

	// Put an object
	data := []byte("Hello from REST!")
	metadata := &objstore.Metadata{
		ContentType: "text/plain",
		Custom: map[string]string{
			"source": "rest-example",
		},
	}

	result, err := client.Put(ctx, "example/rest-test.txt", data, metadata)
	if err != nil {
		log.Printf("Failed to put object: %v", err)
		return
	}
	fmt.Printf("Object stored with ETag: %s\n", result.ETag)

	// Check if exists
	exists, err := client.Exists(ctx, "example/rest-test.txt")
	if err != nil {
		log.Printf("Failed to check existence: %v", err)
		return
	}
	fmt.Printf("Object exists: %v\n", exists)

	// Get metadata
	meta, err := client.GetMetadata(ctx, "example/rest-test.txt")
	if err != nil {
		log.Printf("Failed to get metadata: %v", err)
		return
	}
	fmt.Printf("Object size: %d bytes, Content-Type: %s\n", meta.Size, meta.ContentType)

	// Delete the object
	err = client.Delete(ctx, "example/rest-test.txt")
	if err != nil {
		log.Printf("Failed to delete object: %v", err)
		return
	}
	fmt.Println("Object deleted successfully")
}
