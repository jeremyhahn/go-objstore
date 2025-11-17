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
	"fmt"
	"io"
	"log"
	"time"

	objstorepb "github.com/jeremyhahn/go-objstore/api/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// Connect to the gRPC server
	conn, err := grpc.Dial(
		"localhost:50051",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithTimeout(5*time.Second),
	)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client := objstorepb.NewObjectStoreClient(conn)
	ctx := context.Background()

	fmt.Println("=== ObjectStore gRPC Client Example ===")

	// Example 1: Put an object
	fmt.Println("1. Putting an object...")
	putResp, err := client.Put(ctx, &objstorepb.PutRequest{
		Key:  "example/hello.txt",
		Data: []byte("Hello, gRPC ObjectStore!"),
		Metadata: &objstorepb.Metadata{
			ContentType: "text/plain",
			Custom: map[string]string{
				"author": "example-client",
			},
		},
	})
	if err != nil {
		log.Fatalf("Put failed: %v", err)
	}
	fmt.Printf("   Success: %v, ETag: %s\n\n", putResp.Success, putResp.Etag)

	// Example 2: Get the object back
	fmt.Println("2. Getting the object...")
	getStream, err := client.Get(ctx, &objstorepb.GetRequest{
		Key: "example/hello.txt",
	})
	if err != nil {
		log.Fatalf("Get failed: %v", err)
	}

	var data []byte
	var metadata *objstorepb.Metadata

	for {
		resp, err := getStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("Stream receive failed: %v", err)
		}

		if resp.Metadata != nil {
			metadata = resp.Metadata
		}

		data = append(data, resp.Data...)
	}

	fmt.Printf("   Data: %s\n", string(data))
	fmt.Printf("   ContentType: %s\n", metadata.ContentType)
	fmt.Printf("   Size: %d bytes\n\n", metadata.Size)

	// Example 3: Check if object exists
	fmt.Println("3. Checking if object exists...")
	existsResp, err := client.Exists(ctx, &objstorepb.ExistsRequest{
		Key: "example/hello.txt",
	})
	if err != nil {
		log.Fatalf("Exists failed: %v", err)
	}
	fmt.Printf("   Exists: %v\n\n", existsResp.Exists)

	// Example 4: Get metadata only
	fmt.Println("4. Getting metadata...")
	metaResp, err := client.GetMetadata(ctx, &objstorepb.GetMetadataRequest{
		Key: "example/hello.txt",
	})
	if err != nil {
		log.Fatalf("GetMetadata failed: %v", err)
	}
	fmt.Printf("   ContentType: %s\n", metaResp.Metadata.ContentType)
	fmt.Printf("   Custom metadata: %v\n\n", metaResp.Metadata.Custom)

	// Example 5: Update metadata
	fmt.Println("5. Updating metadata...")
	updateResp, err := client.UpdateMetadata(ctx, &objstorepb.UpdateMetadataRequest{
		Key: "example/hello.txt",
		Metadata: &objstorepb.Metadata{
			ContentType: "text/plain; charset=utf-8",
			Custom: map[string]string{
				"author":  "example-client",
				"version": "1.0",
			},
		},
	})
	if err != nil {
		log.Fatalf("UpdateMetadata failed: %v", err)
	}
	fmt.Printf("   Success: %v\n\n", updateResp.Success)

	// Example 6: List objects
	fmt.Println("6. Listing objects...")
	listResp, err := client.List(ctx, &objstorepb.ListRequest{
		Prefix:     "example/",
		MaxResults: 10,
	})
	if err != nil {
		log.Fatalf("List failed: %v", err)
	}
	fmt.Printf("   Found %d objects:\n", len(listResp.Objects))
	for _, obj := range listResp.Objects {
		fmt.Printf("     - %s (%d bytes)\n", obj.Key, obj.Metadata.Size)
	}
	fmt.Println()

	// Example 7: Put a large object (streaming)
	fmt.Println("7. Putting a large object...")
	largeData := make([]byte, 1024*1024) // 1MB
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	putResp, err = client.Put(ctx, &objstorepb.PutRequest{
		Key:  "example/large-file.bin",
		Data: largeData,
		Metadata: &objstorepb.Metadata{
			ContentType: "application/octet-stream",
		},
	})
	if err != nil {
		log.Fatalf("Put large object failed: %v", err)
	}
	fmt.Printf("   Success: %v, Size: %d bytes\n\n", putResp.Success, len(largeData))

	// Example 8: Delete an object
	fmt.Println("8. Deleting an object...")
	delResp, err := client.Delete(ctx, &objstorepb.DeleteRequest{
		Key: "example/hello.txt",
	})
	if err != nil {
		log.Fatalf("Delete failed: %v", err)
	}
	fmt.Printf("   Success: %v\n\n", delResp.Success)

	// Example 9: Health check
	fmt.Println("9. Checking server health...")
	healthResp, err := client.Health(ctx, &objstorepb.HealthRequest{})
	if err != nil {
		log.Fatalf("Health check failed: %v", err)
	}
	fmt.Printf("   Status: %v\n", healthResp.Status)
	fmt.Printf("   Message: %s\n\n", healthResp.Message)

	fmt.Println("=== All examples completed successfully! ===")
}
