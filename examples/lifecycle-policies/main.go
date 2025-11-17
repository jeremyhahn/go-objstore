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
	"log"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/factory"
)

// Example demonstrating lifecycle policies for automatic deletion and archival

func main() {
	fmt.Println("=== Lifecycle Policies Example ===")

	// Example 1: Delete policy for old logs
	fmt.Println("1. Delete Policy - Old Logs")
	deletePolicyExample()

	// Example 2: Archive policy for old data
	fmt.Println("\n2. Archive Policy - Old Data")
	archivePolicyExample()

	// Example 3: Managing policies
	fmt.Println("\n3. Policy Management")
	policyManagementExample()
}

func deletePolicyExample() {
	// Create storage backend
	storage, err := factory.NewStorage("local", map[string]string{
		"path": "/tmp/objstore-lifecycle",
	})
	if err != nil {
		log.Fatalf("Failed to create storage: %v", err)
	}

	// Create some test log files
	logs := []string{
		"logs/2024-01-15/app.log",
		"logs/2024-02-10/app.log",
		"logs/2024-03-20/app.log",
	}

	for _, key := range logs {
		storage.Put(key, bytes.NewReader([]byte("log data")))
	}
	fmt.Printf("  ✓ Created %d log files\n", len(logs))

	// Create a policy to delete logs older than 30 days
	deletePolicy := common.LifecyclePolicy{
		ID:        "delete-old-logs",
		Prefix:    "logs/",
		Action:    "delete",
		Retention: 30 * 24 * time.Hour,
	}

	if err := storage.AddPolicy(deletePolicy); err != nil {
		log.Fatalf("Failed to add policy: %v", err)
	}
	fmt.Println("  ✓ Added delete policy for logs older than 30 days")

	// List current policies
	policies, _ := storage.GetPolicies()
	fmt.Printf("  ✓ Active policies: %d\n", len(policies))

	// In a real application, the lifecycle manager would run in the background
	// and automatically delete files matching the policy criteria
	fmt.Println("  ℹ Lifecycle manager would run in background to enforce policies")

	// Cleanup
	for _, key := range logs {
		storage.Delete(key)
	}
}

func archivePolicyExample() {
	// Create primary storage
	storage, err := factory.NewStorage("local", map[string]string{
		"path": "/tmp/objstore-primary",
	})
	if err != nil {
		log.Fatalf("Failed to create storage: %v", err)
	}

	// Create archival storage (using local as example, could be Glacier)
	archiveStorage, err := factory.NewArchiver("local", map[string]string{
		"path": "/tmp/objstore-archive",
	})
	if err != nil {
		log.Fatalf("Failed to create archiver: %v", err)
	}

	// Create test data files
	dataFiles := []string{
		"data/reports/2023/q1.json",
		"data/reports/2023/q2.json",
		"data/reports/2024/q1.json",
	}

	for _, key := range dataFiles {
		storage.Put(key, bytes.NewReader([]byte("report data")))
	}
	fmt.Printf("  ✓ Created %d data files\n", len(dataFiles))

	// Create policy to archive data older than 90 days
	archivePolicy := common.LifecyclePolicy{
		ID:          "archive-old-reports",
		Prefix:      "data/reports/",
		Action:      "archive",
		Destination: archiveStorage,
		Retention:   90 * 24 * time.Hour,
	}

	if err := storage.AddPolicy(archivePolicy); err != nil {
		log.Fatalf("Failed to add archive policy: %v", err)
	}
	fmt.Println("  ✓ Added archive policy for reports older than 90 days")
	fmt.Println("  ℹ Old reports will be moved to archive storage automatically")

	// Manually archive one file as demonstration
	if err := storage.Archive("data/reports/2023/q1.json", archiveStorage); err != nil {
		log.Printf("  ⚠ Archive failed: %v", err)
	} else {
		fmt.Println("  ✓ Manually archived: data/reports/2023/q1.json")
	}

	// Cleanup
	for _, key := range dataFiles {
		storage.Delete(key)
	}
}

func policyManagementExample() {
	storage, _ := factory.NewStorage("local", map[string]string{
		"path": "/tmp/objstore-policies",
	})

	// Add multiple policies
	policies := []common.LifecyclePolicy{
		{
			ID:        "delete-temp-files",
			Prefix:    "temp/",
			Action:    "delete",
			Retention: 24 * time.Hour,
		},
		{
			ID:        "delete-cache",
			Prefix:    "cache/",
			Action:    "delete",
			Retention: 7 * 24 * time.Hour,
		},
		{
			ID:        "delete-old-logs",
			Prefix:    "logs/",
			Action:    "delete",
			Retention: 30 * 24 * time.Hour,
		},
	}

	for _, policy := range policies {
		if err := storage.AddPolicy(policy); err != nil {
			log.Printf("  ⚠ Failed to add policy %s: %v", policy.ID, err)
			continue
		}
		fmt.Printf("  ✓ Added policy: %s\n", policy.ID)
	}

	// List all policies
	allPolicies, err := storage.GetPolicies()
	if err != nil {
		log.Fatalf("Failed to get policies: %v", err)
	}
	fmt.Printf("\n  Active Policies (%d):\n", len(allPolicies))
	for _, p := range allPolicies {
		fmt.Printf("    - %s: %s files in '%s' after %v\n",
			p.ID, p.Action, p.Prefix, p.Retention)
	}

	// Remove a policy
	if err := storage.RemovePolicy("delete-cache"); err != nil {
		log.Fatalf("Failed to remove policy: %v", err)
	}
	fmt.Println("\n  ✓ Removed policy: delete-cache")

	// Verify removal
	remainingPolicies, _ := storage.GetPolicies()
	fmt.Printf("  ✓ Remaining policies: %d\n", len(remainingPolicies))

	// Cleanup - remove all policies
	for _, p := range remainingPolicies {
		storage.RemovePolicy(p.ID)
	}
}

// Example: Real-world usage with Glacier archival
func glacierArchiveExample() {
	// Create S3 storage for active data
	s3Storage, err := factory.NewStorage("s3", map[string]string{
		"bucket": "my-active-bucket",
		"region": "us-east-1",
	})
	if err != nil {
		log.Printf("Skipping Glacier example: %v", err)
		return
	}

	// Create Glacier archiver for cold storage
	glacier, err := factory.NewArchiver("glacier", map[string]string{
		"vaultName": "long-term-archive",
		"region":    "us-east-1",
	})
	if err != nil {
		log.Printf("Skipping Glacier example: %v", err)
		return
	}

	// Policy: Archive data to Glacier after 1 year
	archivePolicy := common.LifecyclePolicy{
		ID:          "s3-to-glacier",
		Prefix:      "historical-data/",
		Action:      "archive",
		Destination: glacier,
		Retention:   365 * 24 * time.Hour,
	}

	s3Storage.AddPolicy(archivePolicy)
	fmt.Println("Configured S3-to-Glacier archival policy")

	// Policy: Delete temporary uploads after 7 days
	deletePolicy := common.LifecyclePolicy{
		ID:        "cleanup-uploads",
		Prefix:    "uploads/temp/",
		Action:    "delete",
		Retention: 7 * 24 * time.Hour,
	}

	s3Storage.AddPolicy(deletePolicy)
	fmt.Println("Configured automatic cleanup for temporary uploads")
}

// Example: Multi-tier archival strategy
func multiTierArchivalExample() {
	// Tier 1: Active data in S3
	activeStorage, _ := factory.NewStorage("s3", map[string]string{
		"bucket": "active-data",
		"region": "us-east-1",
	})

	// Tier 2: Warm data in local storage
	warmStorage, _ := factory.NewArchiver("local", map[string]string{
		"path": "/mnt/warm-storage",
	})

	// Tier 3: Cold data in Glacier
	coldStorage, _ := factory.NewArchiver("glacier", map[string]string{
		"vaultName": "cold-archive",
		"region":    "us-east-1",
	})

	// Move to warm storage after 30 days
	warmPolicy := common.LifecyclePolicy{
		ID:          "to-warm",
		Prefix:      "data/",
		Action:      "archive",
		Destination: warmStorage,
		Retention:   30 * 24 * time.Hour,
	}

	// Move to cold storage after 365 days
	coldPolicy := common.LifecyclePolicy{
		ID:          "to-cold",
		Prefix:      "data/",
		Action:      "archive",
		Destination: coldStorage,
		Retention:   365 * 24 * time.Hour,
	}

	activeStorage.AddPolicy(warmPolicy)
	activeStorage.AddPolicy(coldPolicy)

	fmt.Println("Configured multi-tier archival strategy:")
	fmt.Println("  - Active (S3): Recent data")
	fmt.Println("  - Warm (Local): Data > 30 days")
	fmt.Println("  - Cold (Glacier): Data > 365 days")
}

// Note: In production, you would run a lifecycle manager service that:
// 1. Periodically scans objects in storage
// 2. Checks their age against policy rules
// 3. Executes delete or archive actions automatically
// 4. Logs all lifecycle actions for audit trail
func runLifecycleManager(storage common.Storage, interval time.Duration) {
	ctx := context.Background()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			policies, err := storage.GetPolicies()
			if err != nil {
				log.Printf("Failed to get policies: %v", err)
				continue
			}

			for _, policy := range policies {
				// Get all objects matching the prefix
				keys, err := storage.List(policy.Prefix)
				if err != nil {
					log.Printf("Failed to list objects for policy %s: %v", policy.ID, err)
					continue
				}

				for _, key := range keys {
					// Check object age
					meta, err := storage.GetMetadata(ctx, key)
					if err != nil {
						continue
					}

					if meta != nil && !meta.LastModified.IsZero() {
						age := time.Since(meta.LastModified)
						if age > policy.Retention {
							// Execute policy action
							if policy.Action == "delete" {
								storage.Delete(key)
								log.Printf("Deleted %s (age: %v)", key, age)
							} else if policy.Action == "archive" && policy.Destination != nil {
								storage.Archive(key, policy.Destination)
								log.Printf("Archived %s (age: %v)", key, age)
							}
						}
					}
				}
			}
		}
	}
}
