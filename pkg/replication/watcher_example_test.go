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

package replication_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/replication"
)

// ExampleFSNotifyWatcher demonstrates basic usage of the filesystem watcher.
func ExampleFSNotifyWatcher() {
	// Create a temporary directory to watch
	watchDir, err := os.MkdirTemp("", "watch-example")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(watchDir)

	// Create a watcher with configuration
	watcher, err := replication.NewFSNotifyWatcher(replication.FSNotifyWatcherConfig{
		Logger:        adapters.NewNoOpLogger(),
		DebounceDelay: 100 * time.Millisecond,
		EventBuffer:   50,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer watcher.Stop()

	// Start watching the directory
	if err := watcher.Watch(watchDir); err != nil {
		log.Fatal(err)
	}

	// Create a goroutine to handle events
	go func() {
		for event := range watcher.Events() {
			fmt.Printf("Event: %s %s at %s\n",
				event.Operation,
				event.Path,
				event.Timestamp.Format(time.RFC3339))
		}
	}()

	// Simulate some file operations
	testFile := filepath.Join(watchDir, "test.txt")
	if err := os.WriteFile(testFile, []byte("hello"), 0644); err != nil {
		log.Fatal(err)
	}

	time.Sleep(200 * time.Millisecond)

	// Output will vary based on timing
}

// ExampleFSNotifyWatcher_replicationIntegration demonstrates how to integrate
// the watcher with replication for real-time sync.
func ExampleFSNotifyWatcher_replicationIntegration() {
	// This example shows conceptual integration with replication
	// In a real implementation, you would:

	// 1. Create a watcher for the source directory
	logger := adapters.NewNoOpLogger()
	watcher, err := replication.NewFSNotifyWatcher(replication.FSNotifyWatcherConfig{
		Logger:        logger,
		DebounceDelay: 200 * time.Millisecond,
		EventBuffer:   100,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer watcher.Stop()

	// 2. Watch the source directory
	sourceDir := "/path/to/source"
	if err := watcher.Watch(sourceDir); err != nil {
		log.Printf("Watch failed: %v", err)
		return
	}

	// 3. Process events and trigger replication
	ctx := context.Background()
	for event := range watcher.Events() {
		switch event.Operation {
		case "put":
			// Trigger immediate replication for this file
			log.Printf("Replicating changed file: %s", event.Path)
			// replicationManager.SyncObject(ctx, event.Path)

		case "delete":
			// Replicate deletion to destination
			log.Printf("Replicating deletion: %s", event.Path)
			// destinationBackend.Delete(event.Path)
		}
	}

	_ = ctx // suppress unused warning in example
}

// ExampleFSNotifyWatcher_recursiveWatch demonstrates recursive directory watching.
func ExampleFSNotifyWatcher_recursiveWatch() {
	// Create directory structure
	rootDir, err := os.MkdirTemp("", "recursive-watch")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(rootDir)

	subDir := filepath.Join(rootDir, "subdir", "nested")
	if err := os.MkdirAll(subDir, 0755); err != nil {
		log.Fatal(err)
	}

	// Create watcher
	watcher, err := replication.NewFSNotifyWatcher(replication.FSNotifyWatcherConfig{
		Logger: adapters.NewNoOpLogger(),
	})
	if err != nil {
		log.Fatal(err)
	}
	defer watcher.Stop()

	// Watch root directory - subdirectories are automatically watched
	if err := watcher.Watch(rootDir); err != nil {
		log.Fatal(err)
	}

	// Handle events
	go func() {
		for event := range watcher.Events() {
			log.Printf("Detected change in nested directory: %s", event.Path)
		}
	}()

	// Create file in nested directory - will be detected
	testFile := filepath.Join(subDir, "deep.txt")
	if err := os.WriteFile(testFile, []byte("deep file"), 0644); err != nil {
		log.Fatal(err)
	}

	time.Sleep(200 * time.Millisecond)
}

// ExampleFSNotifyWatcher_debouncing demonstrates how debouncing prevents
// duplicate events for rapid file modifications.
func ExampleFSNotifyWatcher_debouncing() {
	watchDir, err := os.MkdirTemp("", "debounce-example")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(watchDir)

	// Configure with longer debounce delay
	watcher, err := replication.NewFSNotifyWatcher(replication.FSNotifyWatcherConfig{
		Logger:        adapters.NewNoOpLogger(),
		DebounceDelay: 500 * time.Millisecond, // 500ms debounce
		EventBuffer:   10,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer watcher.Stop()

	if err := watcher.Watch(watchDir); err != nil {
		log.Fatal(err)
	}

	eventCount := 0
	done := make(chan struct{})

	// Count events
	go func() {
		for range watcher.Events() {
			eventCount++
		}
		close(done)
	}()

	// Write to same file multiple times rapidly
	testFile := filepath.Join(watchDir, "test.txt")
	for i := 0; i < 10; i++ {
		if err := os.WriteFile(testFile, []byte(fmt.Sprintf("version %d", i)), 0644); err != nil {
			log.Fatal(err)
		}
		time.Sleep(50 * time.Millisecond) // Write every 50ms
	}

	// Wait for debouncing to settle
	time.Sleep(1 * time.Second)

	// Due to 500ms debounce, we should receive fewer than 10 events
	fmt.Printf("Received %d events for 10 writes (debouncing reduced duplicates)\n", eventCount)
}

// ExampleFSNotifyWatcher_multipleWatchers demonstrates running multiple
// independent watchers for different directories.
func ExampleFSNotifyWatcher_multipleWatchers() {
	// Create two separate watch directories
	dir1, _ := os.MkdirTemp("", "watch1")
	dir2, _ := os.MkdirTemp("", "watch2")
	defer os.RemoveAll(dir1)
	defer os.RemoveAll(dir2)

	logger := adapters.NewNoOpLogger()

	// Create first watcher for dir1
	watcher1, err := replication.NewFSNotifyWatcher(replication.FSNotifyWatcherConfig{
		Logger: logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer watcher1.Stop()

	// Create second watcher for dir2
	watcher2, err := replication.NewFSNotifyWatcher(replication.FSNotifyWatcherConfig{
		Logger: logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer watcher2.Stop()

	// Start watching
	if err := watcher1.Watch(dir1); err != nil {
		log.Fatal(err)
	}
	if err := watcher2.Watch(dir2); err != nil {
		log.Fatal(err)
	}

	// Handle events from both watchers independently
	go func() {
		for event := range watcher1.Events() {
			log.Printf("Dir1 change: %s", event.Path)
		}
	}()

	go func() {
		for event := range watcher2.Events() {
			log.Printf("Dir2 change: %s", event.Path)
		}
	}()

	// Make changes in both directories
	os.WriteFile(filepath.Join(dir1, "file1.txt"), []byte("data1"), 0644)
	os.WriteFile(filepath.Join(dir2, "file2.txt"), []byte("data2"), 0644)

	time.Sleep(200 * time.Millisecond)
}
