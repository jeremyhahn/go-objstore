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

package replication

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/jeremyhahn/go-objstore/pkg/adapters"
)

// FileSystemEvent represents a filesystem change event.
type FileSystemEvent struct {
	Path      string    `json:"path"`
	Operation string    `json:"operation"` // "put", "delete"
	Timestamp time.Time `json:"timestamp"`
}

// FileSystemWatcher defines the interface for watching filesystem changes.
type FileSystemWatcher interface {
	// Watch starts watching a path (directory or file).
	// For directories, watching is recursive.
	Watch(path string) error

	// Stop stops watching and cleans up resources.
	Stop() error

	// Events returns a read-only channel of filesystem events.
	Events() <-chan FileSystemEvent
}

// FSNotifyWatcher implements FileSystemWatcher using fsnotify.
type FSNotifyWatcher struct {
	watcher       *fsnotify.Watcher
	events        chan FileSystemEvent
	logger        adapters.Logger
	debounceDelay time.Duration

	// Tracking state
	mu        sync.RWMutex
	watching  map[string]bool      // Paths currently being watched
	lastEvent map[string]time.Time // For debouncing
	stopChan  chan struct{}
	stopped   bool

	// Context for managing goroutines
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// FSNotifyWatcherConfig contains configuration options for FSNotifyWatcher.
type FSNotifyWatcherConfig struct {
	Logger        adapters.Logger
	DebounceDelay time.Duration // Default: 100ms
	EventBuffer   int           // Default: 100
}

// NewFSNotifyWatcher creates a new FSNotifyWatcher with the given configuration.
func NewFSNotifyWatcher(config FSNotifyWatcherConfig) (*FSNotifyWatcher, error) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}

	if config.Logger == nil {
		config.Logger = adapters.NewNoOpLogger()
	}

	if config.DebounceDelay == 0 {
		config.DebounceDelay = 100 * time.Millisecond
	}

	if config.EventBuffer == 0 {
		config.EventBuffer = 100
	}

	ctx, cancel := context.WithCancel(context.Background())

	w := &FSNotifyWatcher{
		watcher:       watcher,
		events:        make(chan FileSystemEvent, config.EventBuffer),
		logger:        config.Logger,
		debounceDelay: config.DebounceDelay,
		watching:      make(map[string]bool),
		lastEvent:     make(map[string]time.Time),
		stopChan:      make(chan struct{}),
		ctx:           ctx,
		cancel:        cancel,
	}

	// Start event processing goroutine
	w.wg.Add(1)
	go w.processEvents()

	return w, nil
}

// Watch starts watching a path recursively.
func (w *FSNotifyWatcher) Watch(path string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.stopped {
		return &WatcherError{Op: "watch", Path: path, Err: ErrWatcherStopped}
	}

	// Clean the path
	path = filepath.Clean(path)

	// Check if already watching
	if w.watching[path] {
		w.logger.Debug(w.ctx, "Path already being watched",
			adapters.Field{Key: "path", Value: path})
		return nil
	}

	// Add the root path
	if err := w.watcher.Add(path); err != nil {
		return &WatcherError{Op: "watch", Path: path, Err: err}
	}

	w.watching[path] = true
	w.logger.Info(w.ctx, "Started watching path",
		adapters.Field{Key: "path", Value: path})

	// Walk the directory tree and add all subdirectories
	err := filepath.Walk(path, func(walkPath string, info os.FileInfo, err error) error {
		if err != nil {
			w.logger.Warn(w.ctx, "Error walking path",
				adapters.Field{Key: "path", Value: walkPath},
				adapters.Field{Key: "error", Value: err.Error()})
			return nil // Continue walking
		}

		// Only watch directories
		if info.IsDir() && walkPath != path {
			if err := w.watcher.Add(walkPath); err != nil {
				w.logger.Warn(w.ctx, "Failed to watch subdirectory",
					adapters.Field{Key: "path", Value: walkPath},
					adapters.Field{Key: "error", Value: err.Error()})
				return nil // Continue walking
			}
			w.watching[walkPath] = true
			w.logger.Debug(w.ctx, "Started watching subdirectory",
				adapters.Field{Key: "path", Value: walkPath})
		}

		return nil
	})

	if err != nil {
		return &WatcherError{Op: "walk", Path: path, Err: err}
	}

	return nil
}

// Stop stops the watcher and releases resources.
func (w *FSNotifyWatcher) Stop() error {
	w.mu.Lock()
	if w.stopped {
		w.mu.Unlock()
		return nil
	}
	w.stopped = true
	w.mu.Unlock()

	w.logger.Info(w.ctx, "Stopping filesystem watcher")

	// Signal stop
	close(w.stopChan)

	// Cancel context to stop goroutines
	w.cancel()

	// Close the underlying watcher
	if err := w.watcher.Close(); err != nil {
		w.logger.Error(w.ctx, "Error closing fsnotify watcher",
			adapters.Field{Key: "error", Value: err.Error()})
	}

	// Wait for goroutines to finish
	w.wg.Wait()

	// Close events channel
	close(w.events)

	w.logger.Info(w.ctx, "Filesystem watcher stopped")
	return nil
}

// Events returns the read-only channel of filesystem events.
func (w *FSNotifyWatcher) Events() <-chan FileSystemEvent {
	return w.events
}

// processEvents processes fsnotify events and converts them to FileSystemEvents.
func (w *FSNotifyWatcher) processEvents() {
	defer w.wg.Done()

	for {
		select {
		case event, ok := <-w.watcher.Events:
			if !ok {
				w.logger.Debug(w.ctx, "Watcher events channel closed")
				return
			}

			w.handleEvent(event)

		case err, ok := <-w.watcher.Errors:
			if !ok {
				w.logger.Debug(w.ctx, "Watcher errors channel closed")
				return
			}

			w.logger.Error(w.ctx, "Filesystem watcher error",
				adapters.Field{Key: "error", Value: err.Error()})

		case <-w.stopChan:
			w.logger.Debug(w.ctx, "Watcher stop signal received")
			return

		case <-w.ctx.Done():
			w.logger.Debug(w.ctx, "Watcher context cancelled")
			return
		}
	}
}

// handleEvent processes a single fsnotify event.
func (w *FSNotifyWatcher) handleEvent(event fsnotify.Event) {
	// Skip metadata files and hidden files
	if w.shouldIgnore(event.Name) {
		return
	}

	// Debounce events for the same path
	if !w.shouldProcess(event.Name) {
		return
	}

	// Convert fsnotify event to FileSystemEvent
	fsEvent := w.convertEvent(event)
	if fsEvent == nil {
		return
	}

	// Handle directory creation for recursive watching
	if event.Op&fsnotify.Create == fsnotify.Create {
		w.handleCreate(event.Name)
	}

	// Send event (non-blocking)
	select {
	case w.events <- *fsEvent:
		w.logger.Debug(w.ctx, "Filesystem event emitted",
			adapters.Field{Key: "path", Value: fsEvent.Path},
			adapters.Field{Key: "operation", Value: fsEvent.Operation})
	default:
		w.logger.Warn(w.ctx, "Event channel full, dropping event",
			adapters.Field{Key: "path", Value: event.Name})
	}
}

// convertEvent converts fsnotify.Event to FileSystemEvent.
func (w *FSNotifyWatcher) convertEvent(event fsnotify.Event) *FileSystemEvent {
	var operation string

	switch {
	case event.Op&fsnotify.Create == fsnotify.Create:
		operation = "put"
	case event.Op&fsnotify.Write == fsnotify.Write:
		operation = "put"
	case event.Op&fsnotify.Remove == fsnotify.Remove:
		operation = "delete"
	case event.Op&fsnotify.Rename == fsnotify.Rename:
		operation = "delete"
	case event.Op&fsnotify.Chmod == fsnotify.Chmod:
		// Ignore chmod events
		return nil
	default:
		w.logger.Debug(w.ctx, "Ignoring unknown event type",
			adapters.Field{Key: "path", Value: event.Name},
			adapters.Field{Key: "op", Value: event.Op.String()})
		return nil
	}

	return &FileSystemEvent{
		Path:      event.Name,
		Operation: operation,
		Timestamp: time.Now(),
	}
}

// handleCreate handles directory creation for recursive watching.
func (w *FSNotifyWatcher) handleCreate(path string) {
	// Check if it's a directory
	info, err := os.Stat(path)
	if err != nil {
		// File might have been deleted or is not accessible
		return
	}

	if info.IsDir() {
		w.mu.Lock()
		defer w.mu.Unlock()

		// Add to watcher if not already watching
		if !w.watching[path] {
			if err := w.watcher.Add(path); err != nil {
				w.logger.Warn(w.ctx, "Failed to watch new directory",
					adapters.Field{Key: "path", Value: path},
					adapters.Field{Key: "error", Value: err.Error()})
				return
			}

			w.watching[path] = true
			w.logger.Info(w.ctx, "Started watching new directory",
				adapters.Field{Key: "path", Value: path})
		}

		// Recursively add watches to any subdirectories that were created
		// This handles cases like os.MkdirAll that create multiple directories at once
		_ = filepath.WalkDir(path, func(subpath string, d fs.DirEntry, err error) error { // #nosec G104 -- Best-effort subdirectory watch, individual errors handled in callback
			if err != nil || subpath == path {
				return nil
			}
			if d.IsDir() && !w.watching[subpath] {
				if err := w.watcher.Add(subpath); err != nil {
					w.logger.Warn(w.ctx, "Failed to watch subdirectory",
						adapters.Field{Key: "path", Value: subpath},
						adapters.Field{Key: "error", Value: err.Error()})
				} else {
					w.watching[subpath] = true
					w.logger.Debug(w.ctx, "Started watching subdirectory",
						adapters.Field{Key: "path", Value: subpath})
				}
			}
			return nil
		})
	}
}

// shouldIgnore determines if a path should be ignored.
func (w *FSNotifyWatcher) shouldIgnore(path string) bool {
	base := filepath.Base(path)

	// Ignore hidden files and directories
	if strings.HasPrefix(base, ".") {
		return true
	}

	// Ignore metadata files
	if strings.HasSuffix(base, ".metadata.json") {
		return true
	}

	// Ignore temporary files
	if strings.HasSuffix(base, "~") || strings.HasSuffix(base, ".tmp") {
		return true
	}

	return false
}

// shouldProcess determines if an event should be processed based on debouncing.
func (w *FSNotifyWatcher) shouldProcess(path string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()

	now := time.Now()
	lastTime, exists := w.lastEvent[path]

	if exists && now.Sub(lastTime) < w.debounceDelay {
		return false // Too soon, debounce
	}

	w.lastEvent[path] = now
	return true
}

// WatcherError represents an error from the filesystem watcher.
type WatcherError struct {
	Op   string
	Path string
	Err  error
}

func (e *WatcherError) Error() string {
	return fmt.Sprintf("watcher %s %s: %v", e.Op, e.Path, e.Err)
}

func (e *WatcherError) Unwrap() error {
	return e.Err
}

var (
	// ErrWatcherStopped is returned when operations are attempted on a stopped watcher.
	ErrWatcherStopped = errors.New("watcher is stopped")
)
