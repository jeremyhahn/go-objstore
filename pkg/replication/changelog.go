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
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

// ChangeEvent represents a single change in the change log.
type ChangeEvent struct {
	Key       string          `json:"key"`
	Operation string          `json:"operation"` // "put", "delete"
	Timestamp time.Time       `json:"timestamp"`
	ETag      string          `json:"etag,omitempty"`
	Size      int64           `json:"size,omitempty"`
	Processed map[string]bool `json:"processed"` // policyID -> processed
}

// ChangeLog is the interface for tracking object changes.
type ChangeLog interface {
	// RecordChange records a new change event.
	RecordChange(event ChangeEvent) error

	// GetUnprocessed returns all events not yet processed by the given policy.
	GetUnprocessed(policyID string) ([]ChangeEvent, error)

	// MarkProcessed marks an event as processed by the given policy.
	MarkProcessed(key, policyID string) error

	// Rotate rotates the log file, creating a new one and archiving the old.
	Rotate() error

	// Close closes the change log and releases resources.
	Close() error
}

// JSONLChangeLog implements ChangeLog using JSON Lines format.
// Each line in the file is a JSON-encoded ChangeEvent.
// Thread-safe with mutex protection and atomic writes.
type JSONLChangeLog struct {
	file     *os.File
	filePath string
	mutex    sync.Mutex
	maxSize  int64
}

// NewJSONLChangeLog creates a new JSONL-based change log.
// maxSize is the maximum file size before rotation is triggered.
func NewJSONLChangeLog(filePath string, maxSize int64) (*JSONLChangeLog, error) {
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600) // #nosec G304 -- filePath from configuration, not user input
	if err != nil {
		return nil, fmt.Errorf("failed to open change log file: %w", err)
	}

	return &JSONLChangeLog{
		file:     file,
		filePath: filePath,
		maxSize:  maxSize,
	}, nil
}

// RecordChange records a new change event to the log.
// Thread-safe and performs atomic writes with sync.
func (cl *JSONLChangeLog) RecordChange(event ChangeEvent) error {
	cl.mutex.Lock()
	defer cl.mutex.Unlock()

	// Initialize Processed map if nil
	if event.Processed == nil {
		event.Processed = make(map[string]bool)
	}

	// Set timestamp if not already set
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}

	// Marshal event to JSON
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	// Write to file with newline
	_, err = cl.file.Write(append(data, '\n'))
	if err != nil {
		return fmt.Errorf("failed to write event: %w", err)
	}

	// Sync to disk for durability
	if err := cl.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}

	// Check if rotation is needed
	info, err := cl.file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat file: %w", err)
	}

	if info.Size() >= cl.maxSize {
		return cl.rotate()
	}

	return nil
}

// GetUnprocessed returns all events not yet processed by the given policy.
// Thread-safe read operation.
func (cl *JSONLChangeLog) GetUnprocessed(policyID string) ([]ChangeEvent, error) {
	cl.mutex.Lock()
	defer cl.mutex.Unlock()

	// Seek to beginning
	if _, err := cl.file.Seek(0, 0); err != nil {
		return nil, fmt.Errorf("failed to seek to start: %w", err)
	}

	var events []ChangeEvent
	scanner := bufio.NewScanner(cl.file)

	// Increase buffer size for large lines
	const maxCapacity = 1024 * 1024 // 1MB
	buf := make([]byte, maxCapacity)
	scanner.Buffer(buf, maxCapacity)

	for scanner.Scan() {
		var event ChangeEvent
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			// Log error but continue processing other events
			continue
		}

		// Check if this event has been processed by this policy
		if event.Processed == nil || !event.Processed[policyID] {
			events = append(events, event)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error scanning change log: %w", err)
	}

	return events, nil
}

// MarkProcessed marks an event as processed by the given policy.
// Thread-safe operation that rewrites the entire file.
func (cl *JSONLChangeLog) MarkProcessed(key, policyID string) error {
	cl.mutex.Lock()
	defer cl.mutex.Unlock()

	// Read all events
	if _, err := cl.file.Seek(0, 0); err != nil {
		return fmt.Errorf("failed to seek to start: %w", err)
	}

	var events []ChangeEvent
	scanner := bufio.NewScanner(cl.file)

	// Increase buffer size for large lines
	const maxCapacity = 1024 * 1024 // 1MB
	buf := make([]byte, maxCapacity)
	scanner.Buffer(buf, maxCapacity)

	for scanner.Scan() {
		var event ChangeEvent
		if err := json.Unmarshal(scanner.Bytes(), &event); err != nil {
			// Preserve invalid lines by re-encoding what we have
			continue
		}

		// Mark the matching event as processed
		if event.Key == key {
			if event.Processed == nil {
				event.Processed = make(map[string]bool)
			}
			event.Processed[policyID] = true
		}

		events = append(events, event)
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error scanning change log: %w", err)
	}

	// Rewrite file atomically
	return cl.rewriteFile(events)
}

// Rotate rotates the log file.
func (cl *JSONLChangeLog) Rotate() error {
	cl.mutex.Lock()
	defer cl.mutex.Unlock()
	return cl.rotate()
}

// rotate performs the actual rotation (must be called with mutex held).
func (cl *JSONLChangeLog) rotate() error {
	// Close current file
	if err := cl.file.Close(); err != nil {
		return fmt.Errorf("failed to close file for rotation: %w", err)
	}

	// Create backup filename with timestamp
	backupPath := fmt.Sprintf("%s.%d", cl.filePath, time.Now().Unix())

	// Rename current file to backup
	if err := os.Rename(cl.filePath, backupPath); err != nil {
		// Try to reopen the original file to maintain consistency
		file, openErr := os.OpenFile(cl.filePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
		if openErr != nil {
			return fmt.Errorf("%w: rename: %v, reopen: %v", ErrChangeLogRenameReopen, err, openErr)
		}
		cl.file = file
		return fmt.Errorf("failed to rename file: %w", err)
	}

	// Open new file
	file, err := os.OpenFile(cl.filePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return fmt.Errorf("failed to create new file: %w", err)
	}

	cl.file = file
	return nil
}

// rewriteFile rewrites the entire file with the given events (must be called with mutex held).
func (cl *JSONLChangeLog) rewriteFile(events []ChangeEvent) error {
	// Truncate file
	if err := cl.file.Truncate(0); err != nil {
		return fmt.Errorf("failed to truncate file: %w", err)
	}

	// Seek to beginning
	if _, err := cl.file.Seek(0, 0); err != nil {
		return fmt.Errorf("failed to seek to start: %w", err)
	}

	// Write all events
	for _, event := range events {
		data, err := json.Marshal(event)
		if err != nil {
			return fmt.Errorf("failed to marshal event: %w", err)
		}

		if _, err := cl.file.Write(append(data, '\n')); err != nil {
			return fmt.Errorf("failed to write event: %w", err)
		}
	}

	// Sync to disk
	if err := cl.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}

	return nil
}

// Close closes the change log file.
func (cl *JSONLChangeLog) Close() error {
	cl.mutex.Lock()
	defer cl.mutex.Unlock()

	if cl.file != nil {
		return cl.file.Close()
	}
	return nil
}
