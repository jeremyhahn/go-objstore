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
	"fmt"
	"sync"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/jeremyhahn/go-objstore/pkg/audit"
	"github.com/jeremyhahn/go-objstore/pkg/common"
	"github.com/jeremyhahn/go-objstore/pkg/factory"
	"github.com/jeremyhahn/go-objstore/pkg/local"
)

// Syncer handles synchronization of objects between source and destination backends.
type Syncer struct {
	policy   common.ReplicationPolicy
	source   common.Storage
	dest     common.Storage
	logger   adapters.Logger
	auditLog audit.AuditLogger
	metrics  *ReplicationMetrics
}

// NewSyncer creates a new Syncer with proper encryption wrapping based on the policy.
// It handles three-layer encryption:
// - Layer 1: Backend at-rest encryption (for local backends)
// - Layer 2: Source DEK (client-side encryption)
// - Layer 3: Destination DEK (client-side encryption)
func NewSyncer(
	policy common.ReplicationPolicy,
	backendFactory common.EncrypterFactory,
	sourceFactory common.EncrypterFactory,
	destFactory common.EncrypterFactory,
	logger adapters.Logger,
	auditLog audit.AuditLogger,
) (*Syncer, error) {

	// Create source and destination backends
	source, err := factory.NewStorage(policy.SourceBackend, policy.SourceSettings)
	if err != nil {
		return nil, fmt.Errorf("failed to create source backend: %w", err)
	}

	dest, err := factory.NewStorage(policy.DestinationBackend, policy.DestinationSettings)
	if err != nil {
		return nil, fmt.Errorf("failed to create destination backend: %w", err)
	}

	// Set backend at-rest encryption if applicable (Layer 1)
	if policy.Encryption != nil && policy.Encryption.Backend != nil && policy.Encryption.Backend.Enabled {
		if policy.SourceBackend == "local" {
			if localBackend, ok := source.(*local.Local); ok {
				localBackend.SetAtRestEncrypterFactory(backendFactory)
			}
		}
		if policy.DestinationBackend == "local" {
			if localBackend, ok := dest.(*local.Local); ok {
				localBackend.SetAtRestEncrypterFactory(backendFactory)
			}
		}
	}

	// Apply CLIENT-SIDE DEK encryption based on mode (Layers 2 & 3)
	switch policy.ReplicationMode {
	case common.ReplicationModeTransparent:
		// In transparent mode, decrypt at source and re-encrypt at destination
		if policy.Encryption != nil && policy.Encryption.Source != nil && policy.Encryption.Source.Enabled {
			source = common.NewEncryptedStorage(source, sourceFactory)
		}
		if policy.Encryption != nil && policy.Encryption.Destination != nil && policy.Encryption.Destination.Enabled {
			dest = common.NewEncryptedStorage(dest, destFactory)
		}

	case common.ReplicationModeOpaque:
		// In opaque mode, don't wrap with DEK encryption - copy blobs as-is
		// Backend at-rest encryption still applies if configured

	default:
		return nil, fmt.Errorf("unsupported replication mode: %s", policy.ReplicationMode)
	}

	return &Syncer{
		policy:   policy,
		source:   source,
		dest:     dest,
		logger:   logger,
		auditLog: auditLog,
		metrics:  NewReplicationMetrics(),
	}, nil
}

// SyncAll synchronizes all changed objects from source to destination.
func (s *Syncer) SyncAll(ctx context.Context) (*common.SyncResult, error) {
	startTime := time.Now()
	result := &common.SyncResult{
		PolicyID: s.policy.ID,
	}

	// Detect changes
	detector := NewChangeDetector(s.source, s.dest)
	changedKeys, err := detector.DetectChanges(ctx, s.policy.SourcePrefix)
	if err != nil {
		return nil, fmt.Errorf("change detection failed: %w", err)
	}

	// Sync each changed object
	for _, key := range changedKeys {
		size, err := s.SyncObject(ctx, key)
		if err != nil {
			result.Failed++
			result.Errors = append(result.Errors, fmt.Sprintf("%s: %v", key, err))
			s.logger.Error(ctx, "Object sync failed",
				adapters.Field{Key: "key", Value: key},
				adapters.Field{Key: "error", Value: err.Error()})
		} else {
			result.Synced++
			result.BytesTotal += size
		}
	}

	result.Duration = time.Since(startTime)

	// Update metrics
	s.metrics.IncrementObjectsSynced(int64(result.Synced))
	s.metrics.IncrementErrors(int64(result.Failed))
	s.metrics.IncrementBytesSynced(result.BytesTotal)
	s.metrics.RecordSync(result.Duration)

	s.logger.Info(ctx, "Sync completed",
		adapters.Field{Key: "policy_id", Value: s.policy.ID},
		adapters.Field{Key: "synced", Value: result.Synced},
		adapters.Field{Key: "failed", Value: result.Failed},
		adapters.Field{Key: "duration", Value: result.Duration.String()})

	return result, nil
}

// SyncAllParallel synchronizes all changed objects using a worker pool.
// This provides better performance for large datasets through parallel processing.
func (s *Syncer) SyncAllParallel(ctx context.Context, workerCount int) (*common.SyncResult, error) {
	startTime := time.Now()
	result := &common.SyncResult{
		PolicyID: s.policy.ID,
	}

	if workerCount <= 0 {
		workerCount = 4 // Default to 4 workers
	}

	// Detect changes
	detector := NewChangeDetector(s.source, s.dest)
	changedKeys, err := detector.DetectChanges(ctx, s.policy.SourcePrefix)
	if err != nil {
		return nil, fmt.Errorf("change detection failed: %w", err)
	}

	if len(changedKeys) == 0 {
		result.Duration = time.Since(startTime)
		s.logger.Info(ctx, "No changes detected",
			adapters.Field{Key: "policy_id", Value: s.policy.ID})
		return result, nil
	}

	s.logger.Info(ctx, "Starting parallel sync",
		adapters.Field{Key: "policy_id", Value: s.policy.ID},
		adapters.Field{Key: "objects", Value: len(changedKeys)},
		adapters.Field{Key: "workers", Value: workerCount})

	// Create worker pool
	pool := NewWorkerPool(WorkerPoolConfig{
		WorkerCount: workerCount,
		QueueSize:   len(changedKeys),
		Logger:      s.logger,
	})

	// Start workers with sync processor
	pool.Start(func(ctx context.Context, item WorkItem) WorkResult {
		size, err := s.SyncObject(ctx, item.Key)
		return WorkResult{
			Key:       item.Key,
			Size:      size,
			Err:       err,
			Succeeded: err == nil,
		}
	})

	// Submit all work items
	for _, key := range changedKeys {
		if err := pool.Submit(WorkItem{Key: key}); err != nil {
			s.logger.Error(ctx, "Failed to submit work item",
				adapters.Field{Key: "key", Value: key},
				adapters.Field{Key: "error", Value: err.Error()})
			result.Failed++
		}
	}

	// Collect results
	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		for workResult := range pool.Results() {
			mu.Lock()
			if workResult.Succeeded {
				result.Synced++
				result.BytesTotal += workResult.Size
			} else {
				result.Failed++
				result.Errors = append(result.Errors,
					fmt.Sprintf("%s: %v", workResult.Key, workResult.Err))
			}
			mu.Unlock()
		}
	}()

	// Shutdown pool and wait for results
	pool.Shutdown()
	wg.Wait()

	result.Duration = time.Since(startTime)

	// Update metrics
	s.metrics.IncrementObjectsSynced(int64(result.Synced))
	s.metrics.IncrementErrors(int64(result.Failed))
	s.metrics.IncrementBytesSynced(result.BytesTotal)
	s.metrics.RecordSync(result.Duration)

	s.logger.Info(ctx, "Parallel sync completed",
		adapters.Field{Key: "policy_id", Value: s.policy.ID},
		adapters.Field{Key: "synced", Value: result.Synced},
		adapters.Field{Key: "failed", Value: result.Failed},
		adapters.Field{Key: "bytes", Value: result.BytesTotal},
		adapters.Field{Key: "duration", Value: result.Duration.String()})

	return result, nil
}

// SyncObject synchronizes a single object from source to destination.
// Returns the size of the object synced.
func (s *Syncer) SyncObject(ctx context.Context, key string) (int64, error) {
	// Get from source (automatically decrypted if encrypted)
	reader, err := s.source.GetWithContext(ctx, key)
	if err != nil {
		return 0, fmt.Errorf("failed to read source: %w", err)
	}
	defer reader.Close()

	// Get source metadata
	srcMetadata, err := s.source.GetMetadata(ctx, key)
	if err != nil {
		return 0, fmt.Errorf("failed to get metadata: %w", err)
	}

	// Put to destination (automatically encrypted if enabled)
	err = s.dest.PutWithMetadata(ctx, key, reader, srcMetadata)
	if err != nil {
		_ = s.auditLog.LogObjectMutation(ctx, "replication_failed",
			"", "", "", key, "", "", 0, "failure", err)
		return 0, fmt.Errorf("failed to write destination: %w", err)
	}

	// Audit log success
	_ = s.auditLog.LogObjectMutation(ctx, "replication_success",
		"", "", "", key, "", "", srcMetadata.Size, "success", nil)

	s.logger.Debug(ctx, "Object synced",
		adapters.Field{Key: "key", Value: key},
		adapters.Field{Key: "size", Value: srcMetadata.Size})

	return srcMetadata.Size, nil
}

// GetMetrics returns the current replication metrics.
func (s *Syncer) GetMetrics() *ReplicationMetrics {
	return s.metrics
}

// Close releases any resources held by the syncer.
// This is a no-op for now but provides a cleanup hook for future enhancements.
func (s *Syncer) Close() error {
	return nil
}

// SyncIncremental synchronizes only unprocessed changes from the change log.
// This provides efficient incremental replication by processing only new changes
// since the last sync, rather than scanning all objects.
func (s *Syncer) SyncIncremental(ctx context.Context, changeLog ChangeLog) (*common.SyncResult, error) {
	startTime := time.Now()
	result := &common.SyncResult{
		PolicyID: s.policy.ID,
	}

	// Get unprocessed changes for this policy
	changes, err := changeLog.GetUnprocessed(s.policy.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to get unprocessed changes: %w", err)
	}

	s.logger.Info(ctx, "Starting incremental sync",
		adapters.Field{Key: "policy_id", Value: s.policy.ID},
		adapters.Field{Key: "unprocessed_changes", Value: len(changes)})

	// Process each change
	for _, change := range changes {
		var size int64
		var err error

		switch change.Operation {
		case "put":
			// Sync the object
			size, err = s.SyncObject(ctx, change.Key)
			if err != nil {
				result.Failed++
				result.Errors = append(result.Errors, fmt.Sprintf("%s: %v", change.Key, err))
				s.logger.Error(ctx, "Object sync failed",
					adapters.Field{Key: "key", Value: change.Key},
					adapters.Field{Key: "operation", Value: "put"},
					adapters.Field{Key: "error", Value: err.Error()})
			} else {
				result.Synced++
				result.BytesTotal += size
				// Mark as processed
				if markErr := changeLog.MarkProcessed(change.Key, s.policy.ID); markErr != nil {
					s.logger.Warn(ctx, "Failed to mark change as processed",
						adapters.Field{Key: "key", Value: change.Key},
						adapters.Field{Key: "error", Value: markErr.Error()})
				}
			}

		case "delete":
			// Delete from destination
			err = s.dest.DeleteWithContext(ctx, change.Key)
			if err != nil {
				result.Failed++
				result.Errors = append(result.Errors, fmt.Sprintf("%s: %v", change.Key, err))
				s.logger.Error(ctx, "Object delete failed",
					adapters.Field{Key: "key", Value: change.Key},
					adapters.Field{Key: "operation", Value: "delete"},
					adapters.Field{Key: "error", Value: err.Error()})
			} else {
				result.Deleted++
				// Audit log
				_ = s.auditLog.LogObjectMutation(ctx, "replication_delete",
					"", "", "", change.Key, "", "", 0, "success", nil)

				// Mark as processed
				if markErr := changeLog.MarkProcessed(change.Key, s.policy.ID); markErr != nil {
					s.logger.Warn(ctx, "Failed to mark change as processed",
						adapters.Field{Key: "key", Value: change.Key},
						adapters.Field{Key: "error", Value: markErr.Error()})
				}
			}

		default:
			s.logger.Warn(ctx, "Unknown operation in change log",
				adapters.Field{Key: "key", Value: change.Key},
				adapters.Field{Key: "operation", Value: change.Operation})
		}
	}

	result.Duration = time.Since(startTime)

	// Update metrics
	s.metrics.IncrementObjectsSynced(int64(result.Synced))
	s.metrics.IncrementErrors(int64(result.Failed))
	s.metrics.IncrementBytesSynced(result.BytesTotal)
	s.metrics.RecordSync(result.Duration)

	s.logger.Info(ctx, "Incremental sync completed",
		adapters.Field{Key: "policy_id", Value: s.policy.ID},
		adapters.Field{Key: "synced", Value: result.Synced},
		adapters.Field{Key: "deleted", Value: result.Deleted},
		adapters.Field{Key: "failed", Value: result.Failed},
		adapters.Field{Key: "bytes", Value: result.BytesTotal},
		adapters.Field{Key: "duration", Value: result.Duration.String()})

	return result, nil
}
