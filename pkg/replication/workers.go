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
	"sync/atomic"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
)

// WorkItem represents a single replication work unit.
type WorkItem struct {
	Key string
}

// WorkResult represents the result of processing a work item.
type WorkResult struct {
	Key       string
	Size      int64
	Err       error
	Succeeded bool
}

// WorkerPool manages a pool of workers for parallel object synchronization.
type WorkerPool struct {
	workerCount int
	workQueue   chan WorkItem
	resultQueue chan WorkResult
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	logger      adapters.Logger

	// Shutdown tracking
	shuttingDown atomic.Bool

	// Metrics tracking
	objectsProcessed atomic.Int64
	objectsSucceeded atomic.Int64
	objectsFailed    atomic.Int64
	bytesProcessed   atomic.Int64
}

// WorkerPoolConfig contains configuration for the worker pool.
type WorkerPoolConfig struct {
	WorkerCount int
	QueueSize   int
	Logger      adapters.Logger
}

// NewWorkerPool creates a new worker pool with the specified configuration.
func NewWorkerPool(config WorkerPoolConfig) *WorkerPool {
	if config.WorkerCount <= 0 {
		config.WorkerCount = 4 // Default to 4 workers
	}
	if config.QueueSize <= 0 {
		config.QueueSize = 100 // Default queue size
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &WorkerPool{
		workerCount: config.WorkerCount,
		workQueue:   make(chan WorkItem, config.QueueSize),
		resultQueue: make(chan WorkResult, config.QueueSize),
		ctx:         ctx,
		cancel:      cancel,
		logger:      config.Logger,
	}
}

// Start launches the worker goroutines.
func (wp *WorkerPool) Start(processor func(context.Context, WorkItem) WorkResult) {
	for i := 0; i < wp.workerCount; i++ {
		wp.wg.Add(1)
		go wp.worker(i, processor)
	}
}

// worker is a goroutine that processes work items from the queue.
func (wp *WorkerPool) worker(id int, processor func(context.Context, WorkItem) WorkResult) {
	defer wp.wg.Done()

	wp.logger.Debug(wp.ctx, "Worker started",
		adapters.Field{Key: "worker_id", Value: id})

	for {
		select {
		case <-wp.ctx.Done():
			wp.logger.Debug(wp.ctx, "Worker shutting down",
				adapters.Field{Key: "worker_id", Value: id})
			return

		case item, ok := <-wp.workQueue:
			if !ok {
				wp.logger.Debug(wp.ctx, "Worker queue closed",
					adapters.Field{Key: "worker_id", Value: id})
				return
			}

			// Process the work item
			result := processor(wp.ctx, item)

			// Update metrics
			wp.objectsProcessed.Add(1)
			if result.Succeeded {
				wp.objectsSucceeded.Add(1)
				wp.bytesProcessed.Add(result.Size)
			} else {
				wp.objectsFailed.Add(1)
			}

			// Send result back
			select {
			case wp.resultQueue <- result:
			case <-wp.ctx.Done():
				return
			}
		}
	}
}

// Submit adds a work item to the queue.
// Returns an error if the pool has been shut down.
func (wp *WorkerPool) Submit(item WorkItem) error {
	if wp.shuttingDown.Load() {
		return fmt.Errorf("worker pool is shutting down")
	}

	select {
	case <-wp.ctx.Done():
		return fmt.Errorf("worker pool context cancelled")
	case wp.workQueue <- item:
		return nil
	}
}

// Results returns the result channel for consuming worker outputs.
func (wp *WorkerPool) Results() <-chan WorkResult {
	return wp.resultQueue
}

// Shutdown performs a graceful shutdown of the worker pool.
// It closes the work queue and waits for all workers to finish.
func (wp *WorkerPool) Shutdown() {
	wp.logger.Info(wp.ctx, "Shutting down worker pool",
		adapters.Field{Key: "workers", Value: wp.workerCount})

	// Set shutdown flag to prevent new submissions
	wp.shuttingDown.Store(true)

	// Close work queue to signal no more work
	close(wp.workQueue)

	// Wait for all workers to finish processing queued items
	wp.wg.Wait()

	// Close result queue
	close(wp.resultQueue)

	// Cancel context after workers are done
	wp.cancel()

	wp.logger.Info(wp.ctx, "Worker pool shutdown complete",
		adapters.Field{Key: "processed", Value: wp.objectsProcessed.Load()},
		adapters.Field{Key: "succeeded", Value: wp.objectsSucceeded.Load()},
		adapters.Field{Key: "failed", Value: wp.objectsFailed.Load()},
		adapters.Field{Key: "bytes", Value: wp.bytesProcessed.Load()})
}

// GetMetrics returns the current worker pool metrics.
func (wp *WorkerPool) GetMetrics() WorkerPoolMetrics {
	return WorkerPoolMetrics{
		ObjectsProcessed: wp.objectsProcessed.Load(),
		ObjectsSucceeded: wp.objectsSucceeded.Load(),
		ObjectsFailed:    wp.objectsFailed.Load(),
		BytesProcessed:   wp.bytesProcessed.Load(),
	}
}

// WorkerPoolMetrics contains metrics about worker pool activity.
type WorkerPoolMetrics struct {
	ObjectsProcessed int64
	ObjectsSucceeded int64
	ObjectsFailed    int64
	BytesProcessed   int64
}
