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
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test context cancellation while sending result
func TestWorkerPool_ContextCancelDuringResult(t *testing.T) {
	logger := adapters.NewNoOpLogger()
	ctx, cancel := context.WithCancel(context.Background())

	// Create pool with custom context
	pool := &WorkerPool{
		ctx:         ctx,
		logger:      logger,
		workQueue:   make(chan WorkItem, 10),
		resultQueue: make(chan WorkResult, 1), // Small buffer to cause blocking
	}
	pool.wg.Add(1)

	// Start a single worker
	processor := func(ctx context.Context, item WorkItem) WorkResult {
		time.Sleep(10 * time.Millisecond)
		return WorkResult{
			Key:       item.Key,
			Succeeded: true,
			Size:      100,
		}
	}

	go pool.worker(1, processor)

	// Submit multiple work items to fill the result queue
	for i := 0; i < 5; i++ {
		pool.workQueue <- WorkItem{Key: "key"}
	}

	// Cancel context while worker is trying to send results
	time.Sleep(50 * time.Millisecond)
	cancel()

	// Wait for worker to exit
	pool.wg.Wait()
}

// Test Submit after context cancellation
func TestWorkerPool_SubmitAfterContextCancel(t *testing.T) {
	logger := adapters.NewNoOpLogger()
	ctx, cancel := context.WithCancel(context.Background())

	pool := &WorkerPool{
		ctx:       ctx,
		logger:    logger,
		workQueue: make(chan WorkItem, 2), // Small buffer
	}

	// Fill the work queue to block submissions
	pool.workQueue <- WorkItem{Key: "filler1"}
	pool.workQueue <- WorkItem{Key: "filler2"}

	// Cancel context
	cancel()

	// Give time for context to propagate
	time.Sleep(10 * time.Millisecond)

	// Try to submit - should fail due to context cancellation
	// The select will see both ctx.Done() and a full workQueue, and should choose ctx.Done()
	err := pool.Submit(WorkItem{Key: "test"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "context cancelled")
}

// Test worker receiving from closed queue
func TestWorkerPool_WorkerClosedQueue(t *testing.T) {
	logger := adapters.NewNoOpLogger()
	ctx := context.Background()

	pool := &WorkerPool{
		ctx:         ctx,
		logger:      logger,
		workQueue:   make(chan WorkItem, 10),
		resultQueue: make(chan WorkResult, 10),
	}
	pool.wg.Add(1)

	processor := func(ctx context.Context, item WorkItem) WorkResult {
		return WorkResult{
			Key:       item.Key,
			Succeeded: true,
		}
	}

	go pool.worker(1, processor)

	// Close the work queue (simulates shutdown)
	close(pool.workQueue)

	// Wait for worker to exit
	pool.wg.Wait()
}

// Test Submit when shutdown flag is set
func TestWorkerPool_SubmitDuringShutdown(t *testing.T) {
	logger := adapters.NewNoOpLogger()

	pool := NewWorkerPool(WorkerPoolConfig{
		WorkerCount: 2,
		QueueSize:   10,
		Logger:      logger,
	})

	processor := func(ctx context.Context, item WorkItem) WorkResult {
		return WorkResult{
			Key:       item.Key,
			Succeeded: true,
		}
	}

	pool.Start(processor)

	// Start shutdown
	pool.Shutdown()

	// Try to submit after shutdown started
	err := pool.Submit(WorkItem{Key: "test"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "shutting down")
}

// Test worker processing with both successful and failed results
func TestWorkerPool_MixedResults(t *testing.T) {
	logger := adapters.NewNoOpLogger()

	pool := NewWorkerPool(WorkerPoolConfig{
		WorkerCount: 2,
		QueueSize:   10,
		Logger:      logger,
	})

	processor := func(ctx context.Context, item WorkItem) WorkResult {
		// Fail items with even-numbered keys
		key := item.Key[0]
		if (key-'0')%2 == 0 {
			return WorkResult{
				Key:       item.Key,
				Succeeded: false,
				Err:       assert.AnError,
			}
		}
		return WorkResult{
			Key:       item.Key,
			Succeeded: true,
			Size:      100,
		}
	}

	pool.Start(processor)

	// Submit mix of items
	for i := 0; i < 10; i++ {
		err := pool.Submit(WorkItem{Key: string(rune('0' + i))})
		require.NoError(t, err)
	}

	// Collect results
	successCount := 0
	failCount := 0
	for i := 0; i < 10; i++ {
		result := <-pool.Results()
		if result.Succeeded {
			successCount++
		} else {
			failCount++
		}
	}

	pool.Shutdown()

	assert.Equal(t, 5, successCount)
	assert.Equal(t, 5, failCount)

	// Verify metrics
	metrics := pool.GetMetrics()
	assert.Equal(t, int64(10), metrics.ObjectsProcessed)
	assert.Equal(t, int64(5), metrics.ObjectsSucceeded)
	assert.Equal(t, int64(5), metrics.ObjectsFailed)
	assert.Equal(t, int64(500), metrics.BytesProcessed) // 5 successful * 100 bytes
}
