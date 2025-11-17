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
	"sync"
	"testing"
	"time"

	"github.com/jeremyhahn/go-objstore/pkg/adapters"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWorkerPool_Process(t *testing.T) {
	logger := adapters.NewNoOpLogger()

	pool := NewWorkerPool(WorkerPoolConfig{
		WorkerCount: 2,
		QueueSize:   10,
		Logger:      logger,
	})

	// Track processed items
	var processed sync.Map
	processor := func(ctx context.Context, item WorkItem) WorkResult {
		processed.Store(item.Key, true)
		return WorkResult{
			Key:       item.Key,
			Size:      100,
			Succeeded: true,
		}
	}

	pool.Start(processor)

	// Submit work items
	items := []string{"key1", "key2", "key3", "key4", "key5"}
	for _, key := range items {
		err := pool.Submit(WorkItem{Key: key})
		require.NoError(t, err)
	}

	// Collect results
	results := make([]WorkResult, 0)
	done := make(chan struct{})
	go func() {
		for result := range pool.Results() {
			results = append(results, result)
		}
		close(done)
	}()

	// Shutdown and wait
	pool.Shutdown()
	<-done

	// Verify all items were processed
	assert.Equal(t, len(items), len(results))
	for _, key := range items {
		_, exists := processed.Load(key)
		assert.True(t, exists, "Expected key %s to be processed", key)
	}

	// Verify metrics
	metrics := pool.GetMetrics()
	assert.Equal(t, int64(5), metrics.ObjectsProcessed)
	assert.Equal(t, int64(5), metrics.ObjectsSucceeded)
	assert.Equal(t, int64(0), metrics.ObjectsFailed)
	assert.Equal(t, int64(500), metrics.BytesProcessed)
}

func TestWorkerPool_Concurrency(t *testing.T) {
	logger := adapters.NewNoOpLogger()
	workerCount := 4

	pool := NewWorkerPool(WorkerPoolConfig{
		WorkerCount: workerCount,
		QueueSize:   100,
		Logger:      logger,
	})

	// Track concurrent executions
	var activeWorkers sync.Map
	var maxConcurrent int32
	var mu sync.Mutex

	processor := func(ctx context.Context, item WorkItem) WorkResult {
		// Mark worker as active
		activeWorkers.Store(item.Key, true)

		// Count active workers
		count := 0
		activeWorkers.Range(func(key, value interface{}) bool {
			count++
			return true
		})

		mu.Lock()
		if int32(count) > maxConcurrent {
			maxConcurrent = int32(count)
		}
		mu.Unlock()

		// Simulate work
		time.Sleep(10 * time.Millisecond)

		activeWorkers.Delete(item.Key)

		return WorkResult{
			Key:       item.Key,
			Succeeded: true,
		}
	}

	pool.Start(processor)

	// Submit many work items
	itemCount := 20
	for i := 0; i < itemCount; i++ {
		err := pool.Submit(WorkItem{Key: string(rune(i))})
		require.NoError(t, err)
	}

	// Drain results
	go func() {
		for range pool.Results() {
		}
	}()

	pool.Shutdown()

	// Verify we had concurrent execution
	mu.Lock()
	assert.Greater(t, maxConcurrent, int32(1), "Expected concurrent execution")
	assert.LessOrEqual(t, maxConcurrent, int32(workerCount), "Should not exceed worker count")
	mu.Unlock()
}

func TestWorkerPool_ErrorHandling(t *testing.T) {
	logger := adapters.NewNoOpLogger()

	pool := NewWorkerPool(WorkerPoolConfig{
		WorkerCount: 2,
		QueueSize:   10,
		Logger:      logger,
	})

	processor := func(ctx context.Context, item WorkItem) WorkResult {
		// Fail on specific keys
		if item.Key == "fail1" || item.Key == "fail2" {
			return WorkResult{
				Key:       item.Key,
				Err:       errors.New("simulated error"),
				Succeeded: false,
			}
		}
		return WorkResult{
			Key:       item.Key,
			Size:      100,
			Succeeded: true,
		}
	}

	pool.Start(processor)

	// Submit mixed items
	items := []string{"success1", "fail1", "success2", "fail2", "success3"}
	for _, key := range items {
		err := pool.Submit(WorkItem{Key: key})
		require.NoError(t, err)
	}

	// Collect results
	results := make(map[string]WorkResult)
	done := make(chan struct{})
	go func() {
		for result := range pool.Results() {
			results[result.Key] = result
		}
		close(done)
	}()

	pool.Shutdown()
	<-done

	// Verify results
	assert.Equal(t, 5, len(results))

	// Check successful items
	assert.True(t, results["success1"].Succeeded)
	assert.True(t, results["success2"].Succeeded)
	assert.True(t, results["success3"].Succeeded)

	// Check failed items
	assert.False(t, results["fail1"].Succeeded)
	assert.NotNil(t, results["fail1"].Err)
	assert.False(t, results["fail2"].Succeeded)
	assert.NotNil(t, results["fail2"].Err)

	// Verify metrics
	metrics := pool.GetMetrics()
	assert.Equal(t, int64(5), metrics.ObjectsProcessed)
	assert.Equal(t, int64(3), metrics.ObjectsSucceeded)
	assert.Equal(t, int64(2), metrics.ObjectsFailed)
}

func TestWorkerPool_Shutdown(t *testing.T) {
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

	// Submit some items
	for i := 0; i < 5; i++ {
		err := pool.Submit(WorkItem{Key: string(rune(i))})
		require.NoError(t, err)
	}

	// Drain results
	go func() {
		for range pool.Results() {
		}
	}()

	// Shutdown should complete without hanging
	done := make(chan struct{})
	go func() {
		pool.Shutdown()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown did not complete in time")
	}

	// Submitting after shutdown should fail
	err := pool.Submit(WorkItem{Key: "late"})
	assert.Error(t, err)
}

func TestWorkerPool_LargeWorkload(t *testing.T) {
	logger := adapters.NewNoOpLogger()

	pool := NewWorkerPool(WorkerPoolConfig{
		WorkerCount: 8,
		QueueSize:   100,
		Logger:      logger,
	})

	var processed sync.Map
	processor := func(ctx context.Context, item WorkItem) WorkResult {
		// Simulate processing
		time.Sleep(time.Millisecond)
		processed.Store(item.Key, true)
		return WorkResult{
			Key:       item.Key,
			Size:      1024,
			Succeeded: true,
		}
	}

	pool.Start(processor)

	// Submit 100 items (reduced from 1000 for test performance)
	itemCount := 100
	for i := 0; i < itemCount; i++ {
		err := pool.Submit(WorkItem{Key: string(rune(i))})
		require.NoError(t, err)
	}

	// Drain results
	resultCount := 0
	done := make(chan struct{})
	go func() {
		for range pool.Results() {
			resultCount++
		}
		close(done)
	}()

	pool.Shutdown()
	<-done

	// Verify all items processed
	assert.Equal(t, itemCount, resultCount)

	metrics := pool.GetMetrics()
	assert.Equal(t, int64(itemCount), metrics.ObjectsProcessed)
	assert.Equal(t, int64(itemCount), metrics.ObjectsSucceeded)
	assert.Equal(t, int64(0), metrics.ObjectsFailed)
	assert.Equal(t, int64(itemCount*1024), metrics.BytesProcessed)
}

func TestWorkerPool_ContextCancellation(t *testing.T) {
	logger := adapters.NewNoOpLogger()

	pool := NewWorkerPool(WorkerPoolConfig{
		WorkerCount: 2,
		QueueSize:   10,
		Logger:      logger,
	})

	processor := func(ctx context.Context, item WorkItem) WorkResult {
		// Check if context is cancelled
		select {
		case <-ctx.Done():
			return WorkResult{
				Key:       item.Key,
				Err:       ctx.Err(),
				Succeeded: false,
			}
		default:
			return WorkResult{
				Key:       item.Key,
				Succeeded: true,
			}
		}
	}

	pool.Start(processor)

	// Submit items
	for i := 0; i < 5; i++ {
		err := pool.Submit(WorkItem{Key: string(rune(i))})
		require.NoError(t, err)
	}

	// Drain results
	go func() {
		for range pool.Results() {
		}
	}()

	// Shutdown should cancel context
	pool.Shutdown()

	// Verify shutdown completed
	metrics := pool.GetMetrics()
	assert.Greater(t, metrics.ObjectsProcessed, int64(0))
}

func TestWorkerPool_DefaultConfig(t *testing.T) {
	logger := adapters.NewNoOpLogger()

	// Test with zero/negative values should use defaults
	pool := NewWorkerPool(WorkerPoolConfig{
		WorkerCount: 0,
		QueueSize:   0,
		Logger:      logger,
	})

	assert.NotNil(t, pool)
	assert.Equal(t, 4, pool.workerCount) // Default worker count
	assert.NotNil(t, pool.workQueue)
	assert.NotNil(t, pool.resultQueue)
}
